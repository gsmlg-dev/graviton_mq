defmodule GravitonMQ.MQTTBroker.FeaturePersistenceTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.Core.CommitRef
  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Puback, Publish, Suback, Subscribe}

  @moduletag timeout: 120_000

  setup do
    parent =
      Path.join(System.tmp_dir!(), "graviton-mqtt-feature-#{System.unique_integer([:positive])}")

    File.mkdir!(parent)
    on_exit(fn -> File.rm_rf!(parent) end)
    %{path: Path.join(parent, "broker")}
  end

  defp start(context, extra \\ []) do
    mqtt =
      [
        auth: :anonymous,
        host_principal: :host,
        auth_timeout: 3_000,
        tick_interval: 25,
        persistence: [directory: context.path, stream_id: "mqtt-feature"],
        listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
      ] ++ extra

    start_supervised({GravitonMQ, name: nil, runtime_supervisor_name: nil, mqtt: mqtt},
      id: :broker
    )
  end

  defp port(instance) do
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    port
  end

  defp connect(port, client_id, clean_start) do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    on_exit(fn -> :gen_tcp.close(socket) end)

    send_packet(socket, %Connect{
      version: :v5,
      client_id: client_id,
      clean_start: clean_start,
      properties: [session_expiry_interval: 600]
    })

    {socket, receive_packet(socket)}
  end

  defp send_packet(socket, packet) do
    {:ok, encoded} = Packet.encode(packet)
    :ok = :gen_tcp.send(socket, encoded)
  end

  # CONNACK and a resumed PUBLISH can share one TCP read; keep the remainder.
  defp receive_packet(socket) do
    buffer = Process.get({:mqtt_buffer, socket}, <<>>)

    case Packet.decode(buffer, :v5) do
      {:ok, packet, rest} ->
        Process.put({:mqtt_buffer, socket}, rest)
        packet

      {:more, _} ->
        {:ok, received} = :gen_tcp.recv(socket, 0, 5_000)
        Process.put({:mqtt_buffer, socket}, buffer <> received)
        receive_packet(socket)
    end
  end

  defp wait(instance, predicate, deadline \\ 100) do
    {:ok, status} = GravitonMQ.status(instance)

    cond do
      predicate.(status) -> status
      deadline == 0 -> flunk("status condition timed out: #{inspect(status)}")
      true -> Process.sleep(50) && wait(instance, predicate, deadline - 1)
    end
  end

  defp subscribe(socket, filter, identifier) do
    send_packet(socket, %Subscribe{
      packet_id: 1,
      subscriptions: [{filter, 1}],
      properties: [subscription_identifier: identifier]
    })

    assert %Suback{packet_id: 1, reason_codes: [1]} = receive_packet(socket)
  end

  defp publish(instance, topic, payload) do
    assert {:ok, reference} = GravitonMQ.publish(instance, topic, payload, qos: 1)

    assert_receive {:graviton_mq, _,
                    {:operation, ^reference, {:ok, {:accepted_durable, %CommitRef{}}}}},
                   10_000
  end

  # Offline persistent members: the shared copy is bound to the member's
  # session at publication time and is part of durable state.
  defp seed(instance) do
    {member, %Connack{reason_code: 0}} = connect(port(instance), "durable-member", true)
    subscribe(member, "$share/g/d", 5)
    {plain, %Connack{reason_code: 0}} = connect(port(instance), "durable-plain", true)
    subscribe(plain, "p/#", 6)
    :ok = :gen_tcp.close(member)
    :ok = :gen_tcp.close(plain)
    wait(instance, &(&1.connections == 0))
    publish(instance, "d", "kept")
    publish(instance, "p/x", "plain")

    assert %{pending_deliveries: 2, shared_groups: 1} =
             wait(instance, &(&1.pending_deliveries == 2))
  end

  defp assert_recovered(restarted) do
    assert %{shared_groups: 1, sessions: 2, pending_deliveries: 2, shared_discarded: 0} =
             wait(restarted, &(&1.shared_groups == 1))

    {member, %Connack{reason_code: 0, session_present: true}} =
      connect(port(restarted), "durable-member", false)

    assert %Publish{topic: "d", payload: "kept", qos: 1, packet_id: id, properties: properties} =
             receive_packet(member)

    assert Keyword.get_values(properties, :subscription_identifier) == [5]
    send_packet(member, %Puback{packet_id: id})

    {plain, %Connack{reason_code: 0, session_present: true}} =
      connect(port(restarted), "durable-plain", false)

    assert %Publish{topic: "p/x", payload: "plain", packet_id: id, properties: properties} =
             receive_packet(plain)

    assert Keyword.get_values(properties, :subscription_identifier) == [6]
    send_packet(plain, %Puback{packet_id: id})
    wait(restarted, &(&1.pending_deliveries == 0))
    :ok = :gen_tcp.close(member)
    :ok = :gen_tcp.close(plain)
  end

  # Spec §5 owner-kill evidence: the runtime MQTT owner is killed brutally
  # (no terminate callback, no final checkpoint) and its supervisor restarts
  # it from durable state alone.
  test "shared membership, Subscription Identifiers and bound deliveries survive an owner kill",
       context do
    {:ok, instance} = start(context)
    seed(instance)
    {:ok, %{owner: original}} = GravitonMQ.status(instance)
    monitor = Process.monitor(original)
    Process.exit(original, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^original, :killed}, 2_000

    wait(instance, fn status -> status.owner != original end)
    assert_recovered(instance)
  end

  test "shared membership, Subscription Identifiers and bound deliveries survive an owner restart; a disabled feature refuses startup",
       context do
    {:ok, instance} = start(context)
    seed(instance)
    :ok = stop_supervised(:broker)
    {:ok, restarted} = start(context)
    assert_recovered(restarted)
    :ok = stop_supervised(:broker)

    # The checkpoint holds a shared membership and Subscription Identifiers:
    # an owner that disables either feature must not silently drop them.
    for option <- [shared_subscriptions: false, subscription_identifiers: false] do
      # `GravitonMQ.start_link/1` fails with the owner's init reason nested in
      # the supervisors' standard `:failed_to_start_child` shutdown chain.
      assert {:error, {reason, _child}} = start(context, [option])

      assert {:shutdown,
              {:failed_to_start_child, GravitonMQ.Runtime.Supervisor,
               {:shutdown,
                {:failed_to_start_child, GravitonMQ.Runtime.MQTT.Supervisor,
                 {:shutdown,
                  {:failed_to_start_child, GravitonMQ.Runtime.MQTT.Broker,
                   :checkpoint_capability_mismatch}}}}}} = reason
    end

    # The refused starts left durable state intact.
    {:ok, again} = start(context)
    assert %{shared_groups: 1, sessions: 2} = wait(again, &(&1.shared_groups == 1))
  end
end
