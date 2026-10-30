defmodule GravitonMQ.MQTTBroker.PersistenceTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.Core.CommitRef
  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Connect, Connack, Publish, Puback, Pubrec, Pubrel, Pubcomp}
  alias GravitonMQ.Storage.WAL

  @moduletag timeout: 15_000

  defmodule BarrierPolicy do
    def authenticate(_credentials, _options), do: {:ok, :user}

    def authorize(_principal, :publish, "barrier", options) do
      send(options[:observer], {:authorizing, self()})

      receive do
        :allow -> :ok
      end
    end

    def authorize(_principal, _action, _topic, _options), do: :ok
  end

  setup do
    parent =
      Path.join(
        System.tmp_dir!(),
        "graviton-mqtt-persistence-#{System.unique_integer([:positive])}"
      )

    File.mkdir!(parent)

    flags =
      start_supervised!(
        {Agent,
         fn ->
           table = :ets.new(__MODULE__, [:public])
           :ets.insert(table, {:fault, :none})
           table
         end}
      )

    table = Agent.get(flags, & &1)
    on_exit(fn -> File.rm_rf!(parent) end)
    %{parent: parent, path: Path.join(parent, "broker"), table: table}
  end

  defp broker(context, options \\ []) do
    observer = self()

    fault = fn stage, _detail ->
      case :ets.lookup(context.table, :fault) do
        [{:fault, {:hold, ^stage}}] ->
          send(observer, {:sync_barrier, self()})

          receive do
            :release -> :ok
          end

        [{:fault, {:fail, ^stage}}] ->
          {:error, :eio}

        [{:fault, {:unknown, ^stage}}] ->
          {:unknown, :eio}

        _ ->
          :ok
      end
    end

    mqtt =
      Keyword.merge(
        [
          auth: :anonymous,
          # CONNECT's authentication deadline includes its durable preparation.
          auth_timeout: 3_000,
          host_principal: :host,
          tick_interval: 25,
          persistence: [directory: context.path, stream_id: "mqtt-test", fault_injector: fault],
          listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
        ],
        options
      )

    id = make_ref()

    instance =
      start_supervised!({GravitonMQ, name: nil, runtime_supervisor_name: nil, mqtt: mqtt}, id: id)

    {instance, id}
  end

  defp status(instance) do
    {:ok, status} = GravitonMQ.status(instance)
    status
  end

  defp publish(instance, topic, payload, options) do
    assert {:ok, reference} = GravitonMQ.publish(instance, topic, payload, options)

    assert_receive {:graviton_mq, _,
                    {:operation, ^reference, {:ok, {:accepted_durable, %CommitRef{} = durable}}}},
                   2_000

    durable
  end

  defp subscribe(instance, filter, options) do
    assert {:ok, reference} = GravitonMQ.subscribe(instance, filter, options)
    assert_receive {:graviton_mq, _, {:operation, ^reference, {:ok, subscription}}}, 2_000
    subscription
  end

  test "durable host completion and deliveries follow the final directory synchronization",
       context do
    {instance, _} = broker(context, auth: {BarrierPolicy, observer: self()})
    subscription = subscribe(instance, "barrier", qos: 1)
    assert :ok = GravitonMQ.request(instance, subscription, 1)

    assert {:ok, reference} =
             GravitonMQ.publish(instance, "barrier", "held", qos: 1, retain: true)

    assert_receive {:authorizing, callback}, 2_000
    :ets.insert(context.table, {:fault, {:hold, :directory_sync}})
    send(callback, :allow)
    assert_receive {:sync_barrier, wal}, 2_000
    refute_receive {:graviton_mq, _, {:operation, ^reference, {:ok, _}}}, 50
    refute_receive {:graviton_mq, _, {:delivery, _, _, _}}, 50
    :ets.insert(context.table, {:fault, :none})
    send(wal, :release)

    assert_receive {:graviton_mq, _,
                    {:operation, ^reference, {:ok, {:accepted_durable, durable}}}},
                   2_000

    assert {:ok, boundary} = WAL.durable_through(wal)
    assert boundary.stream_id == durable.stream_id and boundary.position >= durable.position

    assert_receive {:graviton_mq, _, {:delivery, ^subscription, delivery, %{payload: "held"}}},
                   2_000

    assert :ok = GravitonMQ.ack(instance, delivery)
  end

  for qos <- [1, 2] do
    @qos qos
    test "QoS#{qos} positive acknowledgement waits for durable snapshot publication", context do
      {instance, _} = broker(context, auth: {BarrierPolicy, observer: self()})
      socket = connected(status(instance).listeners |> hd() |> Map.fetch!(:port), "network")

      send_packet(socket, %Publish{
        topic: "barrier",
        payload: "wire",
        qos: @qos,
        packet_id: 19,
        retain: true
      })

      assert_receive {:authorizing, callback}, 2_000
      :ets.insert(context.table, {:fault, {:hold, :directory_sync}})
      send(callback, :allow)
      assert_receive {:sync_barrier, wal}, 2_000
      assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 50)
      :ets.insert(context.table, {:fault, :none})
      send(wal, :release)
      response = receive_packet(socket)
      assert response.__struct__ == if(@qos == 1, do: Puback, else: Pubrec)
      assert response.packet_id == 19 and response.reason_code == 0

      assert %{
               profile: :local_durable,
               storage: :ready,
               retained: 1,
               durable_through: %CommitRef{}
             } = status(instance)
    end
  end

  test "PUBCOMP waits for durable incoming QoS2 marker retirement", context do
    {instance, _} = broker(context)
    socket = connected(status(instance).listeners |> hd() |> Map.fetch!(:port), "retirement")
    send_packet(socket, %Publish{topic: "a", payload: "q2", qos: 2, packet_id: 37, retain: true})
    assert %Pubrec{packet_id: 37} = receive_packet(socket)
    :ets.insert(context.table, {:fault, {:hold, :directory_sync}})
    send_packet(socket, %Pubrel{packet_id: 37})
    assert_receive {:sync_barrier, wal}, 2_000
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 50)
    :ets.insert(context.table, {:fault, :none})
    send(wal, :release)
    assert %Pubcomp{packet_id: 37, reason_code: 0} = receive_packet(socket)
  end

  test "restart restores retained content but removes host ownership and prevents caller identity reuse",
       context do
    {instance, id} = broker(context)
    durable = publish(instance, "state/a", "survived", qos: 2, retain: true)
    _subscription = subscribe(instance, "discard/#", qos: 1)
    assert %{host_subscriptions: 1} = status(instance)
    assert :ok = stop_supervised(id)
    {restarted, _} = broker(context)
    assert %{retained: 1, host_subscriptions: 0, durable_through: recovered} = status(restarted)
    assert recovered.stream_id == durable.stream_id and recovered.position > durable.position
    subscription = subscribe(restarted, "state/#", qos: 2, no_local: true)
    assert :ok = GravitonMQ.request(restarted, subscription, 1)

    assert_receive {:graviton_mq, _,
                    {:delivery, ^subscription, _, %{payload: "survived", qos: 2, retain: true}}},
                   2_000
  end

  for stage <- [
        :wal_sync,
        :checkpoint_write,
        :checkpoint_sync,
        :checkpoint_rename,
        :directory_sync
      ] do
    @stage stage
    test "#{stage} failure closes the peer and never acknowledges a publication", context do
      {instance, _} = broker(context)
      socket = connected(status(instance).listeners |> hd() |> Map.fetch!(:port), "failure")
      :ets.insert(context.table, {:fault, {:fail, @stage}})

      send_packet(socket, %Publish{
        topic: "a",
        payload: "uncertain",
        packet_id: 1,
        qos: 1,
        retain: true
      })

      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2_000)
      assert %{storage: :failed, connections: 0} = status(instance)
      assert {:error, :persistence_failed} = GravitonMQ.publish(instance, "a", "cannot-succeed")
    end
  end

  test "a completed but uncertain synchronization reports unknown and recovers possible committed content",
       context do
    {instance, id} = broker(context, auth: {BarrierPolicy, observer: self()})

    assert {:ok, reference} =
             GravitonMQ.publish(instance, "barrier", "possible", qos: 1, retain: true)

    assert_receive {:authorizing, callback}, 2_000
    :ets.insert(context.table, {:fault, {:unknown, :directory_sync}})
    send(callback, :allow)
    assert_receive {:graviton_mq, _, {:operation, ^reference, {:error, :unknown_outcome}}}, 2_000
    refute_receive {:graviton_mq, _, {:operation, ^reference, {:ok, _}}}, 50
    assert %{storage: :failed} = status(instance)
    :ets.insert(context.table, {:fault, :none})
    assert :ok = stop_supervised(id)
    {restarted, _} = broker(context)
    subscription = subscribe(restarted, "barrier", qos: 1)
    assert :ok = GravitonMQ.request(restarted, subscription, 1)
    assert_receive {:graviton_mq, _, {:delivery, ^subscription, _, %{payload: "possible"}}}, 2_000
  end

  test "pre-append snapshot limits report failure without success or retained mutation",
       context do
    {instance, _} =
      broker(context,
        persistence: [
          directory: context.path,
          stream_id: "small",
          max_snapshot_bytes: 1024,
          max_log_bytes: 65_536
        ]
      )

    assert {:ok, reference} =
             GravitonMQ.publish(instance, "a", String.duplicate("x", 2048), qos: 1, retain: true)

    assert_receive {:graviton_mq, _, {:operation, ^reference, {:error, :persistence_failed}}},
                   2_000

    assert %{storage: :failed, retained: 0} = status(instance)
    refute_receive {:graviton_mq, _, {:operation, ^reference, {:ok, _}}}, 30
  end

  test "different durable instances preserve independent retained states", context do
    {first, _} = broker(context)
    second_context = %{context | path: Path.join(context.parent, "other")}
    {second, _} = broker(second_context)
    publish(first, "a", "first", qos: 1, retain: true)
    assert %{retained: 0} = status(second)
    publish(second, "a", "second", qos: 1, retain: true)
    assert %{retained: 1} = status(first)
    assert %{retained: 1} = status(second)
  end

  defp connected(port, id) do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    on_exit(fn -> :gen_tcp.close(socket) end)

    send_packet(socket, %Connect{
      version: :v5,
      client_id: id,
      clean_start: true,
      properties: [session_expiry_interval: 60]
    })

    assert %Connack{reason_code: 0} = receive_packet(socket)
    socket
  end

  defp send_packet(socket, packet) do
    {:ok, encoded} = Packet.encode(packet)
    assert :ok = :gen_tcp.send(socket, encoded)
  end

  defp receive_packet(socket, bytes \\ <<>>) do
    case Packet.decode(bytes, :v5) do
      {:ok, packet, <<>>} ->
        packet

      {:more, _} ->
        assert {:ok, received} = :gen_tcp.recv(socket, 0, 2_000)
        receive_packet(socket, bytes <> received)
    end
  end
end
