defmodule GravitonMQ.MQTTClient.SharedSubscriptionTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Connack, Suback, Subscribe}
  alias GravitonMQ.MQTTClient

  @moduletag :tmp_dir
  @moduletag timeout: 60_000

  test "persistent shared subscriptions are captured, recovered and reported on session loss",
       context do
    directory = Path.join(context.tmp_dir, "store")
    {client, socket} = connected(id: :first, persistence: [directory: directory])

    assert {:ok, subscription} = MQTTClient.subscribe(client, "$share/workers/events/#", qos: 1)

    assert %Subscribe{packet_id: id, subscriptions: [{"$share/workers/events/#", %{qos: 1}}]} =
             receive_packet(socket)

    send_packet(socket, %Suback{packet_id: id, reason_codes: [1]})
    assert_receive {:graviton_mq, ^client, {:operation, ^subscription, {:ok, [1]}}}, 15_000
    refute_receive {:graviton_mq, ^client, {:storage_error, _}}, 100
    assert MQTTClient.status(client).phase == :connected
    :ok = stop_supervised(:first)

    {restarted, _socket} = connected(id: :second, persistence: [directory: directory])

    assert_receive {:graviton_mq, ^restarted,
                    {:subscriptions_lost, %{"$share/workers/events/#" => 1}}},
                   15_000
  end

  test "subscribe accepts a Subscription Identifier option and rejects a duplicate property" do
    {client, socket} = connected([])

    assert {:ok, subscription} =
             MQTTClient.subscribe(client, "ids/#", qos: 1, subscription_identifier: 42)

    assert %Subscribe{packet_id: id, properties: [subscription_identifier: 42]} =
             receive_packet(socket)

    send_packet(socket, %Suback{packet_id: id, reason_codes: [1]})
    assert_receive {:graviton_mq, ^client, {:operation, ^subscription, {:ok, [1]}}}, 1_000

    assert {:error, :invalid_options} =
             MQTTClient.subscribe(client, "ids/#",
               subscription_identifier: 1,
               properties: [subscription_identifier: 2]
             )
  end

  test "subscribe rejects an out-of-range Subscription Identifier option" do
    {client, socket} = connected([])

    for identifier <- [0, 268_435_456, :one] do
      assert {:error, {:invalid_packet, :invalid_property_value}} =
               MQTTClient.subscribe(client, "ids/#", subscription_identifier: identifier)
    end

    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 100)
    assert MQTTClient.status(client).phase == :connected
    assert MQTTClient.status(client).pending == 0
  end

  test "alias options are advertised and the wire property stays forbidden in :properties" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    base = [
      client_id: "alias-options",
      host: "127.0.0.1",
      port: port,
      observer: self(),
      keep_alive: 0
    ]

    assert {:error, :invalid_options} =
             GravitonMQ.Client.MQTT.Config.build(base ++ [properties: [topic_alias_maximum: 1]])

    assert {:error, :invalid_options} =
             GravitonMQ.Client.MQTT.Config.build(base ++ [topic_alias_maximum: 65_536])

    _client =
      start_supervised!(
        {MQTTClient, base ++ [topic_alias_maximum: 4, max_outbound_topic_aliases: 2]}
      )

    {:ok, socket} = :gen_tcp.accept(listener, 5_000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    assert %GravitonMQ.MQTT.Packet.Connect{properties: properties} = receive_packet(socket)
    assert properties[:topic_alias_maximum] == 4
  end

  test "memory clients retain shared subscription admission when the peer permits it" do
    {client, socket} = connected([])

    assert {:ok, subscription} = MQTTClient.subscribe(client, "$share/workers/events/#", qos: 1)

    assert %Subscribe{
             packet_id: id,
             subscriptions: [{"$share/workers/events/#", %{qos: 1}}]
           } = receive_packet(socket)

    send_packet(socket, %Suback{packet_id: id, reason_codes: [1]})
    assert_receive {:graviton_mq, ^client, {:operation, ^subscription, {:ok, [1]}}}, 1_000
    assert MQTTClient.status(client).phase == :connected
    assert MQTTClient.status(client).pending == 0
  end

  defp connected(options) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    client =
      start_supervised!(
        {MQTTClient,
         Keyword.merge(
           [
             client_id: "shared-profile",
             host: "127.0.0.1",
             port: port,
             observer: self(),
             keep_alive: 0,
             handshake_timeout: 30_000,
             operation_timeout: 30_000
           ],
           options
         )}
      )

    {:ok, socket} = :gen_tcp.accept(listener, 5_000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    assert %GravitonMQ.MQTT.Packet.Connect{} = receive_packet(socket)
    send_packet(socket, %Connack{version: :v5, session_present: false, reason_code: 0})

    assert_receive {:graviton_mq, ^client, {:connected, %{shared_subscription_available: 1}}},
                   15_000

    {client, socket}
  end

  defp receive_packet(socket) do
    assert {:ok, bytes} = :gen_tcp.recv(socket, 0, 15_000)
    assert {:ok, packet, <<>>} = Packet.decode(bytes, :v5)
    packet
  end

  defp send_packet(socket, packet) do
    assert {:ok, bytes} = Packet.encode(packet)
    assert :ok = :gen_tcp.send(socket, bytes)
  end
end
