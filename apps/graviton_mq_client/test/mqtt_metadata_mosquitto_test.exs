Code.require_file("support/mosquitto.exs", __DIR__)

defmodule GravitonMQ.MQTTClient.MetadataMosquittoTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.Client.Test.Mosquitto, as: Peer
  alias GravitonMQ.MQTT.Packet.Publish
  alias GravitonMQ.MQTTClient, as: Client

  @moduletag :interop
  @moduletag :paho_interop
  @moduletag timeout: 20_000

  setup do
    directory = Peer.directory!()
    on_exit(fn -> File.rm_rf!(directory) end)
    broker = Peer.broker!(Peer.executables!(), directory, max_qos: 2)

    client =
      start_supervised!(
        {Client,
         host: "127.0.0.1",
         port: broker.port,
         observer: self(),
         client_id: "metadata-#{System.unique_integer([:positive])}"}
      )

    assert_receive {:graviton_mq, ^client, {:connected, %{maximum_qos: 2}}}, 5_000
    %{client: client, port: broker.port}
  end

  test "client publishes exact QoS1/2 metadata through Mosquitto to independent Paho", context do
    receiver = peer!("receive", context.port, "metadata/client-out")
    Peer.eventually!(fn -> Peer.snapshot(receiver).output =~ "READY" end)

    for qos <- [1, 2] do
      assert {:ok, operation} =
               Client.publish(context.client, "metadata/client-out/q#{qos}", "metadata-#{qos}-雪",
                 qos: qos,
                 properties: properties()
               )

      expected = if qos == 1, do: :puback, else: :pubcomp
      assert_receive {:graviton_mq, _, {:operation, ^operation, {:ok, ^expected}}}, 5_000
    end

    assert completed!(receiver) =~ "00ff80002a7e"
    assert :ok = Client.disconnect(context.client)
  end

  test "client receives exact QoS1/2 metadata from independent Paho through Mosquitto", context do
    client = context.client
    assert {:ok, subscription} = Client.subscribe(client, "metadata/client-in/#", qos: 2)
    assert_receive {:graviton_mq, ^client, {:operation, ^subscription, {:ok, [2]}}}, 5_000
    assert :ok = Client.request(client, 2)
    publisher = peer!("publish", context.port, "metadata/client-in")

    for qos <- [1, 2] do
      assert_receive {:graviton_mq, ^client, {:delivery, delivery, %Publish{qos: ^qos} = packet}},
                     5_000

      assert packet.topic == "metadata/client-in/q#{qos}"
      assert packet.payload == "metadata-#{qos}-雪"
      assert packet.retain == false
      assert_metadata(packet.properties)
      assert :ok = Client.ack(client, delivery)
    end

    completed!(publisher)
    Peer.eventually!(fn -> Client.status(client).deliveries == 0 end)
    assert :ok = Client.disconnect(client)
  end

  defp properties do
    [
      payload_format_indicator: 1,
      user_property: {"repeat", "first"},
      content_type: "application/x-graviton; charset=utf-8",
      user_property: {"other", "雪"},
      response_topic: "metadata/replies/雪",
      correlation_data: <<0, 255, 128, 0, 42, 126>>,
      user_property: {"repeat", "second"},
      message_expiry_interval: 60,
      user_property: {"repeat", "first"}
    ]
  end

  defp assert_metadata(actual) do
    for {key, value} <- properties(), key not in [:user_property, :message_expiry_interval] do
      assert Keyword.get_values(actual, key) == [value]
    end

    assert Keyword.get_values(actual, :user_property) == [
             {"repeat", "first"},
             {"other", "雪"},
             {"repeat", "second"},
             {"repeat", "first"}
           ]

    assert [expiry] = Keyword.get_values(actual, :message_expiry_interval)
    assert expiry in 1..60
  end

  defp peer!(mode, port, prefix) do
    Peer.process!(System.fetch_env!("GRAVITON_PAHO_PYTHON"), [
      Path.expand("support/mqtt_metadata_peer.py", __DIR__),
      mode,
      Integer.to_string(port),
      prefix
    ])
  end

  defp completed!(peer) do
    Peer.eventually!(fn -> Peer.snapshot(peer).exit_status != nil end, fn ->
      Peer.snapshot(peer)
    end)

    assert %{exit_status: 0, output: output} = Peer.snapshot(peer)
    assert output =~ "PASS "
    output
  end
end
