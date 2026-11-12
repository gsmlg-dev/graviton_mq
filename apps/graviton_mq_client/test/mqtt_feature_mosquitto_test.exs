Code.require_file("support/mosquitto.exs", __DIR__)
Code.require_file("support/mqtt_wire_tap.exs", __DIR__)

defmodule GravitonMQ.MQTTClient.FeatureMosquittoTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.Client.Test.{Mosquitto, MQTTWireTap}
  alias GravitonMQ.MQTT.Packet.Publish
  alias GravitonMQ.MQTTClient, as: Client

  @moduletag :interop
  @moduletag :tmp_dir
  @moduletag timeout: 60_000

  setup_all do
    %{executables: Mosquitto.executables!()}
  end

  setup do
    directory = Mosquitto.directory!()
    on_exit(fn -> File.rm_rf!(directory) end)
    %{directory: directory}
  end

  defp client!(port, overrides) do
    options =
      Keyword.merge(
        [
          host: "127.0.0.1",
          port: port,
          client_id: "graviton-s4-#{System.unique_integer([:positive])}",
          observer: self(),
          keep_alive: 5,
          session_expiry_interval: 60,
          reconnect: [max_attempts: 3, delay: 50, max_delay: 200],
          operation_timeout: 2_000
        ],
        overrides
      )

    client = start_supervised!({Client, options}, id: Keyword.get(overrides, :id, make_ref()))
    assert_receive {:graviton_mq, ^client, {:connected, _peer}}, 5_000
    client
  end

  defp subscribed!(client, filter, options) do
    {:ok, operation} = Client.subscribe(client, filter, options)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, [_]}}}, 5_000
  end

  defp deliveries(client, count) do
    for _ <- 1..count do
      assert_receive {:graviton_mq, ^client, {:delivery, delivery, %Publish{} = packet}}, 5_000
      :ok = Client.ack(client, delivery)
      packet
    end
  end

  test "outbound aliases to Mosquitto respect its Topic Alias Maximum", context do
    for {configuration, bound} <- [{"", 3}, {"max_topic_alias 1", 1}] do
      broker =
        Mosquitto.broker!(context.executables, context.directory, configuration: configuration)

      tap =
        start_supervised!({MQTTWireTap, upstream_port: broker.port, observer: self()},
          id: make_ref()
        )

      subscriber =
        Mosquitto.process!(
          context.executables.subscriber,
          Mosquitto.cli_args(broker) ++
            ["-t", "alias/#", "-q", "1", "-C", "8", "-W", "10", "-F", "%t|%p"]
        )

      Mosquitto.eventually!(fn -> Mosquitto.snapshot(broker.owner).output =~ "Sending SUBACK" end)
      client = client!(MQTTWireTap.port(tap), max_outbound_topic_aliases: 3)

      for topic <- [
            "alias/a",
            "alias/a",
            "alias/b",
            "alias/b",
            "alias/c",
            "alias/c",
            "alias/d",
            "alias/d"
          ] do
        {:ok, operation} = Client.publish(client, topic, topic, qos: 1)
        assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, :puback}}}, 5_000
      end

      Mosquitto.eventually!(
        fn -> Mosquitto.snapshot(subscriber).exit_status != nil end,
        fn -> Mosquitto.snapshot(subscriber) end,
        10_000
      )

      assert %{exit_status: 0, output: output} = Mosquitto.snapshot(subscriber)
      assert length(Regex.scan(~r/^alias\/[a-d]\|alias\/[a-d]$/m, output)) == 8

      sent = for %{direction: :to_server} = fact <- MQTTWireTap.observed(tap), do: fact
      assert Enum.all?(sent, &(&1.topic_alias == nil or &1.topic_alias in 1..bound))
      assert Enum.count(sent, &(&1.topic == "")) == bound
    end
  end

  test "Mosquitto aliases to our client resolve to full topics and reset on reconnect", context do
    broker =
      Mosquitto.broker!(context.executables, context.directory,
        configuration: "max_topic_alias_broker 10"
      )

    tap = start_supervised!({MQTTWireTap, upstream_port: broker.port, observer: self()})
    client = client!(MQTTWireTap.port(tap), topic_alias_maximum: 5)
    :ok = Client.request(client, 8)
    subscribed!(client, "in/#", qos: 1)
    for _ <- 1..3, do: Mosquitto.publish!(context.executables, broker, "in/a", "x", 1)
    received = deliveries(client, 3)

    assert Enum.all?(
             received,
             &(&1.topic == "in/a" and not Keyword.has_key?(&1.properties, :topic_alias))
           )

    first =
      for %{direction: :to_client, connection: 1} = fact <- MQTTWireTap.observed(tap),
          do: fact.topic

    assert first == ["in/a", "", ""]

    :ok = MQTTWireTap.drop(tap)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    Mosquitto.publish!(context.executables, broker, "in/a", "y", 1)
    assert [%Publish{topic: "in/a"}] = deliveries(client, 1)

    second =
      for %{direction: :to_client, connection: 2} = fact <- MQTTWireTap.observed(tap),
          do: fact.topic

    assert hd(second) == "in/a"
  end

  test "Subscription Identifiers from Mosquitto reach the host", context do
    broker = Mosquitto.broker!(context.executables, context.directory)
    client = client!(broker.port, [])
    :ok = Client.request(client, 4)
    subscribed!(client, "sid/#", qos: 1, subscription_identifier: 7)
    subscribed!(client, "sid/+/x", qos: 1, subscription_identifier: 9)
    Mosquitto.publish!(context.executables, broker, "sid/a/x", "x", 1)
    Process.sleep(500)

    received =
      deliveries(client, 1) ++
        for(
          {:graviton_mq, ^client, {:delivery, d, %Publish{} = p}} <- flush(),
          do: (:ok = Client.ack(client, d)) && p
        )

    ids =
      received
      |> Enum.flat_map(&for({:subscription_identifier, id} <- &1.properties, do: id))
      |> Enum.sort()
      |> Enum.uniq()

    assert ids == [7, 9]
    assert Enum.all?(received, &(&1.topic == "sid/a/x"))
  end

  test "memory and persistent clients share a Mosquitto group; the persistent member resumes it",
       context do
    broker = Mosquitto.broker!(context.executables, context.directory)
    memory = client!(broker.port, client_id: "s4-share-memory")
    directory = Path.join(context.tmp_dir, "store")

    persistent =
      client!(broker.port,
        client_id: "s4-share-durable",
        id: :durable,
        persistence: [directory: directory]
      )

    for client <- [memory, persistent] do
      :ok = Client.request(client, 16)
      subscribed!(client, "$share/g/s/#", qos: 1)
    end

    for index <- 1..6,
        do: Mosquitto.publish!(context.executables, broker, "s/#{index}", "p#{index}", 1)

    Process.sleep(1_000)

    payloads =
      for {:graviton_mq, client, {:delivery, delivery, %Publish{payload: payload}}} <- flush() do
        :ok = Client.ack(client, delivery)
        {client, payload}
      end

    assert Enum.sort(Enum.map(payloads, &elem(&1, 1))) == Enum.map(1..6, &"p#{&1}")

    assert Enum.any?(payloads, &(elem(&1, 0) == memory)) and
             Enum.any?(payloads, &(elem(&1, 0) == persistent))

    :ok = stop_supervised(:durable)

    restarted =
      client!(broker.port,
        client_id: "s4-share-durable",
        id: :durable_again,
        persistence: [directory: directory]
      )

    refute_receive {:graviton_mq, ^restarted, {:subscriptions_lost, _}}, 500
    :ok = Client.request(restarted, 8)

    for index <- 7..10,
        do: Mosquitto.publish!(context.executables, broker, "s/#{index}", "p#{index}", 1)

    Process.sleep(1_000)
    assert Enum.any?(flush(), &match?({:graviton_mq, ^restarted, {:delivery, _, %Publish{}}}, &1))
  end

  defp flush do
    receive do
      message -> [message | flush()]
    after
      0 -> []
    end
  end
end
