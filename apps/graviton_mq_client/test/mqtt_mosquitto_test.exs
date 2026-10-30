Code.require_file("support/mosquitto.exs", __DIR__)

defmodule GravitonMQ.MQTTClient.MosquittoTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.Client.Test.Mosquitto, as: Peer
  alias GravitonMQ.MQTT.Packet.Publish
  alias GravitonMQ.MQTTClient, as: Client

  @moduletag :interop
  @moduletag timeout: 30_000

  setup_all do
    %{executables: Peer.executables!()}
  end

  setup do
    directory = Peer.directory!()
    on_exit(fn -> File.rm_rf!(directory) end)
    %{directory: directory}
  end

  test "MQTT5 negotiation and QoS0/1 publishing reach independent subscribers", context do
    broker = Peer.broker!(context.executables, context.directory)
    client = client!(broker)
    assert_receive {:graviton_mq, ^client, {:connected, peer}}, 5_000
    assert peer.maximum_qos == 1

    for qos <- [0, 1] do
      topic = "graviton/publish/#{qos}"
      payload = "independent-subscriber-#{qos}"
      subscriber = Peer.subscriber!(context.executables, broker, topic)
      assert {:ok, operation} = Client.publish(client, topic, payload, qos: qos)
      result = if qos == 0, do: :sent, else: :puback
      assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^result}}}, 5_000

      Peer.eventually!(
        fn -> Peer.snapshot(subscriber).exit_status != nil end,
        fn -> Peer.snapshot(subscriber) end
      )

      assert %{exit_status: 0, output: output} = Peer.snapshot(subscriber)
      assert output =~ "#{topic}|#{qos}|#{payload}"
    end
  end

  test "a valid UTF-8 Will reaches an independent subscriber after client owner loss", context do
    broker = Peer.broker!(context.executables, context.directory)
    topic = "graviton/will-utf8"
    payload = "will-雪"
    subscriber = Peer.subscriber!(context.executables, broker, topic)

    options =
      client_options(broker,
        will: [
          topic: topic,
          payload: payload,
          qos: 1,
          properties: [payload_format_indicator: 1]
        ]
      )

    client =
      start_supervised!(%{
        id: :will_client,
        start: {Client, :start_link, [options]},
        restart: :temporary
      })

    assert_receive {:graviton_mq, ^client, {:connected, _peer}}, 5_000
    monitor = Process.monitor(client)
    Process.exit(client, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^client, :killed}, 5_000

    Peer.eventually!(
      fn -> Peer.snapshot(subscriber).exit_status != nil end,
      fn -> Peer.snapshot(subscriber) end
    )

    assert %{exit_status: 0, output: output} = Peer.snapshot(subscriber)
    assert output =~ "#{topic}|1|#{payload}"
  end

  test "independent publishers deliver QoS0/1 under host demand and unsubscribe takes effect",
       context do
    broker = Peer.broker!(context.executables, context.directory)
    client = connected_client!(broker)
    assert {:ok, subscribe} = Client.subscribe(client, "graviton/receive/#", qos: 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^subscribe, {:ok, [1]}}}, 5_000

    for qos <- [0, 1] do
      topic = "graviton/receive/#{qos}"
      payload = "independent-publisher-#{qos}"
      Peer.publish!(context.executables, broker, topic, payload, qos)

      Peer.eventually!(
        fn -> Client.status(client).deliveries == 1 end,
        fn -> Client.status(client) end
      )

      refute_receive {:graviton_mq, ^client, {:delivery, _, _}}, 50
      assert :ok = Client.request(client, 1)

      assert_receive {:graviton_mq, ^client,
                      {:delivery, {^client, _delivery_id} = delivery,
                       %Publish{topic: ^topic, payload: ^payload, qos: ^qos}}},
                     5_000

      assert :ok = Client.ack(client, delivery)
      Peer.eventually!(fn -> Client.status(client).deliveries == 0 end)
    end

    assert {:ok, unsubscribe} = Client.unsubscribe(client, "graviton/receive/#")
    assert_receive {:graviton_mq, ^client, {:operation, ^unsubscribe, {:ok, [0]}}}, 5_000
    Peer.publish!(context.executables, broker, "graviton/receive/removed", "not-delivered", 1)
    assert :ok = Client.request(client, 1)
    refute_receive {:graviton_mq, ^client, {:delivery, _, _}}, 200
    assert Client.status(client).phase == :connected
  end

  test "keep alive completes real PINGREQ/PINGRESP and intentional disconnect stops retry",
       context do
    broker = Peer.broker!(context.executables, context.directory)
    client = connected_client!(broker, keep_alive: 1)
    assert_receive {:graviton_mq, ^client, :ping_response}, 5_000
    generation = Client.status(client).generation
    assert :ok = Client.disconnect(client)
    Peer.eventually!(fn -> Client.status(client).phase == :stopped end)

    Peer.eventually!(
      fn -> Peer.snapshot(broker.owner).output =~ "Received DISCONNECT from" end,
      fn -> Peer.snapshot(broker.owner) end
    )

    refute_receive {:graviton_mq, ^client, {:connected, _}}, 250
    assert Client.status(client).generation == generation
  end

  test "negative CONNACK is reported without retry and valid credentials connect", context do
    password_file = Path.join(context.directory, "passwords")
    password = "test-#{System.unique_integer([:positive])}"

    Peer.command!(context.executables.password, ["-b", "-c", password_file, "interop", password])

    broker =
      Peer.broker!(context.executables, context.directory,
        anonymous: false,
        configuration: "password_file #{password_file}"
      )

    rejected = client!(broker)
    assert_receive {:graviton_mq, ^rejected, {:disconnected, {:connack, 0x87}}}, 5_000
    assert Client.status(rejected).phase == :stopped
    generation = Client.status(rejected).generation
    refute_receive {:graviton_mq, ^rejected, {:connected, _}}, 250
    assert Client.status(rejected).generation == generation

    client = connected_client!(broker, username: "interop", password: password)
    assert Client.status(client).phase == :connected
  end

  test "negotiated packet size rejects oversize locally while the connection remains usable",
       context do
    broker =
      Peer.broker!(context.executables, context.directory, configuration: "max_packet_size 128")

    client = client!(broker)
    assert_receive {:graviton_mq, ^client, {:connected, peer}}, 5_000
    assert peer.maximum_packet_size == 128

    assert {:error, :peer_packet_size} =
             Client.publish(client, "graviton/size", :binary.copy("x", 128), qos: 1)

    assert Client.status(client).pending == 0
    assert Client.status(client).phase == :connected
    subscriber = Peer.subscriber!(context.executables, broker, "graviton/size")
    assert {:ok, operation} = Client.publish(client, "graviton/size", "within-limit", qos: 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, :puback}}}, 5_000
    Peer.eventually!(fn -> Peer.snapshot(subscriber).exit_status == 0 end)
    assert Peer.snapshot(subscriber).output =~ "graviton/size|1|within-limit"
  end

  test "a broker ACL produces an explicit negative PUBACK instead of publication success",
       context do
    acl = Path.join(context.directory, "acl")
    File.write!(acl, "topic read graviton/allowed/#\n")

    broker =
      Peer.broker!(context.executables, context.directory, configuration: "acl_file #{acl}")

    client = connected_client!(broker)
    assert {:ok, operation} = Client.publish(client, "graviton/denied", "rejected", qos: 1)

    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:error, {:puback, 0x87}}}},
                   5_000

    assert Client.status(client).pending == 0
    assert Client.status(client).phase == :connected
  end

  test "controlled broker restart reconnects and observes a newly granted subscription",
       context do
    broker = Peer.broker!(context.executables, context.directory)
    client = connected_client!(broker)
    generation = Client.status(client).generation
    assert {:ok, original_subscribe} = Client.subscribe(client, "graviton/reconnect", qos: 1)

    assert_receive {:graviton_mq, ^client, {:operation, ^original_subscribe, {:ok, [1]}}}, 5_000

    Peer.restart!(broker)
    assert_receive {:graviton_mq, ^client, {:connected, _peer}}, 5_000
    assert Client.status(client).generation > generation
    assert {:ok, subscribe} = Client.subscribe(client, "graviton/reconnect", qos: 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^subscribe, {:ok, [1]}}}, 5_000
    assert :ok = Client.request(client, 1)
    Peer.publish!(context.executables, broker, "graviton/reconnect", "after-restart", 1)

    assert_receive {:graviton_mq, ^client,
                    {:delivery, delivery,
                     %Publish{topic: "graviton/reconnect", payload: "after-restart", qos: 1}}},
                   5_000

    assert :ok = Client.ack(client, delivery)
  end

  test "TLS exchanges with independent peers validate trust and hostname", context do
    certificates = Peer.certificates!(context.executables, context.directory)

    broker =
      Peer.broker!(context.executables, context.directory,
        configuration: """
        certfile #{certificates.certificate}
        keyfile #{certificates.private_key}
        """
      )

    tls_options = [
      cacertfile: certificates.ca_certificate,
      server_name_indication: ~c"localhost"
    ]

    client = connected_client!(broker, transport: :tls, tls_options: tls_options)
    cli_options = [host: "localhost", extra: ["--cafile", certificates.ca_certificate]]
    subscriber = Peer.subscriber!(context.executables, broker, "graviton/tls/out", cli_options)
    assert {:ok, publish} = Client.publish(client, "graviton/tls/out", "tls-out", qos: 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^publish, {:ok, :puback}}}, 5_000

    Peer.eventually!(
      fn -> Peer.snapshot(subscriber).exit_status == 0 end,
      fn -> Peer.snapshot(subscriber) end
    )

    assert Peer.snapshot(subscriber).output =~ "graviton/tls/out|1|tls-out"
    assert {:ok, subscribe} = Client.subscribe(client, "graviton/tls/in", qos: 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^subscribe, {:ok, [1]}}}, 5_000
    assert :ok = Client.request(client, 1)
    Peer.publish!(context.executables, broker, "graviton/tls/in", "tls-in", 1, cli_options)

    assert_receive {:graviton_mq, ^client,
                    {:delivery, delivery,
                     %Publish{topic: "graviton/tls/in", payload: "tls-in", qos: 1}}},
                   5_000

    assert :ok = Client.ack(client, delivery)

    for {options, expected_reason} <- [
          {[cacertfile: certificates.untrusted, server_name_indication: ~c"localhost"],
           ~r/unknown_ca|selfsigned_peer/},
          {[cacertfile: certificates.ca_certificate, server_name_indication: ~c"wrong.invalid"],
           ~r/hostname_check_failed/}
        ] do
      rejected =
        client!(broker, transport: :tls, tls_options: options, reconnect: [max_attempts: 0])

      assert_receive {:graviton_mq, ^rejected, {:disconnected, reason}}, 5_000
      assert inspect(reason) =~ expected_reason
      assert Client.status(rejected).phase == :stopped
      refute_receive {:graviton_mq, ^rejected, {:connected, _}}, 50
    end

    rejected =
      client!(broker,
        host: {127, 0, 0, 1},
        transport: :tls,
        tls_options: [cacertfile: certificates.ca_certificate],
        reconnect: [max_attempts: 0]
      )

    assert_receive {:graviton_mq, ^rejected, {:disconnected, ip_reason}}, 5_000
    assert inspect(ip_reason) =~ "hostname_check_failed"
    assert Client.status(rejected).phase == :stopped
    refute_receive {:graviton_mq, ^rejected, {:connected, _}}, 50
  end

  test "a host supervisor shuts down its independent client and network connection", context do
    broker = Peer.broker!(context.executables, context.directory)
    client_id = client_id()
    options = client_options(broker, client_id: client_id)

    host =
      start_supervised!(%{
        id: :client_only_host,
        start: {Supervisor, :start_link, [[{Client, options}], [strategy: :one_for_one]]},
        type: :supervisor
      })

    [{_id, client, _type, _modules}] = Supervisor.which_children(host)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    monitor = Process.monitor(client)
    assert :ok = stop_supervised(:client_only_host)
    assert_receive {:DOWN, ^monitor, :process, ^client, :shutdown}, 5_000

    Peer.eventually!(
      fn ->
        broker.owner
        |> Peer.snapshot()
        |> Map.fetch!(:output)
        |> String.split("\n")
        |> Enum.any?(fn line ->
          line =~ "Client #{client_id}" and
            (line =~ "closed its connection" or line =~ "disconnected")
        end)
      end,
      fn -> Peer.snapshot(broker.owner) end
    )

    refute Process.alive?(client)
  end

  test "a dead delivery observer stops its supervised client without a host restart loop",
       context do
    broker = Peer.broker!(context.executables, context.directory)
    parent = self()
    observer = spawn(fn -> forward_observer(parent) end)
    on_exit(fn -> if Process.alive?(observer), do: Process.exit(observer, :kill) end)

    host =
      start_supervised!(%{
        id: :observer_host,
        start:
          {Supervisor, :start_link,
           [[{Client, client_options(broker, observer: observer)}], [strategy: :one_for_one]]},
        type: :supervisor
      })

    [{_id, client, _type, _modules}] = Supervisor.which_children(host)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    client_monitor = Process.monitor(client)
    host_monitor = Process.monitor(host)
    send(observer, :stop)
    assert_receive {:DOWN, ^client_monitor, :process, ^client, :normal}, 5_000
    refute_receive {:DOWN, ^host_monitor, :process, ^host, _}, 250
    assert Process.alive?(host)

    refute Enum.any?(Supervisor.which_children(host), fn {_id, pid, _type, _modules} ->
             is_pid(pid)
           end)
  end

  defp connected_client!(broker, overrides \\ []) do
    client = client!(broker, overrides)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    client
  end

  defp client!(broker, overrides \\ []) do
    start_supervised!({Client, client_options(broker, overrides)}, id: make_ref())
  end

  defp client_options(broker, overrides) do
    Keyword.merge(
      [
        host: "127.0.0.1",
        port: broker.port,
        client_id: client_id(),
        observer: self(),
        keep_alive: 1,
        session_expiry_interval: 60,
        reconnect: [max_attempts: 3, delay: 50, max_delay: 200],
        max_inflight: 16,
        max_deliveries: 32,
        operation_timeout: 1_000
      ],
      overrides
    )
  end

  defp client_id, do: "graviton-interop-#{System.unique_integer([:positive])}"

  defp forward_observer(parent) do
    receive do
      :stop ->
        :ok

      message ->
        send(parent, message)
        forward_observer(parent)
    end
  end
end
