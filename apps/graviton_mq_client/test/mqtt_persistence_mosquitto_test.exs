Code.require_file("support/mosquitto.exs", __DIR__)
Code.require_file("support/puback_loss_proxy.exs", __DIR__)

defmodule GravitonMQ.MQTTClient.PersistenceMosquittoTest do
  use ExUnit.Case, async: false
  alias GravitonMQ.Client.Test.Mosquitto, as: Peer
  alias GravitonMQ.Client.Test.PubackLossProxy, as: Proxy
  alias GravitonMQ.MQTTClient, as: Client
  @moduletag :interop
  @moduletag :tmp_dir
  @moduletag timeout: 40_000

  setup_all do
    %{executables: Peer.executables!()}
  end

  for {type, qos} <- [{4, 1}, {5, 2}, {7, 2}] do
    @terminal_loss_type type
    @terminal_qos qos
    test "zero-retry terminal restart completes QoS#{qos} protocol after lost type #{type} without reversing its host result",
         context do
      broker = Peer.broker!(context.executables, context.tmp_dir, max_qos: 2)

      proxy =
        start_supervised!(
          {Proxy,
           upstream_port: broker.port, observer: self(), drop_packet_type: @terminal_loss_type}
        )

      options = Keyword.put(options(context, Proxy.port(proxy)), :reconnect, max_attempts: 0)
      first = client!(options)
      assert_receive {:graviton_mq, ^first, {:connected, _}}, 5_000

      assert {:ok, ref} =
               Client.publish(first, "persistent/terminal", "owned", qos: @terminal_qos)

      assert_receive {:graviton_mq, ^first, {:operation_identity, ^ref, stable_id}}, 5_000

      assert_receive {:puback_loss_proxy, ^proxy,
                      {:ack_dropped, :broker, @terminal_loss_type, 1, packet_id}},
                     5_000

      assert_receive {:graviton_mq, ^first, {:operation, ^ref, {:error, :unknown_outcome}}}, 5_000
      assert_receive {:graviton_mq, ^first, {:disconnected, _}}, 5_000
      assert Client.status(first).phase == :stopped
      kill_owner!(first)

      second = client!(options)

      assert_receive {:graviton_mq, ^second,
                      {:recovered_operation_result, ^stable_id, {:error, :unknown_outcome}}},
                     5_000

      assert_receive {:puback_loss_proxy, ^proxy, {:connect, 2, %{clean_start: false}}}, 5_000
      assert_receive {:puback_loss_proxy, ^proxy, {:connack, 2, %{session_present: true}}}, 5_000
      assert_receive {:graviton_mq, ^second, {:connected, _}}, 5_000

      if @terminal_loss_type == 7 do
        assert_receive {:puback_loss_proxy, ^proxy, {:ack_forwarded, :client, 6, 2, ^packet_id}},
                       5_000

        refute_receive {:puback_loss_proxy, ^proxy, {:publish, 2, _}}, 50
      else
        assert_receive {:puback_loss_proxy, ^proxy,
                        {:publish, 2, %{packet_id: ^packet_id, dup: true}}},
                       5_000
      end

      completion = if @terminal_qos == 1, do: :puback, else: :pubcomp

      assert_receive {:graviton_mq, ^second,
                      {:recovered_protocol_result, ^stable_id, {:ok, ^completion}}},
                     5_000

      refute_receive {:graviton_mq, ^second, {:recovered_operation_result, ^stable_id, {:ok, _}}},
                     50

      refute_receive {:graviton_mq, ^second, {:operation, _, _}}, 50
      assert :ok = Client.disconnect(second)
    end
  end

  for action <- [:disconnect, :shutdown] do
    @terminal_action action
    test "#{action} retains a live pending PUBREL for durable restart", context do
      broker = Peer.broker!(context.executables, context.tmp_dir, max_qos: 2)

      proxy =
        start_supervised!(
          {Proxy,
           upstream_port: broker.port, observer: self(), drop_packet_type: 7, close_on_drop: false}
        )

      options = Keyword.put(options(context, Proxy.port(proxy)), :reconnect, max_attempts: 0)
      first = client!(options)
      assert_receive {:graviton_mq, ^first, {:connected, _}}, 5_000
      assert {:ok, ref} = Client.publish(first, "persistent/shutdown", "owned", qos: 2)
      assert_receive {:graviton_mq, ^first, {:operation_identity, ^ref, stable_id}}, 5_000
      assert_receive {:puback_loss_proxy, ^proxy, {:ack_dropped, :broker, 7, 1, packet_id}}, 5_000
      assert Client.status(first).phase == :connected
      if @terminal_action == :disconnect, do: assert(:ok = Client.disconnect(first))
      :ok = GenServer.stop(first, :normal)
      assert_receive {:graviton_mq, ^first, {:operation, ^ref, {:error, :unknown_outcome}}}, 5_000
      Peer.eventually!(fn -> not Proxy.snapshot(proxy).open end)

      second = client!(options)
      assert_receive {:puback_loss_proxy, ^proxy, {:connect, 2, %{clean_start: false}}}, 5_000
      assert_receive {:puback_loss_proxy, ^proxy, {:connack, 2, %{session_present: true}}}, 5_000

      assert_receive {:puback_loss_proxy, ^proxy, {:ack_forwarded, :client, 6, 2, ^packet_id}},
                     5_000

      assert_receive {:graviton_mq, ^second,
                      {:recovered_protocol_result, ^stable_id, {:ok, :pubcomp}}},
                     5_000

      refute_receive {:puback_loss_proxy, ^proxy, {:publish, 2, _}}, 50
      refute_receive {:graviton_mq, ^second, {:operation, _, _}}, 50

      refute_receive {:graviton_mq, ^second, {:recovered_operation_result, ^stable_id, {:ok, _}}},
                     50

      assert :ok = Client.disconnect(second)
    end
  end

  test "an online snapshot older than Session Expiry still resumes a just-crashed connection",
       context do
    broker = Peer.broker!(context.executables, context.tmp_dir, max_qos: 2)

    proxy =
      start_supervised!(
        {Proxy,
         upstream_port: broker.port, observer: self(), drop_packet_type: 7, close_on_drop: false}
      )

    options =
      options(context, Proxy.port(proxy))
      |> Keyword.put(:session_expiry_interval, 1)
      |> Keyword.put(:reconnect, max_attempts: 0)

    first = client!(options)
    assert_receive {:graviton_mq, ^first, {:connected, _}}, 5_000
    assert {:ok, ref} = Client.publish(first, "persistent/online", "owned", qos: 2)
    assert_receive {:graviton_mq, ^first, {:operation_identity, ^ref, stable_id}}, 5_000
    assert_receive {:puback_loss_proxy, ^proxy, {:ack_dropped, :broker, 7, 1, packet_id}}, 5_000
    Process.sleep(1_100)
    assert Client.status(first).phase == :connected
    kill_owner!(first)
    Peer.eventually!(fn -> not Proxy.snapshot(proxy).open end)
    second = client!(options)
    assert_receive {:puback_loss_proxy, ^proxy, {:connect, 2, %{clean_start: false}}}, 5_000
    assert_receive {:puback_loss_proxy, ^proxy, {:connack, 2, %{session_present: true}}}, 5_000

    assert_receive {:puback_loss_proxy, ^proxy, {:ack_forwarded, :client, 6, 2, ^packet_id}},
                   5_000

    assert_receive {:graviton_mq, ^second,
                    {:recovered_operation_result, ^stable_id, {:ok, :pubcomp}}},
                   5_000

    refute_receive {:puback_loss_proxy, ^proxy, {:publish, 2, _}}, 50
    assert :ok = Client.disconnect(second)
  end

  for type <- [5, 7] do
    @loss_type type
    test "owner SIGKILL recovers outgoing QoS2 after lost type #{type} with stable identity",
         context do
      broker = Peer.broker!(context.executables, context.tmp_dir, max_qos: 2)
      subscriber = Peer.subscriber!(context.executables, broker, "persistent/out", qos: 2)

      proxy =
        start_supervised!(
          {Proxy, upstream_port: broker.port, observer: self(), drop_packet_type: @loss_type}
        )

      options = options(context, Proxy.port(proxy))
      first = client!(options)
      assert_receive {:graviton_mq, ^first, {:connected, _}}, 5_000
      assert {:ok, ref} = Client.publish(first, "persistent/out", "once", qos: 2)
      assert_receive {:graviton_mq, ^first, {:operation_identity, ^ref, stable_id}}, 5_000

      assert_receive {:puback_loss_proxy, ^proxy,
                      {:publish, 1, %{packet_id: packet_id, dup: false}}},
                     5_000

      assert_receive {:puback_loss_proxy, ^proxy,
                      {:ack_dropped, :broker, @loss_type, 1, ^packet_id}},
                     5_000

      assert_receive {:graviton_mq, ^first, {:disconnected, _}}, 5_000
      kill_owner!(first)
      second = client!(options)
      assert_receive {:graviton_mq, ^second, {:recovered_operation, ^stable_id, :publish}}, 5_000
      assert_receive {:puback_loss_proxy, ^proxy, {:connect, 2, %{clean_start: false}}}, 5_000
      assert_receive {:puback_loss_proxy, ^proxy, {:connack, 2, %{session_present: true}}}, 5_000
      assert_receive {:graviton_mq, ^second, {:connected, _}}, 5_000

      if @loss_type == 5 do
        assert_receive {:puback_loss_proxy, ^proxy,
                        {:publish, 2, %{packet_id: ^packet_id, dup: true}}},
                       5_000
      else
        refute_receive {:puback_loss_proxy, ^proxy, {:publish, 2, _}}, 50
      end

      assert_receive {:puback_loss_proxy, ^proxy, {:ack_forwarded, :client, 6, 2, ^packet_id}},
                     5_000

      assert_receive {:graviton_mq, ^second,
                      {:recovered_operation_result, ^stable_id, {:ok, :pubcomp}}},
                     5_000

      refute_receive {:graviton_mq, ^second, {:operation, _, _}}, 30
      Peer.eventually!(fn -> Peer.snapshot(subscriber).exit_status != nil end)
      assert %{exit_status: 0, output: output} = Peer.snapshot(subscriber)
      assert output =~ "persistent/out|2|once"
      assert :ok = Client.disconnect(second)
    end
  end

  test "owner kill keeps incoming host ownership without ordinary redelivery and retains subscriptions",
       context do
    broker = Peer.broker!(context.executables, context.tmp_dir, max_qos: 2)
    options = options(context, broker.port)
    first = client!(options)
    assert_receive {:graviton_mq, ^first, {:connected, _}}, 5_000
    assert {:ok, ref} = Client.subscribe(first, "persistent/in", qos: 2)
    assert_receive {:graviton_mq, ^first, {:operation, ^ref, {:ok, [2]}}}, 5_000
    assert :ok = Client.request(first, 1)
    Peer.publish!(context.executables, broker, "persistent/in", "owned", 2)
    assert_receive {:graviton_mq, ^first, {:delivery, _old_ref, packet}}, 5_000
    kill_owner!(first)
    second = client!(options)

    assert_receive {:graviton_mq, ^second, {:recovered_delivery, new_ref, ^packet, :await_host}},
                   5_000

    assert_receive {:graviton_mq, ^second, {:connected, _}}, 5_000
    assert :ok = Client.request(second, 1)
    refute_receive {:graviton_mq, ^second, {:delivery, _, _}}, 100
    assert :ok = Client.ack(second, new_ref)
    Peer.eventually!(fn -> Client.status(second).deliveries == 0 end)
    Peer.publish!(context.executables, broker, "persistent/in", "subscription-survived", 2)

    assert_receive {:graviton_mq, ^second,
                    {:delivery, delivery, %{payload: "subscription-survived"}}},
                   5_000

    assert :ok = Client.ack(second, delivery)
    assert :ok = Client.disconnect(second)
  end

  test "incoming PUBREC ownership survives process death while awaiting PUBREL", context do
    broker = Peer.broker!(context.executables, context.tmp_dir, max_qos: 2)

    proxy =
      start_supervised!(
        {Proxy,
         upstream_port: broker.port,
         observer: self(),
         drop_packet_type: 6,
         drop_direction: :broker}
      )

    options = options(context, Proxy.port(proxy))
    first = client!(options)
    assert_receive {:graviton_mq, ^first, {:connected, _}}, 5_000
    assert {:ok, ref} = Client.subscribe(first, "persistent/phase", qos: 2)
    assert_receive {:graviton_mq, ^first, {:operation, ^ref, {:ok, [2]}}}, 5_000
    assert :ok = Client.request(first, 1)
    Peer.publish!(context.executables, broker, "persistent/phase", "phase", 2)
    assert_receive {:graviton_mq, ^first, {:delivery, delivery, packet}}, 5_000
    assert :ok = Client.ack(first, delivery)
    assert_receive {:puback_loss_proxy, ^proxy, {:ack_dropped, :broker, 6, 1, packet_id}}, 5_000
    assert_receive {:graviton_mq, ^first, {:disconnected, _}}, 5_000
    kill_owner!(first)
    second = client!(options)

    assert_receive {:graviton_mq, ^second, {:recovered_delivery, _, ^packet, :await_pubrel}},
                   5_000

    assert_receive {:puback_loss_proxy, ^proxy, {:connack, 2, %{session_present: true}}}, 5_000

    assert_receive {:puback_loss_proxy, ^proxy, {:ack_forwarded, :client, 7, 2, ^packet_id}},
                   5_000

    Peer.eventually!(fn -> Client.status(second).deliveries == 0 end)
    refute_receive {:graviton_mq, ^second, {:delivery, _, _}}, 100
    assert :ok = Client.disconnect(second)
  end

  test "real downtime reduces retransmitted message expiry", context do
    broker = Peer.broker!(context.executables, context.tmp_dir, max_qos: 2)

    proxy =
      start_supervised!(
        {Proxy, upstream_port: broker.port, observer: self(), drop_packet_type: 5}
      )

    options = options(context, Proxy.port(proxy))
    first = client!(options)
    assert_receive {:graviton_mq, ^first, {:connected, _}}, 5_000

    assert {:ok, ref} =
             Client.publish(first, "persistent/expiry", "expiry",
               qos: 2,
               properties: [message_expiry_interval: 30]
             )

    assert_receive {:graviton_mq, ^first, {:operation_identity, ^ref, stable_id}}, 5_000

    assert_receive {:puback_loss_proxy, ^proxy,
                    {:publish, 1, %{content: <<5, 2, 30::32, "expiry">>}}},
                   5_000

    assert_receive {:puback_loss_proxy, ^proxy, {:ack_dropped, :broker, 5, 1, _}}, 5_000
    assert_receive {:graviton_mq, ^first, {:disconnected, _}}, 5_000
    kill_owner!(first)
    Process.sleep(1_100)
    second = client!(options)

    assert_receive {:puback_loss_proxy, ^proxy,
                    {:publish, 2, %{dup: true, content: <<5, 2, remaining::32, "expiry">>}}},
                   5_000

    assert remaining in 1..29

    assert_receive {:graviton_mq, ^second,
                    {:recovered_operation_result, ^stable_id, {:ok, :pubcomp}}},
                   5_000

    assert :ok = Client.disconnect(second)
  end

  test "an operation deadline elapsed during downtime cannot publish after restart", context do
    broker = Peer.broker!(context.executables, context.tmp_dir, max_qos: 2)

    proxy =
      start_supervised!(
        {Proxy, upstream_port: broker.port, observer: self(), drop_packet_type: 5}
      )

    options = options(context, Proxy.port(proxy))
    first = client!(options)
    assert_receive {:graviton_mq, ^first, {:connected, _}}, 5_000
    assert {:ok, ref} = Client.publish(first, "persistent/deadline", "body", qos: 2)
    assert_receive {:graviton_mq, ^first, {:operation_identity, ^ref, stable_id}}, 5_000
    assert_receive {:puback_loss_proxy, ^proxy, {:ack_dropped, :broker, 5, 1, _}}, 5_000
    assert_receive {:graviton_mq, ^first, {:disconnected, _}}, 5_000
    kill_owner!(first)
    alias GravitonMQ.Client.MQTT.Store
    {:ok, store, snapshot} = Store.open(options[:persistence])
    {:ok, store} = Store.commit(store, %{snapshot | wall: snapshot.wall - 120_000})
    Store.close(store)
    second = client!(options)

    assert_receive {:graviton_mq, ^second,
                    {:recovered_operation_result, ^stable_id, {:error, :unknown_outcome}}},
                   5_000

    assert_receive {:graviton_mq, ^second, {:disconnected, :operation_timeout}}, 5_000
    assert Client.status(second).phase == :stopped
    assert Proxy.snapshot(proxy).connections == 1
    refute_receive {:puback_loss_proxy, ^proxy, {:publish, 2, _}}, 50
  end

  test "an independent BEAM SIGKILL releases inode lock and resumes PUBREL from the same files",
       context do
    broker = Peer.broker!(context.executables, context.tmp_dir, max_qos: 2)

    proxy =
      start_supervised!(
        {Proxy, upstream_port: broker.port, observer: self(), drop_packet_type: 7}
      )

    directory = Path.join(context.tmp_dir, "client-store")
    script = Path.join(context.tmp_dir, "client.exs")

    File.write!(script, """
    alias GravitonMQ.MQTTClient, as: Client
    IO.puts("OS_PID=" <> System.pid())
    {:ok, client} = Client.start_link(host: "127.0.0.1", port: #{Proxy.port(proxy)},
      observer: self(), client_id: "independent-vm", tick_interval: 60_000,
      operation_timeout: 60_000, reconnect: [max_attempts: 2, delay: 60_000, max_delay: 60_000],
      persistence: [directory: #{inspect(directory)}])
    if System.argv() == ["recover"] do
      receive do
        {:graviton_mq, ^client, {:recovered_operation, {uuid, id}, :publish}} ->
          IO.puts("RECOVERED=" <> Base.encode16(uuid) <> ":" <> Integer.to_string(id))
      after 5000 -> raise "no recovered operation" end
      receive do
        {:graviton_mq, ^client, {:recovered_operation_result, {uuid, id}, {:ok, :pubcomp}}} ->
          IO.puts("COMPLETE=" <> Base.encode16(uuid) <> ":" <> Integer.to_string(id))
      after 5000 -> raise "no recovered result" end
    else
      receive do {:graviton_mq, ^client, {:connected, _}} -> :ok after 5000 -> raise "no connection" end
      {:ok, ref} = Client.publish(client, "persistent/vm", "vm", qos: 2)
      receive do
        {:graviton_mq, ^client, {:operation_identity, ^ref, {uuid, id}}} ->
          IO.puts("IDENTITY=" <> Base.encode16(uuid) <> ":" <> Integer.to_string(id))
      after 5000 -> raise "no identity" end
    end
    Process.sleep(:infinity)
    """)

    vm = beam!(script)

    Peer.eventually!(fn -> Peer.snapshot(vm).output =~ "IDENTITY=" end, fn ->
      Peer.snapshot(vm)
    end)

    output = Peer.snapshot(vm).output
    [_, os_pid] = Regex.run(~r/OS_PID=(\d+)/, output)
    [_, uuid, id] = Regex.run(~r/IDENTITY=([0-9A-F]+):(\d+)/, output)
    stable_identity = uuid <> ":" <> id
    assert_receive {:puback_loss_proxy, ^proxy, {:ack_dropped, :broker, 7, 1, packet_id}}, 5_000

    assert {:error, :locked} =
             GravitonMQ.Client.MQTT.Store.open(directory: directory <> "/../client-store")

    assert {_, 0} =
             System.cmd(System.find_executable("kill"), ["-KILL", os_pid], stderr_to_stdout: true)

    Peer.eventually!(fn -> Peer.snapshot(vm).exit_status != nil end)
    Peer.eventually!(fn -> not Proxy.snapshot(proxy).open end)

    second_vm = beam!(script, ["recover"])

    Peer.eventually!(
      fn -> Peer.snapshot(second_vm).output =~ "RECOVERED=" <> stable_identity end,
      fn -> Peer.snapshot(second_vm) end
    )

    assert_receive {:puback_loss_proxy, ^proxy, {:connect, 2, %{clean_start: false}}}, 5_000

    assert_receive {:puback_loss_proxy, ^proxy, {:ack_forwarded, :client, 6, 2, ^packet_id}},
                   5_000

    Peer.eventually!(
      fn -> Peer.snapshot(second_vm).output =~ "COMPLETE=" <> stable_identity end,
      fn -> Peer.snapshot(second_vm) end
    )

    refute_receive {:puback_loss_proxy, ^proxy, {:publish, 2, _}}, 100
  end

  defp options(context, port) do
    [
      host: "127.0.0.1",
      port: port,
      observer: self(),
      client_id: "persistent",
      operation_timeout: 60_000,
      tick_interval: 60_000,
      reconnect: [max_attempts: 2, delay: 60_000, max_delay: 60_000],
      persistence: [directory: Path.join(context.tmp_dir, "client-store")]
    ]
  end

  defp client!(options) do
    {:ok, client} = Client.start_link(options)
    Process.unlink(client)
    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)
    client
  end

  defp kill_owner!(client) do
    monitor = Process.monitor(client)
    Process.exit(client, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^client, :killed}, 2_000
  end

  defp beam!(script, arguments \\ []) do
    paths =
      [GravitonMQ.Core, GravitonMQ.MQTT, GravitonMQ.MQTTClient, GravitonMQ.AMQP10]
      |> Enum.flat_map(fn module ->
        ["-pa", Path.dirname(:code.which(module) |> List.to_string())]
      end)

    Peer.process!(System.find_executable("elixir"), paths ++ [script] ++ arguments)
  end
end
