# S5a throughput and latency baseline, the load generator VM. From the umbrella root:
#   mix run --no-start scripts/bench/run.exs --profile full --out /tmp/s5a
# Spec: docs/superpowers/specs/2026-11-13-s5a-performance-baseline-design.md.
# Drives our own MQTT 5.0 and AMQP 1.0 clients against a broker in a second,
# Mix-free VM (scripts/bench/broker_vm.exs). The figures are not
# interoperability evidence, and no threshold is asserted.
Code.require_file("stats.exs", __DIR__)

defmodule GravitonMQBench.Run do
  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message
  alias GravitonMQ.AMQP10Client, as: AMQP
  alias GravitonMQ.MQTTClient, as: MQTT
  alias GravitonMQBench.Stats

  @broker_script Path.join(__DIR__, "broker_vm.exs")
  @broker_apps [
    :graviton_mq_core,
    :graviton_mq_amqp10,
    :graviton_mq_mqtt,
    :graviton_mq_storage,
    :graviton_mq_runtime,
    :graviton_mq,
    :telemetry,
    :ex_ssl,
    :hell
  ]
  @profiles %{
    "full" => %{warmup_ms: 5_000, measure_ms: 20_000, ns: [1, 8, 64]},
    "smoke" => %{warmup_ms: 1_000, measure_ms: 3_000, ns: [1, 8]}
  }
  @scenarios [
    %{id: "mqtt_qos0_mem", protocol: :mqtt, storage: "memory", qos: 0},
    %{id: "mqtt_qos1_mem", protocol: :mqtt, storage: "memory", qos: 1},
    %{id: "mqtt_qos1_durable", protocol: :mqtt, storage: "durable", qos: 1},
    %{id: "amqp_mem", protocol: :amqp, storage: "memory", durable: false},
    %{id: "amqp_durable", protocol: :amqp, storage: "durable", durable: true}
  ]
  # <<publisher::16, sequence::32, sent_at_us::signed-64>> precedes the padding.
  @header_bytes 14
  @credit 64
  # Spec §8 fixes READY (20 s) and broker stop (10 s). The client bounds are
  # generous because durable acceptance waits for synchronization on a real disk.
  @ready_ms 20_000
  @stop_ms 10_000
  @client_timeout_ms 60_000
  @operation_ms 65_000
  @connect_all_ms 120_000
  @consume_idle_ms 60_000
  @drain_ms 60_000

  def main(arguments) do
    config = options(arguments)
    File.mkdir_p!(config.out)
    File.mkdir_p!(config.data_root)
    environment = environment(config)

    results =
      for scenario <- @scenarios, n <- config.profile.ns do
        IO.puts("running #{scenario.id} n=#{n}")
        scenario(config, scenario, n)
      end

    document = %{
      spec: "docs/superpowers/specs/2026-11-13-s5a-performance-baseline-design.md",
      profile: config.profile_name,
      environment: environment,
      results: results
    }

    path = Path.join(config.out, "results.json")
    File.write!(path, json(document) <> "\n")
    Enum.each(results, &IO.puts(summary_line(&1)))
    IO.puts("wrote #{path}")
    failed = Enum.count(results, &(&1.status == "failed"))
    IO.puts("#{length(results) - failed} passed, #{failed} failed")
    if failed > 0, do: System.halt(1)
  end

  defp options(arguments) do
    {options, rest, invalid} =
      OptionParser.parse(arguments,
        strict: [profile: :string, out: :string, payload_bytes: :integer, data_dir: :string]
      )

    if rest != [] or invalid != [],
      do: raise("unknown arguments: #{inspect({rest, invalid})}")

    name = Keyword.get(options, :profile, "full")
    profile = Map.get(@profiles, name) || raise("--profile must be full or smoke")
    out = Keyword.get(options, :out) || raise("--out <directory> is required")
    bytes = Keyword.get(options, :payload_bytes, 256)

    unless bytes in 16..16_384, do: raise("--payload-bytes must be in 16..16384")

    %{
      profile_name: name,
      profile: profile,
      out: Path.expand(out),
      payload_bytes: bytes,
      data_root: Path.expand(Keyword.get(options, :data_dir, System.tmp_dir!()))
    }
  end

  defp scenario(config, scenario, n) do
    unique = System.unique_integer([:positive])
    name = "#{scenario.id}-#{n}-#{unique}"
    directory = Path.join(config.data_root, "graviton-s5a-" <> name)
    File.mkdir_p!(directory)
    Process.put(:bench_workers, [])
    Process.put(:bench_log, [])
    Process.put(:bench_broker, nil)

    {broker, outcome} =
      try do
        case start_broker(scenario, directory, "s5a-" <> name) do
          {:ok, broker} -> {broker, measure(config, scenario, n, broker, "s5a-" <> name)}
          {:error, reason, broker} -> {broker, {:error, reason}}
        end
      rescue
        error -> {Process.get(:bench_broker), {:error, Exception.message(error)}}
      end

    stop_workers()
    stopped = stop_broker(broker)
    File.rm_rf!(directory)
    flush_mailbox()

    case {outcome, stopped} do
      {{:ok, measured}, :ok} -> Map.merge(base(config, scenario, n, "passed", nil), measured)
      {{:ok, _measured}, {:error, reason}} -> base(config, scenario, n, "failed", reason)
      {{:error, reason}, _} -> base(config, scenario, n, "failed", reason)
    end
  end

  defp base(config, scenario, n, status, reason) do
    %{
      id: scenario.id,
      n: n,
      payload_bytes: config.payload_bytes,
      status: status,
      reason: reason,
      throughput: nil,
      consume_rate: nil,
      accept_latency_us: nil,
      e2e_latency_us: nil,
      duplicates: nil
    }
  end

  defp start_broker(scenario, directory, address) do
    paths =
      Enum.flat_map(@broker_apps, fn app ->
        ["-pa", app |> :code.lib_dir() |> List.to_string() |> Path.join("ebin")]
      end)

    port =
      Port.open({:spawn_executable, System.find_executable("elixir")}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        {:line, 65_536},
        args:
          paths ++
            [
              @broker_script,
              Atom.to_string(scenario.protocol),
              scenario.storage,
              directory,
              address
            ]
      ])

    {:os_pid, wrapper} = Port.info(port, :os_pid)
    broker = %{port: port, os_pids: [wrapper], mqtt_port: 0, amqp_port: 0}
    Process.put(:bench_broker, broker)
    deadline = now_ms() + @ready_ms

    case await_ready(port, deadline) do
      {:ok, os_pid, mqtt_port, amqp_port} ->
        broker = %{
          broker
          | os_pids: Enum.uniq([os_pid, wrapper]),
            mqtt_port: mqtt_port,
            amqp_port: amqp_port
        }

        Process.put(:bench_broker, broker)
        {:ok, broker}

      {:error, reason} ->
        {:error, reason, broker}
    end
  end

  defp await_ready(port, deadline) do
    receive do
      {^port, {:data, {:eol, "READY " <> fields}}} ->
        [os_pid, mqtt_port, amqp_port] =
          fields |> String.split(" ") |> Enum.map(&String.to_integer/1)

        {:ok, os_pid, mqtt_port, amqp_port}

      {^port, {:data, {_, line}}} ->
        log(line)
        await_ready(port, deadline)

      {^port, {:exit_status, status}} ->
        {:error, "broker VM exited with status #{status} before READY: #{log_tail()}"}
    after
      max(deadline - now_ms(), 0) -> {:error, "broker VM printed no READY within 20 s"}
    end
  end

  defp measure(config, scenario, n, broker, address) do
    parent = self()
    window = %{warmup_ms: config.profile.warmup_ms, measure_ms: config.profile.measure_ms}

    subscriber =
      spawn_worker(:subscriber, fn ->
        subscriber(parent, scenario, broker, address, config.payload_bytes)
      end)

    with :ok <- await_ready_workers(broker, [subscriber], now_ms() + @connect_all_ms),
         publishers =
           for(
             index <- 0..(n - 1),
             do:
               spawn_worker({:publisher, index}, fn ->
                 publisher(parent, scenario, broker, address, index, config.payload_bytes)
               end)
           ),
         :ok <- await_ready_workers(broker, publishers, now_ms() + @connect_all_ms) do
      start = now_us()
      window_start = start + window.warmup_ms * 1000
      window_end = window_start + window.measure_ms * 1000
      send(subscriber, {:go, window_start, window_end, List.to_tuple(publishers)})
      for pid <- publishers, do: send(pid, {:go, window_start, window_end})

      deadline = div(window_end, 1000) + @operation_ms + 5_000

      with {:ok, published} <-
             collect_published(broker, Map.new(publishers, &{&1, nil}), deadline),
           counts = Map.new(published, fn {_pid, result} -> {result.index, result.count} end),
           expected = counts |> Map.values() |> Enum.sum(),
           _ = send(subscriber, {:finish, expected}),
           {:ok, consumed} <- await_consumed(broker, subscriber),
           accounting = Stats.accounting(counts, consumed.seen),
           :ok <- Stats.verdict(accounting),
           :ok <- await_drained(broker, scenario, now_ms() + @drain_ms) do
        measured = published |> Map.values() |> Enum.map(& &1.measured) |> Enum.sum()
        accept = published |> Map.values() |> Enum.flat_map(& &1.samples)

        throughput =
          if Map.get(scenario, :qos) == 0,
            do: Stats.rate(length(consumed.e2e), window.measure_ms),
            else: Stats.rate(measured, window.measure_ms)

        {:ok,
         %{
           throughput: throughput,
           consume_rate: Stats.rate(consumed.in_window, window.measure_ms),
           accept_latency_us: Stats.latency_summary(accept),
           e2e_latency_us: Stats.latency_summary(consumed.e2e),
           duplicates: accounting.duplicates,
           published: expected,
           delivered: accounting.delivered
         }}
      end
    end
  end

  defp spawn_worker(role, fun) do
    parent = self()

    {pid, _monitor} =
      spawn_monitor(fn ->
        try do
          fun.()
        rescue
          error ->
            send(parent, {:failed, self(), role, Exception.format(:error, error, __STACKTRACE__)})
        catch
          kind, reason -> send(parent, {:failed, self(), role, inspect({kind, reason})})
        end
      end)

    Process.put(:bench_workers, [pid | Process.get(:bench_workers)])
    pid
  end

  defp await_ready_workers(_broker, [], _deadline), do: :ok

  defp await_ready_workers(broker, pending, deadline) do
    port = broker.port

    receive do
      {:ready, pid} ->
        await_ready_workers(broker, List.delete(pending, pid), deadline)

      {:failed, _pid, role, reason} ->
        {:error, "#{inspect(role)} failed: #{reason}"}

      {:DOWN, _, :process, _, reason} when reason != :normal ->
        {:error, "worker exited: #{inspect(reason)}"}

      {^port, {:exit_status, status}} ->
        {:error, "broker VM exited with status #{status}"}

      {^port, {:data, {_, line}}} ->
        log(line)
        await_ready_workers(broker, pending, deadline)
    after
      max(deadline - now_ms(), 0) -> {:error, "clients did not connect within 120 s"}
    end
  end

  defp collect_published(broker, pending, deadline) do
    port = broker.port

    if Enum.all?(pending, fn {_pid, result} -> result != nil end) do
      {:ok, pending}
    else
      receive do
        {:published, pid, result} ->
          collect_published(broker, Map.put(pending, pid, result), deadline)

        {:failed, _pid, role, reason} ->
          {:error, "#{inspect(role)} failed: #{reason}"}

        {:DOWN, _, :process, _, reason} when reason != :normal ->
          {:error, "worker exited: #{inspect(reason)}"}

        {^port, {:exit_status, status}} ->
          {:error, "broker VM exited with status #{status}: #{log_tail()}"}

        {^port, {:data, {_, line}}} ->
          log(line)
          collect_published(broker, pending, deadline)
      after
        max(deadline - now_ms(), 0) -> {:error, "publishers did not finish"}
      end
    end
  end

  defp await_consumed(broker, subscriber) do
    port = broker.port

    receive do
      {:consumed, ^subscriber, consumed} ->
        {:ok, consumed}

      {:failed, _pid, role, reason} ->
        {:error, "#{inspect(role)} failed: #{reason}"}

      {:DOWN, _, :process, _, reason} when reason != :normal ->
        {:error, "worker exited: #{inspect(reason)}"}

      {^port, {:exit_status, status}} ->
        {:error, "broker VM exited with status #{status}: #{log_tail()}"}

      {^port, {:data, {_, line}}} ->
        log(line)
        await_consumed(broker, subscriber)
    end
  end

  defp await_drained(broker, scenario, deadline) do
    expected_profile = if scenario.storage == "durable", do: "local_durable", else: "memory"

    case broker_status(broker) do
      {:ok, %{"profile" => profile}} when profile != expected_profile ->
        {:error, "broker profile #{profile}, expected #{expected_profile}"}

      {:ok, status} ->
        cond do
          drained?(scenario, status) ->
            :ok

          now_ms() >= deadline ->
            {:error, "not drained: #{inspect(status)}"}

          true ->
            Process.sleep(50)
            await_drained(broker, scenario, deadline)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp drained?(%{protocol: :mqtt}, status),
    do:
      status["storage"] == "ready" and status["pending_deliveries"] == 0 and
        status["prepared"] == 0

  defp drained?(%{protocol: :amqp}, status),
    do: match?(%{"messages" => 0, "inflight" => 0}, status["queue"])

  defp broker_status(broker) do
    port = broker.port
    true = Port.command(port, "STATUS\n")
    await_status(port, now_ms() + @operation_ms)
  end

  defp await_status(port, deadline) do
    receive do
      {^port, {:data, {:eol, "STATUS " <> json}}} ->
        {:ok, :json.decode(json)}

      {^port, {:data, {_, line}}} ->
        log(line)
        await_status(port, deadline)

      {^port, {:exit_status, status}} ->
        {:error, "broker VM exited with status #{status}: #{log_tail()}"}
    after
      max(deadline - now_ms(), 0) -> {:error, "broker VM did not answer STATUS"}
    end
  end

  # Publisher: one connection, a window of one outstanding publication.
  defp publisher(parent, %{protocol: :mqtt} = scenario, broker, address, index, bytes) do
    {:ok, client} =
      MQTT.start_link(
        host: "127.0.0.1",
        port: broker.mqtt_port,
        client_id: "#{address}-publisher-#{index}",
        observer: self(),
        session_expiry_interval: 0,
        handshake_timeout: @client_timeout_ms,
        operation_timeout: @client_timeout_ms
      )

    connected(client)
    expected = if scenario.qos == 0, do: :sent, else: :puback

    publish = fn payload, sequence ->
      {:ok, ^expected} =
        operation(client, MQTT.publish(client, address, payload, qos: scenario.qos))

      if scenario.qos == 0 do
        receive do
          {:bench_delivered, ^sequence} -> :ok
        after
          @operation_ms -> raise("QoS 0 publication #{sequence} was not delivered within 65 s")
        end
      end
    end

    run_publisher(parent, index, bytes, scenario.qos != 0, publish)
    :ok = MQTT.disconnect(client)
    stop_client(client)
  end

  defp publisher(parent, %{protocol: :amqp} = scenario, broker, address, index, bytes) do
    client = amqp_connect(broker, "#{address}-publisher-#{index}", [])
    name = "publisher-#{index}"

    {:ok, sender} =
      operation(client, AMQP.attach(client, 1, name, :sender, address: Value.string(address)))

    header = if scenario.durable, do: [{:header, Value.list([Value.boolean(true)])}], else: []

    publish = fn payload, _sequence ->
      {:ok, raw} = Message.encode_sections(header ++ [{:data, Value.binary(payload)}])
      {:ok, %Outcome.Accepted{}} = operation(client, AMQP.send_message(client, 1, sender, raw))
    end

    run_publisher(parent, index, bytes, true, publish)
    {:ok, :closed} = operation(client, AMQP.close(client))
    stop_client(client)
  end

  defp run_publisher(parent, index, bytes, record_accept?, publish) do
    send(parent, {:ready, self()})

    receive do
      {:go, window_start, window_end} ->
        state = %{
          index: index,
          padding: :binary.copy(<<0>>, bytes - @header_bytes),
          publish: publish,
          record_accept?: record_accept?,
          window_start: window_start,
          window_end: window_end,
          count: 0,
          measured: 0,
          samples: []
        }

        result = publish_loop(state)

        send(
          parent,
          {:published, self(), Map.take(result, [:index, :count, :measured, :samples])}
        )
    after
      @connect_all_ms + @ready_ms -> raise("publisher start gate timeout")
    end
  end

  defp publish_loop(state) do
    started = now_us()

    if started >= state.window_end do
      state
    else
      sequence = state.count + 1
      payload = <<state.index::16, sequence::32, started::signed-64, state.padding::binary>>
      state.publish.(payload, sequence)
      finished = now_us()
      measured? = started >= state.window_start

      publish_loop(%{
        state
        | count: sequence,
          measured: state.measured + if(measured?, do: 1, else: 0),
          samples:
            if(measured? and state.record_accept?,
              do: [finished - started | state.samples],
              else: state.samples
            )
      })
    end
  end

  # Subscriber/receiver: one connection, consumes everything.
  defp subscriber(parent, %{protocol: :mqtt} = scenario, broker, address, _bytes) do
    {:ok, client} =
      MQTT.start_link(
        host: "127.0.0.1",
        port: broker.mqtt_port,
        client_id: "#{address}-subscriber",
        observer: self(),
        session_expiry_interval: 0,
        handshake_timeout: @client_timeout_ms,
        operation_timeout: @client_timeout_ms,
        max_deliveries: 1_024,
        receive_maximum: 1_024
      )

    connected(client)
    qos = scenario.qos
    {:ok, [^qos]} = operation(client, MQTT.subscribe(client, address, qos: qos))
    :ok = MQTT.request(client, 1_024)

    take = fn freed, state ->
      receive do
        {:graviton_mq, ^client, {:delivery, delivery, packet}} ->
          :ok = MQTT.ack(client, delivery)
          state = received(state, packet.payload)

          if freed + 1 >= 256 do
            :ok = MQTT.request(client, freed + 1)
            {0, state}
          else
            {freed + 1, state}
          end

        {:graviton_mq, ^client, {:disconnected, reason}} ->
          raise("subscriber disconnected: #{inspect(reason)}")
      after
        100 -> {freed, state}
      end
    end

    run_subscriber(parent, qos == 0, take)
    :ok = MQTT.disconnect(client)
    stop_client(client)
  end

  defp subscriber(parent, %{protocol: :amqp}, broker, address, bytes) do
    link_bytes = bytes + 1_024
    client = amqp_connect(broker, "#{address}-receiver", max_operations: 512)

    {:ok, receiver} =
      operation(
        client,
        AMQP.attach(client, 1, "receiver", :receiver,
          address: Value.string(address),
          max_message_bytes: link_bytes
        )
      )

    {:ok, @credit} =
      operation(client, AMQP.request(client, 1, receiver, @credit, @credit * link_bytes))

    take = fn freed, state ->
      receive do
        {:graviton_mq, ^client, {:delivery, delivery, 1, ^receiver, raw, %{message_format: 0}}} ->
          {:ok, _settle} = AMQP.settle(client, delivery, %Outcome.Accepted{})
          {freed, received(state, binary_part(raw, byte_size(raw) - bytes, bytes))}

        {:graviton_mq, ^client, {:operation, _settle, {:ok, %Outcome.Accepted{}}}} ->
          if freed + 1 >= 16 do
            {:ok, _request} =
              AMQP.request(client, 1, receiver, freed + 1, (freed + 1) * link_bytes)

            {0, state}
          else
            {freed + 1, state}
          end

        {:graviton_mq, ^client, {:operation, _request, {:ok, granted}}}
        when is_integer(granted) ->
          {freed, state}

        {:graviton_mq, ^client, {:operation, _reference, result}} ->
          raise("receiver operation failed: #{inspect(result)}")

        {:graviton_mq, ^client, {:disconnected, reason}} ->
          raise("receiver disconnected: #{inspect(reason)}")
      after
        100 -> {freed, state}
      end
    end

    run_subscriber(parent, false, take)
    {:ok, :closed} = operation(client, AMQP.close(client))
    stop_client(client)
  end

  defp run_subscriber(parent, notify?, take) do
    send(parent, {:ready, self()})

    receive do
      {:go, window_start, window_end, publishers} ->
        state = %{
          notify?: notify?,
          publishers: publishers,
          window_start: window_start,
          window_end: window_end,
          seen: %{},
          e2e: [],
          in_window: 0,
          expected: nil,
          quiet_since: nil
        }

        state = consume(take, 0, state)
        send(parent, {:consumed, self(), Map.take(state, [:seen, :e2e, :in_window])})

        receive do
          :stop -> :ok
        after
          @drain_ms + @operation_ms -> :ok
        end
    after
      @connect_all_ms + @ready_ms -> raise("subscriber start gate timeout")
    end
  end

  # After {:finish, expected}, consumption ends when every expected key was
  # seen, or when nothing arrived for @consume_idle_ms (the gate then fails).
  defp consume(take, freed, state) do
    state =
      receive do
        {:finish, expected} -> %{state | expected: expected, quiet_since: now_us()}
      after
        0 -> state
      end

    cond do
      state.expected != nil and map_size(state.seen) >= state.expected ->
        state

      state.expected != nil and now_us() - state.quiet_since >= @consume_idle_ms * 1000 ->
        state

      true ->
        {freed, state} = take.(freed, state)
        consume(take, freed, state)
    end
  end

  defp received(state, <<publisher::16, sequence::32, sent_at::signed-64, _::binary>>) do
    at = now_us()
    key = {publisher, sequence}
    first? = not Map.has_key?(state.seen, key)

    if state.notify? and publisher < tuple_size(state.publishers),
      do: send(elem(state.publishers, publisher), {:bench_delivered, sequence})

    %{
      state
      | quiet_since: if(state.quiet_since, do: at, else: nil),
        seen: Map.update(state.seen, key, 1, &(&1 + 1)),
        e2e:
          if(first? and sent_at >= state.window_start and sent_at < state.window_end,
            do: [at - sent_at | state.e2e],
            else: state.e2e
          ),
        in_window:
          state.in_window + if(at >= state.window_start and at < state.window_end, do: 1, else: 0)
    }
  end

  defp received(_state, payload),
    do: raise("malformed benchmark payload of #{byte_size(payload)} bytes")

  defp amqp_connect(broker, container_id, options) do
    {:ok, client} =
      AMQP.start_link(
        [
          host: {127, 0, 0, 1},
          port: broker.amqp_port,
          container_id: container_id,
          observer: self(),
          operation_timeout_ms: @client_timeout_ms
        ] ++
          options
      )

    connected(client)
    {:ok, 1} = operation(client, AMQP.open_session(client, 1))
    client
  end

  defp connected(client) do
    receive do
      {:graviton_mq, ^client, {:connected, _peer}} ->
        :ok

      {:graviton_mq, ^client, {:disconnected, reason}} ->
        raise("connect failed: #{inspect(reason)}")
    after
      @operation_ms -> raise("client connection timeout")
    end
  end

  defp operation(client, {:ok, reference}) do
    receive do
      {:graviton_mq, ^client, {:operation, ^reference, result}} ->
        result

      {:graviton_mq, ^client, {:disconnected, reason}} ->
        raise("operation interrupted: #{inspect(reason)}")
    after
      @operation_ms -> raise("client operation timeout")
    end
  end

  defp operation(_client, admission), do: raise("command admission failed: #{inspect(admission)}")

  defp stop_client(client) do
    if Process.alive?(client), do: GenServer.stop(client, :normal, 5_000)
  end

  defp stop_workers do
    workers = Process.get(:bench_workers, [])
    for pid <- workers, do: send(pid, :stop)
    deadline = now_ms() + 8_000
    Enum.each(workers, &wait_dead(&1, deadline))
    for pid <- workers, Process.alive?(pid), do: Process.exit(pid, :kill)
  end

  defp wait_dead(pid, deadline) do
    if Process.alive?(pid) and now_ms() < deadline do
      Process.sleep(20)
      wait_dead(pid, deadline)
    end
  end

  # Closes the broker VM's stdin (EOF), waits up to 10 s, then kills it.
  defp stop_broker(nil), do: :ok

  defp stop_broker(broker) do
    try do
      Port.close(broker.port)
    rescue
      ArgumentError -> :already_closed
    end

    deadline = now_ms() + @stop_ms
    Enum.each(broker.os_pids, &wait_exited(&1, deadline))
    survivors = Enum.filter(broker.os_pids, &os_alive?/1)
    for pid <- survivors, do: System.cmd("kill", ["-KILL", Integer.to_string(pid)])

    if survivors == [],
      do: :ok,
      else: {:error, "broker VM did not stop within 10 s and was killed"}
  end

  defp wait_exited(os_pid, deadline) do
    if os_alive?(os_pid) and now_ms() < deadline do
      Process.sleep(20)
      wait_exited(os_pid, deadline)
    end
  end

  defp os_alive?(os_pid) do
    case File.read("/proc/#{os_pid}/stat") do
      {:ok, stat} -> not String.contains?(stat, ") Z ")
      {:error, _} -> false
    end
  end

  defp flush_mailbox do
    receive do
      _message -> flush_mailbox()
    after
      0 -> :ok
    end
  end

  defp log(line), do: Process.put(:bench_log, Enum.take([line | Process.get(:bench_log, [])], 20))
  defp log_tail, do: Process.get(:bench_log, []) |> Enum.reverse() |> Enum.join(" | ")

  defp environment(config) do
    %{
      commit: command("git", ["rev-parse", "HEAD"]),
      dirty: command("git", ["status", "--porcelain"]) != "",
      cpu_model: cpu_model(),
      logical_cores: :erlang.system_info(:logical_processors),
      memory_bytes: memory_bytes(),
      otp: otp_version(),
      elixir: System.version(),
      schedulers_online: System.schedulers_online(),
      data_directory: config.data_root,
      filesystem: filesystem(config.data_root),
      block_device: command("findmnt", ["-n", "-o", "SOURCE", "-T", config.data_root]),
      warmup_ms: config.profile.warmup_ms,
      measure_ms: config.profile.measure_ms,
      payload_bytes: config.payload_bytes
    }
  end

  defp command(executable, arguments) do
    case System.find_executable(executable) do
      nil ->
        "unavailable"

      path ->
        case System.cmd(path, arguments, stderr_to_stdout: true) do
          {output, 0} -> String.trim(output)
          {output, status} -> "unavailable (#{status}: #{String.trim(output)})"
        end
    end
  end

  defp cpu_model do
    with {:ok, info} <- File.read("/proc/cpuinfo"),
         [_, model] <- Regex.run(~r/^model name\s*:\s*(.+)$/m, info) do
      String.trim(model)
    else
      _ -> "unavailable"
    end
  end

  defp memory_bytes do
    with {:ok, info} <- File.read("/proc/meminfo"),
         [_, kib] <- Regex.run(~r/^MemTotal:\s+(\d+) kB$/m, info) do
      String.to_integer(kib) * 1024
    else
      _ -> nil
    end
  end

  defp otp_version do
    path = Path.join([:code.root_dir(), "releases", System.otp_release(), "OTP_VERSION"])

    case File.read(path) do
      {:ok, version} -> String.trim(version)
      {:error, _} -> System.otp_release()
    end
  end

  defp filesystem(directory) do
    case command("df", ["-T", directory]) |> String.split("\n") do
      [_header, row | _] -> row |> String.split() |> Enum.at(1, "unavailable")
      _ -> "unavailable"
    end
  end

  defp summary_line(%{status: "failed"} = result),
    do: "FAIL #{result.id} n=#{result.n}: #{result.reason}"

  defp summary_line(result) do
    "PASS #{result.id} n=#{result.n}: #{result.throughput} msg/s, " <>
      "consume #{result.consume_rate} msg/s, " <>
      "accept p50/p99/p999/max #{latency(result.accept_latency_us)} us, " <>
      "e2e #{latency(result.e2e_latency_us)} us, duplicates #{result.duplicates}"
  end

  defp latency(nil), do: "-"
  defp latency(summary), do: "#{summary.p50}/#{summary.p99}/#{summary.p999}/#{summary.max}"

  defp now_ms, do: System.monotonic_time(:millisecond)
  defp now_us, do: System.monotonic_time(:microsecond)
  defp json(value), do: value |> portable() |> :json.encode() |> IO.iodata_to_binary()

  defp portable(value) when is_map(value),
    do: Map.new(value, fn {key, child} -> {to_string(key), portable(child)} end)

  defp portable(value) when is_list(value), do: Enum.map(value, &portable/1)
  defp portable(nil), do: :null
  defp portable(value) when is_boolean(value), do: value
  defp portable(value) when is_atom(value), do: Atom.to_string(value)
  defp portable(value), do: value
end

GravitonMQBench.Run.main(System.argv())
