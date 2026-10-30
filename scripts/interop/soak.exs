# Run through scripts/interop/run.py so Mosquitto/Artemis are real pinned peers:
# mix run --no-start scripts/interop/soak.exs --duration 10 --rate 10 --payload-bytes 2048 --logs /tmp/soak
# Memory profiles only. This is sustained network qualification, not crash,
# storage durability, TLS, clustering or unbounded production load acceptance.
defmodule GravitonMQ.Interop.Soak do
  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message
  alias GravitonMQ.AMQP10Client, as: AMQP
  alias GravitonMQ.MQTTClient, as: MQTT

  @roles [:mqtt_client, :amqp_client, :mqtt_broker, :amqp_broker]

  def run(arguments) do
    config = options(arguments)
    File.mkdir_p!(config.logs)
    samples = Path.join(config.logs, "samples.jsonl")
    if File.exists?(samples), do: raise("choose a new soak evidence directory")
    {:ok, _} = Application.ensure_all_started(:ssl)
    Process.put(:soak_workers, [])
    Process.put(:soak_os_pids, [])
    Process.put(:soak_broker, nil)
    baseline = resources()

    outcome =
      try do
        {:ok, broker} = broker(config)
        Process.put(:soak_broker, broker)
        {:ok, %{listeners: [%{port: mqtt_port}]}} = GravitonMQ.status(broker)
        {:ok, %{listeners: [%{port: amqp_port}]}} = GravitonMQ.amqp10_status(broker)
        parent = self()
        start_worker(:mqtt_client, fn -> mqtt_client(parent, config) end)
        start_worker(:amqp_client, fn -> amqp_client(parent, config) end)
        start_worker(:mqtt_broker, fn -> python_peer(parent, :mqtt, mqtt_port, config) end)
        start_worker(:amqp_broker, fn -> python_peer(parent, :amqp, amqp_port, config) end)
        workers = Process.get(:soak_workers)
        ready(MapSet.new(), now() + 20_000)
        started = now()
        for worker <- workers, do: send(worker.pid, {:go, started})
        record(samples, %{event: :start, config: Map.drop(config, [:logs]), baseline: baseline})

        results =
          collect(
            broker,
            config,
            baseline,
            samples,
            %{},
            %{},
            started,
            started + config.duration * 1000 + 15_000,
            started
          )

        eventually(fn -> drained?(broker, config.address) end, now() + 5_000)

        {:ok,
         %{
           status: :PASS,
           duration_ms: now() - started,
           roles: results,
           final_broker: broker_status(broker, config.address)
         }}
      rescue
        error -> {:error, error, __STACKTRACE__}
      end

    cleanup = cleanup()

    summary =
      case outcome do
        {:ok, summary} ->
          Map.put(summary, :cleanup, cleanup)

        {:error, error, _stack} ->
          %{status: :FAIL, error: Exception.message(error), cleanup: cleanup}
      end

    record(Path.join(config.logs, "summary.json"), summary)
    IO.puts(json(summary))

    case outcome do
      {:ok, _} ->
        unless cleanup.workers_stopped and cleanup.broker_stopped and
                 cleanup.python_peers_stopped,
               do: raise("soak cleanup could not be confirmed")

      {:error, error, stack} ->
        reraise(error, stack)
    end
  end

  defp options(arguments) do
    {options, rest, invalid} =
      OptionParser.parse(arguments,
        strict: [duration: :integer, rate: :integer, payload_bytes: :integer, logs: :string]
      )

    if rest != [] or invalid != [],
      do: raise("unknown soak arguments: #{inspect({rest, invalid})}")

    duration = Keyword.get(options, :duration, env_integer("GRAVITON_SOAK_SECONDS", 10))
    rate = Keyword.get(options, :rate, env_integer("GRAVITON_SOAK_RATE", 10))
    bytes = Keyword.get(options, :payload_bytes, env_integer("GRAVITON_SOAK_PAYLOAD_BYTES", 2048))

    unless duration in 5..3600 and rate in 1..50 and bytes in 16..16_384,
      do: raise("soak bounds: duration5..3600, rate1..50, payload16..16384")

    unique = System.unique_integer([:positive])

    %{
      duration: duration,
      rate: rate,
      payload_bytes: bytes,
      packet_limit: bytes + 1024,
      address: "graviton-g7-soak-#{unique}",
      logs:
        Path.expand(
          Keyword.get(
            options,
            :logs,
            System.get_env("GRAVITON_SOAK_LOGS") ||
              Path.join(System.tmp_dir!(), "graviton-soak-#{unique}")
          )
        )
    }
  end

  defp broker(config) do
    listener = [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]

    GravitonMQ.start_link(
      name: nil,
      runtime_supervisor_name: nil,
      mqtt: [
        auth: :anonymous,
        tick_interval: 20,
        listeners: listener,
        max_connections: 4,
        max_sessions: 4,
        max_inflight: 4,
        receive_maximum: 4,
        max_packet_size: config.packet_limit,
        max_prepared: 8,
        max_pending_deliveries: 8,
        max_pending_bytes: 1_048_576,
        max_offline_deliveries: 8,
        max_offline_bytes: 1_048_576
      ],
      amqp10: [
        container_id: config.address,
        auth: :anonymous,
        queues: [config.address],
        listeners: listener,
        max_connections: 4,
        max_input_bytes: 65_536,
        queue_options: [
          max_messages: 8,
          max_bytes: 1_048_576,
          max_message_bytes: config.packet_limit,
          max_consumers: 4,
          max_inflight: 4,
          max_batch: 4
        ],
        endpoint_options: [max_frame_size: 512, session_options: session_options(config)]
      ]
    )
  end

  defp session_options(config) do
    [
      max_message_bytes: config.packet_limit,
      max_incoming_bytes: config.packet_limit * 4,
      max_outgoing_bytes: config.packet_limit * 4,
      max_incoming_deliveries: 4,
      max_outgoing_deliveries: 4,
      max_pending: 8,
      incoming_window: 64,
      outgoing_window: 64
    ]
  end

  defp start_worker(role, fun) do
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        try do
          result = fun.()
          send(parent, {:done, self(), role, result})
        rescue
          error ->
            send(parent, {:failed, self(), role, Exception.format(:error, error, __STACKTRACE__)})
        catch
          kind, reason -> send(parent, {:failed, self(), role, inspect({kind, reason})})
        end
      end)

    Process.put(:soak_workers, [
      %{pid: pid, monitor: monitor, role: role} | Process.get(:soak_workers)
    ])
  end

  defp mqtt_client(parent, config) do
    {:ok, client} =
      MQTT.start_link(
        host: "127.0.0.1",
        port: System.fetch_env!("GRAVITON_MQTT_TEST_PORT") |> String.to_integer(),
        client_id: config.address,
        observer: self(),
        session_expiry_interval: 0,
        max_operations: 4,
        max_inflight: 2,
        max_deliveries: 2,
        receive_maximum: 2,
        max_subscriptions: 2,
        max_packet_size: config.packet_limit,
        tick_interval: 20,
        keep_alive: 10,
        operation_timeout: 5_000
      )

    try do
      connected(client)
      {:ok, [2]} = operation(client, MQTT.subscribe(client, config.address, qos: 2))
      gate(parent)

      result =
        loop(config, parent, :mqtt_client, fn body, qos ->
          :ok = MQTT.request(client, 1)
          {:ok, publication} = MQTT.publish(client, config.address, body, qos: qos)
          expected = if qos == 1, do: :puback, else: :pubcomp
          {:ok, ^expected} = operation(client, {:ok, publication})

          receive do
            {:graviton_mq, ^client, {:delivery, delivery, packet}} ->
              true =
                packet.topic == config.address and packet.payload == body and packet.qos == qos

              :ok = MQTT.ack(client, delivery)
          after
            5_000 -> raise("MQTT client delivery timeout")
          end

          eventually(fn -> MQTT.status(client).deliveries == 0 end, now() + 5_000)
          status = MQTT.status(client)
          true = status.pending == 0 and status.phase == :connected
        end)

      :ok = MQTT.disconnect(client)
      result
    after
      stop_client(client)
    end
  end

  defp amqp_client(parent, config) do
    {:ok, client} =
      AMQP.start_link(
        host: System.fetch_env!("ARTEMIS_HOST"),
        port: System.fetch_env!("ARTEMIS_PORT") |> String.to_integer(),
        observer: self(),
        container_id: config.address,
        max_frame_size: 512,
        max_operations: 8,
        max_input_bytes: 65_536,
        session_options: session_options(config)
      )

    try do
      connected(client)
      {:ok, 1} = operation(client, AMQP.open_session(client, 1))
      receiver = {Value.string("soak-receiver"), :receiver}
      sender = {Value.string("soak-sender"), :sender}

      {:ok, ^receiver} =
        operation(
          client,
          AMQP.attach(client, 1, "soak-receiver", :receiver,
            address: Value.string(config.address),
            max_message_bytes: config.packet_limit
          )
        )

      {:ok, ^sender} =
        operation(
          client,
          AMQP.attach(client, 1, "soak-sender", :sender, address: Value.string(config.address))
        )

      gate(parent)

      result =
        loop(config, parent, :amqp_client, fn body, _qos ->
          {:ok, raw} = Message.encode_sections([{:data, Value.binary(body)}])
          {:ok, 1} = operation(client, AMQP.request(client, 1, receiver, 1, config.packet_limit))

          {:ok, %Outcome.Accepted{}} =
            operation(client, AMQP.send_message(client, 1, sender, raw))

          reference =
            receive do
              {:graviton_mq, ^client,
               {:delivery, ref, 1, ^receiver, received, %{message_format: 0}}} ->
                {:ok, decoded, <<>>} = Message.decode(received)

                bodies =
                  for {kind, %Value{type: :binary, value: bytes}} <- decoded.sections,
                      kind in [:data, :amqp_value],
                      do: bytes

                true = IO.iodata_to_binary(bodies) == body
                ref
            after
              5_000 -> raise("AMQP client delivery timeout")
            end

          {:ok, %Outcome.Accepted{}} =
            operation(client, AMQP.settle(client, reference, %Outcome.Accepted{}))

          status = AMQP.status(client)

          true =
            status.pending == 0 and status.phase == :active and status.buffered_bytes <= 65_536
        end)

      {:ok, _} = operation(client, AMQP.close(client))
      result
    after
      stop_client(client)
    end
  end

  defp gate(parent) do
    send(parent, {:ready, self()})

    receive do
      {:go, _started} -> :ok
      :stop -> raise("soak stopped before start")
    after
      20_000 -> raise("soak start gate timeout")
    end
  end

  defp connected(client) do
    receive do
      {:graviton_mq, ^client, {:connected, _peer}} ->
        :ok

      {:graviton_mq, ^client, {:disconnected, reason}} ->
        raise("connect failed: #{inspect(reason)}")
    after
      5_000 -> raise("client connection timeout")
    end
  end

  defp operation(client, {:ok, reference}) do
    receive do
      {:graviton_mq, ^client, {:operation, ^reference, result}} ->
        result

      {:graviton_mq, ^client, {:disconnected, reason}} ->
        raise("operation interrupted: #{inspect(reason)}")

      :stop ->
        raise("soak role stopped")
    after
      5_000 -> raise("client operation timeout")
    end
  end

  defp operation(_client, result), do: raise("command admission failed: #{inspect(result)}")

  defp loop(config, parent, role, roundtrip) do
    start = now()

    state = %{
      iterations: 0,
      maximum_latency_us: 0,
      qos1: 0,
      qos2: 0,
      hash: :crypto.hash_init(:sha256)
    }

    result =
      iterations(config, parent, role, roundtrip, start, start + config.duration * 1000, state)

    true = result.iterations >= max(2, div(config.duration * config.rate, 4))

    result
    |> Map.delete(:hash)
    |> Map.put(:sha256, :crypto.hash_final(result.hash) |> Base.encode16(case: :lower))
    |> Map.put(:duration_ms, now() - start)
    |> Map.put(:sent_bytes, result.iterations * config.payload_bytes)
    |> Map.put(:received_bytes, result.iterations * config.payload_bytes)
  end

  defp iterations(config, parent, role, roundtrip, start, deadline, state) do
    if now() >= deadline do
      state
    else
      receive do
        :stop -> raise("soak role stopped")
      after
        0 -> :ok
      end

      before = System.monotonic_time(:microsecond)
      count = state.iterations + 1
      pattern = :binary.list_to_bin(Enum.to_list(0..255))

      body =
        <<count::64,
          binary_part(
            :binary.copy(pattern, div(config.payload_bytes + 255, 256)),
            0,
            config.payload_bytes - 8
          )::binary>>

      qos = 1 + rem(count - 1, 2)
      roundtrip.(body, qos)
      elapsed = System.monotonic_time(:microsecond) - before

      state = %{
        state
        | iterations: count,
          maximum_latency_us: max(state.maximum_latency_us, elapsed),
          qos1: state.qos1 + if(role == :mqtt_client and qos == 1, do: 1, else: 0),
          qos2: state.qos2 + if(role == :mqtt_client and qos == 2, do: 1, else: 0),
          hash: :crypto.hash_update(state.hash, body)
      }

      send(parent, {:progress, self(), role, Map.take(state, [:iterations, :maximum_latency_us])})
      target = start + div(count * 1000, config.rate)
      Process.sleep(max(0, target - now()))
      iterations(config, parent, role, roundtrip, start, deadline, state)
    end
  end

  defp python_peer(parent, role, port_number, config) do
    executable =
      System.fetch_env!(
        if role == :mqtt, do: "GRAVITON_PAHO_PYTHON", else: "GRAVITON_PROTON_PYTHON"
      )

    script = Path.join(__DIR__, "soak_peer.py")

    args = [
      "-u",
      script,
      Atom.to_string(role),
      "--port",
      to_string(port_number),
      "--address",
      config.address,
      "--duration",
      to_string(config.duration),
      "--rate",
      to_string(config.rate),
      "--payload-bytes",
      to_string(config.payload_bytes)
    ]

    port =
      Port.open({:spawn_executable, executable}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        {:line, 16_384},
        {:args, args}
      ])

    send(parent, {:os_peer, self(), Port.info(port, :os_pid) |> elem(1)})

    try do
      peer_events(port, parent, role, now() + config.duration * 1000 + 25_000, nil)
    after
      if Port.info(port) != nil, do: Port.close(port)
    end
  end

  defp peer_events(port, parent, role, deadline, result) do
    receive do
      {^port, {:data, {:eol, line}}} ->
        event = :json.decode(line)

        case event["event"] do
          "ready" ->
            send(parent, {:ready, self()})

            receive do
              {:go, _} -> true = Port.command(port, "GO\n")
              :stop -> true = Port.command(port, "STOP\n")
            after
              20_000 -> raise("independent peer start gate timeout")
            end

          "error" ->
            raise("#{role} independent peer: #{event["error"]}")

          "progress" ->
            send(parent, {:progress, self(), broker_role(role), event})

          "done" ->
            :ok

          _ ->
            raise("unexpected independent peer event")
        end

        result = if event["event"] == "done", do: event, else: result
        peer_events(port, parent, role, deadline, result)

      {^port, {:data, {:noeol, _}}} ->
        raise("independent peer exceeded line bound")

      {^port, {:exit_status, 0}} when not is_nil(result) ->
        result

      {^port, {:exit_status, status}} ->
        raise("independent #{role} peer exited #{status} before successful completion")

      :stop ->
        if Port.info(port), do: Port.command(port, "STOP\n")
        peer_events(port, parent, role, now() + 6_000, result)
    after
      max(0, deadline - now()) -> raise("independent #{role} peer exceeded role deadline")
    end
  end

  defp broker_role(:mqtt), do: :mqtt_broker
  defp broker_role(:amqp), do: :amqp_broker

  defp ready(ready, deadline) do
    if MapSet.size(ready) == length(@roles) do
      :ok
    else
      receive do
        {:ready, pid} ->
          ready(MapSet.put(ready, pid), deadline)

        {:os_peer, _pid, os_pid} ->
          Process.put(:soak_os_pids, [os_pid | Process.get(:soak_os_pids)])
          ready(ready, deadline)

        {:failed, _pid, role, reason} ->
          raise("#{role} setup failed: #{reason}")

        {:DOWN, _monitor, :process, _pid, reason} ->
          raise("soak worker ended during setup: #{inspect(reason)}")
      after
        max(0, deadline - now()) -> raise("four-role setup timeout")
      end
    end
  end

  defp collect(
         broker,
         config,
         baseline,
         samples,
         progress,
         results,
         started,
         deadline,
         next_sample
       ) do
    cond do
      map_size(results) == length(@roles) ->
        results

      now() >= deadline ->
        raise("soak overall deadline exceeded")

      now() >= next_sample ->
        sample = resources()
        true = sample.beam_bytes <= baseline.beam_bytes + 134_217_728
        true = sample.processes <= baseline.processes + 256
        true = sample.fds <= baseline.fds + 64
        true = sample.ports <= baseline.ports + 64
        true = sample.rss_bytes <= 536_870_912
        true = sample.mailbox <= 512
        status = broker_status(broker, config.address)
        true = status.mqtt.pending_deliveries <= 8 and status.mqtt.prepared <= 8
        true = status.amqp.queue.messages <= 8 and status.amqp.queue.inflight <= 4

        record(samples, %{
          event: :sample,
          elapsed_ms: now() - started,
          resources: sample,
          progress: progress,
          broker: status
        })

        collect(
          broker,
          config,
          baseline,
          samples,
          progress,
          results,
          started,
          deadline,
          now() + 1000
        )

      true ->
        receive do
          {:progress, pid, role, count} ->
            info = Process.info(pid, [:memory, :message_queue_len]) || []
            if info != [], do: true = Keyword.fetch!(info, :message_queue_len) <= 64
            entry = %{progress: count, worker: Map.new(info)}

            collect(
              broker,
              config,
              baseline,
              samples,
              Map.put(progress, role, entry),
              results,
              started,
              deadline,
              next_sample
            )

          {:done, _pid, role, result} ->
            collect(
              broker,
              config,
              baseline,
              samples,
              progress,
              Map.put(results, role, result),
              started,
              deadline,
              next_sample
            )

          {:failed, _pid, role, reason} ->
            raise("#{role} failed: #{reason}")

          {:DOWN, _monitor, :process, _pid, :normal} ->
            collect(
              broker,
              config,
              baseline,
              samples,
              progress,
              results,
              started,
              deadline,
              next_sample
            )

          {:DOWN, _monitor, :process, _pid, reason} ->
            raise("soak worker failed: #{inspect(reason)}")
        after
          min(250, max(0, next_sample - now())) ->
            collect(
              broker,
              config,
              baseline,
              samples,
              progress,
              results,
              started,
              deadline,
              next_sample
            )
        end
    end
  end

  defp broker_status(broker, address) do
    {:ok, mqtt} = GravitonMQ.status(broker)
    {:ok, amqp} = GravitonMQ.amqp10_status(broker)

    %{
      mqtt: Map.take(mqtt, [:connections, :sessions, :pending_deliveries, :prepared, :retained]),
      amqp: %{
        connections: amqp.connections,
        operations: amqp.operations,
        links: amqp.links,
        queue: Map.take(Map.fetch!(amqp.queues, address), [:messages, :ready, :inflight, :bytes])
      }
    }
  end

  defp drained?(broker, address) do
    state = broker_status(broker, address)

    state.mqtt.connections == 0 and state.mqtt.pending_deliveries == 0 and
      state.mqtt.prepared == 0 and
      state.amqp.connections == 0 and state.amqp.queue.messages == 0 and
      state.amqp.queue.inflight == 0
  end

  defp resources do
    status = File.read!("/proc/self/status")
    [_, rss] = Regex.run(~r/^VmRSS:\s+(\d+)/m, status)

    %{
      beam_bytes: :erlang.memory(:total),
      processes: :erlang.system_info(:process_count),
      ports: :erlang.system_info(:port_count),
      fds: length(File.ls!("/proc/self/fd")),
      rss_bytes: String.to_integer(rss) * 1024,
      mailbox: Process.info(self(), :message_queue_len) |> elem(1)
    }
  end

  defp cleanup do
    workers = Process.get(:soak_workers, [])
    for worker <- workers, Process.alive?(worker.pid), do: send(worker.pid, :stop)
    deadline = now() + 8_000

    for worker <- workers do
      wait_stopped(worker.pid, deadline)
      if Process.alive?(worker.pid), do: Process.exit(worker.pid, :kill)
    end

    broker = Process.get(:soak_broker)
    if is_pid(broker) and Process.alive?(broker), do: Supervisor.stop(broker, :normal, 5_000)

    %{
      workers_stopped: Enum.all?(workers, &(not Process.alive?(&1.pid))),
      broker_stopped: is_nil(broker) or not Process.alive?(broker),
      python_peers_stopped:
        Enum.all?(Process.get(:soak_os_pids, []), &(not File.exists?("/proc/#{&1}")))
    }
  end

  defp wait_stopped(pid, deadline) do
    if Process.alive?(pid) and now() < deadline do
      Process.sleep(20)
      wait_stopped(pid, deadline)
    end
  end

  defp stop_client(client) do
    if Process.alive?(client), do: GenServer.stop(client, :normal, 5_000)
  end

  defp eventually(predicate, deadline) do
    unless predicate.() do
      if now() >= deadline, do: raise("soak drain deadline exceeded")
      Process.sleep(10)
      eventually(predicate, deadline)
    end
  end

  defp env_integer(name, default),
    do: System.get_env(name, to_string(default)) |> String.to_integer()

  defp now, do: System.monotonic_time(:millisecond)
  defp record(path, event), do: File.write!(path, json(event) <> "\n", [:append])
  defp json(value), do: value |> portable() |> :json.encode() |> IO.iodata_to_binary()

  defp portable(value) when is_map(value),
    do: Map.new(value, fn {key, child} -> {to_string(key), portable(child)} end)

  defp portable(value) when is_list(value), do: Enum.map(value, &portable/1)
  defp portable(nil), do: :null
  defp portable(value) when is_boolean(value), do: value
  defp portable(value) when is_atom(value), do: Atom.to_string(value)
  defp portable(value), do: value
end

GravitonMQ.Interop.Soak.run(System.argv())
