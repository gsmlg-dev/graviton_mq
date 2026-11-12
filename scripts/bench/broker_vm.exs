# Broker VM for scripts/bench/run.exs (S5a). Runs without Mix:
#   elixir -pa <ebin>... scripts/bench/broker_vm.exs <mqtt|amqp> <memory|durable> <directory> <address>
# Prints "READY <os_pid> <mqtt_port> <amqp_port>" (0 = protocol not enabled),
# answers each "STATUS" stdin line with "STATUS <json>", and stops on stdin EOF.
[protocol, storage, directory, address] = System.argv()
true = protocol in ["mqtt", "amqp"] and storage in ["memory", "durable"]
{:ok, _} = Application.ensure_all_started(:graviton_mq_runtime)
:ok = Logger.configure(level: :warning)
listeners = [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]

mqtt =
  [
    auth: :anonymous,
    listeners: listeners,
    max_connections: 256,
    max_sessions: 256,
    max_inflight: 1_024,
    max_pending_deliveries: 262_144,
    max_pending_bytes: 268_435_456,
    max_session_bytes: 268_435_456
  ] ++
    if(storage == "durable",
      do: [persistence: [directory: Path.join(directory, "mqtt"), stream_id: "s5a-bench-mqtt"]],
      else: []
    )

amqp10 =
  [
    container_id: "s5a-bench",
    auth: :anonymous,
    queues: [address],
    listeners: listeners,
    max_connections: 256,
    max_auth_jobs: 128,
    queue_options: [max_messages: 1_000_000, max_bytes: 1_073_741_824]
  ] ++
    if(storage == "durable",
      do: [persistence: [directory: Path.join(directory, "amqp"), stream_id: "s5a-bench-amqp"]],
      else: []
    )

protocol_options = if protocol == "mqtt", do: [mqtt: mqtt], else: [amqp10: amqp10]

{:ok, instance} =
  GravitonMQ.start_link([name: nil, runtime_supervisor_name: nil] ++ protocol_options)

status = fn ->
  if protocol == "mqtt" do
    {:ok, status} = GravitonMQ.status(instance)

    Map.take(status, [:profile, :storage, :connections, :sessions, :pending_deliveries, :prepared])
  else
    {:ok, status} = GravitonMQ.amqp10_status(instance)

    queue =
      case Map.fetch!(status.queues, address) do
        %{} = queue -> Map.take(queue, [:messages, :ready, :inflight])
        state -> %{state: state}
      end

    %{profile: status.profile, connections: status.connections, queue: queue}
  end
end

port = fn
  "mqtt" ->
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    port

  "amqp" ->
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(instance)
    port
end

{mqtt_port, amqp_port} =
  if protocol == "mqtt", do: {port.("mqtt"), 0}, else: {0, port.("amqp")}

IO.puts("READY #{System.pid()} #{mqtt_port} #{amqp_port}")

Stream.repeatedly(fn -> IO.gets("") end)
|> Enum.reduce_while(:running, fn
  "STATUS\n", state ->
    IO.puts("STATUS " <> IO.iodata_to_binary(:json.encode(status.())))
    {:cont, state}

  line, state when is_binary(line) ->
    {:cont, state}

  _eof_or_error, state ->
    {:halt, state}
end)

:ok = Supervisor.stop(instance)
File.rm_rf!(directory)
