[directory] = System.argv()
{:ok, _} = Application.ensure_all_started(:ssl)

{:ok, instance} =
  GravitonMQ.start_link(
    name: nil,
    runtime_supervisor_name: nil,
    mqtt: [
      auth: :anonymous,
      host_principal: :host,
      persistence: [directory: directory, stream_id: "mqtt-vm"],
      listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
    ]
  )

{:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
IO.puts("READY #{System.pid()} #{port}")

Stream.repeatedly(fn -> IO.gets("") end)
|> Enum.reduce_while(nil, fn
  :eof, _ ->
    {:halt, nil}

  "PUBLISH " <> command, _ ->
    [qos, topic, payload] = String.split(String.trim(command), " ")

    {:ok, reference} =
      GravitonMQ.publish(instance, topic, payload, qos: String.to_integer(qos), retain: false)

    receive do
      {:graviton_mq, _, {:operation, ^reference, {:ok, {:accepted_durable, durable}}}} ->
        IO.puts("DURABLE #{topic} #{durable.position}")

      {:graviton_mq, _, {:operation, ^reference, failure}} ->
        IO.inspect(failure, label: "FAIL")
        System.halt(2)
    after
      5_000 -> System.halt(3)
    end

    {:cont, nil}

  _, _ ->
    {:cont, nil}
end)

Supervisor.stop(instance)
