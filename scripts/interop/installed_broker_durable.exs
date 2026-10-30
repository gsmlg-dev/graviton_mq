# Run inside a clean consumer using unpacked Hex archives, never umbrella beams.
root = Path.join(File.cwd!(), "installed-broker-durability")
File.mkdir!(root)

options = [
  name: nil,
  runtime_supervisor_name: nil,
  mqtt: [
    auth: :anonymous,
    host_principal: :archive_host,
    persistence: [directory: Path.join(root, "mqtt"), stream_id: "archive-mqtt"],
    listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
  ],
  amqp10: [
    container_id: "archive-durable",
    auth: :anonymous,
    queues: ["archive-durable"],
    persistence: [directory: Path.join(root, "amqp"), stream_id: "archive-amqp"],
    listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
  ]
]

peer = fn broker, mode ->
  if python = System.get_env("GRAVITON_PROTON_PYTHON") do
    {:ok, %{profile: :local_durable, listeners: [%{port: port}]}} =
      GravitonMQ.amqp10_status(broker)

    code = """
    import sys, proton
    from proton import Message
    from proton.reactor import AtLeastOnce
    from proton.utils import BlockingConnection
    assert proton.VERSION == (0, 40, 0)
    connection = BlockingConnection(sys.argv[1], timeout=5, sasl_enabled=False)
    try:
        if sys.argv[2] == 'send':
            sender = connection.create_sender('archive-durable', options=AtLeastOnce())
            sender.send(Message(body=b'installed-durable', durable=True), timeout=5)
        else:
            receiver = connection.create_receiver('archive-durable', options=AtLeastOnce(), credit=1)
            message = receiver.receive(timeout=5)
            assert message.durable and bytes(message.body) == b'installed-durable'
            receiver.accept()
    finally:
        connection.close()
    """

    {_, 0} =
      System.cmd(python, ["-c", code, "amqp://127.0.0.1:#{port}", mode], stderr_to_stdout: true)
  end
end

{:ok, first} = GravitonMQ.start_link(options)
{:ok, %{owner: owner, profile: :local_durable}} = GravitonMQ.status(first)

{:ok, publication} =
  GravitonMQ.publish(first, "installed/retained", "durable", qos: 1, retain: true)

receive do
  {:graviton_mq, ^owner, {:operation, ^publication, {:ok, {:accepted_durable, boundary}}}} ->
    true = is_struct(boundary, GravitonMQ.Core.CommitRef)
after
  5_000 -> raise "installed MQTT publication did not reach durable-through"
end

peer.(first, "send")
:ok = Supervisor.stop(first)
{:ok, recovered} = GravitonMQ.start_link(options)
{:ok, %{owner: owner, profile: :local_durable, retained: 1}} = GravitonMQ.status(recovered)
{:ok, operation} = GravitonMQ.subscribe(recovered, "installed/#", qos: 1)

subscription =
  receive do
    {:graviton_mq, ^owner, {:operation, ^operation, {:ok, subscription}}} -> subscription
  after
    5_000 -> raise "installed recovered MQTT subscription did not complete"
  end

:ok = GravitonMQ.request(recovered, subscription, 1)

receive do
  {:graviton_mq, ^owner,
   {:delivery, ^subscription, delivery, %{payload: "durable", retain: true}}} ->
    :ok = GravitonMQ.ack(recovered, delivery)
after
  5_000 -> raise "installed MQTT retained content did not recover"
end

peer.(recovered, "receive")
:ok = Supervisor.stop(recovered)
{:ok, final} = GravitonMQ.start_link(options)
{:ok, %{retained: 1}} = GravitonMQ.status(final)
{:ok, %{queues: %{"archive-durable" => %{messages: 0}}}} = GravitonMQ.amqp10_status(final)
:ok = Supervisor.stop(final)

IO.puts("Installed MQTT durable-through/retained recovery PASS")

IO.puts(
  if System.get_env("GRAVITON_PROTON_PYTHON"),
    do: "Installed AMQP durable Proton publication/recovery/retirement PASS",
    else: "Installed AMQP independent durable exchange NOT RUN (set GRAVITON_PROTON_PYTHON)"
)
