Code.require_file("../../../graviton_mq_client/test/support/peer_process.exs", __DIR__)

defmodule GravitonMQ.AMQP10.ProtonTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.Client.Test.PeerProcess
  alias GravitonMQ.Runtime.AMQP10.{Broker, Queue}

  @moduletag :proton_interop
  @moduletag :tmp_dir
  @moduletag timeout: 30_000
  @peer Path.expand("../../../graviton_mq_client/test/support/amqp_peer.py", __DIR__)

  setup_all do
    %{python: System.fetch_env!("GRAVITON_PROTON_PYTHON")}
  end

  test "Proton sends and receives a large message through our embedded memory broker", context do
    broker = broker!()
    url = url(broker)
    body = :binary.copy(<<1, 0, 255, 4>>, 25_000)
    path = body_file(context, body)
    receiver = receiver!(context, url, "orders", path)
    publish!(context, url, "orders", path)
    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(receiver)
    assert output =~ digest(body)
    eventually!(fn -> queue(broker).messages == 0 end)
  end

  test "zero credit retains responsibility in the queue until the peer grants one", context do
    broker = broker!()
    url = url(broker)
    path = body_file(context, "bounded-credit")
    receiver = receiver!(context, url, "orders", path, ["--gate"])
    publish!(context, url, "orders", path)
    assert queue(broker).ready == 1
    assert queue(broker).inflight == 0
    refute PeerProcess.snapshot(receiver).output =~ "received_bytes"
    assert true = PeerProcess.command(receiver, "grant\n")
    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0} = PeerProcess.snapshot(receiver)
    eventually!(fn -> queue(broker).messages == 0 end)
  end

  test "Released is redelivered before Accepted retires the actual message", context do
    broker = broker!()
    url = url(broker)
    path = body_file(context, "release-must-redeliver")
    receiver = receiver!(context, url, "orders", path, [], "release")
    publish!(context, url, "orders", path)
    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(receiver)
    assert output =~ digest("release-must-redeliver")
    eventually!(fn -> queue(broker).messages == 0 end)
  end

  test "a disconnected unsettled receiver returns responsibility to the queue", context do
    broker = broker!()
    url = url(broker)
    path = body_file(context, "disconnect-redelivery")
    first = receiver!(context, url, "orders", path, [], "receive-hold")
    publish!(context, url, "orders", path)
    eventually!(fn -> PeerProcess.snapshot(first).output =~ "unsettled_bytes" end)
    assert queue(broker).inflight == 1
    assert true = PeerProcess.command(first, "disconnect\n")
    eventually!(fn -> PeerProcess.snapshot(first).exit_status != nil end)
    eventually!(fn -> queue(broker).ready == 1 end)
    second = receiver!(context, url, "orders", path)
    eventually!(fn -> PeerProcess.snapshot(second).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(second)
    assert output =~ digest("disconnect-redelivery")
    eventually!(fn -> queue(broker).messages == 0 end)
  end

  test "memory broker refuses durable publications and unknown queue attachment", context do
    broker = broker!()
    path = body_file(context, "cannot-promise-disk-durability")
    {output, status} = proton(context, "send", url(broker), "orders", path, ["--durable"])
    assert status != 0, output
    assert queue(broker).messages == 0
    {output, status} = proton(context, "send", url(broker), "missing", path)
    assert status != 0, output
    assert queue(broker).messages == 0
  end

  test "a final Proton Accepted survives connection exit while the Manager is suspended",
       context do
    broker = broker!()
    path = body_file(context, "final-outcome-before-down")
    receiver = receiver!(context, url(broker), "orders", path, [], "receive-hold")
    publish!(context, url(broker), "orders", path)
    eventually!(fn -> PeerProcess.snapshot(receiver).output =~ "unsettled_bytes" end)
    runtime = child(broker, GravitonMQ.Runtime.Supervisor)
    amqp = child(runtime, GravitonMQ.Runtime.AMQP10.Supervisor)
    manager = child(amqp, Broker)
    queue_owner = child(amqp, {Queue, "orders"})
    connections = child(amqp, :connections)
    assert Queue.status(queue_owner).inflight == 1
    :sys.suspend(manager)

    try do
      assert true = PeerProcess.command(receiver, "accept\n")
      eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
      assert %{exit_status: 0} = PeerProcess.snapshot(receiver)
      eventually!(fn -> DynamicSupervisor.count_children(connections).active == 0 end)
      # Queue must wait for its actual settlement coordinator, rather than
      # requeueing independently on the socket owner's DOWN signal.
      assert %{ready: 0, inflight: 1, messages: 1} = Queue.status(queue_owner)
    after
      :sys.resume(manager)
    end

    eventually!(fn -> Queue.status(queue_owner).messages == 0 end)
  end

  test "two embedded instances have actual independent listeners and queue state", context do
    left = broker!()
    right = broker!()
    refute url(left) == url(right)
    path = body_file(context, "only-left")
    publish!(context, url(left), "orders", path)
    assert queue(left).ready == 1
    assert queue(right).messages == 0
    receiver = receiver!(context, url(left), "orders", path)
    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0} = PeerProcess.snapshot(receiver)
    eventually!(fn -> queue(left).messages == 0 end)
    assert queue(right).messages == 0
  end

  test "Proton Modified is honored on the next delivery and Rejected discards", context do
    broker = broker!()
    path = body_file(context, "modified-by-independent-peer")
    receiver = receiver!(context, url(broker), "orders", path, [], "modified")
    {output, status} = proton(context, "send", url(broker), "orders", path, ["--annotated"])
    assert status == 0, output
    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(receiver)
    assert output =~ digest("modified-by-independent-peer")
    eventually!(fn -> queue(broker).messages == 0 end)
    receiver = receiver!(context, url(broker), "orders", path, [], "reject")
    publish!(context, url(broker), "orders", path)
    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0} = PeerProcess.snapshot(receiver)
    eventually!(fn -> queue(broker).messages == 0 end)
  end

  test "Proton exchanges over verified TLS and rejects a different trust root", context do
    certificate = Path.join(context.tmp_dir, "server.pem")
    key = Path.join(context.tmp_dir, "server.key")
    untrusted = Path.join(context.tmp_dir, "untrusted.pem")
    other_key = Path.join(context.tmp_dir, "untrusted.key")

    for {certificate, key} <- [{certificate, key}, {untrusted, other_key}] do
      {output, status} =
        System.cmd(
          "openssl",
          [
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            key,
            "-out",
            certificate,
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost"
          ],
          stderr_to_stdout: true
        )

      assert status == 0, output
    end

    broker =
      broker!([
        [
          transport: :tls,
          ip: {127, 0, 0, 1},
          port: 0,
          tls_options: [
            certfile: String.to_charlist(certificate),
            keyfile: String.to_charlist(key)
          ]
        ]
      ])

    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    url = "amqps://localhost:#{port}"
    path = body_file(context, "verified-tls")
    receiver = receiver!(context, url, "orders", path, ["--cafile", certificate])
    {output, status} = proton(context, "send", url, "orders", path, ["--cafile", certificate])
    assert status == 0, output
    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0} = PeerProcess.snapshot(receiver)
    {output, status} = proton(context, "send", url, "orders", path, ["--cafile", untrusted])
    assert status != 0, output
    eventually!(fn -> queue(broker).messages == 0 end)
  end

  defp broker!(listeners \\ [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]) do
    specification =
      Supervisor.child_spec(
        {GravitonMQ,
         name: nil,
         runtime_supervisor_name: nil,
         amqp10: [
           container_id: "graviton-#{System.unique_integer([:positive])}",
           auth: :anonymous,
           queues: ["orders"],
           endpoint_options: [max_frame_size: 512],
           listeners: listeners
         ]},
        id: make_ref()
      )

    broker = start_supervised!(specification)
    assert {:ok, %{profile: :memory, listeners: [_]}} = GravitonMQ.amqp10_status(broker)
    broker
  end

  defp url(broker) do
    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    "amqp://127.0.0.1:#{port}"
  end

  defp queue(broker) do
    assert {:ok, %{queues: %{"orders" => status}}} = GravitonMQ.amqp10_status(broker)
    status
  end

  defp child(supervisor, id) do
    {^id, pid, _, _} = List.keyfind(Supervisor.which_children(supervisor), id, 0)
    pid
  end

  defp body_file(context, body) do
    path = Path.join(context.tmp_dir, "body-#{System.unique_integer([:positive])}")
    File.write!(path, body)
    path
  end

  defp receiver!(context, url, address, path, options \\ [], operation \\ "receive") do
    receiver =
      start_supervised!(
        {PeerProcess,
         executable: context.python, args: [@peer, operation, url, address, path] ++ options},
        id: make_ref()
      )

    eventually!(fn ->
      snapshot = PeerProcess.snapshot(receiver)
      if snapshot.exit_status != nil, do: flunk(snapshot.output)
      snapshot.output =~ "\"ready\": true"
    end)

    receiver
  end

  defp publish!(context, url, address, path) do
    {output, status} = proton(context, "send", url, address, path)
    assert status == 0, output
  end

  defp proton(context, operation, url, address, path, options \\ []),
    do:
      System.cmd(context.python, [@peer, operation, url, address, path] ++ options,
        stderr_to_stdout: true
      )

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp eventually!(predicate), do: until!(predicate, System.monotonic_time(:millisecond) + 12_000)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline,
             "independent peer condition timed out"

      Process.sleep(10)
      until!(predicate, deadline)
    end
  end
end
