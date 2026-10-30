Code.require_file("../../../graviton_mq_client/test/support/peer_process.exs", __DIR__)

defmodule GravitonMQ.AMQP10.G5ClauseTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Client.Test.PeerProcess

  @moduletag :proton_interop
  @moduletag timeout: 30_000
  @peer Path.expand("../support/amqp_clause_peer.py", __DIR__)

  test "Proton active names are exclusive across Sessions/connections but release and isolate correctly" do
    left = broker!()
    right = broker!()

    {output, status} =
      System.cmd(python(), [@peer, "names", url(left), url(right)], stderr_to_stdout: true)

    assert status == 0, output

    for field <- [
          "cross_session",
          "cross_connection",
          "opposite_direction",
          "different_container",
          "instance_isolation",
          "reuse",
          "owner_down"
        ] do
      assert output =~ "\"#{field}\": true"
    end

    assert {:ok, %{queues: %{"orders" => %{messages: 0}}}} = GravitonMQ.amqp10_status(left)
    assert {:ok, %{queues: %{"orders" => %{messages: 0}}}} = GravitonMQ.amqp10_status(right)
  end

  test "Proton durable publication with no Rejected outcome receives precondition-failed Detach" do
    broker = broker!()

    {output, status} =
      System.cmd(python(), [@peer, "durable", url(broker)], stderr_to_stdout: true)

    assert status == 0, output
    assert output =~ "\"durable_condition\": true"
    assert output =~ "\"healthy_link\": true"
    assert {:ok, %{queues: %{"orders" => %{messages: 0}}}} = GravitonMQ.amqp10_status(broker)
  end

  test "independent Proton values enforce exact handle-max and errored-Link wire conditions" do
    broker = broker!()

    {output, status} =
      System.cmd(python(), [@peer, "error-boundaries", url(broker)], stderr_to_stdout: true)

    assert status == 0, output
    assert output =~ "\"handle_limit\": true"
    assert output =~ "\"ending_session_discard\": true"
    assert output =~ "\"healthy_session\": true"
    assert output =~ "\"connection_validation\": true"

    for field <- ["flow", "transfer", "attach-same-handle", "attach-new-handle", "disposition"],
        do: assert(output =~ field)

    eventually!(fn ->
      match?({:ok, %{connections: 0, links: 0}}, GravitonMQ.amqp10_status(broker))
    end)

    assert {:ok, %{queues: %{"orders" => %{messages: 0}}}} = GravitonMQ.amqp10_status(broker)
  end

  test "a suspended Connection retains its name until queued Detach actually retires the endpoint" do
    broker = broker!()

    supervisor =
      broker
      |> child(GravitonMQ.Runtime.Supervisor)
      |> child(GravitonMQ.Runtime.AMQP10.Supervisor)

    manager = child(supervisor, GravitonMQ.Runtime.AMQP10.Broker)

    peer =
      start_supervised!(
        {PeerProcess, executable: python(), args: [@peer, "retiring", url(broker)]}
      )

    eventually!(fn -> PeerProcess.snapshot(peer).output =~ "\"ready\": true" end)
    [{_, connection, _, _}] = DynamicSupervisor.which_children(child(supervisor, :connections))

    :sys.replace_state(manager, fn state ->
      Process.flag(:sensitive, false)
      state
    end)

    :ok = :sys.suspend(manager)

    try do
      true = PeerProcess.command(peer, "publish\n")
      eventually!(fn -> endpoint_event?(manager, connection) end)
      :ok = :sys.suspend(connection)
      :ok = :sys.resume(manager)
      eventually!(fn -> match?({:ok, %{operations: 1}}, GravitonMQ.amqp10_status(broker)) end)
      :ok = :sys.suspend(manager)
      :ok = :sys.resume(connection)
      eventually!(fn -> endpoint_event?(manager, connection) end)
      :ok = :sys.suspend(connection)
      :ok = :sys.resume(manager)

      eventually!(fn ->
        match?({:ok, %{links: 0, operations: 0}}, GravitonMQ.amqp10_status(broker))
      end)

      {output, status} =
        System.cmd(python(), [@peer, "retirement-probe", url(broker), "refuse"],
          stderr_to_stdout: true
        )

      assert status == 0, output
      assert output =~ "\"refused\": true"
    after
      for owner <- [manager, connection] do
        if Process.alive?(owner), do: :sys.resume(owner)
      end
    end

    eventually!(fn -> PeerProcess.snapshot(peer).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(peer)
    assert output =~ "\"detached\": true"

    {output, status} =
      System.cmd(python(), [@peer, "retirement-probe", url(broker), "reuse"],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "\"reused\": true"
  end

  defp endpoint_event?(manager, connection) do
    {:messages, messages} = Process.info(manager, :messages)
    Enum.any?(messages, &match?({:amqp10_endpoint_event, ^connection, _, _}, &1))
  end

  defp child(supervisor, identity) do
    {_, owner, _, _} = List.keyfind(Supervisor.which_children(supervisor), identity, 0)
    owner
  end

  test "independent client owners arbitrate names before wire emission and release only their owner" do
    {peer, port} = server!("same-peer")
    {other_peer, other_port} = server!("different-peer")
    first = client!(port)
    second = client!(port)
    different = client!(other_port)
    attach!(first, 1, "same", :sender)

    assert {:error, :attach_not_admitted} =
             Client.attach(second, 1, "same", :sender, address: Value.string("orders"))

    assert length(Regex.scan(~r/"attached": "same"/, PeerProcess.snapshot(peer).output)) == 1
    opposite = attach!(second, 1, "same", :receiver)
    assert {:ok, request} = Client.request(second, 1, opposite, 1, 1_048_576)
    assert_receive {:graviton_mq, ^second, {:operation, ^request, {:ok, 1}}}, 5_000
    assert_receive {:graviton_mq, ^second, {:delivery, token, 1, ^opposite, _, _}}, 5_000
    assert {:ok, operation} = Client.settle(second, token, %Outcome.Accepted{})

    assert_receive {:graviton_mq, ^second, {:operation, ^operation, {:ok, %Outcome.Accepted{}}}},
                   5_000

    attach!(different, 1, "same", :sender)
    send!(first, "same")
    monitor = Process.monitor(first)
    Process.unlink(first)
    Process.exit(first, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^first, :killed}, 5_000

    eventually!(
      fn -> Client.attach(second, 1, "same", :sender, address: Value.string("orders")) end,
      fn result -> match?({:ok, _}, result) end
    )

    assert_receive {:graviton_mq, ^second,
                    {:operation, _, {:ok, {%Value{value: "same"}, :sender}}}},
                   5_000

    send!(second, "same")
    assert {:ok, operation} = Client.detach(second, 1, {Value.string("same"), :sender})
    assert_receive {:graviton_mq, ^second, {:operation, ^operation, {:ok, _}}}, 5_000
    attach!(second, 1, "same", :sender)
    send!(second, "same")
    :ok = GenServer.stop(second)
    :ok = GenServer.stop(different)
    eventually!(fn -> PeerProcess.snapshot(peer).exit_status != nil end)
    eventually!(fn -> PeerProcess.snapshot(other_peer).exit_status != nil end)
    assert %{exit_status: 0} = PeerProcess.snapshot(peer)
    assert %{exit_status: 0} = PeerProcess.snapshot(other_peer)
  end

  defp broker! do
    start_supervised!(
      Supervisor.child_spec(
        {GravitonMQ,
         name: nil,
         runtime_supervisor_name: nil,
         amqp10: [
           container_id: "clause-broker-#{System.unique_integer([:positive])}",
           auth: :anonymous,
           queues: ["orders"],
           listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
         ]},
        id: make_ref()
      )
    )
  end

  defp url(broker) do
    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    "amqp://127.0.0.1:#{port}"
  end

  defp python, do: System.fetch_env!("GRAVITON_PROTON_PYTHON")

  defp server!(container) do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1}, active: false)
    {:ok, {_, port}} = :inet.sockname(socket)
    :gen_tcp.close(socket)

    peer =
      start_supervised!(
        {PeerProcess,
         executable: python(), args: [@peer, "server", "amqp://127.0.0.1:#{port}", container]},
        id: make_ref()
      )

    eventually!(fn -> PeerProcess.snapshot(peer).output =~ "\"ready\": true" end)
    {peer, port}
  end

  defp client!(port) do
    client =
      start_supervised!(
        Supervisor.child_spec(
          {Client, container_id: "same-local", host: "127.0.0.1", port: port, observer: self()},
          id: make_ref(),
          restart: :temporary
        )
      )

    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    assert {:ok, operation} = Client.open_session(client, 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, 1}}}, 5_000
    client
  end

  defp attach!(client, session, name, role) do
    assert {:ok, operation} =
             Client.attach(client, session, name, role, address: Value.string("orders"))

    identity = {Value.string(name), role}
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^identity}}}, 5_000
    identity
  end

  defp send!(client, name) do
    assert {:ok, raw} = Message.encode_sections([{:data, Value.binary("body")}])
    assert {:ok, operation} = Client.send_message(client, 1, {Value.string(name), :sender}, raw)

    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, %Outcome.Accepted{}}}},
                   5_000
  end

  defp eventually!(fun), do: eventually!(fun, & &1)

  defp eventually!(fun, predicate),
    do: until!(fun, predicate, System.monotonic_time(:millisecond) + 8_000)

  defp until!(fun, predicate, deadline) do
    result = fun.()

    if predicate.(result),
      do: result,
      else:
        if(System.monotonic_time(:millisecond) >= deadline,
          do: flunk("peer condition deadline"),
          else:
            (
              Process.sleep(10)
              until!(fun, predicate, deadline)
            )
        )
  end
end
