Code.require_file("../../../graviton_mq_client/test/support/peer_process.exs", __DIR__)

defmodule GravitonMQ.AMQP10.G5LateSettlementTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Client.Test.PeerProcess
  alias GravitonMQ.Runtime.AMQP10.{Broker, Queue}

  @moduletag :proton_interop
  @moduletag timeout: 30_000
  @peer Path.expand("../support/amqp_late_settlement_peer.py", __DIR__)

  test "Proton late sender settlement preserves broker admission and a second Link" do
    broker =
      start_supervised!(
        Supervisor.child_spec(
          {GravitonMQ,
           name: nil,
           runtime_supervisor_name: nil,
           amqp10: [
             container_id: "late-settlement-#{System.unique_integer([:positive])}",
             auth: :anonymous,
             queues: ["orders"],
             listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
           ]},
          id: make_ref()
        )
      )

    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    peer = peer!("broker", port)
    eventually!(fn -> PeerProcess.snapshot(peer).output =~ "\"ready\": true" end)
    runtime = child(broker, GravitonMQ.Runtime.Supervisor)
    amqp = child(runtime, GravitonMQ.Runtime.AMQP10.Supervisor)
    manager = child(amqp, Broker)
    queue = child(amqp, {Queue, "orders"})
    connections = child(amqp, :connections)
    :sys.suspend(manager)

    try do
      assert PeerProcess.command(peer, "send\n")
      eventually!(fn -> PeerProcess.snapshot(peer).output =~ "\"sender_settled\": true" end)
      # The connection processed both wire frames while queue admission waited
      # for its actual manager. Peer settlement cannot drop host responsibility.
      assert DynamicSupervisor.count_children(connections).active == 1
      assert %{messages: 0} = Queue.status(queue)
    after
      :sys.resume(manager)
    end

    assert PeerProcess.command(peer, "continue\n")
    eventually!(fn -> PeerProcess.snapshot(peer).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(peer)
    assert output =~ "\"host_responsibility_preserved\": true"
    assert output =~ "\"second_link\": true"
    eventually!(fn -> Queue.status(queue).messages == 0 end)
  end

  test "public client retains a host token after Proton settles and receives on a second Link" do
    {:ok, reservation} = :gen_tcp.listen(0, ip: {127, 0, 0, 1}, active: false)
    {:ok, {_ip, port}} = :inet.sockname(reservation)
    :ok = :gen_tcp.close(reservation)
    peer = peer!("client", port)
    eventually!(fn -> PeerProcess.snapshot(peer).output =~ "\"ready\": true" end)

    client =
      start_supervised!(
        {Client,
         container_id: "late-settlement-client", host: "127.0.0.1", port: port, observer: self()}
      )

    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    assert {:ok, operation} = Client.open_session(client, 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, 1}}}, 5_000
    first = attach!(client, "first")
    request!(client, first)
    assert_receive {:graviton_mq, ^client, {:delivery, token, 1, ^first, raw, _}}, 5_000
    assert {:ok, decoded, <<>>} = Message.decode(raw)
    assert {:amqp_value, Value.binary("first")} in decoded.sections
    eventually!(fn -> PeerProcess.snapshot(peer).output =~ "\"sender_settled\": true" end)
    assert Client.status(client).phase == :active
    settle!(client, token)
    # Host ownership ends exactly once even though the peer already forgot it.
    assert {:error, :settlement_not_admitted} = Client.settle(client, token, %Outcome.Accepted{})
    second = attach!(client, "second")
    request!(client, second)
    assert_receive {:graviton_mq, ^client, {:delivery, token, 1, ^second, raw, _}}, 5_000
    assert {:ok, decoded, <<>>} = Message.decode(raw)
    assert {:amqp_value, Value.binary("second")} in decoded.sections
    settle!(client, token)
    assert Client.status(client).phase == :active
    :ok = GenServer.stop(client)
    eventually!(fn -> PeerProcess.snapshot(peer).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(peer)
    assert output =~ "\"second_link\": true"
  end

  test "Proton receiver settlement without an outcome requeues broker responsibility and preserves its Session" do
    broker =
      start_supervised!(
        Supervisor.child_spec(
          {GravitonMQ,
           name: nil,
           runtime_supervisor_name: nil,
           amqp10: [
             container_id: "receiver-settlement-broker",
             auth: :anonymous,
             queues: ["orders"],
             listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
           ]},
          id: make_ref()
        )
      )

    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    peer = peer!("broker-receiver", port)
    eventually!(fn -> PeerProcess.snapshot(peer).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(peer)
    assert output =~ "\"responsibility_requeued\": true"
    assert output =~ "\"second_link\": true"
    assert {:ok, %{queues: %{"orders" => %{messages: 0}}}} = GravitonMQ.amqp10_status(broker)
  end

  test "public sender reports unknown without guessing an outcome and continues a second Link" do
    {:ok, reservation} = :gen_tcp.listen(0, ip: {127, 0, 0, 1}, active: false)
    {:ok, {_ip, port}} = :inet.sockname(reservation)
    :ok = :gen_tcp.close(reservation)
    peer = peer!("client-sender", port)
    eventually!(fn -> PeerProcess.snapshot(peer).output =~ "\"ready\": true" end)

    client =
      start_supervised!(
        {Client,
         container_id: "receiver-settlement-client",
         host: "127.0.0.1",
         port: port,
         observer: self()}
      )

    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    assert {:ok, operation} = Client.open_session(client, 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, 1}}}, 5_000
    first = attach!(client, "first", :sender)
    second = attach!(client, "second", :sender)
    assert {:ok, raw} = Message.encode_sections([{:data, Value.binary("first")}])
    assert {:ok, operation} = Client.send_message(client, 1, first, raw)

    assert_receive {:graviton_mq, ^client,
                    {:operation, ^operation, {:unknown, :receiver_settled_without_outcome}}},
                   5_000

    refute_receive {:graviton_mq, ^client, {:operation, ^operation, _}}, 30
    assert Client.status(client).phase == :active
    assert {:ok, raw} = Message.encode_sections([{:data, Value.binary("second")}])
    assert {:ok, operation} = Client.send_message(client, 1, second, raw)

    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, %Outcome.Accepted{}}}},
                   5_000

    assert Client.status(client).phase == :active
    :ok = GenServer.stop(client)
    eventually!(fn -> PeerProcess.snapshot(peer).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(peer)
    assert output =~ "\"second_link\": true"
  end

  defp peer!(mode, port) do
    start_supervised!(
      {PeerProcess,
       executable: System.fetch_env!("GRAVITON_PROTON_PYTHON"),
       args: [@peer, mode, "amqp://127.0.0.1:#{port}"]},
      id: make_ref()
    )
  end

  defp attach!(client, name, role \\ :receiver) do
    assert {:ok, operation} =
             Client.attach(client, 1, name, role, address: Value.string("q"))

    identity = {Value.string(name), role}
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^identity}}}, 5_000
    identity
  end

  defp request!(client, identity) do
    assert {:ok, operation} = Client.request(client, 1, identity, 1, 1_048_576)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, 1}}}, 5_000
  end

  defp settle!(client, token) do
    assert {:ok, operation} = Client.settle(client, token, %Outcome.Accepted{})

    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, %Outcome.Accepted{}}}},
                   5_000
  end

  defp child(supervisor, id) do
    {^id, pid, _, _} = List.keyfind(Supervisor.which_children(supervisor), id, 0)
    pid
  end

  defp eventually!(predicate), do: until!(predicate, System.monotonic_time(:millisecond) + 7_000)

  defp until!(predicate, deadline) do
    if predicate.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("peer condition did not complete before deadline")
      else
        Process.sleep(10)
        until!(predicate, deadline)
      end
    end
  end
end
