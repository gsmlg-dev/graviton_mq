Code.require_file("../support/amqp10_stand_in_queue.exs", __DIR__)

defmodule GravitonMQ.AMQP10.ManagerRequestsTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message, as: MessageCodec
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Runtime.AMQP10.{Broker, Listener, Queue}
  alias GravitonMQ.Test.{StandInAMQP, StandInQueue}

  # Taken-but-unanswered requests are classified at the 5 s deadline.
  @moduletag timeout: 30_000

  test "B4: a taken but unanswered settle is uncertain and fences only its queue" do
    %{manager: manager, stand_in: stand_in, port: port} = stand_in!()
    real = StandInQueue.queue(stand_in)
    publish!(port, "orders", "held-settle")
    {consumer, link, delivery} = consume!(port, "orders")
    bystander = client!(port)
    other = attach!(bystander, "other-publisher", :sender, "other")
    :ok = StandInQueue.hold(stand_in, :settle)
    assert {:ok, _} = operation(Client.settle(consumer, delivery, %Outcome.Accepted{}), consumer)
    assert_receive {:stand_in_held, ^stand_in, :settle}, 2_000

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(bystander, 0, other, message("other")), bystander, 500)

    assert %{queues: %{"orders" => :unresponsive}} = Broker.status(manager)
    assert_receive {:graviton_mq, ^consumer, {:session, 0, {:detached, ^link, error}}}, 7_000
    assert {:peer_detach, %{condition: %Value{value: "amqp:internal-error"}}} = error
    assert %{queues: %{"orders" => :unavailable}, queue_fences: 1} = Broker.status(manager)
    :ok = StandInQueue.release(stand_in)
    # The killed anchor returned the claim; the late settle met a stale lease.
    eventually!(fn ->
      match?(%{messages: 1, ready: 1, inflight: 0, consumers: 0}, Queue.status(real))
    end)
  end

  test "B4: a taken but unanswered claim is uncertain and fences its queue" do
    %{manager: manager, stand_in: stand_in, port: port} = stand_in!()
    real = StandInQueue.queue(stand_in)
    publish!(port, "orders", "held-claim")
    consumer = client!(port)
    link = attach!(consumer, "orders-consumer", :receiver, "orders")
    :ok = StandInQueue.hold(stand_in, :claim)
    assert {:ok, 1} = operation(Client.request(consumer, 0, link, 1, 1_048_576), consumer)
    assert_receive {:stand_in_held, ^stand_in, :claim}, 2_000
    assert_receive {:graviton_mq, ^consumer, {:session, 0, {:detached, ^link, _}}}, 7_000
    refute_received {:graviton_mq, ^consumer, {:delivery, _, _, _, _, _}}
    assert %{queue_fences: 1} = Broker.status(manager)
    :ok = StandInQueue.release(stand_in)

    eventually!(fn ->
      match?(%{messages: 1, ready: 1, inflight: 0, consumers: 0}, Queue.status(real))
    end)
  end

  test "B4: a taken but unanswered enqueue closes only the publishing Connection" do
    %{manager: manager, stand_in: stand_in, port: port} = stand_in!()
    real = StandInQueue.queue(stand_in)
    watcher = client!(port)
    watching = attach!(watcher, "orders-watcher", :receiver, "orders")
    publisher = client!(port)
    sender = attach!(publisher, "orders-publisher", :sender, "orders")
    :ok = StandInQueue.hold(stand_in, :enqueue)
    assert {:ok, sending} = Client.send_message(publisher, 0, sender, message("uncertain"))
    assert_receive {:stand_in_held, ^stand_in, :enqueue}, 2_000
    assert_receive {:graviton_mq, ^publisher, {:operation, ^sending, {:unknown, _}}}, 7_000
    assert_receive {:graviton_mq, ^publisher, {:disconnected, _}}, 2_000
    refute_received {:graviton_mq, ^publisher, {:operation, ^sending, {:ok, _}}}
    # No fence: the other Connection's Link on the same queue stays attached.
    refute_received {:graviton_mq, ^watcher, {:session, 0, {:detached, ^watching, _}}}
    assert Client.status(watcher).phase == :active
    :ok = StandInQueue.release(stand_in)
    assert %{queue_fences: 0, queues: %{"orders" => %{}}} = Broker.status(manager)
    # The documented unknown-outcome case: the message is in the queue.
    eventually!(fn -> Queue.status(real).messages == 1 end)
  end

  test "B4/D10: a fence Releases an enqueue it abandons before it detaches the publisher" do
    %{manager: manager, stand_in: stand_in, port: port} = stand_in!()
    real = StandInQueue.queue(stand_in)
    publish!(port, "orders", "held-settle")
    {consumer, _link, delivery} = consume!(port, "orders")
    publisher = client!(port)
    sender = attach!(publisher, "orders-publisher", :sender, "orders")
    :ok = StandInQueue.hold(stand_in, :settle)
    assert {:ok, _} = operation(Client.settle(consumer, delivery, %Outcome.Accepted{}), consumer)
    assert_receive {:stand_in_held, ^stand_in, :settle}, 2_000
    # The enqueue waits behind the held settle: never taken, so not executed.
    assert {:ok, sending} = Client.send_message(publisher, 0, sender, message("abandoned"))
    assert_receive {:stand_in_queued, ^stand_in, :enqueue}, 2_000

    # The settle's deadline fences the queue. The first of the two events in
    # the publisher's mailbox order must be the Released disposition.
    first =
      receive do
        {:graviton_mq, ^publisher, {:operation, ^sending, result}} -> {:operation, result}
        {:graviton_mq, ^publisher, {:session, 0, {:detached, ^sender, _}}} -> :detached
      after
        7_000 -> flunk("neither a disposition nor a Detach reached the publisher")
      end

    assert {:operation, {:ok, %Outcome.Released{}}} = first
    assert_receive {:graviton_mq, ^publisher, {:session, 0, {:detached, ^sender, _}}}, 2_000
    assert %{queue_fences: 1, queues: %{"orders" => :unavailable}} = Broker.status(manager)
    :ok = StandInQueue.release(stand_in)
    # The abandoned enqueue never executed: only the returned claim remains.
    eventually!(fn ->
      match?(%{messages: 1, ready: 1, inflight: 0, consumers: 0}, Queue.status(real))
    end)

    Process.sleep(200)
    assert %{messages: 1} = Queue.status(real)
  end

  test "B4: a restore request to a fenced queue ends at its deadline and re-probes" do
    %{manager: manager, stand_in: stand_in, port: port} = stand_in!()
    publish!(port, "orders", "restored")
    {consumer, link, delivery} = consume!(port, "orders")
    :ok = StandInQueue.hold(stand_in, :settle)
    assert {:ok, _} = operation(Client.settle(consumer, delivery, %Outcome.Accepted{}), consumer)
    assert_receive {:stand_in_held, ^stand_in, :settle}, 2_000
    assert_receive {:graviton_mq, ^consumer, {:session, 0, {:detached, ^link, _}}}, 7_000
    # The probe starts 1 s after the fence; hold the restore watch it leads to.
    :ok = StandInQueue.release(stand_in)
    :ok = StandInQueue.hold(stand_in, :watch)
    assert_receive {:stand_in_held, ^stand_in, :watch}, 3_000
    # The queue is already fenced: no second fence abandons the restore.
    assert %{queue_fences: 1, queues: %{"orders" => :unavailable}} = Broker.status(manager)
    # The taken restore watch is uncertain at its 5 s deadline; the Manager
    # re-probes (1 s later), and that probe waits behind the held watch.
    refute_receive {:stand_in_queued, ^stand_in, :status}, 4_500
    assert_receive {:stand_in_queued, ^stand_in, :status}, 3_000
    assert %{queue_fences: 1, queues: %{"orders" => :unavailable}} = Broker.status(manager)
    :ok = StandInQueue.release(stand_in)
    eventually!(fn -> match?(%{queues: %{"orders" => %{}}}, Broker.status(manager)) end, 8_000)
    assert %{queue_fences: 1} = Broker.status(manager)
    # Restored: the returned message is delivered again (at-least-once).
    {_consumer, _link, _delivery} = consume!(port, "orders")
  end

  test "B6: a late register reply after its route was removed leaves no consumer" do
    %{stand_in: stand_in, port: port} = stand_in!()
    real = StandInQueue.queue(stand_in)
    consumer = client!(port)
    :ok = StandInQueue.hold(stand_in, :register)

    assert {:ok, _attaching} =
             Client.attach(consumer, 0, "orders-late", :receiver, address: Value.string("orders"))

    assert_receive {:stand_in_held, ^stand_in, :register}, 2_000
    # D11: the route is removed by Connection DOWN while it is registering.
    :ok = GenServer.stop(consumer)
    Process.sleep(100)
    :ok = StandInQueue.release(stand_in)
    eventually!(fn -> Queue.status(real).consumers == 0 end)
    Process.sleep(200)
    assert %{consumers: 0} = Queue.status(real)
  end

  test "B7: Manager death abandons its pending requests and returns its claims" do
    instance =
      start_supervised!(
        {GravitonMQ, name: nil, runtime_supervisor_name: nil, amqp10: options(["orders"])},
        id: make_ref()
      )

    supervisor =
      instance
      |> child(GravitonMQ.Runtime.Supervisor)
      |> child(GravitonMQ.Runtime.AMQP10.Supervisor)

    manager = child(supervisor, Broker)
    queue = child(supervisor, {Queue, "orders"})
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(instance)
    publish!(port, "orders", "first")
    {consumer, _link, delivery} = consume!(port, "orders")
    publisher = client!(port)
    sender = attach!(publisher, "orders-publisher-2", :sender, "orders")
    assert %{messages: 1, inflight: 1, consumers: 1} = Queue.status(queue)
    :ok = :sys.suspend(queue)

    try do
      assert {:ok, _} =
               operation(Client.settle(consumer, delivery, %Outcome.Accepted{}), consumer)

      assert {:ok, _sending} = Client.send_message(publisher, 0, sender, message("abandoned"))
      Process.sleep(100)
      monitor = Process.monitor(manager)
      assert {:error, :unsupported_command} = GenServer.call(manager, :sync)

      capture_log(fn ->
        Process.exit(manager, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^manager, :killed}, 1_000
      end)
    after
      :sys.resume(queue)
    end

    # The exchange tables died with the Manager: neither request executed;
    # the dead anchor returned the claim.
    eventually!(fn ->
      match?(%{messages: 1, ready: 1, inflight: 0, consumers: 0}, Queue.status(queue))
    end)
  end

  defp stand_in!(queues \\ ["orders", "other"]) do
    supervisor =
      start_supervised!({StandInAMQP, {options(queues), "orders", self()}}, id: make_ref())

    children = Supervisor.which_children(supervisor)
    {_, manager, _, _} = List.keyfind(children, Broker, 0)
    {_, stand_in, _, _} = List.keyfind(children, {Queue, "orders"}, 0)
    {_, listeners, _, _} = List.keyfind(children, :listeners, 0)
    [{{Listener, _}, listener, _, _}] = Supervisor.which_children(listeners)
    %{manager: manager, stand_in: stand_in, port: Listener.info(listener).port}
  end

  defp options(queues),
    do: [
      container_id: "requests-broker-#{System.unique_integer([:positive])}",
      auth: :anonymous,
      queues: queues,
      endpoint_options: [max_frame_size: 512],
      listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
    ]

  defp client!(port) do
    client =
      start_supervised!(
        {Client,
         host: {127, 0, 0, 1},
         port: port,
         container_id: "requests-client-#{System.unique_integer([:positive])}",
         max_frame_size: 512},
        id: make_ref()
      )

    assert_receive {:graviton_mq, ^client, {:connected, _}}, 2_000
    assert {:ok, 0} = operation(Client.open_session(client, 0), client)
    client
  end

  defp attach!(client, name, role, address) do
    assert {:ok, identity} =
             operation(
               Client.attach(client, 0, name, role, address: Value.string(address)),
               client
             )

    identity
  end

  defp publish!(port, address, body) do
    client = client!(port)
    link = attach!(client, "publisher-#{System.unique_integer([:positive])}", :sender, address)

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(client, 0, link, message(body)), client)
  end

  defp consume!(port, address) do
    client = client!(port)
    link = attach!(client, "#{address}-consumer", :receiver, address)
    assert {:ok, 1} = operation(Client.request(client, 0, link, 1, 1_048_576), client)
    assert_receive {:graviton_mq, ^client, {:delivery, delivery, 0, ^link, _, _}}, 2_000
    {client, link, delivery}
  end

  defp message(body) do
    {:ok, raw} = MessageCodec.encode_sections([{:data, Value.binary(body)}])
    raw
  end

  defp operation({:ok, reference}, client, timeout \\ 3_000) do
    assert_receive {:graviton_mq, ^client, {:operation, ^reference, result}}, timeout
    result
  end

  defp child(supervisor, identity) do
    {_, pid, _, _} = List.keyfind(Supervisor.which_children(supervisor), identity, 0)
    assert is_pid(pid)
    pid
  end

  defp eventually!(predicate, timeout \\ 3_000),
    do: until!(predicate, System.monotonic_time(:millisecond) + timeout)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline
      Process.sleep(10)
      until!(predicate, deadline)
    end
  end
end
