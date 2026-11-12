defmodule GravitonMQ.AMQP10.AvailabilityTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message, as: MessageCodec
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Runtime.AMQP10.{Broker, Queue}

  # A pending queue request is classified at its 5 s deadline.
  @moduletag timeout: 30_000

  test "a suspended queue delays only its own Links and is fenced at the request deadline" do
    broker = broker!()
    supervisor = amqp_supervisor(broker)
    manager = child(supervisor, Broker)
    orders = child(supervisor, {Queue, "orders"})
    other = child(supervisor, {Queue, "other"})
    connections = child(supervisor, :connections)

    for body <- ["first", "second"], do: enqueue!(orders, "orders", message(body))

    consumer = client!(broker)
    link = attach!(consumer, "orders-consumer", :receiver, "orders")
    assert {:ok, 1} = operation(Client.request(consumer, 0, link, 1, 1_048_576), consumer)
    assert_receive {:graviton_mq, ^consumer, {:delivery, unsettled, 0, ^link, _, _}}, 2_000
    assert %{ready: 1, inflight: 1, consumers: 1} = Queue.status(orders)

    bystander = client!(broker)
    publisher = attach!(bystander, "other-publisher", :sender, "other")
    reader = attach!(bystander, "other-consumer", :receiver, "other")
    connection_count = DynamicSupervisor.count_children(connections).active
    :sys.suspend(orders)

    try do
      deadline = System.monotonic_time(:millisecond) + 5_000
      # The Manager sends the settle to the suspended queue; it is never taken.
      assert {:ok, _} =
               operation(Client.settle(consumer, unsettled, %Outcome.Accepted{}), consumer)

      within!(500, fn ->
        assert {:ok, %Outcome.Accepted{}} =
                 operation(
                   Client.send_message(bystander, 0, publisher, message("unaffected")),
                   bystander
                 )
      end)

      within!(500, fn -> attach!(bystander, "other-late", :receiver, "other") end)

      within!(500, fn ->
        assert {:ok, 1} =
                 operation(Client.request(bystander, 0, reader, 1, 1_048_576), bystander)

        assert_receive {:graviton_mq, ^bystander, {:delivery, reference, 0, ^reader, _, _}}, 500

        assert {:ok, _} =
                 operation(Client.settle(bystander, reference, %Outcome.Accepted{}), bystander)
      end)

      assert System.monotonic_time(:millisecond) < deadline,
             "bystander work must complete before the 5 s request deadline"

      assert child(supervisor, Broker) == manager
      refute_received {:graviton_mq, ^consumer, {:session, 0, {:detached, ^link, _}}}

      # The settle was not executed: the queue is fenced at its deadline.
      assert_receive {:graviton_mq, ^consumer, {:session, 0, {:detached, ^link, error}}}, 7_000
      assert {:peer_detach, %{condition: %Value{value: "amqp:internal-error"}}} = error
      assert child(supervisor, Broker) == manager
      assert DynamicSupervisor.count_children(connections).active == connection_count

      # A new Link on the fenced queue is refused explicitly, not hung.
      assert {:error, _} =
               operation(
                 Client.attach(consumer, 0, "refused", :receiver, address: Value.string("orders")),
                 consumer
               )
    after
      :sys.resume(orders)
    end

    # The fenced lease is released by the queue: no claimed message is lost.
    eventually!(fn -> match?(%{ready: 2, inflight: 0, consumers: 0}, Queue.status(orders)) end)
    eventually!(fn -> Queue.status(other).messages == 0 end)

    # The probe restores the queue and new Links work again.
    eventually!(
      fn ->
        match?({:ok, %{queues: %{"orders" => %{ready: 2}}}}, GravitonMQ.amqp10_status(broker))
      end,
      5_000
    )

    link = attach!(consumer, "orders-again", :receiver, "orders")
    assert {:ok, 1} = operation(Client.request(consumer, 0, link, 1, 1_048_576), consumer)
    assert_receive {:graviton_mq, ^consumer, {:delivery, _, 0, ^link, _, _}}, 2_000
    assert child(supervisor, Broker) == manager
  end

  test "a Connection busier than the capacity timeout is retried, not closed" do
    broker = broker!()
    supervisor = amqp_supervisor(broker)
    orders = child(supervisor, {Queue, "orders"})
    consumer = client!(broker)
    link = attach!(consumer, "busy-consumer", :receiver, "orders")
    assert {:ok, 1} = operation(Client.request(consumer, 0, link, 1, 1_048_576), consumer)

    [{_, connection, _, _}] =
      supervisor |> child(:connections) |> DynamicSupervisor.which_children()

    monitor = Process.monitor(connection)
    raw = message("delayed")
    :sys.suspend(connection)

    try do
      # The queue change pumps the sender route while its Connection is busy.
      enqueue!(orders, "orders", raw)
      Process.sleep(300)
    after
      :sys.resume(connection)
    end

    assert_receive {:graviton_mq, ^consumer, {:delivery, reference, 0, ^link, ^raw, _}}, 2_000
    assert {:ok, _} = operation(Client.settle(consumer, reference, %Outcome.Accepted{}), consumer)
    refute_received {:DOWN, ^monitor, _, _, _}
    assert Process.alive?(connection)
    eventually!(fn -> Queue.status(orders).messages == 0 end)
  end

  defp broker! do
    config = [
      container_id: "broker-#{System.unique_integer([:positive])}",
      auth: :anonymous,
      queues: ["orders", "other"],
      endpoint_options: [max_frame_size: 512],
      listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
    ]

    start_supervised!({GravitonMQ, name: nil, runtime_supervisor_name: nil, amqp10: config},
      id: make_ref()
    )
  end

  defp client!(broker) do
    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)

    client =
      start_supervised!(
        {Client,
         host: {127, 0, 0, 1},
         port: port,
         container_id: "client-#{System.unique_integer([:positive])}",
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

  defp enqueue!(queue, address, raw) do
    assert {:ok, :accepted_in_memory} =
             Queue.enqueue(
               queue,
               raw,
               %GravitonMQ.Core.Message.Format{family: "amqp-1.0", identifier: 0},
               %GravitonMQ.Core.Message.Index{routing: %{"address" => address}},
               false
             )
  end

  defp message(body) do
    {:ok, raw} = MessageCodec.encode_sections([{:data, Value.binary(body)}])
    raw
  end

  defp operation({:ok, reference}, client) do
    assert_receive {:graviton_mq, ^client, {:operation, ^reference, result}}, 8_000
    result
  end

  defp amqp_supervisor(broker),
    do:
      broker
      |> child(GravitonMQ.Runtime.Supervisor)
      |> child(GravitonMQ.Runtime.AMQP10.Supervisor)

  defp child(supervisor, identity) do
    {_, pid, _, _} = List.keyfind(Supervisor.which_children(supervisor), identity, 0)
    assert is_pid(pid)
    pid
  end

  defp within!(milliseconds, callback) do
    started = System.monotonic_time(:millisecond)
    result = callback.()
    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed < milliseconds, "took #{elapsed} ms, limit #{milliseconds} ms"
    result
  end

  defp eventually!(predicate, timeout \\ 2_000),
    do: until!(predicate, System.monotonic_time(:millisecond) + timeout)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline
      Process.sleep(10)
      until!(predicate, deadline)
    end
  end
end
