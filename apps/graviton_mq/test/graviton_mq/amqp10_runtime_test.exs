defmodule GravitonMQ.AMQP10.RuntimeTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message, as: MessageCodec
  alias GravitonMQ.AMQP10.Codec.Value, as: ValueCodec
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Runtime.AMQP10.{Broker, Queue}

  @moduletag timeout: 10_000

  test "the historical empty instance keeps AMQP disabled until explicitly configured" do
    instance = start_supervised!({GravitonMQ, name: nil, runtime_supervisor_name: nil})
    assert {:error, :amqp10_not_enabled} = GravitonMQ.amqp10_status(instance)
  end

  test "the runtime commits before Accepted and retires only after a receiver outcome" do
    broker = broker!()
    sender = client!(broker)
    send_link = attach!(sender, "sender", :sender)
    raw = message("authoritative-body")

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(sender, 0, send_link, raw), sender)

    assert %{ready: 1, inflight: 0} = queue_status(broker)
    receiver = client!(broker)
    receive_link = attach!(receiver, "receiver", :receiver)
    assert {:ok, 1} = operation(Client.request(receiver, 0, receive_link, 1, 1_048_576), receiver)

    assert_receive {:graviton_mq, ^receiver, {:delivery, reference, 0, ^receive_link, ^raw, _}},
                   2_000

    assert %{ready: 0, inflight: 1, messages: 1} = queue_status(broker)
    assert {:ok, _} = operation(Client.settle(receiver, reference, %Outcome.Accepted{}), receiver)
    eventually!(fn -> queue_status(broker).messages == 0 end)
  end

  test "Modified merges annotations and increments delivery-count without rewriting bare content" do
    broker = broker!()
    sender = client!(broker)
    send_link = attach!(sender, "sender", :sender)

    {:ok, prefix} =
      MessageCodec.encode_sections([
        {:header,
         Value.list([
           Value.boolean(false),
           Value.ubyte(4),
           Value.null(),
           Value.boolean(false),
           Value.uint(7)
         ])},
        {:delivery_annotations,
         Value.map([{Value.symbol("x-opt-hop-private"), Value.string("drop")}])},
        {:message_annotations,
         Value.map([
           {Value.symbol("x-opt-keep"), Value.uint(3)},
           {Value.symbol("x-opt-replace"), Value.string("old")}
         ])}
      ])

    # Deliberately noncanonical binary32 body. Its bytes are authoritative.
    body = <<0, 0x53, 0x75, 0xB0, 4::32, "body">>
    raw = prefix <> body

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(sender, 0, send_link, raw), sender)

    receiver = client!(broker)
    link = attach!(receiver, "receiver", :receiver)
    assert {:ok, 1} = operation(Client.request(receiver, 0, link, 1, 1_048_576), receiver)
    assert_receive {:graviton_mq, ^receiver, {:delivery, first, 0, ^link, forwarded, _}}, 2_000
    {:ok, first_message, <<>>} = MessageCodec.decode(forwarded)
    refute List.keymember?(first_message.sections, :delivery_annotations, 0)

    annotations =
      Value.map([
        {Value.symbol("x-opt-replace"), Value.string("new")},
        {Value.symbol("x-opt-added"), Value.boolean(true)}
      ])

    {:ok, encoded} = ValueCodec.encode(annotations)

    modified = %Outcome.Modified{
      delivery_failed?: true,
      message_annotations: annotations,
      encoded_message_annotations: encoded
    }

    assert {:ok, _} = operation(Client.settle(receiver, first, modified), receiver)
    assert {:ok, 1} = operation(Client.request(receiver, 0, link, 1, 1_048_576), receiver)
    assert_receive {:graviton_mq, ^receiver, {:delivery, second, 0, ^link, redelivery, _}}, 2_000
    assert String.ends_with?(redelivery, body)
    {:ok, decoded, <<>>} = MessageCodec.decode(redelivery)
    assert {:header, %Value{value: fields}} = List.keyfind(decoded.sections, :header, 0)
    assert Enum.at(fields, 4) == Value.uint(8)

    assert {:message_annotations, %Value{value: entries}} =
             List.keyfind(decoded.sections, :message_annotations, 0)

    assert Map.new(entries) ==
             Map.new([{Value.symbol("x-opt-keep"), Value.uint(3)} | annotations.value])

    assert {:ok, _} = operation(Client.settle(receiver, second, %Outcome.Accepted{}), receiver)
    eventually!(fn -> queue_status(broker).messages == 0 end)
  end

  test "a temporary writer budget retains the queued delivery until capacity wakes" do
    broker = broker!(endpoint_options: [max_frame_size: 512, session_options: [max_fragments: 2]])
    receiver = client!(broker)
    link = attach!(receiver, "receiver", :receiver)
    supervisor = amqp_supervisor(broker)
    connections = child(supervisor, :connections)
    [{_, connection, _, _}] = DynamicSupervisor.which_children(connections)
    queue = child(supervisor, {Queue, "orders"})
    {generation, write_id} = occupy_writer(connection, 200)
    first = message(:binary.copy("x", 967))
    second = message("second")

    for raw <- [first, second] do
      assert {:ok, :accepted_in_memory} =
               Queue.enqueue(
                 queue,
                 raw,
                 %GravitonMQ.Core.Message.Format{family: "amqp-1.0", identifier: 0},
                 %GravitonMQ.Core.Message.Index{routing: %{"address" => "orders"}},
                 false
               )
    end

    assert {:ok, 2} = operation(Client.request(receiver, 0, link, 2, 2_097_152), receiver)
    refute_receive {:graviton_mq, ^receiver, {:delivery, _, _, _, _, _}}, 50
    refute_receive {:graviton_mq, ^receiver, {:session, 0, {:detached, ^link, _}}}, 50
    assert %{ready: 1, inflight: 1, consumers: 1} = Queue.status(queue)

    # An actual write receipt releases endpoint capacity and supplies the wakeup.
    send(connection, {:amqp10_write, generation, write_id, :ok})
    assert_receive {:graviton_mq, ^receiver, {:delivery, first_ref, 0, ^link, ^first, _}}, 2_000
    assert {:ok, _} = operation(Client.settle(receiver, first_ref, %Outcome.Accepted{}), receiver)
    assert_receive {:graviton_mq, ^receiver, {:delivery, second_ref, 0, ^link, ^second, _}}, 2_000

    assert {:ok, _} =
             operation(Client.settle(receiver, second_ref, %Outcome.Accepted{}), receiver)

    eventually!(fn -> Queue.status(queue).messages == 0 end)
  end

  test "a pending delivery survives another Link advancing its wire ID constructor" do
    broker = broker!(endpoint_options: [max_frame_size: 512, session_options: [max_fragments: 2]])
    receiver = client!(broker)
    blocked = attach!(receiver, "blocked", :receiver)
    other = attach!(receiver, "other", :receiver)
    supervisor = amqp_supervisor(broker)

    [{_, connection, _, _}] =
      supervisor |> child(:connections) |> DynamicSupervisor.which_children()

    queue = child(supervisor, {Queue, "orders"})
    {generation, write_id} = occupy_writer(connection, 200)
    first = message(:binary.copy("x", 967))
    second = message("other-link")

    for raw <- [first, second] do
      assert {:ok, :accepted_in_memory} =
               Queue.enqueue(
                 queue,
                 raw,
                 %GravitonMQ.Core.Message.Format{family: "amqp-1.0", identifier: 0},
                 %GravitonMQ.Core.Message.Index{routing: %{"address" => "orders"}},
                 false
               )
    end

    assert {:ok, 1} = operation(Client.request(receiver, 0, blocked, 1, 1_048_576), receiver)
    eventually!(fn -> match?(%{ready: 1, inflight: 1, consumers: 2}, Queue.status(queue)) end)
    identity = {Value.string("blocked"), :sender}
    capacity = GravitonMQ.Runtime.AMQP10.Connection.capacity(connection, 0, identity)
    assert byte_size(first) == capacity.max_message_bytes
    assert byte_size(first) > capacity.bytes

    # The other Link emits wire delivery-id 0 while this claim waits. Its send
    # advances the Session's next ID to 1, whose constructor needs another byte.
    assert {:ok, 1} = operation(Client.request(receiver, 0, other, 1, 1_048_576), receiver)
    assert_receive {:graviton_mq, ^receiver, {:delivery, ref, 0, ^other, ^second, _}}, 2_000
    assert {:ok, _} = operation(Client.settle(receiver, ref, %Outcome.Accepted{}), receiver)
    capacity = GravitonMQ.Runtime.AMQP10.Connection.capacity(connection, 0, identity)
    assert byte_size(first) == capacity.max_message_bytes
    refute_receive {:graviton_mq, ^receiver, {:session, 0, {:detached, ^blocked, _}}}, 50
    assert %{messages: 1, ready: 0, inflight: 1, consumers: 2} = Queue.status(queue)
    send(connection, {:amqp10_write, generation, write_id, :ok})

    assert_receive {:graviton_mq, ^receiver, {:delivery, first_ref, 0, ^blocked, ^first, _}},
                   2_000

    assert {:ok, _} = operation(Client.settle(receiver, first_ref, %Outcome.Accepted{}), receiver)
    eventually!(fn -> Queue.status(queue).messages == 0 end)
  end

  test "a message above the permanent fragment limit still refuses the receiving Link" do
    broker = broker!(endpoint_options: [max_frame_size: 512, session_options: [max_fragments: 2]])
    receiver = client!(broker)
    link = attach!(receiver, "receiver", :receiver)
    queue = broker |> amqp_supervisor() |> child({Queue, "orders"})

    assert {:ok, :accepted_in_memory} =
             Queue.enqueue(
               queue,
               message(:binary.copy("x", 1100)),
               %GravitonMQ.Core.Message.Format{family: "amqp-1.0", identifier: 0},
               %GravitonMQ.Core.Message.Index{routing: %{"address" => "orders"}},
               false
             )

    assert {:ok, 1} = operation(Client.request(receiver, 0, link, 1, 1_048_576), receiver)
    assert_receive {:graviton_mq, ^receiver, {:session, 0, {:detached, ^link, _}}}, 2_000
    eventually!(fn -> match?(%{ready: 1, inflight: 0, consumers: 0}, Queue.status(queue)) end)
  end

  test "disconnect returns a delivery retained while waiting for writer capacity" do
    broker = broker!(endpoint_options: [max_frame_size: 512, session_options: [max_fragments: 2]])
    receiver = client!(broker)
    link = attach!(receiver, "receiver", :receiver)
    supervisor = amqp_supervisor(broker)

    [{_, connection, _, _}] =
      supervisor |> child(:connections) |> DynamicSupervisor.which_children()

    queue = child(supervisor, {Queue, "orders"})
    occupy_writer(connection, 200)

    assert {:ok, :accepted_in_memory} =
             Queue.enqueue(
               queue,
               message(:binary.copy("x", 700)),
               %GravitonMQ.Core.Message.Format{family: "amqp-1.0", identifier: 0},
               %GravitonMQ.Core.Message.Index{routing: %{"address" => "orders"}},
               false
             )

    assert {:ok, 1} = operation(Client.request(receiver, 0, link, 1, 1_048_576), receiver)
    eventually!(fn -> match?(%{ready: 0, inflight: 1, consumers: 1}, Queue.status(queue)) end)
    event_sequence = Queue.status(queue).event_sequence
    refute_receive {:graviton_mq, ^receiver, {:delivery, _, _, _, _, _}}, 50
    assert Queue.status(queue).event_sequence == event_sequence
    assert {:ok, :closed} = operation(Client.close(receiver), receiver)
    eventually!(fn -> match?(%{ready: 1, inflight: 0, consumers: 0}, Queue.status(queue)) end)
  end

  test "memory admission explicitly rejects durability, TTL and absolute expiry" do
    broker = broker!()
    sender = client!(broker)
    link = attach!(sender, "publisher", :sender)

    headers = [
      [{:header, Value.list([Value.boolean(true)])}],
      [{:header, Value.list([Value.null(), Value.null(), Value.uint(1)])}],
      [{:properties, Value.list(List.duplicate(Value.null(), 8) ++ [Value.timestamp(0)])}]
    ]

    for sections <- headers do
      {:ok, raw} = MessageCodec.encode_sections(sections ++ [{:data, Value.binary("rejected")}])

      assert {:ok,
              %Outcome.Rejected{error: %{condition: %Value{value: "amqp:precondition-failed"}}}} =
               operation(Client.send_message(sender, 0, link, raw), sender)

      assert queue_status(broker).messages == 0
    end
  end

  test "an asynchronous ACL is bounded and its timeout denies before granting credit" do
    observer = self()

    authorize = fn _, action, address ->
      send(observer, {:auth_requested, self(), action, address})

      receive do
        :allow_link -> :allow
      after
        5_000 -> :deny
      end
    end

    broker = broker!(authorize: authorize, max_auth_jobs: 1, auth_timeout: 600)
    first = client!(broker)
    first_operation = Client.attach(first, 0, "pending", :sender, address: Value.string("orders"))
    assert_receive {:auth_requested, task, :send, "orders"}, 1_000
    second = client!(broker)

    assert {:error, _} =
             operation(
               Client.attach(second, 0, "bounded", :sender, address: Value.string("orders")),
               second
             )

    refute_receive {:auth_requested, _, _, _}, 20
    assert {:error, _} = operation(first_operation, first)
    eventually!(fn -> not Process.alive?(task) end)
    assert %{messages: 0, consumers: 0} = queue_status(broker)
    assert {:ok, %{auth_jobs: 0, links: 0}} = GravitonMQ.amqp10_status(broker)
  end

  test "a denied direction does not inherit the permitted peer role" do
    broker =
      broker!(
        authorize: fn
          :anonymous, :send, "orders" -> :allow
          _, _, _ -> :deny
        end
      )

    sender = client!(broker)
    link = attach!(sender, "permitted", :sender)

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(sender, 0, link, message("queued")), sender)

    receiver = client!(broker)

    assert {:error, _} =
             operation(
               Client.attach(receiver, 0, "denied", :receiver, address: Value.string("orders")),
               receiver
             )

    assert %{ready: 1, inflight: 0, consumers: 0} = queue_status(broker)
  end

  test "Manager failure closes old connections and surviving queues release their leases" do
    broker = broker!()
    sender = client!(broker)
    send_link = attach!(sender, "sender", :sender)

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(sender, 0, send_link, message("restart")), sender)

    receiver = client!(broker)
    link = attach!(receiver, "receiver", :receiver)
    assert {:ok, 1} = operation(Client.request(receiver, 0, link, 1, 1_048_576), receiver)
    assert_receive {:graviton_mq, ^receiver, {:delivery, _, 0, ^link, _, _}}, 2_000
    assert queue_status(broker).inflight == 1
    supervisor = amqp_supervisor(broker)
    manager = child(supervisor, Broker)
    queue = child(supervisor, {Queue, "orders"})
    monitor = Process.monitor(manager)

    capture_log(fn ->
      Process.exit(manager, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^manager, :killed}

      eventually!(fn ->
        is_pid(find_child(supervisor, Broker)) and find_child(supervisor, Broker) != manager
      end)

      eventually!(fn -> match?(%{ready: 1, inflight: 0, consumers: 0}, Queue.status(queue)) end)
    end)

    assert child(supervisor, {Queue, "orders"}) == queue
    assert queue_status(broker).messages == 1
  end

  test "Manager debug status excludes a forwarded message and captured authorization data" do
    secret = "private-amqp-#{System.unique_integer([:positive])}"
    authorize = fn _, _, _ -> if byte_size(secret) > 0, do: :allow, else: :deny end
    broker = broker!(authorize: authorize)
    manager = broker |> amqp_supervisor() |> child(Broker)
    :sys.log(manager, true)
    sender = client!(broker)
    link = attach!(sender, "sender", :sender)

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(sender, 0, link, message(secret)), sender)

    assert queue_status(broker).messages == 1
    refute inspect(:sys.get_status(manager)) =~ secret
  end

  defp broker!(options \\ []) do
    config =
      Keyword.merge(
        [
          container_id: "broker-#{System.unique_integer([:positive])}",
          auth: :anonymous,
          queues: ["orders"],
          endpoint_options: [max_frame_size: 512],
          listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
        ],
        options
      )

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

  defp attach!(client, name, role) do
    assert {:ok, identity} =
             operation(
               Client.attach(client, 0, name, role, address: Value.string("orders")),
               client
             )

    identity
  end

  defp message(body) do
    {:ok, raw} = MessageCodec.encode_sections([{:data, Value.binary(body)}])
    raw
  end

  defp occupy_writer(connection, free_bytes) do
    caller = self()

    :sys.replace_state(connection, fn shell ->
      state = :ets.lookup_element(shell.table, :state, 2)
      endpoint = state.endpoint
      id = endpoint.next_write_id

      bytes =
        endpoint.options.max_write_bytes - endpoint.options.control_reserve_bytes - free_bytes

      endpoint = %{
        endpoint
        | next_write_id: id + 1,
          writes: Map.put(endpoint.writes, id, %{bytes: bytes, owner: :connection}),
          write_bytes: endpoint.write_bytes + bytes
      }

      :ets.insert(shell.table, {:state, %{state | endpoint: endpoint}})
      send(caller, {:occupied_writer, endpoint.generation, id})
      shell
    end)

    assert_receive {:occupied_writer, generation, id}
    {generation, id}
  end

  defp operation({:ok, reference}, client) do
    assert_receive {:graviton_mq, ^client, {:operation, ^reference, result}}, 3_000
    result
  end

  defp queue_status(broker) do
    assert {:ok, %{queues: %{"orders" => status}}} = GravitonMQ.amqp10_status(broker)
    status
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

  defp find_child(supervisor, identity) do
    case List.keyfind(Supervisor.which_children(supervisor), identity, 0) do
      {_, pid, _, _} when is_pid(pid) -> pid
      _ -> nil
    end
  end

  defp eventually!(predicate), do: until!(predicate, System.monotonic_time(:millisecond) + 2_000)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline
      Process.sleep(5)
      until!(predicate, deadline)
    end
  end
end
