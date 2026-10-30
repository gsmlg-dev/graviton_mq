defmodule GravitonMQ.AMQP10.RuntimeBoundariesTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message, as: MessageCodec
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Runtime.AMQP10.{Broker, Queue}

  @moduletag timeout: 15_000

  test "500 wire bytes fit a 512-byte receiver limit despite larger logical queue charges" do
    instance = broker!()
    sender = client!(instance)
    send_link = attach!(sender, "publisher", :sender)
    raw = message(:binary.copy("x", 492))
    assert byte_size(raw) == 500

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(sender, 0, send_link, raw), sender)

    assert %{ready: 1, bytes: charged} = queue_status(instance)
    assert charged > 512

    receiver = client!(instance)
    link = attach!(receiver, "bounded", :receiver, max_message_bytes: 512)
    assert {:ok, 1} = operation(Client.request(receiver, 0, link, 1, 512), receiver)
    assert_receive {:graviton_mq, ^receiver, {:delivery, delivery, 0, ^link, ^raw, _}}, 1_000
    assert {:ok, _} = operation(Client.settle(receiver, delivery, %Outcome.Accepted{}), receiver)
    eventually!(fn -> queue_status(instance).messages == 0 end)
  end

  test "a small configured queue grants publication credit without a one-MiB reservation" do
    instance = broker!(queue_options: [max_bytes: 4096, max_message_bytes: 1024])
    sender = client!(instance)
    send_link = attach!(sender, "small-queue", :sender)
    raw = message("small-queue-publication")

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(sender, 0, send_link, raw), sender, 1_000)

    assert %{ready: 1, messages: 1, inflight: 0} = queue_status(instance)

    receiver = client!(instance)
    link = attach!(receiver, "small-queue-consumer", :receiver)
    assert {:ok, 1} = operation(Client.request(receiver, 0, link, 1, 1_048_576), receiver)
    assert_receive {:graviton_mq, ^receiver, {:delivery, delivery, 0, ^link, ^raw, _}}, 1_000
    assert {:ok, _} = operation(Client.settle(receiver, delivery, %Outcome.Accepted{}), receiver)
    eventually!(fn -> queue_status(instance).messages == 0 end)
  end

  test "Manager death releases its leases even while the transport owner is suspended" do
    instance = broker!()
    {queue, manager} = owners(instance)
    sender = client!(instance)
    link = attach!(sender, "publisher", :sender)
    raw = message("manager-owned-responsibility")

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(sender, 0, link, raw), sender)

    before_receiver = connections(instance)
    receiver = client!(instance)
    receive_link = attach!(receiver, "held-delivery", :receiver)
    [connection] = connections(instance) -- before_receiver
    assert {:ok, 1} = operation(Client.request(receiver, 0, receive_link, 1, 1_048_576), receiver)
    assert_receive {:graviton_mq, ^receiver, {:delivery, _, 0, ^receive_link, ^raw, _}}, 1_000
    assert %{inflight: 1, consumers: 1} = Queue.status(queue)
    monitor = Process.monitor(manager)
    :ok = :sys.suspend(connection)

    try do
      capture_log(fn ->
        Process.exit(manager, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^manager, :killed}, 1_000

        eventually!(
          fn -> match?(%{ready: 1, inflight: 0, consumers: 0}, Queue.status(queue)) end,
          500
        )

        assert Process.alive?(connection),
               "queue responsibility must not wait for transport-owner shutdown"
      end)
    after
      resume(connection)
    end

    # Let the intentional rest_for_one restart finish before test teardown.
    eventually!(fn ->
      case GravitonMQ.amqp10_status(instance) do
        {:ok, %{connections: 0}} -> true
        _ -> false
      end
    end)
  end

  test "uncertain detach cleanup fails the Manager and leaves no orphan consumer or claim" do
    instance = broker!()
    {queue, manager} = owners(instance)
    sender = client!(instance)
    link = attach!(sender, "publisher", :sender)
    raw = message("recover-after-uncertain-cleanup")

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(sender, 0, link, raw), sender)

    receiver = client!(instance)
    receive_link = attach!(receiver, "uncertain-consumer", :receiver)
    assert {:ok, 1} = operation(Client.request(receiver, 0, receive_link, 1, 1_048_576), receiver)

    assert_receive {:graviton_mq, ^receiver, {:delivery, released, 0, ^receive_link, ^raw, _}},
                   1_000

    assert {:ok, _} = operation(Client.settle(receiver, released, %Outcome.Released{}), receiver)
    eventually!(fn -> match?(%{ready: 1, inflight: 0, consumers: 1}, Queue.status(queue)) end)
    monitor = Process.monitor(manager)
    :ok = :sys.suspend(queue)

    try do
      capture_log(fn ->
        {:ok, _reference} = Client.detach(receiver, 0, receive_link)
        assert_receive {:DOWN, ^monitor, :process, ^manager, reason}, 6_000
        refute reason == :normal
      end)
    after
      resume(queue)
    end

    eventually!(fn -> match?(%{ready: 1, inflight: 0, consumers: 0}, Queue.status(queue)) end)

    eventually!(fn ->
      case GravitonMQ.amqp10_status(instance) do
        {:ok, _} -> true
        _ -> false
      end
    end)

    {surviving_queue, replacement_manager} = owners(instance)
    assert surviving_queue == queue
    assert replacement_manager != manager
    recovered = client!(instance)
    recovered_link = attach!(recovered, "replacement-consumer", :receiver)

    assert {:ok, 1} =
             operation(Client.request(recovered, 0, recovered_link, 1, 1_048_576), recovered)

    assert_receive {:graviton_mq, ^recovered, {:delivery, delivery, 0, ^recovered_link, ^raw, _}},
                   1_000

    assert {:ok, _} =
             operation(Client.settle(recovered, delivery, %Outcome.Accepted{}), recovered)

    eventually!(fn -> Queue.status(queue).messages == 0 end)
  end

  test "a pump wave limits inspected zero-credit Links, including Links that do no work" do
    instance = broker!(max_pump_actions: 1)
    {_queue, manager} = owners(instance)
    client = client!(instance)
    links = for index <- 1..6, do: attach!(client, "zero-credit-#{index}", :receiver)

    eventually!(fn ->
      {:ok, status} = GravitonMQ.amqp10_status(instance)
      status.links == 6 and status.operations == 0
    end)

    # Sensitive owners suppress VM tracing. Enable it only in this fixture to
    # observe the actual per-callback work bound; production privacy stays on.
    set_sensitive(manager, false)
    :erlang.trace_pattern({Broker, :handle_info, 2}, true, [:local])
    :erlang.trace_pattern({Broker, :pump_route, 2}, true, [:local])
    :erlang.trace(manager, true, [:call, {:tracer, self()}])

    try do
      # A real peer Flow wakes the coordinator. Five other Links retain zero credit.
      assert {:ok, 1} = operation(Client.request(client, 0, hd(links), 1, 1_048_576), client)
      waves = collect_pump_waves(manager, nil, [], System.monotonic_time(:millisecond) + 500)

      assert Enum.sum(waves) >= 6,
             "trace must observe a full six-Link inspection wave: #{inspect(waves)}"

      assert Enum.all?(waves, &(&1 <= 1)),
             "max_pump_actions=1 must bound all inspected routes: #{inspect(waves)}"
    after
      if Process.alive?(manager), do: :erlang.trace(manager, false, [:call])
      :erlang.trace_pattern({Broker, :handle_info, 2}, false, [:local])
      :erlang.trace_pattern({Broker, :pump_route, 2}, false, [:local])
      if Process.alive?(manager), do: set_sensitive(manager, true)
    end
  end

  test "one suspended Connection cannot hold healthy manager work for five seconds per Link" do
    instance = broker!(max_pump_actions: 1)
    slow = client!(instance)
    for index <- 1..6, do: attach!(slow, "stalled-#{index}", :receiver)
    [connection] = connections(instance)
    healthy = client!(instance)
    healthy_link = attach!(healthy, "healthy-publisher", :sender)

    eventually!(fn ->
      {:ok, status} = GravitonMQ.amqp10_status(instance)
      status.links == 7 and status.operations == 0
    end)

    :ok = :sys.suspend(connection)

    try do
      raw = message("healthy-publication")
      started = System.monotonic_time(:millisecond)

      assert {:ok, %Outcome.Accepted{}} =
               operation(Client.send_message(healthy, 0, healthy_link, raw), healthy, 1_000)

      status_task = Task.async(fn -> GravitonMQ.amqp10_status(instance) end)
      assert {:ok, status} = Task.await(status_task, 1_000)
      assert status.queues["orders"].messages == 1
      assert System.monotonic_time(:millisecond) - started < 1_500
    after
      resume(connection)
    end
  end

  defp broker!(options \\ []) do
    config =
      Keyword.merge(
        [
          container_id: "boundary-broker-#{System.unique_integer([:positive])}",
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

  defp client!(instance) do
    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(instance)

    client =
      start_supervised!(
        {Client,
         host: {127, 0, 0, 1},
         port: port,
         container_id: "boundary-client-#{System.unique_integer([:positive])}",
         max_frame_size: 512},
        id: make_ref()
      )

    assert_receive {:graviton_mq, ^client, {:connected, _}}, 2_000
    assert {:ok, 0} = operation(Client.open_session(client, 0), client)
    client
  end

  defp attach!(client, name, role, options \\ []) do
    options = Keyword.put(options, :address, Value.string("orders"))
    assert {:ok, identity} = operation(Client.attach(client, 0, name, role, options), client)
    identity
  end

  defp message(body) do
    {:ok, raw} = MessageCodec.encode_sections([{:data, Value.binary(body)}])
    raw
  end

  defp operation({:ok, reference}, client, timeout \\ 3_000) do
    assert_receive {:graviton_mq, ^client, {:operation, ^reference, result}}, timeout
    result
  end

  defp queue_status(instance) do
    assert {:ok, %{queues: %{"orders" => status}}} = GravitonMQ.amqp10_status(instance)
    status
  end

  defp owners(instance) do
    supervisor = amqp_supervisor(instance)
    {child(supervisor, {Queue, "orders"}), child(supervisor, Broker)}
  end

  defp connections(instance),
    do:
      instance
      |> amqp_supervisor()
      |> child(:connections)
      |> DynamicSupervisor.which_children()
      |> Enum.map(&elem(&1, 1))

  defp amqp_supervisor(instance),
    do:
      instance
      |> child(GravitonMQ.Runtime.Supervisor)
      |> child(GravitonMQ.Runtime.AMQP10.Supervisor)

  defp child(supervisor, identity) do
    {_, pid, _, _} = List.keyfind(Supervisor.which_children(supervisor), identity, 0)
    assert is_pid(pid)
    pid
  end

  defp set_sensitive(owner, sensitive) do
    :sys.replace_state(owner, fn state ->
      Process.flag(:sensitive, sensitive)
      state
    end)
  end

  defp resume(pid) do
    if Process.alive?(pid), do: :sys.resume(pid)
  catch
    :exit, _ -> :ok
  end

  defp eventually!(predicate, timeout \\ 2_000),
    do: until!(predicate, System.monotonic_time(:millisecond) + timeout)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline
      Process.sleep(5)
      until!(predicate, deadline)
    end
  end

  defp collect_pump_waves(manager, current, waves, deadline) do
    remaining = max(0, deadline - System.monotonic_time(:millisecond))

    receive do
      {:trace, ^manager, :call, {Broker, :handle_info, [message, _]}} ->
        waves = if is_integer(current), do: [current | waves], else: waves
        current = if message == :pump, do: 0, else: nil
        collect_pump_waves(manager, current, waves, deadline)

      {:trace, ^manager, :call, {Broker, :pump_route, _}} when is_integer(current) ->
        collect_pump_waves(manager, current + 1, waves, deadline)

      {:trace, ^manager, _, _} ->
        collect_pump_waves(manager, current, waves, deadline)
    after
      remaining ->
        waves = if is_integer(current), do: [current | waves], else: waves
        Enum.reverse(waves)
    end
  end
end
