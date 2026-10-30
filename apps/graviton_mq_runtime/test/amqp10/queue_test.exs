defmodule GravitonMQ.Runtime.AMQP10.QueueTest.LogHandler do
  @moduledoc false
  def log(event, %{config: %{parent: parent}}), do: send(parent, {:queue_privacy_log, event})
end

defmodule GravitonMQ.Runtime.AMQP10.QueueTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias GravitonMQ.Core.{Message, Outcome}
  alias GravitonMQ.Runtime.AMQP10.Queue

  test "starts with a closure child specification and reports bounded memory capacity" do
    config = config(max_messages: 2, max_bytes: 4096, max_message_bytes: 1024)
    specification = Queue.child_spec({config, "private-orders"})
    assert {Queue, :start_link, [initializer]} = specification.start
    assert is_function(initializer, 0)
    refute inspect(specification) =~ "private-orders"
    queue = start_supervised!(specification)
    assert Queue.address(queue) == "private-orders"
    status = Queue.status(queue)
    assert status.profile == :memory
    assert status.free_messages == 2
    assert status.free_bytes == 4096
    assert status.ready == 0
    assert status.inflight == 0
    assert status.limits.max_message_bytes == 1024
    refute inspect(status) =~ "private-orders"
  end

  test "real process enqueues opaque bytes and separates claim output from settlement" do
    queue = queue!()
    raw = <<0, 255, 99, 0>>
    assert {:ok, :accepted_in_memory} = enqueue(queue, raw)
    assert {:ok, generation} = Queue.register(queue, "reader", self())

    assert {:ok, [{delivery, message, metadata}]} =
             Queue.claim(queue, "reader", generation, 1, 4096)

    assert message.encoded_content == raw
    assert metadata.generation == generation
    assert metadata.attempt == 1
    assert :ok = Queue.settle(queue, "reader", generation, delivery.id, %Outcome.Retire{})
    assert Queue.status(queue).messages == 0
    assert Queue.status(queue).free_bytes == Queue.status(queue).limits.max_bytes

    assert {:error, :unknown_delivery} =
             Queue.settle(queue, "reader", generation, delivery.id, %Outcome.Retire{})
  end

  test "controller and registered owner can act while an unrelated process is refused" do
    queue = queue!()
    owner = actor()
    stranger = actor()
    assert {:ok, generation} = Queue.register(queue, "reader", owner)
    assert {:ok, :accepted_in_memory} = enqueue(queue, "opaque")

    assert {:error, :wrong_owner} =
             invoke(stranger, fn -> Queue.claim(queue, "reader", generation, 1, 4096) end)

    assert {:ok, [{delivery, _, _}]} =
             invoke(owner, fn -> Queue.claim(queue, "reader", generation, 1, 4096) end)

    assert {:error, :wrong_owner} =
             invoke(stranger, fn ->
               Queue.settle(queue, "reader", generation, delivery.id, %Outcome.Retire{})
             end)

    assert :ok = Queue.settle(queue, "reader", generation, delivery.id, %Outcome.Retire{})
    assert :ok = invoke(owner, fn -> Queue.disconnect(queue, "reader", generation) end)
    assert {:error, :stale_consumer} = Queue.claim(queue, "reader", generation, 1, 4096)
  end

  test "one owner DOWN returns every associated claim once and removes finite associations" do
    queue = queue!()
    owner = actor()
    assert {:ok, a} = Queue.register(queue, "a", owner)
    assert {:ok, b} = Queue.register(queue, "b", owner)
    assert {:ok, :accepted_in_memory} = enqueue(queue, "a")
    assert {:ok, :accepted_in_memory} = enqueue(queue, "b")
    assert {:ok, [{first, _, _}]} = Queue.claim(queue, "a", a, 1, 4096)
    assert {:ok, [_]} = Queue.claim(queue, "b", b, 1, 4096)
    assert Queue.status(queue).inflight == 2
    Process.exit(owner, :kill)
    eventually(fn -> Queue.status(queue).inflight == 0 and Queue.status(queue).consumers == 0 end)
    assert Queue.status(queue).ready == 2
    sequence = Queue.status(queue).event_sequence
    assert {:ok, fresh} = Queue.register(queue, "new", self())
    assert {:ok, deliveries} = Queue.claim(queue, "new", fresh, 2, 4096)
    assert length(deliveries) == 2

    assert Enum.all?(deliveries, fn {delivery, _, metadata} ->
             delivery.attempt == 2 and metadata.failure_count == 0
           end)

    assert {:error, :stale_consumer} = Queue.settle(queue, "a", a, first.id, %Outcome.Retire{})
    assert Queue.status(queue).event_sequence == sequence + 3
  end

  test "replaced ownership fences stale DOWN and stale commands" do
    queue = queue!()
    old_owner = actor()
    new_owner = actor()
    assert {:ok, old} = Queue.register(queue, "reader", old_owner)
    assert {:ok, :accepted_in_memory} = enqueue(queue, "content")
    assert {:ok, [_]} = Queue.claim(queue, "reader", old, 1, 4096)
    assert {:ok, fresh} = Queue.register(queue, "reader", new_owner)
    assert fresh > old
    assert {:ok, [{delivery, _, _}]} = Queue.claim(queue, "reader", fresh, 1, 4096)
    Process.exit(old_owner, :kill)
    assert {:error, :stale_consumer} = Queue.disconnect(queue, "reader", old)
    assert Queue.status(queue).inflight == 1

    assert :ok =
             invoke(new_owner, fn ->
               Queue.settle(queue, "reader", fresh, delivery.id, %Outcome.Retire{})
             end)
  end

  test "capacity and unsupported memory semantics return exact core failures" do
    queue = queue!(max_messages: 1)

    assert {:error, :durability_unsupported} =
             Queue.enqueue(queue, "a", format(), %Message.Index{}, true)

    assert {:error, :expiry_unsupported} =
             Queue.enqueue(queue, "a", format(), %Message.Index{expires_at: 10}, false)

    assert {:ok, :accepted_in_memory} = enqueue(queue, "a")
    assert {:error, :queue_full} = enqueue(queue, "b")
    assert Queue.status(queue).free_messages == 0
    assert {:ok, generation} = Queue.register(queue, "reader", self())
    assert {:ok, [{delivery, _, _}]} = Queue.claim(queue, "reader", generation, 1, 4096)

    assert {:error, :dead_letter_unsupported} =
             Queue.settle(queue, "reader", generation, delivery.id, %Outcome.Reject{
               reason: "policy",
               action: :dead_letter
             })

    assert Queue.status(queue).inflight == 1
  end

  test "release and Modified preserve original bytes and expose bounded deltas to adapter" do
    queue = queue!()
    assert {:ok, generation} = Queue.register(queue, "reader", self())
    assert {:ok, :accepted_in_memory} = enqueue(queue, <<0, 1, 255>>)
    assert {:ok, [{first, original, _}]} = Queue.claim(queue, "reader", generation, 1, 4096)
    assert :ok = Queue.settle(queue, "reader", generation, first.id, %Outcome.MakeAvailable{})
    assert {:ok, [{second, ^original, _}]} = Queue.claim(queue, "reader", generation, 1, 4096)
    mutation = %Message.Mutation{format: "annotations", encoded_data: <<1, 2>>}

    assert :ok =
             Queue.settle(queue, "reader", generation, second.id, %Outcome.ModifyAndMakeAvailable{
               delivery_failed?: true,
               annotation_mutation: mutation
             })

    assert {:ok, [{third, ^original, metadata}]} =
             Queue.claim(queue, "reader", generation, 1, 4096)

    assert third.attempt == 3
    assert metadata.failure_count == 1
    assert metadata.mutations == [mutation]
  end

  test "watchers coalesce changes until ACK and have a bounded live lifecycle" do
    queue = queue!([], max_connections: 1)
    assert :ok = Queue.watch(queue, self())
    assert :ok = Queue.watch(queue, self())
    watcher = actor()
    assert {:error, :watcher_limit} = Queue.watch(queue, watcher)
    assert {:ok, :accepted_in_memory} = enqueue(queue, "one")
    assert {:ok, :accepted_in_memory} = enqueue(queue, "two")
    assert_receive {:amqp10_queue_changed, ^queue}
    refute_receive {:amqp10_queue_changed, ^queue}, 20
    assert Queue.status(queue).pending_watchers == 1
    assert :ok = Queue.watch_ack(queue, self())
    assert Queue.status(queue).pending_watchers == 0
    assert {:ok, :accepted_in_memory} = enqueue(queue, "three")
    assert_receive {:amqp10_queue_changed, ^queue}
    refute_receive {:amqp10_queue_changed, ^queue}, 20
  end

  test "watcher DOWN frees capacity and delegated ACK is permitted" do
    queue = queue!([], max_connections: 1)
    watcher = actor()
    assert :ok = Queue.watch(queue, watcher)
    assert {:ok, :accepted_in_memory} = enqueue(queue, "change")
    assert Queue.status(queue).pending_watchers == 1
    assert :ok = Queue.watch_ack(queue, watcher)
    Process.exit(watcher, :kill)
    eventually(fn -> Queue.status(queue).watchers == 0 end)
    assert :ok = Queue.watch(queue, self())
    assert Queue.status(queue).watchers == 1
  end

  test "two same-address queues retain independent content identity leases and notifications" do
    first = queue!()
    second = queue!()
    assert :ok = Queue.watch(first, self())
    assert :ok = Queue.watch(second, self())
    assert {:ok, :accepted_in_memory} = enqueue(first, "one")
    assert_receive {:amqp10_queue_changed, ^first}
    refute_receive {:amqp10_queue_changed, ^second}, 20
    assert Queue.status(second).messages == 0
    assert {:ok, a} = Queue.register(first, "reader", self())
    assert {:ok, b} = Queue.register(second, "reader", self())
    assert a == b
    assert {:ok, :accepted_in_memory} = enqueue(second, "two")
    assert {:ok, [{da, ma, _}]} = Queue.claim(first, "reader", a, 1, 4096)
    assert {:ok, [{db, mb, _}]} = Queue.claim(second, "reader", b, 1, 4096)
    assert ma.id != mb.id
    assert da.id != db.id
    assert ma.encoded_content == "one"
    assert mb.encoded_content == "two"
  end

  test "status and raw OTP callback reports redact content queue address and private exception arguments" do
    filters = :logger.get_primary_config().filters
    :logger.remove_primary_filter(:logger_translator)

    :ok =
      :logger.add_handler(
        :amqp_queue_privacy_test,
        GravitonMQ.Runtime.AMQP10.QueueTest.LogHandler,
        %{level: :error, config: %{parent: self()}}
      )

    on_exit(fn ->
      :logger.remove_handler(:amqp_queue_privacy_test)
      :logger.set_primary_config(:filters, filters)
    end)

    private = "synthetic-amqp-private-payload"
    address = "synthetic-private-queue-address"
    {:ok, queue} = Queue.start_link({config(), address})
    Process.unlink(queue)
    on_exit(fn -> if Process.alive?(queue), do: GenServer.stop(queue) end)
    monitor = Process.monitor(queue)
    :ok = :sys.log(queue, true)
    assert {:ok, :accepted_in_memory} = enqueue(queue, private)
    assert Queue.address(queue) == address
    assert {:ok, generation} = Queue.register(queue, address, self())
    assert {:ok, [{_, message, _}]} = Queue.claim(queue, address, generation, 1, 4096)
    assert message.encoded_content == private
    status = inspect(:sys.get_status(queue), limit: :infinity, printable_limit: :infinity)
    refute status =~ private
    refute status =~ address

    :sys.replace_state(queue, fn state ->
      data = :ets.lookup_element(state.table, :state, 2)
      :ets.insert(state.table, {:state, %{data | machine: nil}})
      state
    end)

    capture_log(fn ->
      assert {:error, :queue_call_failed} = enqueue(queue, private)

      assert_receive {:DOWN, ^monitor, :process, ^queue,
                      {{:amqp_queue_callback_failed, _class}, stack}},
                     1000

      assert Enum.all?(stack, fn {_module, _function, arity, _location} -> is_integer(arity) end)
      Logger.flush()
      events = collect_logs([])
      assert Enum.any?(events, &match?(%{msg: {:report, %{label: {:proc_lib, :crash}}}}, &1))
      logged = inspect(events, limit: :infinity, printable_limit: :infinity)
      refute logged =~ private
      refute logged =~ address
    end)
  end

  test "invalid commands and unavailable queues return sanitized errors" do
    queue = queue!()
    assert {:error, :unsupported_command} = GenServer.call(queue, {:private, "secret"})
    dead = actor()
    monitor = Process.monitor(dead)
    Process.exit(dead, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^dead, _}
    assert {:error, :owner_down} = Queue.register(queue, "dead", dead)
    assert {:error, :closed} = enqueue(dead, "secret")
    refute inspect(Queue.status(queue)) =~ "secret"
  end

  test "a caller that dies before admission leaves no queued command or exchange resource" do
    queue = queue!()
    caller = actor()
    monitor = Process.monitor(caller)
    :ok = :sys.suspend(queue)
    token = make_ref()
    send(caller, {:invoke, self(), token, fn -> enqueue(queue, "unadmitted-private-content") end})

    eventually(fn ->
      {:message_queue_len, count} = Process.info(queue, :message_queue_len)
      count > 0
    end)

    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^caller, _}
    :ok = :sys.resume(queue)
    assert Queue.status(queue).messages == 0
    assert Queue.status(queue).event_sequence == 0
    table = :sys.get_state(queue).table
    :ok = GenServer.stop(queue)
    assert :ets.info(table) == :undefined
  end

  defp config(queue_options \\ [], extra \\ []),
    do: Map.merge(%{queue_options: queue_options, max_connections: 4}, Map.new(extra))

  defp queue!(options \\ [], extra \\ []),
    do: start_supervised!(Queue.child_spec({config(options, extra), "orders"}))

  defp format, do: %Message.Format{family: "amqp-1.0", identifier: 0}
  defp enqueue(queue, raw), do: Queue.enqueue(queue, raw, format(), %Message.Index{}, false)

  defp actor do
    pid = spawn(fn -> actor_loop() end)
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    pid
  end

  defp actor_loop do
    receive do
      {:invoke, caller, token, callback} ->
        send(caller, {token, callback.()})
        actor_loop()

      _ ->
        actor_loop()
    end
  end

  defp invoke(actor, callback) do
    token = make_ref()
    send(actor, {:invoke, self(), token, callback})
    assert_receive {^token, result}, 1000
    result
  end

  defp eventually(predicate, attempts \\ 100)
  defp eventually(predicate, 0), do: assert(predicate.())

  defp eventually(predicate, attempts) do
    unless predicate.() do
      Process.sleep(5)
      eventually(predicate, attempts - 1)
    end
  end

  defp collect_logs(events) do
    receive do
      {:queue_privacy_log, event} -> collect_logs([event | events])
    after
      20 -> events
    end
  end
end
