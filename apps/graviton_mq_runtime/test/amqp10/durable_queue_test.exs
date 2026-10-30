defmodule GravitonMQ.Runtime.AMQP10.DurableQueueTest do
  use ExUnit.Case, async: false
  @moduletag :tmp_dir
  @moduletag :capture_log

  alias GravitonMQ.Core.{CommitRef, Message, Outcome}
  alias GravitonMQ.Runtime.AMQP10.{Config, Queue}

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  test "local durable configuration is explicit and retains memory default", %{tmp_dir: dir} do
    assert {:ok, config} =
             Config.build(
               auth: :anonymous,
               container_id: "durable-broker",
               queues: ["orders"],
               persistence: [directory: Path.join(dir, "broker"), stream_id: "stable-broker"]
             )

    assert config.profile == :local_durable
    assert config.persistence.stream_id == "stable-broker"

    assert {:error, _} =
             Config.build(
               auth: :anonymous,
               container_id: "broker",
               queues: ["orders"],
               profile: :local_durable
             )
  end

  test "durable admission survives owner restart with original encoded bytes and identities",
       context do
    config = config(context)
    queue = open!(config)

    assert {:ok, {:accepted_durable, %CommitRef{} = committed}} =
             enqueue(queue, <<0, 255, 128>>, true)

    assert %{profile: :local_durable, durable_through: ^committed} = Queue.status(queue)
    assert {:ok, lease} = Queue.register(queue, "old-reader", self())

    assert {:ok, [{old_delivery, original, metadata}]} =
             Queue.claim(queue, "old-reader", lease, 1, 8192)

    assert metadata.attempt == 1
    assert original.durable?
    :ok = GenServer.stop(queue)
    restored = open!(config)
    assert %{messages: 1, ready: 1, inflight: 0, consumers: 0} = Queue.status(restored)
    assert {:ok, fresh} = Queue.register(restored, "fresh-reader", self())
    assert fresh > lease

    assert {:ok, [{delivery, ^original, metadata}]} =
             Queue.claim(restored, "fresh-reader", fresh, 1, 8192)

    assert delivery.id != old_delivery.id
    assert metadata.attempt == 2
    assert :ok = Queue.settle(restored, "fresh-reader", fresh, delivery.id, %Outcome.Retire{})
    :ok = GenServer.stop(restored)
    empty = open!(config)
    assert Queue.status(empty).messages == 0
  end

  test "sync failure and unknown completion cannot return successful admission", context do
    for {mode, result} <- [{:failed, {:error, :eio}}, {:unknown, {:unknown, :completion_lost}}] do
      config = config(context, Atom.to_string(mode))
      control = :ets.new(:durable_fault, [:public])

      fault = fn
        :wal_sync, _ ->
          case :ets.take(control, :armed) do
            [{:armed, value}] -> value
            [] -> :ok
          end

        _, _ ->
          :ok
      end

      config = put_in(config.persistence.fault_injector, fault)
      queue = open!(config)
      :ets.insert(control, {:armed, result})
      assert {:error, failure} = enqueue(queue, "never-ack-before-sync", true)
      assert failure in [:persistence_failed, :unknown_outcome]
      assert Queue.status(queue).storage == :failed
      assert {:error, :persistence_failed} = enqueue(queue, "subsequent", false)
      :ok = GenServer.stop(queue)
      restored = open!(put_in(config.persistence.fault_injector, nil))
      assert Queue.status(restored).messages in [0, 1]
    end
  end

  test "release, Modified data and Rejected retirement remain exact after recovery", context do
    config = config(context)
    queue = open!(config)
    assert {:ok, {:accepted_durable, _}} = enqueue(queue, "raw-original", true)
    assert {:ok, lease} = Queue.register(queue, "reader", self())
    assert {:ok, [{delivery, original, _}]} = Queue.claim(queue, "reader", lease, 1, 8192)
    assert :ok = Queue.settle(queue, "reader", lease, delivery.id, %Outcome.MakeAvailable{})
    assert {:ok, [{delivery, ^original, _}]} = Queue.claim(queue, "reader", lease, 1, 8192)
    mutation = %Message.Mutation{format: "annotations", encoded_data: <<1, 2, 255>>}

    assert :ok =
             Queue.settle(queue, "reader", lease, delivery.id, %Outcome.ModifyAndMakeAvailable{
               delivery_failed?: true,
               undeliverable_here?: true,
               annotation_mutation: mutation
             })

    :ok = GenServer.stop(queue)
    restored = open!(config)
    assert {:ok, fresh} = Queue.register(restored, "new-reader", self())

    assert {:ok, [{delivery, ^original, meta}]} =
             Queue.claim(restored, "new-reader", fresh, 1, 8192)

    assert meta.attempt == 3
    assert meta.failure_count == 1
    assert meta.mutations == [mutation]

    assert :ok =
             Queue.settle(restored, "new-reader", fresh, delivery.id, %Outcome.Reject{
               reason: "policy",
               description: "discard",
               action: :discard
             })

    :ok = GenServer.stop(restored)
    empty = open!(config)
    assert Queue.status(empty).messages == 0
  end

  test "partial append cannot acknowledge a message and recovery retains the durable prefix",
       context do
    control = :ets.new(:partial_append, [:public])

    fault = fn
      :append_write, _ ->
        case :ets.take(control, :armed) do
          [{:armed, true}] -> {:partial, 21}
          [] -> :ok
        end

      _, _ ->
        :ok
    end

    config = put_in(config(context).persistence.fault_injector, fault)
    queue = open!(config)
    assert {:ok, {:accepted_durable, _}} = enqueue(queue, "confirmed-prefix", true)
    :ets.insert(control, {:armed, true})
    assert {:error, :unknown_outcome} = enqueue(queue, "incomplete-tail", true)
    assert Queue.status(queue).storage == :failed
    :ok = GenServer.stop(queue)
    restored = open!(put_in(config.persistence.fault_injector, nil))
    assert Queue.status(restored).messages == 1
    assert {:ok, lease} = Queue.register(restored, "reader", self())
    assert {:ok, [{_, message, _}]} = Queue.claim(restored, "reader", lease, 1, 8192)
    assert message.encoded_content == "confirmed-prefix"
  end

  test "streaming replay retains claim selection that skipped a larger ready message", context do
    config = config(context)
    queue = open!(config)
    assert {:ok, {:accepted_durable, _}} = enqueue(queue, :binary.copy("large", 300), true)
    assert {:ok, {:accepted_durable, _}} = enqueue(queue, "small", true)
    assert {:ok, lease} = Queue.register(queue, "reader", self())
    assert {:ok, [{_, claimed, _}]} = Queue.claim(queue, "reader", lease, 1, 256)
    assert claimed.encoded_content == "small"
    :ok = GenServer.stop(queue)
    restored = open!(config)
    assert %{messages: 2, ready: 2, inflight: 0} = Queue.status(restored)
    assert {:ok, lease} = Queue.register(restored, "fresh", self())
    assert {:ok, deliveries} = Queue.claim(restored, "fresh", lease, 2, 8192)

    assert Enum.map(deliveries, fn {_, message, _} -> message.encoded_content end) == [
             :binary.copy("large", 300),
             "small"
           ]
  end

  test "one logical transition exceeding a WAL append batch is synchronized through its final event",
       %{tmp_dir: directory} do
    alias GravitonMQ.Core.{MessageId, NodeId}
    alias GravitonMQ.Queue.Machine
    alias GravitonMQ.Runtime.AMQP10.Persistence
    alias GravitonMQ.Storage.WAL

    {:ok, machine} =
      Machine.new(NodeId.new("bulk"),
        incarnation: "stable",
        profile: :local_durable,
        max_messages: 1024,
        max_bytes: 1_048_576,
        max_message_bytes: 512,
        max_inflight: 1024,
        max_batch: 1024
      )

    machine =
      Enum.reduce(1..1024, machine, fn n, machine ->
        message = %Message{
          id: MessageId.new("m-#{n}"),
          encoded_content: <<0>>,
          durable?: true,
          wire_format: %Message.Format{family: "amqp-1.0", identifier: 0},
          index: %Message.Index{}
        }

        {machine, _} = Machine.apply(machine, {:enqueue, :put, message})
        machine
      end)

    {machine, _} = Machine.apply(machine, {:register_consumer, :register, "reader", 1})
    {machine, _} = Machine.apply(machine, {:claim, :claim, "reader", 1, 1024, 1_048_576})
    {_machine, effects} = Machine.apply(machine, {:disconnect, :disconnect, "reader", 1})
    events = for {:event, event} <- effects, do: event
    assert length(events) == 1025

    owner =
      start_supervised!(
        Supervisor.child_spec({WAL, [path: Path.join(directory, "bulk"), stream_id: "bulk"]},
          id: make_ref()
        )
      )

    assert {:ok, %{durable: %CommitRef{position: 1025}}} =
             Persistence.commit(%{owner: owner, durable: :none}, events)

    assert {:ok, 1025} = WAL.fold(owner, :origin, 0, fn _, _, count -> {:cont, count + 1} end)
  end

  defp config(%{tmp_dir: dir}, suffix \\ "state") do
    directory = Path.join(dir, suffix)
    File.mkdir_p!(directory)

    %{
      queue_options: [max_messages: 8, max_bytes: 8192, max_message_bytes: 4096],
      max_connections: 4,
      profile: :local_durable,
      persistence: %{
        directory: directory,
        stream_id: "stable-broker",
        max_record_bytes: 8192,
        max_log_bytes: 1_048_576,
        fault_injector: nil
      }
    }
  end

  defp open!(config) do
    start_supervised!(
      Supervisor.child_spec(Queue.child_spec({config, "orders"}),
        id: make_ref(),
        restart: :temporary
      )
    )
  end

  defp enqueue(queue, raw, durable),
    do:
      Queue.enqueue(
        queue,
        raw,
        %Message.Format{family: "amqp-1.0", identifier: 0},
        %Message.Index{},
        durable
      )
end
