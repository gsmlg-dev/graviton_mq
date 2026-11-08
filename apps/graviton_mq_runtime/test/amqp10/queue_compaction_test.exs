defmodule GravitonMQ.Runtime.AMQP10.QueueCompactionTest do
  # Pure recovery-equivalence checks: no WAL, process or platform dependency.
  use ExUnit.Case, async: true

  alias GravitonMQ.Core.{Message, MessageId, NodeId, Outcome}
  alias GravitonMQ.Queue.{Event, Machine}
  alias GravitonMQ.Runtime.AMQP10.Persistence
  alias GravitonMQ.Storage.WAL.Codec

  @node "amqp-queue/v1:compaction-test"
  @address "orders"
  @options [max_messages: 64, max_bytes: 262_144, max_message_bytes: 4096]
  @record 1024

  test "a compacted history recovers exactly the state of the full history" do
    {machine, history} = scenario()
    assert {:ok, compacted} = Persistence.compaction_events(machine, @address, @record)
    assert length(compacted) > 3, "the snapshot must span several chunk records"
    assert hd(compacted) == hd(history)

    for event <- compacted do
      assert {:ok, size} = Codec.measure(event, :events, @record)
      assert size <= @record
    end

    assert normalize(recover(history)) == normalize(machine)
    assert normalize(recover(compacted)) == normalize(machine)

    # Counters and identities survive, so later events still verify and both
    # histories keep recovering the same state.
    {machine, more} = continue(machine)
    assert more != []
    assert normalize(recover(history ++ more)) == normalize(machine)
    assert normalize(recover(compacted ++ more)) == normalize(machine)

    # A compacted log can itself be compacted again.
    assert {:ok, again} = Persistence.compaction_events(machine, @address, @record)
    assert normalize(recover(again)) == normalize(machine)
  end

  test "per-message counts, mutations, exclusions, claims and ready order are retained" do
    {machine, _history} = scenario()
    assert {:ok, compacted} = Persistence.compaction_events(machine, @address, @record)
    restored = recover(compacted)
    first = Map.fetch!(restored.messages, MessageId.new("m1"))
    assert first.failure_count == 1
    assert first.delivery_failed?
    assert first.undeliverable_here?
    assert first.attempt == 1
    assert [%Message.Mutation{format: "annotations"}] = first.mutations
    assert MapSet.to_list(first.exclusions) == [{"a", 1}]
    assert Map.fetch!(restored.messages, MessageId.new("m3")).status == :claimed

    assert Map.fetch!(restored.messages, MessageId.new("m3")).message.index.routing["weight"] ==
             1.5

    assert map_size(restored.claims) == 1
    assert restored.consumers == %{"a" => 1, "b" => 2, "c" => 3}
    assert Enum.map(:queue.to_list(restored.ready), & &1.value) == ["m1", "m2"]
    assert restored.ready_count == 2
    assert restored.bytes == machine.bytes
    assert restored.event_sequence == machine.event_sequence
    assert restored.delivery_sequence == machine.delivery_sequence
    assert restored.last_registration_generation == 3

    # Exclusion still applies to the original lease after recovery.
    {restored, effects} = Machine.apply(restored, {:claim, :pull, "a", 1, 4, 65_536})
    assert {:operation, :pull, {:ok, 1}} in effects
    assert [{:delivery, delivery, _, _}] = Enum.filter(effects, &match?({:delivery, _, _, _}, &1))
    assert delivery.message_id == MessageId.new("m2")
    assert map_size(restored.claims) == 2
  end

  test "a pristine queue compacts to its identity and an empty snapshot" do
    machine = fresh()

    assert {:ok, [initial, _chunk] = compacted} =
             Persistence.compaction_events(machine, @address, @record)

    assert initial == Persistence.initial_event(machine, @address)
    assert normalize(recover(compacted)) == normalize(machine)
  end

  test "incomplete, reordered, altered or misplaced snapshots are invalid histories" do
    {machine, history} = scenario()
    {:ok, [initial | chunks]} = Persistence.compaction_events(machine, @address, @record)

    assert {:snapshot, _, _, _, _, _} = recover([initial | Enum.drop(chunks, -1)])
    assert {:error, :invalid_queue_history} = recover([initial | Enum.reverse(chunks)])

    [first | rest] = chunks
    altered = %{first | data: %{first.data | "bytes" => "x" <> first.data["bytes"]}}
    assert {:error, :invalid_queue_history} = recover([initial, altered | rest])

    last = List.last(chunks)
    truncated = %{last | data: %{last.data | "bytes" => binary_part(last.data["bytes"], 0, 1)}}

    assert {:error, :invalid_queue_history} =
             recover([initial | Enum.drop(chunks, -1) ++ [truncated]])

    # A snapshot is accepted only directly after the identity event.
    assert {:error, :invalid_queue_history} = recover(Enum.take(history, 2) ++ chunks)
    assert {:error, :invalid_queue_history} = recover(history ++ chunks)
  end

  test "a record limit too small for any snapshot chunk is explicit" do
    assert {:error, :record_limit} = Persistence.compaction_events(fresh(), @address, 256)
  end

  defp scenario do
    machine = fresh()
    mutation = %Message.Mutation{format: "annotations", encoded_data: <<1, 2, 255>>}

    {machine, events, [d1, d2]} =
      run(machine, [
        enqueue("m1"),
        enqueue("m2"),
        enqueue("m3", %{"weight" => 1.5, "tags" => ["x", nil, true]}),
        enqueue("m4"),
        enqueue("m5"),
        {:register_consumer, :op, "a", 1},
        {:register_consumer, :op, "b", 2},
        {:claim, :op, "a", 1, 2, 65_536}
      ])

    {machine, more, [_b, a]} =
      run(machine, [
        {:settle, :op, "a", 1, d1.id,
         %Outcome.ModifyAndMakeAvailable{
           delivery_failed?: true,
           undeliverable_here?: true,
           annotation_mutation: mutation
         }},
        {:settle, :op, "a", 1, d2.id, %Outcome.MakeAvailable{}},
        {:claim, :op, "b", 2, 1, 65_536},
        {:claim, :op, "a", 1, 1, 65_536}
      ])

    events = events ++ more

    {machine, more, [c]} =
      run(machine, [
        {:settle, :op, "a", 1, a.id, %Outcome.Retire{}},
        {:register_consumer, :op, "c", 3},
        {:claim, :op, "c", 3, 1, 65_536}
      ])

    events = events ++ more

    {machine, more, []} =
      run(machine, [
        {:settle, :op, "c", 3, c.id,
         %Outcome.Reject{
           reason: "policy",
           description: "discard",
           details: mutation,
           action: :discard
         }}
      ])

    {machine, [Persistence.initial_event(machine, @address) | events ++ more]}
  end

  defp continue(machine) do
    {machine, events, _deliveries} =
      run(machine, [
        enqueue("m6"),
        {:claim, :op, "a", 1, 4, 65_536},
        {:register_consumer, :op, "d", 4},
        {:disconnect, :op, "a", 1},
        {:register_consumer, :op, "b", 5}
      ])

    {machine, events}
  end

  defp fresh do
    options =
      @options
      |> Keyword.put(:incarnation, "stable-incarnation")
      |> Keyword.put(:profile, :local_durable)

    {:ok, machine} = Machine.new(NodeId.new(@node), options)
    machine
  end

  defp enqueue(id, routing \\ %{}) do
    {:enqueue, :op,
     %Message{
       id: MessageId.new(id),
       encoded_content: :binary.copy(<<id::binary, 0, 255>>, 60),
       durable?: true,
       wire_format: %Message.Format{family: "amqp-1.0", identifier: 0},
       index: %Message.Index{routing: routing, policy: %{"ttl" => nil}}
     }}
  end

  defp run(machine, commands) do
    Enum.reduce(commands, {machine, [], []}, fn command, {machine, events, deliveries} ->
      {machine, effects} = Machine.apply(machine, command)
      assert [{:ok, _}] = for({:operation, :op, result} <- effects, do: result)

      {machine, events ++ for({:event, %Event{} = event} <- effects, do: event),
       deliveries ++ for({:delivery, delivery, _, _} <- effects, do: delivery)}
    end)
  end

  defp recover(events) do
    Enum.reduce_while(events, Persistence.recovery(@node, @address, @options), fn event, acc ->
      case Persistence.replay(event, acc) do
        {:ok, acc} -> {:cont, acc}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp normalize(machine), do: %{machine | ready: :queue.to_list(machine.ready)}
end
