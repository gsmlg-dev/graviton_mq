defmodule GravitonMQ.Queue.ModelTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Core.{CheckpointCodec, DeliveryRef, Message, MessageId, NodeId, Outcome}
  alias GravitonMQ.Queue.Machine

  @seeds [17, 97, 257, 1_009, 4_099, 16_381, 65_537, 202_610_30]
  @steps 500

  # This oracle has a FIFO list, an ownership ledger and accepted/removed
  # counters. It does not call Machine helpers, construct its internal entries,
  # calculate its event IDs or copy its byte-charge formula. Returned delivery
  # references are opaque handles bound to independently predicted messages.
  test "seeded command histories preserve FIFO, responsibility, capacity and checkpoint continuation" do
    coverage =
      for seed <- @seeds do
        :rand.seed(:exsss, {seed, seed + 1, seed + 2})
        {:ok, state} = new()

        model = empty_model()

        {state, model} =
          Enum.reduce(1..@steps, {state, model}, fn step, {state, model} ->
            {state, model} =
              if rem(step, 61) == 0 do
                # Core has no WAL recovery API: qualify portable state restoration
                # plus abandoned-lease release, without claiming disk recovery.
                assert {:ok, checkpoint} = CheckpointCodec.encode(state)
                assert {:ok, restored} = CheckpointCodec.decode(checkpoint)
                assert restored == state
                assert {:error, _} = CheckpointCodec.decode(checkpoint, max_bytes: 1)
                disconnect_all(restored, model, {seed, step, :recovery})
              else
                {state, model}
              end

            command = generated_command(model, seed, step)
            transition(state, model, command, {seed, step})
          end)

        {state, model} = disconnect_all(state, model, {seed, :drain})

        {state, model} =
          transition(
            state,
            model,
            {:register_consumer, :drain_register, "drain", 1_000_000},
            {seed, :drain}
          )

        {state, model} = drain(state, model, seed)
        assert model.accepted == model.removed
        assert state.message_count == 0
        assert state.bytes == 0
        model.coverage
      end

    coverage =
      Enum.reduce(coverage, %{}, fn counts, all ->
        Map.merge(all, counts, fn _key, left, right -> left + right end)
      end)

    for key <- [
          {:enqueue, {:ok, :enqueued}},
          {:register_consumer, {:ok, :registered}},
          {:claim, {:ok, 1}},
          {:settle, {:ok, :settled}},
          {:disconnect, {:ok, :disconnected}},
          {:enqueue, {:error, :queue_full}},
          {:enqueue, {:error, :duplicate_message}},
          {:enqueue, {:error, :message_too_large}},
          {:claim, {:error, :invalid_claim}},
          {:claim, {:error, :stale_consumer}},
          {:settle, {:error, :unknown_delivery}},
          {:register_consumer, {:error, :consumer_limit}}
        ] do
      assert Map.get(coverage, key, 0) > 0, "missing generated coverage: #{inspect(key)}"
    end

    IO.puts(
      "G7 queue transitions=#{Enum.sum(Map.values(coverage))} coverage=#{inspect(Enum.sort(coverage), limit: :infinity)}"
    )

    IO.puts(
      "G7 queue model: #{length(@seeds)} seeds x #{@steps} generated commands; checkpoint every 61 commands; seeds=#{inspect(@seeds)}"
    )
  end

  test "count and logical byte exhaustion reject publication without dropping claimed responsibility" do
    for {options, messages} <- [
          {[max_messages: 1], [message("first", "one"), message("second", "two")]},
          {[max_bytes: 1], [message("first", "one")]}
        ] do
      {:ok, initial} = new(options)
      {state, _} = Machine.apply(initial, {:register_consumer, :register, "reader", 1})
      {state, effects} = Machine.apply(state, {:enqueue, :first, hd(messages)})

      if length(messages) == 1 do
        assert effects == [{:operation, :first, {:error, :queue_full}}]
        assert state.messages == %{}
      else
        assert {:operation, :first, {:ok, :enqueued}} in effects
        {claimed, effects} = Machine.apply(state, {:claim, :claim, "reader", 1, 1, 100_000})
        assert [{:delivery, _, _, _}] = for(effect = {:delivery, _, _, _} <- effects, do: effect)

        assert {^claimed, [{:operation, :full, {:error, :queue_full}}]} =
                 Machine.apply(claimed, {:enqueue, :full, List.last(messages)})

        assert claimed.bytes == state.bytes
        assert claimed.message_count == 1
      end
    end
  end

  test "the oracle detects loss, duplication, reorder, ownership and opaque-content corruption" do
    commands = [
      {:enqueue, :one, message("one", <<0, 255>>)},
      {:enqueue, :two, message("two", "two")},
      {:enqueue, :three, message("three", "three")},
      {:register_consumer, :register_a, "a", 1},
      {:register_consumer, :register_b, "b", 2},
      {:claim, :claim, "a", 1, 1, 100_000}
    ]

    {:ok, initial} = new()

    {state, model} =
      Enum.reduce(commands, {initial, empty_model()}, fn command, {state, model} ->
        transition(state, model, command, :oracle_sensitivity)
      end)

    id = MessageId.new("one")
    entry = Map.fetch!(state.messages, id)

    forged = [
      %{state | ready: :queue.from_list(Enum.reverse(:queue.to_list(state.ready)))},
      %{state | ready: :queue.in(MessageId.new("two"), state.ready)},
      %{state | messages: Map.delete(state.messages, id)},
      %{state | claims: %{}},
      %{state | bytes: state.bytes + 1},
      %{
        state
        | messages:
            Map.put(state.messages, id, %{
              entry
              | message: %{entry.message | encoded_content: "corrupted"}
            })
      }
    ]

    for tampered <- forged do
      assert_raise ExUnit.AssertionError, fn ->
        assert_model(tampered, model, :intentional_corruption)
      end
    end

    [claim] = model.claims

    {same, _model} =
      transition(
        state,
        model,
        {:settle, :wrong_owner, "b", 2, claim.ref, %Outcome.Retire{}},
        :ownership
      )

    assert same == state
  end

  defp empty_model do
    %{
      ready: [],
      messages: %{},
      claims: [],
      consumers: %{},
      accepted: 0,
      removed: 0,
      history: [],
      last_generation: -1,
      coverage: %{}
    }
  end

  defp new(options \\ []) do
    Machine.new(
      NodeId.new("generated-queue"),
      Keyword.merge(
        [
          incarnation: "g7-model",
          profile: :local_durable,
          max_messages: 7,
          max_bytes: 4096,
          max_message_bytes: 512,
          max_consumers: 2,
          max_inflight: 3,
          max_batch: 2
        ],
        options
      )
    )
  end

  defp generated_command(model, seed, step) do
    op = {seed, step}
    consumer = Enum.at(["a", "b", "c"], :rand.uniform(3) - 1)
    generation = Map.get(model.consumers, consumer, 0)
    generation = if rem(step, 19) == 0, do: generation - 1, else: generation

    case :rand.uniform(100) do
      n when n <= 30 ->
        content =
          if rem(step, 17) == 0,
            do: :binary.copy("x", 513),
            else: <<seed::32, step::16, :binary.copy(<<rem(step, 256)>>, rem(step, 20))::binary>>

        {:enqueue, op, message("m#{:rand.uniform(15)}", content)}

      n when n <= 55 ->
        count = if rem(step, 23) == 0, do: 3, else: :rand.uniform(2)
        bytes = if rem(step, 7) == 0, do: 1, else: 100_000
        {:claim, op, consumer, generation, count, bytes}

      n when n <= 75 ->
        claim =
          if model.claims != [] and rem(step, 4) != 0,
            do: Enum.at(model.claims, :rand.uniform(length(model.claims)) - 1),
            else: nil

        {consumer, generation, ref} =
          if claim do
            {claim.consumer, claim.generation, claim.ref}
          else
            ref = if model.history == [], do: DeliveryRef.new("absent"), else: hd(model.history)
            {consumer, generation, ref}
          end

        consumer = if rem(step, 13) == 0, do: "wrong-owner", else: consumer

        outcome =
          case rem(step, 3) do
            0 -> %Outcome.Retire{}
            1 -> %Outcome.MakeAvailable{}
            2 -> %Outcome.Reject{reason: "model-discard", action: :discard}
          end

        {:settle, op, consumer, generation, ref, outcome}

      n when n <= 90 ->
        {:register_consumer, op, consumer, step}

      _ ->
        {:disconnect, op, consumer, generation}
    end
  end

  defp transition(state, model, command, context) do
    {expected, predicted, ids} = predict(model, command)
    {next, effects} = Machine.apply(state, command)
    op = elem(command, 1)

    assert [{:operation, ^op, ^expected}] =
             for(effect = {:operation, _, _} <- effects, do: effect),
           "context=#{inspect(context)} command=#{inspect(command)}"

    deliveries =
      for {:delivery, delivery, message, metadata} <- effects, do: {delivery, message, metadata}

    assert Enum.map(deliveries, fn {delivery, _, _} -> delivery.message_id.value end) == ids,
           "FIFO/responsibility mismatch at #{inspect(context)}"

    predicted =
      Enum.reduce(deliveries, predicted, fn {delivery, delivered, metadata}, acc ->
        id = delivery.message_id.value
        entry = Map.fetch!(acc.messages, id)
        claim = Enum.find(acc.claims, &(&1.id == id))
        assert delivered == entry.message
        assert delivery.consumer_id == claim.consumer
        assert delivery.attempt == entry.attempt
        assert metadata.attempt == entry.attempt
        assert metadata.generation == claim.generation
        assert metadata.failure_count == 0
        assert metadata.mutations == []

        assert is_integer(metadata.charged_bytes) and
                 metadata.charged_bytes > byte_size(delivered.encoded_content)

        %{
          acc
          | claims:
              Enum.map(acc.claims, fn item ->
                if item.id == id, do: Map.put(item, :ref, delivery.id), else: item
              end),
            history: [delivery.id | acc.history]
        }
      end)

    events = for {:event, event} <- effects, do: event

    assert Enum.map(events, & &1.sequence) ==
             Enum.take(Stream.iterate(state.event_sequence + 1, &(&1 + 1)), length(events))

    assert length(Enum.uniq(Enum.map(events, & &1.id))) == length(events)
    assert next.event_sequence == state.event_sequence + length(events)

    if match?({:error, _}, expected) do
      assert next == state
      assert effects == [{:operation, op, expected}]
    end

    if elem(command, 0) not in [:enqueue, :settle], do: assert(next.bytes == state.bytes)
    assert_model(next, predicted, context)
    key = {elem(command, 0), expected}
    predicted = %{predicted | coverage: Map.update(predicted.coverage, key, 1, &(&1 + 1))}
    {next, predicted}
  end

  defp predict(model, {:enqueue, _, message}) do
    id = message.id.value

    cond do
      byte_size(message.encoded_content) > 512 ->
        unchanged(model, :message_too_large)

      Map.has_key?(model.messages, id) ->
        unchanged(model, :duplicate_message)

      map_size(model.messages) == 7 ->
        unchanged(model, :queue_full)

      true ->
        {{:ok, :enqueued},
         %{
           model
           | ready: model.ready ++ [id],
             messages: Map.put(model.messages, id, %{message: message, attempt: 0}),
             accepted: model.accepted + 1
         }, []}
    end
  end

  defp predict(model, {:register_consumer, _, consumer, generation}) do
    cond do
      generation <= model.last_generation ->
        unchanged(model, :stale_generation)

      not Map.has_key?(model.consumers, consumer) and map_size(model.consumers) == 2 ->
        unchanged(model, :consumer_limit)

      true ->
        model = release_owner(model, consumer)

        {{:ok, :registered},
         %{
           model
           | consumers: Map.put(model.consumers, consumer, generation),
             last_generation: generation
         }, []}
    end
  end

  defp predict(model, {:claim, _, consumer, generation, count, bytes}) do
    cond do
      Map.get(model.consumers, consumer) != generation ->
        unchanged(model, :stale_consumer)

      count not in 1..2 ->
        unchanged(model, :invalid_claim)

      true ->
        slots = if bytes == 1, do: 0, else: min(count, 3 - length(model.claims))
        {ids, ready} = Enum.split(model.ready, slots)

        messages =
          Enum.reduce(ids, model.messages, fn id, acc ->
            Map.update!(acc, id, &%{&1 | attempt: &1.attempt + 1})
          end)

        claims =
          model.claims ++
            Enum.map(ids, &%{id: &1, consumer: consumer, generation: generation, ref: nil})

        {{:ok, length(ids)}, %{model | ready: ready, messages: messages, claims: claims}, ids}
    end
  end

  defp predict(model, {:settle, _, consumer, generation, ref, outcome}) do
    claim = Enum.find(model.claims, &(&1.ref == ref))

    cond do
      Map.get(model.consumers, consumer) != generation ->
        unchanged(model, :stale_consumer)

      is_nil(claim) ->
        unchanged(model, :unknown_delivery)

      claim.consumer != consumer or claim.generation != generation ->
        unchanged(model, :wrong_owner)

      true ->
        model = %{model | claims: Enum.reject(model.claims, &(&1.ref == ref))}

        model =
          if is_struct(outcome, Outcome.MakeAvailable) do
            %{model | ready: model.ready ++ [claim.id]}
          else
            %{model | messages: Map.delete(model.messages, claim.id), removed: model.removed + 1}
          end

        {{:ok, :settled}, model, []}
    end
  end

  defp predict(model, {:disconnect, _, consumer, generation}) do
    if Map.get(model.consumers, consumer) != generation do
      unchanged(model, :stale_consumer)
    else
      model = release_owner(model, consumer)
      {{:ok, :disconnected}, %{model | consumers: Map.delete(model.consumers, consumer)}, []}
    end
  end

  defp unchanged(model, reason), do: {{:error, reason}, model, []}

  defp release_owner(model, consumer) do
    {released, retained} = Enum.split_with(model.claims, &(&1.consumer == consumer))
    %{model | claims: retained, ready: model.ready ++ Enum.map(released, & &1.id)}
  end

  defp assert_model(state, model, context) do
    assert :queue.to_list(state.ready) |> Enum.map(& &1.value) == model.ready,
           "ready order mismatch at #{inspect(context)}"

    assert state.ready_count == length(model.ready)
    assert state.message_count == map_size(model.messages)
    assert model.accepted - model.removed == state.message_count

    assert MapSet.new(Map.keys(state.messages) |> Enum.map(& &1.value)) ==
             MapSet.new(Map.keys(model.messages))

    actual_claims =
      for {_ref, claim} <- state.claims,
          do: {claim.message_id.value, claim.consumer_id, claim.generation}

    assert MapSet.new(actual_claims) ==
             MapSet.new(Enum.map(model.claims, &{&1.id, &1.consumer, &1.generation}))

    all = model.ready ++ Enum.map(model.claims, & &1.id)
    assert length(all) == length(Enum.uniq(all))
    assert MapSet.new(all) == MapSet.new(Map.keys(model.messages))
    assert state.consumers == model.consumers
    assert state.bytes == Enum.sum(for {_id, entry} <- state.messages, do: entry.cost)
    assert state.bytes >= 0 and state.bytes <= state.limits.max_bytes
    assert state.message_count <= state.limits.max_messages
    assert map_size(state.claims) <= state.limits.max_inflight

    for {id, expected} <- model.messages do
      entry = Map.fetch!(state.messages, MessageId.new(id))
      assert entry.message == expected.message
      assert entry.attempt == expected.attempt
      assert entry.status == if(id in model.ready, do: :ready, else: :claimed)
    end
  end

  defp disconnect_all(state, model, context) do
    Enum.reduce(Enum.sort(model.consumers), {state, model}, fn {consumer, generation},
                                                               {state, model} ->
      transition(state, model, {:disconnect, {context, consumer}, consumer, generation}, context)
    end)
  end

  defp drain(state, %{ready: []} = model, _seed), do: {state, model}

  defp drain(state, model, seed) do
    {state, model} =
      transition(
        state,
        model,
        {:claim, :drain_claim, "drain", 1_000_000, 2, 100_000},
        {seed, :drain}
      )

    {state, model} =
      Enum.reduce(model.claims, {state, model}, fn claim, {state, model} ->
        command = {:settle, :drain_retire, "drain", 1_000_000, claim.ref, %Outcome.Retire{}}
        {state, model} = transition(state, model, command, {seed, :drain})
        transition(state, model, command, {seed, :duplicate_settlement})
      end)

    drain(state, model, seed)
  end

  defp message(id, content) do
    %Message{
      id: MessageId.new(id),
      encoded_content: content,
      wire_format: %Message.Format{family: "test-opaque", identifier: 0},
      index: %Message.Index{},
      durable?: true
    }
  end
end
