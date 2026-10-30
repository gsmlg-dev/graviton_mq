defmodule GravitonMQ.Queue.MachineTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Core.{Delivery, DeliveryRef, Message, MessageId, NodeId, Outcome}
  alias GravitonMQ.Queue.{Event, EventId, Machine}

  test "explicit stable incarnation and positive bounded options are required" do
    assert {:error, :invalid_options} = Machine.new(NodeId.new("queue"), [])
    assert {:error, :invalid_options} = Machine.new(NodeId.new("queue"), incarnation: self())

    assert {:error, :invalid_options} =
             Machine.new(NodeId.new("queue"), incarnation: "i", max_batch: 0)

    assert {:error, :invalid_node_id} = Machine.new("queue", incarnation: "i")

    assert {:error, :unsupported_profile} =
             Machine.new(NodeId.new("queue"), incarnation: "i", profile: :disk)

    assert {:ok, state} = Machine.new(NodeId.new("queue"), incarnation: "i")
    assert state.profile == :memory
  end

  test "same input produces the same states effects and stable event identities" do
    commands = [
      {:enqueue, :put, message("m", <<0, 255, 1>>)},
      {:register_consumer, :register, "reader", 1},
      {:claim, :pull, "reader", 1, 1, 4096}
    ]

    assert run(new(), commands) == run(new(), commands)
    {state, effects} = run(new(), commands)
    assert state.message_count == 1
    assert state.ready_count == 0
    assert map_size(state.claims) == 1
    events = for {:event, event} <- effects, do: event
    assert Enum.map(events, & &1.sequence) == [1, 2, 3]
    assert length(Enum.uniq(Enum.map(events, & &1.id))) == 3
    assert Enum.all?(events, &event_data?/1)
    assert Enum.any?(effects, &match?({:operation, :pull, {:ok, 1}}, &1))
    assert [{:delivery, %Delivery{attempt: 1}, delivered, metadata}] = deliveries(effects)
    assert delivered == message("m", <<0, 255, 1>>)
    assert metadata.generation == 1
    assert metadata.attempt == 1
    assert metadata.failure_count == 0
    assert metadata.mutations == []
  end

  test "operation runtime references never enter canonical state or logical events" do
    op = make_ref()
    {state, effects} = Machine.apply(new(), {:enqueue, op, message("m")})
    assert {:operation, ^op, {:ok, :enqueued}} = List.last(effects)
    refute contains_runtime?(state)
    for {:event, event} <- effects, do: assert(event_data?(event))
  end

  test "memory profile refuses durability and expiry without changing state" do
    state = new()

    for {msg, reason} <- [
          {%{message("durable") | durable?: true}, :durability_unsupported},
          {%{message("expired") | index: %Message.Index{expires_at: 1}}, :expiry_unsupported}
        ] do
      assert {^state, [{:operation, :put, {:error, ^reason}}]} =
               Machine.apply(state, {:enqueue, :put, msg})
    end
  end

  test "admission validates bounded pure message data before storing or allocating events" do
    state = new(max_message_bytes: 256, max_bytes: 512)
    oversized = message("large", :binary.copy(<<1>>, 257))

    assert {^state, [{:operation, :put, {:error, :message_too_large}}]} =
             Machine.apply(state, {:enqueue, :put, oversized})

    invalid = %{message("bad") | index: %Message.Index{routing: %{"pid" => self()}}}

    assert {^state, [{:operation, :put, {:error, :invalid_message}}]} =
             Machine.apply(state, {:enqueue, :put, invalid})

    deeply_nested = Enum.reduce(1..40, nil, fn _, acc -> [acc] end)
    invalid = %{message("deep") | index: %Message.Index{routing: %{"deep" => deeply_nested}}}

    assert {^state, [{:operation, :put, {:error, :invalid_message}}]} =
             Machine.apply(state, {:enqueue, :put, invalid})

    wide = %{
      message("wide")
      | index: %Message.Index{routing: %{"many" => List.duplicate(0, 1000)}}
    }

    assert {^state, [{:operation, :put, {:error, :message_too_large}}]} =
             Machine.apply(state, {:enqueue, :put, wide})
  end

  test "ready and claimed content share one capacity charge and rejected duplicates are atomic" do
    {state, _} =
      run(new(max_messages: 1), [
        {:enqueue, :put, message("m")},
        {:register_consumer, :reg, "c", 1}
      ])

    {claimed, _} = Machine.apply(state, {:claim, :pull, "c", 1, 1, 4096})
    assert claimed.bytes == state.bytes
    assert claimed.message_count == 1

    assert {^claimed, [{:operation, :dup, {:error, :duplicate_message}}]} =
             Machine.apply(claimed, {:enqueue, :dup, message("m")})

    assert {^claimed, [{:operation, :full, {:error, :queue_full}}]} =
             Machine.apply(claimed, {:enqueue, :full, message("n")})
  end

  test "total byte capacity rejects before canonical insertion" do
    {state, _} = Machine.apply(new(), {:enqueue, :put, message("m")})
    tight = new(max_bytes: state.bytes)
    {tight, _} = Machine.apply(tight, {:enqueue, :put, message("m")})

    assert {^tight, [{:operation, :full, {:error, :queue_full}}]} =
             Machine.apply(tight, {:enqueue, :full, message("n")})
  end

  test "pull claims obey batch byte and global inflight limits and preserve eligible FIFO" do
    {state, _} =
      run(new(max_batch: 2, max_inflight: 1), [
        {:enqueue, :a, message("a")},
        {:enqueue, :b, message("b")},
        {:register_consumer, :reg, "c", 1}
      ])

    assert {^state, [{:operation, :too_many, {:error, :invalid_claim}}]} =
             Machine.apply(state, {:claim, :too_many, "c", 1, 3, 4096})

    assert {^state, [{:operation, :small, {:ok, 0}}]} =
             Machine.apply(state, {:claim, :small, "c", 1, 2, 1})

    {state, effects} = Machine.apply(state, {:claim, :pull, "c", 1, 2, 4096})
    assert [{:delivery, delivery, msg, _}] = deliveries(effects)
    assert msg.id == MessageId.new("a")

    assert {^state, [{:operation, :full, {:ok, 0}}]} =
             Machine.apply(state, {:claim, :full, "c", 1, 2, 4096})

    {state, _} =
      Machine.apply(state, {:settle, :release, "c", 1, delivery.id, %Outcome.MakeAvailable{}})

    {_state, effects} = Machine.apply(state, {:claim, :pull2, "c", 1, 1, 4096})
    assert [{:delivery, _, msg, _}] = deliveries(effects)
    assert msg.id == MessageId.new("b")
  end

  test "successful retirement is terminal exactly once and releases every charge" do
    {state, delivery, _} = claimed()

    {retired, effects} =
      Machine.apply(state, {:settle, :settle, "c", 1, delivery.id, %Outcome.Retire{}})

    assert retired.message_count == 0
    assert retired.bytes == 0
    assert retired.ready_count == 0
    assert retired.claims == %{}
    assert event_types(effects) == ["message_retired"]

    assert {^retired, [{:operation, :again, {:error, :unknown_delivery}}]} =
             Machine.apply(retired, {:settle, :again, "c", 1, delivery.id, %Outcome.Retire{}})
  end

  test "plain release keeps opaque bytes and increases attempt without counting a failed delivery" do
    {state, delivery, original} = claimed()

    {state, effects} =
      Machine.apply(state, {:settle, :release, "c", 1, delivery.id, %Outcome.MakeAvailable{}})

    assert event_types(effects) == ["message_released"]
    assert {:available, state.node_id} in effects
    {_state, effects} = Machine.apply(state, {:claim, :pull, "c", 1, 1, 4096})
    assert [{:delivery, %Delivery{attempt: 2}, ^original, metadata}] = deliveries(effects)
    assert metadata.failure_count == 0
  end

  test "rejection retains policy reason description and opaque details while dead letter is refused" do
    {state, delivery, _} = claimed()
    details = %Message.Mutation{format: "error-info", encoded_data: <<1, 255>>}

    rejection = %Outcome.Reject{
      reason: "policy",
      description: "denied",
      details: details,
      action: :dead_letter
    }

    assert {^state, [{:operation, :reject, {:error, :dead_letter_unsupported}}]} =
             Machine.apply(state, {:settle, :reject, "c", 1, delivery.id, rejection})

    {state, effects} =
      Machine.apply(
        state,
        {:settle, :reject, "c", 1, delivery.id, %{rejection | action: :discard}}
      )

    assert state.message_count == 0

    assert [
             {:event, %Event{type: "message_rejected", data: data}},
             {:operation, :reject, {:ok, :settled}}
           ] = effects

    assert data["reason"] == "policy"
    assert data["description"] == "denied"
    assert data["details"] == %{"format" => "error-info", "encoded_data" => <<1, 255>>}
    assert data["action"] == "discard"
  end

  test "Modified retains flags ordered opaque deltas and excludes only that consumer lease" do
    {state, delivery, original} = claimed()
    first = %Message.Mutation{format: "annotations", encoded_data: <<1, 2>>}
    second = %Message.Mutation{format: "annotations", encoded_data: <<3>>}

    modify = %Outcome.ModifyAndMakeAvailable{
      delivery_failed?: true,
      undeliverable_here?: true,
      annotation_mutation: first
    }

    {state, effects} = Machine.apply(state, {:settle, :modify, "c", 1, delivery.id, modify})
    assert event_types(effects) == ["message_modified"]

    assert {^state, [{:operation, :blocked, {:ok, 0}}]} =
             Machine.apply(state, {:claim, :blocked, "c", 1, 1, 4096})

    {state, _} = Machine.apply(state, {:register_consumer, :reg, "other", 2})
    {state, effects} = Machine.apply(state, {:claim, :pull, "other", 2, 1, 4096})

    assert [{:delivery, %Delivery{attempt: 2} = delivery, ^original, metadata}] =
             deliveries(effects)

    assert metadata.failure_count == 1
    assert metadata.delivery_failed?
    assert metadata.undeliverable_here?
    assert metadata.mutations == [first]

    {state, _} =
      Machine.apply(
        state,
        {:settle, :modify, "other", 2, delivery.id,
         %Outcome.ModifyAndMakeAvailable{annotation_mutation: second}}
      )

    {_state, effects} = Machine.apply(state, {:claim, :pull, "other", 2, 1, 4096})
    assert [{:delivery, %Delivery{attempt: 3}, ^original, metadata}] = deliveries(effects)
    assert metadata.failure_count == 1
    refute metadata.delivery_failed?
    assert metadata.mutations == [first, second]
  end

  test "mutation budget rejects atomically and empty deltas cannot grow a list without bound" do
    {state, delivery, _} = claimed(max_mutation_bytes: 3)
    empty = %Message.Mutation{format: "", encoded_data: ""}
    big = %Message.Mutation{format: "", encoded_data: <<1, 2, 3>>}
    modify = fn mutation -> %Outcome.ModifyAndMakeAvailable{annotation_mutation: mutation} end

    assert {^state, [{:operation, :big, {:error, :mutation_limit}}]} =
             Machine.apply(state, {:settle, :big, "c", 1, delivery.id, modify.(big)})

    {state, delivery} =
      Enum.reduce(1..3, {state, delivery}, fn _, {state, delivery} ->
        {state, _} = Machine.apply(state, {:settle, :modify, "c", 1, delivery.id, modify.(empty)})
        {state, effects} = Machine.apply(state, {:claim, :pull, "c", 1, 1, 4096})
        [{:delivery, next, _, _}] = deliveries(effects)
        {state, next}
      end)

    assert {^state, [{:operation, :full, {:error, :mutation_limit}}]} =
             Machine.apply(state, {:settle, :full, "c", 1, delivery.id, modify.(empty)})
  end

  test "wrong and stale ownership cannot settle or disconnect a current claim" do
    {state, delivery, _} = claimed()
    {state, _} = Machine.apply(state, {:register_consumer, :reg, "other", 2})

    assert {^state, [{:operation, :wrong, {:error, :wrong_owner}}]} =
             Machine.apply(state, {:settle, :wrong, "other", 2, delivery.id, %Outcome.Retire{}})

    for command <- [
          {:claim, :old, "c", 0, 1, 4096},
          {:settle, :old, "c", 0, delivery.id, %Outcome.Retire{}},
          {:disconnect, :old, "c", 0}
        ] do
      assert {^state, [{:operation, :old, {:error, :stale_consumer}}]} =
               Machine.apply(state, command)
    end

    assert {^state, [{:operation, :unknown, {:error, :unknown_delivery}}]} =
             Machine.apply(
               state,
               {:settle, :unknown, "c", 1, DeliveryRef.new("unknown"), %Outcome.Retire{}}
             )
  end

  test "disconnect and lease replacement return responsibility once and fence old traffic" do
    {state, delivery, original} = claimed()
    {state, effects} = Machine.apply(state, {:register_consumer, :replace, "c", 2})
    assert state.ready_count == 1
    assert state.claims == %{}
    assert event_types(effects) == ["message_released", "consumer_registered"]

    assert {^state, [{:operation, :old, {:error, :stale_consumer}}]} =
             Machine.apply(state, {:disconnect, :old, "c", 1})

    assert {^state, [{:operation, :old, {:error, :stale_consumer}}]} =
             Machine.apply(state, {:settle, :old, "c", 1, delivery.id, %Outcome.Retire{}})

    {state, effects} = Machine.apply(state, {:claim, :pull, "c", 2, 1, 4096})
    assert [{:delivery, %Delivery{attempt: 2}, ^original, _}] = deliveries(effects)
    {state, effects} = Machine.apply(state, {:disconnect, :down, "c", 2})
    assert state.ready_count == 1
    assert state.claims == %{}
    assert state.consumers == %{}
    assert event_types(effects) == ["message_released", "consumer_disconnected"]

    assert {^state, [{:operation, :again, {:error, :stale_consumer}}]} =
             Machine.apply(state, {:disconnect, :again, "c", 2})
  end

  test "node-wide registration high-water permits bounded active-slot churn without tombstones" do
    state = new(max_consumers: 1)

    state =
      Enum.reduce(1..20, state, fn generation, state ->
        id = "consumer-#{generation}"
        {state, _} = Machine.apply(state, {:register_consumer, :reg, id, generation})
        assert map_size(state.consumers) == 1
        {state, _} = Machine.apply(state, {:disconnect, :down, id, generation})
        assert state.consumers == %{}
        state
      end)

    assert state.last_registration_generation == 20

    assert {^state, [{:operation, :stale, {:error, :stale_generation}}]} =
             Machine.apply(state, {:register_consumer, :stale, "consumer-1", 1})

    {state, _} = Machine.apply(state, {:register_consumer, :reg, "live", 21})

    assert {^state, [{:operation, :full, {:error, :consumer_limit}}]} =
             Machine.apply(state, {:register_consumer, :full, "another", 22})
  end

  test "disconnect removes undeliverable exclusions before stable consumer identity is reused" do
    {state, delivery, _} = claimed()

    {state, _} =
      Machine.apply(
        state,
        {:settle, :modify, "c", 1, delivery.id,
         %Outcome.ModifyAndMakeAvailable{undeliverable_here?: true}}
      )

    {state, _} = Machine.apply(state, {:disconnect, :down, "c", 1})
    {state, _} = Machine.apply(state, {:register_consumer, :reg, "c", 2})
    {_state, effects} = Machine.apply(state, {:claim, :pull, "c", 2, 1, 4096})
    assert length(deliveries(effects)) == 1
  end

  test "counter exhaustion fails without allocating identities or losing responsibility" do
    max = 18_446_744_073_709_551_615
    exhausted = %{new() | event_sequence: max}

    assert {^exhausted, [{:operation, :put, {:error, :counter_exhausted}}]} =
             Machine.apply(exhausted, {:enqueue, :put, message("m")})

    {state, _} = run(new(), [{:enqueue, :put, message("m")}, {:register_consumer, :reg, "c", 1}])
    exhausted = %{state | delivery_sequence: max}

    assert {^exhausted, [{:operation, :pull, {:error, :counter_exhausted}}]} =
             Machine.apply(exhausted, {:claim, :pull, "c", 1, 1, 4096})

    exhausted = %{state | last_registration_generation: max}

    assert {^exhausted, [{:operation, :reg, {:error, :generation_exhausted}}]} =
             Machine.apply(exhausted, {:register_consumer, :reg, "new", max})
  end

  test "forged struct extensions or missing fields cannot bypass pure bounded data validation" do
    node_id = Map.put(NodeId.new("queue"), :hidden, self())
    assert {:error, :invalid_node_id} = Machine.new(node_id, incarnation: "i")

    state = new()

    for invalid <- [
          Map.put(message("extra"), :hidden, self()),
          %{message("id") | id: Map.put(MessageId.new("id"), :hidden, self())},
          %{message("format") | wire_format: Map.put(message("m").wire_format, :hidden, self())},
          %{message("index") | index: Map.put(%Message.Index{}, :hidden, self())},
          %{message("missing") | index: Map.delete(%Message.Index{}, :routing)}
        ] do
      assert {^state, [{:operation, :put, {:error, :invalid_message}}]} =
               Machine.apply(state, {:enqueue, :put, invalid})
    end

    {state, delivery, _} = claimed()
    mutation = Map.put(%Message.Mutation{format: "a", encoded_data: "b"}, :hidden, self())
    outcome = %Outcome.ModifyAndMakeAvailable{annotation_mutation: mutation}

    assert {^state, [{:operation, :modify, {:error, :invalid_outcome}}]} =
             Machine.apply(state, {:settle, :modify, "c", 1, delivery.id, outcome})
  end

  test "invalid settlement flags and terms leave the current claim intact" do
    {state, delivery, _} = claimed()

    for outcome <- [
          %Outcome.ModifyAndMakeAvailable{delivery_failed?: :yes},
          %Outcome.ModifyAndMakeAvailable{undeliverable_here?: self()},
          %Outcome.Reject{reason: "bad", action: :discard, details: self()},
          :accepted
        ] do
      assert {^state, [{:operation, :settle, {:error, :invalid_outcome}}]} =
               Machine.apply(state, {:settle, :settle, "c", 1, delivery.id, outcome})
    end
  end

  test "mutation capacity shares the total byte budget and is freed on retirement" do
    {initial, delivery, _} = claimed()
    state = %{initial | limits: %{initial.limits | max_bytes: initial.bytes + 3}}
    mutation = %Message.Mutation{format: "a", encoded_data: "b"}
    outcome = %Outcome.ModifyAndMakeAvailable{annotation_mutation: mutation}
    {state, _} = Machine.apply(state, {:settle, :modify, "c", 1, delivery.id, outcome})
    assert state.bytes == initial.bytes + 3
    {state, effects} = Machine.apply(state, {:claim, :pull, "c", 1, 1, 4096})
    [{:delivery, delivery, _, _}] = deliveries(effects)

    assert {^state, [{:operation, :full, {:error, :mutation_limit}}]} =
             Machine.apply(state, {:settle, :full, "c", 1, delivery.id, outcome})

    {state, _} = Machine.apply(state, {:settle, :retire, "c", 1, delivery.id, %Outcome.Retire{}})
    assert state.bytes == 0
    assert state.messages == %{}
  end

  test "ending a lease with many claims is atomic at event counter exhaustion" do
    {state, _} =
      run(new(), [
        {:enqueue, :a, message("a")},
        {:enqueue, :b, message("b")},
        {:register_consumer, :reg, "c", 1},
        {:claim, :pull, "c", 1, 2, 4096}
      ])

    state = %{state | event_sequence: 18_446_744_073_709_551_614}

    for command <- [{:disconnect, :end, "c", 1}, {:register_consumer, :end, "c", 2}] do
      assert {^state, [{:operation, :end, {:error, :counter_exhausted}}]} =
               Machine.apply(state, command)
    end
  end

  defp new(options \\ []) do
    assert {:ok, state} =
             Machine.new(
               NodeId.new("queue/orders"),
               Keyword.merge([incarnation: "memory-1"], options)
             )

    state
  end

  defp message(id, bytes \\ <<0, 255, 7>>) do
    %Message{
      id: MessageId.new(id),
      encoded_content: bytes,
      wire_format: %Message.Format{family: "opaque", identifier: 0},
      index: %Message.Index{}
    }
  end

  defp claimed(options \\ []) do
    original = message("m")

    {state, effects} =
      run(new(options), [
        {:enqueue, :put, original},
        {:register_consumer, :reg, "c", 1},
        {:claim, :pull, "c", 1, 1, 4096}
      ])

    [{:delivery, delivery, ^original, _}] = deliveries(effects)
    {state, delivery, original}
  end

  defp run(state, commands) do
    Enum.reduce(commands, {state, []}, fn command, {state, effects} ->
      {state, next} = Machine.apply(state, command)
      {state, effects ++ next}
    end)
  end

  defp deliveries(effects), do: Enum.filter(effects, &match?({:delivery, _, _, _}, &1))
  defp event_types(effects), do: for({:event, event} <- effects, do: event.type)

  defp event_data?(%Event{id: %EventId{value: id}, node_id: %NodeId{value: node}, data: data}),
    do: is_binary(id) and is_binary(node) and pure_data?(data)

  defp pure_data?(value)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: true

  defp pure_data?(value) when is_list(value), do: Enum.all?(value, &pure_data?/1)

  defp pure_data?(value) when is_map(value),
    do:
      not is_struct(value) and
        Enum.all?(value, fn {key, item} -> is_binary(key) and pure_data?(item) end)

  defp pure_data?(_), do: false

  defp contains_runtime?(value)
       when is_pid(value) or is_port(value) or is_reference(value) or is_function(value),
       do: true

  defp contains_runtime?(value) when is_map(value),
    do:
      value
      |> Map.to_list()
      |> Enum.any?(fn {key, item} -> contains_runtime?(key) or contains_runtime?(item) end)

  defp contains_runtime?(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.any?(&contains_runtime?/1)

  defp contains_runtime?(value) when is_list(value), do: Enum.any?(value, &contains_runtime?/1)
  defp contains_runtime?(_), do: false
end
