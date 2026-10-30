defmodule GravitonMQ.Queue.Machine do
  @moduledoc """
  Pure deterministic pull-claim queue for bounded single-node profiles.

  `new/2` requires a stable binary incarnation supplied by the owner. No clock,
  process, network or storage operations occur here. `apply/2` returns a new
  state and ordered effects. Failed commands leave state unchanged.

  The memory profile rejects durable messages; the local durable profile emits
  logical events whose synchronization is the runtime owner responsibility.
  Expiry-bearing messages and dead-letter actions are rejected.
  This profile offers at-least-once responsibility, not disk durability or
  exactly-once processing. Release, disconnect and replacement append unresolved
  messages to the ready tail. Claims select eligible messages in FIFO order,
  skipping messages excluded from that consumer lease or over its byte budget.

  Limits bound retained message count, logical byte charge, active consumers,
  claims, claim batches and per-message mutation data. Byte charge includes
  opaque content, identity, format and bounded index data; every mutation costs
  at least one byte, including empty deltas. It is a logical data budget, not
  an estimate of the BEAM heap. Exclusions are bounded by active consumers and
  removed when that lease ends. Identities are binary and at most 1024 bytes;
  counters, lease generations, attempts and failure counts are uint64.
  """

  alias GravitonMQ.Core.{Delivery, DeliveryRef, Message, MessageId, NodeId, Outcome}
  alias GravitonMQ.Queue.{DataBudget, Event, EventId, State}

  @max_counter 18_446_744_073_709_551_615
  @max_identity_bytes 1024
  @defaults %{
    max_messages: 10_000,
    max_bytes: 67_108_864,
    max_message_bytes: 1_048_576,
    max_consumers: 1024,
    max_inflight: 1024,
    max_batch: 128,
    max_mutation_bytes: 65_536
  }

  @type transition :: {GravitonMQ.Queue.State.t(), [GravitonMQ.Queue.Effect.t()]}

  @spec new(NodeId.t(), keyword()) :: {:ok, State.t()} | {:error, atom()}
  def new(node_id, options \\ [])

  def new(%NodeId{value: value} = node_id, options)
      when is_binary(value) and byte_size(value) in 1..@max_identity_bytes and
             map_size(node_id) == 2 do
    with :ok <- validate_options(options),
         :ok <- memory_profile(options) do
      {:ok,
       %State{
         node_id: node_id,
         profile: Keyword.get(options, :profile, :memory),
         incarnation: Keyword.fetch!(options, :incarnation),
         limits: Map.merge(@defaults, Map.new(Keyword.take(options, Map.keys(@defaults))))
       }}
    end
  end

  def new(_, _), do: {:error, :invalid_node_id}

  @spec apply(State.t(), GravitonMQ.Queue.Command.t()) :: transition()
  def apply(%State{} = state, {:enqueue, op, message}) do
    with {:ok, cost} <- validate_message(message, state.limits.max_message_bytes, state.profile),
         :ok <- absent_message(state, message.id),
         :ok <- message_capacity(state, cost),
         :ok <- counters(state, 1, 0) do
      entry = %{
        message: message,
        cost: cost,
        mutation_bytes: 0,
        mutations: [],
        attempt: 0,
        failure_count: 0,
        delivery_failed?: false,
        undeliverable_here?: false,
        exclusions: MapSet.new(),
        status: :ready
      }

      state = %{
        state
        | messages: Map.put(state.messages, message.id, entry),
          ready: :queue.in(message.id, state.ready),
          ready_count: state.ready_count + 1,
          message_count: state.message_count + 1,
          bytes: state.bytes + cost
      }

      {state, event} = event(state, "message_enqueued", message_data(message))
      {state, [event, {:available, state.node_id}, success(op, :enqueued)]}
    else
      {:error, reason} -> failure(state, op, reason)
    end
  end

  def apply(%State{} = state, {:register_consumer, op, consumer_id, generation}) do
    with :ok <- registration(state, consumer_id, generation),
         owned = owned_claims(state, consumer_id),
         :ok <- counters(state, length(owned) + 1, 0) do
      {state, effects} = end_lease(state, consumer_id, owned, "generation_replaced")

      state = %{
        state
        | consumers: Map.put(state.consumers, consumer_id, generation),
          last_registration_generation: generation
      }

      {state, event} = event(state, "consumer_registered", owner_data(consumer_id, generation))
      {state, effects ++ [event] ++ available(state) ++ [success(op, :registered)]}
    else
      {:error, reason} -> failure(state, op, reason)
    end
  end

  def apply(%State{} = state, {:claim, op, consumer_id, generation, count, bytes}) do
    with :ok <- owner(state, consumer_id, generation),
         :ok <- claim_limits(state, count, bytes) do
      slots = min(count, state.limits.max_inflight - map_size(state.claims))
      {selected, kept} = select(state, consumer_id, generation, slots, bytes)

      with :ok <- counters(state, length(selected), length(selected)),
           :ok <- attempt_capacity(state, selected) do
        claim_selected(state, op, consumer_id, generation, selected, kept)
      else
        {:error, reason} -> failure(state, op, reason)
      end
    else
      {:error, reason} -> failure(state, op, reason)
    end
  end

  def apply(%State{} = state, {:settle, op, consumer_id, generation, delivery_ref, outcome}) do
    with :ok <- owner(state, consumer_id, generation),
         {:ok, claim} <- find_claim(state, delivery_ref, consumer_id, generation),
         entry = Map.fetch!(state.messages, claim.message_id),
         {:ok, action} <- settlement(state, entry, outcome),
         :ok <- counters(state, 1, 0) do
      settle(state, op, delivery_ref, claim, entry, action)
    else
      {:error, reason} -> failure(state, op, reason)
    end
  end

  def apply(%State{} = state, {:disconnect, op, consumer_id, generation}) do
    with :ok <- owner(state, consumer_id, generation),
         owned = owned_claims(state, consumer_id),
         :ok <- counters(state, length(owned) + 1, 0) do
      {state, effects} = end_lease(state, consumer_id, owned, "disconnect")
      state = %{state | consumers: Map.delete(state.consumers, consumer_id)}
      {state, event} = event(state, "consumer_disconnected", owner_data(consumer_id, generation))
      {state, effects ++ [event] ++ available(state) ++ [success(op, :disconnected)]}
    else
      {:error, reason} -> failure(state, op, reason)
    end
  end

  def apply(%State{} = state, command) do
    op = if is_tuple(command) and tuple_size(command) >= 2, do: elem(command, 1), else: nil
    failure(state, op, :invalid_command)
  end

  defp validate_options(options) do
    if is_list(options) and length(options) <= map_size(@defaults) + 2 and
         Keyword.keyword?(options) and
         length(options) == map_size(Map.new(options)) and
         Enum.all?(options, fn
           {:incarnation, value} -> identity?(value)
           {:profile, _} -> true
           {key, value} -> Map.has_key?(@defaults, key) and positive_uint?(value)
         end) and Keyword.has_key?(options, :incarnation),
       do: :ok,
       else: {:error, :invalid_options}
  end

  defp memory_profile(options) do
    if Keyword.get(options, :profile, :memory) in [:memory, :local_durable],
      do: :ok,
      else: {:error, :unsupported_profile}
  end

  defp validate_message(
         %Message{
           id: %MessageId{value: id},
           encoded_content: content,
           wire_format: %Message.Format{family: family, identifier: identifier},
           index: %Message.Index{} = index,
           durable?: durable
         } = message,
         limit,
         profile
       ) do
    cond do
      not fields?(message, [:id, :encoded_content, :wire_format, :index, :durable?]) or
        not fields?(message.id, [:value]) or
        not fields?(message.wire_format, [:family, :identifier]) or
        not fields?(index, [:routing, :policy, :expires_at, :durability, :management]) or
        not identity?(id) or not identity?(family) or not uint?(identifier) or
        not is_binary(content) or not is_boolean(durable) ->
        {:error, :invalid_message}

      durable and profile != :local_durable ->
        {:error, :durability_unsupported}

      index.expires_at != nil ->
        {:error, :expiry_unsupported}

      byte_size(content) > limit ->
        {:error, :message_too_large}

      not Enum.all?(
        [index.routing, index.policy, index.durability, index.management],
        &plain_map?/1
      ) ->
        {:error, :invalid_message}

      true ->
        case DataBudget.measure(message_data(message), limit) do
          {:ok, cost} -> {:ok, cost}
          {:error, :limit} -> {:error, :message_too_large}
          {:error, :invalid} -> {:error, :invalid_message}
        end
    end
  end

  defp validate_message(_, _, _), do: {:error, :invalid_message}

  defp absent_message(state, id) do
    if Map.has_key?(state.messages, id), do: {:error, :duplicate_message}, else: :ok
  end

  defp message_capacity(state, cost) do
    if state.message_count < state.limits.max_messages and
         cost <= state.limits.max_bytes - state.bytes,
       do: :ok,
       else: {:error, :queue_full}
  end

  defp registration(state, consumer_id, generation) do
    cond do
      not identity?(consumer_id) or not uint?(generation) ->
        {:error, :invalid_consumer}

      state.last_registration_generation == @max_counter ->
        {:error, :generation_exhausted}

      generation <= state.last_registration_generation ->
        {:error, :stale_generation}

      not Map.has_key?(state.consumers, consumer_id) and
          map_size(state.consumers) >= state.limits.max_consumers ->
        {:error, :consumer_limit}

      true ->
        :ok
    end
  end

  defp owner(state, consumer_id, generation) do
    if identity?(consumer_id) and uint?(generation) and
         Map.get(state.consumers, consumer_id) == generation,
       do: :ok,
       else: {:error, :stale_consumer}
  end

  defp claim_limits(state, count, bytes) do
    if positive_uint?(count) and count <= state.limits.max_batch and positive_uint?(bytes),
      do: :ok,
      else: {:error, :invalid_claim}
  end

  defp select(_state, _consumer_id, _generation, 0, _bytes), do: {[], []}

  defp select(state, consumer_id, generation, slots, bytes) do
    {selected, kept, _, _} =
      Enum.reduce(:queue.to_list(state.ready), {[], [], slots, bytes}, fn id,
                                                                          {selected, kept, slots,
                                                                           bytes} ->
        entry = Map.fetch!(state.messages, id)

        if slots > 0 and entry.cost <= bytes and
             not MapSet.member?(entry.exclusions, {consumer_id, generation}) do
          {[id | selected], kept, slots - 1, bytes - entry.cost}
        else
          {selected, [id | kept], slots, bytes}
        end
      end)

    {Enum.reverse(selected), Enum.reverse(kept)}
  end

  defp attempt_capacity(state, selected) do
    if Enum.any?(selected, &(Map.fetch!(state.messages, &1).attempt == @max_counter)),
      do: {:error, :counter_exhausted},
      else: :ok
  end

  defp claim_selected(state, op, _consumer_id, _generation, [], _kept),
    do: {state, [success(op, 0)]}

  defp claim_selected(state, op, consumer_id, generation, selected, kept) do
    state = %{
      state
      | ready: :queue.from_list(kept),
        ready_count: state.ready_count - length(selected)
    }

    {state, effects} =
      Enum.reduce(selected, {state, []}, fn id, {state, effects} ->
        entry = Map.fetch!(state.messages, id)
        entry = %{entry | status: :claimed, attempt: entry.attempt + 1}
        sequence = state.delivery_sequence + 1
        delivery_ref = DeliveryRef.new(stable_id(state, 1, sequence))
        claim = %{message_id: id, consumer_id: consumer_id, generation: generation}

        state = %{
          state
          | messages: Map.put(state.messages, id, entry),
            claims: Map.put(state.claims, delivery_ref, claim),
            delivery_sequence: sequence
        }

        delivery = %Delivery{
          id: delivery_ref,
          message_id: id,
          consumer_id: consumer_id,
          attempt: entry.attempt
        }

        {state, event} =
          event(
            state,
            "delivery_claimed",
            claim_data(delivery_ref, claim) |> Map.put("attempt", entry.attempt)
          )

        {state, [delivery_effect(delivery, entry, generation), event | effects]}
      end)

    {state, Enum.reverse(effects) ++ [success(op, length(selected))]}
  end

  defp find_claim(state, %DeliveryRef{} = ref, consumer_id, generation) do
    case Map.fetch(state.claims, ref) do
      :error -> {:error, :unknown_delivery}
      {:ok, %{consumer_id: ^consumer_id, generation: ^generation} = claim} -> {:ok, claim}
      {:ok, _} -> {:error, :wrong_owner}
    end
  end

  defp find_claim(_, _, _, _), do: {:error, :unknown_delivery}

  defp settlement(_state, _entry, %Outcome.Retire{}), do: {:ok, :retire}
  defp settlement(_state, _entry, %Outcome.MakeAvailable{}), do: {:ok, :release}

  defp settlement(_state, _entry, %Outcome.Reject{action: :dead_letter}),
    do: {:error, :dead_letter_unsupported}

  defp settlement(state, _entry, %Outcome.Reject{action: :discard} = outcome) do
    if fields?(outcome, [:reason, :description, :details, :action]) and
         identity?(outcome.reason) and optional_binary?(outcome.description) and
         match?({:ok, _}, mutation_cost(outcome.details, state.limits.max_mutation_bytes)),
       do: {:ok, {:reject, outcome}},
       else: {:error, :invalid_outcome}
  end

  defp settlement(state, entry, %Outcome.ModifyAndMakeAvailable{} = outcome) do
    with :ok <- modified_fields(outcome),
         {:ok, cost} <-
           mutation_cost(outcome.annotation_mutation, state.limits.max_mutation_bytes),
         true <- cost <= state.limits.max_mutation_bytes - entry.mutation_bytes,
         true <- cost <= state.limits.max_bytes - state.bytes do
      if outcome.delivery_failed? and entry.failure_count == @max_counter,
        do: {:error, :counter_exhausted},
        else: {:ok, {:modify, outcome, cost}}
    else
      {:error, :invalid_outcome} -> {:error, :invalid_outcome}
      _ -> {:error, :mutation_limit}
    end
  end

  defp settlement(_, _, _), do: {:error, :invalid_outcome}

  defp modified_fields(outcome) do
    if fields?(outcome, [:delivery_failed?, :undeliverable_here?, :annotation_mutation]) and
         is_boolean(outcome.delivery_failed?) and is_boolean(outcome.undeliverable_here?),
       do: :ok,
       else: {:error, :invalid_outcome}
  end

  defp mutation_cost(nil, _limit), do: {:ok, 0}

  defp mutation_cost(%Message.Mutation{format: format, encoded_data: data} = mutation, limit)
       when is_binary(format) and byte_size(format) <= @max_identity_bytes and is_binary(data) and
              map_size(mutation) == 3 do
    cost = 1 + byte_size(format) + byte_size(data)
    if cost <= limit, do: {:ok, cost}, else: {:error, :mutation_limit}
  end

  defp mutation_cost(_, _), do: {:error, :invalid_outcome}

  defp settle(state, op, ref, claim, entry, action) when action in [:retire] do
    state = remove_message(state, ref, claim.message_id, entry)
    {state, event} = event(state, "message_retired", claim_data(ref, claim))
    {state, [event, success(op, :settled)]}
  end

  defp settle(state, op, ref, claim, entry, {:reject, outcome}) do
    data =
      Map.merge(claim_data(ref, claim), %{
        "reason" => outcome.reason,
        "description" => outcome.description,
        "details" => mutation_data(outcome.details),
        "action" => "discard"
      })

    state = remove_message(state, ref, claim.message_id, entry)
    {state, event} = event(state, "message_rejected", data)
    {state, [event, success(op, :settled)]}
  end

  defp settle(state, op, ref, claim, entry, :release) do
    state = requeue(state, ref, claim.message_id, entry)

    {state, event} =
      event(state, "message_released", Map.put(claim_data(ref, claim), "cause", "release"))

    {state, [event, {:available, state.node_id}, success(op, :settled)]}
  end

  defp settle(state, op, ref, claim, entry, {:modify, outcome, cost}) do
    entry = %{
      entry
      | delivery_failed?: outcome.delivery_failed?,
        undeliverable_here?: outcome.undeliverable_here?,
        failure_count: entry.failure_count + if(outcome.delivery_failed?, do: 1, else: 0),
        mutations:
          if(outcome.annotation_mutation,
            do: entry.mutations ++ [outcome.annotation_mutation],
            else: entry.mutations
          ),
        mutation_bytes: entry.mutation_bytes + cost,
        cost: entry.cost + cost,
        exclusions:
          if(outcome.undeliverable_here?,
            do: MapSet.put(entry.exclusions, {claim.consumer_id, claim.generation}),
            else: entry.exclusions
          )
    }

    state = requeue(%{state | bytes: state.bytes + cost}, ref, claim.message_id, entry)

    data =
      Map.merge(claim_data(ref, claim), %{
        "delivery_failed" => outcome.delivery_failed?,
        "undeliverable_here" => outcome.undeliverable_here?,
        "annotation_mutation" => mutation_data(outcome.annotation_mutation)
      })

    {state, event} = event(state, "message_modified", data)
    {state, [event, {:available, state.node_id}, success(op, :settled)]}
  end

  defp remove_message(state, ref, id, entry) do
    %{
      state
      | messages: Map.delete(state.messages, id),
        claims: Map.delete(state.claims, ref),
        message_count: state.message_count - 1,
        bytes: state.bytes - entry.cost
    }
  end

  defp requeue(state, ref, id, entry) do
    %{
      state
      | messages: Map.put(state.messages, id, %{entry | status: :ready}),
        claims: Map.delete(state.claims, ref),
        ready: :queue.in(id, state.ready),
        ready_count: state.ready_count + 1
    }
  end

  defp owned_claims(state, consumer_id) do
    state.claims
    |> Enum.filter(fn {_ref, claim} -> claim.consumer_id == consumer_id end)
    |> Enum.sort_by(fn {ref, _claim} -> ref.value end)
  end

  defp end_lease(state, consumer_id, owned, cause) do
    old_generation = Map.get(state.consumers, consumer_id)

    messages =
      Map.new(state.messages, fn {id, entry} ->
        {id,
         %{entry | exclusions: MapSet.delete(entry.exclusions, {consumer_id, old_generation})}}
      end)

    state = %{state | messages: messages}

    {state, effects} =
      Enum.reduce(owned, {state, []}, fn {ref, claim}, {state, effects} ->
        entry = Map.fetch!(state.messages, claim.message_id)
        state = requeue(state, ref, claim.message_id, entry)

        {state, event} =
          event(state, "message_released", Map.put(claim_data(ref, claim), "cause", cause))

        {state, [event | effects]}
      end)

    {state, Enum.reverse(effects)}
  end

  defp delivery_effect(delivery, entry, generation) do
    {:delivery, delivery, entry.message,
     %{
       generation: generation,
       attempt: entry.attempt,
       failure_count: entry.failure_count,
       delivery_failed?: entry.delivery_failed?,
       undeliverable_here?: entry.undeliverable_here?,
       mutations: entry.mutations,
       charged_bytes: entry.cost
     }}
  end

  defp event(state, type, data) do
    sequence = state.event_sequence + 1

    event = %Event{
      id: EventId.new(stable_id(state, 2, sequence)),
      node_id: state.node_id,
      sequence: sequence,
      type: type,
      data: data
    }

    {%{state | event_sequence: sequence}, {:event, event}}
  end

  defp stable_id(state, kind, sequence) do
    node = state.node_id.value
    incarnation = state.incarnation

    <<"gq", kind, byte_size(node)::32, node::binary, byte_size(incarnation)::32,
      incarnation::binary, sequence::64>>
  end

  defp counters(state, events, deliveries) do
    if state.event_sequence <= @max_counter - events and
         state.delivery_sequence <= @max_counter - deliveries,
       do: :ok,
       else: {:error, :counter_exhausted}
  end

  defp message_data(message) do
    %{
      "message_id" => message.id.value,
      "encoded_content" => message.encoded_content,
      "wire_format" => %{
        "family" => message.wire_format.family,
        "identifier" => message.wire_format.identifier
      },
      "durable" => message.durable?,
      "index" => %{
        "routing" => message.index.routing,
        "policy" => message.index.policy,
        "expires_at" => message.index.expires_at,
        "durability" => message.index.durability,
        "management" => message.index.management
      }
    }
  end

  defp owner_data(consumer_id, generation),
    do: %{"consumer_id" => consumer_id, "generation" => generation}

  defp claim_data(ref, claim),
    do:
      Map.merge(owner_data(claim.consumer_id, claim.generation), %{
        "delivery_ref" => ref.value,
        "message_id" => claim.message_id.value
      })

  defp mutation_data(nil), do: nil

  defp mutation_data(mutation),
    do: %{"format" => mutation.format, "encoded_data" => mutation.encoded_data}

  defp identity?(value), do: is_binary(value) and byte_size(value) in 1..@max_identity_bytes
  defp optional_binary?(nil), do: true
  defp optional_binary?(value), do: is_binary(value) and byte_size(value) <= @max_identity_bytes
  defp uint?(value), do: is_integer(value) and value >= 0 and value <= @max_counter
  defp positive_uint?(value), do: uint?(value) and value > 0
  defp plain_map?(value), do: is_map(value) and not is_struct(value)

  defp fields?(value, keys),
    do: map_size(value) == length(keys) + 1 and Enum.all?(keys, &Map.has_key?(value, &1))

  defp available(%State{ready_count: 0}), do: []
  defp available(state), do: [{:available, state.node_id}]
  defp success(op, result), do: {:operation, op, {:ok, result}}
  defp failure(state, op, reason), do: {state, [{:operation, op, {:error, reason}}]}
end
