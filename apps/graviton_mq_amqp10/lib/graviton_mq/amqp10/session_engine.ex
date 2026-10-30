defmodule GravitonMQ.AMQP10.SessionEngine do
  @moduledoc """
  Pure, finite AMQP 1.0 Session and Link transitions.

  Events contain decoded protocol data, opaque format-zero payloads, write
  results and explicit time facts. Effects reserve finite resources before
  emission. Socket acceptance never completes a publication: a terminal peer
  outcome and peer settlement are required. There is no reconnect or resume.
  """

  alias GravitonMQ.AMQP10.{SessionState, Link, Serial, Value, Outcome, DeliveryState}
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Attach, as: AttachCodec
  alias GravitonMQ.AMQP10.Codec.DeliveryState, as: StateCodec
  alias GravitonMQ.AMQP10.Codec.Performative, as: PDUCodec
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.AMQP10.Performative.{Begin, Attach, Flow, Transfer, Disposition, Detach}
  alias GravitonMQ.AMQP10.Terminus.{Source, Target}

  @defaults [
    incoming_window: 128,
    outgoing_window: 128,
    max_handle: 255,
    max_links: 16,
    max_pending: 64,
    max_outgoing_deliveries: 64,
    max_incoming_deliveries: 64,
    max_message_bytes: 1_048_576,
    max_outgoing_bytes: 4_194_304,
    max_incoming_bytes: 4_194_304,
    max_fragments: 4096,
    max_control_queue: 64,
    max_writes: 64,
    operation_timeout: 5000,
    initial_outgoing_id: 0,
    initial_delivery_id: 0,
    remote_channel: nil,
    peer_attach_policy: :auto
  ]
  @uint_max 0xFFFF_FFFF
  @half 0x8000_0000
  @outcomes [
    "amqp:accepted:list",
    "amqp:rejected:list",
    "amqp:released:list",
    "amqp:modified:list"
  ]

  @spec new(non_neg_integer(), non_neg_integer(), keyword()) ::
          {:ok, SessionState.t()} | {:error, term()}
  def new(id, local_channel, options \\ []) do
    with true <- is_list(options) and Keyword.keyword?(options),
         true <- Enum.all?(Keyword.keys(options), &Keyword.has_key?(@defaults, &1)),
         opts = Map.new(Keyword.merge(@defaults, options)),
         :ok <- validate_options(id, local_channel, opts),
         {:ok, session} <-
           SessionState.new(
             id: id,
             local_channel: local_channel,
             remote_channel: opts.remote_channel
           ) do
      {:ok,
       %{
         session
         | options: opts,
           incoming_window: opts.incoming_window,
           outgoing_window: opts.outgoing_window,
           next_outgoing_id: opts.initial_outgoing_id,
           next_delivery_id: opts.initial_delivery_id
       }}
    else
      false -> {:error, :invalid_session_options}
      {:error, _} = error -> error
    end
  end

  defp validate_options(id, channel, opts) do
    capacities =
      Map.drop(opts, [
        :max_handle,
        :initial_outgoing_id,
        :initial_delivery_id,
        :remote_channel,
        :peer_attach_policy
      ])

    cond do
      not uint?(id) or not integer_between?(channel, 0, 65_535) ->
        {:error, :invalid_session_identity}

      not uint?(opts.max_handle) or not uint?(opts.initial_outgoing_id) or
          not uint?(opts.initial_delivery_id) ->
        {:error, :invalid_session_sequence}

      not (is_nil(opts.remote_channel) or integer_between?(opts.remote_channel, 0, 65_535)) ->
        {:error, :invalid_remote_channel}

      opts.peer_attach_policy not in [:auto, :defer] ->
        {:error, :invalid_peer_attach_policy}

      Enum.any?(capacities, fn {_, value} -> not integer_between?(value, 1, @half - 1) end) ->
        {:error, :invalid_session_limit}

      opts.max_message_bytes > opts.max_incoming_bytes or
          opts.max_message_bytes > opts.max_outgoing_bytes ->
        {:error, :invalid_message_budget}

      true ->
        :ok
    end
  end

  @spec step(SessionState.t(), term(), map()) :: {SessionState.t(), list()}
  def step(%SessionState{} = state, event, ctx) when is_map(ctx) do
    now = Map.get(ctx, :now_ms)

    cond do
      not is_integer(now) ->
        {state, []}

      is_integer(state.last_now_ms) and now < state.last_now_ms ->
        {state, []}

      state.phase == :closed ->
        closed_event(state, event)

      true ->
        state = %{state | last_now_ms: now}

        {state, effects} =
          cond do
            handle_limit_violation?(state, event) ->
              {state, effects} = close(state, :handle_out_of_range, false)
              {state, effects ++ [{:connection_error, :handle_out_of_range}]}

            errant_input?(state, event) ->
              close(state, :errant_link, true)

            true ->
              dispatch(state, event, ctx)
          end

        {state, effects} =
          if state.phase == :closed do
            {state, effects}
          else
            {state, emitted} = flush(state, ctx)
            {state, effects ++ emitted}
          end

        {%{state | timer_effects: []}, effects ++ Enum.reverse(state.timer_effects)}
    end
  end

  def step(%SessionState{} = state, _event, _ctx), do: {state, []}

  @doc """
  Conservative admission capacity for one new send, without reserving it.

  At most one send per Link is offered until its terminal settlement completes.
  The caller must re-query after every transition; capacity is neither credit
  granted to a peer nor a promise that a socket write completes publication.
  Broker senders also expose a fenced drain token. The owner can confirm an
  exhausted external source even when Session windows currently prevent sends.
  """
  @spec send_capacity(SessionState.t(), Link.identity(), map()) ::
          %{
            required(:messages) => 0 | 1,
            required(:bytes) => non_neg_integer(),
            optional(:drain_token) => term()
          }
  def send_capacity(%SessionState{phase: :active} = state, identity, ctx) do
    capacity = do_send_capacity(state, identity, ctx)

    case Map.fetch(state.links_by_identity, identity) do
      {:ok,
       %Link{phase: :attached, role: :sender, drain: true, credit: credit, drain_token: token}}
      when credit > 0 and not is_nil(token) ->
        if state.options.peer_attach_policy == :defer,
          do: Map.put(capacity, :drain_token, token),
          else: capacity

      _ ->
        capacity
    end
  end

  def send_capacity(%SessionState{}, _identity, _ctx), do: %{messages: 0, bytes: 0}

  defp do_send_capacity(state, identity, ctx) do
    with {:ok, %Link{phase: :attached, role: :sender} = link} <-
           Map.fetch(state.links_by_identity, identity),
         true <- link.credit > 0 and state.outgoing_window > 0,
         true <-
           state.remote_incoming_window >
             Serial.distance(state.remote_next_incoming_id, state.next_outgoing_id),
         true <- map_size(state.operations) < state.options.max_pending,
         true <- map_size(state.outgoing_by_id) < state.options.max_outgoing_deliveries,
         true <- map_size(state.writes) < state.options.max_writes,
         false <- Map.has_key?(state.outgoing_by_id, state.next_delivery_id),
         false <- Enum.any?(state.outgoing_by_id, fn {_, entry} -> entry.link == identity end),
         true <- :queue.is_empty(state.control_queue) and map_size(state.pending_flows) == 0,
         true <- Map.get(ctx, :write_frames, 0) > 0,
         pdu = %Transfer{
           handle: Value.uint(link.local_handle),
           delivery_id: Value.uint(state.next_delivery_id),
           delivery_tag: Value.binary(<<Serial.add(state.next_token, 0)::32>>),
           message_format: Value.uint(0),
           settled: Value.boolean(false),
           more: Value.boolean(true)
         },
         frame_limit = Map.get(ctx, :max_frame_size, 512),
         {:ok, overhead} <- encode_size(pdu, frame_limit),
         fragment_bytes = min(frame_limit, Map.get(ctx, :write_bytes, 0)) - overhead,
         true <- fragment_bytes > 0 do
      peer_max =
        if link.peer_max_message_bytes == 0,
          do: link.max_message_bytes,
          else: link.peer_max_message_bytes

      bytes =
        Enum.min([
          link.max_message_bytes,
          peer_max,
          state.options.max_outgoing_bytes - state.outgoing_bytes,
          fragment_bytes * state.options.max_fragments
        ])

      if bytes > 0, do: %{messages: 1, bytes: bytes}, else: %{messages: 0, bytes: 0}
    else
      _ -> %{messages: 0, bytes: 0}
    end
  end

  defp closed_event(state, {:command, op, _}),
    do: {state, [{:complete, op, {:error, :session_closed}}]}

  defp closed_event(state, _), do: {state, []}

  defp dispatch(state, {:transport_down, reason}, _ctx), do: close(state, reason, false)

  defp dispatch(state, {:write_result, receipt, result}, ctx),
    do: write_result(state, receipt, result, ctx)

  defp dispatch(state, {:timeout, {:operation, op}, token}, ctx),
    do: operation_timeout(state, op, token, ctx)

  defp dispatch(state, {:timeout, {:admission, _} = key, {generation, _} = token}, ctx) do
    if generation == Map.get(ctx, :generation, 0),
      do: timer_timeout(state, key, token),
      else: {state, []}
  end

  defp dispatch(state, {:timeout, key, token}, _ctx), do: timer_timeout(state, key, token)

  defp dispatch(state, {:operation_timeout, op}, ctx),
    do: operation_timeout(state, op, :endpoint, ctx)

  defp dispatch(%{phase: :idle} = state, {:local_begin, op}, ctx) do
    with {:ok, state, deadline} <- operation(state, op, :begin, nil, [], ctx),
         {:ok, state} <-
           control(
             %{state | phase: :begin_pending, begin_operation: op},
             local_begin(state),
             {:begin, op, :begin_sent}
           ) do
      {state, deadline}
    else
      {:error, reason} -> {state, [{:complete, op, {:error, reason}}]}
    end
  end

  defp dispatch(state, {:local_begin, op}, _ctx),
    do: {state, [{:complete, op, {:error, :session_already_begun}}]}

  defp dispatch(state, {:peer_begin, %Begin{} = pdu}, _ctx) do
    if state.phase in [:idle, :begin_sent] and valid_begin?(pdu) do
      updated = %{
        state
        | next_incoming_id: number(pdu.next_outgoing_id),
          remote_next_incoming_id: state.next_outgoing_id,
          remote_incoming_window: number(pdu.incoming_window),
          remote_outgoing_window: number(pdu.outgoing_window),
          remote_handle_max: number(pdu.handle_max, @uint_max)
      }

      if state.phase == :idle do
        if is_nil(state.remote_channel) do
          close(state, :invalid_begin_mapping, true)
        else
          case control(
                 %{updated | phase: :begin_reply_pending},
                 local_begin(updated),
                 {:begin, nil, :active}
               ) do
            {:ok, next} -> {next, []}
            {:error, reason} -> close(state, reason, true)
          end
        end
      else
        complete(%{updated | phase: :active}, state.begin_operation, {:ok, state.id})
      end
    else
      close(state, :invalid_begin, true)
    end
  end

  defp dispatch(%{phase: :active} = state, {:command, op, command}, ctx),
    do: command(state, op, command, ctx)

  defp dispatch(state, {:command, op, _}, _ctx),
    do: {state, [{:complete, op, {:error, :session_not_active}}]}

  defp dispatch(%{phase: :active} = state, {:peer, %Transfer{} = pdu, payload}, ctx)
       when is_binary(payload),
       do: incoming_transfer(state, pdu, payload, ctx)

  defp dispatch(%{phase: :active} = state, {:peer, pdu, <<>>}, ctx) do
    case pdu do
      %Attach{} -> peer_attach(state, pdu, ctx)
      %Flow{} -> peer_flow(state, pdu, ctx)
      %Disposition{} -> peer_disposition(state, pdu)
      %Detach{} -> peer_detach(state, pdu)
      _ -> close(state, :unexpected_performative, true)
    end
  end

  defp dispatch(state, :flush, _ctx), do: {state, []}
  defp dispatch(state, _event, _ctx), do: close(state, :unexpected_session_event, true)

  defp valid_begin?(pdu),
    do:
      tagged_uint?(pdu.next_outgoing_id) and tagged_uint?(pdu.incoming_window) and
        tagged_uint?(pdu.outgoing_window) and
        (is_nil(pdu.handle_max) or tagged_uint?(pdu.handle_max))

  defp local_begin(state),
    do: %Begin{
      remote_channel: tagged_optional(:ushort, state.remote_channel),
      next_outgoing_id: Value.uint(state.next_outgoing_id),
      incoming_window: Value.uint(state.incoming_window),
      outgoing_window: Value.uint(state.outgoing_window),
      handle_max: Value.uint(state.options.max_handle)
    }

  defp command(state, op, {:attach, {name, role} = identity, opts}, ctx)
       when role in [:sender, :receiver] and is_list(opts) do
    with true <- valid_name?(name) and Keyword.keyword?(opts),
         true <- Enum.all?(Keyword.keys(opts), &(&1 in [:address, :max_message_bytes, :timeout])),
         address = Keyword.get(opts, :address),
         true <- valid_name?(address),
         maximum = Keyword.get(opts, :max_message_bytes, state.options.max_message_bytes),
         true <- integer_between?(maximum, 1, state.options.max_message_bytes),
         false <- Map.has_key?(state.links_by_identity, identity),
         false <- occupied_link_name?(ctx, identity),
         true <- map_size(state.links_by_identity) < state.options.max_links,
         {:ok, handle} <- allocate_handle(state, ctx),
         {:ok, state, deadline} <- operation(state, op, :attach, identity, opts, ctx),
         link = %Link{
           name: name,
           role: role,
           local_handle: handle,
           phase: :attach_pending,
           source: %Source{
             address: if(role == :receiver, do: address),
             outcomes: Value.array(:symbol, Enum.map(@outcomes, &Value.symbol/1))
           },
           target: if(role == :sender, do: %Target{address: address}, else: %Target{}),
           sender_settle_mode: :unsettled,
           receiver_settle_mode: :first,
           max_message_bytes: maximum,
           attach_operation: op
         },
         {:ok, state} <- SessionState.put_link(state, link),
         {:ok, state} <- control(state, local_attach(link), {:attach, identity, op}) do
      {state, deadline}
    else
      _ -> {state, [{:complete, op, {:error, :attach_not_admitted}}]}
    end
  end

  defp command(state, op, {:send, identity, raw, opts}, ctx)
       when is_binary(raw) and is_list(opts) do
    with {:ok, %Link{phase: :attached, role: :sender} = link} <-
           Map.fetch(state.links_by_identity, identity),
         true <-
           Keyword.keyword?(opts) and
             Enum.all?(Keyword.keys(opts), &(&1 in [:delivery_tag, :timeout])),
         true <-
           byte_size(raw) <= link.max_message_bytes and
             (link.peer_max_message_bytes == 0 or byte_size(raw) <= link.peer_max_message_bytes),
         true <- map_size(state.outgoing_by_id) < state.options.max_outgoing_deliveries,
         true <- state.outgoing_bytes + byte_size(raw) <= state.options.max_outgoing_bytes,
         id = {:pending, state.next_token},
         false <- Map.has_key?(state.outgoing_by_id, id),
         tag =
           Keyword.get(opts, :delivery_tag, Value.binary(<<Serial.add(state.next_token, 0)::32>>)),
         true <- valid_tag?(tag),
         false <- Map.has_key?(link.tag_to_delivery, tag),
         {:ok, state, deadline} <- operation(state, op, :send, identity, opts, ctx) do
      # The admitted message owns its bytes, rather than an arbitrarily large input backing binary.
      entry = %{
        op: op,
        link: identity,
        tag: tag,
        raw: :binary.copy(raw),
        offset: 0,
        emitted: false,
        complete: false,
        fragments: 0,
        outcome: nil,
        settled: false
      }

      link = %{link | tag_to_delivery: Map.put(link.tag_to_delivery, tag, id)}
      state = put_link!(state, link)

      state = %{
        state
        | outgoing_by_id: Map.put(state.outgoing_by_id, id, entry),
          send_queue: :queue.in(id, state.send_queue),
          outgoing_bytes: state.outgoing_bytes + byte_size(raw)
      }

      state = update_operation(state, op, &Map.put(&1, :delivery_id, id))
      {state, deadline}
    else
      _ -> {state, [{:complete, op, {:error, :send_not_admitted}}]}
    end
  end

  defp command(state, op, {:demand, identity, slots, bytes}, _ctx) do
    with {:ok, %Link{phase: :attached, role: :receiver} = link} <-
           Map.fetch(state.links_by_identity, identity),
         true <- integer_between?(slots, 1, state.options.max_incoming_deliveries),
         true <- integer_between?(bytes, 1, state.options.max_incoming_bytes),
         grant = min(slots, div(bytes, link.max_message_bytes)),
         true <- grant > 0,
         true <- state.incoming_reserved_slots + grant <= state.options.max_incoming_deliveries,
         true <-
           state.incoming_reserved_bytes + grant * link.max_message_bytes <=
             state.options.max_incoming_bytes do
      link = %{link | credit: link.credit + grant}
      state = put_link!(state, link)

      state = %{
        state
        | incoming_reserved_slots: state.incoming_reserved_slots + grant,
          incoming_reserved_bytes: state.incoming_reserved_bytes + grant * link.max_message_bytes
      }

      {queue_flow(state, identity), [{:complete, op, {:ok, grant}}]}
    else
      _ -> {state, [{:complete, op, {:error, :demand_not_admitted}}]}
    end
  end

  defp command(state, op, {:settle, token, outcome}, ctx) do
    with true <- terminal?(outcome),
         {:ok, _} <- StateCodec.encode_value(outcome, :performative_encode, :invalid_value),
         {:ok, id} <- Map.fetch(state.incoming_token_to_id, token),
         {:ok, %{phase: :delivered} = entry} <- Map.fetch(state.incoming_by_id, id),
         {:ok, link} <- Map.fetch(state.links_by_identity, entry.link),
         true <- outcome_allowed?(link, outcome) do
      if entry.peer_settled do
        {remove_incoming(state, id), [{:complete, op, {:ok, outcome}}]}
      else
        settle_incoming(state, op, id, entry, outcome, ctx)
      end
    else
      _ -> {state, [{:complete, op, {:error, :settlement_not_admitted}}]}
    end
  end

  defp command(state, op, {:accept_link, identity, token}, ctx),
    do: decide_link(state, op, identity, token, :accept, ctx)

  defp command(state, op, {:drain_exhausted, identity, token}, _ctx) do
    case Map.fetch(state.links_by_identity, identity) do
      {:ok, %Link{phase: :attached, role: :sender, drain: true, drain_token: ^token}}
      when not is_nil(token) ->
        state = drain_if_empty(state, identity, true)
        result = if state.links_by_identity[identity].credit == 0, do: :drained, else: :pending
        {state, [{:complete, op, {:ok, result}}]}

      _ ->
        {state, [{:complete, op, {:ok, :stale}}]}
    end
  end

  defp command(state, op, {:reject_link, identity, token, %ProtocolError{} = error}, ctx),
    do: decide_link(state, op, identity, token, {:reject, error}, ctx)

  defp command(state, op, {:detach, identity}, ctx),
    do: detach_link(state, op, identity, nil, ctx)

  defp command(state, op, {:detach, identity, %ProtocolError{} = error}, ctx),
    do: detach_link(state, op, identity, error, ctx)

  defp command(state, op, _command, _ctx),
    do: {state, [{:complete, op, {:error, :unsupported_session_command}}]}

  defp settle_incoming(state, op, id, entry, outcome, ctx) do
    pdu = %Disposition{
      role: Value.boolean(true),
      first: Value.uint(id),
      settled: Value.boolean(true),
      state: outcome
    }

    with {:ok, _size} <- encode_size(pdu, Map.get(ctx, :max_frame_size, 512)),
         {:ok, state, deadline} <- operation(state, op, :settle, entry.link, [], ctx),
         {:ok, state} <- control(state, pdu, {:settle, id, op, outcome}) do
      entry = %{entry | phase: :settling, settlement: {op, outcome}}
      {%{state | incoming_by_id: Map.put(state.incoming_by_id, id, entry)}, deadline}
    else
      _ -> {state, [{:complete, op, {:error, :settlement_not_admitted}}]}
    end
  end

  defp detach_link(state, op, identity, error, ctx) do
    with {:ok, %Link{phase: :attached} = link} <- Map.fetch(state.links_by_identity, identity),
         pdu = %Detach{
           handle: Value.uint(link.local_handle),
           closed: Value.boolean(true),
           error: error
         },
         {:ok, _} <- encode_size(pdu, Map.get(ctx, :max_frame_size, 512)),
         {:ok, state, deadline} <- operation(state, op, :detach, identity, [], ctx),
         link = if(is_nil(error), do: link, else: errored_link(state, link)),
         link = %{link | phase: :detach_pending, detach_operation: op},
         {:ok, state} <-
           control(
             put_link!(state, link),
             pdu,
             {:detach, identity, op}
           ) do
      {arm_timer(state, {:link, identity}), deadline}
    else
      _ -> {state, [{:complete, op, {:error, :detach_not_admitted}}]}
    end
  end

  defp decide_link(state, op, identity, token, decision, ctx) do
    with {:ok, %Link{phase: :admission_pending, admission_token: ^token} = link} <-
           Map.fetch(state.links_by_identity, identity),
         {generation, _sequence} <- token,
         true <- generation == Map.get(ctx, :generation, 0),
         %{token: ^token, at: deadline} <- Map.get(state.deadlines, {:admission, identity}) do
      if state.last_now_ms >= deadline do
        {state, effects} = reject_pending_link(state, link, admission_error(), :admission_timeout)
        {state, effects ++ [{:complete, op, {:error, :link_request_expired}}]}
      else
        decide_live_link(state, op, link, decision, ctx)
      end
    else
      _ -> {state, [{:complete, op, {:error, :stale_link_request}}]}
    end
  end

  defp decide_live_link(state, op, link, :accept, ctx) do
    identity = Link.identity(link)

    with {:ok, _} <- encode_size(local_attach(link), Map.get(ctx, :max_frame_size, 512)),
         {:ok, state, deadline} <- operation(state, op, :accept_link, identity, [], ctx),
         {:ok, state} <- control(state, local_attach(link), {:peer_attach, identity}) do
      link = %{link | phase: :attach_pending, admission_token: nil, attach_operation: op}
      {cancel_timer(put_link!(state, link), {:admission, identity}), deadline}
    else
      _ -> {state, [{:complete, op, {:error, :link_not_admitted}}]}
    end
  end

  defp decide_live_link(state, op, link, {:reject, error}, ctx) do
    detach = %Detach{
      handle: Value.uint(link.local_handle),
      closed: Value.boolean(true),
      error: error
    }

    case encode_size(detach, Map.get(ctx, :max_frame_size, 512)) do
      {:ok, _} ->
        {state, effects} = reject_pending_link(state, link, error, {:rejected, error})
        {state, effects ++ [{:complete, op, {:ok, Link.identity(link)}}]}

      _ ->
        {state, [{:complete, op, {:error, :invalid_link_error}}]}
    end
  end

  defp operation(state, op, kind, identity, options, ctx) do
    timeout = Keyword.get(options, :timeout, state.options.operation_timeout)

    cond do
      Map.has_key?(state.operations, op) ->
        {:error, :duplicate_operation}

      map_size(state.operations) >= state.options.max_pending ->
        {:error, :operation_limit}

      not integer_between?(timeout, 1, @half - 1) ->
        {:error, :invalid_timeout}

      true ->
        token = {Map.get(ctx, :generation, 0), {state.incarnation, state.next_token}}
        at = state.last_now_ms + timeout
        entry = %{kind: kind, link: identity, emitted: false, token: token, deadline: at}

        state = %{
          state
          | operations: Map.put(state.operations, op, entry),
            next_token: state.next_token + 1
        }

        {:ok, state, [{:deadline, {:operation, op}, token, at}]}
    end
  end

  defp complete(state, nil, _result), do: {state, []}

  defp complete(state, op, result) do
    if Map.has_key?(state.operations, op) do
      {%{state | operations: Map.delete(state.operations, op)},
       [{:cancel_deadline, {:operation, op}}, {:complete, op, result}]}
    else
      {state, []}
    end
  end

  defp update_operation(state, nil, _fun), do: state

  defp update_operation(state, op, fun) do
    case Map.fetch(state.operations, op) do
      {:ok, entry} -> %{state | operations: Map.put(state.operations, op, fun.(entry))}
      :error -> state
    end
  end

  defp mark_emitted(state, op), do: update_operation(state, op, &%{&1 | emitted: true})

  defp peer_attach(state, pdu, ctx) do
    with {:ok, _} <- AttachCodec.encode_value(pdu, :performative_encode, :invalid_value),
         remote = number(pdu.handle),
         true <- remote <= state.options.max_handle,
         false <- Map.has_key?(state.remote_handle_to_link, remote) do
      identity = {pdu.name, if(truth?(pdu.role), do: :sender, else: :receiver)}

      case Map.fetch(state.links_by_identity, identity) do
        {:ok, %Link{phase: :attach_sent} = link} -> attach_reply(state, link, pdu)
        {:ok, _} -> close(state, :errant_link, true)
        :error -> peer_new_link(state, identity, pdu, ctx)
      end
    else
      _ -> close(state, :handle_in_use, true)
    end
  end

  defp attach_reply(state, link, pdu) do
    identity = Link.identity(link)

    if supported_attach?(pdu, link.role) and not is_nil(pdu.source) and not is_nil(pdu.target) do
      link = %{
        link
        | remote_handle: number(pdu.handle),
          phase: :attached,
          source: negotiated_source(pdu.source, link.role),
          target: negotiated_target(pdu.target),
          peer_max_message_bytes: number(pdu.max_message_size, 0),
          delivery_count:
            if(link.role == :receiver,
              do: number(pdu.initial_delivery_count),
              else: link.delivery_count
            )
      }

      state = put_link!(state, link)
      {state, effects} = complete(state, link.attach_operation, {:ok, identity})
      {state, effects ++ [{:link_event, {:attached, identity}}]}
    else
      link = %{link | remote_handle: number(pdu.handle)}
      state = put_link!(state, link)
      {state, effects} = complete(state, link.attach_operation, {:error, :unsupported_link})
      {state, more} = refuse_link(state, link, :unsupported_link)
      {state, effects ++ more}
    end
  end

  defp peer_new_link(state, {name, role} = identity, pdu, ctx) do
    if map_size(state.links_by_identity) >= state.options.max_links do
      close(state, :resource_limit, true)
    else
      case allocate_handle(state, ctx) do
        {:ok, handle} ->
          supported? =
            supported_attach?(pdu, role) and not is_nil(pdu.source) and not is_nil(pdu.target)

          link = %Link{
            name: name,
            role: role,
            local_handle: handle,
            remote_handle: number(pdu.handle),
            phase: :attach_pending,
            peer_initiated?: true,
            source: if(supported?, do: negotiated_source(pdu.source, role), else: pdu.source),
            target: pdu.target,
            sender_settle_mode: :unsettled,
            receiver_settle_mode: :first,
            max_message_bytes: state.options.max_message_bytes,
            peer_max_message_bytes: number(pdu.max_message_size, 0),
            delivery_count: if(role == :receiver, do: number(pdu.initial_delivery_count), else: 0)
          }

          state = put_link!(state, link)

          cond do
            not supported? or
                (state.options.peer_attach_policy == :defer and
                   number(pdu.source.distribution_mode) == "copy") ->
              reject_pending_link(state, link, refusal_detach(link).error, :unsupported_link)

            occupied_link_name?(ctx, identity) ->
              reject_pending_link(
                state,
                link,
                %ProtocolError{
                  condition: Value.symbol("amqp:resource-locked"),
                  description: Value.string("link name is already active")
                },
                :link_name_in_use
              )

            state.options.peer_attach_policy == :defer ->
              token = {Map.get(ctx, :generation, 0), {state.incarnation, state.next_token}}
              link = %{link | phase: :admission_pending, admission_token: token}
              state = arm_timer(put_link!(state, link), {:admission, identity}, token)
              {state, [{:link_event, {:attach_requested, identity, token, pdu}}]}

            true ->
              case control(state, local_attach(link), {:peer_attach, identity}) do
                {:ok, state} -> {state, []}
                {:error, reason} -> close(state, reason, true)
              end
          end

        {:error, reason} ->
          close(state, reason, true)
      end
    end
  end

  defp occupied_link_name?(ctx, identity),
    do: MapSet.member?(Map.get(ctx, :occupied_link_names, MapSet.new()), identity)

  defp local_attach(link),
    do: %Attach{
      name: link.name,
      handle: Value.uint(link.local_handle),
      role: Value.boolean(link.role == :receiver),
      snd_settle_mode: Value.ubyte(0),
      rcv_settle_mode: Value.ubyte(0),
      source: negotiated_source(link.source, link.role),
      target: negotiated_target(link.target),
      initial_delivery_count: if(link.role == :sender, do: Value.uint(link.delivery_count)),
      max_message_size: Value.ulong(link.max_message_bytes)
    }

  defp supported_attach?(pdu, local_role) do
    snd = number(pdu.snd_settle_mode, 2)
    rcv = number(pdu.rcv_settle_mode, 0)
    # The receiver's sender-mode field is a preference. Our sender always chooses unsettled.
    snd_ok = if local_role == :sender, do: snd in [0, 2], else: snd == 0

    snd_ok and rcv == 0 and is_nil(pdu.unsettled) and not truth?(pdu.incomplete_unsettled) and
      supported_source?(pdu.source) and supported_target?(pdu.target) and
      no_symbols?(pdu.desired_capabilities)
  end

  defp supported_source?(nil), do: true

  defp supported_source?(%Source{} = source),
    do:
      volatile?(source) and is_nil(source.filter) and is_nil(source.default_outcome) and
        (is_nil(source.distribution_mode) or number(source.distribution_mode) in ["move", "copy"]) and
        literal_address?(source.address) and supported_outcomes?(source.outcomes)

  defp supported_source?(_), do: false
  defp supported_target?(nil), do: true

  defp supported_target?(%Target{} = target),
    do: volatile?(target) and literal_address?(target.address)

  defp supported_target?(_), do: false

  defp volatile?(terminus),
    do:
      number(terminus.durable, 0) == 0 and not truth?(terminus.dynamic) and
        is_nil(terminus.dynamic_node_properties) and number(terminus.timeout, 0) == 0 and
        (is_nil(terminus.expiry_policy) or number(terminus.expiry_policy) == "session-end")

  defp negotiated_source(nil, _role), do: nil

  defp negotiated_source(%Source{} = source, role),
    do: %{
      source
      | capabilities: nil,
        expiry_policy: Value.symbol("session-end"),
        outcomes: Value.array(:symbol, chosen_outcomes(source.outcomes, role))
    }

  defp negotiated_target(nil), do: nil

  defp negotiated_target(%Target{} = target),
    do: %{target | capabilities: nil, expiry_policy: Value.symbol("session-end")}

  defp supported_outcomes?(nil), do: true
  defp supported_outcomes?(%Value{type: :symbol, value: outcome}), do: outcome in @outcomes

  defp supported_outcomes?(%Value{
         type: :array,
         value: %Value.Array{element_type: :symbol, values: values}
       }),
       do: Enum.all?(values, &supported_outcomes?/1)

  defp supported_outcomes?(_), do: false

  # The sender owns the actual Source. An absent receiver preference lets our
  # sender select its supported set; an absent sender declaration guarantees
  # only Accepted. Null and empty multiple fields have identical meaning.
  defp chosen_outcomes(nil, :sender), do: Enum.map(@outcomes, &Value.symbol/1)

  defp chosen_outcomes(%Value{type: :array, value: %Value.Array{values: []}}, :sender),
    do: Enum.map(@outcomes, &Value.symbol/1)

  defp chosen_outcomes(outcomes, _role), do: outcome_symbols(outcomes)

  defp outcome_symbols(nil), do: [Value.symbol("amqp:accepted:list")]
  defp outcome_symbols(%Value{type: :symbol} = outcome), do: [outcome]

  defp outcome_symbols(%Value{type: :array, value: %Value.Array{values: []}}),
    do: [Value.symbol("amqp:accepted:list")]

  defp outcome_symbols(%Value{type: :array, value: %Value.Array{values: values}}), do: values

  defp outcome_allowed?(%Link{source: %Source{outcomes: outcomes}}, outcome),
    do: Value.symbol(outcome_descriptor(outcome)) in outcome_symbols(outcomes)

  defp outcome_descriptor(%Outcome.Accepted{}), do: "amqp:accepted:list"
  defp outcome_descriptor(%Outcome.Rejected{}), do: "amqp:rejected:list"
  defp outcome_descriptor(%Outcome.Released{}), do: "amqp:released:list"
  defp outcome_descriptor(%Outcome.Modified{}), do: "amqp:modified:list"

  defp literal_address?(nil), do: true

  defp literal_address?(%Value{type: :string, value: value}),
    do: is_binary(value) and String.valid?(value)

  defp literal_address?(_), do: false
  defp no_symbols?(nil), do: true
  defp no_symbols?(%Value{type: :array, value: %Value.Array{values: []}}), do: true
  defp no_symbols?(_), do: false

  defp admission_error do
    %ProtocolError{
      condition: Value.symbol("amqp:resource-limit-exceeded"),
      description: Value.string("link admission timed out")
    }
  end

  defp reject_pending_link(state, link, error, reason) do
    identity = Link.identity(link)
    state = put_link!(state, errored_link(state, link))
    {state, effects} = fail_link_operations(state, identity, reason, nil)
    state = cancel_timer(release_link_payloads(state, identity), {:admission, identity})

    link = %{
      Map.fetch!(state.links_by_identity, identity)
      | phase: :refusing,
        admission_token: nil
    }

    state = put_link!(state, link)
    attach = local_attach(%{link | source: nil, target: nil})

    detach = %Detach{
      handle: Value.uint(link.local_handle),
      closed: Value.boolean(true),
      error: error
    }

    with {:ok, state} <- control(state, attach, {:refusal_attach, identity}),
         {:ok, state} <- control(state, detach, {:refusal_detach, identity}) do
      {arm_timer(state, {:link, identity}),
       effects ++ [{:link_event, {:refused, identity, reason}}]}
    else
      {:error, failure} -> append_result(close(state, failure, true), effects)
    end
  end

  defp refuse_link(state, link, reason) do
    identity = Link.identity(link)
    state = put_link!(state, errored_link(state, link))
    {state, effects} = fail_link_operations(state, identity, reason, nil)
    state = release_link_payloads(state, identity)
    link = Map.fetch!(state.links_by_identity, identity)

    case control(state, refusal_detach(link, reason), {:refusal_detach, identity}) do
      {:ok, state} ->
        state = arm_timer(put_link!(state, %{link | phase: :refusing}), {:link, identity})
        {state, effects ++ [{:link_event, {:refused, identity, reason}}]}

      {:error, reason} ->
        append_result(close(state, reason, true), effects)
    end
  end

  defp refusal_detach(link, reason \\ :unsupported_link) do
    condition =
      case reason do
        :message_size_exceeded -> "amqp:link:message-size-exceeded"
        :partial_timeout -> "amqp:link:detach-forced"
        :transfer_limit -> "amqp:resource-limit-exceeded"
        :annotation_limit -> "amqp:resource-limit-exceeded"
        :malformed_annotations -> "amqp:decode-error"
        _ -> "amqp:not-implemented"
      end

    %Detach{
      handle: Value.uint(link.local_handle),
      closed: Value.boolean(true),
      error: %ProtocolError{
        condition: Value.symbol(condition),
        description: Value.string("unsupported or exceeded bounded endpoint profile")
      }
    }
  end

  defp peer_flow(state, pdu, _ctx) do
    with true <-
           (is_nil(pdu.next_incoming_id) or tagged_uint?(pdu.next_incoming_id)) and
             tagged_uint?(pdu.next_outgoing_id) and tagged_uint?(pdu.incoming_window) and
             tagged_uint?(pdu.outgoing_window),
         ack = number(pdu.next_incoming_id, state.options.initial_outgoing_id),
         true <- Serial.compare(state.remote_next_incoming_id, ack) in [:eq, :lt],
         true <- Serial.compare(ack, state.next_outgoing_id) in [:eq, :lt] do
      state = %{
        state
        | remote_next_incoming_id: ack,
          next_incoming_id: number(pdu.next_outgoing_id),
          remote_incoming_window: number(pdu.incoming_window),
          remote_outgoing_window: number(pdu.outgoing_window)
      }

      if is_nil(pdu.handle) do
        if Enum.any?(
             [pdu.delivery_count, pdu.link_credit, pdu.available, pdu.properties],
             &(not is_nil(&1))
           ) or truth?(pdu.drain) do
          close(state, :invalid_flow, true)
        else
          {if(truth?(pdu.echo), do: queue_flow(state, :session), else: state), []}
        end
      else
        case SessionState.fetch_incoming_link(state, number(pdu.handle)) do
          {:ok, %Link{phase: :attached} = link} ->
            link_flow(state, link, pdu)

          {:ok, %Link{peer_initiated?: true, phase: phase} = link}
          when phase in [:admission_pending, :attach_pending] ->
            {state, effects} = link_flow(state, link, pdu)

            state =
              update_link(
                state,
                Link.identity(link),
                &%{&1 | pending_echo?: &1.pending_echo? or truth?(pdu.echo)}
              )

            {state, effects}

          _ ->
            close(state, :unattached_handle, true)
        end
      end
    else
      _ -> close(state, :invalid_flow, true)
    end
  end

  defp link_flow(state, %Link{role: :sender} = link, pdu) do
    if (is_nil(pdu.delivery_count) or tagged_uint?(pdu.delivery_count)) and
         tagged_uint?(pdu.link_credit) do
      count = number(pdu.delivery_count, link.peer_delivery_count || 0)
      consumed = Serial.distance(count, link.delivery_count)

      if consumed < @half do
        token = if truth?(pdu.drain), do: {state.incarnation, state.next_token}

        link = %{
          link
          | credit: max(0, number(pdu.link_credit) - consumed),
            peer_delivery_count: count,
            drain: truth?(pdu.drain),
            drain_token: token
        }

        state = put_link!(%{state | next_token: state.next_token + 1}, link)
        state = if truth?(pdu.echo), do: queue_flow(state, Link.identity(link)), else: state
        {drain_if_empty(state, Link.identity(link)), []}
      else
        close(state, :invalid_flow, true)
      end
    else
      close(state, :invalid_flow, true)
    end
  end

  defp link_flow(state, %Link{role: :receiver} = link, pdu) do
    if tagged_uint?(pdu.delivery_count) do
      skipped = Serial.distance(link.delivery_count, number(pdu.delivery_count))

      cond do
        skipped == 0 ->
          {if(truth?(pdu.echo), do: queue_flow(state, Link.identity(link)), else: state), []}

        truth?(pdu.drain) and skipped <= link.credit ->
          state = release_reservation(state, skipped, skipped * link.max_message_bytes)

          link = %{
            link
            | delivery_count: number(pdu.delivery_count),
              credit: link.credit - skipped
          }

          {if(truth?(pdu.echo),
             do: queue_flow(put_link!(state, link), Link.identity(link)),
             else: put_link!(state, link)
           ), []}

        true ->
          close(state, :invalid_flow, true)
      end
    else
      close(state, :invalid_flow, true)
    end
  end

  defp drain_if_empty(state, identity, confirmed? \\ false) do
    link = Map.fetch!(state.links_by_identity, identity)

    pending =
      Enum.any?(state.outgoing_by_id, fn {_, entry} ->
        entry.link == identity and not entry.complete
      end)

    # A broker's external Queue can have available messages even when this
    # finite send ledger is empty. Only its owner can confirm exhaustion.
    if link.phase == :attached and link.role == :sender and link.drain and not pending and
         (state.options.peer_attach_policy == :auto or confirmed?) do
      state =
        put_link!(state, %{
          link
          | delivery_count: Serial.add(link.delivery_count, link.credit),
            credit: 0,
            drain_token: nil
        })

      queue_flow(state, identity)
    else
      state
    end
  end

  defp peer_disposition(state, pdu) do
    first = number(pdu.first)
    last = number(pdu.last, first)

    cond do
      not uint?(first) or not uint?(last) ->
        close(state, :invalid_disposition, true)

      Serial.span(first, last) == {:error, :ambiguous_range} ->
        close(state, :ambiguous_disposition_range, true)

      not is_nil(pdu.state) and
          not (terminal?(pdu.state) or is_struct(pdu.state, DeliveryState.Received)) ->
        close(state, :invalid_disposition, true)

      truth?(pdu.role) ->
        outgoing_disposition(state, first, last, pdu)

      true ->
        incoming_disposition(state, first, last, pdu)
    end
  end

  defp outgoing_disposition(state, first, last, pdu) do
    Enum.reduce_while(Map.to_list(state.outgoing_by_id), {state, []}, fn {id, _},
                                                                         {state, effects} ->
      if uint?(id) and Serial.member?(id, first, last) do
        entry = Map.fetch!(state.outgoing_by_id, id)

        cond do
          not entry.emitted ->
            {:halt, append_result(close(state, :invalid_disposition, true), effects)}

          terminal?(pdu.state) and
              not outcome_allowed?(Map.fetch!(state.links_by_identity, entry.link), pdu.state) ->
            {:halt, append_result(close(state, :invalid_disposition, true), effects)}

          terminal?(entry.outcome) and not is_nil(pdu.state) and pdu.state != entry.outcome ->
            {:halt, append_result(close(state, :invalid_disposition, true), effects)}

          true ->
            outcome = if is_nil(pdu.state), do: entry.outcome, else: pdu.state
            entry = %{entry | outcome: outcome, settled: entry.settled or truth?(pdu.settled)}
            state = %{state | outgoing_by_id: Map.put(state.outgoing_by_id, id, entry)}

            cond do
              entry.settled and entry.complete ->
                {state, more} = complete_outgoing(state, id, entry)
                {:cont, {state, effects ++ more}}

              true ->
                {:cont,
                 {state,
                  effects ++ [{:link_event, {:delivery_state, entry.link, entry.op, outcome}}]}}
            end
        end
      else
        {:cont, {state, effects}}
      end
    end)
  end

  defp incoming_disposition(state, first, last, pdu) do
    Enum.reduce_while(Map.to_list(state.incoming_by_id), {state, []}, fn {id, entry},
                                                                         {state, effects} ->
      if Serial.member?(id, first, last) do
        if terminal?(entry.peer_state) and not is_nil(pdu.state) and
             pdu.state != entry.peer_state do
          {:halt, append_result(close(state, :invalid_disposition, true), effects)}
        else
          # Peer settlement only ends the peer's tracking. The host still owns
          # the received delivery until it supplies its own settlement outcome.
          entry = %{
            entry
            | peer_settled: entry.peer_settled or truth?(pdu.settled),
              peer_state: if(is_nil(pdu.state), do: entry.peer_state, else: pdu.state)
          }

          state = %{state | incoming_by_id: Map.put(state.incoming_by_id, id, entry)}

          case {entry.peer_settled, entry.settlement} do
            {true, {op, outcome}} ->
              state = cancel_settlement(state, id, op)
              {state, more} = complete(remove_incoming(state, id), op, {:ok, outcome})
              {:cont, {state, effects ++ more}}

            _ ->
              {:cont, {state, effects}}
          end
        end
      else
        {:cont, {state, effects}}
      end
    end)
  end

  defp cancel_settlement(state, id, op) do
    state = cancel_controls(state, op)

    writes =
      Map.new(state.writes, fn
        {receipt, %{action: {:settle, ^id, ^op, _}} = write} ->
          {receipt, %{write | action: :none}}

        write ->
          write
      end)

    %{state | writes: writes}
  end

  defp incoming_transfer(state, pdu, payload, ctx) do
    cond do
      state.incoming_window <= 0 or state.remote_outgoing_window <= 0 ->
        close(state, :window_violation, true)

      true ->
        case SessionState.fetch_incoming_link(state, number(pdu.handle)) do
          {:ok, %Link{role: :receiver, phase: :attached} = link} ->
            state = %{
              state
              | incoming_window: state.incoming_window - 1,
                remote_outgoing_window: state.remote_outgoing_window - 1,
                next_incoming_id: Serial.add(state.next_incoming_id, 1)
            }

            {state, effects} = receive_fragment(state, link, pdu, payload, ctx)
            {if(state.phase == :closed, do: state, else: queue_flow(state, :session)), effects}

          _ ->
            close(state, :unattached_handle, true)
        end
    end
  end

  defp receive_fragment(state, %Link{partial: nil} = link, pdu, payload, ctx) do
    id = number(pdu.delivery_id)

    cond do
      link.credit <= 0 ->
        close(state, :transfer_without_credit, true)

      not uint?(id) or not valid_tag?(pdu.delivery_tag) or not tagged_uint?(pdu.message_format) ->
        close(state, :invalid_transfer, true)

      not is_nil(state.last_remote_delivery_id) and
          id != Serial.add(state.last_remote_delivery_id, 1) ->
        close(state, :invalid_delivery_sequence, true)

      Map.has_key?(state.incoming_by_id, id) or
          Map.has_key?(link.tag_to_delivery, pdu.delivery_tag) ->
        close(state, :duplicate_delivery_id, true)

      unsupported_transfer?(pdu) and not truth?(pdu.aborted) ->
        refuse_link(state, link, :unsupported_transfer)

      true ->
        identity = Link.identity(link)
        token = {Map.get(ctx, :generation, 0), state.id, state.incarnation, state.next_token, id}

        entry = %{
          link: identity,
          token: token,
          tag: pdu.delivery_tag,
          size: 0,
          budget: link.max_message_bytes,
          phase: :assembling,
          peer_settled: false,
          peer_state: nil,
          settlement: nil
        }

        partial = %{id: id, tag: pdu.delivery_tag, parts: [], bytes: 0, fragments: 0}

        link = %{
          link
          | credit: link.credit - 1,
            delivery_count: Serial.add(link.delivery_count, 1),
            tag_to_delivery: Map.put(link.tag_to_delivery, pdu.delivery_tag, id),
            partial: partial
        }

        state = put_link!(state, link)

        state = %{
          state
          | incoming_by_id: Map.put(state.incoming_by_id, id, entry),
            incoming_token_to_id: Map.put(state.incoming_token_to_id, token, id),
            next_token: state.next_token + 1,
            last_remote_delivery_id: id
        }

        receive_fragment(arm_timer(state, {:partial, id}), link, pdu, payload, ctx)
    end
  end

  defp receive_fragment(state, link, pdu, payload, _ctx) do
    partial = link.partial

    cond do
      not compatible_continuation?(pdu, partial) ->
        close(state, :invalid_transfer, true)

      truth?(pdu.aborted) ->
        state = remove_incoming(state, partial.id)
        {state, [{:link_event, {:aborted, Link.identity(link), partial.id}}]}

      unsupported_transfer?(pdu) ->
        refuse_link(state, link, :unsupported_transfer)

      partial.fragments >= state.options.max_fragments ->
        refuse_link(state, link, :transfer_limit)

      partial.bytes + byte_size(payload) > link.max_message_bytes or
          state.incoming_bytes + byte_size(payload) > state.options.max_incoming_bytes ->
        refuse_link(state, link, :message_size_exceeded)

      true ->
        partial = %{
          partial
          | parts: [:binary.copy(payload) | partial.parts],
            bytes: partial.bytes + byte_size(payload),
            fragments: partial.fragments + 1
        }

        entry = Map.fetch!(state.incoming_by_id, partial.id)
        entry = %{entry | size: partial.bytes}

        state = %{
          state
          | incoming_by_id: Map.put(state.incoming_by_id, partial.id, entry),
            incoming_bytes: state.incoming_bytes + byte_size(payload)
        }

        if truth?(pdu.more) do
          {put_link!(state, %{link | partial: partial}), []}
        else
          # All fragments were admitted individually before this final bounded allocation.
          raw = partial.parts |> Enum.reverse() |> IO.iodata_to_binary()
          entry = %{entry | phase: :delivered}
          state = %{state | incoming_by_id: Map.put(state.incoming_by_id, partial.id, entry)}
          state = cancel_timer(put_link!(state, %{link | partial: nil}), {:partial, partial.id})

          case GravitonMQ.AMQP10.Message.check_annotations(
                 raw,
                 %Limits{max_value_bytes: link.max_message_bytes}
               ) do
            {:error, reason} ->
              refuse_link(state, link, reason)

            :ok ->
              {state,
               [
                 {:delivery, Link.identity(link), entry.token, raw,
                  %{
                    delivery_id: partial.id,
                    delivery_tag: partial.tag,
                    message_format: 0,
                    settled: false
                  }}
               ]}
          end
        end
    end
  end

  defp compatible_continuation?(pdu, partial),
    do:
      (is_nil(pdu.delivery_id) or number(pdu.delivery_id) == partial.id) and
        (is_nil(pdu.delivery_tag) or pdu.delivery_tag == partial.tag) and
        (is_nil(pdu.message_format) or number(pdu.message_format) == 0)

  defp unsupported_transfer?(pdu),
    do:
      truth?(pdu.resume) or truth?(pdu.settled) or
        (not is_nil(pdu.message_format) and number(pdu.message_format) != 0) or
        (not is_nil(pdu.rcv_settle_mode) and number(pdu.rcv_settle_mode) != 0) or
        not is_nil(pdu.state)

  # Error markers remain Session-owned Link data until peer Detach (or the
  # bounded Link timeout/Session/Connection teardown). They retain only uint
  # delivery IDs, never message bytes. Per Link each set is bounded by the
  # corresponding configured Session delivery limit, and max_links bounds the
  # aggregate even after released payload slots are reused by other Links.
  defp errored_link(state, link) do
    identity = Link.identity(link)
    incoming = for {id, %{link: ^identity}} <- state.incoming_by_id, do: id

    outgoing =
      for {id, %{link: ^identity, emitted: true}} <- state.outgoing_by_id, uint?(id), do: id

    %{
      link
      | error_detach?: true,
        error_incoming_ids: MapSet.union(link.error_incoming_ids, MapSet.new(incoming)),
        error_outgoing_ids: MapSet.union(link.error_outgoing_ids, MapSet.new(outgoing))
    }
  end

  defp handle_limit_violation?(%{phase: :active} = state, {:peer, %module{handle: handle}, _})
       when module in [Attach, Flow, Transfer, Detach],
       do: tagged_uint?(handle) and number(handle) > state.options.max_handle

  defp handle_limit_violation?(_state, _event), do: false

  defp errant_input?(state, {:peer, %Disposition{} = pdu, _payload}) do
    first = number(pdu.first)
    last = number(pdu.last, first)
    outgoing? = truth?(pdu.role)
    ledger = if outgoing?, do: state.outgoing_by_id, else: state.incoming_by_id

    uint?(first) and uint?(last) and Serial.span(first, last) != {:error, :ambiguous_range} and
      Enum.any?(state.links_by_identity, fn {identity, link} ->
        ids = if outgoing?, do: link.error_outgoing_ids, else: link.error_incoming_ids

        link.error_detach? and
          Enum.any?(ids, fn id ->
            # If a full uint wrap reused an ID on another Link, the current live
            # delivery owns that reference rather than the retired error marker.
            current = Map.get(ledger, id)

            Serial.member?(id, first, last) and
              (is_nil(current) or current.link == identity)
          end)
      end)
  end

  defp errant_input?(state, {:peer, %module{} = pdu, _payload})
       when module in [Attach, Flow, Transfer] do
    handle = number(pdu.handle)

    by_handle =
      case SessionState.fetch_incoming_link(state, handle) do
        {:ok, %Link{error_detach?: true}} -> true
        _ -> false
      end

    by_name =
      case pdu do
        %Attach{name: name, role: role} ->
          match?(
            %Link{error_detach?: true},
            Map.get(
              state.links_by_identity,
              {name, if(truth?(role), do: :sender, else: :receiver)}
            )
          )

        _ ->
          false
      end

    by_handle or by_name
  end

  defp errant_input?(_state, _event), do: false

  defp peer_detach(state, pdu) do
    case SessionState.fetch_incoming_link(state, number(pdu.handle)) do
      {:ok, link} ->
        identity = Link.identity(link)

        attach_reply =
          if link.peer_initiated? and not link.peer_attach_replied?,
            do: local_attach(%{link | source: nil, target: nil})

        reason = if is_nil(pdu.error), do: :link_detached, else: {:peer_detach, pdu.error}
        {state, effects} = fail_link_operations(state, identity, reason, link.detach_operation)

        {state, more} =
          complete(
            state,
            link.detach_operation,
            if(is_nil(pdu.error), do: {:ok, identity}, else: {:error, reason})
          )

        reply =
          if link.phase in [:detach_sent, :refused],
            do: nil,
            else: %Detach{handle: Value.uint(link.local_handle), closed: Value.boolean(true)}

        state = remove_link_resources(state, identity)

        if is_nil(reply) do
          {state, effects ++ more ++ [{:link_event, {:detached, identity, reason}}]}
        else
          queued = if attach_reply, do: control(state, attach_reply, :none), else: {:ok, state}

          with {:ok, state} <- queued,
               {:ok, state} <- control(state, reply, :none) do
            {state, effects ++ more ++ [{:link_event, {:detached, identity, reason}}]}
          else
            {:error, failure} -> append_result(close(state, failure, true), effects ++ more)
          end
        end

      _ ->
        close(state, :unattached_handle, true)
    end
  end

  defp operation_timeout(state, op, token, _ctx) do
    case Map.fetch(state.operations, op) do
      {:ok, entry} ->
        if (token == :endpoint or token == entry.token) and state.last_now_ms >= entry.deadline do
          reason = failure_result(state, entry, :timeout)
          {state, effects} = complete(state, op, reason)
          state = cancel_controls(state, op)

          state =
            case Map.get(entry, :delivery_id) do
              nil -> state
              id -> remove_outgoing(state, id)
            end

          if entry.link != nil do
            case Map.fetch(state.links_by_identity, entry.link) do
              {:ok, _link} when entry.emitted ->
                {state, more} = fail_link_operations(state, entry.link, :link_timeout, nil)
                state = release_link_payloads(state, entry.link)
                link = Map.fetch!(state.links_by_identity, entry.link)

                state =
                  arm_timer(put_link!(state, %{link | phase: :refusing}), {:link, entry.link})

                case control(
                       state,
                       %Detach{
                         handle: Value.uint(link.local_handle),
                         closed: Value.boolean(true)
                       },
                       {:refusal_detach, entry.link}
                     ) do
                  {:ok, state} -> {state, effects ++ more}
                  {:error, reason} -> append_result(close(state, reason, true), effects ++ more)
                end

              {:ok, link} when entry.kind == :settle ->
                {state, more} = refuse_link(state, link, :settlement_timeout)
                {state, effects ++ more}

              {:ok, link} when entry.kind == :accept_link ->
                {state, more} =
                  reject_pending_link(state, link, admission_error(), :admission_timeout)

                {state, effects ++ more}

              {:ok, _} when entry.kind == :attach ->
                {remove_link_resources(state, entry.link), effects}

              _ ->
                {state, effects}
            end
          else
            if entry.kind == :begin,
              do: append_result(close(state, :begin_timeout, true), effects),
              else: {state, effects}
          end
        else
          {state, []}
        end

      :error ->
        {state, []}
    end
  end

  defp write_result(state, receipt, :ok, _ctx) do
    case Map.pop(state.writes, receipt) do
      {nil, _} ->
        {state, []}

      {%{action: action}, writes} ->
        state = %{state | writes: writes}

        case action do
          {:transfer, _id} ->
            state = %{
              state
              | outgoing_window: min(state.options.outgoing_window, state.outgoing_window + 1)
            }

            {queue_flow(state, :session), []}

          {:settle, id, op, outcome} ->
            complete(remove_incoming(state, id), op, {:ok, outcome})

          _ ->
            {state, []}
        end
    end
  end

  defp write_result(state, receipt, {:error, reason}, _ctx) do
    if Map.has_key?(state.writes, receipt),
      do: close(state, {:write_failed, reason}, true),
      else: {state, []}
  end

  defp write_result(state, receipt, _result, ctx),
    do: write_result(state, receipt, {:error, :invalid_write_result}, ctx)

  defp flush(state, ctx) do
    budget = %{
      frames: max(0, Map.get(ctx, :write_frames, 0)),
      bytes: max(0, Map.get(ctx, :write_bytes, 0)),
      control_frames: max(0, Map.get(ctx, :control_frames, Map.get(ctx, :write_frames, 0))),
      control_bytes: max(0, Map.get(ctx, :control_bytes, Map.get(ctx, :write_bytes, 0))),
      max_frame_size: Map.get(ctx, :max_frame_size, 512)
    }

    flush_loop(state, ctx, budget, [])
  end

  defp flush_loop(state, ctx, budget, effects) do
    if state.phase == :closed or map_size(state.writes) >= state.options.max_writes do
      {state, effects}
    else
      case next_control(state) do
        {:ok, pdu, action, next} ->
          case encode_size(pdu, budget.max_frame_size) do
            {:ok, size} when size > budget.max_frame_size ->
              append_result(close(state, :frame_too_small, true), effects)

            {:ok, size} when budget.control_frames > 0 and size <= budget.control_bytes ->
              {next, receipt} = reserve_write(next, action, size, ctx)
              next = emitted_control(next, action)
              budget = spend(budget, size, :control)

              {next, events} =
                case action do
                  {:peer_attach, identity} ->
                    link = Map.fetch!(next.links_by_identity, identity)
                    {next, completed} = complete(next, link.attach_operation, {:ok, identity})
                    {next, [{:link_event, {:attached, identity}}] ++ completed}

                  _ ->
                    {next, []}
                end

              flush_loop(next, ctx, budget, effects ++ [{:emit, receipt, pdu, <<>>}] ++ events)

            {:ok, _} ->
              {state, effects}

            {:error, _} ->
              append_result(close(state, :invalid_local_performative, true), effects)
          end

        :none ->
          case next_data(state, budget) do
            {:ok, next, id, pdu, payload, size} ->
              {next, receipt} = reserve_write(next, {:transfer, id}, size, ctx)
              {next, completed} = finalize_outgoing(next, id)

              flush_loop(
                next,
                ctx,
                spend(budget, size, :data),
                effects ++ [{:emit, receipt, pdu, payload}] ++ completed
              )

            {:error, reason} ->
              append_result(close(state, reason, true), effects)

            :none ->
              {state, effects}
          end
      end
    end
  end

  defp spend(budget, size, :control),
    do: %{
      budget
      | frames: max(0, budget.frames - 1),
        bytes: max(0, budget.bytes - size),
        control_frames: budget.control_frames - 1,
        control_bytes: budget.control_bytes - size
    }

  defp spend(budget, size, :data),
    do: %{
      budget
      | frames: budget.frames - 1,
        bytes: budget.bytes - size,
        control_frames: max(0, budget.control_frames - 1),
        control_bytes: max(0, budget.control_bytes - size)
    }

  defp reserve_write(state, action, size, ctx) do
    receipt = {Map.get(ctx, :generation, 0), {state.incarnation, state.next_receipt}}

    {%{
       state
       | writes: Map.put(state.writes, receipt, %{action: action, bytes: size}),
         next_receipt: state.next_receipt + 1
     }, receipt}
  end

  defp encode_size(pdu, frame_limit) do
    # Bound the value encoder before it allocates any wire children. Current
    # free writer capacity may be zero; the negotiated limit remains absolute.
    limits = %Limits{
      max_frame_size: frame_limit,
      max_value_bytes: max(1, frame_limit - 8),
      max_compound_items: min(65_536, max(1, frame_limit - 8))
    }

    case PDUCodec.encode(pdu, limits) do
      {:ok, encoded} -> {:ok, 8 + byte_size(encoded)}
      {:error, _} = error -> error
    end
  end

  defp control(state, pdu, action) do
    if :queue.len(state.control_queue) < state.options.max_control_queue,
      do: {:ok, %{state | control_queue: :queue.in({pdu, action}, state.control_queue)}},
      else: {:error, :control_limit}
  end

  defp queue_flow(state, identity),
    do: %{state | pending_flows: Map.put(state.pending_flows, identity, true)}

  defp next_control(state) do
    case :queue.out(state.control_queue) do
      {{:value, {pdu, action}}, queue} ->
        {:ok, pdu, action, %{state | control_queue: queue}}

      {:empty, _} ->
        case Enum.sort(Map.keys(state.pending_flows)) do
          [] ->
            :none

          [identity | _] ->
            next = %{state | pending_flows: Map.delete(state.pending_flows, identity)}

            case flow_for(state, identity) do
              nil -> next_control(next)
              flow -> {:ok, flow, {:flow, identity}, next}
            end
        end
    end
  end

  defp flow_for(state, :session), do: session_flow(state)

  defp flow_for(state, identity) do
    case Map.fetch(state.links_by_identity, identity) do
      {:ok, %Link{phase: :attached} = link} ->
        %{
          session_flow(state)
          | handle: Value.uint(link.local_handle),
            delivery_count: Value.uint(link.delivery_count),
            link_credit: Value.uint(link.credit),
            drain: Value.boolean(link.drain),
            echo: Value.boolean(false)
        }

      _ ->
        nil
    end
  end

  defp session_flow(state),
    do: %Flow{
      next_incoming_id: Value.uint(state.next_incoming_id),
      incoming_window: Value.uint(state.options.incoming_window),
      next_outgoing_id: Value.uint(state.next_outgoing_id),
      outgoing_window: Value.uint(state.outgoing_window),
      drain: Value.boolean(false),
      echo: Value.boolean(false)
    }

  defp emitted_control(state, {:begin, op, phase}), do: mark_emitted(%{state | phase: phase}, op)

  defp emitted_control(state, {:attach, identity, op}),
    do: mark_emitted(update_link(state, identity, &%{&1 | phase: :attach_sent}), op)

  defp emitted_control(state, {:peer_attach, identity}) do
    link = Map.fetch!(state.links_by_identity, identity)

    state =
      put_link!(state, %{
        link
        | phase: :attached,
          peer_attach_replied?: true,
          pending_echo?: false
      })

    state = if link.pending_echo?, do: queue_flow(state, identity), else: state
    drain_if_empty(state, identity)
  end

  defp emitted_control(state, {:refusal_attach, identity}),
    do: update_link(state, identity, &%{&1 | peer_attach_replied?: true})

  defp emitted_control(state, {:detach, identity, op}),
    do: mark_emitted(update_link(state, identity, &%{&1 | phase: :detach_sent}), op)

  defp emitted_control(state, {:refusal_detach, identity}),
    do: update_link(state, identity, &%{&1 | phase: :refused})

  defp emitted_control(state, {:settle, _id, op, _outcome}), do: mark_emitted(state, op)

  defp emitted_control(state, {:flow, _}),
    do: %{state | incoming_window: state.options.incoming_window}

  defp emitted_control(state, _), do: state

  defp next_data(%{phase: phase}, _) when phase != :active, do: :none

  defp next_data(state, budget) do
    available =
      state.remote_incoming_window -
        Serial.distance(state.remote_next_incoming_id, state.next_outgoing_id)

    if budget.frames <= 0 or budget.bytes <= 8 or state.outgoing_window <= 0 or available <= 0 do
      :none
    else
      candidate =
        state.send_queue
        |> :queue.to_list()
        |> Enum.find(fn id ->
          case Map.fetch(state.outgoing_by_id, id) do
            {:ok, entry} ->
              link = Map.fetch!(state.links_by_identity, entry.link)

              not entry.complete and link.phase == :attached and
                (entry.emitted or link.credit > 0)

            :error ->
              false
          end
        end)

      if is_nil(candidate), do: :none, else: transfer_fragment(state, candidate, budget)
    end
  end

  defp transfer_fragment(state, id, budget) do
    entry = Map.fetch!(state.outgoing_by_id, id)
    link = Map.fetch!(state.links_by_identity, entry.link)
    wire_id = if entry.emitted, do: id, else: state.next_delivery_id
    abort = entry.emitted and (entry.settled or terminal?(entry.outcome))

    pdu = %Transfer{
      handle: Value.uint(link.local_handle),
      delivery_id: if(not entry.emitted, do: Value.uint(wire_id)),
      delivery_tag: if(not entry.emitted, do: entry.tag),
      message_format: if(not entry.emitted, do: Value.uint(0)),
      settled: if(not entry.emitted, do: Value.boolean(false)),
      more: Value.boolean(not abort),
      aborted: if(abort, do: Value.boolean(true))
    }

    with {:ok, overhead} <- encode_size(pdu, budget.max_frame_size) do
      capacity = min(budget.max_frame_size, budget.bytes) - overhead
      remaining = if abort, do: 0, else: byte_size(entry.raw) - entry.offset

      cond do
        not entry.emitted and Map.has_key?(state.outgoing_by_id, wire_id) ->
          :none

        capacity < 0 or (capacity == 0 and remaining > 0) ->
          :none

        entry.fragments >= state.options.max_fragments and not abort ->
          {:error, :transfer_limit}

        true ->
          count = min(capacity, remaining)
          final = count == remaining
          pdu = %{pdu | more: Value.boolean(not final)}
          {:ok, overhead} = encode_size(pdu, budget.max_frame_size)
          chunk = binary_part(entry.raw, entry.offset, count)
          first = not entry.emitted

          entry = %{
            entry
            | offset: entry.offset + count,
              fragments: entry.fragments + 1,
              emitted: true,
              complete: final
          }

          link =
            if first,
              do: %{
                link
                | credit: link.credit - 1,
                  delivery_count: Serial.add(link.delivery_count, 1)
              },
              else: link

          link =
            if first,
              do: %{link | tag_to_delivery: Map.put(link.tag_to_delivery, entry.tag, wire_id)},
              else: link

          state = put_link!(state, link)

          queue =
            state.send_queue
            |> :queue.to_list()
            |> Enum.map(fn queued -> if queued == id, do: wire_id, else: queued end)
            |> :queue.from_list()

          state = %{
            state
            | outgoing_by_id: state.outgoing_by_id |> Map.delete(id) |> Map.put(wire_id, entry),
              send_queue: queue,
              next_delivery_id:
                if(first, do: Serial.add(wire_id, 1), else: state.next_delivery_id),
              next_outgoing_id: Serial.add(state.next_outgoing_id, 1),
              outgoing_window: state.outgoing_window - 1
          }

          state = mark_emitted(state, entry.op)
          state = update_operation(state, entry.op, &Map.put(&1, :delivery_id, wire_id))

          state =
            if final,
              do: %{state | send_queue: filter_queue(state.send_queue, &(&1 != wire_id))},
              else: state

          state = if final, do: drain_if_empty(state, entry.link), else: state
          {:ok, state, wire_id, pdu, chunk, overhead + count}
      end
    else
      _ -> {:error, :invalid_local_transfer}
    end
  end

  defp finalize_outgoing(state, id) do
    case Map.get(state.outgoing_by_id, id) do
      %{complete: true, settled: true} = entry ->
        complete_outgoing(state, id, entry)

      _ ->
        {state, []}
    end
  end

  defp complete_outgoing(state, id, entry) do
    result =
      if terminal?(entry.outcome),
        do: {:ok, entry.outcome},
        else: {:unknown, :receiver_settled_without_outcome}

    complete(remove_outgoing(state, id), entry.op, result)
  end

  defp arm_timer(state, key, explicit_token \\ nil) do
    if Map.has_key?(state.deadlines, key) do
      state
    else
      token = explicit_token || {state.id, {state.incarnation, state.next_token}}
      at = state.last_now_ms + state.options.operation_timeout

      %{
        state
        | deadlines: Map.put(state.deadlines, key, %{token: token, at: at}),
          next_token: state.next_token + 1,
          timer_effects: [{:deadline, key, token, at} | state.timer_effects]
      }
    end
  end

  defp cancel_timer(state, key) do
    if Map.has_key?(state.deadlines, key) do
      %{
        state
        | deadlines: Map.delete(state.deadlines, key),
          timer_effects: [{:cancel_deadline, key} | state.timer_effects]
      }
    else
      state
    end
  end

  defp timer_timeout(state, key, token) do
    case Map.get(state.deadlines, key) do
      %{token: ^token, at: at} when state.last_now_ms >= at ->
        state = cancel_timer(state, key)

        case key do
          {:admission, identity} ->
            case Map.get(state.links_by_identity, identity) do
              %Link{phase: :admission_pending} = link ->
                reject_pending_link(state, link, admission_error(), :admission_timeout)

              _ ->
                {state, []}
            end

          {:link, _} ->
            close(state, :link_close_timeout, true)

          {:partial, id} ->
            case Map.get(state.incoming_by_id, id) do
              %{link: identity, phase: :assembling} ->
                refuse_link(
                  state,
                  Map.fetch!(state.links_by_identity, identity),
                  :partial_timeout
                )

              _ ->
                {state, []}
            end
        end

      _ ->
        {state, []}
    end
  end

  defp remove_outgoing(state, id) do
    case Map.pop(state.outgoing_by_id, id) do
      {nil, _} ->
        state

      {entry, entries} ->
        state =
          update_link(
            state,
            entry.link,
            &%{&1 | tag_to_delivery: Map.delete(&1.tag_to_delivery, entry.tag)}
          )

        %{
          state
          | outgoing_by_id: entries,
            outgoing_bytes: state.outgoing_bytes - byte_size(entry.raw),
            send_queue: filter_queue(state.send_queue, &(&1 != id))
        }
    end
  end

  defp remove_incoming(state, id) do
    case Map.pop(state.incoming_by_id, id) do
      {nil, _} ->
        state

      {entry, entries} ->
        state =
          update_link(state, entry.link, fn link ->
            partial = if link.partial && link.partial.id == id, do: nil, else: link.partial

            %{
              link
              | partial: partial,
                tag_to_delivery: Map.delete(link.tag_to_delivery, entry.tag)
            }
          end)

        state = cancel_timer(release_reservation(state, 1, entry.budget), {:partial, id})

        %{
          state
          | incoming_by_id: entries,
            incoming_token_to_id: Map.delete(state.incoming_token_to_id, entry.token),
            incoming_bytes: state.incoming_bytes - entry.size
        }
    end
  end

  defp release_reservation(state, slots, bytes),
    do: %{
      state
      | incoming_reserved_slots: state.incoming_reserved_slots - slots,
        incoming_reserved_bytes: state.incoming_reserved_bytes - bytes
    }

  defp release_link_payloads(state, identity) do
    state =
      Enum.reduce(state.outgoing_by_id, state, fn {id, entry}, state ->
        if entry.link == identity, do: remove_outgoing(state, id), else: state
      end)

    state =
      Enum.reduce(state.incoming_by_id, state, fn {id, entry}, state ->
        if entry.link == identity, do: remove_incoming(state, id), else: state
      end)

    state =
      case Map.fetch(state.links_by_identity, identity) do
        {:ok, %Link{role: :receiver} = link} ->
          release_reservation(state, link.credit, link.credit * link.max_message_bytes)

        _ ->
          state
      end

    state = update_link(state, identity, &%{&1 | credit: 0, partial: nil})

    %{
      state
      | pending_flows: Map.delete(state.pending_flows, identity),
        control_queue:
          filter_queue(state.control_queue, fn {_, action} ->
            action_identity(action) != identity
          end)
    }
  end

  defp remove_link_resources(state, identity) do
    state = release_link_payloads(state, identity)
    state = cancel_timer(state, {:admission, identity})
    cancel_timer(SessionState.remove_link(state, identity), {:link, identity})
  end

  defp action_identity({kind, identity, _}) when kind in [:attach, :detach], do: identity

  defp action_identity({kind, identity})
       when kind in [:peer_attach, :refusal_attach, :refusal_detach, :flow],
       do: identity

  defp action_identity(_), do: nil

  defp cancel_controls(state, op),
    do: %{
      state
      | control_queue:
          filter_queue(state.control_queue, fn {_, action} -> action_op(action) != op end)
    }

  defp action_op({:begin, op, _}), do: op
  defp action_op({:attach, _, op}), do: op
  defp action_op({:detach, _, op}), do: op
  defp action_op({:settle, _, op, _}), do: op
  defp action_op(_), do: nil

  defp filter_queue(queue, fun),
    do: queue |> :queue.to_list() |> Enum.filter(fun) |> :queue.from_list()

  defp fail_link_operations(state, identity, reason, except) do
    Enum.reduce(Map.to_list(state.operations), {state, []}, fn {op, entry}, {state, effects} ->
      if entry.link == identity and op != except do
        result = failure_result(state, entry, reason)
        {state, more} = complete(cancel_controls(state, op), op, result)
        {state, effects ++ more}
      else
        {state, effects}
      end
    end)
  end

  defp failure_result(state, %{kind: :send} = operation, reason) do
    case Map.get(state.outgoing_by_id, Map.get(operation, :delivery_id)) do
      %{outcome: outcome}
      when is_struct(outcome, Outcome.Accepted) or is_struct(outcome, Outcome.Released) or
             is_struct(outcome, Outcome.Rejected) or is_struct(outcome, Outcome.Modified) ->
        {:error, {:outcome_known, outcome, reason}}

      _ ->
        if operation.emitted, do: {:unknown, reason}, else: {:error, reason}
    end
  end

  defp failure_result(_state, operation, reason),
    do: if(operation.emitted, do: {:unknown, reason}, else: {:error, reason})

  defp close(%{phase: :closed} = state, _reason, _notify), do: {state, []}

  defp close(state, reason, notify) do
    effects =
      Enum.flat_map(state.operations, fn {op, entry} ->
        [
          {:cancel_deadline, {:operation, op}},
          {:complete, op, failure_result(state, entry, reason)}
        ]
      end)

    state = Enum.reduce(Map.keys(state.deadlines), state, &cancel_timer(&2, &1))

    state = %{
      state
      | phase: :closed,
        operations: %{},
        writes: %{},
        control_queue: {[], []},
        pending_flows: %{},
        send_queue: {[], []},
        outgoing_by_id: %{},
        incoming_by_id: %{},
        incoming_token_to_id: %{},
        outgoing_bytes: 0,
        incoming_bytes: 0,
        incoming_reserved_bytes: 0,
        incoming_reserved_slots: 0,
        links_by_identity: %{},
        local_handle_to_link: %{},
        remote_handle_to_link: %{}
    }

    {state, effects ++ if(notify, do: [{:session_error, reason}], else: [])}
  end

  defp allocate_handle(state, ctx) do
    limit = min(state.remote_handle_max, Map.get(ctx, :max_handle, state.remote_handle_max))

    candidate =
      Enum.find(
        0..min(limit, map_size(state.local_handle_to_link)),
        &(not Map.has_key?(state.local_handle_to_link, &1))
      )

    if is_nil(candidate), do: {:error, :handle_limit}, else: {:ok, candidate}
  end

  defp put_link!(state, link) do
    {:ok, state} = SessionState.put_link(state, link)
    state
  end

  defp update_link(state, identity, fun) do
    case Map.fetch(state.links_by_identity, identity) do
      {:ok, link} -> put_link!(state, fun.(link))
      :error -> state
    end
  end

  defp terminal?(state),
    do:
      is_struct(state, Outcome.Accepted) or is_struct(state, Outcome.Released) or
        is_struct(state, Outcome.Rejected) or is_struct(state, Outcome.Modified)

  defp tagged_uint?(%Value{type: :uint, value: value}), do: uint?(value)
  defp tagged_uint?(_), do: false
  defp uint?(value), do: integer_between?(value, 0, @uint_max)

  defp integer_between?(value, low, high),
    do: is_integer(value) and value >= low and value <= high

  defp valid_name?(%Value{type: :string, value: value}),
    do: is_binary(value) and byte_size(value) <= 255 and String.valid?(value)

  defp valid_name?(_), do: false

  defp valid_tag?(%Value{type: :binary, value: value}),
    do: is_binary(value) and byte_size(value) <= 32

  defp valid_tag?(_), do: false
  defp number(value, default \\ nil)
  defp number(%Value{value: value}, _default), do: value
  defp number(_, default), do: default
  defp truth?(%Value{type: :boolean, value: true}), do: true
  defp truth?(_), do: false
  defp tagged_optional(_type, nil), do: nil
  defp tagged_optional(:ushort, value), do: Value.ushort(value)
  defp append_result({state, more}, effects), do: {state, effects ++ more}
end
