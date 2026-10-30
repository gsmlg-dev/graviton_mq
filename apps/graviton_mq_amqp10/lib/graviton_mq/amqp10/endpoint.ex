defmodule GravitonMQ.AMQP10.Endpoint do
  @moduledoc """
  Bounded, pure AMQP 1.0 connection endpoint shared by the outgoing and broker shells.

  `step/2` consumes generation-fenced facts with an explicit monotonic `now_ms`
  and returns state and effects. The shell interprets writes, absolute deadlines,
  notifications and transport closure. A write receipt proves only local socket
  acceptance. Session-owned delivery responsibility remains in `SessionEngine`.

  This initial profile uses AMQP 1.0 without SASL, volatile nontransactional
  format-zero messages and receiver-first settlement. There is no transport
  reconnection or implicit replay. Frame bodies remain opaque until this layer
  explicitly invokes the performative codec.
  """

  alias GravitonMQ.AMQP10.{ConnectionState, Frame, SessionEngine, Value}
  alias GravitonMQ.AMQP10.Codec.{Limits, Performative, ProtocolHeader}
  alias GravitonMQ.AMQP10.Codec.Value, as: WireValue
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.AMQP10.Performative.{Begin, Close, End, Open, Transfer}

  @defaults [
    generation: 0,
    role: :client,
    max_frame_size: 65_536,
    channel_max: 255,
    max_sessions: 16,
    max_pending_operations: 256,
    max_pending_controls: 32,
    max_write_frames: 64,
    max_write_bytes: 1_048_576,
    control_reserve_frames: 4,
    control_reserve_bytes: 4096,
    operation_timeout_ms: 10_000,
    close_timeout_ms: 5000,
    idle_timeout_ms: 30_000,
    session_options: []
  ]

  defstruct [
    :connection,
    :limits,
    :options,
    :generation,
    phase: :new,
    remote_header: false,
    remote_open: false,
    peer_max_frame_size: 512,
    peer_channel_max: 65_535,
    peer_idle_timeout_ms: 0,
    now_ms: nil,
    last_received_ms: 0,
    last_sent_ms: 0,
    next_write_id: 0,
    next_token: 0,
    next_session_id: 0,
    writes: %{},
    write_bytes: 0,
    pending_controls: [],
    operations: %{},
    deadlines: %{},
    session_controls: %{},
    link_name_conflicts: MapSet.new()
  ]

  @type t :: %__MODULE__{}

  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(options) when is_list(options) do
    if Keyword.keyword?(options), do: build(options), else: {:error, :invalid_options}
  end

  def new(_), do: {:error, :invalid_options}

  @doc "Directional outgoing codec limits; the local Open limits only incoming frames."
  @spec outgoing_limits(t()) :: Limits.t()
  def outgoing_limits(%__MODULE__{} = state) do
    %{
      state.limits
      | max_frame_size: state.peer_max_frame_size,
        max_value_bytes: min(state.peer_max_frame_size, state.options.max_write_bytes)
    }
  end

  @doc """
  Conservative capacity for one queue message on an admitted sender Link.

  This is a snapshot, not a reservation. Broker owners re-query after each
  endpoint transition instead of repeating credit or serial-number arithmetic.
  """
  @spec send_capacity(t(), non_neg_integer(), GravitonMQ.AMQP10.Link.identity()) ::
          %{messages: 0 | 1, bytes: non_neg_integer()}
  def send_capacity(%__MODULE__{phase: :active} = state, session_id, identity) do
    with %{phase: :active} <- Map.get(state.session_controls, session_id),
         {:ok, session} <- Map.fetch(state.connection.sessions_by_identity, session_id),
         true <- map_size(state.operations) < state.options.max_pending_operations,
         true <- state.pending_controls == [] do
      SessionEngine.send_capacity(session, identity, session_context(state, session))
    else
      _ -> %{messages: 0, bytes: 0}
    end
  end

  def send_capacity(%__MODULE__{}, _session_id, _identity), do: %{messages: 0, bytes: 0}

  defp build(options) do
    opts = Map.new(Keyword.merge(@defaults, options))
    container = Map.get(opts, :container_id)

    with :ok <- validate_options(opts, container),
         {:ok, connection} <- ConnectionState.new(local_container_id: tagged(:string, container)) do
      state = %__MODULE__{
        connection: connection,
        generation: opts.generation,
        options: opts,
        limits: %Limits{
          max_frame_size: opts.max_frame_size,
          max_value_bytes: opts.max_frame_size
        }
      }

      case Performative.encode(local_open(state), %Limits{
             max_frame_size: 512,
             max_value_bytes: 504
           }) do
        {:ok, bytes} when byte_size(bytes) <= 504 -> {:ok, state}
        _ -> {:error, :open_exceeds_initial_frame_limit}
      end
    end
  end

  defp validate_options(opts, container) do
    numeric = [
      :max_sessions,
      :max_pending_operations,
      :max_pending_controls,
      :max_write_frames,
      :max_write_bytes,
      :control_reserve_frames,
      :control_reserve_bytes,
      :operation_timeout_ms,
      :close_timeout_ms
    ]

    cond do
      not is_binary(container) or byte_size(container) == 0 or byte_size(container) > 255 or
          not String.valid?(container) ->
        {:error, :invalid_container_id}

      opts.role not in [:client, :broker] ->
        {:error, :invalid_role}

      not is_integer(opts.generation) or opts.generation < 0 ->
        {:error, :invalid_generation}

      not in_range?(opts.max_frame_size, 512, 0xFFFF_FFFF) ->
        {:error, :invalid_max_frame_size}

      not in_range?(opts.channel_max, 0, 65_535) ->
        {:error, :invalid_channel_max}

      not in_range?(opts.idle_timeout_ms, 0, 0xFFFF_FFFF) ->
        {:error, :invalid_idle_timeout}

      Enum.any?(numeric, &(not in_range?(Map.fetch!(opts, &1), 1, 0x7FFF_FFFF))) ->
        {:error, :invalid_limit}

      opts.max_write_frames <= opts.control_reserve_frames or opts.control_reserve_frames < 2 ->
        {:error, :invalid_write_reserve}

      opts.max_write_bytes <= opts.control_reserve_bytes or opts.control_reserve_bytes < 1024 ->
        {:error, :invalid_write_reserve}

      not Keyword.keyword?(opts.session_options) ->
        {:error, :invalid_session_options}

      Keyword.has_key?(opts.session_options, :remote_channel) ->
        {:error, :remote_channel_is_peer_owned}

      Keyword.has_key?(opts.session_options, :peer_attach_policy) ->
        {:error, :peer_attach_policy_is_role_owned}

      not is_nil(Map.get(opts, :hostname)) and not is_binary(Map.get(opts, :hostname)) ->
        {:error, :invalid_hostname}

      Map.get(opts, :sasl, false) != false ->
        {:error, :sasl_not_supported}

      Map.get(opts, :required_capabilities, []) != [] ->
        {:error, :capabilities_not_supported}

      true ->
        :ok
    end
  end

  @spec step(t(), map()) :: {t(), list()}
  def step(
        %__MODULE__{generation: generation} = state,
        %{generation: generation, now_ms: now, event: event} = facts
      )
      when is_integer(now) do
    if is_integer(state.now_ms) and now < state.now_ms do
      {state, []}
    else
      {state, effects} =
        dispatch(
          %{
            state
            | now_ms: now,
              link_name_conflicts: Map.get(facts, :link_name_conflicts, MapSet.new())
          },
          event
        )

      {%{state | link_name_conflicts: MapSet.new()}, effects}
    end
  end

  def step(%__MODULE__{} = state, _), do: {state, []}

  defp dispatch(%{phase: :new} = state, :start) do
    state = %{state | phase: :opening, last_received_ms: state.now_ms, last_sent_ms: state.now_ms}
    {state, header} = emit_header(state)
    {state, open} = emit_frame(state, 0, local_open(state), <<>>, :connection)
    {state, deadline} = set_deadline(state, :open, state.options.operation_timeout_ms)
    {state, header ++ open ++ deadline}
  end

  defp dispatch(state, :start), do: {state, []}
  defp dispatch(%{phase: :closed} = state, {:write_result, _, _}), do: {state, []}
  defp dispatch(state, {:write_result, id, result}), do: write_result(state, id, result)

  defp dispatch(%{phase: :closed} = state, {:command, op, _}),
    do: {state, [{:operation, op, {:error, :closed}}]}

  defp dispatch(%{phase: :closed} = state, _), do: {state, []}
  defp dispatch(state, {:transport_down, reason}), do: shutdown(state, reason)

  defp dispatch(
         state,
         {:header, %ProtocolHeader{protocol_id: 0, major: 1, minor: 0, revision: 0}}
       ) do
    if state.phase == :opening and not state.remote_header,
      do: {%{state | remote_header: true}, []},
      else: connection_error(state, :unexpected_protocol_header)
  end

  defp dispatch(state, {:header, _}), do: shutdown(state, :unsupported_protocol_header)

  defp dispatch(state, {:decode_error, %GravitonMQ.AMQP10.Codec.Error{reason: reason}}),
    do: connection_error(state, {:decode_error, reason})

  defp dispatch(state, {:frame, %Frame{} = frame}), do: incoming_frame(state, frame)
  defp dispatch(state, {:timeout, key, token}), do: timeout(state, key, token)
  defp dispatch(state, {:command, op, command}), do: command(state, op, command)
  defp dispatch(state, _), do: connection_error(state, :invalid_event)

  defp local_open(state) do
    %Open{
      container_id: state.connection.local_container_id,
      hostname: optional_string(Map.get(state.options, :hostname)),
      max_frame_size: tagged(:uint, state.options.max_frame_size),
      channel_max: tagged(:ushort, state.options.channel_max),
      idle_time_out: tagged(:uint, div(state.options.idle_timeout_ms, 2))
    }
  end

  defp incoming_frame(state, frame) do
    cond do
      not state.remote_header ->
        connection_error(state, :frame_before_protocol_header)

      not valid_frame?(frame, state) ->
        connection_error(state, :invalid_frame)

      frame.body == <<>> and not state.remote_open ->
        connection_error(state, :first_frame_must_be_open)

      frame.body == <<>> ->
        received(state)

      ending_session?(state, frame.channel) ->
        case ending_body_scope(frame.body, state.limits) do
          :connection ->
            decode_frame_body(state, frame)

          :end ->
            case Performative.decode(frame.body, state.limits) do
              {:ok, %End{} = pdu, <<>>} ->
                {state, timers} = received(state)
                {state, effects} = route_frame(state, frame.channel, pdu, <<>>)
                {state, timers ++ effects}

              _ ->
                received(state)
            end

          :session ->
            received(state)
        end

      true ->
        decode_frame_body(state, frame)
    end
  end

  defp decode_frame_body(state, frame) do
    case Performative.decode(frame.body, state.limits) do
      {:ok, pdu, rest} when rest == <<>> or is_struct(pdu, Transfer) ->
        {state, timers} = received(state)
        {state, effects} = route_frame(state, frame.channel, pdu, rest)
        {state, timers ++ effects}

      _ ->
        connection_error(state, :malformed_performative)
    end
  end

  defp ending_session?(state, channel) do
    case ConnectionState.fetch_incoming_session(state.connection, channel) do
      {:ok, session} -> match?(%{phase: :ending}, Map.get(state.session_controls, session.id))
      :error -> false
    end
  end

  # Frame headers are validated first. On an ending Session only End can
  # acknowledge/remap it, while Connection Open/Close retain full validation.
  # Read just the bounded descriptor; do not decode ignored Session bodies.
  defp ending_body_scope(<<0, descriptor::binary>>, limits) do
    case WireValue.decode(descriptor, limits) do
      {:ok, %Value{type: :ulong, value: code}, _} when code in [0x10, 0x18] ->
        :connection

      {:ok, %Value{type: :symbol, value: name}, _}
      when name in ["amqp:open:list", "amqp:close:list"] ->
        :connection

      {:ok, %Value{type: :ulong, value: 0x17}, _} ->
        :end

      {:ok, %Value{type: :symbol, value: "amqp:end:list"}, _} ->
        :end

      _ ->
        :session
    end
  end

  defp ending_body_scope(_body, _limits), do: :session

  defp valid_frame?(frame, state) do
    is_binary(frame.body) and is_binary(frame.extended_header) and frame.type == :amqp and
      in_range?(frame.channel, 0, state.options.channel_max) and
      in_range?(frame.data_offset_words, 2, 255) and
      byte_size(frame.extended_header) == frame.data_offset_words * 4 - 8 and
      frame.declared_size == 8 + byte_size(frame.extended_header) + byte_size(frame.body) and
      frame.declared_size <= state.options.max_frame_size
  end

  defp route_frame(%{remote_open: false} = state, 0, %Open{} = pdu, <<>>) do
    connection = %{state.connection | remote_container_id: pdu.container_id}

    state = %{
      state
      | connection: connection,
        phase: :active,
        remote_open: true,
        peer_max_frame_size: scalar(pdu.max_frame_size, 0xFFFF_FFFF),
        peer_channel_max: scalar(pdu.channel_max, 65_535),
        peer_idle_timeout_ms: scalar(pdu.idle_time_out, 0)
    }

    {state, canceled} = cancel_deadline(state, :open)
    {state, idle} = receive_deadline(state)
    {state, heartbeat} = heartbeat_deadline(state)
    {state, [{:event, {:connected, pdu.container_id.value}}] ++ canceled ++ idle ++ heartbeat}
  end

  defp route_frame(%{remote_open: false} = state, _, _, _),
    do: connection_error(state, :first_frame_must_be_open)

  defp route_frame(state, _, %Open{}, _), do: connection_error(state, :duplicate_open)
  defp route_frame(state, _, %Close{} = close, _), do: peer_close(state, close)

  defp route_frame(%{phase: :closing} = state, _, _, _), do: {state, []}
  defp route_frame(state, channel, %Begin{} = pdu, <<>>), do: peer_begin(state, channel, pdu)
  defp route_frame(state, channel, %End{} = pdu, <<>>), do: peer_end(state, channel, pdu)

  defp route_frame(state, channel, pdu, payload) do
    case ConnectionState.fetch_incoming_session(state.connection, channel) do
      {:ok, session} ->
        if Map.fetch!(state.session_controls, session.id).phase == :ending,
          do: {state, []},
          else: engine_step(state, session.id, {:peer, pdu, payload})

      :error ->
        connection_error(state, :unmapped_session_channel)
    end
  end

  defp command(%{phase: phase} = state, op, _) when phase != :active,
    do: {state, [{:operation, op, {:error, :not_connected}}]}

  defp command(state, op, command) do
    cond do
      Map.has_key?(state.operations, op) ->
        {state, [{:operation, op, {:error, :duplicate_operation}}]}

      map_size(state.operations) >= state.options.max_pending_operations ->
        {state, [{:operation, op, {:error, :operation_limit}}]}

      true ->
        admit(state, op, command)
    end
  end

  defp admit(state, op, {:open_session, id, opts}) when is_list(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        operation_error(state, op, :invalid_session_options)

      Keyword.has_key?(opts, :remote_channel) ->
        operation_error(state, op, :remote_channel_is_peer_owned)

      Keyword.has_key?(opts, :peer_attach_policy) ->
        operation_error(state, op, :peer_attach_policy_is_role_owned)

      not in_range?(id, 0, 0xFFFF_FFFF) ->
        operation_error(state, op, :invalid_session_identity)

      Map.has_key?(state.connection.sessions_by_identity, id) ->
        operation_error(state, op, :duplicate_session_identity)

      map_size(state.connection.sessions_by_identity) >= state.options.max_sessions ->
        operation_error(state, op, :session_limit)

      true ->
        with {:ok, channel} <- allocate_channel(state),
             {:ok, session} <-
               SessionEngine.new(id, channel, Keyword.merge(session_options(state), opts)),
             session = %{session | incarnation: state.next_token},
             {:ok, connection} <- ConnectionState.put_session(state.connection, session) do
          state = %{
            state
            | connection: connection,
              next_token: state.next_token + 1,
              session_controls:
                Map.put(state.session_controls, id, %{phase: :opening, open_op: op})
          }

          {state, timers} = track_operation(state, op, id, :open)
          {state, effects} = engine_step(state, id, {:local_begin, op})
          {state, effects ++ timers}
        else
          {:error, reason} -> operation_error(state, op, reason)
        end
    end
  end

  defp admit(state, op, {:session, id, command}) do
    case Map.fetch(state.session_controls, id) do
      {:ok, %{phase: :active}} ->
        {state, timers} = track_operation(state, op, id, :session)
        {state, effects} = engine_step(state, id, {:command, op, command})
        {state, effects ++ timers}

      _ ->
        operation_error(state, op, :session_not_active)
    end
  end

  defp admit(state, op, {:end_session, id}) do
    case Map.fetch(state.session_controls, id) do
      {:ok, %{phase: :active}} ->
        {state, timers} = track_operation(state, op, id, :end)
        {state, effects} = request_end(state, id, nil, op)
        {state, effects ++ timers}

      {:ok, _} ->
        operation_error(state, op, :session_not_active)

      :error ->
        operation_error(state, op, :unknown_session)
    end
  end

  defp admit(state, op, :close) do
    {state, timers} = track_operation(state, op, nil, :close)
    {state, effects} = begin_close(state, nil)
    {state, effects ++ timers}
  end

  defp admit(state, op, _), do: operation_error(state, op, :unsupported_command)

  defp peer_begin(state, channel, pdu) do
    cond do
      Map.has_key?(state.connection.remote_channel_to_session, channel) ->
        connection_error(state, :duplicate_remote_channel)

      scalar(pdu.incoming_window, 0) >= 0x8000_0000 or
          scalar(pdu.outgoing_window, 0) >= 0x8000_0000 ->
        connection_error(state, :ambiguous_session_window)

      pdu.remote_channel != nil ->
        bind_begin(state, channel, pdu)

      map_size(state.connection.sessions_by_identity) >= state.options.max_sessions ->
        connection_error(state, :session_limit)

      true ->
        remote_begin(state, channel, pdu)
    end
  end

  defp bind_begin(state, channel, pdu) do
    with {:ok, session} <-
           ConnectionState.fetch_outgoing_session(state.connection, pdu.remote_channel.value),
         %{phase: phase} = control <- Map.get(state.session_controls, session.id),
         true <- phase in [:opening, :ending],
         {:ok, connection} <-
           ConnectionState.put_session(state.connection, %{session | remote_channel: channel}) do
      state = %{
        state
        | connection: connection,
          session_controls:
            Map.put(state.session_controls, session.id, %{
              control
              | phase: if(phase == :ending, do: :ending, else: :active)
            })
      }

      if phase == :ending do
        {state, []}
      else
        {state, effects} = engine_step(state, session.id, {:peer_begin, pdu})
        {state, complete} = complete_operation(state, control.open_op, {:ok, session.id})
        {state, effects ++ complete ++ [{:event, {:session_opened, session.id}}]}
      end
    else
      _ -> connection_error(state, :invalid_begin_mapping)
    end
  end

  defp remote_begin(state, remote_channel, pdu) do
    id = available_id(state.connection.sessions_by_identity, state.next_session_id)

    with {:ok, local_channel} <- allocate_channel(state),
         {:ok, session} <-
           SessionEngine.new(
             id,
             local_channel,
             Keyword.put(session_options(state), :remote_channel, remote_channel)
           ),
         {:ok, connection} <-
           ConnectionState.put_session(state.connection, %{
             session
             | remote_channel: remote_channel,
               incarnation: state.next_token
           }) do
      state = %{
        state
        | connection: connection,
          next_token: state.next_token + 1,
          next_session_id: id + 1,
          session_controls: Map.put(state.session_controls, id, %{phase: :active})
      }

      {state, effects} = engine_step(state, id, {:peer_begin, pdu})
      {state, effects ++ [{:event, {:session_opened, id}}]}
    else
      {:error, reason} -> connection_error(state, reason)
    end
  end

  defp peer_end(state, channel, pdu) do
    case ConnectionState.fetch_incoming_session(state.connection, channel) do
      {:ok, session} ->
        control = Map.fetch!(state.session_controls, session.id)

        {state, reply} =
          if control.phase == :ending,
            do: {state, []},
            else: emit_frame(state, session.local_channel, %End{}, <<>>, :connection)

        {state, effects} =
          engine_step(state, session.id, {:transport_down, {:session_ended, pdu.error}})

        {state, complete} =
          if Map.has_key?(control, :end_op),
            do:
              complete_operation(
                state,
                control.end_op,
                if(pdu.error, do: {:error, {:peer_end, pdu.error}}, else: {:ok, session.id})
              ),
            else: {state, []}

        {state, failed} = fail_session_operations(state, session.id, :session_ended)
        {:ok, connection} = ConnectionState.remove_session(state.connection, session.id)

        state = %{
          state
          | connection: connection,
            session_controls: Map.delete(state.session_controls, session.id)
        }

        {state, timers} = cancel_session_deadlines(state, session.id)
        state = drop_session_controls(state, session.id)

        {state,
         reply ++
           effects ++
           complete ++ failed ++ timers ++ [{:event, {:session_ended, session.id, pdu.error}}]}

      :error ->
        connection_error(state, :unmapped_session_channel)
    end
  end

  defp engine_step(state, id, event) do
    session = Map.fetch!(state.connection.sessions_by_identity, id)

    {next_session, effects} = SessionEngine.step(session, event, session_context(state, session))

    state =
      if Enum.any?(effects, &match?({:session_error, _}, &1)) do
        # SessionEngine has cleared its Link indexes on error. Retain the old
        # names until the peer acknowledges End, just as for a host-initiated End.
        names =
          session.links_by_identity
          |> Enum.reject(fn {_identity, link} -> link.phase == :refused end)
          |> MapSet.new(fn {identity, _link} -> identity end)

        control =
          Map.fetch!(state.session_controls, id)
          |> Map.update(:ending_link_names, names, &MapSet.union(&1, names))

        %{state | session_controls: Map.put(state.session_controls, id, control)}
      else
        state
      end

    {:ok, connection} = ConnectionState.put_session(state.connection, next_session)
    interpret_session(%{state | connection: connection}, id, effects)
  end

  defp session_context(state, session) do
    %{
      now_ms: state.now_ms,
      generation: state.generation,
      max_frame_size: state.peer_max_frame_size,
      max_handle: Map.get(session, :remote_handle_max, 0xFFFF_FFFF),
      occupied_link_names: occupied_link_names(state, session.id),
      write_frames:
        max(
          0,
          state.options.max_write_frames - state.options.control_reserve_frames -
            map_size(state.writes)
        ),
      write_bytes:
        max(
          0,
          state.options.max_write_bytes - state.options.control_reserve_bytes - state.write_bytes
        ),
      control_frames: max(0, state.options.max_write_frames - 1 - map_size(state.writes)),
      control_bytes: max(0, state.options.max_write_bytes - 512 - state.write_bytes)
    }
  end

  defp occupied_link_names(state, current_session) do
    Enum.reduce(state.connection.sessions_by_identity, state.link_name_conflicts, fn {id, session},
                                                                                     names ->
      if id == current_session do
        names
      else
        names =
          MapSet.union(
            names,
            Map.get(state.session_controls[id], :ending_link_names, MapSet.new())
          )

        Enum.reduce(session.links_by_identity, names, fn {identity, link}, names ->
          if link.phase == :refused, do: names, else: MapSet.put(names, identity)
        end)
      end
    end)
  end

  defp interpret_session(state, id, effects) do
    Enum.reduce(effects, {state, []}, fn effect, {state, output} ->
      {state, more} = session_effect(state, id, effect)
      {state, output ++ more}
    end)
  end

  defp session_effect(state, id, {:emit, receipt, pdu, payload}) do
    session = Map.fetch!(state.connection.sessions_by_identity, id)
    emit_frame(state, session.local_channel, pdu, payload, {:session, id, receipt})
  end

  defp session_effect(state, _id, {:complete, op, result}),
    do: complete_operation(state, op, result)

  defp session_effect(state, id, {:delivery, link, token, raw, metadata}),
    do: {state, [{:delivery, id, link, token, raw, metadata}]}

  defp session_effect(state, id, {:link_event, event}),
    do: {state, [{:event, {:session, id, event}}]}

  defp session_effect(state, id, {:deadline, key, token, at}) do
    key = {:session, id, key}
    {state, canceled} = cancel_deadline(state, key)
    state = %{state | deadlines: Map.put(state.deadlines, key, {token, at})}
    {state, canceled ++ [{:wake_at, state.generation, key, token, at}]}
  end

  defp session_effect(state, id, {:cancel_deadline, key}),
    do: cancel_deadline(state, {:session, id, key})

  defp session_effect(state, _id, {:connection_error, reason}),
    do: connection_error(state, reason)

  defp session_effect(state, id, {:session_error, reason}) do
    {state, effects} = request_end(state, id, reason, nil)
    {state, effects ++ [{:event, {:session_error, id, reason}}]}
  end

  defp emit_header(state) do
    bytes = <<"AMQP", 0, 1, 0, 0>>
    {state, id} = reserve_write(state, byte_size(bytes), :connection)
    {state, [{:send_header, state.generation, id, bytes}]}
  end

  defp emit_frame(state, channel, pdu, payload, owner) do
    case wire_size(pdu, payload, state) do
      {:ok, size} ->
        cond do
          write_capacity?(state, pdu, size) ->
            {state, id} = reserve_write(state, size, owner)
            state = mark_emitted(state, owner)
            state = if is_struct(pdu, Close), do: mark_close_emitted(state), else: state
            {state, heartbeat} = heartbeat_deadline(state)
            {state, [{:send_frame, state.generation, id, channel, pdu, payload}] ++ heartbeat}

          length(state.pending_controls) < state.options.max_pending_controls and
              not is_struct(pdu, Transfer) ->
            {%{
               state
               | pending_controls: state.pending_controls ++ [{channel, pdu, payload, owner}]
             }, []}

          true ->
            shutdown(state, :write_capacity_exceeded)
        end

      {:error, reason} ->
        shutdown(state, {:outgoing_frame, reason})
    end
  end

  defp wire_size(nil, <<>>, _state), do: {:ok, 8}

  defp wire_size(pdu, payload, state) when is_binary(payload) do
    limits = outgoing_limits(state)
    limit = limits.max_frame_size
    # Before the peer Open the mandatory minimum is the only safe send limit.
    case Performative.encode(pdu, limits) do
      {:ok, bytes} when byte_size(bytes) + byte_size(payload) + 8 <= limit ->
        {:ok, byte_size(bytes) + byte_size(payload) + 8}

      {:ok, _} ->
        {:error, :frame_size_limit}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp write_capacity?(state, %Close{}, size),
    do:
      map_size(state.writes) < state.options.max_write_frames and
        state.write_bytes + size <= state.options.max_write_bytes

  defp write_capacity?(state, _, size),
    do:
      map_size(state.writes) < state.options.max_write_frames - 1 and
        state.write_bytes + size <= state.options.max_write_bytes - 512

  defp reserve_write(state, bytes, owner) do
    id = state.next_write_id

    state = %{
      state
      | next_write_id: id + 1,
        writes: Map.put(state.writes, id, %{bytes: bytes, owner: owner}),
        write_bytes: state.write_bytes + bytes,
        last_sent_ms: state.now_ms
    }

    {state, id}
  end

  defp mark_emitted(state, {:operation, op}) do
    update_in(state.operations, fn ops ->
      if Map.has_key?(ops, op), do: Map.update!(ops, op, &Map.put(&1, :emitted, true)), else: ops
    end)
  end

  defp mark_emitted(state, {:session, id, _}) do
    operations =
      Map.new(state.operations, fn {op, data} ->
        {op,
         if(data.session == id and data.kind == :open, do: %{data | emitted: true}, else: data)}
      end)

    %{state | operations: operations}
  end

  defp mark_emitted(state, _), do: state

  defp mark_close_emitted(state) do
    operations =
      Map.new(state.operations, fn {op, data} ->
        {op, if(data.kind == :close, do: %{data | emitted: true}, else: data)}
      end)

    %{state | operations: operations}
  end

  defp write_result(state, id, result) do
    case Map.pop(state.writes, id) do
      {nil, _} ->
        {state, []}

      {write, remaining} ->
        state = %{state | writes: remaining, write_bytes: state.write_bytes - write.bytes}

        {state, effects} =
          case write.owner do
            {:session, session, receipt} ->
              if Map.has_key?(state.connection.sessions_by_identity, session),
                do: engine_step(state, session, {:write_result, receipt, result}),
                else: {state, []}

            _ ->
              {state, []}
          end

        if result == :ok do
          {state, queued} = flush_controls(state)
          {state, sessions} = flush_sessions(state)
          {state, effects ++ queued ++ sessions}
        else
          {state, closed} = shutdown(state, {:write_failed, result})
          {state, effects ++ closed}
        end
    end
  end

  defp flush_controls(%{pending_controls: []} = state), do: {state, []}
  defp flush_controls(%{phase: phase} = state) when phase in [:closing, :closed], do: {state, []}

  defp flush_controls(state) do
    [{channel, pdu, payload, owner} | tail] = state.pending_controls
    {:ok, bytes} = wire_size(pdu, payload, state)

    if write_capacity?(state, pdu, bytes) do
      {state, effects} =
        emit_frame(%{state | pending_controls: tail}, channel, pdu, payload, owner)

      {state, more} = flush_controls(state)
      {state, effects ++ more}
    else
      {state, []}
    end
  end

  defp flush_sessions(%{phase: :active} = state) do
    Enum.reduce(
      Map.keys(state.connection.sessions_by_identity) |> Enum.sort(),
      {state, []},
      fn id, {state, output} ->
        if Map.get(state.session_controls, id, %{})[:phase] == :active do
          {state, effects} = engine_step(state, id, :flush)
          {state, output ++ effects}
        else
          {state, output}
        end
      end
    )
  end

  defp flush_sessions(state), do: {state, []}

  defp track_operation(state, op, session, kind) do
    state = %{
      state
      | operations: Map.put(state.operations, op, %{session: session, kind: kind, emitted: false})
    }

    if kind == :session,
      do: {state, []},
      else: set_deadline(state, {:operation, op}, state.options.operation_timeout_ms)
  end

  defp session_options(state),
    do:
      Keyword.merge(
        [
          operation_timeout: state.options.operation_timeout_ms,
          peer_attach_policy: if(state.options.role == :broker, do: :defer, else: :auto)
        ],
        state.options.session_options
      )

  defp complete_operation(state, op, result) do
    if Map.has_key?(state.operations, op) do
      operation = Map.fetch!(state.operations, op)
      state = %{state | operations: Map.delete(state.operations, op)}
      {state, timers} = cancel_deadline(state, {:operation, op})

      {state, engine_timer} =
        cancel_deadline(state, {:session, operation.session, {:operation, op}})

      {state, [{:operation, op, result}] ++ timers ++ engine_timer}
    else
      {state, []}
    end
  end

  defp timeout(state, key, token) do
    case Map.get(state.deadlines, key) do
      {^token, at} when at <= state.now_ms ->
        state = %{state | deadlines: Map.delete(state.deadlines, key)}
        expired(state, key, token)

      _ ->
        {state, []}
    end
  end

  defp expired(state, :open, _), do: connection_error(state, :open_timeout)
  defp expired(state, :close, _), do: shutdown(state, :close_timeout)
  defp expired(state, :receive_idle, _), do: connection_error(state, :idle_timeout)

  defp expired(%{phase: :active} = state, :heartbeat, _) do
    {state, effects} = emit_frame(state, 0, nil, <<>>, :connection)

    if Map.has_key?(state.deadlines, :heartbeat),
      do: {state, effects},
      else: heartbeat_deadline_with_effects(state, effects)
  end

  defp expired(state, :heartbeat, _), do: {state, []}

  defp expired(state, {:session, id, key}, token) do
    if Map.has_key?(state.connection.sessions_by_identity, id),
      do: engine_step(state, id, {:timeout, key, token}),
      else: {state, []}
  end

  defp expired(state, {:operation, op}, _) do
    case Map.fetch(state.operations, op) do
      {:ok, %{kind: :session, session: id}} ->
        engine_step(state, id, {:timeout, {:operation, op}, :endpoint})

      {:ok, data} ->
        result =
          if data.emitted or data.kind == :close,
            do: {:unknown, :timeout},
            else: {:error, :timeout}

        {state, effects} = complete_operation(state, op, result)

        if data.session != nil do
          {state, ending} = request_end(state, data.session, :operation_timeout, nil)
          {state, effects ++ ending}
        else
          {state, effects}
        end

      :error ->
        {state, []}
    end
  end

  defp expired(state, {:session_close, id}, _),
    do: connection_error(state, {:session_close_timeout, id})

  defp request_end(state, id, reason, op) do
    names =
      state.connection.sessions_by_identity[id].links_by_identity
      |> Enum.reject(fn {_identity, link} -> link.phase == :refused end)
      |> MapSet.new(fn {identity, _link} -> identity end)

    {state, effects} = engine_step(state, id, {:transport_down, reason || :session_ended})
    session = Map.fetch!(state.connection.sessions_by_identity, id)

    control =
      Map.fetch!(state.session_controls, id)
      |> Map.put(:phase, :ending)
      |> Map.update(:ending_link_names, names, &MapSet.union(&1, names))

    control = if is_nil(op), do: control, else: Map.put(control, :end_op, op)
    state = %{state | session_controls: Map.put(state.session_controls, id, control)}
    state = drop_session_controls(state, id)
    {state, canceled} = cancel_session_deadlines(state, id)
    owner = if is_nil(op), do: :connection, else: {:operation, op}

    {state, sent} =
      emit_frame(
        state,
        session.local_channel,
        %End{error: if(reason, do: protocol_error(reason))},
        <<>>,
        owner
      )

    {state, deadline} = set_deadline(state, {:session_close, id}, state.options.close_timeout_ms)
    {state, effects ++ canceled ++ sent ++ deadline}
  end

  defp drop_session_controls(state, id) do
    pending =
      Enum.reject(state.pending_controls, fn
        {_, _, _, {:session, ^id, _}} -> true
        _ -> false
      end)

    %{state | pending_controls: pending}
  end

  defp peer_close(state, close) do
    {state, reply} =
      if state.phase == :closing,
        do: {state, []},
        else: emit_frame(state, 0, %Close{}, <<>>, :connection)

    {state, complete} =
      Enum.reduce(state.operations, {state, []}, fn
        {op, %{kind: :close}}, {state, output} ->
          result = if close.error, do: {:error, {:peer_close, close.error}}, else: {:ok, :closed}
          {state, effects} = complete_operation(state, op, result)
          {state, output ++ effects}

        _, acc ->
          acc
      end)

    reason = if close.error, do: {:peer_close, close.error}, else: :normal
    {state, effects} = shutdown(state, reason)
    {state, reply ++ complete ++ effects}
  end

  defp connection_error(state, reason) do
    if state.phase in [:new, :closed],
      do: shutdown(state, reason),
      else: begin_close(state, reason)
  end

  defp begin_close(%{phase: :closing} = state, _), do: {state, []}

  defp begin_close(state, reason) do
    state = %{state | phase: :closing, pending_controls: []}
    {state, idle} = cancel_deadline(state, :receive_idle)
    {state, heartbeat} = cancel_deadline(state, :heartbeat)

    {state, sent} =
      emit_frame(
        state,
        0,
        %Close{error: if(reason, do: protocol_error(reason))},
        <<>>,
        :connection
      )

    {state, deadline} = set_deadline(state, :close, state.options.close_timeout_ms)
    {state, sent ++ idle ++ heartbeat ++ deadline}
  end

  defp shutdown(%{phase: :closed} = state, _), do: {state, []}

  defp shutdown(state, reason) do
    {state, session_effects} =
      Enum.reduce(Map.keys(state.connection.sessions_by_identity), {state, []}, fn id,
                                                                                   {state, output} ->
        {state, effects} = engine_step(state, id, {:transport_down, reason})
        {state, output ++ effects}
      end)

    {state, operations} =
      Enum.reduce(state.operations, {state, []}, fn {op, data}, {state, output} ->
        result = if data.emitted, do: {:unknown, reason}, else: {:error, reason}
        {state, effects} = complete_operation(state, op, result)
        {state, output ++ effects}
      end)

    canceled =
      Enum.map(state.deadlines, fn {key, {token, _}} ->
        {:cancel_wake, state.generation, key, token}
      end)

    {:ok, connection} =
      ConnectionState.new(
        local_container_id: state.connection.local_container_id,
        remote_container_id: state.connection.remote_container_id
      )

    state = %{
      state
      | phase: :closed,
        connection: connection,
        session_controls: %{},
        operations: %{},
        writes: %{},
        write_bytes: 0,
        pending_controls: [],
        deadlines: %{}
    }

    {state,
     session_effects ++
       operations ++
       canceled ++
       [{:event, {:disconnected, reason}}, {:close_transport, state.generation, reason}]}
  end

  defp fail_session_operations(state, id, reason) do
    Enum.reduce(state.operations, {state, []}, fn
      {op, %{session: ^id} = data}, {state, output} ->
        {state, effects} =
          complete_operation(
            state,
            op,
            if(data.emitted, do: {:unknown, reason}, else: {:error, reason})
          )

        {state, output ++ effects}

      _, acc ->
        acc
    end)
  end

  defp received(state) do
    state = %{state | last_received_ms: state.now_ms}

    if state.remote_open and state.phase == :active,
      do: receive_deadline(state),
      else: {state, []}
  end

  defp receive_deadline(%{options: %{idle_timeout_ms: 0}} = state), do: {state, []}

  defp receive_deadline(state),
    do: set_deadline(state, :receive_idle, state.options.idle_timeout_ms)

  defp heartbeat_deadline(%{phase: :active, peer_idle_timeout_ms: interval} = state)
       when interval > 0,
       do: set_deadline(state, :heartbeat, max(1, div(interval, 2)))

  defp heartbeat_deadline(state), do: {state, []}

  defp heartbeat_deadline_with_effects(state, effects) do
    {state, timers} = heartbeat_deadline(state)
    {state, effects ++ timers}
  end

  defp set_deadline(state, key, delay) do
    {state, canceled} = cancel_deadline(state, key)
    token = state.next_token
    at = state.now_ms + delay

    state = %{
      state
      | next_token: token + 1,
        deadlines: Map.put(state.deadlines, key, {token, at})
    }

    {state, canceled ++ [{:wake_at, state.generation, key, token, at}]}
  end

  defp cancel_deadline(state, key) do
    case Map.pop(state.deadlines, key) do
      {nil, _} ->
        {state, []}

      {{token, _}, remaining} ->
        {%{state | deadlines: remaining}, [{:cancel_wake, state.generation, key, token}]}
    end
  end

  defp cancel_session_deadlines(state, id) do
    Enum.reduce(Map.keys(state.deadlines), {state, []}, fn key, {state, output} ->
      if match?({:session, ^id, _}, key) or key == {:session_close, id} do
        {state, effects} = cancel_deadline(state, key)
        {state, output ++ effects}
      else
        {state, output}
      end
    end)
  end

  defp allocate_channel(state) do
    maximum = min(state.options.channel_max, state.peer_channel_max)

    case Enum.find(0..maximum, &(not Map.has_key?(state.connection.local_channel_to_session, &1))) do
      nil -> {:error, :channel_limit}
      channel -> {:ok, channel}
    end
  end

  defp available_id(sessions, id) do
    id = rem(id, 0x1_0000_0000)
    if Map.has_key?(sessions, id), do: available_id(sessions, id + 1), else: id
  end

  defp operation_error(state, op, reason), do: {state, [{:operation, op, {:error, reason}}]}

  defp protocol_error(%ProtocolError{} = error), do: error

  defp protocol_error(reason),
    do: %ProtocolError{
      condition: tagged(:symbol, condition(reason)),
      description: tagged(:string, inspect(reason, limit: 5, printable_limit: 128))
    }

  defp condition(:idle_timeout), do: "amqp:resource-limit-exceeded"
  defp condition(:session_limit), do: "amqp:resource-limit-exceeded"
  defp condition(:window_violation), do: "amqp:session:window-violation"
  defp condition(:unattached_handle), do: "amqp:session:unattached-handle"
  defp condition(:handle_in_use), do: "amqp:session:handle-in-use"
  defp condition(:errant_link), do: "amqp:session:errant-link"
  defp condition(:resource_limit), do: "amqp:resource-limit-exceeded"
  defp condition(:transfer_limit), do: "amqp:resource-limit-exceeded"
  defp condition(:operation_timeout), do: "amqp:internal-error"
  defp condition(_), do: "amqp:connection:framing-error"
  defp tagged(type, value), do: %Value{type: type, value: value}
  defp optional_string(nil), do: nil
  defp optional_string(value) when is_binary(value), do: tagged(:string, value)
  defp scalar(nil, default), do: default
  defp scalar(%Value{value: value}, _), do: value
  defp in_range?(value, first, last), do: is_integer(value) and value >= first and value <= last
end
