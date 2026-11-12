defmodule GravitonMQ.Client.AMQP10 do
  @moduledoc false
  use GenServer

  @restore_window 8

  alias GravitonMQ.AMQP10.{Endpoint, Frame}
  alias GravitonMQ.AMQP10.Codec.{Error, Performative, Stream}
  alias GravitonMQ.AMQP10.Codec.Frame, as: FrameCodec
  alias GravitonMQ.AMQP10.SASL.{Machine, Plain}
  alias GravitonMQ.Client.AMQP10.{Config, Intent, Reconnect}
  alias GravitonMQ.Client.Transport

  def start_link(options) do
    with {:ok, config} <- Config.build(options),
         {:ok, endpoint} <- Endpoint.new(config.endpoint_options),
         {:ok, stream} <- Stream.new(config.limits, accept: accept(config)) do
      registration = if config.name, do: [name: config.name], else: []
      GenServer.start_link(__MODULE__, {config, endpoint, stream}, registration)
    end
  end

  defp accept(%{sasl: :none}), do: [0]
  defp accept(_config), do: [0, 3]

  @impl true
  def init(argument), do: protect(fn -> initialize(argument) end)

  defp initialize({config, endpoint, stream}) do
    Process.flag(:trap_exit, true)
    Process.flag(:sensitive, true)

    {:ok,
     %{
       config: config,
       endpoint: endpoint,
       stream: stream,
       sasl: nil,
       socket: nil,
       input: <<>>,
       drain_pending: false,
       operations: %{},
       link_locks: %{},
       timers: %{},
       owner: :connecting,
       attempt: 0,
       retry: nil,
       close_cause: nil,
       host_closes: [],
       lost: nil,
       intent: Intent.new(),
       restore_pending: false,
       observer_monitor: Process.monitor(config.observer)
     }, {:continue, :connect}}
  end

  @impl true
  def handle_continue(event, state), do: protect(fn -> continued(event, state) end)

  defp continued(:connect, state), do: {:noreply, connect(state)}

  defp connect(state) do
    case Transport.connect(state.config) do
      {:ok, socket} -> state |> Map.put(:socket, socket) |> begin() |> arm_socket()
      {:error, reason} -> step(state, {:transport_down, {:connect, transport_reason(reason)}})
    end
  end

  @impl true
  def handle_call(request, from, state), do: protect(fn -> call(request, from, state) end)

  defp call(:status, _from, state) do
    {:reply,
     %{
       phase: state.endpoint.phase,
       owner: owner(state),
       generation: state.endpoint.generation,
       attempt: state.attempt,
       next_retry_ms: next_retry_ms(state),
       pending: map_size(state.operations),
       sessions: map_size(state.endpoint.connection.sessions_by_identity),
       writes: map_size(state.endpoint.writes),
       buffered_bytes: byte_size(state.input) + byte_size(state.stream.buffer),
       intent: Intent.counts(state.intent),
       restoring: restoring(state)
     }, state}
  end

  defp call({:settle_delivery, {owner, session, token}, outcome}, from, state)
       when owner == self() do
    if stale_delivery?(state, token),
      do: {:reply, {:error, :stale_delivery}, state},
      else: call({:command, {:session, session, {:settle, token, outcome}}}, from, state)
  end

  defp call({:settle_delivery, _reference, _outcome}, _from, state),
    do: {:reply, {:error, :invalid_delivery}, state}

  defp call({:command, command}, {caller, _tag}, state) do
    cond do
      offline?(state) ->
        offline_command(state, command, caller)

      # A Close already in flight (idle/open timeout, protocol error) becomes
      # final: the owner does not reconnect, and the host learns `:closed`
      # once the transport is released (after_loss/1).
      command == :close and state.endpoint.phase == :closing and
          length(state.host_closes) >= state.config.max_operations ->
        {:reply, {:error, :operation_limit}, state}

      command == :close and state.endpoint.phase == :closing ->
        reference = make_ref()

        {:reply, {:ok, reference},
         %{
           state
           | close_cause: :host_close,
             host_closes: [{caller, reference} | state.host_closes]
         }}

      restoring_target?(state, command) ->
        {:reply, {:error, :restoring}, state}

      true ->
        case admit(state, command, %{caller: caller, declare: declaration(command)}) do
          {:ok, reference, state} -> {:reply, {:ok, reference}, retract(state, command)}
          {:error, reason, state} -> {:reply, {:error, reason}, state}
        end
    end
  end

  defp call(_request, _from, state), do: {:reply, {:error, :unsupported_command}, state}

  # Between connections nothing is queued and nothing claims success (D3).
  defp offline?(%{owner: :offline}), do: true
  defp offline?(%{owner: :connecting, endpoint: %{generation: generation}}), do: generation > 0
  defp offline?(_state), do: false

  defp offline_command(state, :close, caller) do
    reference = make_ref()
    state = %{cancel_retry(state) | close_cause: :host_close}

    state =
      if state.owner == :connecting,
        do: route(state, {:transport_down, :owner_stopped}),
        else: state

    send(caller, {:graviton_mq, self(), {:operation, reference, {:ok, :closed}}})
    {:reply, {:ok, reference}, %{state | owner: :stopped}}
  end

  defp offline_command(state, _command, _caller), do: {:reply, {:error, :disconnected}, state}

  defp restoring(%{intent: %{active: true} = intent}), do: Intent.restoring(intent)
  defp restoring(_state), do: 0

  defp owner(%{owner: :active} = state),
    do: if(restoring(state) > 0, do: :restoring, else: :active)

  defp owner(state), do: state.owner

  # A command on a Session or Link still waiting for restoration (spec §3.4).
  defp restoring_target?(%{intent: %{active: true} = intent}, command) do
    case target(command) do
      {sid, identity} -> Intent.pending?(intent, sid, identity)
      nil -> false
    end
  end

  defp restoring_target?(_state, _command), do: false

  defp target({:open_session, sid, _options}), do: {sid, nil}
  defp target({:end_session, sid}), do: {sid, nil}
  defp target({:session, sid, command}), do: {sid, link_identity(command)}
  defp target(_command), do: nil

  defp link_identity({:attach, identity, _options}), do: identity
  defp link_identity({:send, identity, _raw, _options}), do: identity
  defp link_identity({:demand, identity, _slots, _bytes}), do: identity
  defp link_identity({:detach, identity}), do: identity
  defp link_identity(_command), do: nil

  # Only a successful completion declares (spec §3.5); an admitted detach or
  # End retracts at once, so it is never restored.
  defp declaration({:open_session, sid, options}), do: {:session, sid, options}

  defp declaration({:session, sid, {:attach, identity, options}}),
    do: {:link, sid, identity, options}

  defp declaration(_command), do: nil

  defp retract(state, {:session, sid, {:detach, identity}}),
    do: %{state | intent: Intent.drop_link(state.intent, sid, identity)}

  defp retract(state, {:end_session, sid}),
    do: %{state | intent: Intent.drop_session(state.intent, sid)}

  defp retract(state, _command), do: state

  # Shared by host commands and restoration. `owner` is stored with the
  # operation: `%{caller: pid, declare: declaration | nil}` for the host,
  # `%{caller: :restore, item: key}` for restoration.
  defp admit(state, command, owner) do
    if map_size(state.operations) >= state.config.max_operations do
      {:error, :operation_limit, state}
    else
      reference = make_ref()
      before = state.endpoint.phase
      event = {:command, reference, command}
      {endpoint, effects, link_locks} = transition(state, event)

      rejected =
        Enum.find_value(effects, fn
          {:operation, ^reference, {:error, reason}} -> reason
          _ -> nil
        end)

      state = track_close(%{state | endpoint: endpoint, link_locks: link_locks}, before, event)

      if rejected do
        {:error, rejected, execute(state, effects)}
      else
        state = %{state | operations: Map.put(state.operations, reference, owner)}
        {:ok, reference, execute(state, effects)}
      end
    end
  end

  # An incoming delivery token is {generation, sid, incarnation, n, id}.
  defp stale_delivery?(state, token) when is_tuple(token) and tuple_size(token) == 5,
    do: is_integer(elem(token, 0)) and elem(token, 0) != state.endpoint.generation

  defp stale_delivery?(_state, _token), do: false

  defp next_retry_ms(%{retry: {_timer, _token, at}}), do: max(0, at - now())
  defp next_retry_ms(_state), do: nil

  @impl true
  def handle_info(message, state), do: protect(fn -> info(message, state) end)

  defp info({:tcp, socket, bytes}, %{socket: socket, config: %{transport: :tcp}} = state),
    do: received(state, bytes)

  defp info({:ssl, socket, bytes}, %{socket: socket, config: %{transport: :tls}} = state),
    do: received(state, bytes)

  defp info({kind, socket}, %{socket: socket} = state) when kind in [:tcp_closed, :ssl_closed],
    do: {:noreply, route(state, {:transport_down, :closed})}

  defp info({kind, socket, reason}, %{socket: socket} = state)
       when kind in [:tcp_error, :ssl_error],
       do: {:noreply, route(state, {:transport_down, {:transport, transport_reason(reason)}})}

  defp info({:write_result, generation, id, result}, state) do
    cond do
      generation != state.endpoint.generation -> {:noreply, state}
      match?({:sasl, _}, id) -> {:noreply, sasl_step(state, {:write_result, id, result})}
      true -> {:noreply, step(state, {:write_result, id, result})}
    end
  end

  defp info({:wake, generation, key, token}, state) do
    case Map.get(state.timers, key) do
      {_timer, ^generation, ^token} ->
        state = %{state | timers: Map.delete(state.timers, key)}
        event = {:timeout, key, token}

        if key == Machine.deadline_key(),
          do: {:noreply, sasl_step(state, event)},
          else: {:noreply, step(state, event)}

      _ ->
        {:noreply, state}
    end
  end

  defp info({:drain, generation}, %{drain_pending: true} = state) do
    if generation == state.endpoint.generation do
      state = drain(%{state | drain_pending: false}, state.config.max_input_frames_per_turn)
      {:noreply, resume_input(state)}
    else
      {:noreply, state}
    end
  end

  defp info({:retry, token}, %{retry: {_timer, token, _at}} = state) do
    generation = state.endpoint.generation + 1
    options = Keyword.put(state.config.endpoint_options, :generation, generation)
    # Both were validated by the identical first construction in start_link/1.
    {:ok, endpoint} = Endpoint.new(options)
    {:ok, stream} = Stream.new(state.config.limits, accept: accept(state.config))

    state = %{
      state
      | retry: nil,
        owner: :connecting,
        endpoint: endpoint,
        stream: stream,
        sasl: nil,
        close_cause: nil,
        lost: nil,
        input: <<>>,
        drain_pending: false
    }

    {:noreply, connect(state)}
  end

  # Fenced by generation: a stale wake from a lost connection is ignored and
  # leaves the current connection's pending wake in place.
  defp info(
         {:restore, generation},
         %{restore_pending: true, endpoint: %{generation: generation}} = state
       ) do
    state = %{state | restore_pending: false}

    if state.endpoint.phase == :active,
      do: {:noreply, restore_next(state)},
      else: {:noreply, state}
  end

  defp info({:DOWN, monitor, :process, _pid, _reason}, %{observer_monitor: monitor} = state),
    do: {:stop, :normal, state}

  defp info(_message, state), do: {:noreply, state}

  defp received(state, bytes) do
    if byte_size(bytes) + byte_size(state.input) + byte_size(state.stream.buffer) >
         state.config.max_input_bytes do
      error = Error.new(:frame_decode, :limit_exceeded, :input_buffer_limit)
      {:noreply, route(state, {:decode_error, error})}
    else
      state =
        drain(%{state | input: state.input <> bytes}, state.config.max_input_frames_per_turn)

      {:noreply, resume_input(state)}
    end
  end

  defp drain(%{socket: nil} = state, _remaining), do: state
  defp drain(%{endpoint: %{phase: :closed}} = state, _remaining), do: state
  defp drain(state, 0), do: state

  defp drain(state, remaining) do
    case Stream.next(state.stream, state.input) do
      {:ok, item, stream, rest} ->
        event =
          case item do
            {:protocol_header, header} -> {:header, header}
            {:frame, frame} -> {:frame, frame}
          end

        state |> Map.merge(%{stream: stream, input: rest}) |> route(event) |> drain(remaining - 1)

      {:more, stream} ->
        %{state | stream: stream, input: <<>>}

      {:error, error} ->
        route(%{state | input: <<>>}, {:decode_error, error})
    end
  end

  defp resume_input(%{socket: nil} = state), do: state
  defp resume_input(%{endpoint: %{phase: :closed}} = state), do: state
  defp resume_input(%{input: <<>>} = state), do: arm_socket(state)
  defp resume_input(%{drain_pending: true} = state), do: state

  defp resume_input(state) do
    send(self(), {:drain, state.endpoint.generation})
    %{state | drain_pending: true}
  end

  defp transition(state, event) do
    facts = %{generation: state.endpoint.generation, now_ms: now(), event: event}
    {endpoint, effects} = Endpoint.step(state.endpoint, facts)

    case reserve_link_names(endpoint, state.link_locks) do
      {:ok, locks} ->
        {endpoint, effects, retire_link_names(endpoint, locks)}

      {:conflict, identity} ->
        case event do
          {:command, reference, _command} ->
            {state.endpoint, [{:operation, reference, {:error, :attach_not_admitted}}],
             state.link_locks}

          _ ->
            # Re-run the pure transition before any effects escape. The endpoint
            # refuses this attachment with a null terminus and resource-locked.
            {endpoint, effects} =
              Endpoint.step(
                state.endpoint,
                Map.put(facts, :link_name_conflicts, MapSet.new([identity]))
              )

            {endpoint, effects, retire_link_names(endpoint, state.link_locks)}
        end
    end
  end

  defp reserve_link_names(endpoint, locks) do
    endpoint.connection.sessions_by_identity
    |> Enum.flat_map(fn {sid, session} ->
      Enum.flat_map(session.links_by_identity, fn
        {_identity, %{phase: :refused}} -> []
        {identity, _link} -> [{{sid, session.incarnation, identity}, identity}]
      end)
    end)
    |> Enum.reduce_while({:ok, locks}, fn {key, identity}, {:ok, reserved} ->
      resource =
        {__MODULE__, :link_name, endpoint.connection.local_container_id,
         endpoint.connection.remote_container_id, identity}

      cond do
        Map.has_key?(reserved, key) ->
          {:cont, {:ok, reserved}}

        resource in Map.values(reserved) or
            not :global.set_lock({resource, self()}, [node()], 0) ->
          Enum.each(reserved, fn {new_key, resource} ->
            unless Map.has_key?(locks, new_key),
              do: :global.del_lock({resource, self()}, [node()])
          end)

          {:halt, {:conflict, identity}}

        true ->
          {:cont, {:ok, Map.put(reserved, key, resource)}}
      end
    end)
  end

  defp retire_link_names(endpoint, locks) do
    Map.filter(locks, fn {{sid, incarnation, identity}, resource} ->
      retained =
        endpoint.phase != :closed and
          case Map.get(endpoint.connection.sessions_by_identity, sid) do
            %{incarnation: ^incarnation} = session ->
              endpoint.phase == :closing or
                match?(%{phase: :ending}, Map.get(endpoint.session_controls, sid)) or
                case Map.get(session.links_by_identity, identity) do
                  nil -> false
                  %{phase: :refused} -> false
                  _ -> true
                end

            _ ->
              false
          end

      unless retained, do: :global.del_lock({resource, self()}, [node()])
      retained
    end)
  end

  defp step(state, event) do
    before = state.endpoint.phase
    {endpoint, effects, link_locks} = transition(state, event)
    state = track_close(%{state | endpoint: endpoint, link_locks: link_locks}, before, event)
    execute(state, effects)
  end

  # D1: remember why this Endpoint began a local Close.
  defp track_close(state, before, event) do
    if before not in [:closing, :closed] and state.endpoint.phase == :closing,
      do: %{state | close_cause: close_cause(event)},
      else: state
  end

  defp close_cause({:command, _reference, :close}), do: :host_close
  defp close_cause({:timeout, :receive_idle, _token}), do: :idle_timeout
  defp close_cause({:timeout, :open, _token}), do: :open_timeout
  defp close_cause(_event), do: :protocol

  defp begin(%{config: %{sasl: :none}} = state), do: step(state, :start)

  defp begin(state) do
    with {:ok, credentials} <- credentials(state.config.sasl),
         {:ok, machine} <-
           Machine.new(
             role: :client,
             credentials: credentials,
             hostname: Keyword.get(state.config.endpoint_options, :hostname),
             timeout_ms: state.config.sasl_timeout,
             generation: state.endpoint.generation
           ) do
      sasl_step(%{state | sasl: machine}, :start)
    else
      _invalid -> step(state, {:transport_down, :invalid_sasl_credentials})
    end
  end

  # A password provider is evaluated here, once per connect; its result lives
  # only in the machine, which drops it once sasl-init is emitted.
  defp credentials({:anonymous, nil}), do: {:ok, :anonymous}
  defp credentials({:anonymous, trace}), do: {:ok, {:anonymous, trace}}

  defp credentials({:plain, username, password, authzid}) do
    password = if is_function(password, 0), do: password.(), else: password
    Plain.new(username, password, authzid)
  catch
    _kind, _reason -> {:error, :invalid_plain_message}
  end

  # Input belongs to the SASL machine until it starts the Endpoint or closes.
  defp route(state, event) do
    if state.sasl != nil and Machine.active?(state.sasl),
      do: sasl_step(state, event),
      else: step(state, event)
  end

  defp sasl_step(state, event) do
    facts = %{generation: state.endpoint.generation, now_ms: now(), event: event}
    {machine, effects} = Machine.step(state.sasl, facts)

    # Once a nested step has replaced the machine, or an effect halted the
    # negotiation, this step's remaining effects are stale.
    Enum.reduce_while(effects, %{state | sasl: machine}, fn effect, state ->
      case sasl_effect(effect, state) do
        {:halt, state} -> {:halt, state}
        state when state.sasl == machine -> {:cont, state}
        state -> {:halt, state}
      end
    end)
  end

  defp sasl_effect({:send_bytes, generation, id, bytes}, state),
    do: write(state, generation, id, {:ok, bytes})

  defp sasl_effect({:wake_at, _, _, _, _} = wake, state), do: effect(wake, state)
  defp sasl_effect({:cancel_wake, _, _, _} = cancel, state), do: effect(cancel, state)

  # The machine emits this with its successful outcome, already `:done`, so a
  # failed switch must not be routed (it would reach the unstarted Endpoint)
  # and must stop the following `{:start_endpoint, nil}`. The stream takes only
  # the octets a frame needs, so the switch cannot fail from socket input.
  defp sasl_effect({:expect_header, accept}, state) do
    case Stream.expect_header(state.stream, accept) do
      {:ok, stream} -> %{state | stream: stream}
      {:error, error} -> {:halt, sasl_effect({:close, {:decode_error, error.reason}}, state)}
    end
  end

  defp sasl_effect({:start_endpoint, nil}, state), do: step(state, :start)
  defp sasl_effect({:close, reason}, state), do: step(state, {:transport_down, reason})
  defp sasl_effect(_informational, state), do: state

  defp execute(state, effects), do: Enum.reduce(effects, state, &effect/2)

  defp effect({:send_header, generation, id, bytes}, state),
    do: write(state, generation, id, {:ok, bytes})

  defp effect({:send_frame, generation, id, channel, pdu, payload}, state),
    do: write(state, generation, id, encode_frame(state, channel, pdu, payload))

  defp effect({:operation, reference, result}, state) do
    case Map.pop(state.operations, reference) do
      {nil, _operations} ->
        state

      {%{caller: :restore, item: key}, operations} ->
        state = %{state | operations: operations}

        # A loss failed it: not reported, the entry stays declared (D13).
        if state.endpoint.phase == :closed,
          do: %{state | intent: Intent.interrupt(state.intent)},
          else: restored(state, key, result)

      {%{caller: caller} = owner, operations} ->
        send(caller, {:graviton_mq, self(), {:operation, reference, result}})

        %{state | operations: operations}
        |> declare(Map.get(owner, :declare), result)
        |> schedule_restore()
    end
  end

  defp effect({:delivery, session, link, token, raw, metadata}, state) do
    notify(state, {:delivery, {self(), session, token}, session, link, raw, metadata})
    state
  end

  defp effect({:event, {:connected, _container} = event}, state) do
    notify(state, event)
    start_restore(%{state | owner: :active, attempt: 0})
  end

  defp effect({:event, {:disconnected, reason} = event}, state) do
    notify(state, event)
    %{state | lost: reason, intent: Intent.stop(state.intent), restore_pending: false}
  end

  defp effect({:event, event}, state) do
    notify(state, event)
    observe(state, event)
  end

  defp effect({:wake_at, generation, key, token, at}, state) do
    state = cancel_timer(state, key)
    timer = Process.send_after(self(), {:wake, generation, key, token}, max(0, at - now()))
    %{state | timers: Map.put(state.timers, key, {timer, generation, token})}
  end

  defp effect({:cancel_wake, generation, key, token}, state) do
    case Map.get(state.timers, key) do
      {_timer, ^generation, ^token} -> cancel_timer(state, key)
      _ -> state
    end
  end

  # Release the transport first, then decide: a stopped owner holds no socket
  # or timer (MQTT parity, spec §3.7).
  defp effect({:close_transport, generation, _reason}, state) do
    if generation == state.endpoint.generation,
      do: state |> close_socket() |> after_loss(),
      else: state
  end

  defp write(state, generation, id, encoded) do
    result =
      case encoded do
        {:ok, bytes} -> Transport.send(state.config.transport, state.socket, bytes)
        {:error, error} -> {:error, {:encode, error.class, error.reason}}
      end

    result =
      case result do
        :ok -> :ok
        {:error, reason} -> {:error, transport_reason(reason)}
      end

    # Feed receipts through the mailbox. Pure flushes cannot recursively emit
    # an unbounded number of fragments inside a single callback.
    send(self(), {:write_result, generation, id, result})
    state
  end

  defp encode_frame(state, channel, pdu, payload) do
    limits = Endpoint.outgoing_limits(state.endpoint)

    with {:ok, encoded} <- encode_performative(pdu, limits),
         size = 8 + byte_size(encoded) + byte_size(payload),
         :ok <- frame_size(size, limits.max_frame_size) do
      FrameCodec.encode(
        %Frame{
          declared_size: size,
          data_offset_words: 2,
          type: :amqp,
          channel: channel,
          extended_header: <<>>,
          body: encoded <> payload
        },
        limits
      )
    end
  end

  defp encode_performative(nil, _limits), do: {:ok, <<>>}
  defp encode_performative(pdu, limits), do: Performative.encode(pdu, limits)
  defp frame_size(size, maximum) when size <= maximum, do: :ok

  defp frame_size(_size, _maximum),
    do: {:error, Error.new(:frame_encode, :limit_exceeded, :frame_size_limit)}

  defp arm_socket(%{socket: nil} = state), do: state

  defp arm_socket(state) do
    case Transport.active_once(state.config.transport, state.socket) do
      :ok -> state
      {:error, reason} -> route(state, {:transport_down, {:transport, transport_reason(reason)}})
    end
  end

  defp close_socket(state) do
    Transport.close(state.config.transport, state.socket)

    Enum.reduce(
      Map.keys(state.timers),
      %{state | socket: nil, input: <<>>, drain_pending: false},
      &cancel_timer(&2, &1)
    )
  end

  defp after_loss(%{lost: nil} = state), do: state

  defp after_loss(state) do
    reason = state.lost
    policy = state.config.reconnect

    for {caller, reference} <- Enum.reverse(state.host_closes),
        do: send(caller, {:graviton_mq, self(), {:operation, reference, {:ok, :closed}}})

    state = %{state | lost: nil, host_closes: []}

    cond do
      Reconnect.classify(reason, state.close_cause) == :stop ->
        %{state | owner: :stopped}

      state.attempt >= policy.max_attempts ->
        if policy.max_attempts > 0, do: notify(state, {:reconnect_exhausted, state.attempt})
        %{state | owner: :stopped}

      true ->
        attempt = state.attempt + 1
        notify(state, {:reconnecting, attempt})

        schedule_retry(
          %{state | owner: :offline, attempt: attempt},
          Reconnect.delay(policy, attempt)
        )
    end
  end

  # The first connection has nothing to restore and reports nothing (D13).
  defp start_restore(%{endpoint: %{generation: 0}} = state), do: state

  defp start_restore(state),
    do: %{state | intent: Intent.start(state.intent)} |> schedule_restore() |> finish_restore()

  defp schedule_restore(%{restore_pending: false} = state) do
    if Intent.waiting?(state.intent) do
      send(self(), {:restore, state.endpoint.generation})
      %{state | restore_pending: true}
    else
      state
    end
  end

  defp schedule_restore(state), do: state

  # At most min(8, free operations) restoration operations are outstanding,
  # so host commands are never starved (spec §3.5).
  defp restore_next(state) do
    free = state.config.max_operations - map_size(state.operations)
    window = min(@restore_window - state.intent.outstanding, free)
    {commands, intent} = Intent.take(state.intent, window)

    Enum.reduce(commands, %{state | intent: intent}, fn {key, command}, state ->
      case admit(state, command, %{caller: :restore, item: key}) do
        {:ok, _reference, state} -> state
        {:error, reason, state} -> restored(state, key, {:error, reason})
      end
    end)
  end

  defp restored(state, key, result) do
    {intent, reports} = Intent.complete(state.intent, key, result)
    Enum.each(reports, fn {key, result} -> notify(state, {:restored, key, result}) end)
    %{state | intent: intent} |> schedule_restore() |> finish_restore()
  end

  defp finish_restore(state) do
    if Intent.finished?(state.intent) do
      {counts, intent} = Intent.finish(state.intent)
      notify(state, {:restore_complete, counts})
      %{state | intent: intent}
    else
      state
    end
  end

  defp declare(state, {:session, sid, options}, {:ok, _}),
    do: %{state | intent: Intent.put_session(state.intent, sid, options)}

  defp declare(state, {:link, sid, identity, options}, {:ok, _}),
    do: %{state | intent: Intent.put_link(state.intent, sid, identity, options)}

  defp declare(state, _declaration, _result), do: state

  # The peer ended or detached it: restoring it would resurrect it (D13).
  defp observe(state, {:session, sid, {kind, identity, _reason}})
       when kind in [:detached, :refused],
       do: %{state | intent: Intent.drop_link(state.intent, sid, identity)}

  defp observe(state, {:session_ended, sid, _error}),
    do: %{state | intent: Intent.drop_session(state.intent, sid)}

  defp observe(state, {:session_error, sid, _reason}),
    do: %{state | intent: Intent.drop_session(state.intent, sid)}

  defp observe(state, _event), do: state

  defp schedule_retry(state, delay) do
    token = make_ref()
    timer = Process.send_after(self(), {:retry, token}, delay)
    %{state | retry: {timer, token, now() + delay}}
  end

  defp cancel_retry(%{retry: {timer, _token, _at}} = state) do
    Process.cancel_timer(timer)
    %{state | retry: nil}
  end

  defp cancel_retry(state), do: state

  defp cancel_timer(state, key) do
    case Map.pop(state.timers, key) do
      {nil, _timers} ->
        state

      {{timer, _, _}, timers} ->
        Process.cancel_timer(timer)
        %{state | timers: timers}
    end
  end

  defp notify(state, event), do: send(state.config.observer, {:graviton_mq, self(), event})

  @impl true
  def terminate(_reason, state) do
    protect(fn ->
      state = cancel_retry(state)

      state =
        if state.endpoint.phase == :active,
          do: step(state, {:command, make_ref(), :close}),
          else: state

      state |> step({:transport_down, :owner_stopped}) |> close_socket()
      :ok
    end)
  end

  @impl true
  def format_status(status),
    do: Map.merge(status, %{state: :redacted, message: :redacted, reason: :redacted, log: []})

  defp transport_reason(reason) when is_atom(reason), do: reason
  defp transport_reason({:tls_alert, {alert, _detail}}), do: {:tls_alert, alert}
  defp transport_reason({:encode, class, reason}), do: {:encode, class, reason}
  defp transport_reason(_reason), do: :transport_error

  defp protect(callback) do
    try do
      callback.()
    rescue
      exception ->
        :erlang.raise(
          :error,
          {:amqp_client_callback_failed, exception.__struct__},
          private_stacktrace(__STACKTRACE__)
        )
    catch
      kind, _reason ->
        :erlang.raise(
          kind,
          {:amqp_client_callback_failed, kind},
          private_stacktrace(__STACKTRACE__)
        )
    end
  end

  defp private_stacktrace(stacktrace) do
    Enum.map(stacktrace, fn
      {module, function, arguments, location} when is_list(arguments) ->
        {module, function, length(arguments), location}

      frame ->
        frame
    end)
  end

  defp now, do: System.monotonic_time(:millisecond)
end
