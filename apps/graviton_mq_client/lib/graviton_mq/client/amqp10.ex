defmodule GravitonMQ.Client.AMQP10 do
  @moduledoc false
  use GenServer

  alias GravitonMQ.AMQP10.{Endpoint, Frame}
  alias GravitonMQ.AMQP10.Codec.{Error, Performative, Stream}
  alias GravitonMQ.AMQP10.Codec.Frame, as: FrameCodec
  alias GravitonMQ.Client.AMQP10.Config
  alias GravitonMQ.Client.Transport

  def start_link(options) do
    with {:ok, config} <- Config.build(options),
         {:ok, endpoint} <- Endpoint.new(config.endpoint_options),
         {:ok, stream} <- Stream.new(config.limits) do
      registration = if config.name, do: [name: config.name], else: []
      GenServer.start_link(__MODULE__, {config, endpoint, stream}, registration)
    end
  end

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
       socket: nil,
       input: <<>>,
       drain_pending: false,
       operations: %{},
       link_locks: %{},
       timers: %{},
       observer_monitor: Process.monitor(config.observer)
     }, {:continue, :connect}}
  end

  @impl true
  def handle_continue(event, state), do: protect(fn -> continued(event, state) end)

  defp continued(:connect, state) do
    state =
      case Transport.connect(state.config) do
        {:ok, socket} -> state |> Map.put(:socket, socket) |> step(:start) |> arm_socket()
        {:error, reason} -> step(state, {:transport_down, {:connect, transport_reason(reason)}})
      end

    {:noreply, state}
  end

  @impl true
  def handle_call(request, from, state), do: protect(fn -> call(request, from, state) end)

  defp call(:status, _from, state) do
    {:reply,
     %{
       phase: state.endpoint.phase,
       generation: state.endpoint.generation,
       pending: map_size(state.operations),
       sessions: map_size(state.endpoint.connection.sessions_by_identity),
       writes: map_size(state.endpoint.writes),
       buffered_bytes: byte_size(state.input) + byte_size(state.stream.buffer)
     }, state}
  end

  defp call({:settle_delivery, {owner, session, token}, outcome}, from, state)
       when owner == self(),
       do: call({:command, {:session, session, {:settle, token, outcome}}}, from, state)

  defp call({:settle_delivery, _reference, _outcome}, _from, state),
    do: {:reply, {:error, :invalid_delivery}, state}

  defp call({:command, command}, {caller, _tag}, state) do
    if map_size(state.operations) >= state.config.max_operations do
      {:reply, {:error, :operation_limit}, state}
    else
      reference = make_ref()
      {endpoint, effects, link_locks} = transition(state, {:command, reference, command})

      rejected =
        Enum.find_value(effects, fn
          {:operation, ^reference, {:error, _reason} = result} -> result
          _ -> nil
        end)

      state = %{state | endpoint: endpoint, link_locks: link_locks}

      if rejected do
        {:reply, rejected, execute(state, effects)}
      else
        state = %{state | operations: Map.put(state.operations, reference, caller)}
        {:reply, {:ok, reference}, execute(state, effects)}
      end
    end
  end

  defp call(_request, _from, state), do: {:reply, {:error, :unsupported_command}, state}

  @impl true
  def handle_info(message, state), do: protect(fn -> info(message, state) end)

  defp info({:tcp, socket, bytes}, %{socket: socket, config: %{transport: :tcp}} = state),
    do: received(state, bytes)

  defp info({:ssl, socket, bytes}, %{socket: socket, config: %{transport: :tls}} = state),
    do: received(state, bytes)

  defp info({kind, socket}, %{socket: socket} = state) when kind in [:tcp_closed, :ssl_closed],
    do: {:noreply, step(state, {:transport_down, :closed})}

  defp info({kind, socket, reason}, %{socket: socket} = state)
       when kind in [:tcp_error, :ssl_error],
       do: {:noreply, step(state, {:transport_down, {:transport, transport_reason(reason)}})}

  defp info({:write_result, generation, id, result}, state) do
    if generation == state.endpoint.generation,
      do: {:noreply, step(state, {:write_result, id, result})},
      else: {:noreply, state}
  end

  defp info({:wake, generation, key, token}, state) do
    case Map.get(state.timers, key) do
      {_timer, ^generation, ^token} ->
        state = %{state | timers: Map.delete(state.timers, key)}
        {:noreply, step(state, {:timeout, key, token})}

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

  defp info({:DOWN, monitor, :process, _pid, _reason}, %{observer_monitor: monitor} = state),
    do: {:stop, :normal, state}

  defp info(_message, state), do: {:noreply, state}

  defp received(state, bytes) do
    if byte_size(bytes) + byte_size(state.input) + byte_size(state.stream.buffer) >
         state.config.max_input_bytes do
      error = Error.new(:frame_decode, :limit_exceeded, :input_buffer_limit)
      {:noreply, step(state, {:decode_error, error})}
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

        state |> Map.merge(%{stream: stream, input: rest}) |> step(event) |> drain(remaining - 1)

      {:more, stream} ->
        %{state | stream: stream, input: <<>>}

      {:error, error} ->
        step(%{state | input: <<>>}, {:decode_error, error})
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
    {endpoint, effects, link_locks} = transition(state, event)
    execute(%{state | endpoint: endpoint, link_locks: link_locks}, effects)
  end

  defp execute(state, effects), do: Enum.reduce(effects, state, &effect/2)

  defp effect({:send_header, generation, id, bytes}, state),
    do: write(state, generation, id, {:ok, bytes})

  defp effect({:send_frame, generation, id, channel, pdu, payload}, state),
    do: write(state, generation, id, encode_frame(state, channel, pdu, payload))

  defp effect({:operation, reference, result}, state) do
    case Map.pop(state.operations, reference) do
      {nil, _operations} ->
        state

      {caller, operations} ->
        send(caller, {:graviton_mq, self(), {:operation, reference, result}})
        %{state | operations: operations}
    end
  end

  defp effect({:delivery, session, link, token, raw, metadata}, state) do
    notify(state, {:delivery, {self(), session, token}, session, link, raw, metadata})
    state
  end

  defp effect({:event, event}, state) do
    notify(state, event)
    state
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

  defp effect({:close_transport, generation, _reason}, state) do
    if generation == state.endpoint.generation, do: close_socket(state), else: state
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
      {:error, reason} -> step(state, {:transport_down, {:transport, transport_reason(reason)}})
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
