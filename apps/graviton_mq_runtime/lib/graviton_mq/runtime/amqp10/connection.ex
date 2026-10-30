defmodule GravitonMQ.Runtime.AMQP10.Connection do
  @moduledoc false
  use GenServer, restart: :temporary

  alias GravitonMQ.AMQP10.{Endpoint, Frame}
  alias GravitonMQ.AMQP10.Codec.{Error, Performative, Stream}
  alias GravitonMQ.AMQP10.Codec.Frame, as: FrameCodec
  alias GravitonMQ.AMQP10.Performative.Close
  alias GravitonMQ.Runtime.AMQP10.Writer

  def start_link(configuration) when is_function(configuration, 0),
    do: GenServer.start_link(__MODULE__, configuration)

  def start_link(configuration), do: start_link(fn -> configuration end)

  def child_spec(configuration),
    do: %{
      id: __MODULE__,
      restart: :temporary,
      start: {__MODULE__, :start_link, [fn -> configuration end]}
    }

  def command_handle(owner), do: GenServer.call(owner, :command_handle)

  def command(owner, reference, command) when is_pid(owner),
    do: command(command_handle(owner), reference, command)

  def command(handle, reference, command),
    do: offer(handle, {:command, reference, command}, :command)

  def take_event(table, token), do: take(table, token)

  def capacity(owner, session, identity, timeout \\ 5_000),
    do: GenServer.call(owner, {:capacity, session, identity}, timeout)

  def pump_ack(owner), do: GenServer.cast(owner, :pump_ack)
  def close(owner, reason), do: GenServer.cast(owner, {:close, transport_reason(reason)})

  @impl true
  def init(configuration), do: protect(fn -> initialize(configuration.()) end)

  defp initialize({config, broker, transport}) do
    Process.flag(:trap_exit, true)
    Process.flag(:sensitive, true)

    with {:ok, endpoint} <- Endpoint.new(config.endpoint_options),
         {:ok, stream} <- Stream.new(endpoint.limits) do
      accept_token = make_ref()

      accept_timer =
        Process.send_after(self(), {:accept_timeout, accept_token}, config.connect_timeout)

      commands = new_mailbox()
      events = new_mailbox(broker)

      data = %{
        config: config,
        broker: broker,
        broker_monitor: Process.monitor(broker),
        endpoint: endpoint,
        stream: stream,
        transport: transport,
        socket: nil,
        writer: nil,
        writer_handle: nil,
        input: <<>>,
        drain_pending: false,
        ready_pending: false,
        timers: %{},
        accept_timer: accept_timer,
        accept_token: accept_token,
        commands: %{
          pid: self(),
          table: commands,
          max_entries: endpoint.options.max_pending_operations,
          max_bytes: config.max_input_bytes
        },
        events: %{
          pid: broker,
          table: events,
          max_entries: endpoint.options.max_pending_operations + 64,
          max_bytes: config.max_input_bytes + endpoint.options.max_write_bytes
        },
        event_overflow: false,
        read_timer: nil,
        read_token: nil,
        close_writes: MapSet.new(),
        close_requested: false,
        close_timer: nil,
        close_token: nil
      }

      table = :ets.new(__MODULE__, [:set, :private])
      :ets.insert(table, {:state, data})
      send(broker, {:amqp10_connection_started, self(), data.commands})
      {:ok, %{table: table}}
    else
      {:error, _} -> {:stop, {:shutdown, :invalid_configuration}}
    end
  end

  @impl true
  def handle_call(request, from, shell),
    do: protect(fn -> wrap(call(request, from, data(shell)), shell) end)

  defp call(:command_handle, _from, state), do: {:reply, state.commands, state}

  defp call({:capacity, session, identity}, _from, state),
    do: {:reply, Endpoint.send_capacity(state.endpoint, session, identity), state}

  defp call(_request, _from, state), do: {:reply, {:error, :unsupported_command}, state}

  @impl true
  def handle_cast(message, shell),
    do: protect(fn -> wrap(cast(message, data(shell)), shell) end)

  defp cast(:pump_ack, state), do: {:noreply, %{state | ready_pending: false}}

  defp cast({:close, reason}, state),
    do: continue(step(state, {:transport_down, transport_reason(reason)}))

  defp cast(_message, state), do: {:noreply, state}

  @impl true
  def handle_info(message, shell),
    do: protect(fn -> wrap(info(message, data(shell)), shell) end)

  defp info({:command, token}, state) do
    case take(state.commands.table, token) do
      {:ok, {:command, reference, command}} ->
        continue(step(state, {:command, reference, command}))

      :expired ->
        {:noreply, state}
    end
  end

  defp info({:read, generation, token}, %{read_token: token} = state)
       when not is_nil(token) do
    if generation == state.endpoint.generation do
      state = %{state | read_timer: nil, read_token: nil}

      case recv(state.transport, state.socket) do
        {:ok, bytes} -> received(state, bytes)
        {:error, :timeout} -> continue(schedule_read(state, 5))
        {:error, reason} -> continue(step(state, {:transport_down, transport_reason(reason)}))
      end
    else
      {:noreply, state}
    end
  end

  defp info({:accepted, socket}, %{socket: nil, writer: nil} = state) do
    Process.cancel_timer(state.accept_timer)
    state = %{state | accept_timer: nil, accept_token: nil, socket: socket}

    case handshake(state.transport, socket, state.config.connect_timeout) do
      {:ok, socket} ->
        state = %{state | socket: socket}

        options = [
          owner: self(),
          generation: state.endpoint.generation,
          socket: socket,
          transport: state.transport,
          max_frames: state.endpoint.options.max_write_frames,
          max_bytes: state.endpoint.options.max_write_bytes
        ]

        with :ok <- configure_socket(state),
             {:ok, writer} <- Writer.start_link(options) do
          state = %{state | writer: writer, writer_handle: Writer.handle(writer)}
          state |> step(:start) |> arm_socket() |> continue()
        else
          {:error, reason} ->
            continue(step(state, {:transport_down, {:transport, transport_reason(reason)}}))
        end

      {:error, reason} ->
        continue(step(state, {:transport_down, {:handshake, transport_reason(reason)}}))
    end
  end

  defp info({:accept_timeout, token}, %{accept_token: token} = state) when not is_nil(token),
    do: continue(step(state, {:transport_down, :accept_timeout}))

  defp info({:amqp10_write, generation, id, result}, state) do
    if generation == state.endpoint.generation do
      state = %{state | close_writes: MapSet.delete(state.close_writes, id)}
      state = step(state, {:write_result, id, sanitize_result(result)})

      state =
        if state.close_requested and (result != :ok or MapSet.size(state.close_writes) == 0),
          do: close_socket(state),
          else: state

      continue(state)
    else
      {:noreply, state}
    end
  end

  defp info({:transport_close_timeout, generation, token}, %{close_token: token} = state)
       when not is_nil(token) do
    if generation == state.endpoint.generation,
      do: continue(close_socket(state)),
      else: {:noreply, state}
  end

  defp info({:wake, generation, key, token}, state) do
    case Map.get(state.timers, key) do
      {_timer, ^generation, ^token} ->
        continue(step(%{state | timers: Map.delete(state.timers, key)}, {:timeout, key, token}))

      _ ->
        {:noreply, state}
    end
  end

  defp info({:drain, generation}, %{drain_pending: true} = state) do
    if generation == state.endpoint.generation do
      state = drain(%{state | drain_pending: false}, state.config.max_input_frames_per_turn)
      continue(resume_input(state))
    else
      {:noreply, state}
    end
  end

  defp info({:EXIT, writer, _reason}, %{writer: writer} = state) when not is_nil(writer),
    do: continue(state |> step({:transport_down, :writer_stopped}) |> close_socket())

  defp info({:DOWN, monitor, :process, _pid, _reason}, %{broker_monitor: monitor} = state),
    do: continue(step(state, {:transport_down, :broker_stopped}))

  defp info(_message, state), do: {:noreply, state}

  defp continue(%{event_overflow: true} = state), do: {:stop, :normal, state}

  defp continue(%{endpoint: %{phase: :closed}, close_requested: true, socket: socket} = state)
       when not is_nil(socket),
       do: {:noreply, state}

  defp continue(%{endpoint: %{phase: :closed}} = state), do: {:stop, :normal, state}
  defp continue(state), do: {:noreply, state}

  defp received(state, bytes) do
    if byte_size(bytes) + byte_size(state.input) + byte_size(state.stream.buffer) >
         state.config.max_input_bytes do
      error = Error.new(:frame_decode, :limit_exceeded, :input_buffer_limit)

      continue(
        step(
          %{state | input: <<>>, stream: %{state.stream | buffer: <<>>}},
          {:decode_error, error}
        )
      )
    else
      state =
        drain(%{state | input: state.input <> bytes}, state.config.max_input_frames_per_turn)

      continue(resume_input(state))
    end
  end

  defp drain(%{socket: nil} = state, _remaining), do: state
  defp drain(%{endpoint: %{phase: phase}} = state, _remaining) when phase == :closed, do: state
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

      {:error, %Error{operation: :protocol_header, class: :unsupported, reason: reason}} ->
        step(
          %{state | input: <<>>, stream: %{state.stream | buffer: <<>>}},
          {:transport_down, reason}
        )

      {:error, error} ->
        step(
          %{state | input: <<>>, stream: %{state.stream | buffer: <<>>}},
          {:decode_error, error}
        )
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

  defp step(state, event) do
    {endpoint, effects} =
      Endpoint.step(state.endpoint, %{
        generation: state.endpoint.generation,
        now_ms: now(),
        event: event
      })

    state = Enum.reduce(effects, %{state | endpoint: endpoint}, &effect/2)
    ready(state)
  end

  defp ready(%{ready_pending: true} = state), do: state

  defp ready(state) do
    send(state.broker, {:amqp10_endpoint_ready, self()})
    %{state | ready_pending: true}
  end

  defp effect({:send_header, generation, id, bytes}, state),
    do: write(state, generation, id, {:ok, bytes})

  defp effect({:send_frame, generation, id, channel, pdu, payload}, state) do
    state =
      if is_struct(pdu, Close),
        do: %{state | close_writes: MapSet.put(state.close_writes, id)},
        else: state

    write(state, generation, id, encode_frame(state, channel, pdu, payload))
  end

  defp effect({:operation, _, _} = event, state), do: forward(state, event)
  defp effect({:event, _} = event, state), do: forward(state, event)
  defp effect({:delivery, _, _, _, _, _} = event, state), do: forward(state, event)

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
    cond do
      generation != state.endpoint.generation ->
        state

      MapSet.size(state.close_writes) == 0 ->
        close_socket(state)

      state.close_requested ->
        state

      true ->
        # A peer Close can close the pure endpoint in the same transition that
        # emits our reply. Let the asynchronous Writer accept that reply before
        # closing its socket; this says nothing about delivery acceptance.
        token = make_ref()

        timer =
          Process.send_after(
            self(),
            {:transport_close_timeout, generation, token},
            state.config.send_timeout
          )

        %{state | close_requested: true, close_timer: timer, close_token: token}
    end
  end

  defp forward(state, event) do
    case offer(state.events, event, :event) do
      :ok ->
        state

      {:error, _} ->
        if not state.event_overflow, do: send(state.broker, {:amqp10_endpoint_overflow, self()})
        %{state | event_overflow: true}
    end
  end

  defp write(state, generation, id, encoded) do
    result =
      case encoded do
        {:ok, bytes} -> Writer.enqueue(state.writer_handle, id, bytes)
        {:error, error} -> {:error, {:encode, error.class, error.reason}}
      end

    if result != :ok, do: send(self(), {:amqp10_write, generation, id, result})
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

  defp handshake(:tcp, socket, _timeout), do: {:ok, socket}
  defp handshake(:tls, socket, timeout), do: :ssl.handshake(socket, timeout)

  defp configure_socket(state) do
    options = [
      :binary,
      packet: :raw,
      active: false,
      send_timeout: state.config.send_timeout,
      send_timeout_close: true
    ]

    setopts(state.transport, state.socket, options)
  end

  defp setopts(:tcp, socket, options), do: :inet.setopts(socket, options)
  defp setopts(:tls, socket, options), do: :ssl.setopts(socket, options)
  defp recv(:tcp, socket), do: :gen_tcp.recv(socket, 0, 0)
  defp recv(:tls, socket), do: :ssl.recv(socket, 0, 0)
  defp arm_socket(state), do: schedule_read(state, 0)
  defp schedule_read(%{socket: nil} = state, _delay), do: state
  defp schedule_read(%{read_timer: timer} = state, _delay) when not is_nil(timer), do: state

  defp schedule_read(state, delay) do
    token = make_ref()
    timer = Process.send_after(self(), {:read, state.endpoint.generation, token}, delay)
    %{state | read_timer: timer, read_token: token}
  end

  defp close_socket(state) do
    if state.read_timer, do: Process.cancel_timer(state.read_timer)
    if state.close_timer, do: Process.cancel_timer(state.close_timer)
    state = %{state | read_timer: nil, read_token: nil, close_timer: nil, close_token: nil}

    if state.socket do
      case state.transport do
        :tcp -> :gen_tcp.close(state.socket)
        :tls -> :ssl.close(state.socket)
      end
    end

    if state.writer && Process.alive?(state.writer), do: Process.exit(state.writer, :shutdown)

    Enum.reduce(
      Map.keys(state.timers),
      %{
        state
        | socket: nil,
          input: <<>>,
          stream: %{state.stream | buffer: <<>>},
          drain_pending: false
      },
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

  @impl true
  def terminate(_reason, shell) do
    protect(fn ->
      state = data(shell)
      if state.accept_timer, do: Process.cancel_timer(state.accept_timer)

      try do
        state |> step({:transport_down, :owner_stopped}) |> close_socket()
      after
        if Process.alive?(state.broker),
          do: :ets.give_away(state.events.table, state.broker, {:amqp10_events, self()})
      end

      :ok
    end)
  end

  defp data(shell), do: :ets.lookup_element(shell.table, :state, 2)

  defp wrap({:noreply, state}, shell) do
    :ets.insert(shell.table, {:state, state})
    {:noreply, shell}
  end

  defp wrap({:reply, result, state}, shell) do
    :ets.insert(shell.table, {:state, state})
    {:reply, result, shell}
  end

  defp wrap({:stop, reason, state}, shell) do
    :ets.insert(shell.table, {:state, state})
    {:stop, reason, shell}
  end

  defp new_mailbox(heir \\ nil) do
    options = [:set, :public, write_concurrency: true]
    options = if heir, do: [{:heir, heir, {:amqp10_events, self()}} | options], else: options
    table = :ets.new(__MODULE__, options)
    :ets.insert(table, {:credits, 0, 0})
    table
  end

  defp offer(handle, value, kind) do
    size = :erlang.external_size(value)

    try do
      [count, bytes] = :ets.update_counter(handle.table, :credits, [{2, 1}, {3, size}])

      if count <= handle.max_entries and bytes <= handle.max_bytes do
        token = make_ref()
        # Copy retained sub-binaries without retaining an arbitrary source allocation.
        value = value |> :erlang.term_to_binary() |> :erlang.binary_to_term()
        :ets.insert(handle.table, {token, size, value})

        message =
          case kind do
            :command -> {:command, token}
            :event -> {:amqp10_endpoint_event, self(), handle.table, token}
          end

        send(handle.pid, message)
        :ok
      else
        :ets.update_counter(handle.table, :credits, [{2, -1}, {3, -size}])
        {:error, :mailbox_full}
      end
    rescue
      ArgumentError -> {:error, :closed}
    end
  end

  defp take(table, token) do
    try do
      case :ets.take(table, token) do
        [{^token, size, value}] ->
          :ets.update_counter(table, :credits, [{2, -1}, {3, -size}])
          {:ok, value}

        [] ->
          :expired
      end
    rescue
      ArgumentError -> :expired
    end
  end

  @impl true
  def format_status(status),
    do: Map.merge(status, %{state: :redacted, message: :redacted, reason: :redacted, log: []})

  defp sanitize_result(:ok), do: :ok
  defp sanitize_result({:error, reason}), do: {:error, transport_reason(reason)}
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
          {:amqp_connection_callback_failed, exception.__struct__},
          private_stacktrace(__STACKTRACE__)
        )
    catch
      kind, _reason ->
        :erlang.raise(
          kind,
          {:amqp_connection_callback_failed, kind},
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
