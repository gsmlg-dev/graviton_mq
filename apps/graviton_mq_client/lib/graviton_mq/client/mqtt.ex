defmodule GravitonMQ.Client.MQTT do
  @moduledoc false
  use GenServer

  alias GravitonMQ.Client.MQTT.{Config, Store, Checkpoint}
  alias GravitonMQ.Client.Transport
  alias GravitonMQ.MQTT.Client, as: Machine
  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Disconnect, Publish, Subscribe, Unsubscribe}

  def start_link(options) do
    with {:ok, config} <- Config.build(options),
         {:ok, machine} <- Machine.new(config.connect, config.machine_options) do
      registration = if config.name, do: [name: config.name], else: []
      GenServer.start_link(__MODULE__, {config, machine}, registration)
    end
  end

  @impl true
  def init(argument), do: protect(fn -> initialize(argument) end)

  defp initialize({config, machine}) do
    Process.flag(:trap_exit, true)

    state = %{
      config: config,
      machine: machine,
      socket: nil,
      buffer: <<>>,
      observer_monitor: Process.monitor(config.observer),
      operations: %{},
      store: nil,
      next_operation: 1,
      results: %{},
      poisoned: nil,
      tick: nil,
      handshake: nil,
      retry: nil
    }

    case recover(state) do
      {:ok, state} -> {:ok, state, {:continue, :start}}
      {:error, reason} -> {:stop, {:persistence, reason}}
    end
  end

  @impl true
  def handle_continue(event, state), do: protect(fn -> continued(event, state) end)

  defp continued(:start, state) do
    state = recovered_events(state)

    state =
      Enum.reduce(state.operations, state, fn {id, operation}, current ->
        if operation.deadline <= now(),
          do: step(current, {:operation_timeout, id, now()}),
          else: current
      end)

    {:noreply, step(state, :start)}
  end

  @impl true
  def handle_call(request, from, state), do: protect(fn -> call(request, from, state) end)

  defp call(:status, _from, state) do
    {:reply,
     %{
       phase: state.machine.phase,
       generation: state.machine.generation,
       peer: state.machine.peer,
       pending: map_size(state.operations),
       deliveries: map_size(state.machine.deliveries)
     }, state}
  end

  defp call(_request, _from, %{poisoned: reason} = state) when not is_nil(reason),
    do: {:reply, {:error, {:persistence, reason}}, state}

  defp call({:publish, topic, payload, options}, from, state) do
    if valid_options?(options, [:qos, :retain, :properties]) do
      packet = %Publish{
        topic: topic,
        payload: payload,
        qos: Keyword.get(options, :qos, 0),
        retain: Keyword.get(options, :retain, false),
        properties: Keyword.get(options, :properties, [])
      }

      operation(state, from, :publish, packet)
    else
      {:reply, {:error, :invalid_options}, state}
    end
  end

  defp call({:subscribe, filter, options}, from, state) do
    if valid_options?(options, [
         :qos,
         :no_local,
         :retain_as_published,
         :retain_handling,
         :properties
       ]) do
      subscription = %{
        qos: Keyword.get(options, :qos, 0),
        no_local: Keyword.get(options, :no_local, false),
        retain_as_published: Keyword.get(options, :retain_as_published, false),
        retain_handling: Keyword.get(options, :retain_handling, 0)
      }

      packet = %Subscribe{
        packet_id: 0,
        subscriptions: [{filter, subscription}],
        properties: Keyword.get(options, :properties, [])
      }

      operation(state, from, :subscribe, packet)
    else
      {:reply, {:error, :invalid_options}, state}
    end
  end

  defp call({:unsubscribe, filter}, from, state),
    do: operation(state, from, :unsubscribe, %Unsubscribe{packet_id: 0, filters: [filter]})

  defp call({:demand, demand}, _from, state) do
    event_reply(state, {:demand, demand, now()}, fn
      {:notify, {:demand, {:error, reason}}} -> {:error, reason}
      _ -> nil
    end)
  end

  defp call({:ack, {client, delivery_id}}, _from, state) when client == self() do
    event_reply(state, {:ack, delivery_id, now()}, fn
      {:notify, {:ack, ^delivery_id, {:error, reason}}} -> {:error, reason}
      _ -> nil
    end)
  end

  defp call({:ack, _delivery}, _from, state),
    do: {:reply, {:error, :invalid_delivery}, state}

  defp call(:disconnect, _from, state),
    do: reply_after(:ok, step(state, {:stop, now()}))

  @impl true
  def handle_info(message, state), do: protect(fn -> info(message, state) end)

  defp info(:persistence_stop, state), do: {:stop, :normal, state}

  defp info({:tcp, socket, bytes}, %{socket: socket, config: %{transport: :tcp}} = state),
    do: received(state, bytes)

  defp info({:ssl, socket, bytes}, %{socket: socket, config: %{transport: :tls}} = state),
    do: received(state, bytes)

  defp info({:tcp_closed, socket}, %{socket: socket} = state),
    do: {:noreply, lost(state, :closed)}

  defp info({:ssl_closed, socket}, %{socket: socket} = state),
    do: {:noreply, lost(state, :closed)}

  defp info({:tcp_error, socket, reason}, %{socket: socket} = state),
    do: {:noreply, lost(state, {:transport, reason})}

  defp info({:ssl_error, socket, reason}, %{socket: socket} = state),
    do: {:noreply, lost(state, {:transport, reason})}

  defp info({:tick, generation, token}, %{tick: {_, token}} = state) do
    state = step(%{state | tick: nil}, {:tick, generation, now()})
    {:noreply, schedule_tick(state)}
  end

  defp info({:handshake, generation, token}, %{handshake: {_, token}} = state),
    do: {:noreply, step(%{state | handshake: nil}, {:handshake_timeout, generation, now()})}

  defp info({:retry, token}, %{retry: {_, token}} = state),
    do: {:noreply, step(%{state | retry: nil}, {:retry, now()})}

  defp info({:operation_timeout, ref}, state) do
    if Map.has_key?(state.operations, ref),
      do: {:noreply, step(state, {:operation_timeout, ref, now()})},
      else: {:noreply, state}
  end

  defp info({:DOWN, monitor, :process, _pid, _reason}, %{observer_monitor: monitor} = state),
    do: {:stop, :normal, state}

  # A replaced connection or cancelled timer can still have a queued message.
  defp info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(reason, state), do: protect(fn -> finish(reason, state) end)

  defp finish(reason, state) do
    connected? = state.socket && state.machine.phase == :connected
    {machine, effects} = Machine.transition(state.machine, {:stop, now()})

    state =
      if state.store && !state.poisoned,
        do: persist(%{state | machine: machine}, effects),
        else: %{state | machine: machine}

    if !state.poisoned && connected? &&
         (reason in [:normal, :shutdown] or match?({:shutdown, _}, reason)) do
      case Packet.encode(%Disconnect{}, state.config.limits) do
        {:ok, bytes} when byte_size(bytes) <= state.machine.peer.maximum_packet_size ->
          socket_send(state, bytes)

        _ ->
          :ok
      end
    end

    close(state)

    Enum.each(state.operations, fn {id, operation} ->
      cancel_operation_timer(operation)
      operation_result(state, id, operation, {:error, :unknown_outcome})
    end)

    if state.store, do: Store.close(state.store)

    :ok
  end

  @impl true
  def format_status(status),
    do: Map.merge(status, %{state: :redacted, message: :redacted, reason: :redacted, log: []})

  defp operation(state, {caller, _tag}, kind, packet) do
    ref = make_ref()
    id = if state.store, do: state.next_operation, else: ref
    time = now()
    {machine, effects} = Machine.transition(state.machine, {kind, id, packet, time})

    rejected =
      Enum.find_value(effects, fn
        {:notify, {:operation, ^id, {:error, reason}}} -> {:error, reason}
        _ -> nil
      end)

    if rejected do
      {:reply, rejected, %{state | machine: machine}}
    else
      timer = Process.send_after(self(), {:operation_timeout, id}, state.config.operation_timeout)

      operation = %{
        caller: caller,
        ref: ref,
        timer: timer,
        kind: kind,
        deadline: time + state.config.operation_timeout
      }

      state = %{
        state
        | machine: machine,
          next_operation: state.next_operation + 1,
          operations: Map.put(state.operations, id, operation)
      }

      state = persist(state, effects)

      if state.store && !state.poisoned do
        send(caller, {:graviton_mq, self(), {:operation_identity, ref, {state.store.id, id}}})
      end

      reply_after({:ok, ref}, execute(state, effects))
    end
  end

  defp event_reply(state, event, rejection) do
    {machine, effects} = Machine.transition(state.machine, event)

    case Enum.find_value(effects, rejection) do
      nil -> reply_after(:ok, execute(persist(%{state | machine: machine}, effects), effects))
      error -> {:reply, error, %{state | machine: machine}}
    end
  end

  defp reply_after(_result, %{poisoned: reason} = state) when not is_nil(reason),
    do: {:reply, {:error, {:persistence, reason}}, state}

  defp reply_after(result, state), do: {:reply, result, state}

  defp step(%{poisoned: reason} = state, _event) when not is_nil(reason), do: state

  defp step(state, event) do
    {machine, effects} = Machine.transition(state.machine, event)

    machine =
      if machine.phase == :stopped and state.machine.phase != :stopped,
        do: %{machine | disconnected_at: machine.disconnected_at || now()},
        else: machine

    execute(persist(%{state | machine: machine}, effects), effects)
  end

  defp execute(state, effects),
    do:
      Enum.reduce(effects, state, fn effect, state ->
        if state.poisoned, do: state, else: effect(effect, state)
      end)

  defp recover(%{config: %{persistence: nil}} = state), do: {:ok, state}

  defp recover(state) do
    # Safe ETF decoding accepts existing atoms only. Load the role schema in a
    # fresh VM before the store decoder sees its atom vocabulary.
    with {:module, Checkpoint} <- Code.ensure_loaded(Checkpoint),
         {:ok, store, snapshot} <- Store.open(state.config.persistence) do
      if is_nil(snapshot) do
        {:ok, %{state | store: store}}
      else
        case Checkpoint.restore(snapshot, state.machine, now(), wall_now()) do
          {:ok, machine, meta} ->
            operations =
              machine.outbound
              |> Enum.reject(fn {_id, entry} -> Map.get(entry, :host_completed, false) end)
              |> Map.new(fn {id, entry} ->
                deadline = Map.fetch!(meta.deadlines, id)

                timer =
                  Process.send_after(self(), {:operation_timeout, id}, max(0, deadline - now()))

                {id, %{caller: nil, ref: nil, timer: timer, kind: entry.kind, deadline: deadline}}
              end)

            {:ok,
             %{
               state
               | store: store,
                 machine: machine,
                 operations: operations,
                 next_operation: meta.next_operation,
                 results: meta.results
             }}

          {:error, reason} ->
            Store.close(store)
            {:error, reason}
        end
      end
    end
  end

  defp recovered_events(%{store: nil} = state), do: state

  defp recovered_events(state) do
    Enum.each(state.operations, fn {id, operation} ->
      notify(state, {:recovered_operation, {state.store.id, id}, operation.kind})
    end)

    Enum.each(state.results, fn {id, entry} ->
      notify(state, {:recovered_operation_result, {state.store.id, id}, entry.result})
    end)

    Enum.each(state.machine.deliveries, fn {id, entry} ->
      if entry.delivered do
        notify(state, {:recovered_delivery, {self(), id}, entry.packet, entry.qos2_phase})
      end
    end)

    %{state | results: %{}}
  end

  defp persist(%{store: nil} = state, _effects), do: state
  defp persist(%{poisoned: reason} = state, _effects) when not is_nil(reason), do: state

  defp persist(state, effects) do
    results =
      Enum.reduce(effects, state.results, fn
        {:notify, {:operation, id, result}}, results ->
          case Map.get(state.operations, id) do
            nil -> results
            operation -> Map.put(results, id, %{kind: operation.kind, result: result})
          end

        _, results ->
          results
      end)

    deadlines =
      state.machine.outbound
      |> Enum.reject(fn {_id, entry} -> Map.get(entry, :host_completed, false) end)
      |> Map.new(fn {id, _} ->
        {id, Map.fetch!(state.operations, id).deadline}
      end)

    meta = %{next_operation: state.next_operation, deadlines: deadlines, results: results}

    with {:ok, snapshot} <- Checkpoint.capture(state.machine, meta, now(), wall_now()),
         {:ok, store} <- Store.commit(state.store, snapshot) do
      %{state | store: store, results: results}
    else
      {:error, reason, store} -> poison(%{state | store: store}, reason)
      {:error, reason} -> poison(state, {:failed, reason})
    end
  end

  defp poison(state, reason) do
    state = close(%{state | poisoned: reason})
    notify(state, {:storage_error, reason})

    Enum.each(state.operations, fn {id, operation} ->
      cancel_operation_timer(operation)
      operation_result(state, id, operation, {:error, :unknown_outcome})
    end)

    send(self(), :persistence_stop)
    %{state | operations: %{}}
  end

  defp notify(state, event), do: send(state.config.observer, {:graviton_mq, self(), event})

  defp cancel_operation_timer(%{timer: timer}) when is_reference(timer),
    do: Process.cancel_timer(timer)

  defp cancel_operation_timer(_), do: :ok

  defp operation_result(state, id, %{caller: nil}, result),
    do: notify(state, {:recovered_operation_result, {state.store.id, id}, result})

  defp operation_result(_state, _id, operation, result),
    do: send(operation.caller, {:graviton_mq, self(), {:operation, operation.ref, result}})

  defp effect({:open, generation}, state) do
    state = close(state)

    case connect(state.config) do
      {:ok, socket} ->
        state = %{state | socket: socket, buffer: <<>>}
        state = step(state, {:transport_up, generation, now()})

        if state.socket do
          state
          |> arm_socket()
          |> schedule(:handshake, state.config.handshake_timeout, generation)
          |> schedule_tick()
        else
          state
        end

      {:error, reason} ->
        step(state, {:transport_down, generation, {:connect, reason}, now()})
    end
  end

  defp effect({:send, _packet, _token}, %{socket: nil} = state), do: state

  defp effect({:send, packet, token}, state) do
    generation = state.machine.generation

    case Packet.encode(packet, state.config.limits) do
      {:ok, bytes} ->
        step(state, {:sent, generation, token, socket_send(state, bytes), now()})

      {:error, error} ->
        step(state, {:protocol_error, generation, {:encode, error.reason}, now()})
    end
  end

  defp effect({:close, _reason}, state), do: close(state)

  defp effect({:retry, attempt}, state) do
    delay =
      min(
        state.config.reconnect.max_delay,
        state.config.reconnect.delay * Integer.pow(2, min(attempt - 1, 20))
      )

    schedule(state, :retry, delay, nil)
  end

  defp effect({:notify, {:operation, id, result}}, state) do
    case Map.pop(state.operations, id) do
      {nil, _} ->
        state

      {operation, operations} ->
        cancel_operation_timer(operation)
        operation_result(state, id, operation, result)
        %{state | operations: operations, results: Map.delete(state.results, id)}
    end
  end

  defp effect({:notify, {:delivery, id, packet}}, state) do
    send(state.config.observer, {:graviton_mq, self(), {:delivery, {self(), id}, packet}})
    state
  end

  defp effect({:notify, {:protocol_complete, id, result}}, state) do
    if state.store,
      do: notify(state, {:recovered_protocol_result, {state.store.id, id}, result}),
      else: notify(state, {:protocol_result, id, result})

    state
  end

  defp effect({:notify, {:connected, _peer} = event}, state) do
    send(state.config.observer, {:graviton_mq, self(), event})
    cancel(state, :handshake)
  end

  defp effect({:notify, event}, state) do
    send(state.config.observer, {:graviton_mq, self(), event})
    state
  end

  defp received(state, bytes) do
    state = parse(%{state | buffer: state.buffer <> bytes})
    {:noreply, if(state.socket, do: arm_socket(state), else: state)}
  end

  defp parse(%{buffer: <<>>} = state), do: state

  defp parse(state) do
    case Packet.decode(state.buffer, :v5, state.config.limits) do
      {:ok, packet, rest} ->
        state =
          step(
            %{state | buffer: rest},
            {:packet, state.machine.generation, packet, now()}
          )

        if state.socket && state.machine.phase in [:awaiting_connack, :connected],
          do: parse(state),
          else: %{state | buffer: <<>>}

      {:more, _} ->
        state

      {:error, error} ->
        step(
          state,
          {:protocol_error, state.machine.generation, {:decode, error.class, error.reason}, now()}
        )
    end
  end

  defp lost(state, reason),
    do: step(state, {:transport_down, state.machine.generation, reason, now()})

  defp connect(config), do: Transport.connect(config)

  defp socket_send(%{config: config, socket: socket}, bytes),
    do: Transport.send(config.transport, socket, bytes)

  defp arm_socket(%{config: config, socket: socket} = state) do
    case Transport.active_once(config.transport, socket) do
      :ok -> state
      {:error, reason} -> lost(state, {:transport, reason})
    end
  end

  defp close(state) do
    Transport.close(state.config.transport, state.socket)

    state
    |> Map.merge(%{socket: nil, buffer: <<>>})
    |> cancel(:tick)
    |> cancel(:handshake)
    |> cancel(:retry)
  end

  defp schedule_tick(%{tick: nil, socket: socket} = state) when not is_nil(socket),
    do: schedule(state, :tick, state.config.tick_interval, state.machine.generation)

  defp schedule_tick(state), do: state

  defp schedule(state, kind, delay, generation) do
    state = cancel(state, kind)
    token = make_ref()
    message = if kind == :retry, do: {:retry, token}, else: {kind, generation, token}
    timer = Process.send_after(self(), message, delay)
    Map.put(state, kind, {timer, token})
  end

  defp cancel(state, kind) do
    if timer = Map.fetch!(state, kind), do: Process.cancel_timer(elem(timer, 0))
    Map.put(state, kind, nil)
  end

  defp valid_options?(options, keys),
    do:
      is_list(options) and Keyword.keyword?(options) and
        Enum.all?(Keyword.keys(options), &(&1 in keys))

  # OTP also reports a callback exception through proc_lib and the host
  # supervisor. Those paths do not call format_status, so never propagate
  # stack frames containing argument values or exception messages with data.
  # Preserve the failure class and source locations, and fail the process.
  defp protect(callback) do
    try do
      callback.()
    rescue
      exception ->
        :erlang.raise(
          :error,
          {:mqtt_client_callback_failed, exception.__struct__},
          private_stacktrace(__STACKTRACE__)
        )
    catch
      kind, _reason ->
        :erlang.raise(
          kind,
          {:mqtt_client_callback_failed, kind},
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

  defp wall_now, do: System.system_time(:millisecond)
  defp now, do: System.monotonic_time(:millisecond)
end
