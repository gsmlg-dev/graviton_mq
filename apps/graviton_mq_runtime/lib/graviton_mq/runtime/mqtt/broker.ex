defmodule GravitonMQ.Runtime.MQTT.Broker do
  @moduledoc false
  use GenServer

  alias GravitonMQ.MQTT.Broker, as: Machine
  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.Publish
  alias GravitonMQ.Runtime.MQTT.{Auth, Persistence, Writer}

  def start_link(configuration), do: GenServer.start_link(__MODULE__, configuration)

  def child_spec({config, parent}) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [fn -> {config, parent} end]}}
  end

  @impl true
  def init(argument), do: protect(fn -> initialize(argument.()) end)

  defp initialize({config, parent}) do
    Process.flag(:trap_exit, true)
    Process.flag(:sensitive, true)

    with {:ok, machine} <- Machine.new(config.machine_options),
         {:ok, persistence, machine} <-
           Persistence.open(config.persistence, machine, now(), wall()) do
      state = %{
        config: config,
        machine: machine,
        persistence: persistence,
        storage_failure: nil,
        parent: parent,
        auth_supervisor: nil,
        operation_supervisor: nil,
        callers: %{},
        caller_ids: %{},
        host_operations: %{},
        jobs: %{},
        connections: %{},
        next_caller: 1,
        caller_epoch: :crypto.strong_rand_bytes(16),
        next_operation: 1,
        next_connection: 1,
        timer: nil
      }

      {:ok, state, {:continue, :wire}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:wire, state) do
    protect(fn ->
      children = Supervisor.which_children(state.parent)
      {_, auth, _, _} = List.keyfind(children, :auth_tasks, 0)
      {_, operations, _, _} = List.keyfind(children, :operation_tasks, 0)
      state = %{state | auth_supervisor: auth, operation_supervisor: operations}
      {:noreply, state |> step({:tick, now()}) |> schedule()}
    end)
  end

  @impl true
  def handle_call(request, from, state), do: protect(fn -> call(request, from, state) end)

  defp call(:status, _from, state) do
    deliveries =
      Enum.flat_map(state.machine.sessions, fn {_, session} -> session.deliveries end) ++
        Enum.flat_map(state.machine.hosts, fn {_, host} -> host.deliveries end)

    status = %{
      owner: self(),
      profile: if(state.persistence, do: :local_durable, else: :memory),
      storage: if(state.storage_failure, do: :failed, else: :ready),
      durable_through: if(state.persistence, do: state.persistence.durable, else: :none),
      connections: map_size(state.connections),
      sessions: map_size(state.machine.sessions),
      retained: map_size(state.machine.retained),
      host_subscriptions: map_size(state.machine.hosts),
      pending_deliveries: length(deliveries),
      prepared: map_size(state.machine.operations),
      limits: state.machine.limits
    }

    {:reply, status, state}
  end

  defp call({:host, _command}, _from, %{storage_failure: failure} = state)
       when not is_nil(failure),
       do: {:reply, {:error, :persistence_failed}, state}

  defp call({:host, _command}, _from, %{config: %{host_principal: nil}} = state),
    do: {:reply, {:error, :host_access_disabled}, state}

  defp call({:host, command}, from, state) do
    with {:ok, command, mode} <- host_command(command, state.config.limits) do
      {state, caller_id} = caller(state, elem(from, 0))
      id = state.next_operation
      reference = make_ref()

      operation = %{
        caller_id: caller_id,
        caller: elem(from, 0),
        from: from,
        reference: reference,
        mode: mode,
        command: elem(command, 0)
      }

      state = %{
        state
        | next_operation: id + 1,
          host_operations: Map.put(state.host_operations, id, operation)
      }

      event = {:host_operation, caller_id, id, {:as, state.config.host_principal, command}, now()}
      {machine, effects} = Machine.transition(state.machine, event)

      rejection =
        Enum.find_value(effects, fn
          {:complete, ^id, {:error, reason}} -> {:error, reason}
          _ -> nil
        end)

      if mode == :async and rejection do
        state = %{state | host_operations: Map.delete(state.host_operations, id)}
        effects = Enum.reject(effects, &match?({:complete, ^id, _}, &1))
        {:reply, rejection, commit_and_execute(state, machine, effects)}
      else
        state = commit_and_execute(state, machine, effects)

        cond do
          state.storage_failure != nil and mode == :sync -> {:noreply, state}
          state.storage_failure != nil -> {:reply, {:error, state.storage_failure}, state}
          mode == :async -> {:reply, {:ok, reference}, state}
          true -> {:noreply, state}
        end
      end
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp call({:connection_open, _reader, _writer}, _from, %{storage_failure: failure} = state)
       when not is_nil(failure),
       do: {:reply, {:error, :persistence_failed}, state}

  defp call({:connection_open, reader, writer}, _from, state) when is_pid(reader) do
    if map_size(state.connections) >= state.machine.limits.max_connections do
      {:reply, {:error, :connection_limit}, state}
    else
      id = state.next_connection
      generation = 1
      monitor = Process.monitor(reader)

      connection = %{
        reader: reader,
        writer: writer,
        generation: generation,
        monitor: monitor,
        opened_at: now(),
        pending: MapSet.new(),
        closing: nil
      }

      state = %{
        state
        | next_connection: id + 1,
          connections: Map.put(state.connections, id, connection)
      }

      state = step(state, {:connection_open, id, generation, now()})

      if Map.has_key?(state.machine.connections, id),
        do: {:reply, {:ok, id, generation}, state},
        else: {:reply, {:error, :connection_limit}, state}
    end
  end

  defp call({:writer_ready, id, generation, writer, peer}, _from, state) do
    case Map.get(state.connections, id) do
      %{generation: ^generation, closing: nil} = connection ->
        connection = Map.merge(connection, %{writer: writer, peer: peer})
        {:reply, :ok, %{state | connections: Map.put(state.connections, id, connection)}}

      _ ->
        {:reply, {:error, :closed}, state}
    end
  end

  defp call({:packet, id, generation, packet, encoded}, _from, state) do
    case Map.get(state.connections, id) do
      %{generation: ^generation, closing: nil} ->
        {:reply, :ok, step(state, {:packet, id, generation, packet, encoded, now()})}

      _ ->
        {:reply, {:error, :closed}, state}
    end
  end

  defp call({:connection_down, id, generation, reason}, _from, state) do
    state = step(state, {:connection_down, id, generation, reason, now()})
    {:reply, :ok, close_connection(state, id, generation, reason, true)}
  end

  defp call({:protocol_error, id, generation, reason}, _from, state),
    do: {:reply, :ok, step(state, {:protocol_error, id, generation, reason, now()})}

  @impl true
  def handle_info(message, state), do: protect(fn -> info(message, state) end)

  defp info({:tick, token}, %{timer: {_, token}} = state) do
    state = step(%{state | timer: nil}, {:tick, now()})

    state =
      Enum.reduce(state.connections, state, fn {id, connection}, state ->
        case Map.get(state.machine.connections, id) do
          %{phase: phase} when phase != :connected ->
            if now() - connection.opened_at >= state.config.connect_timeout,
              do:
                state
                |> step({:connection_down, id, connection.generation, :connect_timeout, now()})
                |> close_connection(id, connection.generation, :connect_timeout, true),
              else: state

          _ ->
            state
        end
      end)

    {:noreply, schedule(state)}
  end

  defp info({:auth_result, id, generation, tag, decision}, state) do
    case Map.get(state.jobs, id) do
      %{tag: ^tag, monitor: monitor} ->
        Process.demonitor(monitor, [:flush])

        {:noreply,
         step(
           %{state | jobs: Map.delete(state.jobs, id)},
           {:auth_result, id, generation, decision, now()}
         )}

      _stale ->
        {:noreply, state}
    end
  end

  defp info({:mqtt_write, id, generation, send_id, result}, state) do
    state =
      case Map.get(state.connections, id) do
        %{generation: ^generation} = connection ->
          connection = %{connection | pending: MapSet.delete(connection.pending, send_id)}
          %{state | connections: Map.put(state.connections, id, connection)}

        _ ->
          state
      end

    state = step(state, {:write_result, id, generation, send_id, result, now()})

    case Map.get(state.connections, id) do
      %{generation: ^generation, closing: reason} when reason != nil ->
        {:noreply, close_connection(state, id, generation, reason, result != :ok)}

      _ ->
        {:noreply, state}
    end
  end

  defp info({:reader_down, id, generation}, state),
    do: {:noreply, step(state, {:connection_down, id, generation, :transport_loss, now()})}

  defp info({:EXIT, owner, _reason}, %{persistence: %{owner: owner}} = state),
    do: {:noreply, fail_storage(state, :unknown_outcome)}

  defp info({:DOWN, monitor, :process, pid, _reason}, state) do
    cond do
      match?(%{monitor: ^monitor}, Map.get(state.callers, pid)) ->
        %{id: id} = state.callers[pid]

        state = %{
          state
          | callers: Map.delete(state.callers, pid),
            caller_ids: Map.delete(state.caller_ids, id),
            host_operations:
              Map.reject(state.host_operations, fn {_, op} -> op.caller_id == id end)
        }

        {:noreply, step(state, {:host_down, id, now()})}

      connection =
          Enum.find(state.connections, fn {_, connection} -> connection.monitor == monitor end) ->
        {id, connection} = connection
        state = %{state | connections: Map.delete(state.connections, id)}

        {:noreply,
         step(state, {:connection_down, id, connection.generation, :transport_loss, now()})}

      job = Enum.find(state.jobs, fn {_, job} -> job.monitor == monitor end) ->
        {id, job} = job
        state = %{state | jobs: Map.delete(state.jobs, id)}

        {:noreply,
         step(state, {:auth_result, id, job.generation, {:error, :callback_failed}, now()})}

      true ->
        {:noreply, state}
    end
  end

  defp info(_stale, state), do: {:noreply, state}

  @impl true
  def terminate(reason, state), do: protect(fn -> finish(reason, state) end)

  defp finish(_reason, state) do
    if state.timer, do: Process.cancel_timer(elem(state.timer, 0))
    Persistence.close(state.persistence)

    Enum.each(state.host_operations, fn {_, operation} ->
      if operation.mode == :async do
        send(
          operation.caller,
          {:graviton_mq, self(), {:operation, operation.reference, {:error, :unknown_outcome}}}
        )
      else
        GenServer.reply(operation.from, {:error, :unknown_outcome})
      end
    end)

    :ok
  end

  @impl true
  def format_status(status),
    do:
      Map.merge(
        status,
        %{state: :redacted, message: :redacted, reason: :redacted, log: []}
      )

  defp step(%{storage_failure: failure} = state, _event) when not is_nil(failure), do: state

  defp step(state, event) do
    {machine, effects} = Machine.transition(state.machine, event)
    commit_and_execute(state, machine, effects)
  end

  defp commit_and_execute(state, machine, effects) do
    case Persistence.commit(state.persistence, machine, now(), wall()) do
      {:ok, persistence} ->
        %{state | machine: machine, persistence: persistence} |> execute(effects) |> clean()

      {:error, reason} ->
        fail_storage(state, reason)
    end
  end

  defp fail_storage(state, reason) do
    Enum.each(state.jobs, fn {_, job} ->
      Process.demonitor(job.monitor, [:flush])
      Process.exit(job.pid, :kill)
    end)

    Enum.each(state.host_operations, fn {_, operation} ->
      result = {:error, reason}

      if operation.mode == :async,
        do:
          send(
            operation.caller,
            {:graviton_mq, self(), {:operation, operation.reference, result}}
          ),
        else: GenServer.reply(operation.from, result)
    end)

    state =
      Enum.reduce(state.connections, state, fn {id, connection}, state ->
        close_connection(state, id, connection.generation, :persistence_failed, true)
      end)

    %{state | storage_failure: reason, jobs: %{}, host_operations: %{}} |> clean()
  end

  defp execute(state, effects), do: Enum.reduce(effects, state, &effect/2)

  defp effect(_effect, %{storage_failure: failure} = state) when not is_nil(failure), do: state

  defp effect({:authorize, id, request, deadline}, state) do
    case Map.get(state.machine.operations, id) do
      nil ->
        state

      operation ->
        owner = self()
        generation = operation.generation
        tag = make_ref()
        config = state.config
        auth = state.auth_supervisor

        request =
          case {operation.source, request} do
            {{:network, connection, _}, {:authenticate, credentials}} ->
              peer =
                case Map.get(state.connections, connection) do
                  nil -> %{}
                  connection -> Map.get(connection, :peer, %{})
                end

              {:authenticate, Map.put(credentials, :peer, peer)}

            _ ->
              request
          end

        task = fn ->
          decision = authorize(auth, config.auth, request, deadline)
          send(owner, {:auth_result, id, generation, tag, decision})
        end

        case Task.Supervisor.start_child(state.operation_supervisor, task) do
          {:ok, pid} ->
            job = %{pid: pid, monitor: Process.monitor(pid), tag: tag, generation: generation}
            %{state | jobs: Map.put(state.jobs, id, job)}

          {:error, _reason} ->
            step(state, {:auth_result, id, generation, {:error, :callback_busy}, now()})
        end
    end
  end

  defp effect({:complete, id, result}, state) do
    case Map.pop(state.host_operations, id) do
      {nil, _} ->
        state

      {operation, operations} ->
        result =
          case {operation.command, result} do
            {:subscribe, {:ok, subscription}} ->
              {:ok, {:graviton_mq_subscription, self(), subscription}}

            {:publish, {:ok, :accepted_memory}} ->
              if state.persistence,
                do: {:ok, {:accepted_durable, state.persistence.durable}},
                else: {:ok, :accepted_in_memory}

            {_, {:ok, :ok}} ->
              :ok

            {_, result} ->
              result
          end

        if operation.mode == :async,
          do:
            send(
              operation.caller,
              {:graviton_mq, self(), {:operation, operation.reference, result}}
            ),
          else: GenServer.reply(operation.from, result)

        %{state | host_operations: operations}
    end
  end

  defp effect({:host_delivery, subscription, delivery, message}, state) do
    case Map.get(state.machine.hosts, subscription) do
      %{caller: caller} ->
        if pid = Map.get(state.caller_ids, caller) do
          send(
            pid,
            {:graviton_mq, self(),
             {:delivery, {:graviton_mq_subscription, self(), subscription},
              {:graviton_mq_delivery, self(), delivery},
              Map.take(message, [:topic, :payload, :qos, :retain, :properties])}}
          )
        end

        state

      _ ->
        state
    end
  end

  defp effect({:send, id, generation, send_id, packet}, state) do
    result =
      with %{generation: ^generation, writer: writer} <- Map.get(state.connections, id),
           {:ok, encoded} <- Packet.encode(packet, state.config.limits) do
        Writer.enqueue(writer, send_id, encoded)
      else
        {:error, _} = error -> error
        _closed -> {:error, :closed}
      end

    if result == :ok do
      connection = state.connections[id]
      connection = %{connection | pending: MapSet.put(connection.pending, send_id)}
      %{state | connections: Map.put(state.connections, id, connection)}
    else
      step(state, {:write_result, id, generation, send_id, result, now()})
    end
  end

  defp effect({:close, id, generation, reason}, state) do
    close_connection(state, id, generation, reason, false)
  end

  defp close_connection(state, id, generation, reason, force) do
    case Map.get(state.connections, id) do
      %{generation: ^generation} = connection ->
        if force or MapSet.size(connection.pending) == 0 do
          send(connection.reader, {:mqtt_close, reason})
          Process.demonitor(connection.monitor, [:flush])
          %{state | connections: Map.delete(state.connections, id)}
        else
          %{state | connections: Map.put(state.connections, id, %{connection | closing: reason})}
        end

      _ ->
        state
    end
  end

  defp authorize(supervisor, policy, requests, deadline) when is_list(requests),
    do: Enum.map(requests, &authorize(supervisor, policy, &1, deadline))

  defp authorize(supervisor, policy, request, deadline) do
    remaining = deadline - now()

    if remaining > 0,
      do: Auth.run(supervisor, policy, request, min(remaining, 60_000)),
      else: {:error, :callback_timeout}
  end

  defp clean(state) do
    jobs =
      Map.reject(state.jobs, fn {id, job} ->
        if Map.has_key?(state.machine.operations, id) do
          false
        else
          Process.demonitor(job.monitor, [:flush])
          Process.exit(job.pid, :kill)
          true
        end
      end)

    active =
      MapSet.new(
        Enum.map(state.machine.hosts, fn {_, host} -> host.caller end) ++
          Enum.map(state.host_operations, fn {_, op} -> op.caller_id end)
      )

    callers =
      Map.reject(state.callers, fn {_pid, caller} ->
        if MapSet.member?(active, caller.id),
          do: false,
          else:
            (
              Process.demonitor(caller.monitor, [:flush])
              true
            )
      end)

    %{
      state
      | jobs: jobs,
        callers: callers,
        caller_ids: Map.new(callers, fn {pid, caller} -> {caller.id, pid} end)
    }
  end

  defp caller(state, pid) do
    case Map.get(state.callers, pid) do
      %{id: id} ->
        {state, id}

      nil ->
        id = {state.caller_epoch, state.next_caller}
        caller = %{id: id, monitor: Process.monitor(pid)}

        {%{
           state
           | next_caller: state.next_caller + 1,
             callers: Map.put(state.callers, pid, caller),
             caller_ids: Map.put(state.caller_ids, id, pid)
         }, id}
    end
  end

  defp host_command({:publish, topic, payload, options}, limits) do
    if keyword?(options, [:qos, :retain, :properties]) do
      qos = Keyword.get(options, :qos, 0)

      packet = %Publish{
        topic: topic,
        payload: payload,
        qos: qos,
        packet_id: if(qos == 0, do: nil, else: 1),
        retain: Keyword.get(options, :retain, false),
        properties: Keyword.get(options, :properties, [])
      }

      case Packet.encode(packet, limits) do
        {:ok, encoded} ->
          {:ok, packet, <<>>} = Packet.decode(encoded, :v5, limits)
          {:ok, {:publish, packet, encoded}, :async}

        {:error, _reason} ->
          {:error, :invalid_publication}
      end
    else
      {:error, :invalid_options}
    end
  end

  defp host_command({:subscribe, filter, options}, _limits) do
    if keyword?(options, [:qos, :no_local, :retain_as_published, :retain_handling]) do
      opts = Map.new(options)
      {:ok, {:subscribe, filter, opts}, :async}
    else
      {:error, :invalid_options}
    end
  end

  defp host_command({:demand, {:graviton_mq_subscription, owner, id}, count}, _limits)
       when owner == self(),
       do: {:ok, {:demand, id, count}, :sync}

  defp host_command({:unsubscribe, {:graviton_mq_subscription, owner, id}}, _limits)
       when owner == self(),
       do: {:ok, {:unsubscribe, id}, :sync}

  defp host_command({:ack, {:graviton_mq_delivery, owner, id}}, _limits)
       when owner == self(),
       do: {:ok, {:ack, id}, :sync}

  defp host_command(_command, _limits), do: {:error, :not_owner}

  defp keyword?(options, allowed),
    do:
      is_list(options) and Keyword.keyword?(options) and
        Enum.all?(Keyword.keys(options), &(&1 in allowed))

  defp schedule(state) do
    token = make_ref()
    timer = Process.send_after(self(), {:tick, token}, state.config.tick_interval)
    %{state | timer: {timer, token}}
  end

  defp protect(callback) do
    try do
      callback.()
    catch
      kind, _reason ->
        stack =
          Enum.map(__STACKTRACE__, fn
            {module, function, args, location} when is_list(args) ->
              {module, function, length(args), location}

            frame ->
              frame
          end)

        :erlang.raise(kind, {:mqtt_broker_callback_failed, kind}, stack)
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
  defp wall, do: System.system_time(:millisecond)
end
