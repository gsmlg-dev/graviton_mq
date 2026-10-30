defmodule GravitonMQ.Runtime.AMQP10.Broker do
  @moduledoc false
  use GenServer

  alias GravitonMQ.AMQP10.{Forwarding, Outcome, OutcomeMapper, Value}
  alias GravitonMQ.AMQP10.Codec.{Limits, Message}
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.Core.Message, as: CoreMessage
  alias GravitonMQ.Core.Outcome, as: CoreOutcome
  alias GravitonMQ.Runtime.AMQP10.{Connection, Queue}

  def start_link(configuration), do: GenServer.start_link(__MODULE__, configuration)

  def child_spec(configuration),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [fn -> configuration end]}}

  def status(owner) do
    table = :ets.new(__MODULE__, [:set, :public])
    token = make_ref()

    try do
      :ok = GenServer.call(owner, {:status, table, token})
      [{^token, result}] = :ets.take(table, token)
      result
    after
      :ets.delete(table)
    end
  end

  @impl true
  def init(configuration) do
    Process.flag(:trap_exit, true)
    Process.flag(:sensitive, true)
    {config, parent} = configuration.()
    table = :ets.new(__MODULE__, [:set, :private])

    :ets.insert(
      table,
      {:state,
       %{
         config: config,
         parent: parent,
         queues: %{},
         auth_supervisor: nil,
         connections: %{},
         routes: %{},
         link_names: MapSet.new(),
         operations: %{},
         jobs: %{},
         pump_scheduled: false,
         pump_remaining: [],
         pump_again: false
       }}
    )

    {:ok, %{table: table}, {:continue, :wire}}
  end

  @impl true
  def handle_continue(:wire, shell) do
    protect(fn ->
      state = data(shell)
      children = Supervisor.which_children(state.parent)
      {_, auth, _, _} = List.keyfind(children, :auth_tasks, 0)

      queues =
        Map.new(state.config.queues, fn address ->
          {_, queue, _, _} = List.keyfind(children, {Queue, address}, 0)
          :ok = Queue.watch(queue, self())
          {address, queue}
        end)

      save(shell, %{state | auth_supervisor: auth, queues: queues})
    end)
  end

  @impl true
  def handle_call({:status, table, token}, {caller, _}, shell) do
    protect(fn ->
      if :ets.info(table, :owner) == caller and :ets.info(table, :protection) == :public do
        state = data(shell)

        result = %{
          profile: state.config.profile,
          connections: map_size(state.connections),
          links: map_size(state.routes),
          auth_jobs: map_size(state.jobs),
          operations: map_size(state.operations),
          queues: Map.new(state.queues, fn {address, queue} -> {address, Queue.status(queue)} end)
        }

        :ets.insert(table, {token, result})
      end

      {:reply, :ok, shell}
    end)
  end

  def handle_call(_, _from, shell), do: {:reply, {:error, :unsupported_command}, shell}

  @impl true
  def handle_info(message, shell), do: protect(fn -> save(shell, info(message, data(shell))) end)

  defp info({:amqp10_connection_started, connection, %{pid: connection} = handle}, state)
       when is_pid(connection) do
    cond do
      Map.has_key?(state.connections, connection) ->
        state

      map_size(state.connections) >= state.config.max_connections ->
        Connection.close(connection, :connection_limit)
        state

      true ->
        record = %{
          monitor: Process.monitor(connection),
          handle: handle,
          closing: nil,
          peer_container_id: nil
        }

        %{state | connections: Map.put(state.connections, connection, record)}
    end
  end

  defp info({:amqp10_endpoint_event, connection, table, token}, state) do
    case Connection.take_event(table, token) do
      {:ok, event} ->
        if Map.has_key?(state.connections, connection),
          do: schedule(event(connection, event, state)),
          else: state

      :expired ->
        state
    end
  end

  defp info({:"ETS-TRANSFER", table, connection, {:amqp10_events, connection}}, state) do
    # Event tokens precede this signal from the same Connection. Its final
    # outcomes remain readable after process exit, before DOWN releases leases.
    :ets.delete(table)
    state
  end

  defp info({:amqp10_endpoint_overflow, connection}, state),
    do: close_connection(state, connection, :event_limit)

  defp info({:force_close, connection, token}, state) do
    case Map.get(state.connections, connection) do
      %{closing: {_timer, ^token}} -> Process.exit(connection, :kill)
      _ -> :ok
    end

    state
  end

  defp info({:amqp10_endpoint_ready, connection}, state) do
    if Map.has_key?(state.connections, connection) do
      Connection.pump_ack(connection)
      schedule(state)
    else
      state
    end
  end

  defp info({:amqp10_queue_changed, queue}, state) do
    if queue in Map.values(state.queues) do
      :ok = Queue.watch_ack(queue, self())
      schedule(state)
    else
      state
    end
  end

  defp info(:pump, state), do: pump(state)

  defp info({reference, result}, state) when is_reference(reference) do
    case Map.pop(state.jobs, reference) do
      {nil, _} ->
        state

      {job, jobs} ->
        Process.demonitor(reference, [:flush])
        Process.cancel_timer(job.timer)
        decision = if result == :allow and now() < job.deadline, do: :allow, else: :deny
        state = %{state | jobs: jobs}

        if route_matches?(state, job.key, job.token),
          do: decide(state, job.key, decision),
          else: state
    end
  end

  defp info({:auth_timeout, reference}, state) do
    case Map.pop(state.jobs, reference) do
      {nil, _} ->
        state

      {job, jobs} ->
        Process.demonitor(reference, [:flush])
        Process.exit(job.pid, :kill)
        state = %{state | jobs: jobs}

        if route_matches?(state, job.key, job.token),
          do: decide(state, job.key, :deny),
          else: state
    end
  end

  defp info({:DOWN, reference, :process, pid, _reason}, state) do
    case Map.get(state.jobs, reference) do
      nil ->
        case Map.get(state.connections, pid) do
          %{monitor: ^reference} -> remove_connection(state, pid)
          _ -> state
        end

      job ->
        Process.cancel_timer(job.timer)
        state = %{state | jobs: Map.delete(state.jobs, reference)}

        if route_matches?(state, job.key, job.token),
          do: decide(state, job.key, :deny),
          else: state
    end
  end

  defp info(_, state), do: state

  defp event(connection, {:event, {:connected, container}}, state) do
    record = Map.fetch!(state.connections, connection)

    %{
      state
      | connections:
          Map.put(state.connections, connection, %{record | peer_container_id: container})
    }
  end

  defp event(
         connection,
         {:event, {:session, sid, {:attach_requested, identity, token, pdu}}},
         state
       ) do
    key = {connection, sid, identity}
    state = remove_route(state, key)
    role = elem(identity, 1)

    address =
      case if(role == :receiver, do: pdu.target, else: pdu.source) do
        %{address: %Value{type: :string, value: address}} -> address
        _ -> nil
      end

    if active_name?(state, connection, identity) do
      issue_untracked(
        state,
        connection,
        sid,
        {:reject_link, identity, token,
         error("amqp:resource-locked", "link name is already active")}
      )
    else
      case Map.fetch(state.queues, address) do
        {:ok, queue} ->
          if closing?(state, connection) do
            state
          else
            route = %{
              key: key,
              token: token,
              queue: queue,
              address: address,
              role: role,
              phase: :authorizing,
              consumer: nil,
              generation: nil,
              busy: nil,
              credit: false,
              receiving: false
            }

            state = %{
              state
              | routes: Map.put(state.routes, key, route),
                link_names: MapSet.put(state.link_names, key)
            }

            authorize(state, key)
          end

        :error ->
          issue_untracked(
            state,
            connection,
            sid,
            {:reject_link, identity, token, error("amqp:not-found", "queue is not configured")}
          )
      end
    end
  end

  defp event(connection, {:event, {:session, sid, {:attached, identity}}}, state) do
    key = {connection, sid, identity}

    case Map.get(state.routes, key) do
      %{phase: :accepting} = route ->
        put_route(state, key, %{route | phase: :active})

      _ ->
        state
    end
  end

  defp event(connection, {:event, {:session, sid, {kind, identity, _}}}, state)
       when kind in [:detached, :refused],
       do:
         state
         |> remove_route({connection, sid, identity})
         |> release_names(fn key -> key == {connection, sid, identity} end)

  defp event(connection, {:event, {:session, sid, {:aborted, identity, _}}}, state) do
    key = {connection, sid, identity}

    case Map.get(state.routes, key) do
      nil -> state
      route -> put_route(state, key, %{route | credit: false, receiving: false})
    end
  end

  defp event(connection, {:event, {:session_error, sid, _}}, state),
    do: remove_routes(state, fn {conn, session, _} -> conn == connection and session == sid end)

  defp event(connection, {:event, {:session_ended, sid, _}}, state) do
    predicate = fn {conn, session, _} -> conn == connection and session == sid end
    state |> remove_routes(predicate) |> release_names(predicate)
  end

  defp event(connection, {:event, {:disconnected, _}}, state),
    do: remove_connection(state, connection)

  defp event(connection, {:delivery, sid, identity, token, raw, metadata}, state) do
    key = {connection, sid, identity}

    case Map.get(state.routes, key) do
      %{phase: :active, role: :receiver} = route ->
        state = put_route(state, key, %{route | credit: false, receiving: true})
        outcome = admit_message(route, raw, metadata, state.config)

        case outcome do
          :unknown ->
            close_connection(state, connection, :queue_outcome_unknown)

          {:reject_durable, outcome} ->
            issue(state, key, {:settle, outcome.error}, {:settle, token, outcome})

          outcome ->
            issue(state, key, :settle, {:settle, token, outcome})
        end

      _ ->
        close_connection(state, connection, :unadmitted_delivery)
    end
  end

  defp event(connection, {:operation, reference, result}, state) do
    case Map.pop(state.operations, reference) do
      {nil, _} ->
        state

      {operation, operations} ->
        state = %{state | operations: operations}

        if elem(operation.key, 0) == connection and
             route_matches?(state, operation.key, operation.token),
           do: complete(state, operation, result),
           else: state
    end
  end

  defp event(_, _, state), do: state

  defp active_name?(state, connection, identity) do
    peer = Map.fetch!(state.connections, connection).peer_container_id

    Enum.any?(state.link_names, fn {owner, _sid, name} ->
      name == identity and Map.fetch!(state.connections, owner).peer_container_id == peer
    end)
  end

  defp authorize(state, key) do
    if map_size(state.jobs) >= state.config.max_auth_jobs do
      decide(state, key, :deny)
    else
      route = Map.fetch!(state.routes, key)
      action = if route.role == :receiver, do: :send, else: :receive
      callback = state.config.authorize
      address = route.address

      task =
        authorization_task(state.auth_supervisor, fn ->
          Process.flag(:sensitive, true)

          try do
            if callback.(:anonymous, action, address) == :allow, do: :allow, else: :deny
          rescue
            _ -> :deny
          catch
            _, _ -> :deny
          end
        end)

      case task do
        {:ok, task} ->
          timer = Process.send_after(self(), {:auth_timeout, task.ref}, state.config.auth_timeout)

          job = %{
            pid: task.pid,
            timer: timer,
            deadline: now() + state.config.auth_timeout,
            key: key,
            token: route.token
          }

          %{state | jobs: Map.put(state.jobs, task.ref, job)}

        :unavailable ->
          decide(state, key, :deny)
      end
    end
  end

  defp decide(state, key, :allow) do
    route = Map.fetch!(state.routes, key)

    case lease(route, elem(key, 0)) do
      {:ok, route} ->
        state = put_route(state, key, %{route | phase: :accepting})
        issue(state, key, :accept, {:accept_link, elem(key, 2), route.token})

      {:error, _} ->
        decide(state, key, :deny)
    end
  end

  defp decide(state, key, :deny) do
    route = Map.fetch!(state.routes, key)

    state =
      issue_untracked(
        state,
        elem(key, 0),
        elem(key, 1),
        {:reject_link, elem(key, 2), route.token,
         error("amqp:unauthorized-access", "link admission denied")}
      )

    remove_route(state, key)
  end

  defp lease(%{role: :receiver} = route, _connection), do: {:ok, route}

  defp lease(route, _connection) do
    consumer = :crypto.strong_rand_bytes(24)

    # This coordinator consumes ordered Connection outcome events before DOWN.
    # Monitoring the socket owner in Queue would race that final settlement
    # against a DOWN delivered independently to Queue. Queue instead monitors
    # the coordinator; it releases all leases if this responsibility owner dies.
    case Queue.register(route.queue, consumer, self()) do
      {:ok, generation} ->
        {:ok, %{route | consumer: consumer, generation: generation}}

      {:error, reason} when reason in [:unknown_outcome, :queue_call_failed, :closed] ->
        exit({:shutdown, :queue_registration_uncertain})

      {:error, _} = error ->
        error
    end
  end

  defp admit_message(route, raw, %{message_format: 0}, config) do
    maximum = Keyword.get(config.queue_options, :max_message_bytes, 1_048_576)

    with {:ok, message, <<>>} <- Message.decode(raw, %Limits{max_value_bytes: maximum}),
         {:ok, durable?} <- profile_message(message.sections, config.profile),
         {:ok, acceptance} <-
           Queue.enqueue(
             route.queue,
             raw,
             %CoreMessage.Format{family: "amqp-1.0", identifier: 0},
             %CoreMessage.Index{routing: %{"address" => route.address}},
             durable?
           ),
         :ok <- accepted(acceptance, config.profile) do
      %Outcome.Accepted{}
    else
      {:error, reason}
      when reason in [:unknown_outcome, :persistence_failed, :queue_call_failed, :closed] ->
        :unknown

      {:error, :durability_unsupported} ->
        {:reject_durable,
         %Outcome.Rejected{
           error: error("amqp:precondition-failed", "memory queue cannot accept durable messages")
         }}

      {:error, :expiry_unsupported} ->
        %Outcome.Rejected{
          error: error("amqp:precondition-failed", "message requires another profile")
        }

      {:error, reason} when reason in [:queue_full, :message_too_large, :byte_limit] ->
        %Outcome.Rejected{error: error("amqp:resource-limit-exceeded", "queue capacity exceeded")}

      _ ->
        %Outcome.Rejected{error: error("amqp:decode-error", "message was not admitted")}
    end
  end

  defp admit_message(_, _, _, _),
    do: %Outcome.Rejected{error: error("amqp:not-implemented", "unsupported message format")}

  defp accepted(:accepted_in_memory, :memory), do: :ok
  defp accepted({:accepted_durable, %GravitonMQ.Core.CommitRef{}}, :local_durable), do: :ok
  defp accepted(_, _), do: {:error, :unknown_outcome}

  defp profile_message(sections, profile) do
    header =
      case List.keyfind(sections, :header, 0) do
        {:header, %Value{type: :list, value: fields}} -> fields
        nil -> []
      end

    properties =
      case List.keyfind(sections, :properties, 0) do
        {:properties, %Value{type: :list, value: fields}} -> fields
        nil -> []
      end

    cond do
      Enum.at(header, 0) == Value.boolean(true) and profile != :local_durable ->
        {:error, :durability_unsupported}

      present?(Enum.at(header, 2)) or present?(Enum.at(properties, 8)) ->
        {:error, :expiry_unsupported}

      true ->
        {:ok, Enum.at(header, 0) == Value.boolean(true)}
    end
  end

  defp present?(nil), do: false
  defp present?(%Value{type: :null}), do: false
  defp present?(_), do: true

  defp complete(state, %{kind: :send} = operation, result) do
    route = Map.fetch!(state.routes, operation.key)

    outcome =
      case result do
        {:ok, outcome} -> OutcomeMapper.to_core(outcome)
        {:error, {:outcome_known, outcome, _}} -> OutcomeMapper.to_core(outcome)
        _ -> %CoreOutcome.MakeAvailable{}
      end

    case Queue.settle(route.queue, route.consumer, route.generation, operation.delivery, outcome) do
      :ok ->
        state = put_route(state, operation.key, %{route | busy: nil})

        if match?({:ok, _}, result),
          do: state,
          else: detach(state, operation.key, "delivery interrupted")

      {:error, _} ->
        detach(state, operation.key, "settlement resource limit")
    end
  end

  defp complete(state, operation, {:ok, _}) do
    route = Map.fetch!(state.routes, operation.key)

    route =
      case operation.kind do
        :demand -> %{route | busy: nil, credit: true}
        :settle -> %{route | busy: nil, receiving: false}
        _ -> %{route | busy: nil}
      end

    put_route(state, operation.key, route)
  end

  defp complete(
         state,
         %{kind: :settle, rejection_error: %ProtocolError{} = error} = operation,
         _result
       ),
       do: detach(state, operation.key, error)

  defp complete(state, operation, _result),
    do: detach(state, operation.key, "link operation failed")

  defp pump(state) do
    {batch, remaining} = Enum.split(state.pump_remaining, state.config.max_pump_actions)

    state =
      Enum.reduce(batch, %{state | pump_remaining: remaining}, fn key, state ->
        {state, _action?} = pump_route(state, key)
        state
      end)

    cond do
      remaining != [] ->
        send(self(), :pump)
        state

      state.pump_again ->
        schedule(%{state | pump_scheduled: false, pump_again: false})

      true ->
        %{state | pump_scheduled: false}
    end
  end

  defp pump_route(state, key) do
    candidate = if closing?(state, elem(key, 0)), do: nil, else: Map.get(state.routes, key)

    case candidate do
      %{phase: :active, busy: nil, role: :receiver, credit: false, receiving: false} = route ->
        status = Queue.status(route.queue)
        maximum = session_maximum(state.config)

        if is_map(status) and status.free_messages > 0 and status.free_bytes >= maximum do
          {issue(state, key, :demand, {:demand, elem(key, 2), 1, maximum}), true}
        else
          {state, false}
        end

      %{phase: :active, busy: nil, role: :sender} = route ->
        case capacity(elem(key, 0), elem(key, 1), elem(key, 2)) do
          {:ok, %{messages: messages, bytes: bytes} = capacity} when messages > 0 and bytes > 0 ->
            status = Queue.status(route.queue)
            if not is_map(status), do: exit({:shutdown, :queue_unavailable})
            # Queue data charges include identity/index/mutations. Wire capacity
            # bounds only the outbound view; these are deliberately different units.
            charge_limit =
              min(
                status.limits.max_bytes,
                status.limits.max_message_bytes + status.limits.max_mutation_bytes
              )

            case Queue.claim(route.queue, route.consumer, route.generation, 1, charge_limit) do
              {:ok, [{delivery, message, metadata}]} ->
                case Forwarding.prepare(message.encoded_content, metadata, capacity.bytes) do
                  {:ok, bytes} ->
                    {issue(state, key, {:send, delivery.id}, {:send, elem(key, 2), bytes, []}),
                     true}

                  {:error, _} ->
                    :ok =
                      Queue.settle(
                        route.queue,
                        route.consumer,
                        route.generation,
                        delivery.id,
                        %CoreOutcome.MakeAvailable{}
                      )

                    {detach(state, key, "message cannot fit this link"), true}
                end

              {:ok, []} ->
                drain_exhausted(state, key, capacity)

              {:error, _} ->
                {detach(state, key, "queue lease is unavailable"), true}
            end

          {:ok, %{drain_token: _} = capacity} ->
            case Queue.status(route.queue) do
              %{ready: 0} -> drain_exhausted(state, key, capacity)
              status when is_map(status) -> {state, false}
              _ -> exit({:shutdown, :queue_unavailable})
            end

          {:ok, _} ->
            {state, false}

          :closed ->
            {close_connection(state, elem(key, 0), :owner_not_responding), false}
        end

      _ ->
        {state, false}
    end
  end

  defp drain_exhausted(state, key, %{drain_token: token}),
    do: {issue(state, key, :drain, {:drain_exhausted, elem(key, 2), token}), true}

  defp drain_exhausted(state, _key, _capacity), do: {state, false}

  defp session_maximum(config),
    do:
      config.endpoint_options
      |> Keyword.get(:session_options, [])
      |> Keyword.get(:max_message_bytes, 1_048_576)

  defp issue(state, key, kind, command) do
    route = Map.fetch!(state.routes, key)
    reference = make_ref()
    operation = %{key: key, token: route.token, kind: kind, delivery: nil}

    operation =
      case kind do
        {:send, delivery} -> %{operation | kind: :send, delivery: delivery}
        {:settle, error} -> Map.merge(operation, %{kind: :settle, rejection_error: error})
        _ -> operation
      end

    case send_command(state, elem(key, 0), reference, {:session, elem(key, 1), command}) do
      :ok ->
        state = put_route(state, key, %{route | busy: reference})
        %{state | operations: Map.put(state.operations, reference, operation)}

      _ ->
        close_connection(state, elem(key, 0), :command_limit)
    end
  end

  defp issue_untracked(state, connection, sid, command) do
    case send_command(state, connection, make_ref(), {:session, sid, command}) do
      :ok -> state
      _ -> close_connection(state, connection, :command_limit)
    end
  end

  defp send_command(state, connection, reference, command) do
    case Map.get(state.connections, connection) do
      %{handle: handle} -> Connection.command(handle, reference, command)
      nil -> {:error, :closed}
    end
  end

  defp detach(state, key, %ProtocolError{} = error) do
    state =
      issue_untracked(
        state,
        elem(key, 0),
        elem(key, 1),
        {:detach, elem(key, 2), error}
      )

    remove_route(state, key)
  end

  defp detach(state, key, description),
    do: detach(state, key, error("amqp:resource-limit-exceeded", description))

  defp close_connection(state, connection, reason) do
    case Map.get(state.connections, connection) do
      %{closing: nil} = record ->
        Connection.close(connection, reason)
        token = make_ref()

        timer =
          Process.send_after(self(), {:force_close, connection, token}, state.config.send_timeout)

        record = %{record | closing: {timer, token}}
        %{state | connections: Map.put(state.connections, connection, record)}

      _ ->
        state
    end
  end

  defp remove_connection(state, connection) do
    case Map.pop(state.connections, connection) do
      {nil, _} ->
        state

      {record, connections} ->
        if record.closing, do: Process.cancel_timer(elem(record.closing, 0))
        Process.demonitor(record.monitor, [:flush])
        state = %{state | connections: connections}
        predicate = fn {conn, _, _} -> conn == connection end
        state |> remove_routes(predicate) |> release_names(predicate) |> schedule()
    end
  end

  # Queue routes can retire before an asynchronous Detach reaches its owner.
  # The separate bounded reservations survive until the endpoint confirms it.
  defp release_names(state, predicate),
    do: %{state | link_names: MapSet.reject(state.link_names, predicate)}

  defp remove_routes(state, predicate) do
    Enum.reduce(Map.keys(state.routes), state, fn key, state ->
      if predicate.(key), do: remove_route(state, key), else: state
    end)
  end

  defp remove_route(state, key) do
    case Map.pop(state.routes, key) do
      {nil, _} ->
        state

      {route, routes} ->
        if route.consumer do
          case Queue.disconnect(route.queue, route.consumer, route.generation) do
            :ok -> :ok
            {:error, :stale_consumer} -> :ok
            {:error, _} -> exit({:shutdown, :queue_cleanup_uncertain})
          end
        end

        jobs =
          Enum.reduce(state.jobs, %{}, fn {ref, job}, jobs ->
            if job.key == key do
              Process.cancel_timer(job.timer)
              Process.demonitor(ref, [:flush])
              Process.exit(job.pid, :kill)
              jobs
            else
              Map.put(jobs, ref, job)
            end
          end)

        operations = Map.reject(state.operations, fn {_, op} -> op.key == key end)
        %{state | routes: routes, operations: operations, jobs: jobs}
    end
  end

  defp put_route(state, key, route), do: %{state | routes: Map.put(state.routes, key, route)}
  defp route_matches?(state, key, token), do: match?(%{token: ^token}, Map.get(state.routes, key))

  defp closing?(state, connection),
    do: not match?(%{closing: nil}, Map.get(state.connections, connection))

  defp schedule(%{pump_scheduled: true} = state), do: %{state | pump_again: true}

  defp schedule(state) do
    send(self(), :pump)
    %{state | pump_scheduled: true, pump_remaining: Map.keys(state.routes), pump_again: false}
  end

  defp capacity(connection, sid, identity) do
    {:ok, Connection.capacity(connection, sid, identity, 25)}
  catch
    :exit, _ -> :closed
  end

  defp authorization_task(supervisor, callback) do
    {:ok, Task.Supervisor.async_nolink(supervisor, callback)}
  rescue
    _ -> :unavailable
  catch
    :exit, _ -> :unavailable
  end

  defp error(condition, description),
    do: %ProtocolError{condition: Value.symbol(condition), description: Value.string(description)}

  defp now, do: System.monotonic_time(:millisecond)
  defp data(shell), do: :ets.lookup_element(shell.table, :state, 2)

  defp save(shell, state) do
    :ets.insert(shell.table, {:state, state})
    {:noreply, shell}
  end

  @impl true
  def terminate(_, shell) do
    state = data(shell)

    Enum.each(state.connections, fn {connection, _} ->
      Connection.close(connection, :broker_stopped)
    end)

    Enum.each(state.jobs, fn {reference, job} ->
      Process.cancel_timer(job.timer)
      Process.demonitor(reference, [:flush])
      Process.exit(job.pid, :kill)
    end)
  end

  @impl true
  def format_status(status),
    do: Map.merge(status, %{state: :redacted, message: :redacted, reason: :redacted, log: []})

  defp protect(callback) do
    try do
      callback.()
    rescue
      exception ->
        :erlang.raise(
          :error,
          {:amqp_broker_callback_failed, exception.__struct__},
          private_stacktrace(__STACKTRACE__)
        )
    catch
      :exit, {:shutdown, reason} when is_atom(reason) ->
        exit({:shutdown, reason})

      kind, _ ->
        :erlang.raise(
          kind,
          {:amqp_broker_callback_failed, kind},
          private_stacktrace(__STACKTRACE__)
        )
    end
  end

  defp private_stacktrace(stacktrace),
    do:
      Enum.map(stacktrace, fn
        {module, function, arguments, location} when is_list(arguments) ->
          {module, function, length(arguments), location}

        frame ->
          frame
      end)
end
