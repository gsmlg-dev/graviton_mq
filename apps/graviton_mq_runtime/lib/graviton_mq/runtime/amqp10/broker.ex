defmodule GravitonMQ.Runtime.AMQP10.Broker do
  @moduledoc false
  use GenServer

  alias GravitonMQ.AMQP10.{Forwarding, Outcome, OutcomeMapper, Value}
  alias GravitonMQ.AMQP10.Codec.{Limits, Message}
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.Core.Message, as: CoreMessage
  alias GravitonMQ.Core.Outcome, as: CoreOutcome
  alias GravitonMQ.Runtime.AMQP10.{Connection, Queue, QueueRequests}

  # A busy Connection may miss one capacity query; its routes are retried after
  # this delay instead of closing the Connection or spinning the pump.
  @capacity_timeout 25
  @capacity_retry 50
  # A fenced queue is probed outside this process until it answers again.
  @probe_delay 1_000
  # The Manager never waits for a queue. A request missing this deadline (the
  # former GenServer.call timeout) is classified by taking it back.
  @request_deadline 5_000
  @max_queue_requests 4_096

  def start_link(configuration), do: GenServer.start_link(__MODULE__, configuration)

  def child_spec(configuration),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [fn -> configuration end]}}

  # Under the 1 s status bound (D14) the caller waits at most this long for
  # the queues.
  @status_deadline 900

  # The Manager answers from its own state. The caller then asks every
  # unfenced queue concurrently under one deadline, so a stuck queue is
  # reported as :unresponsive and delays neither the caller nor the Manager.
  # Queue pids never leave this function.
  def status(owner) when is_pid(owner) do
    deadline = now() + @status_deadline
    # The Manager writes into this exchange, so it must never be deleted
    # before that write: should this caller die, the table passes to the
    # Manager, which deletes it.
    table = :ets.new(__MODULE__, [:set, :public, {:heir, owner, :amqp10_status}])
    token = make_ref()
    :ok = call_status(owner, table, token)

    try do
      [{^token, result}] = :ets.take(table, token)

      requests =
        for {address, {:queue, queue}} <- result.queues,
            into: %{},
            do: {address, Queue.send_request(queue, table, :status)}

      queues =
        Map.new(result.queues, fn
          {address, :unavailable} ->
            {address, :unavailable}

          {address, {:queue, _}} ->
            {address, queue_status(table, Map.fetch!(requests, address), deadline)}
        end)

      %{result | queues: queues}
    after
      :ets.delete(table)
    end
  end

  # A failed call hands the exchange to the Manager instead of deleting it, so
  # a late status handler finds it intact; the Manager deletes it when the
  # ETS-TRANSFER arrives, after that handler. The exit is kept: a timed-out
  # status is never a result.
  defp call_status(owner, table, token) do
    GenServer.call(owner, {:status, table, token}, 5_000)
  catch
    :exit, reason ->
      hand_over(table, owner)
      exit(reason)
  end

  defp hand_over(table, owner) do
    :ets.give_away(table, owner, :amqp10_status)
  rescue
    # give_away/3 fails here only for a Manager that is no longer alive, which
    # can no longer write the table.
    ArgumentError -> :ets.delete(table)
  end

  defp queue_status(table, {request, token}, deadline) do
    case Queue.response(table, token, :gen_server.receive_response(request, {:abs, deadline})) do
      {:known, status} when is_map(status) -> status
      _ -> :unresponsive
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
         anchors: %{},
         probes: %{},
         auth_supervisor: nil,
         connections: %{},
         authenticating: MapSet.new(),
         sasl_failures: %{auth: 0, sys: 0, sys_perm: 0, sys_temp: 0},
         routes: %{},
         link_names: MapSet.new(),
         operations: %{},
         jobs: %{},
         pump_scheduled: false,
         pump_remaining: [],
         pump_again: false,
         pump_busy: MapSet.new(),
         retry_timer: nil,
         exchanges: %{},
         requests: QueueRequests.new(@max_queue_requests),
         reqids: :gen_server.reqids_new(),
         request_timers: %{},
         status_waiters: %{},
         deferred: false,
         queue_fences: 0
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
          {address, queue}
        end)

      state = %{state | auth_supervisor: auth, queues: queues}

      # One caller-owned exchange per queue for the Manager's lifetime; the
      # boot `watch` below is the only remaining blocking call (path 1).
      exchanges =
        Map.new(queues, fn {_, queue} -> {queue, :ets.new(__MODULE__, [:set, :public])} end)

      state = %{state | exchanges: exchanges}

      state =
        Enum.reduce(Map.values(queues), state, fn queue, state ->
          case Queue.watch(queue, self()) do
            :ok -> %{state | anchors: Map.put(state.anchors, queue, anchor())}
            {:error, _} -> start_probe(state, queue)
          end
        end)

      save(shell, state)
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
          authenticating: MapSet.size(state.authenticating),
          sasl_failures: state.sasl_failures,
          operations: map_size(state.operations),
          queue_requests: QueueRequests.size(state.requests),
          queue_fences: state.queue_fences,
          queues:
            Map.new(state.queues, fn {address, queue} ->
              {address, if(fenced?(state, queue), do: :unavailable, else: {:queue, queue})}
            end)
        }

        # The owning caller deletes its table only after this reply; a caller
        # whose call failed has given it to this process instead (status/1).
        true = :ets.insert(table, {token, result})
      end

      {:reply, :ok, shell}
    end)
  end

  def handle_call(_, _from, shell), do: {:reply, {:error, :unsupported_command}, shell}

  @impl true
  def handle_info(message, shell),
    do: protect(fn -> save(shell, receive_message(message, data(shell))) end)

  # Replies and queue DOWNs for asynchronous requests are matched first; every
  # other message keeps its existing handler.
  defp receive_message(message, state) do
    case :gen_server.check_response(message, state.reqids, true) do
      {response, {queue, id}, reqids} ->
        queue_response(%{state | reqids: reqids}, queue, id, response)

      _no_request_or_no_reply ->
        info(message, state)
    end
  end

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
          peer_container_id: nil,
          # Deny-only until the Connection reports its negotiated principal.
          principal: :unauthenticated
        }

        %{state | connections: Map.put(state.connections, connection, record)}
    end
  end

  # Connection-local SASL facts: a principal (sent before its Endpoint starts,
  # hence before any Link event) and bounded counters. The principal returned
  # by the host callback (including any username the host puts in it) is
  # retained here for the connection's lifetime and passed to authorize/3.
  # SASL credentials and the authentication policy never reach this process.
  defp info({:amqp10_principal, connection, principal}, state) do
    case Map.fetch(state.connections, connection) do
      {:ok, record} ->
        %{
          state
          | connections: Map.put(state.connections, connection, %{record | principal: principal})
        }

      :error ->
        state
    end
  end

  defp info({:amqp10_sasl_authenticating, connection, active?}, state)
       when is_map_key(state.connections, connection) do
    authenticating =
      if active?,
        do: MapSet.put(state.authenticating, connection),
        else: MapSet.delete(state.authenticating, connection)

    %{state | authenticating: authenticating}
  end

  defp info({:amqp10_sasl_failure, _connection, code}, state)
       when code in [:auth, :sys, :sys_perm, :sys_temp],
       do: %{state | sasl_failures: Map.update!(state.sasl_failures, code, &(&1 + 1))}

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

  # A status exchange whose caller died or gave up (status/1). The status
  # handler skips a table its caller no longer owns, so it never writes here.
  defp info({:"ETS-TRANSFER", table, _caller, :amqp10_status}, state) do
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
    cond do
      queue not in Map.values(state.queues) ->
        state

      # A fenced queue is acknowledged again when its probe restores it.
      fenced?(state, queue) ->
        state

      QueueRequests.pending?(state.requests, queue, :watch_ack) ->
        state

      true ->
        case queue_request(state, queue, :watch_ack, {:watch_ack, self()}, nil, %{}) do
          {:ok, state} -> state
          {:error, :full, state} -> fence(state, queue)
        end
    end
  end

  defp info(:pump, state), do: pump(state)

  defp info({:pump_retry, timer}, %{retry_timer: timer} = state),
    do: schedule(%{state | retry_timer: nil})

  defp info({:probe_queue, queue}, state) do
    if fenced?(state, queue) and not Enum.any?(state.probes, &(elem(&1, 1) == queue)) do
      {_pid, reference} = spawn_monitor(fn -> probe(queue) end)
      %{state | probes: Map.put(state.probes, reference, queue)}
    else
      state
    end
  end

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

  defp info({:DOWN, reference, :process, _pid, reason}, %{probes: probes} = state)
       when is_map_key(probes, reference) do
    {queue, probes} = Map.pop!(probes, reference)
    restore(%{state | probes: probes}, queue, reason)
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

  defp info({:queue_deadline, queue, token}, state) do
    case Map.get(state.request_timers, queue) do
      {^token, _timer, _deadline} ->
        state = %{state | request_timers: Map.delete(state.request_timers, queue)}
        {expired, requests} = QueueRequests.expire(state.requests, queue, now())
        table = Map.fetch!(state.exchanges, queue)

        expired
        |> Enum.reduce(%{state | requests: requests}, fn entry, state ->
          answered(state, entry, Queue.classify(table, entry.token))
        end)
        |> arm_requests(queue)

      _ ->
        state
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
              queue_ref: nil,
              pending_delivery: nil,
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
      %{phase: :active, role: :receiver, queue_ref: nil} = route ->
        state = put_route(state, key, %{route | credit: false, receiving: true})

        case prepare_message(route, raw, metadata, state.config) do
          {:enqueue, message} ->
            case queue_request(state, route.queue, :enqueue, message, key, %{delivery: token}) do
              {:ok, state} -> state
              # Not executed: the sender may resend (owner Q5).
              {:error, :full, state} -> settle_publication(state, key, token, %Outcome.Released{})
            end

          outcome ->
            settle_publication(state, key, token, outcome)
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
    principal = Map.fetch!(state.connections, elem(key, 0)).principal

    # A connection without a reported principal is never offered to the host.
    if map_size(state.jobs) >= state.config.max_auth_jobs or principal == :unauthenticated do
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
            if callback.(principal, action, address) == :allow, do: :allow, else: :deny
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

    cond do
      fenced?(state, route.queue) -> reject(state, key, unavailable())
      route.role == :receiver -> accept(state, key, route)
      true -> register(state, key, route)
    end
  end

  defp decide(state, key, :deny),
    do: reject(state, key, error("amqp:unauthorized-access", "link admission denied"))

  defp reject(state, key, error) do
    route = Map.fetch!(state.routes, key)

    state =
      issue_untracked(
        state,
        elem(key, 0),
        elem(key, 1),
        {:reject_link, elem(key, 2), route.token, error}
      )

    remove_route(state, key)
  end

  defp accept(state, key, route) do
    state = put_route(state, key, %{route | phase: :accepting})
    issue(state, key, :accept, {:accept_link, elem(key, 2), route.token})
  end

  # This coordinator consumes ordered Connection outcome events before DOWN.
  # Monitoring the socket owner in Queue would race that final settlement
  # against a DOWN delivered independently to Queue. Queue instead monitors a
  # per-queue anchor that lives exactly as long as this coordinator unless the
  # coordinator fences that queue; either death releases all its leases there.
  defp register(state, key, route) do
    consumer = :crypto.strong_rand_bytes(24)
    message = {:register, consumer, Map.fetch!(state.anchors, route.queue)}
    state = put_route(state, key, %{route | phase: :registering})

    case queue_request(state, route.queue, :register, message, key, %{consumer: consumer}) do
      {:ok, state} ->
        state

      {:error, :full, state} ->
        reject(state, key, error("amqp:resource-limit-exceeded", "queue request limit reached"))
    end
  end

  # Path 12. A fenced queue has already lost every lease with its killed anchor.
  defp cleanup(state, queue, consumer, generation) do
    if fenced?(state, queue) do
      state
    else
      case queue_request(state, queue, :disconnect, {:disconnect, consumer, generation}, nil, %{}) do
        {:ok, state} -> state
        {:error, :full, state} -> fence(state, queue)
      end
    end
  end

  defp prepare_message(route, raw, %{message_format: 0}, config) do
    maximum = Keyword.get(config.queue_options, :max_message_bytes, 1_048_576)

    with {:ok, message, <<>>} <- Message.decode(raw, %Limits{max_value_bytes: maximum}),
         {:ok, durable?} <- profile_message(message.sections, config.profile) do
      {:enqueue,
       {:enqueue, raw, %CoreMessage.Format{family: "amqp-1.0", identifier: 0},
        %CoreMessage.Index{routing: %{"address" => route.address}}, durable?}}
    else
      {:error, reason} -> refusal(reason)
      _ -> refusal(:decode_error)
    end
  end

  defp prepare_message(_, _, _, _),
    do: %Outcome.Rejected{error: error("amqp:not-implemented", "unsupported message format")}

  # No disposition is invented for an unknown publication outcome. Enqueue
  # holds no lease, so only the publishing Connection is closed.
  defp settle_publication(state, key, _token, :unknown),
    do: close_connection(state, elem(key, 0), :queue_outcome_unknown)

  defp settle_publication(state, key, token, {:reject_durable, outcome}),
    do: issue(state, key, {:settle, outcome.error}, {:settle, token, outcome})

  defp settle_publication(state, key, token, outcome),
    do: issue(state, key, :settle, {:settle, token, outcome})

  defp publication(:not_executed, _profile), do: %Outcome.Released{}
  defp publication(:uncertain, _profile), do: :unknown

  defp publication({:known, {:ok, acceptance}}, profile),
    do: if(accepted(acceptance, profile) == :ok, do: %Outcome.Accepted{}, else: :unknown)

  defp publication({:known, {:error, reason}}, _profile), do: refusal(reason)

  defp refusal(:persistence_failed), do: :unknown

  defp refusal(:durability_unsupported),
    do:
      {:reject_durable,
       %Outcome.Rejected{
         error: error("amqp:precondition-failed", "memory queue cannot accept durable messages")
       }}

  defp refusal(:expiry_unsupported),
    do: %Outcome.Rejected{
      error: error("amqp:precondition-failed", "message requires another profile")
    }

  defp refusal(reason) when reason in [:queue_full, :message_too_large, :byte_limit],
    do: %Outcome.Rejected{error: error("amqp:resource-limit-exceeded", "queue capacity exceeded")}

  defp refusal(_reason),
    do: %Outcome.Rejected{error: error("amqp:decode-error", "message was not admitted")}

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

    state = put_route(state, operation.key, %{route | busy: nil})
    message = {:settle, route.consumer, route.generation, operation.delivery, outcome}

    # Issued before any later cleanup disconnect of this route: one queue
    # serves the Manager's requests in order (final outcomes before DOWN).
    case queue_request(state, route.queue, :settle, message, operation.key, %{result: result}) do
      {:ok, state} -> state
      {:error, :full, state} -> fence(state, route.queue)
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

  # Path 4.
  defp answered(state, %{kind: :register} = entry, outcome) do
    {route, state} = take_route(state, entry)

    case {outcome, route} do
      {{:known, {:ok, generation}}, %{phase: :registering} = route} ->
        accept(state, entry.key, %{route | consumer: entry.data.consumer, generation: generation})

      {{:known, {:ok, generation}}, _removed_or_replaced} ->
        # Invariant: a lease registered for a route that no longer exists is
        # released at once, so no lease is orphaned.
        cleanup(state, entry.queue, entry.data.consumer, generation)

      {{:known, {:error, _}}, nil} ->
        state

      {{:known, {:error, _}}, _route} ->
        decide(state, entry.key, :deny)

      {_not_executed_or_uncertain, _route} ->
        # A registration may exist in the queue: fencing kills its anchor and
        # rejects this registering Link.
        fence(state, entry.queue)
    end
  end

  defp answered(state, %{kind: :disconnect}, {:known, :ok}), do: state
  defp answered(state, %{kind: :disconnect}, {:known, {:error, :stale_consumer}}), do: state
  # The queue may still hold this lease and its claims.
  defp answered(state, %{kind: :disconnect} = entry, _outcome), do: fence(state, entry.queue)

  # Path 6. A lost peer outcome fences: claims return and the message is
  # redelivered (at-least-once).
  defp answered(state, %{kind: :settle} = entry, outcome) do
    {route, state} = take_route(state, entry)

    case {outcome, route} do
      {{:known, _}, nil} ->
        state

      {{:known, :ok}, _route} ->
        if match?({:ok, _}, entry.data.result),
          do: schedule(state),
          else: detach(state, entry.key, "delivery interrupted")

      {{:known, {:error, _}}, _route} ->
        detach(state, entry.key, "settlement resource limit")

      {_not_executed_or_uncertain, _route} ->
        fence(state, entry.queue)
    end
  end

  # Path 10.
  defp answered(state, %{kind: :claim} = entry, outcome) do
    {route, state} = take_route(state, entry)
    capacity = entry.data.capacity

    case {outcome, route} do
      {{:known, _}, nil} ->
        # The route's cleanup disconnect was issued after this claim and
        # returns anything it claimed.
        state

      {{:known, {:ok, [{delivery, message, metadata}]}}, route} ->
        case Forwarding.prepare(message.encoded_content, metadata, capacity.max_message_bytes) do
          {:ok, bytes} ->
            state
            |> put_route(entry.key, %{route | pending_delivery: {delivery.id, bytes}})
            |> schedule()

          {:error, _} ->
            unfit(state, entry.key, route, delivery.id)
        end

      {{:known, {:ok, []}}, _route} ->
        state |> drain_exhausted(entry.key, capacity) |> elem(0)

      {{:known, {:error, _}}, _route} ->
        detach(state, entry.key, "queue lease is unavailable")

      {_not_executed_or_uncertain, _route} ->
        # The claim may have executed; fencing releases it.
        fence(state, entry.queue)
    end
  end

  # Path 11.
  defp answered(state, %{kind: :unfit} = entry, outcome) do
    {route, state} = take_route(state, entry)

    cond do
      outcome in [:not_executed, :uncertain] -> fence(state, entry.queue)
      route -> detach(state, entry.key, "message cannot fit this link")
      true -> state
    end
  end

  # Paths 7–9.
  defp answered(state, %{kind: :status} = entry, outcome) do
    {waiters, status_waiters} = Map.pop(state.status_waiters, entry.queue, MapSet.new())
    state = %{state | status_waiters: status_waiters}

    case outcome do
      {:known, status} when is_map(status) ->
        Enum.reduce(waiters, state, fn key, state ->
          elem(continue_route(state, key, status), 0)
        end)

      _ ->
        fence(state, entry.queue)
    end
  end

  # Path 5. A gone Link or Connection means the publisher saw no disposition:
  # the documented unknown-outcome case.
  defp answered(state, %{kind: :enqueue} = entry, outcome) do
    case take_route(state, entry) do
      {nil, state} ->
        state

      {_route, state} ->
        settle_publication(
          state,
          entry.key,
          entry.data.delivery,
          publication(outcome, state.config.profile)
        )
    end
  end

  # Path 3.
  defp answered(state, %{kind: :watch_ack}, {:known, :ok}), do: schedule(state)
  defp answered(state, %{kind: :watch_ack} = entry, _outcome), do: fence(state, entry.queue)

  # Path 13: watch, then watch_ack, then a fresh anchor.
  defp answered(state, %{kind: :restore_watch} = entry, {:known, :ok}) do
    case queue_request(state, entry.queue, :restore_ack, {:watch_ack, self()}, nil, %{}) do
      {:ok, state} -> state
      {:error, :full, state} -> start_probe(state, entry.queue, @probe_delay)
    end
  end

  defp answered(state, %{kind: :restore_ack} = entry, {:known, :ok}) do
    if fenced?(state, entry.queue),
      do: schedule(%{state | anchors: Map.put(state.anchors, entry.queue, anchor())}),
      else: state
  end

  defp answered(state, %{kind: kind} = entry, _outcome)
       when kind in [:restore_watch, :restore_ack],
       do: start_probe(state, entry.queue, @probe_delay)

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

  defp pump_route(state, key), do: continue_route(state, key, nil)

  # `status` is the queue status this route waited for, or nil inside a wave.
  defp continue_route(state, key, status) do
    candidate = if closing?(state, elem(key, 0)), do: nil, else: Map.get(state.routes, key)

    case candidate do
      %{
        phase: :active,
        busy: nil,
        queue_ref: nil,
        role: :receiver,
        credit: false,
        receiving: false
      } =
          route ->
        maximum = session_maximum(state.config)

        case status do
          nil ->
            need_status(state, key, route.queue)

          %{free_messages: messages, free_bytes: bytes} when messages > 0 and bytes >= maximum ->
            {issue(state, key, :demand, {:demand, elem(key, 2), 1, maximum}), true}

          _ ->
            {state, false}
        end

      %{phase: :active, busy: nil, queue_ref: nil, role: :sender} = route ->
        case capacity(state, key) do
          {:ok, %{messages: messages, bytes: bytes} = capacity} when messages > 0 and bytes > 0 ->
            pump_sender(state, key, route, capacity, status)

          {:ok, %{drain_token: _} = capacity} ->
            case status do
              nil ->
                need_status(state, key, route.queue)

              %{ready: 0} when is_nil(route.pending_delivery) ->
                drain_exhausted(state, key, capacity)

              _ ->
                {state, false}
            end

          {:ok, _} ->
            {state, false}

          :busy ->
            {busy(state, elem(key, 0)), false}

          :closed ->
            {close_connection(state, elem(key, 0), :owner_not_responding), false}
        end

      _ ->
        {state, false}
    end
  end

  defp pump_sender(state, key, %{pending_delivery: {delivery, bytes}} = route, capacity, _status) do
    if byte_size(bytes) <= capacity.bytes do
      state = put_route(state, key, %{route | pending_delivery: nil})
      {issue(state, key, {:send, delivery}, {:send, elem(key, 2), bytes, []}), true}
    else
      {state, false}
    end
  end

  defp pump_sender(state, key, route, _capacity, nil), do: need_status(state, key, route.queue)

  defp pump_sender(state, key, route, capacity, status) do
    # Queue data charges include identity/index/mutations. Wire capacity
    # bounds only the outbound view; these are deliberately different units.
    charge_limit =
      min(
        status.limits.max_bytes,
        status.limits.max_message_bytes + status.limits.max_mutation_bytes
      )

    message = {:claim, route.consumer, route.generation, 1, charge_limit}

    case queue_request(state, route.queue, :claim, message, key, %{capacity: capacity}) do
      {:ok, state} -> {state, true}
      {:error, :full, state} -> {state, false}
    end
  end

  # One status request per queue serves every route waiting on it.
  defp need_status(state, key, queue) do
    case Map.fetch(state.status_waiters, queue) do
      {:ok, waiters} ->
        {%{
           state
           | status_waiters: Map.put(state.status_waiters, queue, MapSet.put(waiters, key))
         }, true}

      :error ->
        case queue_request(state, queue, :status, :status, nil, %{}) do
          {:ok, state} ->
            {%{state | status_waiters: Map.put(state.status_waiters, queue, MapSet.new([key]))},
             true}

          {:error, :full, state} ->
            {state, false}
        end
    end
  end

  defp unfit(state, key, route, delivery) do
    message =
      {:settle, route.consumer, route.generation, delivery, %CoreOutcome.MakeAvailable{}}

    case queue_request(state, route.queue, :unfit, message, key, %{}) do
      {:ok, state} -> state
      {:error, :full, state} -> fence(state, route.queue)
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

  # Issues one asynchronous request. A route-scoped request (`key` not nil)
  # occupies that route (`queue_ref`) until its reply, deadline or fence.
  defp queue_request(state, queue, kind, message, key, data) do
    if QueueRequests.full?(state.requests) do
      {:error, :full, %{state | deferred: true}}
    else
      route = if key, do: Map.get(state.routes, key)
      {request, token} = Queue.send_request(queue, Map.fetch!(state.exchanges, queue), message)

      fields = %{
        token: token,
        kind: kind,
        key: key,
        route_token: route && route.token,
        data: data
      }

      {:ok, entry, requests} =
        QueueRequests.add(state.requests, queue, fields, now() + @request_deadline)

      reqids = :gen_server.reqids_add(request, {queue, entry.id}, state.reqids)
      state = %{state | requests: requests, reqids: reqids}
      state = if route, do: put_route(state, key, %{route | queue_ref: entry.id}), else: state
      {:ok, arm_requests(state, queue)}
    end
  end

  defp queue_response(state, queue, id, response) do
    case QueueRequests.resolve(state.requests, queue, id) do
      {:pending, entry, requests} ->
        state = arm_requests(%{state | requests: requests}, queue)
        outcome = Queue.response(Map.fetch!(state.exchanges, queue), entry.token, response)
        state |> answered(entry, outcome) |> resume_deferred()

      {:waiting, entry, requests} ->
        # Already classified at its deadline or fence; free its exchange slot.
        :ets.delete(Map.fetch!(state.exchanges, queue), entry.token)
        resume_deferred(%{state | requests: requests})

      {:unknown, _requests} ->
        state
    end
  end

  defp resume_deferred(%{deferred: true} = state) do
    if QueueRequests.full?(state.requests), do: state, else: schedule(%{state | deferred: false})
  end

  defp resume_deferred(state), do: state

  # One timer per queue, for its head deadline only.
  defp arm_requests(state, queue) do
    deadline = QueueRequests.next_deadline(state.requests, queue)

    case Map.get(state.request_timers, queue) do
      {_token, _timer, ^deadline} ->
        state

      current ->
        if current, do: Process.cancel_timer(elem(current, 1))
        timers = Map.delete(state.request_timers, queue)

        if deadline do
          token = make_ref()
          timer = Process.send_after(self(), {:queue_deadline, queue, token}, deadline, abs: true)
          %{state | request_timers: Map.put(timers, queue, {token, timer, deadline})}
        else
          %{state | request_timers: timers}
        end
    end
  end

  # The route that issued `entry`, with its request slot cleared, or nil when
  # that route was removed or replaced meanwhile.
  defp take_route(state, entry) do
    case Map.get(state.routes, entry.key) do
      %{token: token, queue_ref: id} = route when token == entry.route_token and id == entry.id ->
        route = %{route | queue_ref: nil}
        {route, put_route(state, entry.key, route)}

      _ ->
        {nil, state}
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

        state = %{
          state
          | connections: connections,
            authenticating: MapSet.delete(state.authenticating, connection)
        }

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
        state = %{state | routes: routes, operations: operations, jobs: jobs}

        # A route still registering has no consumer yet; its late reply is
        # released by the register clause of answered/3.
        if route.consumer,
          do: cleanup(state, route.queue, route.consumer, route.generation),
          else: state
    end
  end

  # Fencing isolates one queue whose state may have diverged from this
  # coordinator. Killing its lease anchor makes Queue return every claim held
  # by this coordinator there when it next runs; only that queue's Links are
  # detached. Messages stay in the queue process. Pending requests to it are
  # taken back (never executed) or covered by the killed anchor; abandoned
  # enqueues are answered before any Detach below.
  defp fence(state, queue) do
    case Map.pop(state.anchors, queue) do
      {nil, _} ->
        state

      {anchor, anchors} ->
        Process.exit(anchor, :kill)

        state = %{
          state
          | anchors: anchors,
            queue_fences: state.queue_fences + 1,
            status_waiters: Map.delete(state.status_waiters, queue)
        }

        {abandoned, requests} = QueueRequests.abandon_all(state.requests, queue)
        state = arm_requests(%{state | requests: requests}, queue)
        table = Map.fetch!(state.exchanges, queue)

        state =
          Enum.reduce(abandoned, state, fn entry, state ->
            abandoned(state, entry, Queue.classify(table, entry.token))
          end)

        state =
          state.routes
          |> Enum.filter(fn {_, route} ->
            route.queue == queue and route.phase != :authorizing
          end)
          |> Enum.reduce(state, fn
            {key, %{phase: :registering}}, state -> reject(state, key, unavailable())
            {key, _route}, state -> detach(state, key, unavailable())
          end)

        state |> start_probe(queue, @probe_delay) |> schedule()
    end
  end

  defp abandoned(state, %{kind: :enqueue} = entry, outcome), do: answered(state, entry, outcome)
  # Lease requests need nothing here: the killed anchor releases them.
  defp abandoned(state, _entry, _outcome), do: state

  defp start_probe(state, queue, delay \\ 0) do
    Process.send_after(self(), {:probe_queue, queue}, delay)
    state
  end

  defp probe(queue) do
    exit(if is_map(Queue.status(queue)), do: :available, else: :unavailable)
  end

  defp restore(state, queue, :available) do
    if fenced?(state, queue) do
      case queue_request(state, queue, :restore_watch, {:watch, self()}, nil, %{}) do
        {:ok, state} -> state
        {:error, :full, state} -> start_probe(state, queue, @probe_delay)
      end
    else
      state
    end
  end

  defp restore(state, queue, _reason), do: start_probe(state, queue, @probe_delay)

  defp anchor do
    coordinator = self()

    spawn(fn ->
      reference = Process.monitor(coordinator)

      receive do
        {:DOWN, ^reference, :process, _, _} -> :ok
      end
    end)
  end

  defp fenced?(state, queue), do: not Map.has_key?(state.anchors, queue)
  defp unavailable, do: error("amqp:internal-error", "queue is unavailable")

  defp put_route(state, key, route), do: %{state | routes: Map.put(state.routes, key, route)}
  defp route_matches?(state, key, token), do: match?(%{token: ^token}, Map.get(state.routes, key))

  defp closing?(state, connection),
    do: not match?(%{closing: nil}, Map.get(state.connections, connection))

  defp schedule(%{pump_scheduled: true} = state), do: %{state | pump_again: true}

  defp schedule(state) do
    send(self(), :pump)

    %{
      state
      | pump_scheduled: true,
        pump_remaining: Map.keys(state.routes),
        pump_again: false,
        pump_busy: MapSet.new()
    }
  end

  # GenServer.call uses a process alias, so a reply after timeout is dropped.
  defp capacity(state, {connection, sid, identity}) do
    if MapSet.member?(state.pump_busy, connection),
      do: :busy,
      else: {:ok, Connection.capacity(connection, sid, identity, @capacity_timeout)}
  catch
    :exit, {:timeout, _} -> :busy
    :exit, _ -> :closed
  end

  defp busy(state, connection) do
    state = %{state | pump_busy: MapSet.put(state.pump_busy, connection)}

    if state.retry_timer do
      state
    else
      token = make_ref()
      Process.send_after(self(), {:pump_retry, token}, @capacity_retry)
      %{state | retry_timer: token}
    end
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
