defmodule GravitonMQ.Runtime.AMQP10.Queue do
  @moduledoc """
  Serialized owner of one bounded AMQP queue.

  Core owns transitions; this process owns consumer monitoring and coalesced
  watcher notifications. A controller may register a connection as the owner
  of a consumer lease. Both may subsequently claim, settle or disconnect that
  lease. Only the owner is monitored; its death returns every unresolved claim.

  Memory admission returns `{:ok, :accepted_in_memory}` and retains no event
  log. Opt-in local durability returns `{:ok, {:accepted_durable, commit_ref}}`
  after the event WAL reaches that durable-through boundary. Every persistent
  transition is synchronized before its effects escape. Recovery streams real
  logical events, validates stable identities and releases lost leases before
  admission. Failed/unknown storage freezes further transitions explicitly.
  A call timeout returns `{:error, :unknown_outcome}` because an admitted command
  may still execute. Queue loss is not proof that a message was never accepted.

  Claim byte limits use core charged units, including retained index and
  mutation data. A protocol adapter must separately bound its wire encoding.
  `watch_ack/2` clears a pending notification; watchers query current state
  after acknowledging to observe changes coalesced before that acknowledgement.
  """
  use GenServer

  alias GravitonMQ.Core.{Message, MessageId}
  alias GravitonMQ.Queue.{Event, Machine}
  alias GravitonMQ.Runtime.AMQP10.Persistence

  @max_generation 18_446_744_073_709_551_615

  def start_link(argument) do
    initializer = if is_function(argument, 0), do: argument, else: fn -> argument end
    GenServer.start_link(__MODULE__, initializer)
  end

  def child_spec({config, address}) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [fn -> {config, address} end]}
    }
  end

  def address(queue), do: request(queue, :address)
  def status(queue), do: request(queue, :status)
  def watch(queue, watcher), do: request(queue, {:watch, watcher})
  def watch_ack(queue, watcher), do: request(queue, {:watch_ack, watcher})

  def enqueue(queue, raw, wire_format, index, durable?),
    do: request(queue, {:enqueue, raw, wire_format, index, durable?})

  def register(queue, consumer_id, owner),
    do: request(queue, {:register, consumer_id, owner})

  def claim(queue, consumer_id, generation, count, charged_bytes),
    do: request(queue, {:claim, consumer_id, generation, count, charged_bytes})

  def settle(queue, consumer_id, generation, delivery_ref, outcome),
    do: request(queue, {:settle, consumer_id, generation, delivery_ref, outcome})

  def disconnect(queue, consumer_id, generation),
    do: request(queue, {:disconnect, consumer_id, generation})

  @impl true
  def init(initializer) do
    Process.flag(:sensitive, true)
    Process.flag(:trap_exit, true)
    protect(fn -> initialize(initializer.()) end)
  end

  defp initialize({%{queue_options: options, max_connections: maximum} = config, address})
       when is_integer(maximum) and maximum in 1..1024 and is_binary(address) and
              byte_size(address) in 1..255 do
    if String.valid?(address) and not String.contains?(address, <<0>>) do
      with {:ok, persistence, machine} <-
             Persistence.open(Map.get(config, :persistence), address, options) do
        data = %{
          address: address,
          machine: machine,
          persistence: persistence,
          storage_failure: nil,
          max_watchers: maximum,
          consumers: %{},
          owners: %{},
          owner_refs: %{},
          watchers: %{},
          watcher_refs: %{}
        }

        # OTP's debug ring includes callback replies and states outside
        # format_status/1. Keep all queue data behind a private table handle.
        table = :ets.new(__MODULE__, [:set, :private])
        true = :ets.insert(table, {:state, data})
        {:ok, %{table: table}}
      else
        {:error, reason} -> {:stop, reason}
      end
    else
      {:stop, :invalid_options}
    end
  end

  defp initialize(_argument), do: {:stop, :invalid_options}

  @impl true
  def handle_call({:execute, table, token}, from, state) do
    protect(fn ->
      case take_request(table, token, elem(from, 0)) do
        {:ok, message} ->
          data = :ets.lookup_element(state.table, :state, 2)
          {:reply, result, data} = call(message, from, data)
          true = :ets.insert(state.table, {:state, data})
          put_result(table, token, result)
          {:reply, :ok, state}

        :abandoned ->
          {:reply, :ok, state}
      end
    end)
  end

  def handle_call(_message, _from, state), do: {:reply, {:error, :unsupported_command}, state}

  defp call(:address, _from, state), do: {:reply, state.address, state}

  defp call(:status, _from, state) do
    machine = state.machine

    status = %{
      profile: machine.profile,
      storage: if(state.storage_failure, do: :failed, else: :ready),
      durable_through: if(state.persistence, do: state.persistence.durable, else: :none),
      messages: machine.message_count,
      ready: machine.ready_count,
      inflight: map_size(machine.claims),
      consumers: map_size(state.consumers),
      bytes: machine.bytes,
      free_messages:
        if(state.storage_failure,
          do: 0,
          else: machine.limits.max_messages - machine.message_count
        ),
      free_bytes:
        if(state.storage_failure, do: 0, else: machine.limits.max_bytes - machine.bytes),
      limits: machine.limits,
      watchers: map_size(state.watchers),
      pending_watchers: Enum.count(state.watchers, fn {_pid, watcher} -> watcher.pending? end),
      event_sequence: machine.event_sequence
    }

    {:reply, status, state}
  end

  defp call({:enqueue, raw, wire_format, index, durable?}, _from, state) do
    message = %Message{
      id: MessageId.new(:crypto.strong_rand_bytes(24)),
      encoded_content: raw,
      wire_format: wire_format,
      index: index,
      durable?: durable?
    }

    {state, result, []} = transition(state, fn op -> {:enqueue, op, message} end)

    accepted =
      if state.persistence,
        do: {:accepted_durable, state.persistence.durable},
        else: :accepted_in_memory

    {:reply, success(result, accepted), state}
  end

  defp call({:register, id, owner}, {controller, _tag}, state) do
    with :ok <- live_owner(owner),
         :ok <- registration_controller(state, id, controller),
         {:ok, generation} <- next_generation(state) do
      {next, result, []} =
        transition(state, fn op -> {:register_consumer, op, id, generation} end)

      case result do
        {:ok, :registered} ->
          next = next |> detach_consumer(id) |> attach_consumer(id, generation, owner, controller)
          {:reply, {:ok, generation}, next}

        {:error, _} = error ->
          {:reply, error, next}
      end
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp call({:claim, id, generation, count, bytes}, {caller, _tag}, state) do
    with :ok <- lease_controller(state, id, generation, caller) do
      {state, result, deliveries} =
        transition(state, fn op -> {:claim, op, id, generation, count, bytes} end)

      case result do
        {:ok, count} when count == length(deliveries) -> {:reply, {:ok, deliveries}, state}
        {:error, _} = error -> {:reply, error, state}
      end
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp call({:settle, id, generation, ref, outcome}, {caller, _tag}, state) do
    with :ok <- lease_controller(state, id, generation, caller) do
      {state, result, []} =
        transition(state, fn op -> {:settle, op, id, generation, ref, outcome} end)

      {:reply, success(result), state}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp call({:disconnect, id, generation}, {caller, _tag}, state) do
    with :ok <- lease_controller(state, id, generation, caller) do
      {state, result, []} = transition(state, fn op -> {:disconnect, op, id, generation} end)
      state = if match?({:ok, _}, result), do: detach_consumer(state, id), else: state
      {:reply, success(result), state}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp call({:watch, watcher}, {controller, _tag}, state) do
    cond do
      not local_pid?(watcher) ->
        {:reply, {:error, :invalid_watcher}, state}

      not Process.alive?(watcher) ->
        {:reply, {:error, :watcher_down}, state}

      Map.has_key?(state.watchers, watcher) ->
        registered = Map.fetch!(state.watchers, watcher)

        result =
          if controller in [watcher, registered.controller],
            do: :ok,
            else: {:error, :wrong_watcher}

        {:reply, result, state}

      map_size(state.watchers) >= state.max_watchers ->
        {:reply, {:error, :watcher_limit}, state}

      true ->
        ref = Process.monitor(watcher)
        registered = %{ref: ref, controller: controller, pending?: false}

        {:reply, :ok,
         %{
           state
           | watchers: Map.put(state.watchers, watcher, registered),
             watcher_refs: Map.put(state.watcher_refs, ref, watcher)
         }}
    end
  end

  defp call({:watch_ack, watcher}, {caller, _tag}, state) do
    case Map.fetch(state.watchers, watcher) do
      {:ok, registered} when caller == watcher or caller == registered.controller ->
        {:reply, :ok,
         %{state | watchers: Map.put(state.watchers, watcher, %{registered | pending?: false})}}

      {:ok, _} ->
        {:reply, {:error, :wrong_watcher}, state}

      :error ->
        {:reply, {:error, :not_watching}, state}
    end
  end

  defp call(_message, _from, state), do: {:reply, {:error, :unsupported_command}, state}

  @impl true
  def handle_info(message, state) do
    protect(fn ->
      data = :ets.lookup_element(state.table, :state, 2)

      case info(message, data) do
        {:noreply, data} ->
          true = :ets.insert(state.table, {:state, data})
          {:noreply, state}

        {:stop, reason, data} ->
          true = :ets.insert(state.table, {:state, data})
          {:stop, reason, state}
      end
    end)
  end

  defp info({:DOWN, ref, :process, pid, _reason}, state) do
    cond do
      Map.get(state.owner_refs, ref) == pid -> release_owner(state, pid)
      Map.get(state.watcher_refs, ref) == pid -> {:noreply, remove_watcher(state, pid)}
      true -> {:noreply, state}
    end
  end

  defp info({:EXIT, owner, _reason}, %{persistence: %{owner: owner}} = state),
    do: {:noreply, notify(%{state | storage_failure: :persistence_failed})}

  defp info(_message, state), do: {:noreply, state}

  defp release_owner(state, pid) do
    owned = Map.fetch!(state.owners, pid).consumers |> Enum.sort()

    Enum.reduce_while(owned, {:noreply, state}, fn id, {:noreply, state} ->
      lease = Map.fetch!(state.consumers, id)

      {state, result, []} =
        transition(state, fn op -> {:disconnect, op, id, lease.generation} end)

      case result do
        {:ok, :disconnected} ->
          {:cont, {:noreply, detach_consumer(state, id)}}

        {:error, :counter_exhausted} ->
          {:halt, {:stop, {:shutdown, :queue_counter_exhausted}, state}}

        {:error, _} ->
          {:halt, {:noreply, state}}
      end
    end)
  end

  defp transition(%{storage_failure: reason} = state, _command) when not is_nil(reason),
    do: {state, {:error, :persistence_failed}, []}

  defp transition(state, command) do
    op = make_ref()
    {machine, effects} = Machine.apply(state.machine, command.(op))

    {result, deliveries, changed?} =
      Enum.reduce(effects, {nil, [], false}, fn
        {:operation, ^op, result}, {nil, deliveries, changed?} ->
          {result, deliveries, changed?}

        {:delivery, delivery, message, metadata}, {result, deliveries, changed?} ->
          {result, [{delivery, message, metadata} | deliveries], changed?}

        {:event, %Event{}}, {result, deliveries, _changed?} ->
          # Runtime synchronizes logical events before exposing effects in the
          # durable profile; the memory profile retains no historical log.
          {result, deliveries, true}

        {:available, _node_id}, {result, deliveries, _changed?} ->
          {result, deliveries, true}
      end)

    true = result != nil
    events = for {:event, %Event{} = event} <- effects, do: event

    case Persistence.commit(state.persistence, events) do
      {:ok, persistence} ->
        state = %{state | machine: machine, persistence: persistence}
        state = if changed?, do: notify(state), else: state
        {state, result, Enum.reverse(deliveries)}

      {:error, reason} ->
        {notify(%{state | storage_failure: reason}), {:error, reason}, []}
    end
  end

  defp notify(state) do
    watchers =
      Map.new(state.watchers, fn {pid, registered} ->
        unless registered.pending?, do: send(pid, {:amqp10_queue_changed, self()})
        {pid, %{registered | pending?: true}}
      end)

    %{state | watchers: watchers}
  end

  defp attach_consumer(state, id, generation, owner, controller) do
    {owner_info, owner_refs} =
      case Map.fetch(state.owners, owner) do
        {:ok, registered} ->
          {registered, state.owner_refs}

        :error ->
          ref = Process.monitor(owner)
          {%{ref: ref, consumers: MapSet.new()}, Map.put(state.owner_refs, ref, owner)}
      end

    owner_info = %{owner_info | consumers: MapSet.put(owner_info.consumers, id)}

    %{
      state
      | consumers:
          Map.put(state.consumers, id, %{
            generation: generation,
            owner: owner,
            controller: controller
          }),
        owners: Map.put(state.owners, owner, owner_info),
        owner_refs: owner_refs
    }
  end

  defp detach_consumer(state, id) do
    case Map.pop(state.consumers, id) do
      {nil, _} ->
        state

      {lease, consumers} ->
        owner = Map.fetch!(state.owners, lease.owner)
        remaining = MapSet.delete(owner.consumers, id)

        if MapSet.size(remaining) == 0 do
          Process.demonitor(owner.ref, [:flush])

          %{
            state
            | consumers: consumers,
              owners: Map.delete(state.owners, lease.owner),
              owner_refs: Map.delete(state.owner_refs, owner.ref)
          }
        else
          %{
            state
            | consumers: consumers,
              owners: Map.put(state.owners, lease.owner, %{owner | consumers: remaining})
          }
        end
    end
  end

  defp remove_watcher(state, watcher) do
    {registered, watchers} = Map.pop!(state.watchers, watcher)
    Process.demonitor(registered.ref, [:flush])
    %{state | watchers: watchers, watcher_refs: Map.delete(state.watcher_refs, registered.ref)}
  end

  defp registration_controller(state, id, caller) do
    case Map.fetch(state.consumers, id) do
      :error -> :ok
      {:ok, lease} when caller == lease.controller or caller == lease.owner -> :ok
      {:ok, _} -> {:error, :wrong_owner}
    end
  end

  defp lease_controller(state, id, generation, caller) do
    case Map.fetch(state.consumers, id) do
      {:ok, %{generation: ^generation} = lease} ->
        if caller in [lease.controller, lease.owner], do: :ok, else: {:error, :wrong_owner}

      _ ->
        {:error, :stale_consumer}
    end
  end

  defp next_generation(%{machine: %{last_registration_generation: @max_generation}}),
    do: {:error, :generation_exhausted}

  defp next_generation(state), do: {:ok, state.machine.last_registration_generation + 1}

  defp live_owner(owner) do
    cond do
      not local_pid?(owner) -> {:error, :invalid_owner}
      not Process.alive?(owner) -> {:error, :owner_down}
      true -> :ok
    end
  end

  defp local_pid?(pid), do: is_pid(pid) and node(pid) == node()
  defp success({:ok, _}), do: :ok
  defp success({:error, _} = error), do: error
  defp success({:ok, _}, value), do: {:ok, value}
  defp success({:error, _} = error, _value), do: error

  defp request(queue, message) do
    # The exchange belongs to the caller and is destroyed on caller death.
    # Incoming/outgoing GenServer debug events contain only an opaque handle
    # and token, never request content or claimed messages.
    table = :ets.new(__MODULE__, [:set, :public])
    token = make_ref()

    try do
      true = :ets.insert(table, {token, message})

      case GenServer.call(queue, {:execute, table, token}) do
        :ok ->
          [{^token, result}] = :ets.take(table, token)
          result

        {:error, _} = error ->
          error
      end
    rescue
      _exception -> {:error, :queue_call_failed}
    catch
      :exit, {:timeout, _} -> {:error, :unknown_outcome}
      :exit, {:noproc, _} -> {:error, :closed}
      :exit, {:normal, _} -> {:error, :closed}
      :exit, {:shutdown, _} -> {:error, :closed}
      :exit, _reason -> {:error, :queue_call_failed}
    after
      :ets.delete(table)
    end
  end

  defp take_request(table, token, caller) do
    if :ets.info(table, :owner) == caller and :ets.info(table, :protection) == :public do
      case :ets.take(table, token) do
        [{^token, message}] -> {:ok, message}
        [] -> :abandoned
      end
    else
      :abandoned
    end
  rescue
    ArgumentError -> :abandoned
  end

  defp put_result(table, token, result) do
    :ets.insert(table, {token, result})
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def terminate(_reason, state) do
    case :ets.lookup(state.table, :state) do
      [{:state, data}] -> Persistence.close(data.persistence)
      _ -> :ok
    end
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
          {:amqp_queue_callback_failed, exception.__struct__},
          private_stacktrace(__STACKTRACE__)
        )
    catch
      kind, _reason ->
        :erlang.raise(
          kind,
          {:amqp_queue_callback_failed, kind},
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
end
