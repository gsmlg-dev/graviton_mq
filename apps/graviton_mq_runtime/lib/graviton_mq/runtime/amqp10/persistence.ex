defmodule GravitonMQ.Runtime.AMQP10.Persistence do
  @moduledoc false
  # Queue history is a log of logical events replayed through the machine.
  # Compaction replaces it with the identity event followed by a snapshot of the
  # durable machine state, split into "queue_snapshot/v1" chunk events that each
  # fit one WAL record. Replay restores the snapshot exactly, including event,
  # delivery and generation counters, so later events still verify against the
  # machine. Compaction runs before an append when the log has passed half its
  # limit (and has at least doubled since its base), when an append is rejected
  # by the log limit, and on open. Only the durable machine is ever compacted.

  alias GravitonMQ.Core.{CommitRef, DeliveryRef, Message, MessageId, NodeId, Outcome}
  alias GravitonMQ.Queue.{Event, EventId, Machine, State}
  alias GravitonMQ.Storage.WAL

  @snapshot "queue_snapshot/v1"
  @max_counter 18_446_744_073_709_551_615

  def open(nil, _address, options) do
    Machine.new(
      NodeId.new(:crypto.strong_rand_bytes(24)),
      Keyword.put(options, :incarnation, :crypto.strong_rand_bytes(24))
    )
    |> case do
      {:ok, machine} -> {:ok, nil, machine}
      error -> error
    end
  end

  def open(config, address, options) do
    node =
      <<"amqp-queue/v1:", byte_size(config.stream_id)::16, config.stream_id::binary,
        byte_size(address)::16, address::binary>>

    path =
      Path.join(config.directory, Base.encode16(:crypto.hash(:sha256, address), case: :lower))

    wal_options = [
      path: path,
      stream_id: node,
      max_record_bytes: config.max_record_bytes,
      max_log_bytes: config.max_log_bytes,
      fault_injector: config.fault_injector
    ]

    with {:ok, owner} <- WAL.start_link(wal_options) do
      persistence = %{
        owner: owner,
        durable: :none,
        address: address,
        max_record_bytes: config.max_record_bytes
      }

      result = recover(owner, node, address, options)

      case result do
        {:ok, recovered} ->
          {:ok, durable} = WAL.durable_through(owner)
          persistence = %{persistence | durable: durable}

          # The recovered machine is exactly the durable state, so a log left at
          # or over the threshold (possibly at its hard limit) compacts first.
          with {:ok, persistence} <- maybe_compact(persistence, recovered),
               {:ok, machine, events} <- release_leases(recovered),
               {:ok, persistence} <- commit(persistence, events, recovered) do
            {:ok, persistence, machine}
          else
            error ->
              close(persistence)
              error
          end

        error ->
          close(persistence)
          error
      end
    end
  end

  def commit(persistence, events), do: persistence |> write(events) |> full_log()

  # `durable` is the machine state before `events`, i.e. what the log holds
  # once every earlier commit succeeded. It is the only state ever compacted.
  def commit(nil, _events, _durable), do: {:ok, nil}
  def commit(persistence, [], _durable), do: {:ok, persistence}

  def commit(persistence, events, durable) do
    with {:ok, persistence} <- maybe_compact(persistence, durable) do
      case write(persistence, events) do
        {:error, :log_limit} ->
          with {:ok, persistence} <- compact(persistence, durable),
               do: persistence |> write(events) |> full_log()

        result ->
          full_log(result)
      end
    end
  catch
    :exit, _ -> {:error, :unknown_outcome}
  end

  defp write(nil, _events), do: {:ok, nil}
  defp write(persistence, []), do: {:ok, persistence}

  defp write(persistence, events) do
    case append_batches(persistence.owner, events) do
      {:ok, %CommitRef{} = ref} ->
        case WAL.sync(persistence.owner, ref) do
          {:ok, %CommitRef{stream_id: stream, position: position} = durable}
          when stream == ref.stream_id and position >= ref.position ->
            {:ok, %{persistence | durable: durable}}

          _ ->
            {:error, :unknown_outcome}
        end

      {:error, reason} ->
        {:error, reason}
    end
  catch
    :exit, _ -> {:error, :unknown_outcome}
  end

  defp full_log({:error, :log_limit}), do: {:error, :persistence_failed}
  defp full_log(result), do: result

  defp maybe_compact(persistence, durable) do
    case WAL.usage(persistence.owner) do
      {:ok, %{bytes: bytes, base_bytes: base, limit: limit}} ->
        if bytes >= div(limit, 2) and bytes >= 2 * base,
          do: compact(persistence, durable),
          else: {:ok, persistence}

      {:error, reason} ->
        {:error, failure(reason)}
    end
  end

  defp compact(persistence, durable) do
    with {:ok, events} <-
           compaction_events(durable, persistence.address, persistence.max_record_bytes),
         {:ok, %CommitRef{} = ref} <- WAL.compact(persistence.owner, events) do
      {:ok, %{persistence | durable: ref}}
    else
      {:error, reason} -> {:error, failure(reason)}
    end
  end

  defp failure(:unknown_outcome), do: :unknown_outcome
  defp failure({:io, _, _}), do: :unknown_outcome
  defp failure({:unknown_outcome, _, _}), do: :unknown_outcome
  defp failure({:poisoned, _}), do: :unknown_outcome
  defp failure(_), do: :persistence_failed

  defp append_batches(owner, events) do
    # A disconnect/recovery can release more than one physical append batch.
    # Append every chunk before one final sync; no effect escapes on a prefix.
    Enum.reduce_while(Enum.chunk_every(events, 1024), {:ok, nil}, fn batch, {:ok, previous} ->
      case WAL.append(owner, batch) do
        {:ok, reference} -> {:cont, {:ok, reference}}
        {:error, {:io, _, _}} -> {:halt, {:error, :unknown_outcome}}
        {:error, {:unknown_outcome, _, _}} -> {:halt, {:error, :unknown_outcome}}
        _ when not is_nil(previous) -> {:halt, {:error, :unknown_outcome}}
        # Nothing was written: the caller may compact and retry.
        {:error, :log_limit} -> {:halt, {:error, :log_limit}}
        _ -> {:halt, {:error, :persistence_failed}}
      end
    end)
  end

  def close(nil), do: :ok

  def close(persistence) do
    if Process.alive?(persistence.owner), do: WAL.close(persistence.owner), else: :ok
  end

  defp recover(owner, node, address, options) do
    initial = recovery(node, address, options)

    case WAL.fold(owner, :origin, initial, fn _ref, event, acc ->
           case replay(event, acc) do
             {:ok, machine} -> {:cont, machine}
             {:error, _} = error -> {:halt, error}
           end
         end) do
      {:ok, ^initial} -> initialize(owner, node, address, options)
      {:ok, {:error, _} = error} -> error
      {:ok, %State{} = machine} -> {:ok, machine}
      _ -> {:error, :queue_recovery_failed}
    end
  end

  @doc false
  def recovery(node, address, options), do: {:initial, node, address, options}

  defp initialize(owner, node, address, options) do
    incarnation = :crypto.strong_rand_bytes(24)

    with {:ok, machine} <- new(node, incarnation, options),
         event = initial_event(machine, address),
         {:ok, _persistence} <- commit(%{owner: owner, durable: :none}, [event]) do
      {:ok, machine}
    end
  end

  @doc false
  def replay(event, {:initial, node, address, options}) do
    with %Event{type: "queue_initialized/v1", data: %{"incarnation" => incarnation}} <- event,
         {:ok, machine} <- new(node, incarnation, options),
         true <- event == initial_event(machine, address) do
      {:ok, machine}
    else
      _ -> {:error, :queue_identity_or_profile_mismatch}
    end
  end

  def replay(
        %Event{type: @snapshot} = event,
        {:snapshot, machine, sequence, count, received, chunks}
      ) do
    with %{"index" => ^received, "count" => ^count, "bytes" => chunk} when is_binary(chunk) <-
           event.data,
         true <- event == snapshot_event(machine, sequence, received, count, chunk) do
      snapshot_chunk(machine, sequence, count, received + 1, [chunk | chunks])
    else
      _ -> {:error, :invalid_queue_history}
    end
  end

  def replay(_event, {:snapshot, _, _, _, _, _}), do: {:error, :invalid_queue_history}

  # A snapshot is only valid as the base of a compacted log, directly after the
  # identity event, i.e. while the machine is still pristine.
  def replay(%Event{type: @snapshot} = event, %State{} = machine) do
    with true <- pristine?(machine),
         %{"index" => 0, "count" => count, "bytes" => chunk}
         when is_integer(count) and count > 0 and is_binary(chunk) <- event.data,
         true <- event == snapshot_event(machine, event.sequence, 0, count, chunk) do
      snapshot_chunk(machine, event.sequence, count, 1, [chunk])
    else
      _ -> {:error, :invalid_queue_history}
    end
  end

  def replay(%Event{} = event, machine) do
    with {:ok, command} <- command(event, machine),
         {next, effects} <- Machine.apply(machine, command),
         [generated] <- for({:event, item} <- effects, do: item),
         true <- equivalent?(generated, event) do
      {:ok, next}
    else
      _ -> {:error, :invalid_queue_history}
    end
  rescue
    _ -> {:error, :invalid_queue_history}
  end

  defp new(node, incarnation, options),
    do:
      Machine.new(
        NodeId.new(node),
        options |> Keyword.put(:incarnation, incarnation) |> Keyword.put(:profile, :local_durable)
      )

  @doc false
  def initial_event(machine, address) do
    %Event{
      id: EventId.new(<<"queue-init/v1:", machine.node_id.value::binary>>),
      node_id: machine.node_id,
      sequence: 0,
      type: "queue_initialized/v1",
      data: %{
        "incarnation" => machine.incarnation,
        "address" => address,
        "version" => 1,
        "limits" => Map.new(machine.limits, fn {key, value} -> {Atom.to_string(key), value} end)
      }
    }
  end

  defp command(%Event{type: "message_enqueued", data: data}, _machine),
    do: {:ok, {:enqueue, :recovery, message(data)}}

  defp command(%Event{type: "consumer_registered", data: data}, _),
    do: {:ok, {:register_consumer, :recovery, data["consumer_id"], data["generation"]}}

  defp command(%Event{type: "consumer_disconnected", data: data}, _),
    do: {:ok, {:disconnect, :recovery, data["consumer_id"], data["generation"]}}

  defp command(%Event{type: "delivery_claimed", data: data}, machine) do
    entry = Map.fetch!(machine.messages, MessageId.new(data["message_id"]))
    {:ok, {:claim, :recovery, data["consumer_id"], data["generation"], 1, entry.cost}}
  end

  defp command(%Event{type: type, data: data}, _)
       when type in [
              "message_retired",
              "message_rejected",
              "message_released",
              "message_modified"
            ] do
    outcome =
      case type do
        "message_retired" ->
          %Outcome.Retire{}

        "message_released" ->
          %Outcome.MakeAvailable{}

        "message_rejected" ->
          true = data["action"] == "discard"

          %Outcome.Reject{
            reason: data["reason"],
            description: data["description"],
            details: mutation(data["details"]),
            action: :discard
          }

        "message_modified" ->
          %Outcome.ModifyAndMakeAvailable{
            delivery_failed?: data["delivery_failed"],
            undeliverable_here?: data["undeliverable_here"],
            annotation_mutation: mutation(data["annotation_mutation"])
          }
      end

    {:ok,
     {:settle, :recovery, data["consumer_id"], data["generation"],
      DeliveryRef.new(data["delivery_ref"]), outcome}}
  end

  defp command(_, _), do: {:error, :event_type}

  defp message(data) do
    %{
      "message_id" => id,
      "encoded_content" => raw,
      "wire_format" => %{"family" => family, "identifier" => identifier},
      "durable" => durable,
      "index" => %{
        "routing" => routing,
        "policy" => policy,
        "expires_at" => expiry,
        "durability" => durability,
        "management" => management
      }
    } = data

    %Message{
      id: MessageId.new(id),
      encoded_content: raw,
      durable?: durable,
      wire_format: %Message.Format{family: family, identifier: identifier},
      index: %Message.Index{
        routing: routing,
        policy: policy,
        expires_at: expiry,
        durability: durability,
        management: management
      }
    }
  end

  defp mutation(nil), do: nil

  defp mutation(%{"format" => format, "encoded_data" => bytes}),
    do: %Message.Mutation{format: format, encoded_data: bytes}

  defp equivalent?(
         %Event{type: "message_released"} = generated,
         %Event{data: %{"cause" => cause}} = event
       )
       when cause in ["release", "disconnect", "generation_replaced"] do
    %{generated | data: Map.put(generated.data, "cause", cause)} == event
  end

  defp equivalent?(generated, event), do: generated == event

  defp release_leases(machine) do
    result =
      machine.consumers
      |> Enum.sort()
      |> Enum.reduce_while({:ok, machine, []}, fn {consumer, generation},
                                                  {:ok, machine, batches} ->
        {machine, effects} =
          Machine.apply(machine, {:disconnect, :recovery, consumer, generation})

        if {:operation, :recovery, {:ok, :disconnected}} in effects do
          {:cont, {:ok, machine, [for({:event, event} <- effects, do: event) | batches]}}
        else
          {:halt, {:error, :queue_recovery_failed}}
        end
      end)

    case result do
      {:ok, machine, batches} -> {:ok, machine, batches |> Enum.reverse() |> List.flatten()}
      error -> error
    end
  end

  @doc false
  def compaction_events(%State{} = machine, address, max_record_bytes) do
    bytes = :erlang.term_to_binary(snapshot(machine), [:deterministic])
    # Each chunk event adds a bounded envelope (identities, type and field
    # names) to its bytes. The WAL still measures and rejects any record that
    # does not fit, so an underestimate fails explicitly.
    size = max_record_bytes - 256 - byte_size(machine.node_id.value)

    if size > 0 do
      chunks = chunks(bytes, size, [])
      count = length(chunks)

      events =
        chunks
        |> Enum.with_index()
        |> Enum.map(fn {chunk, index} ->
          snapshot_event(machine, machine.event_sequence, index, count, chunk)
        end)

      {:ok, [initial_event(machine, address) | events]}
    else
      {:error, :record_limit}
    end
  end

  defp chunks(bytes, size, acc) when byte_size(bytes) <= size, do: Enum.reverse([bytes | acc])

  defp chunks(bytes, size, acc) do
    <<chunk::binary-size(size), rest::binary>> = bytes
    chunks(rest, size, [chunk | acc])
  end

  defp snapshot_event(machine, sequence, index, count, chunk) do
    %Event{
      id: EventId.new(<<"queue-snapshot/v1:", sequence::64, index::32>>),
      node_id: machine.node_id,
      sequence: sequence,
      type: @snapshot,
      data: %{"index" => index, "count" => count, "bytes" => chunk}
    }
  end

  defp snapshot_chunk(machine, sequence, count, received, chunks) when received < count,
    do: {:ok, {:snapshot, machine, sequence, count, received, chunks}}

  defp snapshot_chunk(machine, sequence, _count, _received, chunks) do
    case restore(machine, chunks |> Enum.reverse() |> IO.iodata_to_binary()) do
      {:ok, %State{event_sequence: ^sequence} = restored} -> {:ok, restored}
      _ -> {:error, :invalid_queue_history}
    end
  end

  defp pristine?(machine) do
    machine.event_sequence == 0 and machine.delivery_sequence == 0 and
      machine.last_registration_generation == -1 and map_size(machine.messages) == 0 and
      map_size(machine.consumers) == 0
  end

  # Ready messages first in ready order, then claimed messages by identity.
  defp snapshot(machine) do
    ready = :queue.to_list(machine.ready)
    claimed = for {id, %{status: :claimed}} <- machine.messages, do: id

    %{
      "version" => 1,
      "event_sequence" => machine.event_sequence,
      "delivery_sequence" => machine.delivery_sequence,
      "last_registration_generation" => machine.last_registration_generation,
      "consumers" => machine.consumers |> Enum.sort() |> Enum.map(fn {id, g} -> [id, g] end),
      "claims" =>
        machine.claims
        |> Enum.sort_by(fn {ref, _claim} -> ref.value end)
        |> Enum.map(fn {ref, claim} ->
          [ref.value, claim.message_id.value, claim.consumer_id, claim.generation]
        end),
      "messages" =>
        Enum.map(ready ++ Enum.sort_by(claimed, & &1.value), fn id ->
          entry_data(Map.fetch!(machine.messages, id))
        end)
    }
  end

  defp entry_data(entry) do
    %{
      "message" => message_data(entry.message),
      "cost" => entry.cost,
      "mutation_bytes" => entry.mutation_bytes,
      "mutations" => Enum.map(entry.mutations, &[&1.format, &1.encoded_data]),
      "attempt" => entry.attempt,
      "failure_count" => entry.failure_count,
      "delivery_failed" => entry.delivery_failed?,
      "undeliverable_here" => entry.undeliverable_here?,
      "exclusions" => entry.exclusions |> Enum.sort() |> Enum.map(fn {c, g} -> [c, g] end),
      "status" => Atom.to_string(entry.status)
    }
  end

  defp message_data(message) do
    %{
      "message_id" => message.id.value,
      "encoded_content" => message.encoded_content,
      "wire_format" => %{
        "family" => message.wire_format.family,
        "identifier" => message.wire_format.identifier
      },
      "durable" => message.durable?,
      "index" => %{
        "routing" => message.index.routing,
        "policy" => message.index.policy,
        "expires_at" => message.index.expires_at,
        "durability" => message.index.durability,
        "management" => message.index.management
      }
    }
  end

  # Rebuilds only the dynamic fields onto the pristine machine that already
  # carries the verified identity, incarnation, profile and limits. Compressed
  # ETF is refused; decoding never creates atoms.
  defp restore(template, <<131, tag, _::binary>> = bytes) when tag != 80 do
    {term, used} = :erlang.binary_to_term(bytes, [:safe, :used])
    true = used == byte_size(bytes)

    %{
      "version" => 1,
      "event_sequence" => events,
      "delivery_sequence" => deliveries,
      "last_registration_generation" => generation,
      "consumers" => consumer_data,
      "claims" => claim_data,
      "messages" => entry_data
    } = term

    true = uint?(events) and uint?(deliveries) and (generation == -1 or uint?(generation))

    consumers =
      Map.new(consumer_data, fn [id, g] when is_binary(id) ->
        true = uint?(g) and g <= generation
        {id, g}
      end)

    entries = Enum.map(entry_data, &entry(&1, consumers))
    messages = Map.new(entries, &{&1.message.id, &1})
    ready = for %{status: :ready} = entry <- entries, do: entry.message.id

    claims =
      Map.new(claim_data, fn [ref, id, consumer, g] when is_binary(ref) ->
        id = MessageId.new(id)
        %{status: :claimed} = Map.fetch!(messages, id)
        ^g = Map.fetch!(consumers, consumer)
        {DeliveryRef.new(ref), %{message_id: id, consumer_id: consumer, generation: g}}
      end)

    claimed = MapSet.new(Map.values(claims), & &1.message_id)
    bytes = entries |> Enum.map(& &1.cost) |> Enum.sum()
    limits = template.limits

    true =
      map_size(messages) == length(entries) and map_size(claims) == length(claim_data) and
        MapSet.size(claimed) == map_size(claims) and
        map_size(claims) == length(entries) - length(ready) and
        map_size(consumers) == length(consumer_data) and
        map_size(messages) <= limits.max_messages and bytes <= limits.max_bytes and
        map_size(consumers) <= limits.max_consumers and map_size(claims) <= limits.max_inflight

    {:ok,
     %State{
       template
       | messages: messages,
         ready: :queue.from_list(ready),
         ready_count: length(ready),
         claims: claims,
         consumers: consumers,
         message_count: map_size(messages),
         bytes: bytes,
         event_sequence: events,
         delivery_sequence: deliveries,
         last_registration_generation: generation
     }}
  rescue
    _ -> {:error, :invalid_queue_history}
  end

  defp restore(_template, _bytes), do: {:error, :invalid_queue_history}

  defp entry(data, consumers) do
    %{
      "message" => message,
      "cost" => cost,
      "mutation_bytes" => mutation_bytes,
      "mutations" => mutations,
      "attempt" => attempt,
      "failure_count" => failures,
      "delivery_failed" => delivery_failed,
      "undeliverable_here" => undeliverable_here,
      "exclusions" => exclusions,
      "status" => status
    } = data

    true =
      Enum.all?([cost, mutation_bytes, attempt, failures], &uint?/1) and
        is_boolean(delivery_failed) and is_boolean(undeliverable_here) and
        status in ["ready", "claimed"]

    %{
      message: message(message),
      cost: cost,
      mutation_bytes: mutation_bytes,
      mutations:
        Enum.map(mutations, fn [format, bytes] when is_binary(format) and is_binary(bytes) ->
          %Message.Mutation{format: format, encoded_data: bytes}
        end),
      attempt: attempt,
      failure_count: failures,
      delivery_failed?: delivery_failed,
      undeliverable_here?: undeliverable_here,
      exclusions:
        MapSet.new(exclusions, fn [consumer, g] ->
          true = Map.get(consumers, consumer) == g
          {consumer, g}
        end),
      status: if(status == "ready", do: :ready, else: :claimed)
    }
  end

  defp uint?(value), do: is_integer(value) and value >= 0 and value <= @max_counter
end
