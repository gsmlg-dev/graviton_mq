defmodule GravitonMQ.Runtime.AMQP10.Persistence do
  @moduledoc false

  alias GravitonMQ.Core.{CommitRef, DeliveryRef, Message, MessageId, NodeId, Outcome}
  alias GravitonMQ.Queue.{Event, EventId, Machine}
  alias GravitonMQ.Storage.WAL

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
      persistence = %{owner: owner, durable: :none}
      result = recover(owner, node, address, options)

      case result do
        {:ok, machine} ->
          {:ok, durable} = WAL.durable_through(owner)
          persistence = %{persistence | durable: durable}

          with {:ok, machine, events} <- release_leases(machine),
               {:ok, persistence} <- commit(persistence, events) do
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

  def commit(nil, _events), do: {:ok, nil}
  def commit(persistence, []), do: {:ok, persistence}

  def commit(persistence, events) do
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

  defp append_batches(owner, events) do
    # A disconnect/recovery can release more than one physical append batch.
    # Append every chunk before one final sync; no effect escapes on a prefix.
    Enum.reduce_while(Enum.chunk_every(events, 1024), {:ok, nil}, fn batch, {:ok, previous} ->
      case WAL.append(owner, batch) do
        {:ok, reference} -> {:cont, {:ok, reference}}
        {:error, {:io, _, _}} -> {:halt, {:error, :unknown_outcome}}
        {:error, {:unknown_outcome, _, _}} -> {:halt, {:error, :unknown_outcome}}
        _ when not is_nil(previous) -> {:halt, {:error, :unknown_outcome}}
        _ -> {:halt, {:error, :persistence_failed}}
      end
    end)
  end

  def close(nil), do: :ok

  def close(persistence) do
    if Process.alive?(persistence.owner), do: WAL.close(persistence.owner), else: :ok
  end

  defp recover(owner, node, address, options) do
    initial = {:initial, node, address, options}

    case WAL.fold(owner, :origin, initial, fn _ref, event, acc ->
           case replay(event, acc) do
             {:ok, machine} -> {:cont, machine}
             {:error, _} = error -> {:halt, error}
           end
         end) do
      {:ok, ^initial} -> initialize(owner, node, address, options)
      {:ok, {:error, _} = error} -> error
      {:ok, machine} -> {:ok, machine}
      _ -> {:error, :queue_recovery_failed}
    end
  end

  defp initialize(owner, node, address, options) do
    incarnation = :crypto.strong_rand_bytes(24)

    with {:ok, machine} <- new(node, incarnation, options),
         event = initial_event(machine, address),
         {:ok, _persistence} <- commit(%{owner: owner, durable: :none}, [event]) do
      {:ok, machine}
    end
  end

  defp replay(event, {:initial, node, address, options}) do
    with %Event{type: "queue_initialized/v1", data: %{"incarnation" => incarnation}} <- event,
         {:ok, machine} <- new(node, incarnation, options),
         true <- event == initial_event(machine, address) do
      {:ok, machine}
    else
      _ -> {:error, :queue_identity_or_profile_mismatch}
    end
  end

  defp replay(%Event{} = event, machine) do
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

  defp initial_event(machine, address) do
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

  defp command(%Event{type: "message_enqueued", data: data}, _machine) do
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

    message = %Message{
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

    {:ok, {:enqueue, :recovery, message}}
  end

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
end
