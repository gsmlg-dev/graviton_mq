defmodule GravitonMQ.Runtime.MQTT.Persistence do
  @moduledoc false
  # Every committed change appends a full broker snapshot and recovery uses only
  # the latest one. Once the log passes half its limit (and has at least doubled
  # since the last compaction), it is compacted to the latest committed
  # snapshot. An append rejected by the log limit compacts and retries once;
  # if the snapshot still does not fit, the commit fails explicitly.
  alias GravitonMQ.Core.{CheckpointCodec, CommitRef}
  alias GravitonMQ.MQTT.Broker.Checkpoint
  alias GravitonMQ.Storage.WAL

  @record "mqtt-broker-snapshot/v1"

  def open(nil, machine, _now, _wall), do: {:ok, nil, machine}

  def open(config, machine, now, wall) do
    options = [
      path: config.directory,
      stream_id: config.stream_id,
      mode: :opaque,
      max_record_bytes: config.max_snapshot_bytes + 64,
      max_log_bytes: config.max_log_bytes,
      fault_injector: config.fault_injector
    ]

    case WAL.start_link(options) do
      {:ok, owner} ->
        case recover(owner, config, machine, now, wall) do
          {:ok, persistence, machine} ->
            {:ok, persistence, machine}

          {:error, reason} ->
            WAL.close(owner)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, {:mqtt_persistence_open, reason}}
    end
  end

  defp recover(owner, config, template, now, wall) do
    with {:ok, latest} <-
           WAL.fold_records(owner, :origin, nil, fn
             _reference, @record, bytes, _previous -> {:cont, bytes}
             _reference, _type, _bytes, _previous -> {:error, :checkpoint_record_type}
           end),
         {:ok, machine} <- restore(latest, template, config, now, wall) do
      persistence = %{owner: owner, config: config, saved: nil, saved_bytes: latest, durable: nil}

      # A log left at or over the threshold (possibly at its hard limit) is
      # compacted before the recovery snapshot is appended.
      with {:ok, persistence} <- maybe_compact(persistence),
           {:ok, persistence} <- commit(persistence, machine, now, wall) do
        {:ok, persistence, machine}
      else
        {:error, reason} -> {:error, {:mqtt_persistence_recovery, failure(reason)}}
      end
    end
  end

  defp restore(nil, template, _config, _now, _wall), do: {:ok, template}

  defp restore(bytes, template, config, now, wall) do
    # Load the role's closed schema atom set before safe ETF decoding in a new VM.
    # Unknown atoms in data are still rejected; decoding never creates atoms.
    with true <- Code.ensure_loaded?(Checkpoint),
         {:ok, snapshot} <- CheckpointCodec.decode(bytes, codec_options(config)),
         {:ok, machine} <- Checkpoint.restore(template, snapshot, now, wall) do
      {:ok, machine}
    else
      false -> {:error, :checkpoint_schema_unavailable}
      error -> error
    end
  end

  def commit(nil, _machine, _now, _wall), do: {:ok, nil}

  def commit(persistence, machine, now, wall) do
    with {:ok, snapshot} <- Checkpoint.capture(machine, now, wall) do
      if snapshot.state == persistence.saved do
        {:ok, persistence}
      else
        append(persistence, snapshot)
      end
    else
      {:error, _} -> {:error, :persistence_failed}
    end
  catch
    :exit, _ -> {:error, :unknown_outcome}
  end

  defp append(persistence, snapshot) do
    with {:ok, bytes} <- CheckpointCodec.encode(snapshot, codec_options(persistence.config)),
         {:ok, persistence, reference} <- append_record(persistence, bytes) do
      case WAL.sync(persistence.owner, reference) do
        {:ok, %CommitRef{stream_id: stream, position: position} = durable}
        when stream == reference.stream_id and position >= reference.position ->
          # The snapshot is durable. A failed or uncertain compaction still
          # poisons the log, so it is reported rather than hidden.
          %{persistence | saved: snapshot.state, saved_bytes: bytes, durable: durable}
          |> maybe_compact()
          |> case do
            {:ok, persistence} -> {:ok, persistence}
            {:error, reason} -> {:error, failure(reason)}
          end

        _failure ->
          {:error, :unknown_outcome}
      end
    else
      {:error, reason} -> {:error, failure(reason)}
    end
  end

  # The log limit rejects an append before writing anything, so compacting to
  # the latest committed snapshot and retrying once cannot duplicate a record.
  defp append_record(persistence, bytes) do
    case WAL.append_records(persistence.owner, [{@record, bytes}]) do
      {:ok, reference} ->
        {:ok, persistence, reference}

      {:error, :log_limit} when is_binary(persistence.saved_bytes) ->
        with {:ok, persistence} <- compact(persistence),
             {:ok, reference} <- WAL.append_records(persistence.owner, [{@record, bytes}]) do
          {:ok, persistence, reference}
        end

      {:error, _} = error ->
        error
    end
  end

  defp maybe_compact(%{saved_bytes: nil} = persistence), do: {:ok, persistence}

  defp maybe_compact(persistence) do
    with {:ok, usage} <- WAL.usage(persistence.owner) do
      if compact?(usage), do: compact(persistence), else: {:ok, persistence}
    end
  end

  defp compact?(%{bytes: bytes, base_bytes: base, limit: limit}),
    do: bytes >= div(limit, 2) and bytes >= 2 * base

  defp compact(persistence) do
    case WAL.compact_records(persistence.owner, [{@record, persistence.saved_bytes}]) do
      {:ok, %CommitRef{} = durable} -> {:ok, %{persistence | durable: durable}}
      {:error, _} = error -> error
    end
  end

  defp failure(:unknown_outcome), do: :unknown_outcome
  defp failure({:io, _, _}), do: :unknown_outcome
  defp failure({:unknown_outcome, _, _}), do: :unknown_outcome
  defp failure({:poisoned, _}), do: :unknown_outcome
  defp failure(_), do: :persistence_failed

  def close(nil), do: :ok

  def close(persistence) do
    if Process.alive?(persistence.owner), do: WAL.close(persistence.owner), else: :ok
  catch
    :exit, _ -> :ok
  end

  defp codec_options(config), do: [max_bytes: config.max_snapshot_bytes, max_items: 500_000]
end
