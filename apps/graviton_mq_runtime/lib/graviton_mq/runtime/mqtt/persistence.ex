defmodule GravitonMQ.Runtime.MQTT.Persistence do
  @moduledoc false
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
      persistence = %{owner: owner, config: config, saved: nil, durable: nil}

      case commit(persistence, machine, now, wall) do
        {:ok, persistence} -> {:ok, persistence, machine}
        {:error, reason} -> {:error, {:mqtt_persistence_recovery, reason}}
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
         {:ok, reference} <- WAL.append_records(persistence.owner, [{@record, bytes}]) do
      case WAL.sync(persistence.owner, reference) do
        {:ok, %CommitRef{stream_id: stream, position: position} = durable}
        when stream == reference.stream_id and position >= reference.position ->
          {:ok, %{persistence | saved: snapshot.state, durable: durable}}

        _failure ->
          {:error, :unknown_outcome}
      end
    else
      {:error, {:io, _, _}} -> {:error, :unknown_outcome}
      {:error, {:unknown_outcome, _, _}} -> {:error, :unknown_outcome}
      {:error, {:poisoned, _}} -> {:error, :unknown_outcome}
      {:error, _} -> {:error, :persistence_failed}
    end
  end

  def close(nil), do: :ok

  def close(persistence) do
    if Process.alive?(persistence.owner), do: WAL.close(persistence.owner), else: :ok
  catch
    :exit, _ -> :ok
  end

  defp codec_options(config), do: [max_bytes: config.max_snapshot_bytes, max_items: 500_000]
end
