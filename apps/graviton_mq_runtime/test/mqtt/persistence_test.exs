defmodule GravitonMQ.Runtime.MQTT.PersistenceTest do
  use ExUnit.Case, async: false
  @moduletag :tmp_dir
  @moduletag :capture_log

  alias GravitonMQ.MQTT.Broker, as: Machine
  alias GravitonMQ.Runtime.MQTT.Persistence
  alias GravitonMQ.Storage.WAL

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  # Each durable operation waits for a synchronous write; on a contended disk
  # (63-418 ms per write observed 2026-11-11/12) 60 s is too small.
  @tag timeout: 300_000
  test "repeated snapshots compact to the latest one instead of exhausting the log",
       %{tmp_dir: dir} do
    config = config(dir, 16_384)
    {:ok, machine} = Machine.new()
    assert {:ok, persistence, machine} = Persistence.open(config, machine, now(), wall())

    persistence =
      Enum.reduce(1..300, persistence, fn _, persistence ->
        # Force a new committed snapshot as if the broker state had changed.
        assert {:ok, persistence} =
                 Persistence.commit(%{persistence | saved: :changed}, machine, now(), wall())

        assert {:ok, usage} = WAL.usage(persistence.owner)
        assert usage.bytes <= usage.limit
        persistence
      end)

    assert persistence.durable.position > 300
    durable = persistence.durable
    assert :ok = Persistence.close(persistence)
    assert [log] = log_files(dir)
    assert [_, generation] = Regex.run(~r/\Aevents\.([1-9][0-9]*)\.wal\z/, log)
    # 300 snapshots in a 16 KiB log must compact more than once.
    assert String.to_integer(generation) >= 2

    assert {:ok, reopened, _machine} = Persistence.open(config, machine, now(), wall())
    assert reopened.durable.position > durable.position
    assert :ok = Persistence.close(reopened)
  end

  test "a broker whose log already reached its limit compacts on open", %{tmp_dir: dir} do
    config = config(dir, 1_048_576)
    {:ok, machine} = Machine.new()
    assert {:ok, persistence, machine} = Persistence.open(config, machine, now(), wall())

    persistence =
      Enum.reduce(1..40, persistence, fn _, persistence ->
        {:ok, persistence} =
          Persistence.commit(%{persistence | saved: :changed}, machine, now(), wall())

        persistence
      end)

    assert :ok = Persistence.close(persistence)
    size = File.stat!(Path.join(dir, "events.wal")).size
    assert size > config.max_snapshot_bytes + 64
    stuck = %{config | max_log_bytes: size}

    assert {:ok, reopened, _machine} = Persistence.open(stuck, machine, now(), wall())
    assert log_files(dir) == ["events.1.wal"]

    assert {:ok, reopened} =
             Persistence.commit(%{reopened | saved: :changed}, machine, now(), wall())

    assert :ok = Persistence.close(reopened)
  end

  test "a failed compaction is reported and never acknowledged", %{tmp_dir: dir} do
    control = :ets.new(:mqtt_compaction_fault, [:public])

    fault = fn stage, _ ->
      case :ets.lookup(control, stage) do
        [{^stage, result}] -> result
        [] -> :ok
      end
    end

    config = %{config(dir, 16_384) | fault_injector: fault}
    {:ok, machine} = Machine.new()
    assert {:ok, persistence, machine} = Persistence.open(config, machine, now(), wall())
    :ets.insert(control, {:compact_directory_sync, {:unknown, :lost_completion}})

    result =
      Enum.reduce_while(1..300, persistence, fn _, persistence ->
        case Persistence.commit(%{persistence | saved: :changed}, machine, now(), wall()) do
          {:ok, persistence} -> {:cont, persistence}
          {:error, reason} -> {:halt, {:failed, reason, persistence}}
        end
      end)

    assert {:failed, :unknown_outcome, persistence} = result
    assert :ok = Persistence.close(persistence)
    :ets.delete(control, :compact_directory_sync)
    assert {:ok, reopened, _machine} = Persistence.open(config, machine, now(), wall())
    assert :ok = Persistence.close(reopened)
  end

  @v1_fixture Path.expand(
                "../../../graviton_mq_mqtt/test/fixtures/broker_checkpoint_v1.etf",
                __DIR__
              )

  # The WAL record type stays "mqtt-broker-snapshot/v1" (owner decision Q9);
  # only the snapshot term carries the schema version.
  test "a v1 snapshot record from before S4 recovers and is rewritten as v2", %{tmp_dir: dir} do
    config = %{config(dir, 1_048_576) | max_snapshot_bytes: 65_536}

    {:ok, wal} =
      WAL.start_link(
        path: dir,
        stream_id: config.stream_id,
        mode: :opaque,
        max_record_bytes: config.max_snapshot_bytes + 64,
        max_log_bytes: config.max_log_bytes,
        fault_injector: nil
      )

    {:ok, reference} =
      WAL.append_records(wal, [{"mqtt-broker-snapshot/v1", File.read!(@v1_fixture)}])

    {:ok, _durable} = WAL.sync(wal, reference)
    :ok = WAL.close(wal)

    {:ok, machine} = Machine.new()
    assert {:ok, persistence, recovered} = Persistence.open(config, machine, now(), wall())

    assert %{subscription_identifier: nil, shared: nil} =
             recovered.sessions["reader"].subscriptions["fixture/#"]

    assert {:ok, %{version: 2}} =
             GravitonMQ.Core.CheckpointCodec.decode(persistence.saved_bytes,
               max_bytes: 65_536,
               max_items: 500_000
             )

    assert :ok = Persistence.close(persistence)
  end

  defp config(dir, max_log_bytes) do
    %{
      directory: dir,
      stream_id: "mqtt-persistence-test",
      max_snapshot_bytes: 4096,
      max_log_bytes: max_log_bytes,
      fault_injector: nil
    }
  end

  defp log_files(dir),
    do: dir |> File.ls!() |> Enum.filter(&String.starts_with?(&1, "events")) |> Enum.sort()

  defp now, do: System.monotonic_time(:millisecond)
  defp wall, do: System.system_time(:millisecond)
end
