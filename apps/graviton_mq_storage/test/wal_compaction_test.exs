defmodule GravitonMQ.Storage.WALCompactionTest do
  use ExUnit.Case, async: true
  @moduletag :capture_log

  alias GravitonMQ.Core.{CommitRef, NodeId}
  alias GravitonMQ.Queue.{Event, EventId}
  alias GravitonMQ.Storage.WAL

  @stream "opaque/compact"

  setup do
    Process.flag(:trap_exit, true)

    parent =
      Path.join(System.tmp_dir!(), "graviton-wal-compact-#{System.unique_integer([:positive])}")

    File.mkdir_p!(parent)
    table = :ets.new(:wal_compaction_faults, [:public])
    on_exit(fn -> File.rm_rf!(parent) end)

    fault = fn stage, _ ->
      case :ets.take(table, stage) do
        [{^stage, result}] -> result
        [] -> :ok
      end
    end

    {:ok, path: Path.join(parent, "log"), table: table, fault: fault}
  end

  test "compaction replaces the log with base items and keeps positions monotonic", %{
    path: path
  } do
    assert {:ok, wal} = open(path)
    assert {:ok, ref} = WAL.append_records(wal, for(n <- 1..5, do: {"snap", "old-#{n}"}))
    assert {:ok, %CommitRef{position: 5} = ^ref} = WAL.sync(wal, ref)
    assert {:ok, before} = WAL.usage(wal)

    assert {:ok, %CommitRef{position: 7} = compacted} =
             WAL.compact_records(wal, [{"snap", "base-1"}, {"snap", "base-2"}])

    assert {:ok, ^compacted} = WAL.durable_through(wal)
    assert {:ok, usage} = WAL.usage(wal)
    assert usage.bytes == usage.base_bytes
    assert usage.limit == before.limit
    refute File.exists?(Path.join(path, "events.wal"))
    assert File.exists?(Path.join(path, "events.1.wal"))

    assert {:ok, [{6, "base-1"}, {7, "base-2"}]} = positioned(wal)
    assert {:ok, later} = WAL.append_records(wal, [{"snap", "later"}])
    assert later.position == 8
    assert {:ok, ^later} = WAL.sync(wal, later)
    assert {:ok, [{6, "base-1"}, {7, "base-2"}, {8, "later"}]} = positioned(wal)

    assert {:ok, [{8, "later"}]} =
             WAL.fold_records(wal, CommitRef.new(@stream, 7), [], fn ref, _, bytes, acc ->
               {:cont, acc ++ [{ref.position, bytes}]}
             end)

    assert :ok = WAL.close(wal)
    assert {:ok, wal} = open(path)
    assert {:ok, ^later} = WAL.durable_through(wal)
    assert {:ok, [{6, "base-1"}, {7, "base-2"}, {8, "later"}]} = positioned(wal)
    assert {:ok, %{base_bytes: base}} = WAL.usage(wal)
    assert base == usage.base_bytes

    # Repeated compaction keeps advancing generations and positions.
    assert {:ok, %CommitRef{position: 9}} = WAL.compact_records(wal, [{"snap", "base-3"}])
    assert File.ls!(path) |> Enum.filter(&String.starts_with?(&1, "events")) == ["events.2.wal"]
    assert :ok = WAL.close(wal)
    assert {:ok, wal} = open(path)
    assert {:ok, %CommitRef{position: 9}} = WAL.durable_through(wal)
    assert {:ok, [{9, "base-3"}]} = positioned(wal)
    assert :ok = WAL.close(wal)
  end

  test "event logs compact to logical events and reject the opaque API", %{path: path} do
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "queue/compact")
    assert {:ok, ref} = WAL.append(wal, [event(1), event(2), event(3)])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert {:error, :stream_mode} = WAL.compact_records(wal, [{"snap", "x"}])
    assert {:ok, %CommitRef{position: 4}} = WAL.compact(wal, [event(3)])
    assert :ok = WAL.close(wal)
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "queue/compact")

    assert {:ok, [item]} =
             WAL.fold(wal, :origin, [], fn _, item, acc -> {:cont, [item | acc]} end)

    assert item == event(3)
    assert :ok = WAL.close(wal)
  end

  test "compaction refuses unsynchronized records without poisoning", %{path: path} do
    assert {:ok, wal} = open(path)
    assert {:ok, ref} = WAL.append_records(wal, [{"snap", "unsynced"}])
    assert {:error, :unsynced_records} = WAL.compact_records(wal, [{"snap", "base"}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert {:error, :empty_batch} = WAL.compact_records(wal, [])
    assert {:error, :invalid_record} = WAL.compact_records(wal, [:bad])
    assert {:ok, %CommitRef{position: 2}} = WAL.compact_records(wal, [{"snap", "base"}])
    assert :ok = WAL.close(wal)
  end

  test "a base that cannot fit the log limit is an explicit error and changes nothing", %{
    path: path
  } do
    assert {:ok, wal} = open(path, max_record_bytes: 256, max_log_bytes: 512)
    assert {:ok, ref} = WAL.append_records(wal, [{"snap", "kept"}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    big = for _ <- 1..3, do: {"snap", :binary.copy("x", 200)}
    assert {:error, :log_limit} = WAL.compact_records(wal, big)
    assert {:ok, ^ref} = WAL.durable_through(wal)
    assert {:ok, [{1, "kept"}]} = positioned(wal)
    assert {:ok, %CommitRef{position: 2}} = WAL.compact_records(wal, [{"snap", "fits"}])
    assert :ok = WAL.close(wal)
  end

  test "a log at its limit can be compacted and then accepts appends again", %{path: path} do
    assert {:ok, wal} = open(path, max_record_bytes: 256, max_log_bytes: 1024)
    item = {"snap", :binary.copy("y", 150)}
    {:ok, ref} = fill(wal, item)
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert {:error, :log_limit} = WAL.append_records(wal, [item])
    assert :ok = WAL.close(wal)

    assert {:ok, wal} = open(path, max_record_bytes: 256, max_log_bytes: 1024)
    assert {:error, :log_limit} = WAL.append_records(wal, [item])
    assert {:ok, compacted} = WAL.compact_records(wal, [item])
    assert compacted.position == ref.position + 1
    assert {:ok, next} = WAL.append_records(wal, [item])
    assert {:ok, ^next} = WAL.sync(wal, next)
    assert :ok = WAL.close(wal)
  end

  # Stages strictly before the checkpoint rename must recover the old log; a
  # rename that executed (even with an unknown completion) commits the new one.
  for {stage, fault, outcome} <- [
        {:compact_write, {:error, :injected_eio}, :old},
        {:compact_write, {:partial, 9}, :old},
        {:compact_sync, {:error, :injected_eio}, :old},
        {:compact_sync, {:unknown, :lost_completion}, :old},
        {:compact_directory_sync, {:error, :injected_eio}, :old},
        {:wal_sync, {:error, :injected_eio}, :old},
        {:checkpoint_write, {:error, :injected_eio}, :old},
        {:checkpoint_write, {:partial, 3}, :old},
        {:checkpoint_sync, {:error, :injected_eio}, :old},
        {:checkpoint_rename, {:error, :injected_eio}, :old},
        {:checkpoint_rename, {:unknown, :lost_completion}, :new},
        {:directory_sync, {:error, :injected_eio}, :new},
        {:directory_sync, {:unknown, :lost_completion}, :new}
      ] do
    test "compaction failure at #{stage} #{inspect(fault)} poisons and recovers #{outcome} state",
         %{path: path, table: table, fault: fault} do
      assert {:ok, wal} = open(path, fault_injector: fault)
      assert {:ok, ref} = WAL.append_records(wal, [{"snap", "old-1"}, {"snap", "old-2"}])
      assert {:ok, ^ref} = WAL.sync(wal, ref)
      :ets.insert(table, {unquote(stage), unquote(Macro.escape(fault))})
      assert {:error, failure} = WAL.compact_records(wal, [{"snap", "new"}])
      assert {:error, {:poisoned, ^failure}} = WAL.durable_through(wal)
      assert {:error, {:poisoned, ^failure}} = WAL.append_records(wal, [{"snap", "x"}])
      assert {:error, {:poisoned, ^failure}} = WAL.compact_records(wal, [{"snap", "x"}])
      assert :ok = WAL.close(wal)

      assert {:ok, recovered} = open(path)

      case unquote(outcome) do
        :old ->
          assert {:ok, ^ref} = WAL.durable_through(recovered)
          assert {:ok, [{1, "old-1"}, {2, "old-2"}]} = positioned(recovered)
          assert log_files(path) == ["events.wal"]

        :new ->
          assert {:ok, %CommitRef{position: 3}} = WAL.durable_through(recovered)
          assert {:ok, [{3, "new"}]} = positioned(recovered)
          assert log_files(path) == ["events.1.wal"]
      end

      assert {:ok, next} = WAL.append_records(recovered, [{"snap", "after"}])
      assert {:ok, ^next} = WAL.sync(recovered, next)
      assert :ok = WAL.close(recovered)
    end
  end

  test "failure to remove the retired generation after commit is not a compaction failure", %{
    path: path,
    table: table,
    fault: fault
  } do
    assert {:ok, wal} = open(path, fault_injector: fault)
    assert {:ok, ref} = WAL.append_records(wal, [{"snap", "old"}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    :ets.insert(table, {:compact_remove, {:error, :injected_eio}})

    assert {:ok, %CommitRef{position: 2} = compacted} =
             WAL.compact_records(wal, [{"snap", "new"}])

    assert log_files(path) == ["events.1.wal", "events.wal"]
    assert :ok = WAL.close(wal)
    assert {:ok, recovered} = open(path)
    assert {:ok, ^compacted} = WAL.durable_through(recovered)
    assert {:ok, [{2, "new"}]} = positioned(recovered)
    assert log_files(path) == ["events.1.wal"]
    assert :ok = WAL.close(recovered)
  end

  test "stale generation files are removed on open and never adopted", %{path: path} do
    assert {:ok, wal} = open(path)
    assert {:ok, ref} = WAL.append_records(wal, [{"snap", "live"}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert :ok = WAL.close(wal)
    File.write!(Path.join(path, "events.1.wal"), "stale uncommitted generation")
    File.write!(Path.join(path, "events.9.wal"), "stale uncommitted generation")
    assert {:ok, wal} = open(path)
    assert {:ok, [{1, "live"}]} = positioned(wal)
    assert log_files(path) == ["events.wal"]
    assert :ok = WAL.close(wal)
  end

  test "generation files without any checkpoint are not discarded", %{path: path} do
    File.mkdir_p!(path)
    File.write!(Path.join(path, "events.3.wal"), "possibly acknowledged")
    assert {:error, :checkpoint_corrupt} = open(path)
    assert File.read!(Path.join(path, "events.3.wal")) == "possibly acknowledged"
  end

  test "a checkpoint naming a missing generation is a truncated durable prefix", %{path: path} do
    assert {:ok, wal} = open(path)
    assert {:ok, ref} = WAL.append_records(wal, [{"snap", "old"}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert {:ok, _} = WAL.compact_records(wal, [{"snap", "new"}])
    assert :ok = WAL.close(wal)
    File.rm!(Path.join(path, "events.1.wal"))
    assert {:error, :durable_prefix_truncated} = open(path)
  end

  test "a log written in the original single-file format opens unchanged", %{path: path} do
    File.mkdir_p!(path)
    header = crc(<<"GMWL", 1, 1, byte_size(@stream)::16, @stream::binary>>)
    frames = legacy_frame(1, "snap", "first") <> legacy_frame(2, "snap", "second")
    log = header <> frames
    File.write!(Path.join(path, "events.wal"), log)

    checkpoint =
      crc(<<"GMCP", 1, 1, byte_size(@stream)::16, @stream::binary, 2::64, byte_size(log)::64>>)

    File.write!(Path.join(path, "durable.checkpoint"), checkpoint)
    assert {:ok, wal} = open(path)
    assert {:ok, %CommitRef{position: 2}} = WAL.durable_through(wal)
    assert {:ok, [{1, "first"}, {2, "second"}]} = positioned(wal)
    assert File.read!(Path.join(path, "events.wal")) == log
    assert File.read!(Path.join(path, "durable.checkpoint")) == checkpoint
    assert {:ok, ref} = WAL.append_records(wal, [{"snap", "third"}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert :ok = WAL.close(wal)

    expected = log <> legacy_frame(3, "snap", "third")
    assert File.read!(Path.join(path, "events.wal")) == expected

    assert File.read!(Path.join(path, "durable.checkpoint")) ==
             crc(
               <<"GMCP", 1, 1, byte_size(@stream)::16, @stream::binary, 3::64,
                 byte_size(expected)::64>>
             )
  end

  defp fill(wal, item, last \\ nil) do
    case WAL.append_records(wal, [item]) do
      {:ok, ref} -> fill(wal, item, ref)
      {:error, :log_limit} -> {:ok, last}
    end
  end

  defp open(path, options \\ []) do
    result = WAL.start_link([path: path, stream_id: @stream, mode: :opaque] ++ options)

    case result do
      {:ok, owner} -> on_exit(fn -> if Process.alive?(owner), do: WAL.close(owner) end)
      _ -> :ok
    end

    result
  end

  defp positioned(wal),
    do:
      WAL.fold_records(wal, :origin, [], fn ref, _, bytes, acc ->
        {:cont, acc ++ [{ref.position, bytes}]}
      end)

  defp log_files(path),
    do: path |> File.ls!() |> Enum.filter(&String.starts_with?(&1, "events")) |> Enum.sort()

  defp crc(body), do: <<body::binary, :erlang.crc32(body)::32>>

  defp legacy_frame(position, type, body) do
    payload = <<byte_size(type)::16, type::binary, body::binary>>
    head = <<"GMR1", byte_size(payload)::32, position::64>>
    <<head::binary, :erlang.crc32(head)::32, payload::binary, :erlang.crc32(payload)::32>>
  end

  defp event(sequence) do
    %Event{
      id: EventId.new("event-#{sequence}"),
      node_id: NodeId.new("queue/compact"),
      sequence: sequence,
      type: "compacted",
      data: %{"n" => sequence}
    }
  end
end
