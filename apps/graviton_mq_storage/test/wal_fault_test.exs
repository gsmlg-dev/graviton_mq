defmodule GravitonMQ.Storage.WALFaultTest do
  use ExUnit.Case, async: true
  @moduletag :capture_log

  alias GravitonMQ.Core.CommitRef
  alias GravitonMQ.Storage.WAL

  setup do
    Process.flag(:trap_exit, true)

    parent =
      Path.join(System.tmp_dir!(), "graviton-wal-fault-#{System.unique_integer([:positive])}")

    File.mkdir_p!(parent)
    table = :ets.new(:wal_faults, [:public])
    on_exit(fn -> File.rm_rf!(parent) end)

    fault = fn stage, _ ->
      case :ets.take(table, stage) do
        [{^stage, result}] -> result
        [] -> :ok
      end
    end

    {:ok, path: Path.join(parent, "log"), table: table, fault: fault}
  end

  test "reopen establishes a fresh real synchronization before exposing a recovered boundary", %{
    path: path
  } do
    assert {:ok, wal} = open(path)
    assert {:ok, ref} = WAL.append_records(wal, [{"snapshot/v1", "data"}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert :ok = WAL.close(wal)

    fault = fn
      :wal_sync, _ -> {:error, :injected_sync_failure}
      _, _ -> :ok
    end

    assert {:error, {:io, :wal_sync, :injected_sync_failure}} = open(path, fault_injector: fault)
    assert {:ok, wal} = open(path)
    assert {:ok, ^ref} = WAL.durable_through(wal)
    assert :ok = WAL.close(wal)
  end

  for stage <- [
        :wal_sync,
        :checkpoint_write,
        :checkpoint_sync,
        :checkpoint_rename,
        :directory_sync
      ] do
    test "#{stage} failure is explicit and poisons all durability/mutation operations", %{
      path: path,
      table: table,
      fault: fault
    } do
      assert {:ok, wal} = open(path, fault_injector: fault)
      assert {:ok, ref} = WAL.append_records(wal, [{"snapshot/v1", "saved"}])
      :ets.insert(table, {unquote(stage), {:error, :injected_eio}})
      assert {:error, {:io, unquote(stage), :injected_eio} = failure} = WAL.sync(wal, ref)
      assert {:error, {:poisoned, ^failure}} = WAL.durable_through(wal)
      assert {:error, {:poisoned, ^failure}} = WAL.sync(wal, ref)
      assert {:error, {:poisoned, ^failure}} = WAL.append_records(wal, [{"snapshot/v1", "next"}])
      assert :ok = WAL.close(wal)
      assert {:ok, recovered} = open(path)
      assert {:ok, ^ref} = WAL.durable_through(recovered)
      assert {:ok, ["saved"]} = records(recovered)
      assert :ok = WAL.close(recovered)
    end
  end

  test "unknown post-sync result never becomes a successful ACK", %{
    path: path,
    table: table,
    fault: fault
  } do
    assert {:ok, wal} = open(path, fault_injector: fault)
    assert {:ok, ref} = WAL.append_records(wal, [{"snapshot/v1", "possibly-durable"}])
    :ets.insert(table, {:wal_sync, {:unknown, :lost_completion}})

    assert {:error, {:unknown_outcome, :wal_sync, :lost_completion} = failure} =
             WAL.sync(wal, ref)

    assert {:error, {:poisoned, ^failure}} = WAL.durable_through(wal)
    assert :ok = WAL.close(wal)
    assert {:ok, recovered} = open(path)
    assert {:ok, ^ref} = WAL.durable_through(recovered)
    assert {:ok, ["possibly-durable"]} = records(recovered)
    assert :ok = WAL.close(recovered)
  end

  test "a partial unsynchronized record is truncated without losing the durable prefix", %{
    path: path,
    table: table,
    fault: fault
  } do
    assert {:ok, wal} = open(path, fault_injector: fault)
    assert {:ok, ref} = WAL.append_records(wal, [{"snapshot/v1", "durable"}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    size = File.stat!(Path.join(path, "events.wal")).size
    :ets.insert(table, {:append_write, {:partial, 23}})

    assert {:error, {:io, :append_write, :partial_write} = reason} =
             WAL.append_records(wal, [{"snapshot/v1", "incomplete"}])

    assert {:error, {:poisoned, ^reason}} = WAL.durable_through(wal)
    assert File.stat!(Path.join(path, "events.wal")).size == size + 23
    assert :ok = WAL.close(wal)
    assert {:ok, recovered} = open(path)
    assert File.stat!(Path.join(path, "events.wal")).size == size
    assert {:ok, ^ref} = WAL.durable_through(recovered)
    assert {:ok, ["durable"]} = records(recovered)

    assert {:ok, %CommitRef{position: 2}} =
             WAL.append_records(recovered, [{"snapshot/v1", "replacement"}])

    assert :ok = WAL.close(recovered)
  end

  test "a fully written record with failed completion may survive as unknown outcome", %{
    path: path,
    table: table,
    fault: fault
  } do
    assert {:ok, wal} = open(path, fault_injector: fault)
    :ets.insert(table, {:append_write, {:partial, 1_000_000}})

    assert {:error, {:io, :append_write, :partial_write}} =
             WAL.append_records(wal, [{"snapshot/v1", "survives"}])

    assert :ok = WAL.close(wal)
    assert {:ok, recovered} = open(path)
    assert {:ok, %CommitRef{position: 1}} = WAL.durable_through(recovered)
    assert {:ok, ["survives"]} = records(recovered)
    assert :ok = WAL.close(recovered)
  end

  test "durable-prefix truncation is rejected and never repaired", %{path: path} do
    assert {:ok, wal} = open(path)
    assert {:ok, ref} = WAL.append_records(wal, [{"snapshot/v1", "confirmed"}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert :ok = WAL.close(wal)
    filename = Path.join(path, "events.wal")
    bytes = File.read!(filename)
    truncated = binary_part(bytes, 0, byte_size(bytes) - 1)
    File.write!(filename, truncated)
    assert {:error, :durable_prefix_or_log_limit} = open(path)
    assert File.read!(filename) == truncated
  end

  test "complete payload corruption is rejected, including unsynchronized records", %{path: path} do
    assert {:ok, wal} = open(path)
    assert {:ok, _} = WAL.append_records(wal, [{"snapshot/v1", "unconfirmed"}])
    assert :ok = WAL.close(wal)
    filename = Path.join(path, "events.wal")
    bytes = File.read!(filename)
    size = byte_size(bytes) - 1
    <<head::binary-size(size), final>> = bytes
    corrupt = <<head::binary, Bitwise.bxor(final, 1)>>
    File.write!(filename, corrupt)
    assert {:error, :record_checksum_or_payload} = open(path)
    assert File.read!(filename) == corrupt
  end

  test "header length corruption cannot masquerade as an incomplete tail", %{path: path} do
    assert {:ok, wal} = open(path)
    assert {:ok, _} = WAL.append_records(wal, [{"snapshot/v1", "unconfirmed"}])
    assert :ok = WAL.close(wal)
    filename = Path.join(path, "events.wal")
    bytes = File.read!(filename)
    header_size = 12 + byte_size("opaque/test")
    prefix_size = header_size + 4
    <<prefix::binary-size(prefix_size), _length::32, tail::binary>> = bytes
    corrupt = <<prefix::binary, 500_000::32, tail::binary>>
    File.write!(filename, corrupt)
    assert {:error, :record_header_checksum} = open(path)
    assert File.read!(filename) == corrupt
  end

  test "checkpoint corruption is explicit", %{path: path} do
    assert {:ok, wal} = open(path)
    assert :ok = WAL.close(wal)
    File.write!(Path.join(path, "durable.checkpoint"), <<0, 1, 2>>)
    assert {:error, :checkpoint_corrupt} = open(path)
  end

  test "streaming early halt does not read the next corrupted record", %{path: path} do
    assert {:ok, wal} = open(path)

    assert {:ok, _} =
             WAL.append_records(wal, [{"snapshot/v1", "first"}, {"snapshot/v1", "second"}])

    filename = Path.join(path, "events.wal")
    {:ok, fd} = :file.open(String.to_charlist(filename), [:raw, :binary, :read, :write])
    size = File.stat!(filename).size
    :ok = :file.pwrite(fd, size - 1, <<0>>)
    :ok = :file.close(fd)

    assert {:ok, ["first"]} =
             WAL.fold_records(wal, :origin, [], fn _, _, bytes, _ -> {:halt, [bytes]} end)

    assert {:error, :record_checksum_or_payload} = records(wal)
    assert :ok = WAL.close(wal)
  end

  test "limits reject an entire batch before writing its first record", %{path: path} do
    assert {:ok, wal} = open(path, max_record_bytes: 128, max_log_bytes: 128)

    assert {:error, :log_limit} =
             WAL.append_records(wal, [
               {"type", :binary.copy("x", 60)},
               {"type", :binary.copy("x", 60)}
             ])

    assert {:ok, []} = records(wal)
    assert {:ok, %CommitRef{position: 1}} = WAL.append_records(wal, [{"type", "fits"}])
    assert :ok = WAL.close(wal)
  end

  test "a completely truncated WAL cannot be mistaken for a brand-new stream", %{path: path} do
    assert {:ok, wal} = open(path)
    assert {:ok, ref} = WAL.append_records(wal, [{"snapshot/v1", "confirmed"}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert :ok = WAL.close(wal)
    checkpoint = File.read!(Path.join(path, "durable.checkpoint"))
    File.write!(Path.join(path, "events.wal"), <<>>)
    assert {:error, :durable_prefix_truncated} = open(path)
    assert File.read!(Path.join(path, "durable.checkpoint")) == checkpoint
  end

  defp open(path, options \\ []) do
    result = WAL.start_link([path: path, stream_id: "opaque/test", mode: :opaque] ++ options)

    case result do
      {:ok, owner} -> on_exit(fn -> if Process.alive?(owner), do: WAL.close(owner) end)
      _ -> :ok
    end

    result
  end

  defp records(wal),
    do: WAL.fold_records(wal, :origin, [], fn _, _, bytes, acc -> {:cont, acc ++ [bytes]} end)
end
