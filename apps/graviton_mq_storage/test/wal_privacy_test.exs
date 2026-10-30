defmodule GravitonMQ.Storage.WALPrivacyTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.Storage.WAL

  test "fault callback exceptions poison the writer without logging record content" do
    path =
      Path.join(System.tmp_dir!(), "graviton-wal-private-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(path) end)

    fault = fn
      :append_write, _ -> raise "private callback data"
      _, _ -> :ok
    end

    {:ok, wal} =
      WAL.start_link(
        path: path,
        stream_id: "private-stream",
        mode: :opaque,
        fault_injector: fault
      )

    on_exit(fn -> if Process.alive?(wal), do: WAL.close(wal) end)

    assert {:error, {:io, :append_write, :fault_callback_failed}} =
             WAL.append_records(wal, [{"secret/v1", "private-payload"}])

    assert {:error, {:poisoned, {:io, :append_write, :fault_callback_failed}}} =
             WAL.durable_through(wal)

    status = inspect(:sys.get_status(wal))
    refute status =~ "private-payload"
    refute status =~ "private-stream"
    refute status =~ "private callback data"
    assert Process.alive?(wal)
    assert :ok = WAL.close(wal)
  end
end
