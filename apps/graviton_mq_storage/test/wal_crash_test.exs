defmodule GravitonMQ.Storage.WALCrashTest do
  use ExUnit.Case, async: false
  @moduletag :capture_log

  alias GravitonMQ.Core.CommitRef
  alias GravitonMQ.Storage.WAL

  setup do
    Process.flag(:trap_exit, true)

    parent =
      Path.join(System.tmp_dir!(), "graviton-wal-crash-#{System.unique_integer([:positive])}")

    File.mkdir_p!(parent)
    on_exit(fn -> File.rm_rf!(parent) end)
    {:ok, path: Path.join(parent, "log")}
  end

  test "a second independent BEAM cannot acquire a live directory", %{path: path} do
    assert {:ok, wal} = open(path)
    alias_path = path <> "-alias"
    File.ln_s!(path, alias_path)

    for target <- [path, alias_path] do
      {output, status} =
        beam("""
        Process.flag(:trap_exit, true)
        case GravitonMQ.Storage.WAL.start_link(path: #{inspect(target)}, stream_id: "crash", mode: :opaque) do
          {:error, :already_open} -> IO.puts("EXCLUSIVE")
          other -> IO.inspect(other); :erlang.halt(2)
        end
        """)

      assert status == 0, output
      assert output =~ "EXCLUSIVE"
    end

    assert :ok = WAL.close(wal)
  end

  test "killing the owning process releases the kernel lock without a stale lockfile", %{
    path: path
  } do
    assert {:ok, wal} = open(path)
    assert {:ok, ref} = WAL.append_records(wal, [{"snapshot/v1", "confirmed"}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    Process.unlink(wal)
    monitor = Process.monitor(wal)
    Process.exit(wal, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^wal, :killed}
    assert {:ok, recovered} = open(path)
    assert {:ok, ^ref} = WAL.durable_through(recovered)
    assert :ok = WAL.close(recovered)
  end

  for synced <- [false, true] do
    test "VM SIGKILL after #{if synced, do: "sync", else: "append"} preserves complete records and releases ownership",
         %{path: path} do
      synced = unquote(synced)

      {output, status} =
        beam("""
        {:ok, wal} = GravitonMQ.Storage.WAL.start_link(path: #{inspect(path)}, stream_id: "crash", mode: :opaque)
        {:ok, ref} = GravitonMQ.Storage.WAL.append_records(wal, [{"snapshot/v1", "survived-vm"}])
        #{if synced, do: "{:ok, ^ref} = GravitonMQ.Storage.WAL.sync(wal, ref)", else: "_ = ref"}
        IO.puts("READY")
        System.cmd("kill", ["-KILL", System.pid()])
        """)

      assert status == 137, output
      assert output =~ "READY"
      assert {:ok, recovered} = open(path)
      assert {:ok, %CommitRef{position: 1}} = WAL.durable_through(recovered)

      assert {:ok, ["survived-vm"]} =
               WAL.fold_records(recovered, :origin, [], fn _, _, bytes, acc ->
                 {:cont, [bytes | acc]}
               end)

      assert :ok = WAL.close(recovered)
    end
  end

  test "a VM killed during a partial write repairs only the incomplete tail", %{path: path} do
    {output, status} =
      beam("""
      fault = fn
        :append_write, _ ->
          case Process.get(:first_write, true) do
            true -> Process.put(:first_write, false); :ok
            false -> {:partial, 21}
          end
        _, _ -> :ok
      end
      {:ok, wal} = GravitonMQ.Storage.WAL.start_link(path: #{inspect(path)}, stream_id: "crash", mode: :opaque, fault_injector: fault)
      {:ok, ref} = GravitonMQ.Storage.WAL.append_records(wal, [{"snapshot/v1", "durable-prefix"}])
      {:ok, ^ref} = GravitonMQ.Storage.WAL.sync(wal, ref)
      {:error, _} = GravitonMQ.Storage.WAL.append_records(wal, [{"snapshot/v1", "partial-tail"}])
      IO.puts("PARTIAL")
      System.cmd("kill", ["-KILL", System.pid()])
      """)

    assert status == 137, output
    assert output =~ "PARTIAL"
    assert {:ok, recovered} = open(path)
    assert {:ok, %CommitRef{position: 1}} = WAL.durable_through(recovered)

    assert {:ok, ["durable-prefix"]} =
             WAL.fold_records(recovered, :origin, [], fn _, _, bytes, acc ->
               {:cont, [bytes | acc]}
             end)

    assert :ok = WAL.close(recovered)
  end

  defp open(path) do
    result = WAL.start_link(path: path, stream_id: "crash", mode: :opaque)

    case result do
      {:ok, owner} -> on_exit(fn -> if Process.alive?(owner), do: WAL.close(owner) end)
      _ -> :ok
    end

    result
  end

  defp beam(script) do
    # Explicitly use the current build's beams and active .tool-versions runtime.
    # No Mix invocation, shared build state, external broker or distributed node.
    paths =
      [:graviton_mq_core, :graviton_mq_storage]
      |> Enum.map(fn app -> :code.lib_dir(app) |> List.to_string() |> Path.join("ebin") end)

    args = Enum.flat_map(paths, fn path -> ["-pa", path] end) ++ ["-e", script]

    System.cmd(System.find_executable("elixir"), args,
      env: [{"ERL_FLAGS", "+S 2:2"}],
      stderr_to_stdout: true
    )
  end
end
