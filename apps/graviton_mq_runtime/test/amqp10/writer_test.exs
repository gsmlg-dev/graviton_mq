defmodule GravitonMQ.Runtime.AMQP10.WriterTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias GravitonMQ.Runtime.AMQP10.Writer

  @moduletag timeout: 10_000

  setup do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])

    {:ok, {_, port}} = :inet.sockname(listener)
    {:ok, peer} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    {:ok, socket} = :gen_tcp.accept(listener, 1_000)
    :ok = :gen_tcp.close(listener)
    :ok = :inet.setopts(socket, send_timeout: 200, send_timeout_close: true)

    on_exit(fn ->
      :gen_tcp.close(socket)
      :gen_tcp.close(peer)
    end)

    %{socket: socket, peer: peer}
  end

  test "frame admission never waits for the writer and preserves stream/result order", context do
    {writer, handle} = writer!(context, max_frames: 2, max_bytes: 100)

    suspended(writer, fn ->
      assert :ok = Writer.enqueue(handle, :first, "first")
      assert :ok = Writer.enqueue(handle, :second, "second")
      assert {:error, :writer_full} = Writer.enqueue(handle, :overflow, "")
      refute_receive {:amqp10_write, _, _, _}, 20
    end)

    assert_receive {:amqp10_write, 3, :first, :ok}
    assert_receive {:amqp10_write, 3, :second, :ok}
    assert {:ok, "firstsecond"} = :gen_tcp.recv(context.peer, 11, 1_000)
    assert :ok = Writer.enqueue(handle, :reused, "again")
    assert_receive {:amqp10_write, 3, :reused, :ok}
    assert {:ok, "again"} = :gen_tcp.recv(context.peer, 5, 1_000)
    refute_receive {:amqp10_write, _, :overflow, _}, 20
  end

  test "byte admission rejects oversize and aggregate overflow without consuming credits",
       context do
    {writer, handle} = writer!(context, max_frames: 8, max_bytes: 4)

    suspended(writer, fn ->
      assert {:error, :writer_full} = Writer.enqueue(handle, :oversize, "12345")
      assert :ok = Writer.enqueue(handle, :first, "abc")
      assert {:error, :writer_full} = Writer.enqueue(handle, :overflow, "de")
      assert :ok = Writer.enqueue(handle, :last_byte, "d")
      assert {:error, :writer_full} = Writer.enqueue(handle, :full, "x")
    end)

    assert_receive {:amqp10_write, 3, :first, :ok}
    assert_receive {:amqp10_write, 3, :last_byte, :ok}
    assert {:ok, "abcd"} = :gen_tcp.recv(context.peer, 4, 1_000)
    assert :ok = Writer.enqueue(handle, :reused, "efgh")
    assert_receive {:amqp10_write, 3, :reused, :ok}
    assert {:ok, "efgh"} = :gen_tcp.recv(context.peer, 4, 1_000)
  end

  test "concurrent admission cannot exceed frame or byte credits", context do
    {writer, handle} = writer!(context, max_frames: 7, max_bytes: 14)

    admitted =
      suspended(writer, fn ->
        results =
          1..64
          |> Task.async_stream(fn id -> {id, Writer.enqueue(handle, id, "xx")} end,
            max_concurrency: 16,
            timeout: 1_000
          )
          |> Enum.map(fn {:ok, result} -> result end)

        accepted = for {id, :ok} <- results, do: id
        assert length(accepted) == 7
        assert Enum.count(results, &match?({_, {:error, :writer_full}}, &1)) == 57
        accepted
      end)

    for id <- admitted, do: assert_receive({:amqp10_write, 3, ^id, :ok})
    assert {:ok, bytes} = :gen_tcp.recv(context.peer, 14, 1_000)
    assert bytes == String.duplicate("x", 14)
    assert :ok = Writer.enqueue(handle, :all_bytes, String.duplicate("y", 14))
    assert_receive {:amqp10_write, 3, :all_bytes, :ok}
    assert {:ok, bytes} = :gen_tcp.recv(context.peer, 14, 1_000)
    assert bytes == String.duplicate("y", 14)
  end

  test "a small encoded sub-binary cannot retain a large backing allocation", context do
    bytes = binary_part(:binary.copy("s", 1_048_576), 0, 256)
    assert :binary.referenced_byte_size(bytes) > byte_size(bytes)
    {writer, handle} = writer!(context, max_frames: 1, max_bytes: 256)

    suspended(writer, fn ->
      assert :ok = Writer.enqueue(handle, :compact, bytes)

      [{_token, :compact, retained}] =
        Enum.filter(:ets.tab2list(handle.table), &match?({_, :compact, _}, &1))

      assert retained == bytes
      assert :binary.referenced_byte_size(retained) <= 256
    end)

    assert_receive {:amqp10_write, 3, :compact, :ok}
    assert {:ok, ^bytes} = :gen_tcp.recv(context.peer, 256, 1_000)
  end

  test "a blocked active write keeps its credits and socket timeout stops the writer", context do
    :ok = :inet.setopts(context.socket, sndbuf: 1_024, send_timeout: 200)
    :ok = :inet.setopts(context.peer, recbuf: 1_024)
    payload = :binary.copy("x", 4_194_304)
    # inet_tcp may first accept data into its driver queue. Fill that queue
    # before observing a subsequent write blocked by the non-reading peer.
    :ok = :gen_tcp.send(context.socket, payload)
    {writer, handle} = writer!(context, max_frames: 1, max_bytes: byte_size(payload))
    monitor = Process.monitor(writer)
    assert :ok = Writer.enqueue(handle, :blocked, payload)
    await_empty_mailbox(writer)
    refute_receive {:amqp10_write, 3, :blocked, _}, 20
    assert {:error, :writer_full} = Writer.enqueue(handle, :overflow, "x")
    assert_receive {:amqp10_write, 3, :blocked, {:error, :timeout}}, 2_000
    assert_receive {:DOWN, ^monitor, :process, ^writer, {:shutdown, :socket_write_failed}}
    assert {:error, :closed} = Writer.enqueue(handle, :after_failure, "x")
    assert :undefined == :ets.info(handle.table)
  end

  test "send failure is reported with identity and generation, then deletes the queue", context do
    {writer, handle} = writer!(context)
    monitor = Process.monitor(writer)
    :ok = :gen_tcp.close(context.socket)

    suspended(writer, fn ->
      assert :ok = Writer.enqueue(handle, :failed, "private-payload")
      assert :ok = Writer.enqueue(handle, :queued, "must-not-succeed")
    end)

    assert_receive {:amqp10_write, 3, :failed, {:error, :closed}}
    assert_receive {:DOWN, ^monitor, :process, ^writer, {:shutdown, :socket_write_failed}}
    assert {:error, :closed} = Writer.enqueue(handle, :late, "later")
    assert :undefined == :ets.info(handle.table)
    refute_receive {:amqp10_write, 3, :queued, :ok}, 20
  end

  test "normal broker owner death stops its writer and releases the table", context do
    owner = lifetime!()
    {writer, handle} = writer!(context, owner: owner)
    monitor = Process.monitor(writer)
    owner_monitor = Process.monitor(owner)

    suspended(writer, fn ->
      assert :ok = Writer.enqueue(handle, :queued, "must-not-send")
      send(owner, :finish)
      assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :normal}
    end)

    assert_receive {:DOWN, ^monitor, :process, ^writer, :normal}
    assert {:error, :closed} = Writer.enqueue(handle, :late, "later")
    assert :undefined == :ets.info(handle.table)
    assert {:error, :timeout} = :gen_tcp.recv(context.peer, 1, 20)
    # The reader retains socket ownership; Writer does not close a live reader's socket.
    assert :ok = :gen_tcp.send(context.socket, "reader")
    assert {:ok, "reader"} = :gen_tcp.recv(context.peer, 6, 1_000)
  end

  test "normal reader death stops its writer even while the broker remains alive", context do
    reader = lifetime!()
    {writer, handle} = writer!(context, lifetime_owner: reader)
    monitor = Process.monitor(writer)
    send(reader, :finish)
    assert_receive {:DOWN, ^monitor, :process, ^writer, :normal}
    assert {:error, :closed} = Writer.enqueue(handle, :late, "later")
    assert :undefined == :ets.info(handle.table)
    assert Process.alive?(self())
  end

  test "status and callback crash reports omit encoded bytes and private exception arguments",
       context do
    {writer, handle} = writer!(context)
    private = "private-writer-packet-#{System.unique_integer([:positive])}"
    :ok = :sys.log(writer, true)
    assert :ok = Writer.enqueue(handle, :status, private)
    assert_receive {:amqp10_write, 3, :status, :ok}
    refute inspect(:sys.get_status(writer)) =~ private
    monitor = Process.monitor(writer)

    log =
      capture_log(fn ->
        # A broken internal socket must fail without publishing callback arguments.
        :sys.replace_state(writer, &%{&1 | socket: {:invalid_socket, private}})

        suspended(writer, fn ->
          assert :ok = Writer.enqueue(handle, :crash, private)
          assert :ok = Writer.enqueue(handle, :queued, private)
          refute inspect(:sys.get_status(writer)) =~ private
        end)

        assert_receive {:DOWN, ^monitor, :process, ^writer, reason}
        refute inspect(reason) =~ private
      end)

    refute log =~ private
    assert :undefined == :ets.info(handle.table)
    assert {:error, :closed} = Writer.enqueue(handle, :late, private)
  end

  defp writer!(context, overrides \\ []) do
    options =
      Keyword.merge(
        [
          owner: self(),
          lifetime_owner: self(),
          generation: 3,
          socket: context.socket,
          transport: :tcp,
          max_frames: 8,
          max_bytes: 100
        ],
        overrides
      )

    writer = start_supervised!({Writer, options}, id: make_ref(), restart: :temporary)
    {writer, Writer.handle(writer)}
  end

  defp lifetime! do
    start_supervised!(
      {Task,
       fn ->
         receive do
           :finish -> :ok
         end
       end},
      id: make_ref(),
      restart: :temporary
    )
  end

  defp suspended(writer, callback) do
    :ok = :sys.suspend(writer)

    try do
      callback.()
    after
      if Process.alive?(writer), do: :sys.resume(writer)
    end
  end

  defp await_empty_mailbox(writer, attempts \\ 100)
  defp await_empty_mailbox(_writer, 0), do: flunk("writer never started its socket write")

  defp await_empty_mailbox(writer, attempts) do
    case Process.info(writer, :message_queue_len) do
      {:message_queue_len, 0} ->
        :ok

      {:message_queue_len, _} ->
        Process.sleep(1)
        await_empty_mailbox(writer, attempts - 1)

      nil ->
        flunk("writer stopped before its socket write was observed")
    end
  end
end
