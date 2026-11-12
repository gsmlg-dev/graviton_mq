Code.require_file("../support/tls.exs", __DIR__)

defmodule GravitonMQ.Runtime.AMQP10.ConnectionInputTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Codec.{Performative, Frame}
  alias GravitonMQ.AMQP10.Frame, as: Envelope
  alias GravitonMQ.AMQP10.Performative.{Begin, Close, Open}
  alias GravitonMQ.Runtime.AMQP10.{Config, Connection, Writer}
  alias GravitonMQ.Runtime.Test.TLS

  @compile {:no_warn_undefined, :ssl}
  @moduletag timeout: 10_000
  @header <<"AMQP", 0, 1, 0, 0>>

  test "an idle TCP connection receives no polling wakeups and still handles data and close" do
    {connection, peer} = connected!()
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    assert {:ok, [active: :once]} = :inet.getopts(data(connection).socket, [:active])

    assert idle_messages(connection, 200) == []

    # Input is still delivered after the idle interval.
    :ok = :gen_tcp.send(peer, heartbeat() <> wire(peer_begin(), 7))
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:session_opened, _}}}, 1_000

    monitor = Process.monitor(connection)
    :gen_tcp.close(peer)

    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, :closed}}},
                   1_000

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 1_000
  end

  test "socket errors and messages for a replaced socket are fenced" do
    {connection, peer} = connected!()
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    socket = data(connection).socket
    {:ok, stale} = :gen_tcp.listen(0, [:binary, active: false])
    on_exit(fn -> :gen_tcp.close(stale) end)
    send(connection, {:tcp, stale, <<"bad">>})
    send(connection, {:tcp_closed, stale})
    send(connection, {:tcp_error, stale, :econnreset})
    assert %{messages: 0, bytes: 0} = Connection.capacity(connection, 10, {"x", :sender})
    assert data(connection).endpoint.phase == :active

    monitor = Process.monitor(connection)
    send(connection, {:tcp_error, socket, :econnreset})

    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, :econnreset}}},
                   1_000

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 1_000
  end

  test "a TLS connection is driven by active-once input" do
    {socket, peer} = TLS.server_pair!(TLS.certificates!())
    on_exit(fn -> :ssl.close(peer) end)
    connection = start_supervised!({Connection, {config!([]), relay!(), :tls}}, id: make_ref())
    # The ex_ssl server socket is owned by this test process (its handshaker).
    :ok = SSL.controlling_process(socket, connection)
    send(connection, {:accepted, socket})
    :ok = :ssl.send(peer, @header)
    assert {:ok, @header} = :ssl.recv(peer, 8, 1_000)
    {:ok, <<size::32, _::binary>> = frame_header} = :ssl.recv(peer, 8, 1_000)
    {:ok, _} = :ssl.recv(peer, size - byte_size(frame_header), 1_000)

    :ok = :ssl.send(peer, wire(%Open{container_id: Value.string("tls-peer")}))

    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "tls-peer"}}},
                   1_000

    assert {:ok, [active: :once]} = SSL.getopts(data(connection).socket, [:active])
    assert idle_messages(connection, 200) == []

    monitor = Process.monitor(connection)
    :ssl.close(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, _}}}, 1_000
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 1_000
  end

  # T5
  test "a TLS Writer blocked by a non-reading peer does not stop the peer's Close" do
    {connection, peer} = tls_connected!(send_timeout: 3_000)
    monitor = monitor!(connection)
    %{writer: writer, writer_handle: handle} = data(connection)
    assert fill_until_blocked(handle, writer)

    sent = System.monotonic_time(:millisecond)
    :ok = :ssl.send(peer, wire(%Close{}))
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, _}}}, 2_000
    assert System.monotonic_time(:millisecond) - sent < 2_000
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 4_000
  end

  test "a TLS Writer blocked by a non-reading peer closes by send_timeout" do
    {connection, _peer} = tls_connected!(send_timeout: 1_000)
    monitor = monitor!(connection)
    %{writer: writer, writer_handle: handle} = data(connection)
    assert fill_until_blocked(handle, writer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, _}}}, 4_000
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 4_000
  end

  # Messages the connection receives while nothing happens on its socket.
  defp idle_messages(connection, interval) do
    :erlang.trace(connection, true, [:receive, {:tracer, self()}])
    Process.sleep(interval)
    :erlang.trace(connection, false, [:receive])
    # Acknowledgements of earlier ready hints are not socket wakeups.
    connection |> collect_trace([]) |> Enum.reject(&match?({:"$gen_cast", :pump_ack}, &1))
  end

  defp collect_trace(connection, acc) do
    receive do
      {:trace, ^connection, :receive, message} -> collect_trace(connection, [message | acc])
    after
      20 -> Enum.reverse(acc)
    end
  end

  defp connected! do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])

    {:ok, {_, port}} = :inet.sockname(listener)
    {:ok, peer} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000)
    {:ok, socket} = :gen_tcp.accept(listener, 1000)
    :gen_tcp.close(listener)
    connection = start_supervised!({Connection, {config!([]), relay!(), :tcp}}, id: make_ref())
    :ok = :gen_tcp.controlling_process(socket, connection)
    send(connection, {:accepted, socket})
    :ok = :gen_tcp.send(peer, @header)
    assert {:ok, @header} = :gen_tcp.recv(peer, 8, 1000)
    {:ok, <<size::32, _::binary>>} = :gen_tcp.recv(peer, 8, 1000)
    {:ok, _} = :gen_tcp.recv(peer, size - 8, 1000)
    on_exit(fn -> :gen_tcp.close(peer) end)
    {connection, peer}
  end

  # An ex_ssl server socket handed to a Connection; header and Open exchanged.
  defp tls_connected!(options) do
    {socket, peer} = TLS.server_pair!(TLS.certificates!(), recbuf: 8_192)
    on_exit(fn -> :ssl.close(peer) end)

    connection =
      start_supervised!({Connection, {config!(options), relay!(), :tls}}, id: make_ref())

    :ok = SSL.controlling_process(socket, connection)
    send(connection, {:accepted, socket})
    :ok = :ssl.send(peer, @header)
    assert {:ok, @header} = :ssl.recv(peer, 8, 1_000)
    {:ok, <<size::32, _::binary>> = frame_header} = :ssl.recv(peer, 8, 1_000)
    {:ok, _} = :ssl.recv(peer, size - byte_size(frame_header), 1_000)
    :ok = :ssl.send(peer, wire(%Open{container_id: Value.string("tls-peer")}))

    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "tls-peer"}}},
                   1_000

    {connection, peer}
  end

  # Unknown write ids are ignored by the Endpoint (write_result {nil, _}).
  defp fill_until_blocked(handle, writer, remaining \\ 2_000) do
    cond do
      blocked_in_tls_send?(writer) ->
        true

      remaining == 0 ->
        false

      true ->
        _ = Writer.enqueue(handle, {:t5_fill, remaining}, :binary.copy(<<0>>, 65_536))
        Process.sleep(2)
        fill_until_blocked(handle, writer, remaining - 1)
    end
  end

  # Blocked: inside SSL.send and still there 200 ms later without consuming a
  # queued write. One sample can catch an ordinary send in progress.
  defp blocked_in_tls_send?(writer) do
    with {:ok, queued} <- in_tls_send(writer),
         :ok <- Process.sleep(200),
         {:ok, later} when later >= queued <- in_tls_send(writer) do
      true
    else
      _other -> false
    end
  end

  defp in_tls_send(writer) do
    case Process.info(writer, [:current_stacktrace, :message_queue_len]) do
      [current_stacktrace: stack, message_queue_len: queued] ->
        if Enum.any?(stack, &match?({SSL, _, _, _}, &1)), do: {:ok, queued}, else: :error

      nil ->
        :error
    end
  end

  defp monitor!(pid) do
    monitor = Process.monitor(pid)
    _ = :sys.get_state(pid)
    monitor
  end

  defp relay! do
    owner = self()
    start_supervised!({Task, fn -> relay(owner) end}, id: make_ref())
  end

  defp relay(owner) do
    receive do
      {:amqp10_endpoint_event, connection, table, token} ->
        case Connection.take_event(table, token) do
          {:ok, event} -> send(owner, {:amqp10_endpoint_event, connection, event})
          :expired -> :ok
        end

        relay(owner)

      {:"ETS-TRANSFER", table, _connection, {:amqp10_events, _}} ->
        :ets.delete(table)
        relay(owner)

      {:amqp10_endpoint_ready, connection} = message ->
        Connection.pump_ack(connection)
        send(owner, message)
        relay(owner)

      other ->
        send(owner, other)
        relay(owner)
    end
  end

  defp data(connection) do
    caller = self()
    reference = make_ref()

    :sys.replace_state(connection, fn shell ->
      send(caller, {reference, :ets.lookup_element(shell.table, :state, 2)})
      shell
    end)

    receive do
      {^reference, data} -> data
    after
      1000 -> flunk("missing connection state")
    end
  end

  defp config!(options) do
    {:ok, config} =
      Config.build(
        Keyword.merge([container_id: "broker", queues: ["q"], auth: :anonymous], options)
      )

    config
  end

  defp peer_open(peer), do: :gen_tcp.send(peer, wire(%Open{container_id: Value.string("peer")}))

  defp heartbeat, do: <<8::32, 2, 0, 0::16>>

  defp peer_begin,
    do: %Begin{
      next_outgoing_id: Value.uint(0),
      incoming_window: Value.uint(32),
      outgoing_window: Value.uint(32)
    }

  defp wire(pdu, channel \\ 0) do
    {:ok, body} = Performative.encode(pdu)

    {:ok, bytes} =
      Frame.encode(%Envelope{
        declared_size: 8 + byte_size(body),
        data_offset_words: 2,
        type: :amqp,
        channel: channel,
        extended_header: <<>>,
        body: body
      })

    bytes
  end
end
