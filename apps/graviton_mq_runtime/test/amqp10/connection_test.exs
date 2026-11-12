defmodule GravitonMQ.Runtime.AMQP10.ConnectionTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Codec.{Performative, Frame}
  alias GravitonMQ.AMQP10.Frame, as: Envelope
  alias GravitonMQ.AMQP10.Performative.{Open, Begin, Attach, Close, Flow, Transfer, Disposition}
  alias GravitonMQ.AMQP10.Terminus.{Source, Target}
  alias GravitonMQ.Runtime.AMQP10.{Authenticator, Config, Connection}
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.SASL, as: SASLCodec
  alias GravitonMQ.AMQP10.SASL.{Init, Mechanisms, Outcome, Plain}

  @moduletag timeout: 10_000

  defmodule Policy do
    @moduledoc false
    @behaviour GravitonMQ.Runtime.AMQP10.Auth

    @impl true
    def authenticate(_credentials, options) do
      send(Keyword.fetch!(options, :test), {:authenticating, self()})

      case Keyword.fetch!(options, :mode) do
        :sleep ->
          Process.sleep(:infinity)

        :gate ->
          receive do
            {:release, result} -> result
          end
      end
    end
  end

  @header <<"AMQP", 0, 1, 0, 0>>
  @sasl <<"AMQP", 3, 1, 0, 0>>

  test "split header and coalesced Open/heartbeats drain under a one-frame turn budget" do
    {connection, peer} = accepted!(max_input_frames_per_turn: 1)
    assert :ok = :gen_tcp.send(peer, <<"AM">>)
    assert {:error, :timeout} = :gen_tcp.recv(peer, 1, 20)

    assert :ok =
             :gen_tcp.send(
               peer,
               <<"QP", 0, 1, 0, 0>> <>
                 wire(%Open{container_id: Value.string("peer")}) <> heartbeat() <> heartbeat()
             )

    assert {:ok, @header} = :gen_tcp.recv(peer, 8, 1000)
    assert %Open{} = read_pdu(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    assert %{messages: 0, bytes: 0} = Connection.capacity(connection, 0, {"missing", :sender})
    assert Process.alive?(connection)
  end

  test "socket writes do not complete an open before peer handshake" do
    {connection, _peer} = connected!()
    refute_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, _}}}, 30
    assert %{messages: 0, bytes: 0} = Connection.capacity(connection, 0, {"x", :sender})
  end

  test "ready hints coalesce until manager acknowledges" do
    {connection, peer} = connected!()
    assert_receive {:amqp10_endpoint_ready, ^connection}
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    Connection.capacity(connection, 0, {"x", :sender})
    refute_receive {:amqp10_endpoint_ready, ^connection}, 20
    Connection.pump_ack(connection)
    Connection.command(connection, :bad, :unsupported)
    assert_receive {:amqp10_endpoint_event, ^connection, {:operation, :bad, {:error, _}}}
    assert_receive {:amqp10_endpoint_ready, ^connection}
  end

  test "deferred Attach requires manager decision before Attach response" do
    {connection, peer} = connected!()
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    :ok = :gen_tcp.send(peer, wire(peer_begin(), 7))
    assert %Begin{} = read_pdu(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:session_opened, sid}}}

    attach = %Attach{
      name: Value.string("work"),
      handle: Value.uint(19),
      role: Value.boolean(false),
      snd_settle_mode: Value.ubyte(0),
      rcv_settle_mode: Value.ubyte(0),
      source: %Source{address: Value.string("q")},
      target: %Target{address: Value.string("q")},
      initial_delivery_count: Value.uint(0)
    }

    :ok = :gen_tcp.send(peer, wire(attach, 7))

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:event, {:session, ^sid, {:attach_requested, identity, token, %Attach{}}}}}

    assert {:error, :timeout} = :gen_tcp.recv(peer, 1, 20)
    Connection.command(connection, :accept, {:session, sid, {:accept_link, identity, token}})
    assert %Attach{name: %Value{value: "work"}, role: %Value{value: true}} = read_pdu(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:operation, :accept, {:ok, ^identity}}}
    assert %{messages: 0, bytes: 0} = Connection.capacity(connection, sid, identity)
  end

  test "sender capacity waits for peer credit and Accepted settlement completes publication" do
    {connection, peer} = connected!()
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    :ok = :gen_tcp.send(peer, wire(peer_begin(), 7))
    assert %Begin{} = read_pdu(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:session_opened, sid}}}

    attach = %Attach{
      name: Value.string("outgoing"),
      handle: Value.uint(19),
      role: Value.boolean(true),
      snd_settle_mode: Value.ubyte(0),
      rcv_settle_mode: Value.ubyte(0),
      source: %Source{address: Value.string("q")},
      target: %Target{address: Value.string("q")}
    }

    :ok = :gen_tcp.send(peer, wire(attach, 7))

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:event, {:session, ^sid, {:attach_requested, identity, token, _}}}}

    :ok =
      Connection.command(connection, :accept, {:session, sid, {:accept_link, identity, token}})

    assert %Attach{} = read_pdu(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:operation, :accept, {:ok, ^identity}}}
    assert %{messages: 0, bytes: 0} = Connection.capacity(connection, sid, identity)

    flow = %Flow{
      next_incoming_id: Value.uint(0),
      incoming_window: Value.uint(32),
      next_outgoing_id: Value.uint(0),
      outgoing_window: Value.uint(32),
      handle: Value.uint(19),
      delivery_count: Value.uint(0),
      link_credit: Value.uint(1)
    }

    :ok = :gen_tcp.send(peer, wire(flow, 7))
    await_credit(connection, sid, identity)
    raw = <<0, 255, 128, 1, 2, 3>>
    :ok = Connection.command(connection, :publish, {:session, sid, {:send, identity, raw, []}})
    assert {%Transfer{delivery_id: %Value{value: delivery_id}}, ^raw} = read_envelope(peer)
    refute_receive {:amqp10_endpoint_event, ^connection, {:operation, :publish, _}}, 20

    disposition = %Disposition{
      role: Value.boolean(true),
      first: Value.uint(delivery_id),
      settled: Value.boolean(true),
      state: %GravitonMQ.AMQP10.Outcome.Accepted{}
    }

    :ok = :gen_tcp.send(peer, wire(disposition, 7))

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:operation, :publish, {:ok, %GravitonMQ.AMQP10.Outcome.Accepted{}}}}
  end

  test "command token admission bounds suspended owner without exposing private data" do
    {connection, peer} = connected!(endpoint_options: [max_pending_operations: 1])
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    handle = Connection.command_handle(connection)
    :sys.log(connection, true)
    :sys.suspend(connection)

    try do
      assert :ok = Connection.command(handle, :first, {:unsupported, "private-command"})
      assert {:error, :mailbox_full} = Connection.command(handle, :full, :unsupported)
      assert [{:credits, 1, bytes}] = :ets.lookup(handle.table, :credits)
      assert bytes <= handle.max_bytes
      refute inspect(:sys.get_status(connection)) =~ "private-command"
    after
      :sys.resume(connection)
    end

    assert_receive {:amqp10_endpoint_event, ^connection, {:operation, :first, {:error, _}}}
    assert :ok = Connection.command(handle, :reuse, :unsupported)
    assert_receive {:amqp10_endpoint_event, ^connection, {:operation, :reuse, {:error, _}}}
    assert [{:credits, 0, 0}] = :ets.lookup(handle.table, :credits)

    assert {:error, :mailbox_full} =
             Connection.command(
               handle,
               :large,
               {:unsupported, :binary.copy("x", handle.max_bytes)}
             )

    assert [{:credits, 0, 0}] = :ets.lookup(handle.table, :credits)
  end

  test "final events remain readable after normal connection stop and tables are reclaimed" do
    config = config!(connect_timeout: 1000)
    connection = start_supervised!({Connection, {config, self(), :tcp}})
    handle = Connection.command_handle(connection)
    monitor = Process.monitor(connection)
    :ok = Connection.close(connection, :finished)
    assert_receive {:amqp10_endpoint_event, ^connection, table, token}
    assert_receive {:"ETS-TRANSFER", ^table, ^connection, {:amqp10_events, ^connection}}
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
    assert {:ok, {:event, {:disconnected, :finished}}} = Connection.take_event(table, token)
    assert :expired = Connection.take_event(table, token)
    assert [{:credits, 0, 0}] = :ets.lookup(table, :credits)
    :ets.delete(table)
    assert :undefined == :ets.info(handle.table)
    assert {:error, :closed} = Connection.command(handle, :late, :unsupported)
  end

  test "an unresponsive manager cannot grow event storage beyond its finite limits" do
    connection =
      start_supervised!(
        {Connection,
         {config!(
            connect_timeout: 1000,
            endpoint_options: [max_pending_operations: 1]
          ), self(), :tcp}}
      )

    handle = Connection.command_handle(connection)
    limit = 65

    for index <- 1..limit do
      :ok = Connection.command(handle, index, :unsupported)
      Connection.capacity(connection, 0, {"missing", :sender})
    end

    monitor = Process.monitor(connection)
    :ok = Connection.command(handle, :overflow, :unsupported)
    assert_receive {:amqp10_endpoint_overflow, ^connection}
    assert_receive {:"ETS-TRANSFER", table, ^connection, {:amqp10_events, ^connection}}
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
    assert [{:credits, ^limit, bytes}] = :ets.lookup(table, :credits)
    assert bytes <= 2_097_152
    assert :ets.info(table, :size) == limit + 1
    :ets.delete(table)
    assert :undefined == :ets.info(handle.table)
  end

  test "peer Close handshake releases connection quota without waiting for timeout" do
    {connection, peer} = connected!(endpoint_options: [close_timeout_ms: 2000])
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    monitor = monitor!(connection)
    :ok = :gen_tcp.send(peer, wire(%Close{}))
    assert %Close{} = read_pdu(peer)
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 500
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
  end

  test "final Close writer wait has a finite deadline even when writer is suspended" do
    {connection, peer} = connected!(send_timeout: 50)
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    writer = data(connection).writer
    writer_monitor = Process.monitor(writer)
    monitor = monitor!(connection)
    :sys.suspend(writer)

    try do
      :ok = :gen_tcp.send(peer, wire(%Close{}))
      assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, _}}}
      assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 1000
      assert_receive {:DOWN, ^writer_monitor, :process, ^writer, _}, 1000
      assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
    after
      if Process.alive?(writer), do: :sys.resume(writer)
    end
  end

  test "TLS handshake with a silent peer respects the finite connection timeout" do
    directory =
      Path.join(System.tmp_dir!(), "graviton-amqp-tls-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    certificate = Path.join(directory, "certificate.pem")
    key = Path.join(directory, "key.pem")
    on_exit(fn -> File.rm_rf!(directory) end)

    {_output, 0} =
      System.cmd(
        "openssl",
        [
          "req",
          "-x509",
          "-newkey",
          "rsa:2048",
          "-nodes",
          "-keyout",
          key,
          "-out",
          certificate,
          "-days",
          "1",
          "-subj",
          "/CN=localhost"
        ],
        stderr_to_stdout: true
      )

    {:ok, _} = Application.ensure_all_started(:ssl)

    {:ok, listener} =
      :ssl.listen(0, [
        :binary,
        ip: {127, 0, 0, 1},
        active: false,
        certfile: String.to_charlist(certificate),
        keyfile: String.to_charlist(key)
      ])

    on_exit(fn -> :ssl.close(listener) end)
    {:ok, {_, port}} = :ssl.sockname(listener)
    {:ok, peer} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000)
    on_exit(fn -> :gen_tcp.close(peer) end)
    config = config!(connect_timeout: 40)
    broker = relay!()
    caller = self()

    # Own and queue the accepted socket before its admission timer exists;
    # release initialization only after the test has installed its monitor.
    initializer = fn ->
      {:ok, socket} = :ssl.transport_accept(listener, 1000)
      send(caller, {:tls_socket_ready, self()})

      receive do
        {:begin_handshake, ^caller} ->
          send(self(), {:accepted, socket})
          {config, broker, :tls}
      after
        1000 -> exit(:tls_fixture_setup_timeout)
      end
    end

    {:ok, supervisor} = ExUnit.fetch_test_supervisor()

    startup =
      Task.async(fn ->
        Supervisor.start_child(supervisor, %{
          id: make_ref(),
          restart: :temporary,
          start: {Connection, :start_link, [initializer]}
        })
      end)

    assert_receive {:tls_socket_ready, connection}, 1000
    monitor = Process.monitor(connection)
    send(connection, {:begin_handshake, self()})
    assert {:ok, ^connection} = Task.await(startup)

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:event, {:disconnected, {:handshake, :timeout}}}},
                   1000

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
  end

  test "disconnect resolves emitted Begin as unknown and stops normally" do
    {connection, peer} = connected!()
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    Connection.command(connection, :begin, {:open_session, 10, []})
    assert %Begin{} = read_pdu(peer)
    monitor = monitor!(connection)
    :gen_tcp.close(peer)

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:operation, :begin, {:unknown, :closed}}}

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
  end

  test "a suspended writer does not block capacity and cleanup releases its ETS table" do
    {connection, peer} = connected!()
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    state = data(connection)
    writer = state.writer
    table = state.writer_handle.table
    :sys.suspend(writer)

    try do
      Connection.command(connection, :begin, {:open_session, 10, []})

      assert %{messages: 0, bytes: 0} =
               Connection.capacity(connection, 10, {"x", :sender}, 100)

      assert :ets.info(table) != :undefined
    after
      :sys.resume(writer)
    end

    assert %Begin{} = read_pdu(peer)
    monitor = monitor!(writer)
    Connection.close(connection, :test_finished)
    assert_receive {:DOWN, ^monitor, :process, ^writer, _}
    assert :ets.info(table) == :undefined
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
  end

  test "malformed frame produces Close and a finite normal stop" do
    {connection, peer} = connected!(endpoint_options: [close_timeout_ms: 40])
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    monitor = monitor!(connection)
    :ok = :gen_tcp.send(peer, <<7::32, 2, 0, 0::16>>)
    assert %Close{error: %{}} = read_pdu(peer)
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 1000
  end

  test "input frame bound is local and oversize declaration is rejected before its body" do
    {connection, peer} = connected!(endpoint_options: [max_frame_size: 512, close_timeout_ms: 40])
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    monitor = monitor!(connection)
    :ok = :gen_tcp.send(peer, <<513::32, 2, 0, 0::16>>)
    assert %Close{error: %{}} = read_pdu(peer)
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 1000
  end

  test "sasl: :disabled answers a SASL header with the AMQP header and keeps its reason" do
    {connection, peer} = accepted!(sasl: :disabled)
    monitor = monitor!(connection)
    :ok = :gen_tcp.send(peer, @sasl)
    assert {:ok, @header} = :gen_tcp.recv(peer, 8, 1000)

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:event, {:disconnected, :sasl_not_supported}}}

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
  end

  test "unsupported versions and protocol IDs receive the supported header then close the socket" do
    for header <- [
          <<"AMQP", 0, 0, 9, 1>>,
          <<"AMQP", 0, 1, 1, 0>>,
          <<"AMQP", 0, 2, 0, 0>>,
          <<"AMQP", 4, 1, 0, 0>>
        ] do
      {connection, peer} = accepted!()
      monitor = monitor!(connection)
      assert :ok = :gen_tcp.send(peer, header)
      # The reply follows the peer header and precedes the close (TR-2.2).
      assert {:ok, @header} = :gen_tcp.recv(peer, 8, 1000)
      assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 1000
      assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
      refute_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, _}}}, 10
    end
  end

  test "malformed protocol magic receives the supported header before bounded teardown" do
    {connection, peer} = accepted!()
    monitor = monitor!(connection)
    assert :ok = :gen_tcp.send(peer, "HTTP/1.1")
    assert {:ok, @header} = :gen_tcp.recv(peer, 8, 1000)
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 1000
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
    refute_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, _}}}, 10
  end

  test "start notification provides its command handle before socket handshake" do
    connection = start_supervised!({Connection, {config!(connect_timeout: 1000), self(), :tcp}})
    assert_receive {:amqp10_connection_started, ^connection, %{pid: ^connection} = handle}
    assert :ok = Connection.command(handle, :without_socket, :unsupported)
    assert_receive {:amqp10_endpoint_event, ^connection, table, token}
    assert {:ok, {:operation, :without_socket, {:error, _}}} = Connection.take_event(table, token)
  end

  test "waiting for socket ownership is finite" do
    {connection, monitor} = monitored_start!(config!(connect_timeout: 30), relay!())

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:event, {:disconnected, :accept_timeout}}},
                   1000

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
  end

  test "broker death closes connection, writer and socket" do
    broker =
      start_supervised!(
        {Task,
         fn ->
           receive do
             :finish -> :ok
           end
         end}
      )

    {connection, peer} = connected!([], broker)
    writer = data(connection).writer
    monitor = monitor!(connection)
    writer_monitor = monitor!(writer)
    send(broker, :finish)
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
    assert_receive {:DOWN, ^writer_monitor, :process, ^writer, _}
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
  end

  test "writer death closes connection and reports transport loss" do
    {connection, peer} = connected!()
    monitor = monitor!(connection)
    Process.exit(data(connection).writer, :kill)

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:event, {:disconnected, :writer_stopped}}}

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
  end

  test "stale socket, write generation and timer tokens do not mutate live endpoint" do
    {connection, peer} = connected!()
    peer_open(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    send(connection, {:tcp, make_ref(), <<"bad">>})
    send(connection, {:amqp10_write, 99, 0, {:error, :closed}})
    send(connection, {:wake, 0, :open, make_ref()})
    send(connection, {:drain, 99})
    assert %{messages: 0, bytes: 0} = Connection.capacity(connection, 10, {"x", :sender})
    assert data(connection).endpoint.phase == :active
  end

  test "status and callback crash reports redact private messages and arguments" do
    {connection, peer} = connected!()
    private = "private-amqp-#{System.unique_integer([:positive])}"
    :sys.log(connection, true)
    assert :ok = Connection.command(connection, :private_command, {:unsupported, private})
    :ok = :gen_tcp.send(peer, wire(%Open{container_id: Value.string(private)}))
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, ^private}}}
    refute inspect(:sys.get_status(connection)) =~ private
    monitor = Process.monitor(connection)

    log =
      capture_log(fn ->
        :sys.replace_state(connection, fn shell ->
          data = :ets.lookup_element(shell.table, :state, 2)
          :ets.insert(shell.table, {:state, %{data | socket: {:broken, private}}})
          shell
        end)

        # Input is event driven; a local close reaches the broken socket.
        Connection.close(connection, :test_finished)

        assert_receive {:DOWN, ^monitor, :process, ^connection, reason}
        refute inspect(reason) =~ private
      end)

    refute log =~ private
  end

  test "the default anonymous policy negotiates SASL ANONYMOUS before the AMQP layer" do
    {connection, peer} = accepted!()
    :ok = :gen_tcp.send(peer, @sasl)
    assert {:ok, @sasl} = :gen_tcp.recv(peer, 8, 1000)
    assert %Mechanisms{} = mechanisms = read_sasl(peer)
    assert Mechanisms.names(mechanisms) == ["ANONYMOUS"]
    :ok = :gen_tcp.send(peer, sasl_wire(%Init{mechanism: Value.symbol("ANONYMOUS")}))
    assert %Outcome{code: %Value{type: :ubyte, value: 0}} = read_sasl(peer)
    assert_receive {:amqp10_principal, ^connection, :anonymous}
    assert {:error, :timeout} = :gen_tcp.recv(peer, 1, 20)
    :ok = :gen_tcp.send(peer, @header <> wire(%Open{container_id: Value.string("peer")}))
    assert {:ok, @header} = :gen_tcp.recv(peer, 8, 1000)
    assert %Open{} = read_pdu(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
  end

  test "a no-SASL peer under the default policy records the anonymous principal" do
    {connection, _peer} = connected!()
    assert_receive {:amqp10_principal, ^connection, :anonymous}
  end

  test "ANONYMOUS with an invalid trace is refused with an auth outcome and counted" do
    {connection, peer} = accepted!()
    monitor = monitor!(connection)
    :ok = :gen_tcp.send(peer, @sasl)
    assert {:ok, @sasl} = :gen_tcp.recv(peer, 8, 1000)
    assert %Mechanisms{} = read_sasl(peer)

    :ok =
      :gen_tcp.send(
        peer,
        sasl_wire(%Init{
          mechanism: Value.symbol("ANONYMOUS"),
          initial_response: Value.binary(<<0xFF>>)
        })
      )

    assert %Outcome{code: %Value{value: 1}} = read_sasl(peer)
    assert_receive {:amqp10_sasl_failure, ^connection, :auth}

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:event, {:disconnected, {:sasl, :auth}}}}

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
  end

  test "required SASL answers a no-SASL header with the SASL header and no Open" do
    {connection, peer} = accepted!(sasl: :required)
    monitor = monitor!(connection)
    :ok = :gen_tcp.send(peer, @header)
    assert {:ok, @sasl} = :gen_tcp.recv(peer, 8, 1000)

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:event, {:disconnected, :sasl_required}}}

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
  end

  test "a silent peer is closed by the SASL negotiation deadline" do
    # The negotiation deadline is armed when the socket arrives; monitor first.
    {connection, peer, socket} = pending!(sasl_timeout: 30)
    monitor = Process.monitor(connection)
    send(connection, {:accepted, socket})

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:event, {:disconnected, :sasl_timeout}}},
                   1000

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
  end

  test "a protocol violation during SASL closes without an outcome" do
    {connection, peer} = accepted!()
    monitor = monitor!(connection)
    :ok = :gen_tcp.send(peer, @sasl)
    assert {:ok, @sasl} = :gen_tcp.recv(peer, 8, 1000)
    assert %Mechanisms{} = read_sasl(peer)
    # An AMQP-typed frame in the SASL layer.
    :ok = :gen_tcp.send(peer, heartbeat())

    assert_receive {:amqp10_endpoint_event, ^connection,
                    {:event, {:disconnected, {:decode_error, :amqp_frame_in_sasl_layer}}}}

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 1000)
  end

  # Spec §5.2: a missed auth_timeout is sys-temp. The Connection owns the one
  # absolute deadline; the Authenticator watcher is only a later backstop.
  test "a callback that misses auth_timeout is always refused with sys-temp" do
    for _attempt <- 1..5 do
      {connection, peer} = authenticated!([mode: :sleep], auth_timeout: 30)
      monitor = monitor!(connection)
      plain_init!(peer, "graviton-password")
      assert_receive {:authenticating, task}, 1000
      # The 30 ms deadline may kill the task before this monitor exists.
      task_monitor = Process.monitor(task)
      assert %Outcome{code: %Value{value: 4}} = read_sasl(peer)
      assert_receive {:amqp10_sasl_failure, ^connection, :sys_temp}
      refute_received {:amqp10_sasl_failure, ^connection, _other}

      assert_receive {:amqp10_endpoint_event, ^connection,
                      {:event, {:disconnected, {:sasl, :sys_temp}}}}

      assert_killed(task, task_monitor)
      assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 1000
    end
  end

  test "a killed callback is sys-temp and any other callback exit is sys" do
    for {reason, code, value} <- [{:kill, :sys_temp, 4}, {:callback_crashed, :sys, 2}] do
      {connection, peer} = authenticated!(mode: :gate)
      plain_init!(peer, "graviton-password")
      assert_receive {:authenticating, task}, 1000

      capture_log(fn ->
        Process.exit(task, reason)
        assert %Outcome{code: %Value{value: ^value}} = read_sasl(peer)
      end)

      assert_receive {:amqp10_sasl_failure, ^connection, ^code}
    end
  end

  test "stale and late authentication results are ignored" do
    {connection, peer} = authenticated!(mode: :gate)
    plain_init!(peer, "graviton-password")
    assert_receive {:authenticating, task}, 1000

    # A result whose reference is not the current authentication.
    send(connection, {:amqp10_sasl_result, {:sasl_auth, 41}, {:ok, :forged}})
    send(connection, {:amqp10_sasl_result, make_ref(), {:error, :auth}})
    assert {:error, :timeout} = :gen_tcp.recv(peer, 1, 30)
    assert_receive {:amqp10_sasl_authenticating, ^connection, true}
    refute_received {:amqp10_sasl_authenticating, ^connection, false}

    send(task, {:release, {:ok, {:user, "graviton-user"}}})
    assert %Outcome{code: %Value{value: 0}} = read_sasl(peer)
    assert_receive {:amqp10_principal, ^connection, {:user, "graviton-user"}}
    assert_receive {:amqp10_sasl_authenticating, ^connection, false}

    # A late result for the finished authentication changes nothing.
    send(connection, {:amqp10_sasl_result, {:sasl_auth, 0}, {:error, :auth}})
    :ok = :gen_tcp.send(peer, @header <> wire(%Open{container_id: Value.string("peer")}))
    assert {:ok, @header} = :gen_tcp.recv(peer, 8, 1000)
    assert %Open{} = read_pdu(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
    refute_received {:amqp10_principal, ^connection, :forged}
    refute_received {:amqp10_sasl_failure, ^connection, _code}
  end

  test "a pipelined header and Open wait, untouched, for the authentication result" do
    {connection, peer} = authenticated!(mode: :gate)
    :ok = :gen_tcp.send(peer, @sasl)
    assert {:ok, @sasl} = :gen_tcp.recv(peer, 8, 1000)
    assert %Mechanisms{} = read_sasl(peer)
    {:ok, message} = Plain.encode(%Plain{username: "graviton-user", password: "secret"})
    init = %Init{mechanism: Value.symbol("PLAIN"), initial_response: Value.binary(message)}

    :ok =
      :gen_tcp.send(
        peer,
        sasl_wire(init) <> @header <> wire(%Open{container_id: Value.string("peer")})
      )

    assert_receive {:authenticating, task}, 1000
    # Paused: neither the held header nor the Open reaches the Endpoint.
    assert {:error, :timeout} = :gen_tcp.recv(peer, 1, 30)
    assert data(connection).endpoint.phase == :new
    send(task, {:release, {:ok, :pipelined}})
    assert %Outcome{code: %Value{value: 0}} = read_sasl(peer)
    assert {:ok, @header} = :gen_tcp.recv(peer, 8, 1000)
    assert %Open{} = read_pdu(peer)
    assert_receive {:amqp10_principal, ^connection, :pipelined}
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "peer"}}}
  end

  test "transport loss while input is paused cancels the running callback" do
    {connection, peer} = authenticated!(mode: :gate)
    monitor = monitor!(connection)
    :ok = :gen_tcp.send(peer, @sasl)
    assert {:ok, @sasl} = :gen_tcp.recv(peer, 8, 1000)
    assert %Mechanisms{} = read_sasl(peer)
    {:ok, message} = Plain.encode(%Plain{username: "graviton-user", password: "secret"})
    init = %Init{mechanism: Value.symbol("PLAIN"), initial_response: Value.binary(message)}
    :ok = :gen_tcp.send(peer, sasl_wire(init) <> @header)
    assert_receive {:authenticating, task}, 1000
    task_monitor = Process.monitor(task)
    :gen_tcp.close(peer)
    assert_killed(task, task_monitor)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, :closed}}}
    assert_receive {:amqp10_sasl_authenticating, ^connection, false}
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 1000
  end

  test "credentials never reach Connection state or status during or after authentication" do
    password = "connection-password-#{System.unique_integer([:positive])}"

    log =
      capture_log(fn ->
        for result <- [{:ok, {:user, "graviton-user"}}, {:error, {:denied, password}}] do
          {connection, peer} = authenticated!(mode: :gate)
          monitor = monitor!(connection)
          plain_init!(peer, password)
          assert_receive {:authenticating, task}, 1000

          # Mid-authentication.
          refute_secret(connection, password)

          send(task, {:release, result})
          assert %Outcome{code: %Value{value: code}} = read_sasl(peer)

          case code do
            0 ->
              assert_receive {:amqp10_principal, ^connection, _principal}
              refute_secret(connection, password)
              Connection.close(connection, :test_finished)

            1 ->
              :ok
          end

          assert_receive {:DOWN, ^monitor, :process, ^connection, reason}, 1000
          refute inspect(reason) =~ password
        end
      end)

    refute log =~ password
  end

  # A monitor is a signal from this test process, while socket input (and
  # signals from other processes) reach the monitored process by another
  # path; the two are unordered, so a stimulus could stop it before the
  # monitor is installed (`:noproc`). A synchronous call afterwards
  # guarantees the monitor is in place before any stimulus is sent.
  defp monitor!(pid) do
    monitor = Process.monitor(pid)
    _ = :sys.get_state(pid)
    monitor
  end

  # A sensitive, blocked callback task cannot answer a synchronous call, and
  # it may be killed before its monitor exists: `:noproc` is accepted only
  # for a task that is really gone.
  defp assert_killed(task, monitor) do
    assert_receive {:DOWN, ^monitor, :process, ^task, reason}, 1000
    assert reason in [:killed, :noproc]
    refute Process.alive?(task)
  end

  # Starts a Connection whose initialization waits until the test has
  # monitored it, so no timer armed during init can fire first.
  defp monitored_start!(config, broker) do
    caller = self()

    initializer = fn ->
      send(caller, {:monitor_fixture, self()})

      receive do
        {:monitored, ^caller} -> {config, broker, :tcp}
      after
        1000 -> exit(:monitor_fixture_timeout)
      end
    end

    {:ok, supervisor} = ExUnit.fetch_test_supervisor()

    startup =
      Task.async(fn ->
        Supervisor.start_child(supervisor, %{
          id: make_ref(),
          restart: :temporary,
          start: {Connection, :start_link, [initializer]}
        })
      end)

    assert_receive {:monitor_fixture, connection}, 1000
    monitor = Process.monitor(connection)
    send(connection, {:monitored, self()})
    assert {:ok, ^connection} = Task.await(startup)
    {connection, monitor}
  end

  defp accepted!(options \\ [], broker \\ self()) do
    {connection, peer, socket} = pending!(options, broker)
    send(connection, {:accepted, socket})
    {connection, peer}
  end

  # A started Connection owning its socket, before `{:accepted, socket}`.
  defp pending!(options, broker \\ self()) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])

    {:ok, {_, port}} = :inet.sockname(listener)
    {:ok, peer} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000)
    {:ok, socket} = :gen_tcp.accept(listener, 1000)
    :gen_tcp.close(listener)
    broker = if broker == self(), do: relay!(), else: broker
    connection = start_supervised!({Connection, {config!(options), broker, :tcp}}, id: make_ref())
    :ok = :gen_tcp.controlling_process(socket, connection)
    on_exit(fn -> :gen_tcp.close(peer) end)
    {connection, peer, socket}
  end

  # The broker sends nothing until the peer's protocol header (owner Q7).
  defp connected!(options \\ [], broker \\ self()) do
    {connection, peer} = accepted!(options, broker)
    assert {:error, :timeout} = :gen_tcp.recv(peer, 1, 20)
    :ok = :gen_tcp.send(peer, @header)
    assert {:ok, @header} = :gen_tcp.recv(peer, 8, 1000)
    assert %Open{} = read_pdu(peer)
    {connection, peer}
  end

  defp read_sasl(peer) do
    {:ok, <<size::32, _::binary>> = header} = :gen_tcp.recv(peer, 8, 1000)
    {:ok, body} = :gen_tcp.recv(peer, size - 8, 1000)

    {:ok, %Envelope{type: :sasl} = frame, <<>>} =
      Frame.decode(header <> body, Limits.default(), types: [:sasl])

    {:ok, decoded} = SASLCodec.decode(frame.body)
    decoded
  end

  defp sasl_wire(body) do
    {:ok, bytes} = SASLCodec.encode_frame(body)
    bytes
  end

  defp authenticated!(policy_options, options \\ []) do
    parent =
      start_supervised!(
        %{
          id: make_ref(),
          type: :supervisor,
          start:
            {Supervisor, :start_link,
             [
               [Supervisor.child_spec({Task.Supervisor, max_children: 4}, id: :sasl_tasks)],
               [strategy: :one_for_one]
             ]}
        },
        id: make_ref()
      )

    policy = {Policy, [test: self()] ++ policy_options}
    {:ok, authenticator} = Supervisor.start_child(parent, {Authenticator, {policy, parent}})

    accepted!([auth: policy, allow_insecure_plain: true, authenticator: authenticator] ++ options)
  end

  defp plain_init!(peer, password) do
    :ok = :gen_tcp.send(peer, @sasl)
    assert {:ok, @sasl} = :gen_tcp.recv(peer, 8, 1000)
    assert %Mechanisms{} = read_sasl(peer)
    {:ok, message} = Plain.encode(%Plain{username: "graviton-user", password: password})

    :ok =
      :gen_tcp.send(
        peer,
        sasl_wire(%Init{
          mechanism: Value.symbol("PLAIN"),
          initial_response: Value.binary(message)
        })
      )
  end

  defp refute_secret(connection, secret) do
    state = data(connection)
    refute inspect(state, limit: :infinity, printable_limit: :infinity) =~ secret
    refute :binary.match(:erlang.term_to_binary(state), secret) != :nomatch
    refute inspect(:sys.get_status(connection), limit: :infinity) =~ secret
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

  # Mirrors the runtime: the policy stays with the Authenticator, and a
  # Listener publishes the Authenticator pid in the SASL map.
  defp config!(options) do
    {authenticator, options} = Keyword.pop(options, :authenticator)

    {:ok, config} =
      Config.build(
        Keyword.merge([container_id: "broker", queues: ["q"], auth: :anonymous], options)
      )

    config = Map.delete(config, :auth)
    if authenticator, do: put_in(config, [:sasl, :authenticator], authenticator), else: config
  end

  defp peer_open(peer), do: :gen_tcp.send(peer, wire(%Open{container_id: Value.string("peer")}))

  defp peer_begin,
    do: %Begin{
      next_outgoing_id: Value.uint(0),
      incoming_window: Value.uint(32),
      outgoing_window: Value.uint(32)
    }

  defp heartbeat, do: <<8::32, 2, 0, 0::16>>

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

  defp read_envelope(peer) do
    {:ok, <<size::32, _::binary-size(4)>> = header} = :gen_tcp.recv(peer, 8, 1000)
    body = if size > 8, do: elem(:gen_tcp.recv(peer, size - 8, 1000), 1), else: <<>>
    {:ok, frame, <<>>} = Frame.decode(header <> body)
    {:ok, pdu, payload} = Performative.decode(frame.body)
    {pdu, payload}
  end

  defp read_pdu(peer) do
    {pdu, <<>>} = read_envelope(peer)
    pdu
  end

  defp await_credit(connection, sid, identity, remaining \\ 100)
  defp await_credit(_connection, _sid, _identity, 0), do: flunk("peer credit was never observed")

  defp await_credit(connection, sid, identity, remaining) do
    case Connection.capacity(connection, sid, identity) do
      %{messages: 1, bytes: bytes} when bytes > 0 ->
        :ok

      _ ->
        Process.sleep(2)
        await_credit(connection, sid, identity, remaining - 1)
    end
  end
end
