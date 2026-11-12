Code.require_file("support/tls.exs", __DIR__)

defmodule GravitonMQ.Runtime.HellHandoffTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias GravitonMQ.Runtime.HellHandoff
  alias GravitonMQ.Runtime.MQTT.Listener
  alias GravitonMQ.Runtime.Test.TLS

  @compile {:no_warn_undefined, :ssl}
  @moduletag timeout: 20_000
  # Below the module timeout so a stalled stop fails the call, not the case.
  @stop_call 8_000

  defmodule Probe do
    use GenServer, restart: :temporary

    def start_link({observer, linger}), do: GenServer.start_link(__MODULE__, {observer, linger})

    @impl true
    def init({observer, linger}) do
      if linger > 0, do: Process.flag(:trap_exit, true)
      {:ok, %{observer: observer, linger: linger, socket: nil}}
    end

    @impl true
    def handle_call({:controlling_process, owner}, _from, state),
      do: {:reply, SSL.controlling_process(state.socket, owner), state}

    def handle_call(:arm, _from, state), do: {:reply, arm(state.socket), state}

    @impl true
    def handle_info({:accepted, socket} = message, state) do
      send(state.observer, {message, self()})
      :ok = arm(socket)
      {:noreply, %{state | socket: socket}}
    end

    def handle_info({kind, socket, data}, state) when kind in [:tcp, :ssl] do
      send(state.observer, {:data, socket, data})
      {:noreply, state}
    end

    def handle_info(_other, state), do: {:noreply, state}

    @impl true
    def terminate(_reason, state) do
      send(state.observer, {:terminating, self(), System.monotonic_time(:millisecond)})
      Process.sleep(state.linger)
    end

    defp arm(%SSL.Socket{} = socket), do: SSL.setopts(socket, active: :once)
    defp arm(socket), do: :inet.setopts(socket, active: :once)
  end

  # Stands in for a protocol listener: it owns the Hell server, publishes the
  # Connection start argument (or withholds it and answers late, or loses
  # its table, or never answers) like a listener before and after wiring.
  defmodule Owner do
    use GenServer

    alias GravitonMQ.Runtime.HellHandoff

    def start_link(options), do: GenServer.start_link(__MODULE__, options)

    @impl true
    def init(options) do
      Process.flag(:trap_exit, true)
      table = options.table || HellHandoff.table()

      listener = %{
        ip: {127, 0, 0, 1},
        port: 0,
        backlog: 16,
        transport: options.transport,
        tls_options: options.tls_options
      }

      settings = %{
        send_timeout: 4_321,
        handshake_timeout: options.handshake_timeout,
        max_connections: options.max_connections
      }

      {:ok, server} = HellHandoff.start_link(listener, settings, table, options.handoff)
      state = Map.merge(options, %{server: server, table: table, calls: 0})
      if options.publish, do: :ok = HellHandoff.publish(table, spec(state))
      {:ok, state}
    end

    @impl true
    def handle_call(:server, _from, state), do: {:reply, state.server, state}
    def handle_call(:calls, _from, state), do: {:reply, state.calls, state}

    def handle_call(:stop_server, _from, state),
      do: {:reply, HellHandoff.stop(state.server, state.table), state}

    def handle_call(:connection_spec, from, state) do
      if is_integer(state.reply_after),
        do: Process.send_after(self(), {:answer, from}, state.reply_after)

      {:noreply, %{state | calls: state.calls + 1}}
    end

    @impl true
    def handle_info({:answer, from}, state) do
      GenServer.reply(from, spec(state))
      {:noreply, state}
    end

    def handle_info(_message, state), do: {:noreply, state}

    defp spec(state),
      do: %{connections: state.connections, child: {Probe, {state.observer, state.linger}}}
  end

  setup_all do
    %{certificates: TLS.certificates!()}
  end

  for transport <- [:tcp, :tls] do
    describe "#{transport}" do
      @describetag transport: transport

      test "hands an accepted socket to a supervised child and lives exactly as long as it",
           context do
        %{owner: owner, server: server, port: port} = server(context)
        client = connect(context, port)

        assert_receive {{:accepted, raw}, probe}, 1_000
        assert_owner(context, raw, probe)
        # After publication, handlers read the table and never call the listener.
        assert GenServer.call(owner, :calls) == 0

        assert {:ok, [send_timeout: 4_321, send_timeout_close: true, nodelay: false]} =
                 getopts(context, raw, [:send_timeout, :send_timeout_close, :nodelay])

        :ok = client_send(context, client, "hello")
        assert_receive {:data, ^raw, "hello"}, 1_000

        :ok = GenServer.stop(probe)
        assert {:error, :closed} = client_recv(context, client, 1_000)
        assert eventually(fn -> Hell.connection_pids(server) == {:ok, []} end)
      end

      test "the handler ignores stray messages and redacts its status", context do
        %{server: server, port: port} = server(context)
        client = connect(context, port)
        assert_receive {{:accepted, raw}, _probe}, 1_000
        assert {:ok, [handler]} = Hell.connection_pids(server)
        send(handler, :stray)
        assert {:status, ^handler, _module, status} = :sys.get_status(handler)
        refute inspect(status, limit: :infinity) =~ "Hell.Socket"
        refute inspect(status, limit: :infinity) =~ inspect(raw)
        assert Process.alive?(handler)
        assert {:error, :timeout} = client_recv(context, client, 50)
      end

      test "closes the connection when the child supervisor refuses it", context do
        %{server: server, port: port} = server(context, max_children: 0)
        client = connect(context, port)

        assert {:error, :closed} = client_recv(context, client, 1_000)
        refute_receive {{:accepted, _raw}, _probe}, 50
        assert eventually(fn -> Hell.connection_pids(server) == {:ok, []} end)
      end

      test "a connection accepted before the listener publishes waits for its answer", context do
        %{owner: owner, port: port} = server(context, publish: false, reply_after: 800)
        started = System.monotonic_time(:millisecond)
        client = connect(context, port)

        assert_receive {{:accepted, raw}, _probe}, 2_000
        assert System.monotonic_time(:millisecond) - started >= 800
        assert GenServer.call(owner, :calls) == 1
        :ok = client_send(context, client, "late")
        assert_receive {:data, ^raw, "late"}, 1_000
      end

      test "a listener whose table is gone closes the connection and says so", context do
        table = Task.async(fn -> HellHandoff.table() end) |> Task.await()

        log =
          capture_log(fn ->
            %{server: server, port: port} = server(context, publish: false, table: table)
            client = connect(context, port)

            assert {:error, :closed} = client_recv(context, client, 1_000)
            assert eventually(fn -> Hell.connection_pids(server) == {:ok, []} end)
          end)

        assert log =~ "Hell handoff closed a connection: its listener is unavailable"
        refute_received {{:accepted, _raw}, _probe}
      end

      test "stopping the server closes accepted connections and stops their live children",
           context do
        %{owner: owner, port: port} = server(context)
        client = connect(context, port)
        assert_receive {{:accepted, _raw}, probe}, 1_000
        probe_monitor = monitor!(probe)

        assert :ok = GenServer.call(owner, :stop_server, @stop_call)
        assert {:error, :closed} = client_recv(context, client, 1_000)
        # A non-owner close sends the owner nothing; the handler stops the child itself.
        assert_receive {:DOWN, ^probe_monitor, :process, ^probe, :shutdown}, 1_000
      end

      test "stopping releases slow children in parallel across acceptors and leaves nothing behind",
           context do
        %{owner: owner, server: server, port: port} = server(context, linger: 1_500)
        {clients, handlers} = spread(context, port, server, [])
        probes = for _ <- clients, do: receive_probe()
        started = System.monotonic_time(:millisecond)

        assert :ok = GenServer.call(owner, :stop_server, @stop_call)

        elapsed = System.monotonic_time(:millisecond) - started
        starts = for _ <- probes, do: assert_receive({:terminating, _probe, at}, 1_000) && at
        assert Enum.max(starts) - Enum.min(starts) < 750
        assert elapsed < 4_000
        refute Process.alive?(server)
        refute Enum.any?(handlers ++ probes, &Process.alive?/1)
        for client <- clients, do: assert({:error, :closed} = client_recv(context, client, 1_000))
      end

      test "a child that outlives the release budget is killed", context do
        %{owner: owner, server: server, port: port} =
          server(context, linger: 2_000, handoff: [connection_stop: 200])

        client = connect(context, port)
        probe = receive_probe()
        probe_monitor = monitor!(probe)
        started = System.monotonic_time(:millisecond)

        assert :ok = GenServer.call(owner, :stop_server, @stop_call)

        assert System.monotonic_time(:millisecond) - started < 1_500
        assert_receive {:DOWN, ^probe_monitor, :process, ^probe, :killed}, 1_000
        refute Process.alive?(server)
        assert {:error, :closed} = client_recv(context, client, 1_000)
      end

      test "stopping before publication does not wait on handlers blocked in the listener call",
           context do
        %{owner: owner, server: server, port: port} =
          server(context, publish: false, reply_after: :never)

        client = connect(context, port)
        assert eventually(fn -> GenServer.call(owner, :calls) == 1 end)
        {:ok, handlers} = Hell.connection_pids(server)
        started = System.monotonic_time(:millisecond)

        assert :ok = GenServer.call(owner, :stop_server, @stop_call)
        assert System.monotonic_time(:millisecond) - started < 500

        log =
          capture_log(fn ->
            :ok = stop_supervised(Owner)
            assert eventually(fn -> not Enum.any?([server | handlers], &Process.alive?/1) end)
          end)

        assert log =~ "its listener is unavailable"
        assert {:error, :closed} = client_recv(context, client, 1_000)
        refute_received {{:accepted, _raw}, _probe}
      end

      # T8 (unit): the rebind error shape is the same for TLS (ex_ssl returns
      # the :gen_tcp.listen/2 error unchanged), so the retry applies.
      test "rebinding a fixed port fails explicitly once the retry budget is spent", context do
        {:ok, holder} =
          :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])

        {:ok, port} = :inet.port(holder)
        Process.flag(:trap_exit, true)
        started = System.monotonic_time(:millisecond)

        assert {:error, {:shutdown, {:failed_to_start_child, :listener, :eaddrinuse}}} =
                 HellHandoff.start_link(
                   %{
                     ip: {127, 0, 0, 1},
                     port: port,
                     backlog: 16,
                     transport: context.transport,
                     tls_options: tls_options(context)
                   },
                   %{send_timeout: 1_000, handshake_timeout: 1_000, max_connections: 16},
                   HellHandoff.table()
                 )

        elapsed = System.monotonic_time(:millisecond) - started
        assert elapsed >= 1_000 and elapsed < 2_000
        refute_receive {:EXIT, _pid, _reason}, 50
      end
    end
  end

  describe "tls only" do
    @describetag transport: :tls

    # T1
    test "after handoff the handler is no longer the owner, the Connection is, and its exit closes the peer",
         context do
      %{server: server, port: port} = server(context)
      client = connect(context, port)
      assert_receive {{:accepted, raw}, probe}, 1_000
      {:ok, [handler]} = Hell.connection_pids(server)
      test = self()

      # The function runs inside the handler process.
      :sys.replace_state(handler, fn state ->
        send(test, {:from_handler, SSL.controlling_process(raw, test)})
        state
      end)

      assert_receive {:from_handler, {:error, :not_owner}}
      assert :ok = GenServer.call(probe, {:controlling_process, probe})
      :ok = GenServer.stop(probe)
      assert {:error, :closed} = client_recv(context, client, 1_000)
    end

    # T2 (handoff level)
    test "a TLS 1.2-only client is refused with protocol_version before any child starts",
         context do
      %{port: port} = server(context)

      assert {:error, {:tls_alert, {:protocol_version, _description}}} =
               TLS.connect(port, context.certificates, versions: [:"tlsv1.2"])

      refute_receive {{:accepted, _raw}, _probe}, 100
    end

    # T4: handshake budget
    test "silent and stalled peers are closed by the handshake timeout before any child starts",
         context do
      %{server: server, port: port} = server(context, handshake_timeout: 300)

      # A bare TCP peer, and one stalled inside a ClientHello record.
      for prefix <- [<<>>, <<22, 3, 1, 0, 64, 1, 0, 0, 60>>] do
        {:ok, raw} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
        started = System.monotonic_time(:millisecond)
        :ok = :gen_tcp.send(raw, prefix)
        assert :closed = closed(raw, started + 2_000)
        elapsed = System.monotonic_time(:millisecond) - started
        assert elapsed >= 250 and elapsed < 1_000, "closed after #{elapsed} ms"
      end

      refute_received {{:accepted, _raw}, _probe}
      assert eventually(fn -> Hell.connection_pids(server) == {:ok, []} end)
    end

    # The runtime owns active_chunk_size (gsmlg-dev/http_fetch#169): input
    # buffered while unarmed is delivered at most 16 KiB per arm, like TCP's
    # buffer, so the protocols' input bounds are unchanged.
    test "every active-once delivery is bounded to 16 KiB", context do
      %{port: port} = server(context)
      client = connect(context, port)
      assert_receive {{:accepted, raw}, probe}, 1_000

      # The first delivery consumes the probe's only arm.
      :ok = client_send(context, client, "hello")
      assert_receive {:data, ^raw, "hello"}, 1_000

      payload = :binary.copy(<<7>>, 65_536)
      :ok = client_send(context, client, payload)
      Process.sleep(200)

      chunks =
        for _ <- 1..4 do
          :ok = GenServer.call(probe, :arm)
          assert_receive {:data, ^raw, chunk}, 1_000
          assert byte_size(chunk) <= 16_384
          chunk
        end

      assert IO.iodata_to_binary(chunks) == payload
    end

    # T4: num_connections = max_connections bounds handshakes too. Hell's
    # num_connections is per acceptor, so the listener-wide bound on
    # concurrent TLS connections (handshakes included) is HellHandoff's 4
    # acceptors x max_connections. Waiting acceptors take turns (FIFO), so
    # each of the 4 holds exactly one.
    test "max_connections bounds concurrent TLS connections per acceptor, including handshakes",
         context do
      %{server: server, port: port} = server(context, max_connections: 1)
      clients = for _ <- 1..4, do: connect(context, port)
      probes = for _ <- clients, do: receive_probe()
      assert {:ok, handlers} = Hell.connection_pids(server)
      assert length(handlers) == 4

      assert {:error, _reason} = TLS.connect(port, context.certificates, [], 500)
      refute_received {{:accepted, _raw}, _probe}

      for probe <- probes, do: :ok = GenServer.stop(probe)
      for client <- clients, do: assert({:error, :closed} = client_recv(context, client, 1_000))
      # Hell's acceptor rechecks the limit once per second (5 attempts).
      assert {:ok, _next} = TLS.connect(port, context.certificates, [], 4_000)
      assert_receive {{:accepted, _raw}, _probe}, 4_000
    end
  end

  test "a handler released before Hell hands it a socket stops at once" do
    {:ok, handler} = GenServer.start(HellHandoff, {self(), HellHandoff.table(), 5_000})
    monitor = Process.monitor(handler)
    send(handler, :hell_handoff_release)
    assert_receive {:DOWN, ^monitor, :process, ^handler, {:shutdown, :released}}, 1_000
  end

  test "shutdown budgets nest and the listener child spec uses the outermost one" do
    budgets = HellHandoff.budgets()

    assert budgets[:connection_stop] + budgets[:kill_wait] < budgets[:handler_shutdown]
    assert budgets[:handler_shutdown] < budgets[:stop_wait]
    assert budgets[:handler_shutdown] + budgets[:stop_wait] < budgets[:owner_shutdown]
    assert budgets[:owner_shutdown] == HellHandoff.owner_shutdown()

    for transport <- [:tcp, :tls] do
      assert %{shutdown: shutdown} = Listener.child_spec({%{}, %{transport: transport}, self()})
      assert shutdown == HellHandoff.owner_shutdown()
    end
  end

  defp server(context, options \\ []) do
    {supervisor_options, options} = Keyword.split(options, [:max_children])

    connections =
      start_supervised!({DynamicSupervisor, [strategy: :one_for_one] ++ supervisor_options})

    options =
      %{
        publish: true,
        reply_after: 0,
        table: nil,
        linger: 0,
        handoff: [],
        handshake_timeout: 1_000,
        max_connections: 64
      }
      |> Map.merge(Map.new(options))
      |> Map.merge(%{transport: context.transport, tls_options: tls_options(context)})

    owner =
      start_supervised!(
        {Owner, Map.merge(options, %{connections: connections, observer: self()})},
        restart: :temporary,
        id: Owner
      )

    server = GenServer.call(owner, :server)
    {:ok, {_ip, port}} = Hell.listener_info(server)
    %{owner: owner, server: server, port: port}
  end

  defp tls_options(%{transport: :tcp}), do: []

  defp tls_options(%{transport: :tls, certificates: certificates}),
    do: TLS.listener_options(certificates)

  defp connect(%{transport: :tcp}, port) do
    {:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    client
  end

  defp connect(%{transport: :tls, certificates: certificates}, port) do
    {:ok, client} = TLS.connect(port, certificates)
    client
  end

  defp client_send(%{transport: :tcp}, client, data), do: :gen_tcp.send(client, data)
  defp client_send(%{transport: :tls}, client, data), do: :ssl.send(client, data)
  defp client_recv(%{transport: :tcp}, client, timeout), do: :gen_tcp.recv(client, 0, timeout)
  defp client_recv(%{transport: :tls}, client, timeout), do: :ssl.recv(client, 0, timeout)
  defp getopts(%{transport: :tcp}, raw, keys), do: :inet.getopts(raw, keys)
  defp getopts(%{transport: :tls}, raw, keys), do: SSL.getopts(raw, keys)

  defp assert_owner(%{transport: :tcp}, raw, probe),
    do: assert(:erlang.port_info(raw, :connected) == {:connected, probe})

  defp assert_owner(%{transport: :tls}, raw, probe) do
    assert {:error, :not_owner} = SSL.controlling_process(raw, self())
    assert :ok = GenServer.call(probe, {:controlling_process, probe})
  end

  # Monitor, then a synchronous call so the monitor is in place before any
  # stimulus from another process.
  defp monitor!(pid) do
    monitor = Process.monitor(pid)
    _ = :sys.get_state(pid)
    monitor
  end

  # Reads (and drops) any alert bytes until the server closes.
  defp closed(raw, deadline) do
    case :gen_tcp.recv(raw, 0, max(deadline - System.monotonic_time(:millisecond), 0)) do
      {:ok, _bytes} -> closed(raw, deadline)
      {:error, reason} -> reason
    end
  end

  # Connects until at least 8 handlers run under at least 2 acceptors.
  defp spread(context, port, server, clients) do
    {:ok, handlers} = Hell.connection_pids(server)
    parents = handlers |> Enum.map(&Process.info(&1, :parent)) |> Enum.uniq()

    if length(clients) >= 8 and length(handlers) == length(clients) and length(parents) >= 2 do
      {clients, handlers}
    else
      assert length(clients) < 64
      client = connect(context, port)

      assert eventually(fn ->
               Hell.connection_pids(server) |> elem(1) |> length() > length(clients)
             end)

      spread(context, port, server, [client | clients])
    end
  end

  defp receive_probe do
    assert_receive {{:accepted, _raw}, probe}, 1_000
    probe
  end

  defp eventually(predicate, deadline \\ System.monotonic_time(:millisecond) + 2_000) do
    cond do
      predicate.() -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true -> eventually_after_yield(predicate, deadline)
    end
  end

  defp eventually_after_yield(predicate, deadline) do
    receive do
    after
      10 -> eventually(predicate, deadline)
    end
  end
end
