defmodule GravitonMQ.Runtime.HellHandoffTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias GravitonMQ.Runtime.HellHandoff
  alias GravitonMQ.Runtime.MQTT.Listener

  @moduletag timeout: 10_000
  # Below the module timeout so a stalled stop fails the call, not the case.
  @stop_call 8_000

  defmodule Probe do
    use GenServer, restart: :temporary

    def start_link({observer, linger}), do: GenServer.start_link(__MODULE__, {observer, linger})

    @impl true
    def init({observer, linger}) do
      if linger > 0, do: Process.flag(:trap_exit, true)
      {:ok, {observer, linger}}
    end

    @impl true
    def handle_info({:accepted, socket} = message, {observer, _linger} = state) do
      send(observer, {message, self()})
      :ok = :inet.setopts(socket, active: :once)
      {:noreply, state}
    end

    def handle_info({:tcp, _socket, _data} = message, {observer, _linger} = state) do
      send(observer, message)
      {:noreply, state}
    end

    def handle_info(_other, state), do: {:noreply, state}

    @impl true
    def terminate(_reason, {observer, linger}) do
      send(observer, {:terminating, self(), System.monotonic_time(:millisecond)})
      Process.sleep(linger)
    end
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
      listener = %{ip: {127, 0, 0, 1}, port: 0, backlog: 16}
      {:ok, server} = HellHandoff.start_link(listener, 4_321, table, options.handoff)
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

  test "hands an accepted socket to a supervised child and lives exactly as long as it" do
    %{owner: owner, server: server, port: port} = server()
    client = connect(port)

    assert_receive {{:accepted, raw}, probe}, 1_000
    assert :erlang.port_info(raw, :connected) == {:connected, probe}
    # After publication, handlers read the table and never call the listener.
    assert GenServer.call(owner, :calls) == 0

    assert {:ok, [send_timeout: 4_321, send_timeout_close: true, nodelay: false]} =
             :inet.getopts(raw, [:send_timeout, :send_timeout_close, :nodelay])

    :ok = :gen_tcp.send(client, "hello")
    assert_receive {:tcp, ^raw, "hello"}, 1_000

    :ok = GenServer.stop(probe)
    assert {:error, :closed} = :gen_tcp.recv(client, 0, 1_000)
    assert eventually(fn -> Hell.connection_pids(server) == {:ok, []} end)
  end

  test "the handler ignores stray messages and redacts its status" do
    %{server: server, port: port} = server()
    client = connect(port)
    assert_receive {{:accepted, raw}, _probe}, 1_000
    assert {:ok, [handler]} = Hell.connection_pids(server)
    send(handler, :stray)
    assert {:status, ^handler, _module, status} = :sys.get_status(handler)
    refute inspect(status, limit: :infinity) =~ "Hell.Socket"
    refute inspect(status, limit: :infinity) =~ inspect(raw)
    assert Process.alive?(handler)
    assert {:error, :timeout} = :gen_tcp.recv(client, 0, 50)
  end

  test "closes the connection when the child supervisor refuses it" do
    %{server: server, port: port} = server(max_children: 0)
    client = connect(port)

    assert {:error, :closed} = :gen_tcp.recv(client, 0, 1_000)
    refute_receive {{:accepted, _raw}, _probe}, 50
    assert eventually(fn -> Hell.connection_pids(server) == {:ok, []} end)
  end

  test "a connection accepted before the listener publishes waits for its answer" do
    %{owner: owner, port: port} = server(publish: false, reply_after: 800)
    started = System.monotonic_time(:millisecond)
    client = connect(port)

    assert_receive {{:accepted, raw}, _probe}, 2_000
    assert System.monotonic_time(:millisecond) - started >= 800
    assert GenServer.call(owner, :calls) == 1
    :ok = :gen_tcp.send(client, "late")
    assert_receive {:tcp, ^raw, "late"}, 1_000
  end

  test "a listener whose table is gone closes the connection and says so" do
    table = Task.async(fn -> HellHandoff.table() end) |> Task.await()

    log =
      capture_log(fn ->
        %{server: server, port: port} = server(publish: false, table: table)
        client = connect(port)

        assert {:error, :closed} = :gen_tcp.recv(client, 0, 1_000)
        assert eventually(fn -> Hell.connection_pids(server) == {:ok, []} end)
      end)

    assert log =~ "Hell handoff closed a connection: its listener is unavailable"
    refute_received {{:accepted, _raw}, _probe}
  end

  test "stopping the server closes accepted connections and stops their live children" do
    %{owner: owner, port: port} = server()
    client = connect(port)
    assert_receive {{:accepted, _raw}, probe}, 1_000
    probe_monitor = Process.monitor(probe)

    assert :ok = GenServer.call(owner, :stop_server, @stop_call)
    assert {:error, :closed} = :gen_tcp.recv(client, 0, 1_000)
    # A non-owner close sends the owner no `{:tcp_closed, socket}`; the
    # handler stops the child itself.
    assert_receive {:DOWN, ^probe_monitor, :process, ^probe, :shutdown}, 1_000
  end

  test "stopping releases slow children in parallel across acceptors and leaves nothing behind" do
    %{owner: owner, server: server, port: port} = server(linger: 1_500)
    # Hell stops acceptor subtrees one after another; spread the handlers
    # over several so only a parallel drain can stop them together.
    {clients, handlers} = spread(port, server, [])
    probes = for _ <- clients, do: receive_probe()
    started = System.monotonic_time(:millisecond)

    assert :ok = GenServer.call(owner, :stop_server, @stop_call)

    elapsed = System.monotonic_time(:millisecond) - started
    starts = for _ <- probes, do: assert_receive({:terminating, _probe, at}, 1_000) && at
    assert Enum.max(starts) - Enum.min(starts) < 750
    assert elapsed < 4_000
    refute Process.alive?(server)
    refute Enum.any?(handlers ++ probes, &Process.alive?/1)
    for client <- clients, do: assert({:error, :closed} = :gen_tcp.recv(client, 0, 1_000))
  end

  test "a child that outlives the release budget is killed" do
    %{owner: owner, server: server, port: port} =
      server(linger: 2_000, handoff: [connection_stop: 200])

    client = connect(port)
    probe = receive_probe()
    probe_monitor = Process.monitor(probe)
    started = System.monotonic_time(:millisecond)

    assert :ok = GenServer.call(owner, :stop_server, @stop_call)

    assert System.monotonic_time(:millisecond) - started < 1_500
    assert_receive {:DOWN, ^probe_monitor, :process, ^probe, :killed}, 1_000
    refute Process.alive?(server)
    assert {:error, :closed} = :gen_tcp.recv(client, 0, 1_000)
  end

  test "stopping before publication does not wait on handlers blocked in the listener call" do
    %{owner: owner, server: server, port: port} = server(publish: false, reply_after: :never)
    client = connect(port)
    assert eventually(fn -> GenServer.call(owner, :calls) == 1 end)
    {:ok, handlers} = Hell.connection_pids(server)
    started = System.monotonic_time(:millisecond)

    assert :ok = GenServer.call(owner, :stop_server, @stop_call)
    assert System.monotonic_time(:millisecond) - started < 500

    # The blocked call exits once the listener itself is gone.
    log =
      capture_log(fn ->
        :ok = stop_supervised(Owner)
        assert eventually(fn -> not Enum.any?([server | handlers], &Process.alive?/1) end)
      end)

    assert log =~ "its listener is unavailable"
    assert {:error, :closed} = :gen_tcp.recv(client, 0, 1_000)
    refute_received {{:accepted, _raw}, _probe}
  end

  test "a handler released before Hell hands it a socket stops at once" do
    {:ok, handler} =
      GenServer.start(HellHandoff, {self(), HellHandoff.table(), 5_000})

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

  test "rebinding a fixed port fails explicitly once the retry budget is spent" do
    {:ok, holder} =
      :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])

    {:ok, port} = :inet.port(holder)
    # The owning listener traps exits; a failed linked start leaves no EXIT.
    Process.flag(:trap_exit, true)
    started = System.monotonic_time(:millisecond)

    assert {:error, {:shutdown, {:failed_to_start_child, :listener, :eaddrinuse}}} =
             HellHandoff.start_link(
               %{ip: {127, 0, 0, 1}, port: port, backlog: 16},
               1_000,
               HellHandoff.table()
             )

    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed >= 1_000 and elapsed < 2_000
    refute_receive {:EXIT, _pid, _reason}, 50
  end

  defp server(options \\ []) do
    {supervisor_options, options} = Keyword.split(options, [:max_children])

    connections =
      start_supervised!({DynamicSupervisor, [strategy: :one_for_one] ++ supervisor_options})

    options =
      Map.merge(
        %{publish: true, reply_after: 0, table: nil, linger: 0, handoff: []},
        Map.new(options)
      )

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

  # Connects until at least 8 handlers run under at least 2 acceptors.
  defp spread(port, server, clients) do
    {:ok, handlers} = Hell.connection_pids(server)
    parents = handlers |> Enum.map(&Process.info(&1, :parent)) |> Enum.uniq()

    if length(clients) >= 8 and length(handlers) == length(clients) and length(parents) >= 2 do
      {clients, handlers}
    else
      assert length(clients) < 64
      client = connect(port)

      assert eventually(fn ->
               Hell.connection_pids(server) |> elem(1) |> length() > length(clients)
             end)

      spread(port, server, [client | clients])
    end
  end

  defp receive_probe do
    assert_receive {{:accepted, _raw}, probe}, 1_000
    probe
  end

  defp connect(port) do
    {:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    client
  end

  defp eventually(predicate, deadline \\ System.monotonic_time(:millisecond) + 1_000) do
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
