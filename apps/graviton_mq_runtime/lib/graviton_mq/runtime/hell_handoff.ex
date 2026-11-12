defmodule GravitonMQ.Runtime.HellHandoff do
  @moduledoc false
  # Hell owns plain-TCP and TLS accept only (TLS: ex_ssl through
  # `Hell.Transports.SSL`, handshaken in the handler before
  # `handle_connection/2`; a failed handshake starts no Connection); a
  # protocol Connection owns everything after it.
  #
  # Contract:
  #
  #   * The listener process creates `table/0` and calls `start_link/4`. Once
  #     wired it calls `publish/2` with
  #     `%{connections: dynamic_supervisor, child: child_spec}`; until then
  #     it answers `GenServer.call(listener, :connection_spec, :infinity)`
  #     with the same value. Handlers read the published value without
  #     calling the listener, so a stopping listener never waits on them.
  #   * For each accepted socket the handler starts `child` under
  #     `connections`, transfers socket ownership to it and sends it
  #     `{:accepted, raw_socket}`. The child arms its own input
  #     (`active: :once`) and keeps backpressure and close handling. A TLS
  #     socket handed over is a `%SSL.Socket{}` the Connection owns; its exit
  #     closes the TLS process through the owner monitor.
  #   * The handler monitors the child and lives exactly as long as it, so
  #     Hell's connection accounting and shutdown cover it. Hell closes the
  #     socket whenever the handler exits. A socket closed by a non-owner sends
  #     the owner no `{:tcp_closed, _}`/`{:ssl_closed, _}`, so a stopping or failing handler
  #     stops its child itself.
  #   * Consequently a listener failure closes that listener's connections.
  #
  # Known limits:
  #
  #   * The handler sends `{:accepted, raw}` before Hell re-arms
  #     `active: :once`, but the first `{:tcp, raw, data}` comes from the
  #     port, a different sender. Erlang does not formally order messages from
  #     two senders; locally it is practically safe. A Connection whose socket
  #     is still nil drops such data.
  #   * A `:kill` of the handler skips its terminate, leaving the Connection
  #     serving its socket outside Hell's accounting.
  #   * The rebind retry runs in the listener's init, so it blocks the
  #     `GravitonMQ.Runtime.ListenerSupervisor` (and `GravitonMQ.status/1`,
  #     which lists its children) for up to `@rebind_budget` per conflicting
  #     listener.
  #   * Worst-case shutdown wall time: a listener supervisor stops its
  #     listeners sequentially in reverse order, so N TCP listeners take at
  #     most N x `owner_shutdown/0` (each listener's `stop/2` is bounded by
  #     the drain plus the residual `Hell.stop/2` wait).
  use Hell.Handler

  alias GravitonMQ.Runtime.TLSOptions

  require Logger

  # Shutdown budgets.
  #
  # Hell stops its acceptor subtrees one after another, so `stop/1` first
  # suspends accept and then releases every handler's Connection in
  # parallel; the final `Hell.stop/2` only meets handlers that missed that
  # drain, each bounded by Hell's `shutdown_timeout`.
  #
  #   release/1: GenServer.stop wait 5_000 + kill wait 500   <= 5_500
  #   handler shutdown_timeout and parallel drain wait          6_000
  #   Hell.stop/2 wait after the drain                          7_000
  #   listener child-spec shutdown >= drain + stop + margin    14_000
  @connection_stop 5_000
  @kill_wait 500
  @handler_shutdown 6_000
  @stop_wait @handler_shutdown + 1_000
  @owner_shutdown @handler_shutdown + @stop_wait + 1_000

  # A supervisor restarts a killed listener before the previous Hell tree,
  # which was linked to it, has closed its listen socket. Rebinding a fixed
  # port is retried briefly on exactly that address-in-use result.
  @rebind_budget 1_000
  @rebind_interval 20

  @num_acceptors 4
  @release :hell_handoff_release

  def owner_shutdown, do: @owner_shutdown

  @doc false
  def budgets,
    do: [
      connection_stop: @connection_stop,
      kill_wait: @kill_wait,
      handler_shutdown: @handler_shutdown,
      stop_wait: @stop_wait,
      owner_shutdown: @owner_shutdown
    ]

  # The calling listener owns the table, so it disappears with the listener.
  def table, do: :ets.new(__MODULE__, [:protected, read_concurrency: true])

  def publish(table, %{connections: _, child: _} = spec) do
    true = :ets.insert(table, {:spec, spec})
    :ok
  end

  # `settings`: %{send_timeout:, handshake_timeout:, max_connections:}.
  # `options` exist for tests only; production uses the defaults above.
  def start_link(listener, settings, table, options \\ []) do
    connection_stop = Keyword.get(options, :connection_stop, @connection_stop)

    options =
      [
        port: listener.port,
        transport_module: transport_module(listener.transport),
        transport_options: transport_options(listener, settings),
        handler_module: __MODULE__,
        handler_options: {self(), table, connection_stop},
        num_acceptors: @num_acceptors,
        read_timeout: :infinity,
        # The TLS handshake runs in the handler before handle_connection/2;
        # it also bounds the server flight write and sni_fun. A no-op for TCP.
        handshake_timeout: settings.handshake_timeout,
        shutdown_timeout: @handler_shutdown
      ] ++ connection_limit(listener.transport, settings)

    start(options, System.monotonic_time(:millisecond) + @rebind_budget)
  end

  defp transport_module(:tcp), do: Hell.Transports.TCP
  defp transport_module(:tls), do: Hell.Transports.SSL

  defp transport_options(listener, settings) do
    carrier = [
      ip: listener.ip,
      backlog: listener.backlog,
      reuseaddr: true,
      buffer: 16_384,
      recbuf: 32_768,
      send_timeout: settings.send_timeout,
      send_timeout_close: true,
      # Hell defaults nodelay to true; false preserves the pre-Hell
      # listener behaviour. Changing it is a separate decision.
      nodelay: false
    ]

    case listener.transport do
      :tcp ->
        carrier

      # Validated by TLSOptions in the protocol Config; passwords as closures.
      # ex_ssl coalesces decrypted input; bounding each active-once delivery
      # to the TCP `buffer` keeps the protocols' input limits unchanged
      # (runtime-owned; rejected in user tls_options).
      :tls ->
        carrier ++ [active_chunk_size: 16_384] ++ TLSOptions.hell_options(listener.tls_options)
    end
  end

  # A TLS connection costs a handler and an ex_ssl server process from accept,
  # so the protocol's max_connections also bounds in-progress handshakes. TCP
  # keeps Hell's default (S1a).
  defp connection_limit(:tls, settings), do: [num_connections: settings.max_connections]
  defp connection_limit(:tcp, _settings), do: []

  # Called by the listener that started `server`. Before publication,
  # handlers may be blocked in their `:connection_spec` call to this very
  # listener and cannot be drained; the parent's `:shutdown` exit stops Hell
  # in the background and those calls exit as soon as the listener does.
  def stop(server, table) do
    cond do
      not Process.alive?(server) ->
        :ok

      :ets.lookup(table, :spec) == [] ->
        Process.exit(server, :shutdown)
        :ok

      true ->
        _ = guarded(fn -> Hell.suspend(server) end)
        _ = guarded(fn -> drain(server) end)
        halt(server)
        :ok
    end
  end

  defp halt(server) do
    Hell.stop(server, @stop_wait)
  catch
    :exit, :timeout ->
      Logger.warning("Hell listener stop exceeded #{@stop_wait} ms")

    :exit, _already_stopped ->
      :ok
  end

  defp start(options, deadline) do
    case Hell.start_link(options) do
      {:error, {:shutdown, {:failed_to_start_child, :listener, :eaddrinuse}}} = error ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(@rebind_interval)
          start(options, deadline)
        else
          error
        end

      result ->
        result
    end
  end

  defp drain(server) do
    handlers =
      case Hell.connection_pids(server) do
        {:ok, handlers} -> handlers
        :error -> []
      end

    monitors =
      Enum.map(handlers, fn handler ->
        monitor = Process.monitor(handler)
        send(handler, @release)
        monitor
      end)

    deadline = System.monotonic_time(:millisecond) + @handler_shutdown

    Enum.each(monitors, fn monitor ->
      remaining = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
      after
        remaining -> Process.demonitor(monitor, [:flush])
      end
    end)
  end

  @impl Hell.Handler
  def handle_connection(socket, {listener, table, connection_stop}) do
    Process.flag(:sensitive, true)

    with {:listener, {:ok, %{connections: connections, child: child}}} <-
           {:listener, connection_spec(listener, table)},
         {:supervisor, {:ok, {:ok, connection}}} <-
           {:supervisor, guarded(fn -> DynamicSupervisor.start_child(connections, child) end)} do
      hand_off(socket, connections, connection, connection_stop)
    else
      {:listener, :unavailable} ->
        Logger.warning("Hell handoff closed a connection: its listener is unavailable")
        {:close, nil}

      {:supervisor, :unavailable} ->
        Logger.warning(
          "Hell handoff closed a connection: its connection supervisor is unavailable"
        )

        {:close, nil}

      {:supervisor, {:ok, _refused}} ->
        {:close, nil}
    end
  end

  # `handle_close/2` keeps Hell's default: in rc.13 the only `{:shutdown, _}`
  # stops of a live handler are this module's own (`:local_closed` after its
  # Connection is gone or released, `:released`), so nothing is left to
  # release there. Hell's `:shutdown` and failures reach the callbacks below.

  @impl Hell.Handler
  def handle_shutdown(_socket, state), do: release(state)

  @impl Hell.Handler
  def handle_error(_reason, _socket, state), do: release(state)

  @impl GenServer
  def handle_info(
        {:DOWN, monitor, :process, _pid, _reason},
        {socket, {_connections, _connection, monitor, _stop} = state}
      ),
      do: {:stop, {:shutdown, :local_closed}, {socket, state}}

  # Released before its socket was handed over: exit owning the raw socket,
  # which closes it.
  def handle_info(@release, {nil, _options} = state),
    do: {:stop, {:shutdown, :released}, state}

  def handle_info(@release, {socket, state}) do
    :ok = release(state)
    {:stop, {:shutdown, :local_closed}, {socket, :released}}
  end

  def handle_info(_stale, state), do: {:noreply, state}

  @impl GenServer
  def format_status(status),
    do:
      Map.merge(
        status,
        %{state: :redacted, message: :redacted, reason: :redacted, log: []}
      )

  # Before the listener has published its start argument, ask it directly;
  # the call waits through its startup and exits at once if it is gone.
  defp connection_spec(listener, table) do
    case :ets.lookup(table, :spec) do
      [{:spec, spec}] -> {:ok, spec}
      [] -> guarded(fn -> GenServer.call(listener, :connection_spec, :infinity) end)
    end
  rescue
    ArgumentError -> :unavailable
  end

  defp guarded(callback) do
    {:ok, callback.()}
  catch
    :exit, _reason -> :unavailable
  end

  defp hand_off(socket, connections, connection, connection_stop) do
    case socket.transport_module.controlling_process(socket.socket, connection) do
      :ok ->
        monitor = Process.monitor(connection)
        send(connection, {:accepted, socket.socket})
        {:continue, {connections, connection, monitor, connection_stop}, {:persistent, :infinity}}

      {:error, _reason} ->
        _ = DynamicSupervisor.terminate_child(connections, connection)
        {:close, nil}
    end
  end

  # A trapping Connection ignores exit signals from anyone but its
  # supervisor, so it is stopped synchronously, then killed past the budget.
  # Connections are temporary children; their supervisor just drops them.
  defp release({_connections, connection, monitor, connection_stop}) do
    try do
      GenServer.stop(connection, :shutdown, connection_stop)
    catch
      :exit, _reason -> Process.exit(connection, :kill)
    end

    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
    after
      @kill_wait -> :ok
    end
  end

  defp release(_state), do: :ok
end
