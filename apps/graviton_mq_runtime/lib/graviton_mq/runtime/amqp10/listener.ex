defmodule GravitonMQ.Runtime.AMQP10.Listener do
  @moduledoc false
  use GenServer

  alias GravitonMQ.Runtime.AMQP10.{Broker, Connection}
  alias GravitonMQ.Runtime.HellHandoff

  def start_link(configuration), do: GenServer.start_link(__MODULE__, configuration)

  # TCP and TLS listeners share this shutdown; it exceeds the bounded
  # Hell stop so the parent never kills a TCP listener mid-stop.
  # The connection spec is published in a :protected ETS table that any
  # process can read, so only the keys the Connection uses are kept
  # (allowlist): future auth or credential options are never published.
  @connection_keys [
    :connect_timeout,
    :send_timeout,
    :max_input_bytes,
    :max_input_frames_per_turn,
    :endpoint_options
  ]

  def child_spec(options),
    do: %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [fn -> options end]},
      shutdown: HellHandoff.owner_shutdown()
    }

  def info(pid), do: GenServer.call(pid, :info)

  @impl true
  def init(configuration) do
    Process.flag(:trap_exit, true)
    Process.flag(:sensitive, true)
    {config, listener, parent} = configuration.()

    options = [
      :binary,
      ip: listener.ip,
      packet: 0,
      active: false,
      reuseaddr: true,
      backlog: listener.backlog,
      buffer: 16_384,
      recbuf: 32_768,
      send_timeout: config.send_timeout,
      send_timeout_close: true
    ]

    handoff = if listener.transport == :tcp, do: HellHandoff.table()

    result =
      case listener.transport do
        :tcp -> HellHandoff.start_link(listener, config.send_timeout, handoff)
        :tls -> :ssl.listen(listener.port, options ++ listener.tls_options)
      end

    case result do
      {:ok, socket} ->
        {:ok, {ip, port}} =
          case listener.transport do
            :tcp -> Hell.listener_info(socket)
            :tls -> :ssl.sockname(socket)
          end

        {:ok,
         %{
           socket: socket,
           handoff: handoff,
           transport: listener.transport,
           parent: parent,
           config: Map.take(config, @connection_keys),
           broker: nil,
           connections: nil,
           wiring: nil,
           waiting: [],
           info: %{id: listener.id, transport: listener.transport, ip: ip, port: port}
         }, {:continue, :wire}}

      {:error, _} ->
        {:stop, {:listener_setup_failed, listener.transport}}
    end
  rescue
    _ -> {:stop, :listener_setup_failed}
  end

  # Wiring asks the protocol supervisor for the Broker and connections
  # without blocking: that supervisor may itself be stopping this listener's
  # supervisor, which only works while this process still serves its own
  # parent's exit. Plain TCP accept runs in Hell meanwhile; handlers accepted
  # before wiring wait in `:connection_spec` and later read the published
  # value.
  # `:which_children` is the request `Supervisor.which_children/1` sends; OTP
  # has no public asynchronous form, and a blocking call here can deadlock
  # with a supervisor that is terminating this listener's ancestors.
  @impl true
  def handle_continue(:wire, state),
    do: {:noreply, %{state | wiring: :gen_server.send_request(state.parent, :which_children)}}

  @impl true
  def handle_call(:info, _from, state), do: {:reply, state.info, state}

  def handle_call(:connection_spec, from, %{transport: :tcp, broker: nil} = state),
    do: {:noreply, %{state | waiting: [from | state.waiting]}}

  def handle_call(:connection_spec, _from, %{transport: :tcp} = state),
    do: {:reply, connection_spec(state), state}

  @impl true
  def handle_info(message, %{wiring: request} = state) when request != nil do
    case :gen_server.check_response(message, request) do
      {:reply, children} -> wire(children, %{state | wiring: nil})
      {:error, {_reason, _parent}} -> {:stop, {:shutdown, :wiring_failed}, state}
      :no_reply -> info(message, state)
    end
  end

  def handle_info(message, state), do: info(message, state)

  defp wire(children, state) do
    with {_, broker, _, _} when is_pid(broker) <- List.keyfind(children, Broker, 0),
         {_, connections, _, _} when is_pid(connections) <-
           List.keyfind(children, :connections, 0) do
      state = %{state | broker: broker, connections: connections}

      case state.transport do
        :tcp ->
          :ok = HellHandoff.publish(state.handoff, connection_spec(state))

          state.waiting
          |> Enum.reverse()
          |> Enum.each(&GenServer.reply(&1, connection_spec(state)))

        :tls ->
          send(self(), :accept)
      end

      {:noreply, %{state | waiting: []}}
    else
      _not_running -> {:stop, {:shutdown, :wiring_failed}, state}
    end
  end

  defp info({:EXIT, server, _reason}, %{transport: :tcp, socket: server} = state),
    do: {:stop, :listener_closed, state}

  defp info(:accept, %{transport: :tls} = state) do
    case :ssl.transport_accept(state.socket, 25) do
      {:ok, socket} ->
        case DynamicSupervisor.start_child(
               state.connections,
               {Connection, {state.config, state.broker, :tls}}
             ) do
          {:ok, connection} ->
            if :ssl.controlling_process(socket, connection) == :ok do
              send(connection, {:accepted, socket})
            else
              :ssl.close(socket)
              DynamicSupervisor.terminate_child(state.connections, connection)
            end

          {:error, _} ->
            :ssl.close(socket)
        end

        send(self(), :accept)
        {:noreply, state}

      {:error, :timeout} ->
        send(self(), :accept)
        {:noreply, state}

      {:error, _} ->
        {:stop, :listener_closed, state}
    end
  end

  defp info(_stale, state), do: {:noreply, state}

  defp connection_spec(state),
    do: %{connections: state.connections, child: {Connection, {state.config, state.broker, :tcp}}}

  @impl true
  def terminate(_reason, %{transport: :tcp, socket: server, handoff: table}),
    do: HellHandoff.stop(server, table)

  def terminate(_reason, %{transport: :tls, socket: socket}), do: :ssl.close(socket)

  @impl true
  def format_status(status),
    do: Map.merge(status, %{state: :redacted, message: :redacted, reason: :redacted, log: []})
end
