defmodule GravitonMQ.Runtime.AMQP10.Listener do
  @moduledoc false
  use GenServer

  alias GravitonMQ.Runtime.AMQP10.{Authenticator, Broker, Connection}
  alias GravitonMQ.Runtime.{HellHandoff, TLSOptions}

  def start_link(configuration), do: GenServer.start_link(__MODULE__, configuration)

  # TCP and TLS listeners share this shutdown; it exceeds the bounded
  # Hell stop so the parent never kills a listener mid-stop.
  # The connection spec is published in a :protected ETS table that any
  # process can read, so only the keys the Connection uses are kept
  # (allowlist): future auth or credential options are never published.
  @connection_keys [
    :connect_timeout,
    :send_timeout,
    :max_input_bytes,
    :max_input_frames_per_turn,
    :endpoint_options,
    # Non-secret SASL settings; the authentication policy is never in it.
    :sasl
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
    handoff = HellHandoff.table()

    case HellHandoff.start_link(listener, handoff_settings(config), handoff) do
      {:ok, server} ->
        {:ok, {ip, port}} = Hell.listener_info(server)

        {:ok,
         %{
           socket: server,
           handoff: handoff,
           transport: listener.transport,
           parent: parent,
           config:
             config
             |> Map.take(@connection_keys)
             |> Map.update!(
               :sasl,
               &Map.put(
                 &1,
                 :client_certificate_verified,
                 listener.transport == :tls and TLSOptions.verify_peer?(listener.tls_options)
               )
             ),
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
  # parent's exit. Accept (TCP and TLS) runs in Hell meanwhile; handlers accepted
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

  def handle_call(:connection_spec, from, %{broker: nil} = state),
    do: {:noreply, %{state | waiting: [from | state.waiting]}}

  def handle_call(:connection_spec, _from, state),
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
           List.keyfind(children, :connections, 0),
         {_, authenticator, _, _} when is_pid(authenticator) <-
           List.keyfind(children, Authenticator, 0) do
      # The Authenticator pid is published; its policy never is.
      state = %{
        state
        | broker: broker,
          connections: connections,
          config: put_in(state.config, [:sasl, :authenticator], authenticator)
      }

      :ok = HellHandoff.publish(state.handoff, connection_spec(state))

      state.waiting
      |> Enum.reverse()
      |> Enum.each(&GenServer.reply(&1, connection_spec(state)))

      {:noreply, %{state | waiting: []}}
    else
      _not_running -> {:stop, {:shutdown, :wiring_failed}, state}
    end
  end

  defp info({:EXIT, server, _reason}, %{socket: server} = state),
    do: {:stop, :listener_closed, state}

  defp info(_stale, state), do: {:noreply, state}

  defp connection_spec(state),
    do: %{
      connections: state.connections,
      child: {Connection, {state.config, state.broker, state.transport}}
    }

  @impl true
  def terminate(_reason, %{socket: server, handoff: table}), do: HellHandoff.stop(server, table)

  @impl true
  def format_status(status),
    do: Map.merge(status, %{state: :redacted, message: :redacted, reason: :redacted, log: []})

  defp handoff_settings(config),
    do: %{
      send_timeout: config.send_timeout,
      handshake_timeout: config.connect_timeout,
      max_connections: config.max_connections
    }
end
