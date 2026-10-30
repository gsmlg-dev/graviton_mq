defmodule GravitonMQ.Runtime.MQTT.Listener do
  @moduledoc false
  use GenServer

  alias GravitonMQ.Runtime.MQTT.{Broker, Connection}

  def start_link(configuration), do: GenServer.start_link(__MODULE__, configuration)

  def child_spec(options),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [fn -> options end]}}

  def info(listener), do: GenServer.call(listener, :info)

  @impl true
  def init(configuration) do
    Process.flag(:trap_exit, true)
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

    opened =
      case listener.transport do
        :tcp -> :gen_tcp.listen(listener.port, options)
        :tls -> :ssl.listen(listener.port, options ++ listener.tls_options)
      end

    case opened do
      {:ok, socket} ->
        address =
          case listener.transport do
            :tcp -> :inet.sockname(socket)
            :tls -> :ssl.sockname(socket)
          end

        {:ok, {ip, port}} = address

        config =
          config
          |> Map.take([
            :limits,
            :connect_timeout,
            :send_timeout,
            :max_writer_packets,
            :max_writer_bytes
          ])
          |> Map.put(
            :client_certificate_verified,
            Keyword.get(listener.tls_options, :verify, :verify_none) == :verify_peer
          )

        {:ok,
         %{
           config: config,
           parent: parent,
           socket: socket,
           transport: listener.transport,
           info: %{id: listener.id, transport: listener.transport, ip: ip, port: port},
           broker: nil,
           connections: nil
         }, {:continue, :wire}}

      {:error, _reason} ->
        {:stop, {:listener_setup_failed, listener.transport}}
    end
  end

  @impl true
  def handle_continue(:wire, state) do
    children = Supervisor.which_children(state.parent)
    {_, broker, _, _} = List.keyfind(children, Broker, 0)
    {_, connections, _, _} = List.keyfind(children, :connections, 0)
    send(self(), :accept)
    {:noreply, %{state | broker: broker, connections: connections}}
  end

  @impl true
  def handle_call(:info, _from, state), do: {:reply, state.info, state}

  @impl true
  def handle_info(:accept, state) do
    result =
      case state.transport do
        :tcp -> :gen_tcp.accept(state.socket, 50)
        :tls -> :ssl.transport_accept(state.socket, 50)
      end

    case result do
      {:ok, socket} ->
        case DynamicSupervisor.start_child(
               state.connections,
               {Connection, {state.config, state.broker, state.transport}}
             ) do
          {:ok, reader} ->
            transferred =
              case state.transport do
                :tcp -> :gen_tcp.controlling_process(socket, reader)
                :tls -> :ssl.controlling_process(socket, reader)
              end

            case transferred do
              :ok ->
                send(reader, {:accepted, socket})

              {:error, _reason} ->
                close(state.transport, socket)
                Process.exit(reader, :kill)
            end

          {:error, _reason} ->
            close(state.transport, socket)
        end

        send(self(), :accept)
        {:noreply, state}

      {:error, :timeout} ->
        send(self(), :accept)
        {:noreply, state}

      {:error, _reason} ->
        {:stop, :listener_closed, state}
    end
  end

  def handle_info(_stale, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state), do: close(state.transport, state.socket)
  defp close(:tcp, socket), do: :gen_tcp.close(socket)
  defp close(:tls, socket), do: :ssl.close(socket)

  @impl true
  def format_status(status),
    do:
      Map.merge(
        status,
        %{state: :redacted, message: :redacted, reason: :redacted, log: []}
      )
end
