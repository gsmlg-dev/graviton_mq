defmodule GravitonMQ.Runtime.AMQP10.Listener do
  @moduledoc false
  use GenServer

  alias GravitonMQ.Runtime.AMQP10.{Broker, Connection}

  def start_link(configuration), do: GenServer.start_link(__MODULE__, configuration)

  def child_spec(options),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [fn -> options end]}}

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

    result =
      case listener.transport do
        :tcp -> :gen_tcp.listen(listener.port, options)
        :tls -> :ssl.listen(listener.port, options ++ listener.tls_options)
      end

    case result do
      {:ok, socket} ->
        {:ok, {ip, port}} =
          case listener.transport do
            :tcp -> :inet.sockname(socket)
            :tls -> :ssl.sockname(socket)
          end

        {:ok,
         %{
           socket: socket,
           transport: listener.transport,
           parent: parent,
           config: Map.drop(config, [:authorize, :listeners]),
           broker: nil,
           connections: nil,
           info: %{id: listener.id, transport: listener.transport, ip: ip, port: port}
         }, {:continue, :wire}}

      {:error, _} ->
        {:stop, {:listener_setup_failed, listener.transport}}
    end
  rescue
    _ -> {:stop, :listener_setup_failed}
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
    accepted =
      case state.transport do
        :tcp -> :gen_tcp.accept(state.socket, 25)
        :tls -> :ssl.transport_accept(state.socket, 25)
      end

    case accepted do
      {:ok, socket} ->
        case DynamicSupervisor.start_child(
               state.connections,
               {Connection, {state.config, state.broker, state.transport}}
             ) do
          {:ok, connection} ->
            transferred =
              case state.transport do
                :tcp -> :gen_tcp.controlling_process(socket, connection)
                :tls -> :ssl.controlling_process(socket, connection)
              end

            if transferred == :ok do
              send(connection, {:accepted, socket})
            else
              close(state.transport, socket)
              DynamicSupervisor.terminate_child(state.connections, connection)
            end

          {:error, _} ->
            close(state.transport, socket)
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

  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_, state), do: close(state.transport, state.socket)
  defp close(:tcp, socket), do: :gen_tcp.close(socket)
  defp close(:tls, socket), do: :ssl.close(socket)

  @impl true
  def format_status(status),
    do: Map.merge(status, %{state: :redacted, message: :redacted, reason: :redacted, log: []})
end
