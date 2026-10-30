defmodule GravitonMQ.Runtime.MQTT.Connection do
  @moduledoc false
  use GenServer, restart: :temporary

  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.Runtime.MQTT.Writer

  def start_link(configuration), do: GenServer.start_link(__MODULE__, configuration)

  def child_spec(options),
    do: %{
      id: __MODULE__,
      restart: :temporary,
      start: {__MODULE__, :start_link, [fn -> options end]}
    }

  @impl true
  def init(configuration), do: protect(fn -> initialize(configuration.()) end)

  defp initialize({config, broker, transport}) do
    Process.flag(:trap_exit, true)
    Process.flag(:sensitive, true)

    case GenServer.call(broker, {:connection_open, self(), nil}) do
      {:ok, id, generation} ->
        {:ok,
         %{
           config: config,
           broker: broker,
           broker_monitor: Process.monitor(broker),
           transport: transport,
           socket: nil,
           writer: nil,
           id: id,
           generation: generation,
           buffer: <<>>,
           closing: false
         }}

      {:error, reason} ->
        {:stop, {:shutdown, reason}}
    end
  end

  @impl true
  def handle_info(message, state), do: protect(fn -> info(message, state) end)

  defp info({:accepted, socket}, %{socket: nil} = state) do
    case handshake(state.transport, socket, state.config.connect_timeout) do
      {:ok, socket} ->
        state = %{state | socket: socket}

        options = [
          owner: state.broker,
          lifetime_owner: self(),
          connection_id: state.id,
          generation: state.generation,
          socket: socket,
          transport: state.transport,
          max_packets: state.config.max_writer_packets,
          max_bytes: state.config.max_writer_bytes
        ]

        with {:ok, writer} <- Writer.start_link(options),
             handle <- Writer.handle(writer),
             :ok <-
               GenServer.call(
                 state.broker,
                 {:writer_ready, state.id, state.generation, handle, peer(state)}
               ) do
          {:noreply, arm(%{state | writer: writer})}
        else
          _failure -> {:stop, :normal, state}
        end

      {:error, _reason} ->
        close_socket(state.transport, socket)
        {:stop, :normal, state}
    end
  end

  defp info({:tcp, socket, bytes}, %{socket: socket, transport: :tcp} = state),
    do: received(state, bytes)

  defp info({:ssl, socket, bytes}, %{socket: socket, transport: :tls} = state),
    do: received(state, bytes)

  defp info(:parse, %{closing: false} = state), do: parse(state)
  defp info({:tcp_closed, socket}, %{socket: socket} = state), do: lost(state)
  defp info({:ssl_closed, socket}, %{socket: socket} = state), do: lost(state)
  defp info({:tcp_error, socket, _reason}, %{socket: socket} = state), do: lost(state)
  defp info({:ssl_error, socket, _reason}, %{socket: socket} = state), do: lost(state)
  defp info({:mqtt_close, _reason}, state), do: {:stop, :normal, %{state | closing: true}}
  defp info({:EXIT, writer, _reason}, %{writer: writer} = state), do: lost(state)

  defp info({:DOWN, monitor, :process, _pid, _reason}, %{broker_monitor: monitor} = state),
    do: {:stop, :normal, state}

  defp info(_stale, state), do: {:noreply, state}

  defp received(%{closing: true} = state, _bytes), do: {:noreply, state}

  defp received(state, bytes) do
    if byte_size(state.buffer) + byte_size(bytes) <= state.config.limits.max_packet_size + 16_384,
      do: parse(%{state | buffer: state.buffer <> bytes}),
      else: protocol_error(state, :parser_limit)
  end

  defp parse(%{buffer: <<>>} = state), do: {:noreply, arm(state)}

  defp parse(state) do
    version =
      case state.buffer do
        <<1::4, _::4, _::binary>> -> :auto
        _ -> :v5
      end

    case Packet.decode(state.buffer, version, state.config.limits) do
      {:ok, packet, rest} ->
        encoded = binary_part(state.buffer, 0, byte_size(state.buffer) - byte_size(rest))

        {packet, encoded} =
          if :binary.referenced_byte_size(encoded) > byte_size(encoded) do
            encoded = :binary.copy(encoded)
            {:ok, packet, <<>>} = Packet.decode(encoded, version, state.config.limits)
            {packet, encoded}
          else
            {packet, encoded}
          end

        case GenServer.call(state.broker, {:packet, state.id, state.generation, packet, encoded}) do
          :ok ->
            state = %{state | buffer: rest}

            if rest == <<>>,
              do: {:noreply, arm(state)},
              else:
                (
                  send(self(), :parse)
                  {:noreply, state}
                )

          {:error, _reason} ->
            {:noreply, %{state | buffer: <<>>, closing: true}}
        end

      {:more, _needed} ->
        {:noreply, arm(state)}

      {:error, error} ->
        protocol_error(state, {error.class, error.reason})
    end
  end

  defp protocol_error(state, reason) do
    GenServer.call(state.broker, {:protocol_error, state.id, state.generation, reason})
    {:noreply, %{state | closing: true, buffer: <<>>}}
  end

  defp lost(state) do
    GenServer.call(state.broker, {:connection_down, state.id, state.generation, :transport_loss})
    {:stop, :normal, state}
  end

  defp arm(%{closing: true} = state), do: state

  defp arm(state) do
    result =
      case state.transport do
        :tcp -> :inet.setopts(state.socket, active: :once)
        :tls -> :ssl.setopts(state.socket, active: :once)
      end

    case result do
      :ok ->
        state

      {:error, _reason} ->
        send(self(), {:mqtt_close, :closed})
        state
    end
  end

  defp peer(state) do
    address =
      case state.transport do
        :tcp -> :inet.peername(state.socket)
        :tls -> :ssl.peername(state.socket)
      end

    context =
      case address do
        {:ok, {ip, port}} -> %{ip: ip, port: port, transport: state.transport}
        _ -> %{transport: state.transport}
      end

    certificate =
      if state.transport == :tls do
        case :ssl.peercert(state.socket) do
          {:ok, der} -> der
          _ -> nil
        end
      end

    Map.merge(context, %{
      certificate: certificate,
      certificate_verified: certificate != nil and state.config.client_certificate_verified
    })
  end

  defp handshake(:tcp, socket, _timeout), do: {:ok, socket}
  defp handshake(:tls, socket, timeout), do: :ssl.handshake(socket, timeout)
  defp close_socket(:tcp, socket), do: :gen_tcp.close(socket)
  defp close_socket(:tls, socket), do: :ssl.close(socket)

  @impl true
  def terminate(reason, state), do: protect(fn -> finish(reason, state) end)

  defp finish(_reason, state) do
    if state.socket, do: close_socket(state.transport, state.socket)
    if state.writer && Process.alive?(state.writer), do: Process.exit(state.writer, :shutdown)
    send(state.broker, {:reader_down, state.id, state.generation})
    :ok
  end

  @impl true
  def format_status(status),
    do:
      Map.merge(
        status,
        %{state: :redacted, message: :redacted, reason: :redacted, log: []}
      )

  defp protect(callback) do
    try do
      callback.()
    catch
      kind, _reason ->
        stack =
          Enum.map(__STACKTRACE__, fn
            {module, function, args, location} when is_list(args) ->
              {module, function, length(args), location}

            frame ->
              frame
          end)

        :erlang.raise(kind, {:mqtt_connection_callback_failed, kind}, stack)
    end
  end
end
