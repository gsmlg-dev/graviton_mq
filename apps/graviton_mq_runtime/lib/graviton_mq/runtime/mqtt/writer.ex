defmodule GravitonMQ.Runtime.MQTT.Writer do
  @moduledoc false
  use GenServer, restart: :temporary

  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  # Obtain once, before admitting writes. Admission never calls the writer,
  # which may be waiting for the socket's configured send timeout.
  def handle(writer), do: GenServer.call(writer, :handle)

  def enqueue(handle, send_id, bytes) when is_binary(bytes) do
    size = byte_size(bytes)

    try do
      [packets, total] = :ets.update_counter(handle.table, :credits, [{2, 1}, {3, size}])

      if packets <= handle.max_packets and total <= handle.max_bytes do
        # An encoded sub-binary must not retain a larger backing allocation.
        bytes =
          if :binary.referenced_byte_size(bytes) > size, do: :binary.copy(bytes), else: bytes

        token = make_ref()
        true = :ets.insert(handle.table, {token, send_id, bytes})
        # sys keeps debug-ring messages outside format_status/1. Keep both the
        # packet and its caller's correlation value out of that ring entirely.
        send(handle.pid, {:write, token})
        :ok
      else
        :ets.update_counter(handle.table, :credits, [{2, -1}, {3, -size}])
        {:error, :writer_full}
      end
    rescue
      ArgumentError -> {:error, :closed}
    end
  end

  @impl true
  def init(options) do
    # proc_lib crash reports must also omit queued encoded packets.
    Process.flag(:sensitive, true)
    protect(fn -> initialize(options) end)
  end

  defp initialize(options) do
    owner = Keyword.fetch!(options, :owner)
    lifetime_owner = Keyword.fetch!(options, :lifetime_owner)
    max_packets = Keyword.fetch!(options, :max_packets)
    max_bytes = Keyword.fetch!(options, :max_bytes)
    true = is_integer(max_packets) and max_packets > 0
    true = is_integer(max_bytes) and max_bytes > 0
    table = :ets.new(__MODULE__, [:set, :public, write_concurrency: true])
    true = :ets.insert(table, {:credits, 0, 0})

    {:ok,
     %{
       owner: owner,
       lifetime_owner: lifetime_owner,
       owner_monitor: Process.monitor(owner),
       lifetime_monitor: Process.monitor(lifetime_owner),
       connection_id: Keyword.fetch!(options, :connection_id),
       generation: Keyword.fetch!(options, :generation),
       socket: Keyword.fetch!(options, :socket),
       transport: Keyword.fetch!(options, :transport),
       handle: %{pid: self(), table: table, max_packets: max_packets, max_bytes: max_bytes}
     }}
  end

  @impl true
  def handle_call(request, from, state), do: protect(fn -> call(request, from, state) end)

  defp call(:handle, _from, state), do: {:reply, state.handle, state}

  @impl true
  def handle_info(message, state), do: protect(fn -> info(message, state) end)

  defp info({:write, token}, state) do
    # DOWN can be behind already admitted writes in the mailbox. Do not drain
    # that queue after its broker or reader has died.
    if Process.alive?(state.owner) and Process.alive?(state.lifetime_owner) do
      [{^token, send_id, bytes}] = :ets.take(state.handle.table, token)
      result = socket_send(state.transport, state.socket, bytes)
      :ets.update_counter(state.handle.table, :credits, [{2, -1}, {3, -byte_size(bytes)}])

      send(
        state.owner,
        {:mqtt_write, state.connection_id, state.generation, send_id, result}
      )

      case result do
        :ok -> {:noreply, state}
        {:error, _reason} -> {:stop, {:shutdown, :socket_write_failed}, state}
      end
    else
      {:stop, :normal, state}
    end
  end

  defp info({:DOWN, monitor, :process, _pid, _reason}, state)
       when monitor == state.owner_monitor or monitor == state.lifetime_monitor,
       do: {:stop, :normal, state}

  defp info(_message, state), do: {:noreply, state}

  @impl true
  def format_status(status),
    do: Map.merge(status, %{state: :redacted, message: :redacted, reason: :redacted, log: []})

  defp socket_send(:tcp, socket, bytes), do: :gen_tcp.send(socket, bytes)
  defp socket_send(:tls, socket, bytes), do: :ssl.send(socket, bytes)

  # Callback exceptions must not expose packet arguments through proc_lib or
  # the reader's linked EXIT. Retain the class and source locations only.
  defp protect(callback) do
    try do
      callback.()
    rescue
      exception ->
        :erlang.raise(
          :error,
          {:mqtt_writer_callback_failed, exception.__struct__},
          private_stacktrace(__STACKTRACE__)
        )
    catch
      kind, _reason ->
        :erlang.raise(
          kind,
          {:mqtt_writer_callback_failed, kind},
          private_stacktrace(__STACKTRACE__)
        )
    end
  end

  defp private_stacktrace(stacktrace) do
    Enum.map(stacktrace, fn
      {module, function, arguments, location} when is_list(arguments) ->
        {module, function, length(arguments), location}

      frame ->
        frame
    end)
  end
end
