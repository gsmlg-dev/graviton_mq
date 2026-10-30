defmodule GravitonMQ.Client.Test.PubackLossProxy do
  @moduledoc false
  use GenServer
  import Bitwise

  def start_link(options), do: GenServer.start_link(__MODULE__, options)
  def port(owner), do: GenServer.call(owner, :port)
  def snapshot(owner), do: GenServer.call(owner, :snapshot)

  @impl true
  def init(options) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])

    {:ok, {_, port}} = :inet.sockname(listener)
    owner = self()
    acceptor = spawn_link(fn -> accept(listener, owner) end)

    {:ok,
     %{
       listener: listener,
       port: port,
       acceptor: acceptor,
       upstream_port: Keyword.fetch!(options, :upstream_port),
       observer: Keyword.fetch!(options, :observer),
       drop_packet_type: Keyword.get(options, :drop_packet_type, 4),
       drop_direction: Keyword.get(options, :drop_direction, :broker),
       close_on_drop: Keyword.get(options, :close_on_drop, true),
       connection: 0,
       client: nil,
       broker: nil,
       client_buffer: <<>>,
       broker_buffer: <<>>,
       dropped: false
     }}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  def handle_call(:snapshot, _from, state),
    do:
      {:reply,
       %{connections: state.connection, dropped: state.dropped, open: state.client != nil}, state}

  @impl true
  def handle_info({:accepted, client}, %{client: nil} = state) do
    {:ok, broker} =
      :gen_tcp.connect({127, 0, 0, 1}, state.upstream_port, [:binary, active: false], 1_000)

    :ok = :inet.setopts(client, active: :once)
    :ok = :inet.setopts(broker, active: :once)
    {:noreply, %{state | client: client, broker: broker, connection: state.connection + 1}}
  end

  def handle_info({:accepted, client}, state) do
    :gen_tcp.close(client)
    {:stop, :unexpected_concurrent_connection, state}
  end

  def handle_info({:tcp, socket, bytes}, %{client: socket} = state) do
    state = relay(%{state | client_buffer: state.client_buffer <> bytes}, :client)
    if state.client, do: :inet.setopts(state.client, active: :once)
    {:noreply, state}
  end

  def handle_info({:tcp, socket, bytes}, %{broker: socket} = state) do
    state = relay(%{state | broker_buffer: state.broker_buffer <> bytes}, :broker)
    if state.broker, do: :inet.setopts(state.broker, active: :once)
    {:noreply, state}
  end

  def handle_info({kind, socket}, state) when kind == :tcp_closed do
    if socket in [state.client, state.broker],
      do: {:noreply, close_pair(state)},
      else: {:noreply, state}
  end

  def handle_info({:tcp_error, socket, reason}, state) do
    if socket in [state.client, state.broker],
      do: {:stop, {:proxy_socket_error, reason}, close_pair(state)},
      else: {:noreply, state}
  end

  def handle_info({:tcp, _old_socket, _bytes}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    close_pair(state)
    :gen_tcp.close(state.listener)
    Process.exit(state.acceptor, :shutdown)
  end

  defp accept(listener, owner) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        :ok = :gen_tcp.controlling_process(socket, owner)
        send(owner, {:accepted, socket})
        accept(listener, owner)

      {:error, :closed} ->
        :ok
    end
  end

  defp relay(state, direction) do
    key = if direction == :client, do: :client_buffer, else: :broker_buffer

    case envelope(Map.fetch!(state, key)) do
      :more ->
        state

      {:ok, header, body, wire, rest} ->
        state = Map.put(state, key, rest)

        if direction == state.drop_direction and header >>> 4 == state.drop_packet_type and
             not state.dropped do
          <<packet_id::16, _::binary>> = body

          if direction == :broker and state.drop_packet_type == 4,
            do: notify(state, {:puback_dropped, state.connection, packet_id})

          notify(
            state,
            {:ack_dropped, direction, state.drop_packet_type, state.connection, packet_id}
          )

          state = %{state | dropped: true}
          if state.close_on_drop, do: close_pair(state), else: state
        else
          observe(state, direction, header, body)
          target = if direction == :client, do: state.broker, else: state.client
          :ok = :gen_tcp.send(target, wire)
          relay(state, direction)
        end
    end
  end

  # Independent MQTT envelope parsing: no GravitonMQ codec is used as an oracle.
  defp envelope(<<>>), do: :more

  defp envelope(<<header, rest::binary>> = wire) when byte_size(wire) <= 1_048_576 do
    case variable_integer(rest, 0, 0) do
      :more ->
        :more

      {:ok, length, body_and_rest} when byte_size(body_and_rest) < length ->
        :more

      {:ok, length, body_and_rest} ->
        <<body::binary-size(length), rest::binary>> = body_and_rest
        consumed = byte_size(wire) - byte_size(rest)
        {:ok, header, body, binary_part(wire, 0, consumed), rest}
    end
  end

  defp variable_integer(<<>>, _value, _shift), do: :more

  defp variable_integer(<<byte, rest::binary>>, value, shift) when shift < 28 do
    value = value + ((byte &&& 0x7F) <<< shift)

    if (byte &&& 0x80) == 0,
      do: {:ok, value, rest},
      else: variable_integer(rest, value, shift + 7)
  end

  defp observe(state, :client, 0x10, <<0, 4, "MQTT", 5, flags, _keep_alive::16, rest::binary>>) do
    {:ok, property_length, rest} = variable_integer(rest, 0, 0)

    <<_properties::binary-size(property_length), length::16, id::binary-size(length), _::binary>> =
      rest

    notify(state, {:connect, state.connection, %{clean_start: (flags &&& 2) != 0, client_id: id}})
  end

  defp observe(state, direction, header, body) when header >>> 4 == 3 do
    <<length::16, topic::binary-size(length), rest::binary>> = body
    qos = header >>> 1 &&& 3

    {packet_id, content} =
      if qos == 0,
        do: {nil, rest},
        else:
          (
            <<id::16, content::binary>> = rest
            {id, content}
          )

    event = if direction == :client, do: :publish, else: :incoming_publish

    notify(
      state,
      {event, state.connection,
       %{
         dup: (header &&& 8) != 0,
         qos: header >>> 1 &&& 3,
         packet_id: packet_id,
         topic: topic,
         content: content,
         header: header,
         body: body
       }}
    )
  end

  defp observe(state, :broker, 0x20, <<flags, reason, _::binary>>) do
    notify(
      state,
      {:connack, state.connection, %{session_present: (flags &&& 1) != 0, reason_code: reason}}
    )
  end

  defp observe(state, direction, header, <<packet_id::16, _::binary>>)
       when (header >>> 4) in [4, 5, 6, 7] do
    if direction == :broker and header >>> 4 == 4,
      do: notify(state, {:puback_forwarded, state.connection, packet_id})

    notify(state, {:ack_forwarded, direction, header >>> 4, state.connection, packet_id})
  end

  defp observe(_state, _direction, _header, _body), do: :ok

  defp notify(state, fact), do: send(state.observer, {:puback_loss_proxy, self(), fact})

  defp close_pair(state) do
    for socket <- [state.client, state.broker], socket != nil, do: :gen_tcp.close(socket)
    %{state | client: nil, broker: nil, client_buffer: <<>>, broker_buffer: <<>>}
  end
end
