defmodule GravitonMQ.Client.Test.MQTTWireTap do
  @moduledoc false
  # Byte relay for interoperability evidence. It forwards every byte unchanged
  # and records PUBLISH topic, Topic Alias and Subscription Identifiers with an
  # independent parser (no GravitonMQ codec is used as an oracle). It is a
  # recorder, not an MQTT peer.
  use GenServer
  import Bitwise

  def start_link(options), do: GenServer.start_link(__MODULE__, options)
  def port(tap), do: GenServer.call(tap, :port)
  def observed(tap), do: GenServer.call(tap, :observed)
  def drop(tap), do: GenServer.call(tap, :drop)

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
       connections: 0,
       sockets: %{},
       observed: []
     }}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}
  def handle_call(:observed, _from, state), do: {:reply, Enum.reverse(state.observed), state}

  def handle_call(:drop, _from, state) do
    for {socket, _} <- state.sockets, do: :gen_tcp.close(socket)
    {:reply, :ok, %{state | sockets: %{}}}
  end

  @impl true
  def handle_info({:accepted, client}, state) do
    {:ok, server} =
      :gen_tcp.connect({127, 0, 0, 1}, state.upstream_port, [:binary, active: false], 1_000)

    connection = state.connections + 1

    sockets =
      state.sockets
      |> Map.put(client, %{
        peer: server,
        direction: :to_server,
        connection: connection,
        buffer: <<>>
      })
      |> Map.put(server, %{
        peer: client,
        direction: :to_client,
        connection: connection,
        buffer: <<>>
      })

    :ok = :inet.setopts(client, active: :once)
    :ok = :inet.setopts(server, active: :once)
    {:noreply, %{state | connections: connection, sockets: sockets}}
  end

  def handle_info({:tcp, socket, bytes}, state) do
    case Map.fetch(state.sockets, socket) do
      {:ok, entry} ->
        state = relay(state, socket, %{entry | buffer: entry.buffer <> bytes})
        if Map.has_key?(state.sockets, socket), do: :inet.setopts(socket, active: :once)
        {:noreply, state}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({closed, socket}, state) when closed in [:tcp_closed, :tcp_error] do
    {:noreply, close_pair(state, socket)}
  end

  def handle_info({:tcp_error, socket, _reason}, state), do: {:noreply, close_pair(state, socket)}

  @impl true
  def terminate(_reason, state) do
    for {socket, _} <- state.sockets, do: :gen_tcp.close(socket)
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

  defp close_pair(state, socket) do
    case Map.fetch(state.sockets, socket) do
      {:ok, %{peer: peer}} ->
        :gen_tcp.close(socket)
        :gen_tcp.close(peer)
        %{state | sockets: state.sockets |> Map.delete(socket) |> Map.delete(peer)}

      :error ->
        state
    end
  end

  defp relay(state, socket, entry) do
    case envelope(entry.buffer) do
      :more ->
        %{state | sockets: Map.put(state.sockets, socket, entry)}

      {:ok, header, body, wire, rest} ->
        :ok = :gen_tcp.send(entry.peer, wire)
        state = observe(state, entry, header, body)
        relay(state, socket, %{entry | buffer: rest})
    end
  end

  defp envelope(<<>>), do: :more

  defp envelope(<<header, rest::binary>> = wire) do
    case variable_integer(rest, 0, 0) do
      :more ->
        :more

      {:ok, length, body_and_rest} when byte_size(body_and_rest) < length ->
        :more

      {:ok, length, body_and_rest} ->
        <<body::binary-size(length), rest::binary>> = body_and_rest
        {:ok, header, body, binary_part(wire, 0, byte_size(wire) - byte_size(rest)), rest}
    end
  end

  defp variable_integer(<<>>, _value, _shift), do: :more

  defp variable_integer(<<byte, rest::binary>>, value, shift) when shift < 28 do
    value = value + ((byte &&& 0x7F) <<< shift)

    if (byte &&& 0x80) == 0,
      do: {:ok, value, rest},
      else: variable_integer(rest, value, shift + 7)
  end

  defp observe(state, entry, header, body) when header >>> 4 == 3 do
    qos = header >>> 1 &&& 3
    <<length::16, topic::binary-size(length), rest::binary>> = body
    rest = if qos == 0, do: rest, else: binary_part(rest, 2, byte_size(rest) - 2)
    {:ok, property_length, rest} = variable_integer(rest, 0, 0)
    <<properties::binary-size(property_length), _payload::binary>> = rest

    fact =
      Map.merge(
        %{
          direction: entry.direction,
          connection: entry.connection,
          topic: topic,
          qos: qos,
          dup: (header &&& 8) != 0
        },
        properties(properties, %{topic_alias: nil, subscription_identifiers: []})
      )

    send(state.observer, {:mqtt_wire_tap, self(), fact})
    %{state | observed: [fact | state.observed]}
  end

  defp observe(state, _entry, _header, _body), do: state

  defp properties(<<>>, acc), do: acc

  defp properties(<<0x23, value::16, rest::binary>>, acc),
    do: properties(rest, %{acc | topic_alias: value})

  defp properties(<<0x0B, rest::binary>>, acc) do
    {:ok, value, rest} = variable_integer(rest, 0, 0)
    properties(rest, %{acc | subscription_identifiers: acc.subscription_identifiers ++ [value]})
  end

  defp properties(<<0x01, _, rest::binary>>, acc), do: properties(rest, acc)
  defp properties(<<0x02, _::32, rest::binary>>, acc), do: properties(rest, acc)

  defp properties(<<id, length::16, _::binary-size(length), rest::binary>>, acc)
       when id in [0x03, 0x08, 0x09],
       do: properties(rest, acc)

  defp properties(
         <<0x26, kl::16, _::binary-size(kl), vl::16, _::binary-size(vl), rest::binary>>,
         acc
       ),
       do: properties(rest, acc)
end
