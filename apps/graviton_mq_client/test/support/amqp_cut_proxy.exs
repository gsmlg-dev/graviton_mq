defmodule GravitonMQ.Client.Test.AMQPCutProxy do
  @moduledoc false
  # Multi-accept TCP relay for AMQP reconnect evidence (S3 R1–R6, F1–F3).
  # Every accepted client socket gets its own upstream socket. `cut/1`,
  # `blackhole/1` and `cut_on_disposition/1` act on the current pairs;
  # `refuse/2` acts on later accepts (accept, then close at once).
  # With `parse: true` (no-SASL plain TCP only) frame envelopes and
  # descriptors are parsed independently of the Graviton codec; TLS is relayed
  # opaquely. The observer receives `{:proxy_connection, n, monotonic_ms}` for
  # every accept, `{:proxy_cut, n, :disposition}`, `{:proxy_transfer, n}` for
  # each client Transfer and `{:proxy_closed, n}`.
  use GenServer

  def start_link(options), do: GenServer.start_link(__MODULE__, options)
  def port(proxy), do: GenServer.call(proxy, :port)
  def cut(proxy), do: GenServer.call(proxy, :cut)
  def blackhole(proxy), do: GenServer.call(proxy, {:mode, :blackhole})
  def cut_on_disposition(proxy), do: GenServer.call(proxy, {:mode, :cut_on_disposition})

  def refuse(proxy, refuse?) when is_boolean(refuse?),
    do: GenServer.call(proxy, {:refuse, refuse?})

  @impl true
  def init(options) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])

    {:ok, port} = :inet.port(listener)
    owner = self()
    acceptor = spawn_link(fn -> accept_loop(listener, owner) end)
    {host, upstream_port} = Keyword.fetch!(options, :upstream)

    {:ok,
     %{
       listener: listener,
       port: port,
       acceptor: acceptor,
       upstream: {String.to_charlist(host), upstream_port},
       observer: Keyword.fetch!(options, :observer),
       parse: Keyword.get(options, :parse, true),
       refuse: false,
       count: 0,
       pairs: %{},
       sockets: %{}
     }}
  end

  defp accept_loop(listener, owner) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        :ok = :gen_tcp.controlling_process(socket, owner)
        send(owner, {:accepted, socket})
        accept_loop(listener, owner)

      {:error, _closed} ->
        :ok
    end
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}
  def handle_call({:refuse, refuse?}, _from, state), do: {:reply, :ok, %{state | refuse: refuse?}}

  def handle_call(:cut, _from, state),
    do: {:reply, :ok, Enum.reduce(Map.keys(state.pairs), state, &close_pair(&2, &1))}

  def handle_call({:mode, mode}, _from, state),
    do:
      {:reply, :ok,
       %{state | pairs: Map.new(state.pairs, fn {n, pair} -> {n, %{pair | mode: mode}} end)}}

  @impl true
  def handle_info({:accepted, socket}, state) do
    n = state.count + 1
    send(state.observer, {:proxy_connection, n, System.monotonic_time(:millisecond)})
    state = %{state | count: n}

    if state.refuse do
      :gen_tcp.close(socket)
      {:noreply, state}
    else
      {host, port} = state.upstream
      {:ok, broker} = :gen_tcp.connect(host, port, [:binary, active: false], 2_000)
      :ok = :inet.setopts(socket, active: :once)
      :ok = :inet.setopts(broker, active: :once)

      pair = %{
        client: socket,
        broker: broker,
        mode: :relay,
        client_buffer: <<>>,
        broker_buffer: <<>>,
        client_header: true,
        broker_header: true
      }

      sockets = state.sockets |> Map.put(socket, {n, :client}) |> Map.put(broker, {n, :broker})
      {:noreply, %{state | pairs: Map.put(state.pairs, n, pair), sockets: sockets}}
    end
  end

  def handle_info({:tcp, socket, bytes}, state) do
    case Map.fetch(state.sockets, socket) do
      {:ok, {n, direction}} ->
        state = relay(state, n, direction, bytes)
        if Map.has_key?(state.pairs, n), do: :inet.setopts(socket, active: :once)
        {:noreply, state}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({kind, socket}, state) when kind in [:tcp_closed],
    do: {:noreply, close_socket(state, socket)}

  def handle_info({:tcp_error, socket, _reason}, state),
    do: {:noreply, close_socket(state, socket)}

  def handle_info(_message, state), do: {:noreply, state}

  defp close_socket(state, socket) do
    case Map.fetch(state.sockets, socket) do
      {:ok, {n, _}} -> close_pair(state, n)
      :error -> state
    end
  end

  defp relay(state, n, direction, bytes) do
    pair = Map.fetch!(state.pairs, n)

    cond do
      pair.mode == :blackhole ->
        state

      not state.parse ->
        _ = :gen_tcp.send(target(pair, direction), bytes)
        state

      true ->
        {buffer, _header} = keys(direction)
        pair = Map.update!(pair, buffer, &(&1 <> bytes))
        if byte_size(Map.fetch!(pair, buffer)) > 1_048_576, do: raise("test proxy buffer limit")
        frames(%{state | pairs: Map.put(state.pairs, n, pair)}, n, direction)
    end
  end

  defp frames(state, n, direction) do
    pair = Map.fetch!(state.pairs, n)
    {buffer, header} = keys(direction)

    case envelope(Map.fetch!(pair, buffer), Map.fetch!(pair, header)) do
      :more ->
        state

      {:ok, wire, body, rest} ->
        pair = pair |> Map.put(buffer, rest) |> Map.put(header, false)
        state = %{state | pairs: Map.put(state.pairs, n, pair)}

        if direction == :broker and pair.mode == :cut_on_disposition and disposition?(body) do
          send(state.observer, {:proxy_cut, n, :disposition})
          close_pair(state, n)
        else
          if direction == :client and transfer?(body),
            do: send(state.observer, {:proxy_transfer, n})

          _ = :gen_tcp.send(target(pair, direction), wire)
          frames(state, n, direction)
        end
    end
  end

  defp keys(:client), do: {:client_buffer, :client_header}
  defp keys(:broker), do: {:broker_buffer, :broker_header}
  defp target(pair, :client), do: pair.broker
  defp target(pair, :broker), do: pair.client

  defp envelope(input, true) when byte_size(input) < 8, do: :more
  defp envelope(<<wire::binary-size(8), rest::binary>>, true), do: {:ok, wire, <<>>, rest}
  defp envelope(input, false) when byte_size(input) < 8, do: :more

  defp envelope(<<size::32, offset, _::binary>> = input, false) do
    if size < 8 or offset < 2 or offset * 4 > size or size > 1_048_576,
      do: raise("invalid test peer envelope")

    if byte_size(input) < size do
      :more
    else
      <<wire::binary-size(size), rest::binary>> = input
      {:ok, wire, binary_part(wire, offset * 4, size - offset * 4), rest}
    end
  end

  defp disposition?(<<0, 0x53, 0x15, _::binary>>), do: true
  defp disposition?(<<0, 0x80, 0x15::64, _::binary>>), do: true
  defp disposition?(_body), do: false
  defp transfer?(<<0, 0x53, 0x14, _::binary>>), do: true
  defp transfer?(<<0, 0x80, 0x14::64, _::binary>>), do: true
  defp transfer?(_body), do: false

  defp close_pair(state, n) do
    case Map.pop(state.pairs, n) do
      {nil, _pairs} ->
        state

      {pair, pairs} ->
        :gen_tcp.close(pair.client)
        :gen_tcp.close(pair.broker)
        send(state.observer, {:proxy_closed, n})
        %{state | pairs: pairs, sockets: Map.drop(state.sockets, [pair.client, pair.broker])}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(Map.keys(state.pairs), &close_pair(state, &1))
    :gen_tcp.close(state.listener)
    if Process.alive?(state.acceptor), do: Process.exit(state.acceptor, :shutdown)
    :ok
  end
end
