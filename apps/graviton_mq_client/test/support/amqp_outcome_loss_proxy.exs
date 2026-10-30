defmodule GravitonMQ.Client.Test.AMQPOutcomeLossProxy do
  @moduledoc false
  use GenServer

  def start_link(options), do: GenServer.start_link(__MODULE__, options)
  def port(owner), do: GenServer.call(owner, :port)

  @impl true
  def init(options) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    {:ok, port} = :inet.port(listener)
    owner = self()

    acceptor =
      spawn_link(fn ->
        case :gen_tcp.accept(listener) do
          {:ok, socket} ->
            :ok = :gen_tcp.controlling_process(socket, owner)
            send(owner, {:accepted, socket})

          {:error, :closed} ->
            :ok
        end
      end)

    {:ok,
     %{
       listener: listener,
       port: port,
       acceptor: acceptor,
       upstream_port: Keyword.fetch!(options, :upstream_port),
       observer: Keyword.fetch!(options, :observer),
       client: nil,
       broker: nil,
       client_buffer: <<>>,
       broker_buffer: <<>>,
       client_header: true,
       broker_header: true,
       dropped: false
     }}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  @impl true
  def handle_info({:accepted, socket}, state) do
    {:ok, broker} =
      :gen_tcp.connect({127, 0, 0, 1}, state.upstream_port, [:binary, active: false], 1000)

    :ok = :inet.setopts(socket, active: :once)
    :ok = :inet.setopts(broker, active: :once)
    {:noreply, %{state | client: socket, broker: broker}}
  end

  def handle_info({:tcp, socket, bytes}, state)
      when socket == state.client or socket == state.broker do
    direction = if socket == state.client, do: :client, else: :broker
    key = if direction == :client, do: :client_buffer, else: :broker_buffer
    buffer = Map.fetch!(state, key) <> bytes
    if byte_size(buffer) > 1_048_576, do: raise("test proxy buffer limit")
    state = relay(Map.put(state, key, buffer), direction)
    :ok = :inet.setopts(socket, active: :once)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, state)
      when socket == state.client or socket == state.broker,
      do: {:noreply, close_pair(state)}

  def handle_info({:tcp_error, _socket, reason}, state),
    do: {:stop, {:proxy_error, reason}, state}

  def handle_info(_message, state), do: {:noreply, state}

  # Independent envelope/descriptor parsing: no Graviton codec is an oracle
  # for selecting the real Artemis acknowledgement to lose.
  defp relay(state, direction) do
    {key, header_key, target} =
      if direction == :client,
        do: {:client_buffer, :client_header, state.broker},
        else: {:broker_buffer, :broker_header, state.client}

    input = Map.fetch!(state, key)

    case envelope(input, Map.fetch!(state, header_key)) do
      :more ->
        state

      {:ok, wire, body, rest} ->
        state = state |> Map.put(key, rest) |> Map.put(header_key, false)

        if direction == :broker and disposition?(body) and not state.dropped do
          send(state.observer, {:amqp_outcome_dropped, self()})
          relay(%{state | dropped: true}, direction)
        else
          :ok = :gen_tcp.send(target, wire)
          relay(state, direction)
        end
    end
  end

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

  defp close_pair(state) do
    if state.client, do: :gen_tcp.close(state.client)
    if state.broker, do: :gen_tcp.close(state.broker)
    %{state | client: nil, broker: nil}
  end

  @impl true
  def terminate(_reason, state) do
    close_pair(state)
    :gen_tcp.close(state.listener)
    if Process.alive?(state.acceptor), do: Process.exit(state.acceptor, :shutdown)
    :ok
  end
end
