defmodule GravitonMQ.AMQP10Client.ReconnectTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.AMQP10.{Error, Value}
  alias GravitonMQ.Client.AMQP10.{Config, Reconnect}
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.AMQP10.Codec.{Frame, Performative}
  alias GravitonMQ.AMQP10.Performative.{Close, Open}

  @header <<"AMQP", 0, 1, 0, 0>>

  describe "configuration" do
    test "reconnect mirrors the MQTT policy bounds and defaults to one connection" do
      assert {:ok, %{reconnect: %{max_attempts: 0, delay: 250, max_delay: 5_000}}} =
               Config.build(container_id: "c", observer: self())

      assert {:ok, %{reconnect: %{max_attempts: 100, delay: 1, max_delay: 60_000}}} =
               Config.build(
                 container_id: "c",
                 observer: self(),
                 reconnect: [max_attempts: 100, delay: 1, max_delay: 60_000]
               )

      for reconnect <- [
            [max_attempts: -1],
            [max_attempts: 101],
            [max_attempts: :infinity],
            [delay: 0],
            [delay: 60_001],
            [delay: 500, max_delay: 499],
            [max_delay: 60_001],
            [jitter: true],
            :yes,
            [{"delay", 1}]
          ] do
        assert {:error, :invalid_options} =
                 Config.build(container_id: "c", observer: self(), reconnect: reconnect)
      end
    end
  end

  describe "policy" do
    test "the delay before attempt n is min(max_delay, delay * 2^(n-1)) without jitter" do
      policy = %{max_attempts: 10, delay: 250, max_delay: 5_000}

      assert Enum.map(1..7, &Reconnect.delay(policy, &1)) ==
               [250, 500, 1_000, 2_000, 4_000, 5_000, 5_000]

      assert Reconnect.delay(%{policy | delay: 60_000, max_delay: 60_000}, 100) == 60_000
      # The exponent cap matters: an uncapped 2^99 would be computed first.
      assert Reconnect.delay(%{delay: 1, max_delay: 60_000}, 100) == 60_000
    end

    test "transient losses retry and terminal failures stop" do
      forced = %Error{condition: Value.symbol("amqp:connection:forced")}
      denied = %Error{condition: Value.symbol("amqp:unauthorized-access")}

      for reason <- [
            :closed,
            {:transport, :econnreset},
            {:write_failed, {:error, :timeout}},
            {:connect, :econnrefused},
            {:connect, :timeout},
            {:connect, :nxdomain},
            {:connect, :closed},
            :sasl_timeout,
            {:sasl, :sys_temp},
            :normal,
            {:peer_close, forced}
          ] do
        assert Reconnect.classify(reason, nil) == :retry, inspect(reason)
      end

      for reason <- [
            {:sasl, :auth},
            {:sasl, :sys},
            {:sasl, :sys_perm},
            {:sasl, {:unknown, 9}},
            :sasl_not_offered,
            :sasl_mechanism_not_offered,
            :sasl_protocol_error,
            :invalid_sasl_credentials,
            {:connect, {:tls_alert, :unknown_ca}},
            {:connect, :transport_error},
            {:write_failed, {:error, {:encode, :limit_exceeded, :frame_size_limit}}},
            {:decode_error, :input_buffer_limit},
            {:peer_close, denied},
            :owner_stopped,
            :close_timeout
          ] do
        assert Reconnect.classify(reason, nil) == :stop, inspect(reason)
      end

      # D1: a locally initiated Close is classified by its recorded cause.
      for reason <- [:normal, :close_timeout, :closed] do
        assert Reconnect.classify(reason, :idle_timeout) == :retry
        assert Reconnect.classify(reason, :open_timeout) == :retry
        assert Reconnect.classify(reason, :host_close) == :stop
        assert Reconnect.classify(reason, :protocol) == :stop
      end
    end
  end

  describe "shell" do
    test "the default policy keeps one connection and a stopped, inspectable owner" do
      {listener, client} = client!([])
      socket = handshake!(listener, client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:disconnected, :closed}}, 1_000
      refute_receive {:graviton_mq, ^client, {:reconnecting, _}}, 100
      refute_received {:graviton_mq, ^client, {:reconnect_exhausted, _}}

      assert %{owner: :stopped, phase: :closed, attempt: 0, next_retry_ms: nil} =
               Client.status(client)

      assert {:error, :closed} = Client.open_session(client, 1)
      assert {:error, :timeout} = :gen_tcp.accept(listener, 200)
    end

    test "a transport loss reconnects on a fresh generation and fences old deliveries" do
      {listener, client} = client!(reconnect: [max_attempts: 3, delay: 300, max_delay: 300])
      socket = handshake!(listener, client)
      assert %{owner: :active, generation: 0} = Client.status(client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:disconnected, :closed}}, 1_000
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000

      assert %{owner: :offline, attempt: 1, next_retry_ms: remaining} = Client.status(client)
      assert remaining in 0..300
      # Nothing is queued and nothing claims success while offline.
      assert {:error, :disconnected} = Client.open_session(client, 1)

      assert {:error, :disconnected} =
               Client.request(client, 1, {Value.string("l"), :receiver}, 1, 1)

      socket = handshake!(listener, client)

      assert %{owner: :active, generation: 1, attempt: 0, next_retry_ms: nil} =
               Client.status(client)

      # A generation-0 delivery reference never reaches the new connection.
      assert {:error, :stale_delivery} =
               Client.settle(
                 client,
                 {client, 1, {0, 1, 0, 0, 0}},
                 %GravitonMQ.AMQP10.Outcome.Accepted{}
               )

      assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 50)
    end

    test "peer Close is retried only for connection:forced or an unrequested error-less Close" do
      for {error, action} <- [
            {%Error{condition: Value.symbol("amqp:connection:forced")}, :retry},
            {nil, :retry},
            {%Error{condition: Value.symbol("amqp:unauthorized-access")}, :stop}
          ] do
        {listener, client} = client!(reconnect: [max_attempts: 1, delay: 20, max_delay: 20])
        socket = handshake!(listener, client)
        :ok = :gen_tcp.send(socket, frame(%Close{error: error}))
        assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 1_000

        case action do
          :retry ->
            assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
            handshake!(listener, client)

          :stop ->
            refute_receive {:graviton_mq, ^client, {:reconnecting, _}}, 100
            assert %{owner: :stopped} = Client.status(client)
        end
      end
    end

    test "a host close is final" do
      {listener, client} = client!(reconnect: [max_attempts: 3, delay: 20, max_delay: 20])
      socket = handshake!(listener, client)
      assert {:ok, closing} = Client.close(client)
      assert %Close{error: nil} = recv_pdu!(socket)
      :ok = :gen_tcp.send(socket, frame(%Close{}))
      assert_receive {:graviton_mq, ^client, {:operation, ^closing, {:ok, :closed}}}, 1_000
      assert_receive {:graviton_mq, ^client, {:disconnected, :normal}}, 1_000
      refute_receive {:graviton_mq, ^client, {:reconnecting, _}}, 100
      assert %{owner: :stopped} = Client.status(client)
    end

    test "the budget resets only when the peer's Open arrives and exhaustion is reported" do
      {listener, client} = client!(reconnect: [max_attempts: 2, delay: 20, max_delay: 20])
      socket = handshake!(listener, client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000

      # TCP is accepted but the peer never sends Open: no reset.
      for attempt <- [2] do
        {:ok, refused} = :gen_tcp.accept(listener, 1_000)
        :ok = :gen_tcp.close(refused)
        assert_receive {:graviton_mq, ^client, {:reconnecting, ^attempt}}, 1_000
      end

      {:ok, refused} = :gen_tcp.accept(listener, 1_000)
      :ok = :gen_tcp.close(refused)
      assert_receive {:graviton_mq, ^client, {:reconnect_exhausted, 2}}, 1_000
      assert %{owner: :stopped, next_retry_ms: nil} = Client.status(client)
      assert %{socket: nil, retry: nil, timers: timers} = :sys.get_state(client)
      assert timers == %{}
      assert {:error, :timeout} = :gen_tcp.accept(listener, 200)
    end

    test "backoff gaps follow the formula" do
      {listener, client} = client!(reconnect: [max_attempts: 4, delay: 40, max_delay: 100])
      socket = handshake!(listener, client)
      :ok = :gen_tcp.close(socket)

      accepted =
        for _ <- 1..4 do
          {:ok, refused} = :gen_tcp.accept(listener, 2_000)
          at = System.monotonic_time(:millisecond)
          :ok = :gen_tcp.close(refused)
          at
        end

      gaps = accepted |> Enum.chunk_every(2, 1, :discard) |> Enum.map(fn [a, b] -> b - a end)

      # Gap k separates attempts k+1 and k+2: delay(k+1) = 80, 100, 100.
      for {gap, minimum} <- Enum.zip(gaps, [80, 100, 100]) do
        assert gap >= minimum, "gap #{gap} ms < #{minimum} ms"
        assert gap <= minimum + 500, "gap #{gap} ms is far above #{minimum} ms"
      end

      assert_receive {:graviton_mq, ^client, {:reconnect_exhausted, 4}}, 1_000
    end

    test "close/1 while offline stops at once; observer death while offline stops the owner" do
      {listener, client} = client!(reconnect: [max_attempts: 3, delay: 5_000, max_delay: 5_000])
      socket = handshake!(listener, client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
      assert {:ok, closing} = Client.close(client)
      assert_receive {:graviton_mq, ^client, {:operation, ^closing, {:ok, :closed}}}, 100
      assert %{owner: :stopped, next_retry_ms: nil} = Client.status(client)
      assert {:error, :timeout} = :gen_tcp.accept(listener, 200)

      observer = spawn(fn -> receive(do: (:stop -> :ok)) end)

      {listener, client} =
        client!(observer: observer, reconnect: [max_attempts: 3, delay: 5_000, max_delay: 5_000])

      {:ok, socket} = :gen_tcp.accept(listener, 1_000)
      assert {:ok, @header} = :gen_tcp.recv(socket, 8, 1_000)
      assert %Open{} = recv_pdu!(socket)
      :ok = :gen_tcp.send(socket, @header <> frame(open()))
      eventually!(fn -> Client.status(client).owner == :active end)
      monitor = Process.monitor(client)
      assert %{owner: :active} = Client.status(client)
      :ok = :gen_tcp.close(socket)
      eventually!(fn -> Client.status(client).owner == :offline end)
      send(observer, :stop)
      assert_receive {:DOWN, ^monitor, :process, ^client, :normal}, 1_000
    end

    test "a receive idle timeout against a silent peer reconnects" do
      {listener, client} =
        client!(
          idle_timeout_ms: 200,
          close_timeout_ms: 50,
          reconnect: [max_attempts: 1, delay: 20, max_delay: 20]
        )

      socket = handshake!(listener, client)
      assert %Close{} = recv_pdu!(socket, 2_000)
      assert_receive {:graviton_mq, ^client, {:disconnected, :close_timeout}}, 1_000
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
      handshake!(listener, client)
    end
  end

  defp client!(options) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    options =
      Keyword.merge([container_id: "reconnect-client", host: "127.0.0.1", port: port], options)

    {listener, start_supervised!(Supervisor.child_spec({Client, options}, id: make_ref()))}
  end

  # Accepts one connection, completes header + Open and waits for :connected.
  defp handshake!(listener, client) do
    {:ok, socket} = :gen_tcp.accept(listener, 2_000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    assert {:ok, @header} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = recv_pdu!(socket)
    :ok = :gen_tcp.send(socket, @header <> frame(open()))
    assert_receive {:graviton_mq, ^client, {:connected, "peer"}}, 1_000
    socket
  end

  defp open, do: %Open{container_id: Value.string("peer"), max_frame_size: Value.uint(512)}

  defp frame(pdu, channel \\ 0) do
    {:ok, bytes} = Performative.encode(pdu)
    <<8 + byte_size(bytes)::32, 2, 0, channel::16, bytes::binary>>
  end

  defp recv_pdu!(socket, timeout \\ 1_000) do
    {_channel, pdu} = recv_frame!(socket, timeout)
    pdu
  end

  defp recv_frame!(socket, timeout) do
    assert {:ok, <<size::32>>} = :gen_tcp.recv(socket, 4, timeout)
    assert {:ok, rest} = :gen_tcp.recv(socket, size - 4, timeout)
    assert {:ok, frame, <<>>} = Frame.decode(<<size::32, rest::binary>>)
    assert {:ok, pdu, _payload} = Performative.decode(frame.body)
    {frame.channel, pdu}
  end

  defp eventually!(predicate, timeout \\ 2_000),
    do: until!(predicate, System.monotonic_time(:millisecond) + timeout)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline
      Process.sleep(5)
      until!(predicate, deadline)
    end
  end
end
