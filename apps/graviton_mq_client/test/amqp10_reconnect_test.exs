defmodule GravitonMQ.AMQP10Client.ReconnectTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.AMQP10.{Error, Value}
  alias GravitonMQ.Client.AMQP10.{Config, Reconnect}
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.AMQP10.Codec.{Frame, Performative}
  alias GravitonMQ.AMQP10.Performative.{Attach, Begin, Close, Detach, Open}
  alias GravitonMQ.AMQP10.Terminus.{Source, Target}

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
      {:ok, refused} = :gen_tcp.accept(listener, 1_000)
      :ok = :gen_tcp.close(refused)
      assert_receive {:graviton_mq, ^client, {:reconnecting, 2}}, 1_000

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

    test "a host close during an idle-timeout Close is still final" do
      {listener, client} =
        client!(
          idle_timeout_ms: 200,
          close_timeout_ms: 300,
          reconnect: [max_attempts: 1, delay: 20, max_delay: 20]
        )

      socket = handshake!(listener, client)
      assert %Close{} = recv_pdu!(socket, 2_000)
      assert {:ok, closing} = Client.close(client)
      refute_receive {:graviton_mq, ^client, {:operation, ^closing, _}}, 100
      assert_receive {:graviton_mq, ^client, {:disconnected, :close_timeout}}, 1_000
      assert_receive {:graviton_mq, ^client, {:operation, ^closing, {:ok, :closed}}}, 100
      refute_receive {:graviton_mq, ^client, {:reconnecting, _}}, 200
      assert %{owner: :stopped} = Client.status(client)
      assert {:error, :timeout} = :gen_tcp.accept(listener, 100)
    end
  end

  describe "restoration" do
    test "declared Sessions and Links are restored in order and reported; in-flight ones are not" do
      {listener, client} = client!(reconnect: [max_attempts: 2, delay: 20, max_delay: 20])
      socket = handshake!(listener, client)
      session!(client, socket, 2)
      session!(client, socket, 1)
      channel = 1
      kept = sender!(client, socket, channel, "kept", 9)
      refused = sender!(client, socket, channel, "refused-later", 10)

      assert {:ok, inflight} =
               Client.attach(client, 1, "in-flight", :sender, address: Value.string("q"))

      assert {^channel, %Attach{}} = recv_frame!(socket)
      assert %{intent: %{sessions: 2, links: 2}} = Client.status(client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:operation, ^inflight, {:unknown, :closed}}}, 1_000
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
      socket = handshake!(listener, client)

      # Ascending Session identity: Session 1 gets the first channel.
      assert {0, %Begin{}} = recv_frame!(socket)
      assert {1, %Begin{}} = recv_frame!(socket)
      assert %{owner: :restoring, restoring: 4} = Client.status(client)
      assert {:error, :restoring} = Client.send_message(client, 1, kept, <<0, 0x53, 0x77, 0x40>>)

      assert {:error, :restoring} =
               Client.attach(client, 1, "new", :sender, address: Value.string("q"))

      :ok = :gen_tcp.send(socket, begin_frame(0) <> begin_frame(1))
      assert_receive {:graviton_mq, ^client, {:restored, {:session, 1}, {:ok, 1}}}, 1_000
      assert_receive {:graviton_mq, ^client, {:restored, {:session, 2}, {:ok, 2}}}, 1_000
      assert {0, %Attach{name: %Value{value: "kept"}} = first} = recv_frame!(socket)
      assert {0, %Attach{name: %Value{value: "refused-later"}} = second} = recv_frame!(socket)
      :ok = :gen_tcp.send(socket, attach_reply(first, 0, 9) <> refusal(second, 0, 10))
      assert_receive {:graviton_mq, ^client, {:restored, {:link, 1, ^kept}, {:ok, ^kept}}}, 1_000

      assert_receive {:graviton_mq, ^client, {:restored, {:link, 1, ^refused}, {:error, _}}},
                     1_000

      assert_receive {:graviton_mq, ^client, {:restore_complete, %{restored: 3, failed: 1}}},
                     1_000

      assert %{owner: :active, restoring: 0, intent: %{sessions: 2, links: 1}} =
               Client.status(client)

      refute Enum.any?(
               drain_frames(socket),
               &match?({_, %Attach{name: %Value{value: "in-flight"}}}, &1)
             )
    end

    test "host detach and End remove declarations at admission" do
      {listener, client} = client!(reconnect: [max_attempts: 1, delay: 20, max_delay: 20])
      socket = handshake!(listener, client)
      session!(client, socket, 1)
      session!(client, socket, 2)
      link = sender!(client, socket, 0, "detached", 9)
      assert {:ok, _} = Client.detach(client, 1, link)
      assert {:ok, _} = Client.end_session(client, 2)
      assert %{intent: %{sessions: 1, links: 0}} = Client.status(client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
      socket = handshake!(listener, client)
      assert {0, %Begin{}} = recv_frame!(socket)
      :ok = :gen_tcp.send(socket, begin_frame(0))

      assert_receive {:graviton_mq, ^client, {:restore_complete, %{restored: 1, failed: 0}}},
                     1_000

      assert drain_frames(socket) == []
    end

    test "an empty intent still reports completion after every reconnect, never on first connect" do
      {listener, client} = client!(reconnect: [max_attempts: 1, delay: 20, max_delay: 20])
      socket = handshake!(listener, client)
      refute_received {:graviton_mq, ^client, {:restore_complete, _}}
      :ok = :gen_tcp.close(socket)
      handshake!(listener, client)

      assert_receive {:graviton_mq, ^client, {:restore_complete, %{restored: 0, failed: 0}}},
                     1_000
    end
  end

  defp session!(client, socket, identity) do
    assert {:ok, operation} = Client.open_session(client, identity)
    assert {channel, %Begin{}} = recv_frame!(socket)
    :ok = :gen_tcp.send(socket, begin_frame(channel))
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^identity}}}, 1_000
  end

  defp sender!(client, socket, channel, name, handle) do
    session = channel_session(client, channel)

    assert {:ok, operation} =
             Client.attach(client, session, name, :sender, address: Value.string("q"))

    assert {^channel, %Attach{} = local} = recv_frame!(socket)
    :ok = :gen_tcp.send(socket, attach_reply(local, channel, handle))
    identity = {Value.string(name), :sender}
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^identity}}}, 1_000
    identity
  end

  # Sessions opened as 2 then 1 hold channels 0 and 1; tests pass the channel.
  defp channel_session(_client, 0), do: 1
  defp channel_session(_client, 1), do: 1

  defp begin_frame(channel),
    do:
      frame(
        %Begin{
          remote_channel: Value.ushort(channel),
          next_outgoing_id: Value.uint(0),
          incoming_window: Value.uint(16),
          outgoing_window: Value.uint(16)
        },
        channel
      )

  defp attach_reply(local, channel, handle),
    do:
      frame(
        %{
          local
          | handle: Value.uint(handle),
            role: Value.boolean(true),
            source: %Source{address: Value.string("q")},
            target: %Target{address: Value.string("q")},
            initial_delivery_count: nil
        },
        channel
      )

  defp refusal(local, channel, handle),
    do:
      frame(
        %{
          local
          | handle: Value.uint(handle),
            role: Value.boolean(true),
            source: %Source{address: Value.string("q")},
            target: nil,
            initial_delivery_count: nil
        },
        channel
      ) <>
        frame(
          %Detach{
            handle: Value.uint(handle),
            closed: Value.boolean(true),
            error: %Error{condition: Value.symbol("amqp:not-found")}
          },
          channel
        )

  defp drain_frames(socket, frames \\ []) do
    case :gen_tcp.recv(socket, 4, 100) do
      {:ok, <<size::32>>} ->
        {:ok, rest} = :gen_tcp.recv(socket, size - 4, 1_000)
        {:ok, frame, <<>>} = Frame.decode(<<size::32, rest::binary>>)

        case Performative.decode(frame.body) do
          {:ok, pdu, _} -> drain_frames(socket, [{frame.channel, pdu} | frames])
          _heartbeat -> drain_frames(socket, frames)
        end

      {:error, :timeout} ->
        Enum.reverse(frames)
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

  defp recv_frame!(socket, timeout \\ 1_000) do
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
