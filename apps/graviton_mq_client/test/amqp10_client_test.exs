defmodule GravitonMQ.AMQP10ClientTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Codec.{Frame, Performative}
  alias GravitonMQ.AMQP10.Performative.{Attach, Begin, Close, Detach, End, Flow, Open}
  alias GravitonMQ.AMQP10.Terminus.{Source, Target}

  @header <<"AMQP", 0, 1, 0, 0>>

  test "TLS configuration cannot replace the owner's passive binary framing or send limits" do
    for option <- [
          [active: true],
          [mode: :list],
          [packet: 4],
          [header: 2],
          [packet_size: 0],
          [deliver: :port],
          [send_timeout: :infinity],
          [send_timeout_close: false]
        ] do
      assert {:error, :invalid_options} =
               GravitonMQ.Client.AMQP10.Config.build(
                 container_id: "tls-owner",
                 observer: self(),
                 transport: :tls,
                 tls_options: option
               )

      assert {:error, :invalid_options} =
               GravitonMQ.Client.MQTT.Config.build(
                 client_id: "tls-owner",
                 observer: self(),
                 transport: :tls,
                 tls_options: option
               )
    end
  end

  test "configuration rejects insecure TLS, SASL and invalid bounded options" do
    for options <- [
          [container_id: ""],
          [container_id: <<255>>],
          [username: "not-sasl"],
          [max_operations: 0],
          [max_input_frames_per_turn: 0],
          [max_input_bytes: 0],
          [max_frame_size: 511],
          [max_frame_size: 131_072, max_input_bytes: 65_536],
          [session_options: [remote_channel: 1]],
          [transport: :websocket],
          [transport: :tls, tls_options: [verify: :verify_none]],
          [transport: :tls, tls_options: [server_name_indication: :disable]],
          [host: {256, 0, 0, 1}],
          [amqp_typo: true]
        ] do
      assert {:error, _} = Client.start_link(Keyword.merge([container_id: "invalid"], options))
    end

    specification =
      Client.child_spec(container_id: "private-container", tls_options: [password: "private-key"])

    refute inspect(specification) =~ "private-key"
  end

  test "refused connection is observable and commands never claim success" do
    {listener, port} = listener()
    :ok = :gen_tcp.close(listener)
    client = start_supervised!({Client, container_id: "refused", host: "127.0.0.1", port: port})
    assert_receive {:graviton_mq, ^client, {:disconnected, {:connect, _reason}}}, 1_000
    assert Client.status(client).phase == :closed
    assert {:error, :closed} = Client.open_session(client, 1)
  end

  test "split header and coalesced envelopes drain over bounded mailbox turns" do
    {client, socket} = peer(max_input_frames_per_turn: 1)
    assert {:ok, @header} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{max_frame_size: %Value{value: 65_536}} = recv_pdu(socket)
    :ok = :gen_tcp.send(socket, <<"AMQ">>)
    refute_receive {:graviton_mq, ^client, {:connected, _}}, 30
    heartbeats = :binary.copy(<<8::32, 2, 0, 0::16>>, 40)
    :ok = :gen_tcp.send(socket, <<"P", 0, 1, 0, 0>> <> frame(open()) <> heartbeats)
    assert_receive {:graviton_mq, ^client, {:connected, "peer"}}, 1_000
    assert Client.status(client).phase == :active
    assert {:ok, reference} = Client.open_session(client, 77)
    assert %Begin{} = recv_pdu(socket)
    # No peer Begin has arrived: the successful TCP send cannot complete it.
    refute_receive {:graviton_mq, ^client, {:operation, ^reference, {:ok, _}}}, 30
    :ok = :gen_tcp.close(socket)
    assert_receive {:graviton_mq, ^client, {:operation, ^reference, {:unknown, :closed}}}, 1_000
    assert_receive {:graviton_mq, ^client, {:disconnected, :closed}}, 1_000
  end

  test "local operation admission stays bounded while a peer withholds Begin" do
    {client, socket} = connected(max_operations: 1)
    assert {:ok, reference} = Client.open_session(client, 7)
    assert %Begin{} = recv_pdu(socket)
    assert {:error, :operation_limit} = Client.open_session(client, 8)
    assert Client.status(client).pending == 1
    :ok = :gen_tcp.close(socket)
    assert_receive {:graviton_mq, ^client, {:operation, ^reference, {:unknown, :closed}}}, 1_000
    assert Client.status(client).pending == 0
  end

  test "AMQP without SASL rejects a peer SASL header explicitly" do
    {client, socket} = peer(close_timeout_ms: 30)
    assert {:ok, @header} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = recv_pdu(socket)
    :ok = :gen_tcp.send(socket, <<"AMQP", 3, 1, 0, 0>>)
    assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 1_000
    assert Client.status(client).phase == :closed
  end

  test "malformed frame after Open sends Close and reaches a finite close timeout" do
    {client, socket} = connected(close_timeout_ms: 30)
    :ok = :gen_tcp.send(socket, <<7::32, 2, 0, 0::16>>)
    assert %Close{error: error} = recv_pdu(socket)
    assert error.condition.type == :symbol
    assert_receive {:graviton_mq, ^client, {:disconnected, :close_timeout}}, 1_000
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "peer idle heartbeat uses the shell timer and never completes an operation" do
    {client, socket} = connected(peer_open: %{open() | idle_time_out: Value.uint(100)})
    assert {:ok, <<8::32, 2, 0, 0::16>>} = :gen_tcp.recv(socket, 8, 1_000)
    assert Client.status(client).phase == :active
    assert Client.status(client).pending == 0
  end

  test "an observer dying closes its independently owned connection" do
    observer =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    {client, socket} = peer(observer: observer)
    monitor = Process.monitor(client)
    assert {:ok, @header} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = recv_pdu(socket)
    send(observer, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^client, :normal}, 1_000
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "host shutdown sends Close and removes the actual socket" do
    {client, socket} = connected()
    :ok = GenServer.stop(client)
    assert %Close{error: nil} = recv_pdu(socket)
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "a non-reading peer bounds blocked writes, status and shutdown without reporting success" do
    maximum = 10_485_760

    {client, socket} =
      connected(
        send_timeout: 50,
        operation_timeout_ms: 2_000,
        peer_open: %{open() | max_frame_size: Value.uint(65_536)},
        session_options: [
          max_message_bytes: maximum,
          max_outgoing_bytes: maximum,
          max_incoming_bytes: maximum,
          outgoing_window: 4096
        ]
      )

    assert {:ok, opening} = Client.open_session(client, 1)
    assert %Begin{} = recv_pdu(socket)

    :ok =
      :gen_tcp.send(
        socket,
        frame(%Begin{
          remote_channel: Value.ushort(0),
          next_outgoing_id: Value.uint(0),
          incoming_window: Value.uint(4096),
          outgoing_window: Value.uint(4096)
        })
      )

    assert_receive {:graviton_mq, ^client, {:operation, ^opening, {:ok, 1}}}, 1_000

    assert {:ok, attaching} =
             Client.attach(client, 1, "pressure", :sender,
               address: Value.string("q"),
               max_message_bytes: maximum
             )

    assert %Attach{} = local = recv_pdu(socket)

    remote = %{
      local
      | handle: Value.uint(9),
        role: Value.boolean(true),
        source: %Source{address: Value.string("q")},
        target: %Target{address: Value.string("q")},
        initial_delivery_count: nil
    }

    :ok =
      :gen_tcp.send(
        socket,
        frame(remote) <>
          frame(%Flow{
            next_incoming_id: Value.uint(0),
            incoming_window: Value.uint(4096),
            next_outgoing_id: Value.uint(0),
            outgoing_window: Value.uint(4096),
            handle: Value.uint(9),
            delivery_count: Value.uint(0),
            link_credit: Value.uint(1)
          })
      )

    identity = {Value.string("pressure"), :sender}
    assert_receive {:graviton_mq, ^client, {:operation, ^attaching, {:ok, ^identity}}}, 1_000
    # Stop reading after negotiation and shrink the advertised TCP receive window.
    :ok = :inet.setopts(socket, recbuf: 1024, buffer: 1024)
    body = :binary.copy(<<1>>, 8_388_608)
    raw = <<0, 0x53, 0x75, 0xB0, byte_size(body)::32, body::binary>>
    started = System.monotonic_time(:millisecond)
    assert {:ok, operation} = Client.send_message(client, 1, identity, raw)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:unknown, _reason}}}, 2_000
    assert_receive {:graviton_mq, ^client, {:disconnected, _reason}}, 1_000
    assert Client.status(client).phase == :closed
    assert System.monotonic_time(:millisecond) - started < 2_500
    refute_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, _}}}, 30
    assert :ok = GenServer.stop(client, :normal, 1_000)
  end

  test "Link-name reservations survive pending End and release for a reused Session incarnation" do
    {first, socket} = connected(container_id: "end-name-owners")
    {second, second_socket} = connected(container_id: "end-name-owners")
    session!(first, socket)
    session!(second, second_socket)
    identity = sender!(first, socket, "end-owned")
    assert {:ok, ending} = Client.end_session(first, 1)
    assert %End{} = recv_pdu(socket)

    assert {:error, :attach_not_admitted} =
             Client.attach(second, 1, "end-owned", :sender, address: Value.string("q"))

    assert {:error, :timeout} = :gen_tcp.recv(second_socket, 4, 30)
    :ok = :gen_tcp.send(socket, frame(%End{}))
    assert_receive {:graviton_mq, ^first, {:operation, ^ending, {:ok, 1}}}, 1_000
    assert ^identity = sender!(second, second_socket, "end-owned")
    session!(first, socket)

    assert {:error, :attach_not_admitted} =
             Client.attach(first, 1, "end-owned", :sender, address: Value.string("q"))

    :ok = GenServer.stop(first)
    assert {:ok, operation} = Client.detach(second, 1, identity)
    assert %Detach{} = recv_pdu(second_socket)

    :ok =
      :gen_tcp.send(
        second_socket,
        frame(%Detach{handle: Value.uint(9), closed: Value.boolean(true)})
      )

    assert_receive {:graviton_mq, ^second, {:operation, ^operation, {:ok, ^identity}}}, 1_000
    assert ^identity = sender!(second, second_socket, "end-owned")
  end

  test "incoming attachments across client owners refuse the duplicate before granting termini" do
    {first, socket} = connected(container_id: "incoming-name-owners")
    {second, second_socket} = connected(container_id: "incoming-name-owners")
    session!(first, socket)
    session!(second, second_socket)

    incoming = %Attach{
      name: Value.string("peer-owned"),
      handle: Value.uint(9),
      role: Value.boolean(false),
      snd_settle_mode: Value.ubyte(0),
      rcv_settle_mode: Value.ubyte(0),
      source: %Source{address: Value.string("q")},
      target: %Target{address: Value.string("q")},
      initial_delivery_count: Value.uint(0)
    }

    :ok = :gen_tcp.send(socket, frame(incoming))
    assert %Attach{source: %Source{}, target: %Target{}} = recv_pdu(socket)
    :ok = :gen_tcp.send(second_socket, frame(incoming))
    assert %Attach{source: nil, target: nil} = recv_pdu(second_socket)

    assert %Detach{error: %{condition: %Value{value: "amqp:resource-locked"}}} =
             recv_pdu(second_socket)

    assert Client.status(first).phase == :active
    assert Client.status(second).phase == :active
    assert {:ok, operation} = Client.open_session(first, 2)
    assert %Begin{} = recv_pdu(socket)
    refute_receive {:graviton_mq, ^first, {:operation, ^operation, {:error, _}}}, 30
  end

  defp session!(client, socket) do
    assert {:ok, operation} = Client.open_session(client, 1)
    assert %Begin{next_outgoing_id: next_id} = recv_pdu(socket)

    :ok =
      :gen_tcp.send(
        socket,
        frame(%Begin{
          remote_channel: Value.ushort(0),
          next_outgoing_id: next_id,
          incoming_window: Value.uint(16),
          outgoing_window: Value.uint(16)
        })
      )

    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, 1}}}, 1_000
  end

  defp sender!(client, socket, name) do
    assert {:ok, operation} = Client.attach(client, 1, name, :sender, address: Value.string("q"))
    assert %Attach{} = local = recv_pdu(socket)

    :ok =
      :gen_tcp.send(
        socket,
        frame(%{
          local
          | handle: Value.uint(9),
            role: Value.boolean(true),
            source: %Source{address: Value.string("q")},
            target: %Target{address: Value.string("q")},
            initial_delivery_count: nil
        })
      )

    identity = {Value.string(name), :sender}
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^identity}}}, 1_000
    identity
  end

  test "public status and OTP formatted state do not include message arguments" do
    {client, socket} = connected()
    assert {:error, :invalid_delivery} = Client.settle(client, {self(), 1, 2}, :not_an_outcome)
    assert {:error, :unsupported_command} = GenServer.call(client, {:private, "secret-payload"})
    refute inspect(Client.status(client)) =~ "secret-payload"
    refute inspect(:sys.get_status(client)) =~ "private-container"
    assert Process.info(client, :messages) == {:messages, []}
    :gen_tcp.close(socket)
  end

  defp listener do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    {listener, port}
  end

  defp peer(options) do
    {listener, port} = listener()

    options =
      Keyword.merge([container_id: "private-container", host: "127.0.0.1", port: port], options)

    client = start_supervised!(Supervisor.child_spec({Client, options}, id: make_ref()))
    {:ok, socket} = :gen_tcp.accept(listener, 1_000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    {client, socket}
  end

  defp connected(options \\ []) do
    {pdu, options} = Keyword.pop(options, :peer_open, open())
    {client, socket} = peer(options)
    assert {:ok, @header} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = recv_pdu(socket)
    :ok = :gen_tcp.send(socket, @header <> frame(pdu))
    assert_receive {:graviton_mq, ^client, {:connected, "peer"}}, 1_000
    {client, socket}
  end

  defp open, do: %Open{container_id: Value.string("peer"), max_frame_size: Value.uint(512)}

  defp frame(pdu) do
    {:ok, bytes} = Performative.encode(pdu)
    <<8 + byte_size(bytes)::32, 2, 0, 0::16, bytes::binary>>
  end

  defp recv_pdu(socket) do
    assert {:ok, <<size::32>>} = :gen_tcp.recv(socket, 4, 1_000)
    assert {:ok, rest} = :gen_tcp.recv(socket, size - 4, 1_000)
    assert {:ok, frame, <<>>} = Frame.decode(<<size::32, rest::binary>>)
    assert {:ok, pdu, <<>>} = Performative.decode(frame.body)
    pdu
  end
end
