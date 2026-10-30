defmodule GravitonMQ.MQTTClientTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTTClient

  test "supervisor child specifications keep credentials out of crash metadata" do
    specification =
      MQTTClient.child_spec(
        client_id: "private",
        username: "private-user",
        password: "private-secret"
      )

    refute inspect(specification) =~ "private-secret"
    refute inspect(specification) =~ "private-user"
  end

  test "configuration rejects unsupported versions, invalid limits and unknown options" do
    for options <- [
          [version: :v3_1_1],
          [max_deliveries: 0],
          [port: -1],
          [transport: :websocket],
          [reconnect: [max_attempts: -1]],
          [mqtt_typo: true],
          [host: {256, 0, 0, 1}],
          [host: {:invalid, 0, 0, 1}],
          [host: [-1]],
          [properties: [receive_maximum: 65_535]],
          [properties: [topic_alias_maximum: 10]],
          [tls_options: [verify: :verify_none], transport: :tls],
          [tls_options: [server_name_indication: :disable], transport: :tls]
        ] do
      assert {:error, _} = MQTTClient.start_link([client_id: "bad"] ++ options)
    end
  end

  test "an invalid UTF-8 Will fails before starting an outgoing connection" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    fields = [topic: "will/topic", payload: <<0xFF>>, properties: [payload_format_indicator: 1]]

    for will <- [fields, struct!(GravitonMQ.MQTT.Packet.Will, fields)] do
      assert {:error, :payload_format_invalid} =
               MQTTClient.start_link(
                 client_id: "invalid-will",
                 host: "127.0.0.1",
                 port: port,
                 will: will
               )
    end

    assert {:error, :timeout} = :gen_tcp.accept(listener, 50)
  end

  test "unexpected remote Session Present closes the connection without a local session" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    client =
      start_supervised!(
        {MQTTClient,
         client_id: "missing-session",
         host: "127.0.0.1",
         port: port,
         observer: self(),
         clean_start: false,
         reconnect: [max_attempts: 2]}
      )

    {:ok, socket} = :gen_tcp.accept(listener, 1_000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    {:ok, _connect} = :gen_tcp.recv(socket, 0, 1_000)
    :ok = :gen_tcp.send(socket, <<0x20, 3, 1, 0, 0>>)
    assert_receive {:graviton_mq, ^client, {:disconnected, :local_session_missing}}, 1_000
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
    refute_receive {:graviton_mq, ^client, {:connected, _}}, 50
    assert MQTTClient.status(client).phase == :stopped
    assert {:error, :timeout} = :gen_tcp.accept(listener, 50)
  end

  test "refused connections return observable failure without claiming a connection" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    {:ok, port} = :inet.port(listener)
    :ok = :gen_tcp.close(listener)

    client =
      start_supervised!(
        {MQTTClient,
         client_id: "refused",
         host: "127.0.0.1",
         port: port,
         observer: self(),
         reconnect: [max_attempts: 0]}
      )

    assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 2_000
    assert MQTTClient.status(client).phase == :stopped
    assert {:error, :not_connected} = MQTTClient.publish(client, "a", "payload", qos: 1)
    assert MQTTClient.status(client).pending == 0
  end

  test "malformed peer input fails explicitly and status never reveals credentials" do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])

    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    client =
      start_supervised!(
        {MQTTClient,
         client_id: "malformed",
         host: "127.0.0.1",
         port: port,
         observer: self(),
         username: "sensitive-user",
         password: "sensitive-password"}
      )

    {:ok, socket} = :gen_tcp.accept(listener, 1_000)
    {:ok, <<0x10, _rest::binary>>} = :gen_tcp.recv(socket, 0, 1_000)
    :ok = :gen_tcp.send(socket, <<0x20, 3, 0, 0, 0, 0xC0, 1, 0>>)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 1_000
    assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 1_000
    status = MQTTClient.status(client)
    assert status.phase == :stopped
    refute inspect(status) =~ "sensitive"
    assert {:error, :not_connected} = MQTTClient.subscribe(client, "a/#", qos: 1)
    :gen_tcp.close(socket)
  end

  test "a sent QoS1 timeout is unknown and closes the connection" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    client =
      start_supervised!(
        {MQTTClient,
         client_id: "timeout",
         host: "127.0.0.1",
         port: port,
         observer: self(),
         operation_timeout: 50}
      )

    {:ok, socket} = :gen_tcp.accept(listener, 1_000)
    {:ok, _} = :gen_tcp.recv(socket, 0, 1_000)
    :ok = :gen_tcp.send(socket, <<0x20, 3, 0, 0, 0>>)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 1_000
    assert {:ok, ref} = MQTTClient.publish(client, "a", "x", qos: 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^ref, {:error, :unknown_outcome}}}, 1_000
    assert_receive {:graviton_mq, ^client, {:disconnected, :operation_timeout}}, 1_000
    assert MQTTClient.status(client).phase == :stopped
    :gen_tcp.close(socket)
  end

  test "the local packet limit rejects admission without closing a healthy connection" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    client =
      start_supervised!(
        {MQTTClient,
         client_id: "packet-limit",
         host: "127.0.0.1",
         port: port,
         observer: self(),
         max_packet_size: 128}
      )

    {:ok, socket} = :gen_tcp.accept(listener, 1_000)
    {:ok, _} = :gen_tcp.recv(socket, 0, 1_000)
    :ok = :gen_tcp.send(socket, <<0x20, 3, 0, 0, 0>>)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 1_000
    assert {:error, _reason} = MQTTClient.publish(client, "a", :binary.copy("x", 200), qos: 1)
    assert MQTTClient.status(client).phase == :connected
    assert MQTTClient.status(client).pending == 0
    :gen_tcp.close(socket)
  end

  test "an oversized announced peer packet disconnects with the specific reason before its body" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    client =
      start_supervised!(
        {MQTTClient,
         client_id: "incoming-size-limit",
         host: "127.0.0.1",
         port: port,
         observer: self(),
         max_packet_size: 128}
      )

    {:ok, socket} = :gen_tcp.accept(listener, 1000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    {:ok, _connect} = :gen_tcp.recv(socket, 0, 1000)
    :ok = :gen_tcp.send(socket, <<0x20, 3, 0, 0, 0>>)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 1000

    # Total size129 exceeds the advertised128; no body need be allocated/read.
    :ok = :gen_tcp.send(socket, <<0x30, 127>>)
    assert {:ok, encoded} = :gen_tcp.recv(socket, 0, 1000)

    assert {:ok, %GravitonMQ.MQTT.Packet.Disconnect{reason_code: 0x95}, <<>>} =
             GravitonMQ.MQTT.Codec.Packet.decode(encoded, :v5)

    assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 1000
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1000)
    assert MQTTClient.status(client).phase == :stopped
  end

  test "normal shutdown cannot send DISCONNECT above the negotiated peer limit" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    client =
      start_supervised!(
        {MQTTClient, client_id: "shutdown-limit", host: "127.0.0.1", port: port, observer: self()}
      )

    {:ok, socket} = :gen_tcp.accept(listener, 1_000)
    {:ok, _} = :gen_tcp.recv(socket, 0, 1_000)
    :ok = :gen_tcp.send(socket, <<0x20, 8, 0, 0, 5, 0x27, 1::32>>)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 1_000
    :ok = GenServer.stop(client)
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "host shutdown invokes cleanup and gracefully disconnects a connected client" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    {:ok, host} =
      Supervisor.start_link(
        [
          {MQTTClient,
           client_id: "graceful-host", host: "127.0.0.1", port: port, observer: self()}
        ],
        strategy: :one_for_one
      )

    Process.unlink(host)
    on_exit(fn -> if Process.alive?(host), do: Supervisor.stop(host) end)
    [{_, client, _, _}] = Supervisor.which_children(host)
    {:ok, socket} = :gen_tcp.accept(listener, 1_000)
    {:ok, _} = :gen_tcp.recv(socket, 0, 1_000)
    :ok = :gen_tcp.send(socket, <<0x20, 3, 0, 0, 0>>)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 1_000
    :ok = Supervisor.stop(host)
    assert {:ok, <<0xE0, 0>>} = :gen_tcp.recv(socket, 0, 1_000)
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "the client monitors its receiver and closes the owned socket when it dies" do
    receiver =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    {:ok, client} =
      MQTTClient.start_link(
        client_id: "receiver",
        host: "127.0.0.1",
        port: port,
        observer: receiver
      )

    monitor = Process.monitor(client)
    {:ok, socket} = :gen_tcp.accept(listener, 1_000)
    {:ok, _} = :gen_tcp.recv(socket, 0, 1_000)
    send(receiver, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^client, :normal}, 1_000
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "a supervised receiver death does not restart into a dead observer or kill its host" do
    receiver =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    {:ok, host} =
      Supervisor.start_link(
        [
          {MQTTClient,
           client_id: "supervised-receiver", host: "127.0.0.1", port: port, observer: receiver}
        ],
        strategy: :one_for_one
      )

    Process.unlink(host)
    on_exit(fn -> if Process.alive?(host), do: Supervisor.stop(host) end)
    monitor = Process.monitor(host)
    [{_, client, _, _}] = Supervisor.which_children(host)
    client_monitor = Process.monitor(client)
    {:ok, socket} = :gen_tcp.accept(listener, 1_000)
    {:ok, _} = :gen_tcp.recv(socket, 0, 1_000)
    send(receiver, :stop)
    assert_receive {:DOWN, ^client_monitor, :process, ^client, :normal}, 1_000
    refute_receive {:DOWN, ^monitor, :process, ^host, _}, 200
    assert [{_, :undefined, _, _}] = Supervisor.which_children(host)
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end
end
