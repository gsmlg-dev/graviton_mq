defmodule GravitonMQ.MQTTBroker.ConnectionTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Connack, Disconnect}
  alias GravitonMQ.Runtime.MQTT.{Broker, Listener}

  @moduletag timeout: 10_000

  # These exercise our socket boundary with independently constructed wire
  # packets; external-client interoperability is qualified separately.
  defmodule Policy do
    def authenticate(%{client_id: client_id}, options) do
      send(options[:observer], {:authenticated, client_id})
      {:ok, :client}
    end

    def authorize(_principal, _action, _resource, _options), do: :ok
  end

  test "an idle socket times out before authenticating or creating a session" do
    instance = broker(auth: {Policy, observer: self()}, connect_timeout: 150)
    %{owner: owner, port: port} = status(instance)
    socket = connect(port)

    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 20)
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
    refute_receive {:authenticated, _client_id}, 20
    assert eventually(fn -> match?(%{connections: 0, sessions: 0}, status(instance)) end)
    assert %{owner: ^owner} = status(instance)
  end

  test "fragmented CONNECT authenticates only after the final byte and then serves packets" do
    instance = broker(auth: {Policy, observer: self()})
    %{owner: owner, port: port} = status(instance)
    socket = connect(port)
    <<first, rest::binary>> = connect_packet("fragmented")
    prefix_size = byte_size(rest) - 1
    <<prefix::binary-size(prefix_size), last>> = rest

    assert :ok = :gen_tcp.send(socket, <<first>>)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 20)
    refute_receive {:authenticated, _client_id}, 20

    assert :ok = :gen_tcp.send(socket, prefix)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 20)
    refute_receive {:authenticated, _client_id}, 20
    assert %{owner: ^owner, connections: 1, sessions: 0, prepared: 0} = status(instance)

    assert :ok = :gen_tcp.send(socket, <<last>>)
    assert_receive {:authenticated, "fragmented"}, 1_000
    assert %Connack{reason_code: 0} = receive_packet(socket)
    assert_ping(socket)
    assert %{owner: ^owner, connections: 1, sessions: 1} = status(instance)
  end

  test "coalesced PINGREQ packets each receive a response before a real keep alive timeout" do
    instance = broker()
    %{owner: owner, port: port} = status(instance)
    socket = connect(port)
    assert :ok = :gen_tcp.send(socket, connect_packet("keepalive", 1))
    assert %Connack{reason_code: 0} = receive_packet(socket)

    sent_at = System.monotonic_time(:millisecond)
    assert :ok = :gen_tcp.send(socket, <<0xC0, 0, 0xC0, 0>>)
    assert {:ok, <<0xD0, 0, 0xD0, 0>>} = :gen_tcp.recv(socket, 4, 1_000)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 100)
    assert %Disconnect{reason_code: 0x8D} = receive_packet(socket, 2_000)
    assert System.monotonic_time(:millisecond) - sent_at >= 1_400
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
    assert eventually(fn -> match?(%{connections: 0, sessions: 0}, status(instance)) end)
    assert %{owner: ^owner} = status(instance)
  end

  test "a malformed peer closes without disturbing the owner or an active connection" do
    instance = broker()
    %{owner: owner, port: port} = status(instance)
    healthy = connected(port, "healthy")
    malformed = connect(port)

    assert :ok = :gen_tcp.send(malformed, <<0, 0>>)
    assert {:error, :closed} = :gen_tcp.recv(malformed, 0, 1_000)
    assert eventually(fn -> match?(%{connections: 1, sessions: 1}, status(instance)) end)
    assert %{owner: ^owner} = status(instance)
    assert_ping(healthy)
  end

  test "oversized announced packets use reason0x95 and preserve another network session" do
    instance = broker(max_packet_size: 128)
    %{owner: owner, port: port} = status(instance)
    healthy = connected(port, "healthy-limit")
    oversized = connected(port, "oversized-limit")
    assert :ok = :gen_tcp.send(oversized, <<0x30, 127>>)
    assert %Disconnect{reason_code: 0x95} = receive_packet(oversized)
    assert {:error, :closed} = :gen_tcp.recv(oversized, 0, 1000)
    assert eventually(fn -> match?(%{connections: 1, sessions: 1}, status(instance)) end)
    assert %{owner: ^owner} = status(instance)
    assert_ping(healthy)
  end

  test "owner failure closes old readers and sockets before serving with empty memory" do
    instance = broker()
    %{owner: owner, port: port} = status(instance)
    socket = connected(port, "old-owner")
    publish_retained(instance, owner)
    assert %{connections: 1, sessions: 1, retained: 1} = status(instance)
    mqtt = mqtt_supervisor(instance)
    connections = child(mqtt, :connections)
    [{_id, reader, _type, _modules}] = DynamicSupervisor.which_children(connections)
    owner_monitor = Process.monitor(owner)
    reader_monitor = Process.monitor(reader)

    capture_log(fn ->
      Process.exit(owner, :kill)
      assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}, 1_000
      assert_receive {:DOWN, ^reader_monitor, :process, ^reader, _reason}, 1_000
      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)

      assert eventually(fn ->
               replacement = find_child(mqtt, Broker)
               is_pid(replacement) and replacement != owner
             end)

      assert %{
               owner: replacement,
               connections: 0,
               sessions: 0,
               retained: 0,
               prepared: 0,
               pending_deliveries: 0
             } = status(instance)

      assert replacement != owner
      refute Process.alive?(reader)
      replacement_socket = connected(status(instance).port, "new-owner")
      assert_ping(replacement_socket)
    end)
  end

  test "listener failure preserves the owner, retained state and existing connection" do
    instance = broker()
    %{owner: owner, port: port} = status(instance)
    socket = connected(port, "listener-survivor")
    publish_retained(instance, owner)
    mqtt = mqtt_supervisor(instance)
    listener = child(mqtt, {Listener, :tcp})
    listener_monitor = Process.monitor(listener)

    capture_log(fn ->
      Process.exit(listener, :kill)
      assert_receive {:DOWN, ^listener_monitor, :process, ^listener, :killed}, 1_000

      assert eventually(fn ->
               replacement = find_child(mqtt, {Listener, :tcp})
               is_pid(replacement) and replacement != listener
             end)

      assert %{owner: ^owner, connections: 1, sessions: 1, retained: 1, port: new_port} =
               status(instance)

      assert_ping(socket)
      replacement_socket = connected(new_port, "new-listener")
      assert_ping(replacement_socket)
      assert %{owner: ^owner, connections: 2, sessions: 2, retained: 1} = status(instance)
    end)
  end

  defp broker(options \\ []) do
    start_supervised!(
      {GravitonMQ,
       name: nil,
       runtime_supervisor_name: nil,
       mqtt:
         Keyword.merge(
           [
             auth: :anonymous,
             host_principal: :test_host,
             connect_timeout: 500,
             tick_interval: 10,
             listeners: [[id: :tcp, transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
           ],
           options
         )},
      id: make_ref()
    )
  end

  defp status(instance) do
    assert {:ok, %{listeners: [%{port: port}]} = status} = GravitonMQ.status(instance)
    Map.put(status, :port, port)
  end

  defp connect(port) do
    assert {:ok, socket} =
             :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)

    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp connected(port, client_id) do
    socket = connect(port)
    assert :ok = :gen_tcp.send(socket, connect_packet(client_id))
    assert %Connack{reason_code: 0} = receive_packet(socket)
    socket
  end

  defp connect_packet(client_id, keep_alive \\ 0) do
    # MQTT 5, Clean Start, no credentials or Will, empty properties.
    body = <<0, 4, "MQTT", 5, 2, keep_alive::16, 0, byte_size(client_id)::16, client_id::binary>>
    assert byte_size(body) < 128
    <<0x10, byte_size(body), body::binary>>
  end

  defp assert_ping(socket) do
    assert :ok = :gen_tcp.send(socket, <<0xC0, 0>>)
    assert {:ok, <<0xD0, 0>>} = :gen_tcp.recv(socket, 2, 1_000)
  end

  defp receive_packet(socket, timeout \\ 1_000) do
    receive_packet(socket, <<>>, System.monotonic_time(:millisecond) + timeout)
  end

  defp receive_packet(socket, bytes, deadline) do
    case Packet.decode(bytes, :v5) do
      {:ok, packet, <<>>} ->
        packet

      {:more, _needed} ->
        remaining = max(deadline - System.monotonic_time(:millisecond), 0)
        assert {:ok, more} = :gen_tcp.recv(socket, 0, remaining)
        receive_packet(socket, bytes <> more, deadline)

      other ->
        flunk("unexpected MQTT response: #{inspect(other)}")
    end
  end

  defp publish_retained(instance, owner) do
    assert {:ok, reference} = GravitonMQ.publish(instance, "state/value", "memory", retain: true)

    assert_receive {:graviton_mq, ^owner, {:operation, ^reference, {:ok, :accepted_in_memory}}},
                   1_000
  end

  defp mqtt_supervisor(instance) do
    instance
    |> child(GravitonMQ.Runtime.Supervisor)
    |> child(GravitonMQ.Runtime.MQTT.Supervisor)
  end

  defp child(supervisor, id) do
    pid = find_child(supervisor, id)
    assert is_pid(pid)
    pid
  end

  defp find_child(supervisor, id) do
    case List.keyfind(Supervisor.which_children(supervisor), id, 0) do
      {^id, pid, _type, _modules} when is_pid(pid) -> pid
      _ -> nil
    end
  end

  defp eventually(predicate) do
    eventually(predicate, System.monotonic_time(:millisecond) + 1_000)
  end

  defp eventually(predicate, deadline) do
    cond do
      predicate.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(5)
        eventually(predicate, deadline)
    end
  end
end
