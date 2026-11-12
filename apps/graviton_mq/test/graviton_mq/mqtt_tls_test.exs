Code.require_file("../../../graviton_mq_runtime/test/support/tls.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.TLSTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.Connack
  alias GravitonMQ.Runtime.Test.TLS

  @compile {:no_warn_undefined, :ssl}
  @moduletag timeout: 30_000

  defmodule Policy do
    def authenticate(credentials, options) do
      send(options[:observer], {:peer, credentials.client_id, credentials.peer})
      {:ok, :client}
    end

    def authorize(_principal, _action, _resource, _options), do: :ok
  end

  setup_all do
    %{certificates: TLS.certificates!()}
  end

  test "a verified TLS 1.3 client completes CONNECT and PINGREQ; the server socket is TLS 1.3",
       %{certificates: certificates} do
    instance = broker(certificates)
    socket = tls_connected(port(instance), certificates, "tls-round-trip")
    assert_ping(socket)

    assert {:ok, information} = SSL.connection_information(server_socket(connection!(instance)))
    assert information[:protocol] == :"tlsv1.3"
    assert information[:peer_authenticated] == false
  end

  # T2
  test "a TLS 1.2-only client is refused during the handshake and reaches no owner",
       %{certificates: certificates} do
    instance = broker(certificates, auth: {Policy, observer: self()})

    assert {:error, {:tls_alert, {:protocol_version, _description}}} =
             TLS.connect(port(instance), certificates, versions: [:"tlsv1.2"])

    refute_receive {:peer, _client_id, _peer}, 100
    assert %{active: 0} = DynamicSupervisor.count_children(connections(instance))
    assert %{connections: 0, sessions: 0} = status(instance)
  end

  # T3: optional client certificate under verify_peer
  test "optional client certificates give exact peer facts and agree with ex_ssl",
       %{certificates: certificates} do
    instance =
      broker(certificates, [auth: {Policy, observer: self()}],
        cacertfile: certificates.ca,
        verify: :verify_peer,
        fail_if_no_peer_cert: false
      )

    port = port(instance)
    anonymous = tls_connected(port, certificates, "no-certificate")

    assert_receive {:peer, "no-certificate",
                    %{certificate: nil, certificate_verified: false, transport: :tls}}

    assert {:ok, information} = SSL.connection_information(server_socket(connection!(instance)))
    assert information[:peer_authenticated] == false
    :ok = :ssl.close(anonymous)
    assert eventually(fn -> status(instance).connections == 0 end)

    _identified =
      tls_connected(port, certificates, "with-certificate", TLS.client_identity(certificates))

    client_der = certificates.client_der

    assert_receive {:peer, "with-certificate",
                    %{certificate: ^client_der, certificate_verified: true, transport: :tls}}

    assert {:ok, information} = SSL.connection_information(server_socket(connection!(instance)))
    assert information[:peer_authenticated] == :client

    refused!(TLS.connect(port, certificates, TLS.stranger_identity(certificates)))
    refute_receive {:peer, "refused", _peer}, 200
  end

  # T3: required client certificate
  test "a required client certificate refuses an anonymous client and admits a trusted one",
       %{certificates: certificates} do
    instance =
      broker(certificates, [auth: {Policy, observer: self()}],
        cacertfile: certificates.ca,
        verify: :verify_peer,
        fail_if_no_peer_cert: true
      )

    port = port(instance)
    refused!(TLS.connect(port, certificates))
    refute_receive {:peer, "refused", _peer}, 200

    _identified = tls_connected(port, certificates, "required", TLS.client_identity(certificates))
    client_der = certificates.client_der
    assert_receive {:peer, "required", %{certificate: ^client_der, certificate_verified: true}}
  end

  # T3: verify_none requests no certificate
  test "a listener without verify_peer requests no client certificate",
       %{certificates: certificates} do
    instance = broker(certificates, auth: {Policy, observer: self()})

    _socket =
      tls_connected(
        port(instance),
        certificates,
        "not-requested",
        TLS.client_identity(certificates)
      )

    assert_receive {:peer, "not-requested",
                    %{certificate: nil, certificate_verified: false, transport: :tls}}
  end

  # T4 (listener level; replaces the old Connection-level handshake test)
  test "silent and stalled TLS peers are closed within connect_timeout before any Connection",
       %{certificates: certificates} do
    instance = broker(certificates, auth: {Policy, observer: self()}, connect_timeout: 300)

    for prefix <- [<<>>, <<22, 3, 1, 0, 64, 1, 0, 0, 60>>] do
      {:ok, raw} =
        :gen_tcp.connect({127, 0, 0, 1}, port(instance), [:binary, active: false], 1_000)

      started = System.monotonic_time(:millisecond)
      :ok = :gen_tcp.send(raw, prefix)
      assert {:error, :timeout} = :gen_tcp.recv(raw, 0, 150)
      assert %{active: 0} = DynamicSupervisor.count_children(connections(instance))
      assert %{connections: 0} = status(instance)
      assert :closed = closed(raw, started + 2_000)
      elapsed = System.monotonic_time(:millisecond) - started
      assert elapsed >= 250 and elapsed < 1_500, "closed after #{elapsed} ms"
    end

    refute_received {:peer, _client_id, _peer}
  end

  # Helpers (used by all MQTT TLS tests).

  defp broker(certificates, options \\ [], tls \\ []) do
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
             listeners: [tls_listener(certificates, tls)]
           ],
           options
         )},
      id: make_ref()
    )
  end

  defp tls_listener(certificates, tls),
    do: [
      id: :tls,
      transport: :tls,
      ip: {127, 0, 0, 1},
      port: 0,
      tls_options: TLS.listener_options(certificates, tls)
    ]

  defp status(instance) do
    assert {:ok, status} = GravitonMQ.status(instance)
    status
  end

  defp port(instance, id \\ :tls) do
    %{port: port} = Enum.find(status(instance).listeners, &(&1.id == id))
    port
  end

  defp tls_connected(port, certificates, client_id, options \\ []) do
    assert {:ok, socket} = TLS.connect(port, certificates, options)
    on_exit(fn -> :ssl.close(socket) end)
    assert :ok = :ssl.send(socket, connect_packet(client_id))
    assert %Connack{reason_code: 0} = receive_packet(socket)
    socket
  end

  defp connect_packet(client_id) do
    # MQTT 5, Clean Start, keep alive 0, no credentials/Will, empty properties.
    body = <<0, 4, "MQTT", 5, 2, 0::16, 0, byte_size(client_id)::16, client_id::binary>>
    <<0x10, byte_size(body), body::binary>>
  end

  defp assert_ping(socket) do
    assert :ok = :ssl.send(socket, <<0xC0, 0>>)
    assert {:ok, <<0xD0, 0>>} = :ssl.recv(socket, 2, 2_000)
  end

  defp receive_packet(socket, timeout \\ 2_000) do
    {[packet], _rest} =
      receive_packets(socket, 1, <<>>, System.monotonic_time(:millisecond) + timeout)

    packet
  end

  # Reads exactly `count` packets; returns them in wire order plus leftovers.
  defp receive_packets(socket, count, bytes, deadline, packets \\ [])

  defp receive_packets(_socket, 0, bytes, _deadline, packets), do: {Enum.reverse(packets), bytes}

  defp receive_packets(socket, count, bytes, deadline, packets) do
    case Packet.decode(bytes, :v5) do
      {:ok, packet, rest} ->
        receive_packets(socket, count - 1, rest, deadline, [packet | packets])

      {:more, _needed} ->
        remaining = max(deadline - System.monotonic_time(:millisecond), 0)
        assert {:ok, more} = :ssl.recv(socket, 0, remaining)
        receive_packets(socket, count, bytes <> more, deadline, packets)

      other ->
        flunk("unexpected MQTT bytes: #{inspect(other)}")
    end
  end

  defp mqtt_supervisor(instance) do
    instance
    |> child(GravitonMQ.Runtime.Supervisor)
    |> child(GravitonMQ.Runtime.MQTT.Supervisor)
  end

  defp connections(instance), do: child(mqtt_supervisor(instance), :connections)

  defp connection!(instance) do
    supervisor = connections(instance)

    assert eventually(fn -> match?([_], DynamicSupervisor.which_children(supervisor)) end)
    [{_id, connection, _type, _modules}] = DynamicSupervisor.which_children(supervisor)
    connection
  end

  # The MQTT Connection state is a plain map; :sys.get_state/1 bypasses the
  # redacting format_status/1 and also works while the process is suspended.
  defp server_socket(connection), do: :sys.get_state(connection).socket

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

  # TLS 1.3: the client may finish before the server rejects its certificate;
  # then the server's alert (or close) is the first read result.
  defp refused!({:error, _reason}), do: :ok

  defp refused!({:ok, socket}) do
    _ = :ssl.send(socket, connect_packet("refused"))
    assert {:error, _reason} = :ssl.recv(socket, 0, 2_000)
  end

  defp closed(raw, deadline) do
    case :gen_tcp.recv(raw, 0, max(deadline - System.monotonic_time(:millisecond), 0)) do
      {:ok, _bytes} -> closed(raw, deadline)
      {:error, reason} -> reason
    end
  end

  defp eventually(predicate, timeout \\ 2_000),
    do: eventually_until(predicate, System.monotonic_time(:millisecond) + timeout)

  defp eventually_until(predicate, deadline) do
    cond do
      predicate.() -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true -> Process.sleep(5) && eventually_until(predicate, deadline)
    end
  end
end
