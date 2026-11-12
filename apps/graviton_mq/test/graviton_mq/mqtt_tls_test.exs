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
