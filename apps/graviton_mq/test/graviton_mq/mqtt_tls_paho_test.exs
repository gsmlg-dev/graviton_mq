Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)
Code.require_file("../../../graviton_mq_runtime/test/support/tls.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.TLSPahoTest do
  use ExUnit.Case, async: false
  @moduletag :paho_interop
  @moduletag timeout: 60_000

  alias GravitonMQ.Client.Test.Mosquitto
  alias GravitonMQ.Runtime.Test.TLS

  defmodule Policy do
    def authenticate(credentials, options) do
      send(options[:observer], {:peer, credentials.peer})
      {:ok, :client}
    end

    def authorize(_principal, _action, _resource, _options), do: :ok
  end

  setup_all do
    %{certificates: TLS.certificates!()}
  end

  test "Paho completes a verified TLS 1.3 round trip; the broker records TLS 1.3",
       %{certificates: certificates} do
    instance = broker(certificates)
    peer = peer!(["round-trip", port(instance), certificates.ca, "--hold", "2"])
    assert eventually!(fn -> tls_protocol(instance) == :"tlsv1.3" end)
    IO.puts("broker protocol=tlsv1.3; host #{String.trim(TLS.openssl!(["version"]))}")
    assert completed!(peer) =~ "Paho TLS round trip PASS"
  end

  test "Paho presents a client certificate; the mTLS principal facts are exact",
       %{certificates: certificates} do
    instance =
      broker(certificates, [auth: {Policy, observer: self()}],
        cacertfile: certificates.ca,
        verify: :verify_peer,
        fail_if_no_peer_cert: true
      )

    peer =
      peer!([
        "round-trip",
        port(instance),
        certificates.ca,
        "--cert",
        certificates.client_certificate,
        "--key",
        certificates.client_key
      ])

    client_der = certificates.client_der
    assert_receive {:peer, %{certificate: ^client_der, certificate_verified: true}}, 5_000
    assert completed!(peer) =~ "Paho TLS round trip PASS"
  end

  test "a TLS1.2-only Paho client is refused", %{certificates: certificates} do
    instance = broker(certificates)
    peer = peer!(["tls12-only", port(instance), certificates.ca])
    assert completed!(peer) =~ "TLS1.2-only refused"
    assert {:ok, %{connections: 0, sessions: 0}} = GravitonMQ.status(instance)
  end

  defp broker(certificates, options \\ [], tls \\ []) do
    start_supervised!(
      {GravitonMQ,
       name: nil,
       runtime_supervisor_name: nil,
       mqtt:
         Keyword.merge(
           [
             auth: :anonymous,
             host_principal: :host,
             listeners: [
               [
                 transport: :tls,
                 ip: {127, 0, 0, 1},
                 port: 0,
                 tls_options: TLS.listener_options(certificates, tls)
               ]
             ]
           ],
           options
         )},
      id: make_ref()
    )
  end

  defp port(instance) do
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    Integer.to_string(port)
  end

  defp peer!(arguments),
    do:
      Mosquitto.process!(
        System.fetch_env!("GRAVITON_PAHO_PYTHON"),
        [Path.expand("../support/mqtt_tls_peer.py", __DIR__) | arguments]
      )

  # The round-trip peer holds its connection for --hold seconds after PASS.
  defp completed!(peer) do
    Mosquitto.eventually!(
      fn -> Mosquitto.snapshot(peer).exit_status != nil end,
      fn -> Mosquitto.snapshot(peer) end,
      15_000
    )

    assert %{exit_status: 0, output: output} = Mosquitto.snapshot(peer)
    IO.puts(output)
    output
  end

  defp tls_protocol(instance) do
    connections =
      instance
      |> find(GravitonMQ.Runtime.Supervisor)
      |> find(GravitonMQ.Runtime.MQTT.Supervisor)
      |> find(:connections)

    # The socket is nil until the Connection receives {:accepted, socket}.
    with [{_, connection, _, _} | _] <- DynamicSupervisor.which_children(connections),
         %SSL.Socket{} = socket <- :sys.get_state(connection).socket,
         {:ok, information} <- SSL.connection_information(socket, [:protocol]) do
      information[:protocol]
    else
      _ -> nil
    end
  end

  defp find(supervisor, id) do
    {^id, pid, _, _} = List.keyfind(Supervisor.which_children(supervisor), id, 0)
    pid
  end

  defp eventually!(predicate, deadline \\ System.monotonic_time(:millisecond) + 5_000) do
    cond do
      predicate.() -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true -> Process.sleep(20) && eventually!(predicate, deadline)
    end
  end
end
