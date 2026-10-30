Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.MosquittoTest do
  use ExUnit.Case, async: false
  @moduletag :interop

  alias GravitonMQ.Client.Test.Mosquitto

  defmodule Policy do
    @behaviour GravitonMQ.Runtime.MQTT.Auth
    def authenticate(credentials, options) do
      if observer = Keyword.get(options, :observer),
        do: send(observer, {:authentication_peer, credentials.peer})

      case Keyword.get(options, :mode) do
        :timeout ->
          receive do
            :finish -> {:ok, :user}
          end

        _ ->
          case credentials do
            %{username: "user", password: "synthetic-password", peer: %{ip: {127, 0, 0, 1}}} ->
              {:ok, :user}

            _ ->
              {:error, :denied}
          end
      end
    end

    def authorize(:trusted_host, _action, _resource, _options), do: :ok
    def authorize(:user, :subscribe, "allowed/" <> _filter, _options), do: :ok

    def authorize(:user, action, "allowed/" <> _topic, _options)
        when action in [:publish, :deliver],
        do: :ok

    def authorize(_principal, _action, _resource, _options), do: {:error, :denied}
  end

  setup do
    %{executables: Mosquitto.executables!()}
  end

  defp broker(options \\ [], listener_options \\ []) do
    listener = Keyword.merge([transport: :tcp, ip: {127, 0, 0, 1}, port: 0], listener_options)

    instance =
      start_supervised!(
        {GravitonMQ,
         name: nil,
         runtime_supervisor_name: nil,
         mqtt:
           Keyword.merge(
             [auth: :anonymous, host_principal: :trusted_host, listeners: [listener]],
             options
           )},
        id: make_ref()
      )

    assert {:ok, %{owner: owner, listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    %{instance: instance, owner: owner, port: port}
  end

  defp subscription(broker, filter, options) do
    assert {:ok, reference} = GravitonMQ.subscribe(broker.instance, filter, options)
    assert_receive {:graviton_mq, owner, {:operation, ^reference, {:ok, subscription}}}, 1_000
    assert owner == broker.owner
    subscription
  end

  defp publish(broker, topic, payload, options) do
    assert {:ok, reference} = GravitonMQ.publish(broker.instance, topic, payload, options)

    assert_receive {:graviton_mq, owner, {:operation, ^reference, {:ok, :accepted_in_memory}}},
                   1_000

    assert owner == broker.owner
  end

  defp subscriber(executables, broker, filter, options \\ []) do
    # Mosquitto's debug stdout is otherwise block-buffered when it is a pipe.
    # Observe the independent client's actual SUBACK before publishing.
    stdbuf = System.find_executable("stdbuf") || flunk("broker qualification requires stdbuf")

    process =
      Mosquitto.process!(
        stdbuf,
        ["-oL", executables.subscriber] ++
          Mosquitto.cli_args(broker, options) ++
          [
            "-t",
            filter,
            "-q",
            to_string(Keyword.get(options, :qos, 1)),
            "-C",
            "1",
            "-W",
            to_string(Keyword.get(options, :wait, 5)),
            "-d",
            "-F",
            "%t|%q|%r|%p"
          ]
      )

    Mosquitto.eventually!(
      fn -> Mosquitto.snapshot(process).output =~ "received SUBACK" end,
      fn -> Mosquitto.snapshot(process) end
    )

    process
  end

  defp completed(process) do
    Mosquitto.eventually!(
      fn -> Mosquitto.snapshot(process).exit_status != nil end,
      fn -> Mosquitto.snapshot(process) end
    )

    snapshot = Mosquitto.snapshot(process)
    assert snapshot.exit_status == 0, snapshot.output
    snapshot.output
  end

  defp failed(process) do
    Mosquitto.eventually!(
      fn -> Mosquitto.snapshot(process).exit_status != nil end,
      fn -> Mosquitto.snapshot(process) end
    )

    snapshot = Mosquitto.snapshot(process)
    assert snapshot.exit_status != 0, snapshot.output
    snapshot.output
  end

  defp force_disconnect(process) do
    port = :sys.get_state(process).port
    {:os_pid, pid} = Port.info(port, :os_pid)
    {_, 0} = System.cmd(System.find_executable("kill"), ["-KILL", Integer.to_string(pid)])
    Mosquitto.eventually!(fn -> Mosquitto.snapshot(process).exit_status != nil end)
  end

  test "independent MQTT5 publishers enter the real broker and monitored host path", context do
    broker = broker()
    subscription = subscription(broker, "ingress/+", qos: 1)

    for qos <- [0, 1] do
      :ok = GravitonMQ.request(broker.instance, subscription, 1)
      Mosquitto.publish!(context.executables, broker, "ingress/value", "qos#{qos}", qos)

      assert_receive {:graviton_mq, owner,
                      {:delivery, ^subscription, delivery,
                       %{topic: "ingress/value", payload: payload, qos: ^qos}}},
                     1_000

      assert owner == broker.owner
      assert payload == "qos#{qos}"
      assert :ok = GravitonMQ.ack(broker.instance, delivery)
    end
  end

  test "host publications and independent network publishers reach independent subscribers",
       context do
    broker = broker()

    for qos <- [0, 1] do
      peer = subscriber(context.executables, broker, "egress/value")
      publish(broker, "egress/value", "host#{qos}", qos: qos)
      assert completed(peer) =~ "egress/value|#{qos}|0|host#{qos}"
    end

    peer = subscriber(context.executables, broker, "network/#")
    Mosquitto.publish!(context.executables, broker, "network/value", "network", 1)
    assert completed(peer) =~ "network/value|1|0|network"
  end

  test "retained content replays to an independent client and an empty retained publish deletes it",
       context do
    broker = broker()

    Mosquitto.command!(
      context.executables.publisher,
      Mosquitto.cli_args(broker) ++ ["-t", "retained/value", "-m", "saved", "-q", "1", "-r"]
    )

    peer = subscriber(context.executables, broker, "retained/#")
    assert completed(peer) =~ "retained/value|1|1|saved"

    Mosquitto.command!(
      context.executables.publisher,
      Mosquitto.cli_args(broker) ++ ["-t", "retained/value", "-n", "-q", "1", "-r"]
    )

    assert {:ok, %{retained: 0}} = GravitonMQ.status(broker.instance)
  end

  test "two operational brokers isolate wire routes and retained state", context do
    first = broker()
    second = broker()
    first_subscription = subscription(first, "isolation/#", qos: 1)
    second_subscription = subscription(second, "isolation/#", qos: 1)
    assert :ok = GravitonMQ.request(first.instance, first_subscription, 1)
    assert :ok = GravitonMQ.request(second.instance, second_subscription, 1)
    Mosquitto.publish!(context.executables, first, "isolation/value", "first", 1)

    assert_receive {:graviton_mq, owner,
                    {:delivery, ^first_subscription, _, %{payload: "first"}}},
                   1_000

    assert owner == first.owner
    refute_receive {:graviton_mq, _, {:delivery, ^second_subscription, _, _}}, 50
    Mosquitto.publish!(context.executables, second, "isolation/value", "second", 1)

    assert_receive {:graviton_mq, second_owner,
                    {:delivery, ^second_subscription, _, %{payload: "second"}}},
                   1_000

    assert second_owner == second.owner
    assert first.port != second.port
  end

  test "independent clients exchange over verified TLS", context do
    directory = Mosquitto.directory!()
    on_exit(fn -> File.rm_rf!(directory) end)
    certificates = Mosquitto.certificates!(context.executables, directory)

    broker =
      broker([],
        transport: :tls,
        tls_options: [certfile: certificates.certificate, keyfile: certificates.private_key]
      )

    tls = [host: "localhost", extra: ["--cafile", certificates.ca_certificate]]
    peer = subscriber(context.executables, broker, "secure/#", tls)
    Mosquitto.publish!(context.executables, broker, "secure/value", "tls", 1, tls)
    assert completed(peer) =~ "secure/value|1|0|tls"

    untrusted =
      Mosquitto.process!(
        context.executables.publisher,
        Mosquitto.cli_args(broker, host: "localhost", extra: ["--cafile", certificates.untrusted]) ++
          ["-t", "secure/value", "-m", "rejected", "-q", "1", "-d"]
      )

    output = failed(untrusted)
    assert output =~ "certificate verify failed"
    refute output =~ "received CONNACK"

    Mosquitto.eventually!(fn ->
      match?({:ok, %{sessions: 0, connections: 0}}, GravitonMQ.status(broker.instance))
    end)
  end

  test "optional TLS client certificates report authentication facts accurately", context do
    directory = Mosquitto.directory!()
    on_exit(fn -> File.rm_rf!(directory) end)
    certificates = Mosquitto.certificates!(context.executables, directory)

    broker =
      broker([auth: {Policy, [observer: self()]}],
        transport: :tls,
        tls_options: [
          certfile: certificates.certificate,
          keyfile: certificates.private_key,
          cacertfile: certificates.ca_certificate,
          verify: :verify_peer,
          fail_if_no_peer_cert: false
        ]
      )

    credentials = [
      host: "localhost",
      extra: ["--cafile", certificates.ca_certificate, "-u", "user", "-P", "synthetic-password"]
    ]

    Mosquitto.publish!(
      context.executables,
      broker,
      "allowed/value",
      "no-certificate",
      1,
      credentials
    )

    assert_receive {:authentication_peer,
                    %{certificate: nil, certificate_verified: false, transport: :tls}},
                   1_000

    certificate = Path.join(directory, "client.pem")
    key = Path.join(directory, "client.key")
    request = Path.join(directory, "client.csr")
    extensions = Path.join(directory, "client.ext")

    File.write!(extensions, """
    basicConstraints=critical,CA:FALSE
    keyUsage=critical,digitalSignature,keyEncipherment
    extendedKeyUsage=clientAuth
    """)

    Mosquitto.command!(context.executables.openssl, [
      "req",
      "-new",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-subj",
      "/CN=independent-client",
      "-keyout",
      key,
      "-out",
      request
    ])

    Mosquitto.command!(context.executables.openssl, [
      "x509",
      "-req",
      "-in",
      request,
      "-CA",
      certificates.ca_certificate,
      "-CAkey",
      certificates.ca_private_key,
      "-set_serial",
      "2",
      "-days",
      "1",
      "-extfile",
      extensions,
      "-out",
      certificate
    ])

    with_certificate =
      Keyword.update!(credentials, :extra, &(&1 ++ ["--cert", certificate, "--key", key]))

    Mosquitto.publish!(
      context.executables,
      broker,
      "allowed/value",
      "with-certificate",
      1,
      with_certificate
    )

    assert_receive {:authentication_peer,
                    %{certificate: der, certificate_verified: true, transport: :tls}},
                   1_000

    assert is_binary(der) and byte_size(der) > 0
  end

  test "authentication, publish ACL and wildcard scope denial reach independent clients",
       context do
    broker = broker(auth: {Policy, []})

    bad =
      Mosquitto.process!(
        context.executables.publisher,
        Mosquitto.cli_args(broker) ++
          ["-u", "user", "-P", "wrong", "-t", "allowed/value", "-m", "x", "-q", "1", "-d"]
      )

    assert failed(bad) =~ ~r/not authori[sz]ed/i
    credentials = [extra: ["-u", "user", "-P", "synthetic-password"]]
    peer = subscriber(context.executables, broker, "allowed/#", credentials)
    Mosquitto.publish!(context.executables, broker, "allowed/value", "allowed", 1, credentials)
    assert completed(peer) =~ "allowed/value|1|0|allowed"

    denied =
      Mosquitto.process!(
        context.executables.publisher,
        Mosquitto.cli_args(broker, credentials) ++
          ["-t", "denied/value", "-m", "x", "-q", "1", "-d"]
      )

    # Mosquitto 2.1.2 reports a negative PUBACK as a warning and exits zero.
    output = completed(denied)
    assert output =~ "received PUBACK (Mid: 1, RC:135)"
    assert output =~ ~r/not authori[sz]ed/i

    scope =
      Mosquitto.process!(
        context.executables.subscriber,
        Mosquitto.cli_args(broker, credentials) ++ ["-t", "#", "-q", "1", "-W", "1", "-d"]
      )

    output = completed(scope)
    assert output =~ "received SUBACK"
    assert output =~ "Subscribed (mid: 1): 135"
  end

  test "callback timeout fails authentication without creating a session", context do
    broker = broker(auth: {Policy, [mode: :timeout]}, auth_timeout: 50)

    peer =
      Mosquitto.process!(
        context.executables.publisher,
        Mosquitto.cli_args(broker) ++ ["-t", "allowed/value", "-m", "x", "-q", "1", "-d"]
      )

    assert failed(peer) =~ "received CONNACK"
    assert {:ok, %{sessions: 0}} = GravitonMQ.status(broker.instance)
  end

  test "QoS2 subscription requests receive a real QoS1 grant", context do
    broker = broker(maximum_qos: 1)
    peer = subscriber(context.executables, broker, "maximum/#", qos: 2)
    publish(broker, "maximum/value", "bounded", qos: 1)
    output = completed(peer)
    assert output =~ "Subscribed (mid: 1): 1"
    assert output =~ "maximum/value|1|0|bounded"
  end

  test "independent QoS2 publisher and subscriber complete both handshakes through the broker",
       context do
    broker = broker(maximum_qos: 2)
    host = subscription(broker, "qos2/#", qos: 2)
    assert :ok = GravitonMQ.request(broker.instance, host, 1)
    peer = subscriber(context.executables, broker, "qos2/#", qos: 2)

    output =
      Mosquitto.command!(
        context.executables.publisher,
        Mosquitto.cli_args(broker) ++ ["-t", "qos2/value", "-m", "two-phase", "-q", "2", "-d"]
      )

    assert output =~ "received PUBREC"
    assert output =~ "sending PUBREL"
    assert output =~ "received PUBCOMP"
    assert completed(peer) =~ "qos2/value|2|0|two-phase"

    assert_receive {:graviton_mq, owner,
                    {:delivery, ^host, delivery, %{payload: "two-phase", qos: 2}}},
                   1_000

    assert owner == broker.owner
    assert :ok = GravitonMQ.ack(broker.instance, delivery)
    refute_receive {:graviton_mq, ^owner, {:delivery, ^host, _, _}}, 100
  end

  test "a persistent independent subscriber receives QoS1 queued while disconnected", context do
    broker = broker()
    options = [extra: ["-i", "persistent-peer", "-c", "-x", "10"]]
    peer = subscriber(context.executables, broker, "offline/#", options)
    force_disconnect(peer)

    Mosquitto.eventually!(fn ->
      match?({:ok, %{connections: 0, sessions: 1}}, GravitonMQ.status(broker.instance))
    end)

    Mosquitto.publish!(context.executables, broker, "offline/value", "queued", 1)

    resumed =
      Mosquitto.process!(
        context.executables.subscriber,
        Mosquitto.cli_args(broker, options) ++
          ["-t", "offline/#", "-q", "1", "-C", "1", "-W", "5", "-d", "-F", "%t|%q|%p"]
      )

    assert completed(resumed) =~ "offline/value|1|queued"
  end

  test "abnormal independent-client death publishes its delayed Will", context do
    broker = broker()
    subscription = subscription(broker, "will/#", qos: 1)
    assert :ok = GravitonMQ.request(broker.instance, subscription, 1)

    will = [
      "--will-topic",
      "will/value",
      "--will-payload",
      "gone",
      "--will-qos",
      "1",
      "-D",
      "CONNECT",
      "session-expiry-interval",
      "10",
      "-D",
      "WILL",
      "will-delay-interval",
      "1"
    ]

    peer = subscriber(context.executables, broker, "idle/none", extra: will)
    force_disconnect(peer)
    refute_receive {:graviton_mq, _, {:delivery, ^subscription, _, _}}, 100

    assert_receive {:graviton_mq, owner,
                    {:delivery, ^subscription, delivery,
                     %{topic: "will/value", payload: "gone", qos: 1}}},
                   3_000

    assert owner == broker.owner
    assert :ok = GravitonMQ.ack(broker.instance, delivery)
  end

  test "retained message expiry is observed by a later independent subscriber", context do
    broker = broker()

    Mosquitto.publish!(context.executables, broker, "expires/value", "short-lived", 1,
      extra: ["-r", "-D", "PUBLISH", "message-expiry-interval", "1"]
    )

    first = subscriber(context.executables, broker, "expires/#")
    assert completed(first) =~ "expires/value|1|1|short-lived"

    Mosquitto.eventually!(fn ->
      match?({:ok, %{retained: 0}}, GravitonMQ.status(broker.instance))
    end)

    late = subscriber(context.executables, broker, "expires/#", wait: 1)
    output = failed(late)
    refute output =~ "short-lived"
  end

  test "independent subscriber keep alive receives real PINGRESP", context do
    broker = broker()
    peer = subscriber(context.executables, broker, "heartbeat/#", extra: ["-k", "5"], wait: 10)

    Mosquitto.eventually!(
      fn -> Mosquitto.snapshot(peer).output =~ "received PINGRESP" end,
      fn -> Mosquitto.snapshot(peer) end,
      8_000
    )

    publish(broker, "heartbeat/value", "alive", qos: 1)
    assert completed(peer) =~ "heartbeat/value|1|0|alive"
  end
end
