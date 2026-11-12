Code.require_file("../../../graviton_mq_runtime/test/support/tls.exs", __DIR__)

defmodule GravitonMQ.AMQP10TLSTest.Auth do
  @moduledoc false
  @behaviour GravitonMQ.Runtime.AMQP10.Auth

  @impl true
  def authenticate(credentials, options) do
    send(Keyword.fetch!(options, :test), {:peer, credentials.peer})

    if credentials.username == "graviton-user" and credentials.password == "graviton-password",
      do: {:ok, {:user, "graviton-user"}},
      else: {:error, :denied}
  end
end

defmodule GravitonMQ.AMQP10TLSTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Codec.{Frame, Limits, Performative}
  alias GravitonMQ.AMQP10.Codec.SASL, as: SASLCodec
  alias GravitonMQ.AMQP10.Frame, as: Envelope
  alias GravitonMQ.AMQP10.Performative.Open
  alias GravitonMQ.AMQP10.SASL.{Init, Mechanisms, Outcome, Plain}
  alias GravitonMQ.AMQP10TLSTest.Auth
  alias GravitonMQ.Runtime.Test.TLS

  @compile {:no_warn_undefined, :ssl}
  @moduletag timeout: 30_000
  @amqp <<"AMQP", 0, 1, 0, 0>>
  @sasl <<"AMQP", 3, 1, 0, 0>>

  setup_all do
    %{certificates: TLS.certificates!()}
  end

  test "a verified TLS 1.3 client exchanges headers and Open; the server socket is TLS 1.3",
       %{certificates: certificates} do
    broker = broker!(certificates)
    socket = opened!(port(broker), certificates)
    assert {:ok, information} = SSL.connection_information(server_socket(connection!(broker)))
    assert information[:protocol] == :"tlsv1.3"
    assert :ok = :ssl.close(socket)
  end

  # Migrated from runtime/test/amqp10/connection_test.exs (spec §7.1).
  test "a silent TCP peer to a TLS listener is closed within connect_timeout and starts no Connection",
       %{certificates: certificates} do
    broker = broker!(certificates, connect_timeout: 300)
    {:ok, raw} = :gen_tcp.connect({127, 0, 0, 1}, port(broker), [:binary, active: false], 1_000)
    started = System.monotonic_time(:millisecond)
    assert {:error, :timeout} = :gen_tcp.recv(raw, 0, 150)
    assert %{active: 0} = DynamicSupervisor.count_children(connections(broker))
    assert :closed = closed(raw, started + 2_000)
    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed >= 250 and elapsed < 1_500, "closed after #{elapsed} ms"
    assert %{active: 0} = DynamicSupervisor.count_children(connections(broker))
  end

  # T2
  test "a TLS 1.2-only client is refused during the handshake and starts no Connection",
       %{certificates: certificates} do
    broker = broker!(certificates)

    assert {:error, {:tls_alert, {:protocol_version, _description}}} =
             TLS.connect(port(broker), certificates, versions: [:"tlsv1.2"])

    assert %{active: 0} = DynamicSupervisor.count_children(connections(broker))
  end

  # T3 through SASL PLAIN over TLS: the Auth callback sees peer/1 facts.
  test "optional client certificates give exact SASL peer facts and agree with ex_ssl",
       %{certificates: certificates} do
    broker =
      broker!(certificates, [auth: {Auth, [test: self()]}],
        cacertfile: certificates.ca,
        verify: :verify_peer,
        fail_if_no_peer_cert: false
      )

    port = port(broker)
    {:ok, anonymous} = TLS.connect(port, certificates)
    plain!(anonymous)
    assert_receive {:peer, %{certificate: nil, certificate_verified: false, transport: :tls}}
    assert {:ok, information} = SSL.connection_information(server_socket(connection!(broker)))
    assert information[:peer_authenticated] == false
    :ok = :ssl.close(anonymous)
    assert eventually(fn -> DynamicSupervisor.count_children(connections(broker)).active == 0 end)

    {:ok, identified} = TLS.connect(port, certificates, TLS.client_identity(certificates))
    plain!(identified)
    client_der = certificates.client_der
    assert_receive {:peer, %{certificate: ^client_der, certificate_verified: true}}
    assert {:ok, information} = SSL.connection_information(server_socket(connection!(broker)))
    assert information[:peer_authenticated] == :client

    case TLS.connect(port, certificates, TLS.stranger_identity(certificates)) do
      {:error, _reason} -> :ok
      {:ok, stranger} -> assert {:error, _reason} = :ssl.recv(stranger, 0, 2_000)
    end

    refute_receive {:peer, _peer}, 200
  end

  # T4: stalled mid-handshake (the silent case is the migrated test above)
  test "a peer stalled inside its ClientHello is closed within connect_timeout",
       %{certificates: certificates} do
    broker = broker!(certificates, connect_timeout: 300)
    {:ok, raw} = :gen_tcp.connect({127, 0, 0, 1}, port(broker), [:binary, active: false], 1_000)
    started = System.monotonic_time(:millisecond)
    :ok = :gen_tcp.send(raw, <<22, 3, 1, 0, 64, 1, 0, 0, 60>>)
    assert :closed = closed(raw, started + 2_000)
    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed >= 250 and elapsed < 1_500, "closed after #{elapsed} ms"
    assert %{active: 0} = DynamicSupervisor.count_children(connections(broker))
  end

  # Helpers (used by all AMQP TLS tests).

  defp broker!(certificates, options \\ [], tls \\ []) do
    config =
      Keyword.merge(
        [
          container_id: "tls-#{System.unique_integer([:positive])}",
          auth: :anonymous,
          queues: ["q"],
          endpoint_options: [max_frame_size: 512],
          listeners: [tls_listener(certificates, tls)]
        ],
        options
      )

    start_supervised!({GravitonMQ, name: nil, runtime_supervisor_name: nil, amqp10: config},
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

  defp port(broker) do
    assert {:ok, %{listeners: listeners}} = GravitonMQ.amqp10_status(broker)
    %{port: port} = Enum.find(listeners, &(&1.id == :tls))
    port
  end

  # Header exchange and the broker's Open, then the peer's Open.
  defp opened!(port, certificates, options \\ []) do
    assert {:ok, socket} = TLS.connect(port, certificates, options)
    on_exit(fn -> :ssl.close(socket) end)
    :ok = :ssl.send(socket, @amqp)
    assert {:ok, @amqp} = :ssl.recv(socket, 8, 2_000)
    assert %Open{} = read_pdu!(socket)
    :ok = :ssl.send(socket, wire(%Open{container_id: Value.string("tls-peer")}))
    socket
  end

  defp plain!(socket) do
    :ok = :ssl.send(socket, @sasl)
    assert {:ok, @sasl} = :ssl.recv(socket, 8, 2_000)
    assert %Mechanisms{} = read_sasl!(socket)

    {:ok, message} =
      Plain.encode(%Plain{username: "graviton-user", password: "graviton-password"})

    init = %Init{mechanism: Value.symbol("PLAIN"), initial_response: Value.binary(message)}
    {:ok, frame} = SASLCodec.encode_frame(init)
    :ok = :ssl.send(socket, frame)
    assert %Outcome{code: %Value{type: :ubyte, value: 0}} = read_sasl!(socket)
  end

  defp read_sasl!(socket) do
    {:ok, <<size::32, _::binary>> = header} = :ssl.recv(socket, 8, 2_000)
    {:ok, body} = :ssl.recv(socket, size - 8, 2_000)

    {:ok, %Envelope{type: :sasl} = frame, <<>>} =
      Frame.decode(header <> body, Limits.default(), types: [:sasl])

    {:ok, decoded} = SASLCodec.decode(frame.body)
    decoded
  end

  defp wire(pdu, channel \\ 0) do
    {:ok, body} = Performative.encode(pdu)

    {:ok, bytes} =
      Frame.encode(%Envelope{
        declared_size: 8 + byte_size(body),
        data_offset_words: 2,
        type: :amqp,
        channel: channel,
        extended_header: <<>>,
        body: body
      })

    bytes
  end

  defp read_pdu!(socket) do
    {:ok, <<size::32, _::binary>> = header} = :ssl.recv(socket, 8, 2_000)
    body = if size > 8, do: elem(:ssl.recv(socket, size - 8, 2_000), 1), else: <<>>
    {:ok, frame, <<>>} = Frame.decode(header <> body)
    {:ok, pdu, _payload} = Performative.decode(frame.body)
    pdu
  end

  defp amqp_supervisor(broker) do
    broker
    |> child(GravitonMQ.Runtime.Supervisor)
    |> child(GravitonMQ.Runtime.AMQP10.Supervisor)
  end

  defp connections(broker), do: child(amqp_supervisor(broker), :connections)

  defp connection!(broker) do
    supervisor = connections(broker)
    assert eventually(fn -> match?([_], DynamicSupervisor.which_children(supervisor)) end)
    [{_id, connection, _type, _modules}] = DynamicSupervisor.which_children(supervisor)
    connection
  end

  # The AMQP Connection keeps its state in a private ETS table; read it from
  # inside the Connection process.
  defp server_socket(connection) do
    test = self()
    reference = make_ref()

    :sys.replace_state(connection, fn shell ->
      send(test, {reference, :ets.lookup_element(shell.table, :state, 2).socket})
      shell
    end)

    receive do
      {^reference, socket} -> socket
    after
      1_000 -> flunk("no AMQP Connection state")
    end
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
