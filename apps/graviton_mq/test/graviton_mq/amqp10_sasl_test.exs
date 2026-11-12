defmodule GravitonMQ.AMQP10SASLTest.Auth do
  @moduledoc false
  @behaviour GravitonMQ.Runtime.AMQP10.Auth

  @impl true
  def authenticate(credentials, options) do
    test = Keyword.fetch!(options, :test)
    send(test, {:authenticating, self(), Map.delete(credentials, :password)})

    case Keyword.get(options, :mode, :check) do
      :check ->
        if credentials.username == "graviton-user" and
             credentials.password == "graviton-password",
           do: {:ok, {:user, "graviton-user"}},
           else: {:error, {:denied, credentials.password}}

      :sleep ->
        Process.sleep(:infinity)

      :raise ->
        raise ArgumentError, "leaked #{credentials.password}"

      :invalid ->
        {:weird, credentials.password}

      :anonymous ->
        {:ok, {:anonymous_peer, credentials.peer.port}}
    end
  end
end

defmodule GravitonMQ.AMQP10SASLTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Codec.{Frame, Limits, Performative}
  alias GravitonMQ.AMQP10.Codec.SASL, as: SASLCodec
  alias GravitonMQ.AMQP10.Frame, as: Envelope
  alias GravitonMQ.AMQP10.Performative.{Attach, Begin, Detach, Open}
  alias GravitonMQ.AMQP10.SASL.{Init, Mechanisms, Outcome, Plain}
  alias GravitonMQ.AMQP10.Terminus.{Source, Target}
  alias GravitonMQ.AMQP10SASLTest.Auth

  @moduletag timeout: 15_000
  @amqp <<"AMQP", 0, 1, 0, 0>>
  @sasl <<"AMQP", 3, 1, 0, 0>>

  test "a PLAIN principal reaches authorize/3 before any Link admission" do
    test = self()

    authorize = fn principal, action, address ->
      send(test, {:authorized, principal, action, address})
      :allow
    end

    broker = plain_broker!(authorize: authorize)
    socket = connect!(broker)

    assert %Outcome{code: %Value{value: 0}} =
             negotiate!(socket, plain_init("graviton-user", "graviton-password"))

    assert_receive {:authenticating, _task,
                    %{mechanism: :plain, username: "graviton-user"} = credentials}

    refute Map.has_key?(credentials, :password)

    assert %{
             peer: %{
               transport: :tcp,
               ip: {127, 0, 0, 1},
               certificate: nil,
               certificate_verified: false
             }
           } = credentials

    attach_sender!(socket)
    assert_receive {:authorized, {:user, "graviton-user"}, :send, "orders"}, 2_000
  end

  test "the default anonymous policy accepts ANONYMOUS with pipelined AMQP input" do
    broker = broker!([])
    socket = connect!(broker)
    :ok = :gen_tcp.send(socket, @sasl)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Mechanisms{} = mechanisms = read_sasl!(socket)
    assert Mechanisms.names(mechanisms) == ["ANONYMOUS"]

    :ok =
      :gen_tcp.send(
        socket,
        sasl_frame(%Init{mechanism: Value.symbol("ANONYMOUS")}) <>
          @amqp <> wire(%Open{container_id: Value.string("raw-peer")})
      )

    assert %Outcome{code: %Value{value: 0}} = read_sasl!(socket)
    assert {:ok, @amqp} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = read_pdu!(socket)
  end

  test "octet-at-a-time PLAIN negotiation reaches the AMQP layer" do
    socket = connect!(plain_broker!())
    send_octets!(socket, @sasl)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Mechanisms{} = read_sasl!(socket)
    send_octets!(socket, sasl_frame(plain_init("graviton-user", "graviton-password")))
    assert %Outcome{code: %Value{value: 0}} = read_sasl!(socket)
    send_octets!(socket, @amqp <> wire(%Open{container_id: Value.string("raw-peer")}))
    assert {:ok, @amqp} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = read_pdu!(socket)
  end

  test "wrong credentials are refused with auth, counted and never reach the AMQP layer" do
    broker = plain_broker!()
    socket = connect!(broker)

    assert %Outcome{code: %Value{value: 1}, additional_data: nil} =
             negotiate!(socket, plain_init("graviton-user", "wrong"))

    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)

    eventually!(fn ->
      match?(
        {:ok, %{sasl_failures: %{auth: 1}, authenticating: 0, links: 0}},
        GravitonMQ.amqp10_status(broker)
      )
    end)
  end

  test "callback exceptions and invalid returns map to sys, deadlines to sys-temp" do
    for {mode, code} <- [raise: 2, invalid: 2, sleep: 4] do
      broker = plain_broker!(auth_timeout: 50, auth_options: [mode: mode])
      socket = connect!(broker)

      assert %Outcome{code: %Value{value: ^code}} =
               negotiate!(socket, plain_init("graviton-user", "graviton-password"))

      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
    end
  end

  test "max_sasl_jobs bounds callbacks and transport loss kills a running callback" do
    broker =
      plain_broker!(
        max_sasl_jobs: 1,
        auth_timeout: 3_000,
        sasl_timeout: 10_000,
        auth_options: [mode: :sleep]
      )

    first = connect!(broker)
    :ok = :gen_tcp.send(first, @sasl)
    assert {:ok, @sasl} = :gen_tcp.recv(first, 8, 1_000)
    assert %Mechanisms{} = read_sasl!(first)
    :ok = :gen_tcp.send(first, sasl_frame(plain_init("graviton-user", "graviton-password")))
    assert_receive {:authenticating, task, _credentials}, 1_000
    eventually!(fn -> match?({:ok, %{authenticating: 1}}, GravitonMQ.amqp10_status(broker)) end)

    second = connect!(broker)

    assert %Outcome{code: %Value{value: 4}} =
             negotiate!(second, plain_init("graviton-user", "graviton-password"))

    monitor = Process.monitor(task)
    :ok = :gen_tcp.close(first)
    assert_receive {:DOWN, ^monitor, :process, ^task, :killed}, 2_000

    eventually!(fn ->
      match?({:ok, %{authenticating: 0, connections: 0}}, GravitonMQ.amqp10_status(broker))
    end)
  end

  test "a required policy refuses no-SASL peers and unoffered ANONYMOUS" do
    broker = plain_broker!()
    socket = connect!(broker)
    :ok = :gen_tcp.send(socket, @amqp)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)

    socket = connect!(broker)

    assert %Outcome{code: %Value{value: 1}} =
             negotiate!(socket, %Init{mechanism: Value.symbol("ANONYMOUS")})
  end

  test "a listed ANONYMOUS mechanism still invokes the callback with peer data" do
    broker =
      plain_broker!(sasl_mechanisms: ["PLAIN", "ANONYMOUS"], auth_options: [mode: :anonymous])

    socket = connect!(broker)

    assert %Outcome{code: %Value{value: 0}} =
             negotiate!(socket, %Init{
               mechanism: Value.symbol("ANONYMOUS"),
               initial_response: Value.binary("sirhc")
             })

    assert_receive {:authenticating, _task,
                    %{
                      mechanism: :anonymous,
                      trace: "sirhc",
                      username: nil,
                      peer: %{transport: :tcp, port: port}
                    }}

    assert is_integer(port)
  end

  test "credentials and policy options never appear in logs, status, published specs or crash reports" do
    password = "sentinel-password-#{System.unique_integer([:positive])}"
    secret = "policy-secret-#{System.unique_integer([:positive])}"

    log =
      capture_log(fn ->
        for mode <- [:check, :raise, :invalid] do
          broker = plain_broker!(auth_options: [mode: mode, secret: secret])
          socket = connect!(broker)
          assert %Outcome{} = negotiate!(socket, plain_init("graviton-user", password))
          assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
          assert {:ok, status} = GravitonMQ.amqp10_status(broker)
          refute inspect(status) =~ password
          amqp = amqp_supervisor(broker)

          for {_id, pid, _type, _modules} <- Supervisor.which_children(amqp), is_pid(pid) do
            rendered = inspect(:sys.get_status(pid))
            refute rendered =~ password
            refute rendered =~ secret
          end

          listeners =
            for {_, pid, _, _} <- Supervisor.which_children(child(amqp, :listeners)), do: pid

          for table <- :ets.all(), :ets.info(table, :owner) in listeners do
            refute inspect(:ets.tab2list(table)) =~ secret
          end
        end

        broker = plain_broker!(auth_timeout: 3_000, auth_options: [mode: :sleep, secret: secret])
        socket = connect!(broker)
        :ok = :gen_tcp.send(socket, @sasl)
        assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)
        assert %Mechanisms{} = read_sasl!(socket)
        :ok = :gen_tcp.send(socket, sasl_frame(plain_init("graviton-user", password)))
        assert_receive {:authenticating, _task, _credentials}, 1_000

        [{_, connection, _, _}] =
          DynamicSupervisor.which_children(child(amqp_supervisor(broker), :connections))

        refute inspect(:sys.get_status(connection)) =~ password
        # Mid-authentication: Connection state, status and published specs.
        refute_leak!(broker, connection, [password, secret])

        assert {:ok, %{authenticating: 1, sasl_failures: failures}} =
                 GravitonMQ.amqp10_status(broker)

        refute inspect(failures) =~ password
        monitor = Process.monitor(connection)

        :sys.replace_state(connection, fn shell ->
          data = :ets.lookup_element(shell.table, :state, 2)
          :ets.insert(shell.table, {:state, %{data | socket: {:broken, password}}})
          shell
        end)

        GravitonMQ.Runtime.AMQP10.Connection.close(connection, :test_finished)
        assert_receive {:DOWN, ^monitor, :process, ^connection, reason}, 2_000
        refute inspect(reason) =~ password
      end)

    refute log =~ password
    refute log =~ secret
  end

  test "a successful PLAIN negotiation keeps no credential in the Connection or published specs" do
    password = "success-password-#{System.unique_integer([:positive])}"
    secret = "success-secret-#{System.unique_integer([:positive])}"
    broker = plain_broker!(auth_options: [mode: :anonymous, secret: secret])
    socket = connect!(broker)

    assert %Outcome{code: %Value{value: 0}} =
             negotiate!(socket, plain_init("graviton-user", password))

    :ok = :gen_tcp.send(socket, @amqp <> wire(%Open{container_id: Value.string("raw-peer")}))
    assert {:ok, @amqp} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = read_pdu!(socket)

    [{_, connection, _, _}] =
      DynamicSupervisor.which_children(child(amqp_supervisor(broker), :connections))

    refute_leak!(broker, connection, [password, secret])

    assert {:ok, %{authenticating: 0, sasl_failures: %{auth: 0, sys: 0, sys_temp: 0}}} =
             GravitonMQ.amqp10_status(broker)
  end

  # Minor 6: the Broker's record default is a deny-only principal.
  test "Link admission without a reported principal is denied without authorize/3" do
    test = self()

    authorize = fn principal, action, address ->
      send(test, {:authorized, principal, action, address})
      :allow
    end

    broker = broker!(authorize: authorize)
    manager = child(amqp_supervisor(broker), GravitonMQ.Runtime.AMQP10.Broker)
    socket = connect!(broker)

    # Registered before any header: the default denies.
    eventually!(fn -> principals(manager) != [] end)
    assert [default] = principals(manager)
    assert default == :unauthenticated

    :ok = :gen_tcp.send(socket, @amqp <> wire(%Open{container_id: Value.string("raw-peer")}))
    assert {:ok, @amqp} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = read_pdu!(socket)
    eventually!(fn -> principals(manager) == [:anonymous] end)

    # Put the record back to its default, as if no principal had arrived.
    :sys.replace_state(manager, fn shell ->
      state = :ets.lookup_element(shell.table, :state, 2)

      connections =
        Map.new(state.connections, fn {pid, record} -> {pid, %{record | principal: default}} end)

      :ets.insert(shell.table, {:state, %{state | connections: connections}})
      shell
    end)

    :ok =
      :gen_tcp.send(
        socket,
        wire(
          %Begin{
            next_outgoing_id: Value.uint(0),
            incoming_window: Value.uint(32),
            outgoing_window: Value.uint(32)
          },
          1
        ) <> wire(sender_attach(), 1)
      )

    assert %Begin{} = read_pdu!(socket)
    assert %Attach{target: nil} = read_pdu!(socket)

    assert %Detach{error: %{condition: %Value{value: "amqp:unauthorized-access"}}} =
             read_pdu!(socket)

    refute_received {:authorized, _principal, _action, _address}
  end

  defp principals(manager) do
    caller = self()
    reference = make_ref()

    :sys.replace_state(manager, fn shell ->
      state = :ets.lookup_element(shell.table, :state, 2)
      send(caller, {reference, Enum.map(state.connections, fn {_, r} -> r.principal end)})
      shell
    end)

    assert_receive {^reference, principals}, 1_000
    principals
  end

  defp sender_attach do
    %Attach{
      name: Value.string("sasl-sender"),
      handle: Value.uint(0),
      role: Value.boolean(false),
      snd_settle_mode: Value.ubyte(0),
      rcv_settle_mode: Value.ubyte(0),
      source: %Source{address: Value.string("raw")},
      target: %Target{address: Value.string("orders")},
      initial_delivery_count: Value.uint(0)
    }
  end

  defp broker!(options) do
    config =
      Keyword.merge(
        [
          container_id: "sasl-#{System.unique_integer([:positive])}",
          auth: :anonymous,
          queues: ["orders"],
          endpoint_options: [max_frame_size: 512],
          listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
        ],
        options
      )

    start_supervised!({GravitonMQ, name: nil, runtime_supervisor_name: nil, amqp10: config},
      id: make_ref()
    )
  end

  defp plain_broker!(options \\ []) do
    {auth_options, options} = Keyword.pop(options, :auth_options, [])

    broker!(
      Keyword.merge(
        [auth: {Auth, [test: self()] ++ auth_options}, allow_insecure_plain: true],
        options
      )
    )
  end

  defp connect!(broker) do
    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp negotiate!(socket, init) do
    :ok = :gen_tcp.send(socket, @sasl)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Mechanisms{} = read_sasl!(socket)
    :ok = :gen_tcp.send(socket, sasl_frame(init))
    read_sasl!(socket)
  end

  defp attach_sender!(socket) do
    :ok = :gen_tcp.send(socket, @amqp <> wire(%Open{container_id: Value.string("raw-peer")}))
    assert {:ok, @amqp} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = read_pdu!(socket)

    :ok =
      :gen_tcp.send(
        socket,
        wire(
          %Begin{
            next_outgoing_id: Value.uint(0),
            incoming_window: Value.uint(32),
            outgoing_window: Value.uint(32)
          },
          1
        )
      )

    assert %Begin{} = read_pdu!(socket)

    :ok = :gen_tcp.send(socket, wire(sender_attach(), 1))
  end

  # The Connection keeps its state in a private ETS table; read it in-process.
  defp refute_leak!(broker, connection, values) do
    caller = self()
    reference = make_ref()

    :sys.replace_state(connection, fn shell ->
      send(caller, {reference, :ets.lookup_element(shell.table, :state, 2)})
      shell
    end)

    assert_receive {^reference, state}, 1_000
    printed = inspect(state, limit: :infinity, printable_limit: :infinity)
    encoded = :erlang.term_to_binary(state)
    status = inspect(:sys.get_status(connection), limit: :infinity)

    listeners =
      for {_, pid, _, _} <- Supervisor.which_children(child(amqp_supervisor(broker), :listeners)),
          do: pid

    published =
      for table <- :ets.all(), :ets.info(table, :owner) in listeners, do: :ets.tab2list(table)

    # The published SASL map carries settings and the Authenticator pid only.
    assert Map.keys(state.config.sasl) |> Enum.sort() ==
             Enum.sort([
               :allow_insecure_plain,
               :auth_timeout,
               :authenticate,
               :authenticator,
               :client_certificate_verified,
               :max_jobs,
               :mechanisms,
               :mode,
               :timeout
             ])

    refute Map.has_key?(state.config, :auth)

    for value <- values do
      refute printed =~ value
      refute :binary.match(encoded, value) != :nomatch
      refute status =~ value
      refute :binary.match(:erlang.term_to_binary(published), value) != :nomatch
    end
  end

  defp send_octets!(socket, bytes) do
    for <<octet <- bytes>>, do: :ok = :gen_tcp.send(socket, <<octet>>)
    :ok
  end

  defp amqp_supervisor(broker),
    do:
      broker
      |> child(GravitonMQ.Runtime.Supervisor)
      |> child(GravitonMQ.Runtime.AMQP10.Supervisor)

  defp child(supervisor, identity) do
    {_, pid, _, _} = List.keyfind(Supervisor.which_children(supervisor), identity, 0)
    assert is_pid(pid)
    pid
  end

  defp eventually!(predicate), do: until!(predicate, System.monotonic_time(:millisecond) + 2_000)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline, "condition timed out"
      Process.sleep(10)
      until!(predicate, deadline)
    end
  end

  defp plain_init(username, password) do
    {:ok, message} = Plain.encode(%Plain{username: username, password: password})

    %Init{
      mechanism: Value.symbol("PLAIN"),
      initial_response: Value.binary(message)
    }
  end

  defp sasl_frame(body) do
    {:ok, bytes} = SASLCodec.encode_frame(body)
    bytes
  end

  defp read_sasl!(socket) do
    {:ok, <<size::32, _::binary>> = header} = :gen_tcp.recv(socket, 8, 1_000)
    {:ok, body} = :gen_tcp.recv(socket, size - 8, 1_000)

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
    {:ok, <<size::32, _::binary>> = header} = :gen_tcp.recv(socket, 8, 1_000)
    body = if size > 8, do: elem(:gen_tcp.recv(socket, size - 8, 1_000), 1), else: <<>>
    {:ok, frame, <<>>} = Frame.decode(header <> body)
    {:ok, pdu, _payload} = Performative.decode(frame.body)
    pdu
  end
end
