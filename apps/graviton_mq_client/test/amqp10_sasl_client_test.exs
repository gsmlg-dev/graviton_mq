defmodule GravitonMQ.AMQP10Client.SASLTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Codec.{Frame, Limits, Performative}
  alias GravitonMQ.AMQP10.Codec.SASL, as: SASLCodec
  alias GravitonMQ.AMQP10.Frame, as: Envelope
  alias GravitonMQ.AMQP10.Performative.Open
  alias GravitonMQ.AMQP10.SASL.{Challenge, Init, Mechanisms, Outcome, Plain}
  alias GravitonMQ.Client.AMQP10.Config

  @amqp <<"AMQP", 0, 1, 0, 0>>
  @sasl <<"AMQP", 3, 1, 0, 0>>

  test "SASL configuration is validated without echoing values" do
    long = String.duplicate("a", 255)

    for options <- [
          [sasl: :scram],
          [sasl: {:plain, "u", "p"}],
          [sasl: {:plain, "", "p"}, allow_insecure_plain: true],
          [sasl: {:plain, "u", "p\0"}, allow_insecure_plain: true],
          [sasl: {:plain, "u", "p", authzid: <<255>>}, allow_insecure_plain: true],
          [sasl: {:plain, "u", "p", other: 1}, allow_insecure_plain: true],
          [sasl: {:plain, long, long, authzid: long}, allow_insecure_plain: true],
          [sasl: {:anonymous, <<0xFF>>}],
          [sasl: :anonymous, sasl_timeout: 0],
          [sasl: :anonymous, sasl_timeout: 10_001],
          [allow_insecure_plain: :yes]
        ] do
      assert {:error, :invalid_options} =
               Config.build(Keyword.merge([container_id: "c", observer: self()], options))
    end

    assert {:ok, %{sasl: :none, sasl_timeout: 5_000}} =
             Config.build(container_id: "c", observer: self())

    assert {:ok, %{sasl: {:anonymous, nil}}} =
             Config.build(container_id: "c", observer: self(), sasl: :anonymous)

    assert {:ok, %{sasl: {:anonymous, "trace"}}} =
             Config.build(container_id: "c", observer: self(), sasl: {:anonymous, "trace"})

    assert {:ok, %{sasl: {:plain, "u", "p", "z"}}} =
             Config.build(
               container_id: "c",
               observer: self(),
               transport: :tls,
               sasl: {:plain, "u", "p", authzid: "z"}
             )

    provider = fn -> "p" end

    assert {:ok, %{sasl: {:plain, "u", ^provider, nil}}} =
             Config.build(
               container_id: "c",
               observer: self(),
               sasl: {:plain, "u", provider},
               allow_insecure_plain: true
             )
  end

  test "PLAIN negotiates first and accepts outcome, header and Open in one segment" do
    {listener, port} = server!()

    client =
      client!(port,
        sasl: {:plain, "user", "secret", authzid: "admin"},
        allow_insecure_plain: true,
        hostname: "broker.example"
      )

    socket = accept!(listener)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)
    assert {:error, _reason} = Client.open_session(client, 1)

    offered = Value.array(:symbol, [Value.symbol("PLAIN"), Value.symbol("ANONYMOUS")])
    :ok = :gen_tcp.send(socket, @sasl <> sasl_frame(%Mechanisms{server_mechanisms: offered}))

    assert %Init{
             mechanism: %Value{type: :symbol, value: "PLAIN"},
             initial_response: %Value{type: :binary, value: response},
             hostname: %Value{type: :string, value: "broker.example"}
           } = read_sasl!(socket)

    assert {:ok, %Plain{authzid: "admin", username: "user", password: "secret"}} =
             Plain.parse(response)

    refute_receive {:graviton_mq, ^client, {:connected, _}}, 20

    :ok =
      :gen_tcp.send(socket, sasl_frame(%Outcome{code: Value.ubyte(0)}) <> @amqp <> frame(open()))

    assert {:ok, @amqp} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = recv_pdu!(socket)
    assert_receive {:graviton_mq, ^client, {:connected, "peer"}}, 1_000
    assert Client.status(client).phase == :active
  end

  test "octet-at-a-time server negotiation input yields the same connection" do
    {listener, port} = server!()
    client = client!(port, sasl: :anonymous)
    socket = accept!(listener)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)

    send_octets!(
      socket,
      @sasl <> sasl_frame(%Mechanisms{server_mechanisms: Value.symbol("ANONYMOUS")})
    )

    assert %Init{mechanism: %Value{value: "ANONYMOUS"}, initial_response: nil} =
             read_sasl!(socket)

    send_octets!(socket, sasl_frame(%Outcome{code: Value.ubyte(0)}) <> @amqp <> frame(open()))
    assert {:ok, @amqp} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = recv_pdu!(socket)
    assert_receive {:graviton_mq, ^client, {:connected, "peer"}}, 1_000
  end

  test "failure outcomes disconnect with exact reasons and never connect" do
    for {code, reason} <- [
          {1, :auth},
          {2, :sys},
          {3, :sys_perm},
          {4, :sys_temp},
          {9, {:unknown, 9}}
        ] do
      {client, socket} = awaiting_outcome!()
      :ok = :gen_tcp.send(socket, sasl_frame(%Outcome{code: Value.ubyte(code)}))
      assert_receive {:graviton_mq, ^client, {:disconnected, {:sasl, ^reason}}}, 1_000
      refute_received {:graviton_mq, ^client, {:connected, _}}
      assert %{phase: :closed, pending: 0} = Client.status(client)
      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
    end
  end

  # The stream takes only the octets a frame needs, so a decoded outcome never
  # leaves buffered bytes and the header switch cannot fail from socket input.
  # The failure is forced here to pin the defensive path: it closes with the
  # decode reason, sends no AMQP header and never starts the Endpoint.
  test "a failed switch to the AMQP header closes without starting the Endpoint" do
    {client, socket} = awaiting_outcome!()
    outcome = sasl_frame(%Outcome{code: Value.ubyte(0)})

    :sys.replace_state(client, fn state ->
      %{state | stream: %{state.stream | buffer: outcome <> "AM"}}
    end)

    :ok = :gen_tcp.send(socket, "Q")

    assert_receive {:graviton_mq, ^client, {:disconnected, {:decode_error, :invalid_stream}}},
                   1_000

    refute_received {:graviton_mq, ^client, {:connected, _}}
    assert %{phase: :closed} = Client.status(client)
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "no SASL offer, an unoffered mechanism, a challenge and silence close explicitly" do
    {listener, port} = server!()
    client = client!(port, sasl: :anonymous)
    socket = accept!(listener)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)
    :ok = :gen_tcp.send(socket, @amqp)
    assert_receive {:graviton_mq, ^client, {:disconnected, :sasl_not_offered}}, 1_000

    {listener, port} = server!()
    client = client!(port, sasl: {:plain, "u", "p"}, allow_insecure_plain: true)
    socket = accept!(listener)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)

    :ok =
      :gen_tcp.send(
        socket,
        @sasl <> sasl_frame(%Mechanisms{server_mechanisms: Value.symbol("ANONYMOUS")})
      )

    assert_receive {:graviton_mq, ^client, {:disconnected, :sasl_mechanism_not_offered}}, 1_000
    # No sasl-init was sent before the close.
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)

    {client, socket} = awaiting_outcome!()
    :ok = :gen_tcp.send(socket, sasl_frame(%Challenge{challenge: Value.binary(<<>>)}))
    assert_receive {:graviton_mq, ^client, {:disconnected, :sasl_protocol_error}}, 1_000

    {listener, port} = server!()
    client = client!(port, sasl: :anonymous, sasl_timeout: 50)
    _socket = accept!(listener)
    assert_receive {:graviton_mq, ^client, {:disconnected, :sasl_timeout}}, 1_000
  end

  test "sasl: :none keeps the raw AMQP header" do
    {listener, port} = server!()
    _client = client!(port, [])
    socket = accept!(listener)
    assert {:ok, @amqp} = :gen_tcp.recv(socket, 8, 1_000)
  end

  test "a password provider is evaluated at connect and passwords never leak" do
    password = "sentinel-client-password-#{System.unique_integer([:positive])}"
    provider = fn -> password end

    spec =
      Client.child_spec(
        container_id: "c",
        sasl: {:plain, "user", password},
        allow_insecure_plain: true
      )

    refute inspect(spec) =~ password

    {listener, port} = server!()
    client = client!(port, sasl: {:plain, "user", provider}, allow_insecure_plain: true)
    socket = accept!(listener)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)
    refute inspect(Client.status(client)) =~ password
    refute inspect(:sys.get_status(client)) =~ password

    :ok =
      :gen_tcp.send(
        socket,
        @sasl <> sasl_frame(%Mechanisms{server_mechanisms: Value.symbol("PLAIN")})
      )

    assert %Init{initial_response: %Value{value: response}} = init = read_sasl!(socket)
    assert {:ok, %Plain{password: ^password}} = Plain.parse(response)
    refute inspect(init) =~ password
    :ok = :gen_tcp.send(socket, sasl_frame(%Outcome{code: Value.ubyte(1)}))
    assert_receive {:graviton_mq, ^client, {:disconnected, {:sasl, :auth}}} = event, 1_000
    refute inspect(event) =~ password
    refute inspect(:sys.get_status(client)) =~ password

    {listener, port} = server!()

    client =
      client!(port,
        sasl: {:plain, "user", fn -> String.duplicate("x", 600) end},
        allow_insecure_plain: true
      )

    _socket = accept!(listener)
    assert_receive {:graviton_mq, ^client, {:disconnected, :invalid_sasl_credentials}}, 1_000
  end

  # Owner Q11: the provider is retained (for a later re-authentication), but
  # it runs once per connect and its result does not outlive sasl-init.
  test "a provider runs once per connect and its password does not outlive sasl-init" do
    test = self()
    suffix = Integer.to_string(System.unique_integer([:positive]))
    password = "provided-password-" <> suffix

    # The closure captures only the suffix, so the whole password can exist
    # in client state only if the shell or the machine retained the result.
    provider = fn ->
      send(test, :provided)
      "provided-password-" <> suffix
    end

    refute_received :provided
    {listener, port} = server!()
    client = client!(port, sasl: {:plain, "user", provider}, allow_insecure_plain: true)
    socket = accept!(listener)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)
    assert_receive :provided, 1_000

    :ok =
      :gen_tcp.send(
        socket,
        @sasl <> sasl_frame(%Mechanisms{server_mechanisms: Value.symbol("PLAIN")})
      )

    assert %Init{initial_response: %Value{value: response}} = read_sasl!(socket)
    assert {:ok, %Plain{password: ^password}} = Plain.parse(response)
    refute_retained!(client, password)

    :ok =
      :gen_tcp.send(socket, sasl_frame(%Outcome{code: Value.ubyte(0)}) <> @amqp <> frame(open()))

    assert {:ok, @amqp} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = recv_pdu!(socket)
    assert_receive {:graviton_mq, ^client, {:connected, "peer"}}, 1_000
    refute_retained!(client, password)
    refute_received :provided
  end

  test "status, a sensitive mailbox, crash reports and termination reasons hide the password" do
    password = "crash-password-#{System.unique_integer([:positive])}"
    {listener, port} = server!()

    client =
      start_supervised!(
        Supervisor.child_spec(
          {Client,
           container_id: "sasl-client",
           host: "127.0.0.1",
           port: port,
           sasl: {:plain, "user", password},
           allow_insecure_plain: true},
          id: make_ref(),
          restart: :temporary
        )
      )

    socket = accept!(listener)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)

    # The configured binary password is retained (owner Q11), redacted.
    refute inspect(:sys.get_status(client), limit: :infinity) =~ password
    refute inspect(Client.status(client)) =~ password

    # A sensitive process hides its mailbox, dictionary and backtrace.
    :ok = :sys.suspend(client)
    send(client, {:probe, password})

    for item <- [:messages, :dictionary, :backtrace] do
      refute inspect(Process.info(client, item), limit: :infinity) =~ password
    end

    assert {:messages, []} = Process.info(client, :messages)
    :ok = :sys.resume(client)
    monitor = Process.monitor(client)

    log =
      capture_log(fn ->
        :sys.terminate(client, {:crash_test, :reason})
        assert_receive {:DOWN, ^monitor, :process, ^client, reason}, 1_000
        refute inspect(reason) =~ password
      end)

    assert log =~ "State: :redacted"
    refute log =~ password
  end

  test "a failing or invalid provider disconnects without echoing the credential" do
    password = "provider-password-#{System.unique_integer([:positive])}"

    log =
      capture_log(fn ->
        for provider <- [fn -> raise "leaked #{password}" end, fn -> {:bad, password} end] do
          {listener, port} = server!()
          client = client!(port, sasl: {:plain, "user", provider}, allow_insecure_plain: true)
          _socket = accept!(listener)

          assert_receive {:graviton_mq, ^client, {:disconnected, :invalid_sasl_credentials}},
                         1_000

          refute inspect(:sys.get_status(client)) =~ password
        end
      end)

    refute log =~ password
  end

  # Printed (unbounded) and external-format state, plus status and get_status.
  defp refute_retained!(client, secret) do
    state = :sys.get_state(client)
    refute inspect(state, limit: :infinity, printable_limit: :infinity) =~ secret
    refute :binary.match(:erlang.term_to_binary(state), secret) != :nomatch
    refute inspect(Client.status(client)) =~ secret
    refute inspect(:sys.get_status(client), limit: :infinity) =~ secret
  end

  defp server! do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    {listener, port}
  end

  defp client!(port, options) do
    options = Keyword.merge([container_id: "sasl-client", host: "127.0.0.1", port: port], options)
    start_supervised!(Supervisor.child_spec({Client, options}, id: make_ref()))
  end

  defp accept!(listener) do
    {:ok, socket} = :gen_tcp.accept(listener, 1_000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp awaiting_outcome! do
    {listener, port} = server!()
    client = client!(port, sasl: :anonymous)
    socket = accept!(listener)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)

    :ok =
      :gen_tcp.send(
        socket,
        @sasl <> sasl_frame(%Mechanisms{server_mechanisms: Value.symbol("ANONYMOUS")})
      )

    assert %Init{} = read_sasl!(socket)
    {client, socket}
  end

  defp send_octets!(socket, bytes) do
    for <<octet <- bytes>>, do: :ok = :gen_tcp.send(socket, <<octet>>)
    :ok
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

  defp open, do: %Open{container_id: Value.string("peer"), max_frame_size: Value.uint(512)}

  defp frame(pdu) do
    {:ok, bytes} = Performative.encode(pdu)
    <<8 + byte_size(bytes)::32, 2, 0, 0::16, bytes::binary>>
  end

  defp recv_pdu!(socket) do
    {:ok, <<size::32>>} = :gen_tcp.recv(socket, 4, 1_000)
    {:ok, rest} = :gen_tcp.recv(socket, size - 4, 1_000)
    {:ok, frame, <<>>} = Frame.decode(<<size::32, rest::binary>>)
    {:ok, pdu, <<>>} = Performative.decode(frame.body)
    pdu
  end
end
