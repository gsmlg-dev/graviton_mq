defmodule GravitonMQ.AMQP10.SASL.MachineTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.{Frame, Value}
  alias GravitonMQ.AMQP10.Codec
  alias GravitonMQ.AMQP10.Codec.{Error, Limits, Performative, ProtocolHeader}
  alias GravitonMQ.AMQP10.Codec.Frame, as: FrameCodec
  alias GravitonMQ.AMQP10.Codec.SASL, as: SASLCodec
  alias GravitonMQ.AMQP10.Performative.Open
  alias GravitonMQ.AMQP10.SASL.{Init, Machine, Mechanisms, Outcome, Plain, Response}

  @sasl <<"AMQP", 3, 1, 0, 0>>
  @amqp <<"AMQP", 0, 1, 0, 0>>

  describe "server" do
    test "a SASL header is answered with the SASL header and the configured mechanisms" do
      machine = server!(mechanisms: ["PLAIN", "ANONYMOUS"], authenticate: true)
      {machine, effects} = step(machine, {:header, header(3)})
      assert [<<@sasl::binary, frame::binary>>] = sent(effects)
      assert %Mechanisms{} = mechanisms = decode_sasl(frame)
      assert Mechanisms.names(mechanisms) == ["PLAIN", "ANONYMOUS"]
      assert machine.phase == :await_init

      {_machine, effects} = step(server!(), {:header, header(3)})
      assert [<<@sasl::binary, frame::binary>>] = sent(effects)

      assert %Mechanisms{server_mechanisms: %Value{type: :symbol, value: "ANONYMOUS"}} =
               decode_sasl(frame)
    end

    test "optional mode hands a no-SASL header to the AMQP layer" do
      {machine, effects} = step(server!(), {:header, header(0)})

      assert [
               {:cancel_wake, 0, :sasl_negotiation, 0},
               {:established, :no_sasl},
               {:start_endpoint, %ProtocolHeader{protocol_id: 0}}
             ] = effects

      refute Machine.active?(machine)
    end

    test "header refusals flush the reply header before closing" do
      for {options, header, reply, reason} <- [
            {[mode: :required], header(0), @sasl, :sasl_required},
            {[mode: :disabled], header(3), @amqp, :sasl_not_supported}
          ] do
        {machine, effects} = step(server!(options), {:header, header})
        assert [{:send_bytes, 0, id, ^reply}] = effects

        assert {_machine, []} =
                 step(machine, {:frame, sasl_frame(%Response{response: Value.binary("x")})})

        {machine, effects} = step(machine, {:write_result, id, :ok})
        assert [{:cancel_wake, 0, :sasl_negotiation, 0}, {:close, ^reason}] = effects
        refute Machine.active?(machine)
      end

      error = Error.new(:protocol_header, :malformed, :invalid_protocol_magic)

      for {mode, reply} <- [optional: @amqp, required: @sasl, disabled: @amqp] do
        {machine, [{:send_bytes, 0, id, ^reply}]} =
          step(server!(mode: mode), {:decode_error, error})

        assert {_, [_, {:close, :unsupported_protocol_header}]} =
                 step(machine, {:write_result, id, {:error, :closed}})
      end
    end

    test "ANONYMOUS without a callback succeeds and waits for the AMQP header" do
      machine = sasl_ready!(server!())
      {machine, effects} = step(machine, {:frame, sasl_frame(anonymous_init("trace"))})
      assert [{:expect_header, [0]}, {:send_bytes, 0, _id, outcome} | rest] = effects
      assert %Outcome{code: %Value{type: :ubyte, value: 0}} = decode_sasl(outcome)
      assert rest == [{:outcome, :ok}, {:established, {:sasl, :anonymous}}]
      assert machine.phase == :await_amqp_header

      {machine, effects} = step(machine, {:header, header(0)})

      assert [{:cancel_wake, 0, :sasl_negotiation, 0}, {:start_endpoint, %ProtocolHeader{}}] =
               effects

      refute Machine.active?(machine)
    end

    test "PLAIN moves credentials into one authenticate effect and never stores them" do
      machine = sasl_ready!(server!(mechanisms: ["PLAIN"], authenticate: true))
      init = plain_init("graviton-user", "sentinel-password", "broker.example")
      {machine, effects} = step(machine, {:frame, sasl_frame(init)})

      assert [
               {:expect_header, [0]},
               {:authenticate, {:sasl_auth, 0} = ref,
                %{
                  mechanism: :plain,
                  username: "graviton-user",
                  password: "sentinel-password",
                  authzid: nil,
                  trace: nil,
                  hostname: "broker.example"
                }}
             ] = effects

      refute_retained(machine, ["sentinel-password", "graviton-user"])
      {machine, effects} = step(machine, {:auth_result, ref, :ok})

      assert [{:send_bytes, 0, _id, outcome}, {:outcome, :ok}, {:established, {:sasl, :plain}}] =
               effects

      assert %Outcome{code: %Value{value: 0}} = decode_sasl(outcome)
      assert machine.phase == :await_amqp_header
      refute_retained(machine, ["sentinel-password", "graviton-user"])

      {machine, _effects} = step(machine, {:header, header(0)})
      refute_retained(machine, ["sentinel-password", "graviton-user"])
    end

    test "credentials are not retained after a failure, a cancel or a pipelined header" do
      ready = sasl_ready!(server!(mechanisms: ["PLAIN"], authenticate: true))
      init = sasl_frame(plain_init("graviton-user", "sentinel-password"))
      {authenticating, [_, {:authenticate, ref, _}]} = step(ready, {:frame, init})

      for event <- [
            {:auth_result, ref, {:error, :auth}},
            {:transport_down, :closed},
            {:timeout, :sasl_negotiation, 0},
            {:header, header(0)}
          ] do
        {machine, _effects} = step(authenticating, event)
        refute_retained(machine, ["sentinel-password", "graviton-user"])
      end
    end

    test "every callback failure code becomes a flushed outcome then a close" do
      for code <- [:auth, :sys, :sys_perm, :sys_temp] do
        machine = sasl_ready!(server!(mechanisms: ["PLAIN"], authenticate: true))

        {machine, [_, {:authenticate, ref, _}]} =
          step(machine, {:frame, sasl_frame(plain_init("u", "p"))})

        {machine, effects} = step(machine, {:auth_result, ref, {:error, code}})
        assert [{:outcome, ^code}, {:send_bytes, 0, id, outcome}] = effects
        number = GravitonMQ.AMQP10.SASL.code(code)
        assert %Outcome{code: %Value{type: :ubyte, value: ^number}} = decode_sasl(outcome)
        {_machine, effects} = step(machine, {:write_result, id, :ok})
        assert List.last(effects) == {:close, {:sasl, code}}
      end
    end

    test "unoffered mechanisms and invalid messages are refused with auth and no callback" do
      for {options, init} <- [
            {[], plain_init("u", "p")},
            {[mechanisms: ["PLAIN"], authenticate: true], anonymous_init(nil)},
            {[mechanisms: ["PLAIN"], authenticate: true],
             %Init{mechanism: Value.symbol("PLAIN"), initial_response: Value.binary("no-nuls")}},
            {[mechanisms: ["PLAIN"], authenticate: true],
             %Init{mechanism: Value.symbol("PLAIN")}},
            {[], anonymous_init(<<0xFF>>)},
            {[], %Init{mechanism: Value.symbol("SCRAM-SHA-256")}}
          ] do
        machine = sasl_ready!(server!(options))
        {_machine, effects} = step(machine, {:frame, sasl_frame(init)})
        assert [{:outcome, :auth}, {:send_bytes, 0, _id, outcome}] = effects
        assert %Outcome{code: %Value{value: 1}} = decode_sasl(outcome)
      end
    end

    test "ANONYMOUS under a callback policy is still authenticated with its trace" do
      machine = sasl_ready!(server!(mechanisms: ["PLAIN", "ANONYMOUS"], authenticate: true))
      {_machine, effects} = step(machine, {:frame, sasl_frame(anonymous_init("sirhc"))})

      assert [
               {:expect_header, [0]},
               {:authenticate, _ref, %{mechanism: :anonymous, trace: "sirhc", password: nil}}
             ] = effects
    end

    test "protocol violations close without an outcome" do
      machine = sasl_ready!(server!())

      assert {_, [{:cancel_wake, _, _, _}, {:close, :sasl_protocol_error}]} =
               step(machine, {:frame, sasl_frame(%Response{response: Value.binary("x")})})

      assert {_, [{:cancel_wake, _, _, _}, {:close, {:decode_error, :empty_sasl_frame}}]} =
               step(
                 machine,
                 {:frame, %{sasl_frame(%Response{response: Value.binary("x")}) | body: <<>>}}
               )

      assert {_, [{:cancel_wake, _, _, _}, {:close, {:decode_error, :frame_size_limit}}]} =
               step(
                 machine,
                 {:decode_error, Error.new(:frame_decode, :limit_exceeded, :frame_size_limit)}
               )

      plain = sasl_ready!(server!(mechanisms: ["PLAIN"], authenticate: true))

      {plain, [_, {:authenticate, ref, _}]} =
        step(plain, {:frame, sasl_frame(plain_init("u", "p"))})

      second_init = Error.new(:protocol_header, :malformed, :invalid_protocol_magic)

      assert {_,
              [
                {:cancel_authentication, ^ref},
                {:cancel_wake, _, _, _},
                {:close, :sasl_protocol_error}
              ]} =
               step(plain, {:decode_error, second_init})
    end

    test "a pipelined AMQP header is retained until the outcome" do
      machine = sasl_ready!(server!(mechanisms: ["PLAIN"], authenticate: true))

      {machine, [_, {:authenticate, ref, _}]} =
        step(machine, {:frame, sasl_frame(plain_init("u", "p"))})

      assert {machine, []} = step(machine, {:header, header(0)})
      {machine, effects} = step(machine, {:auth_result, ref, :ok})
      assert {:start_endpoint, %ProtocolHeader{protocol_id: 0}} = List.last(effects)
      refute Machine.active?(machine)
    end

    test "deadline, transport loss, stale results, generations and clocks are fenced" do
      machine = server!()
      assert {_, [{:close, :sasl_timeout}]} = step(machine, {:timeout, :sasl_negotiation, 0})
      assert {^machine, []} = step(machine, {:timeout, :sasl_negotiation, 99})

      plain = sasl_ready!(server!(mechanisms: ["PLAIN"], authenticate: true))

      {plain, [_, {:authenticate, ref, _}]} =
        step(plain, {:frame, sasl_frame(plain_init("u", "p"))})

      assert {^plain, []} = step(plain, {:auth_result, {:sasl_auth, 42}, :ok})

      assert {_, [{:cancel_authentication, ^ref}, {:cancel_wake, _, _, _}, {:close, :closed}]} =
               step(plain, {:transport_down, :closed})

      assert {^plain, []} =
               Machine.step(plain, %{generation: 1, now_ms: 5, event: {:auth_result, ref, :ok}})

      assert {^plain, []} =
               Machine.step(plain, %{generation: 0, now_ms: -1, event: {:auth_result, ref, :ok}})
    end

    test "a callback policy requires SASL, so a no-SASL header never reaches the Endpoint" do
      machine = server!(mechanisms: ["PLAIN"], authenticate: true)
      {machine, [{:send_bytes, 0, id, @sasl}]} = step(machine, {:header, header(0)})
      {_machine, effects} = step(machine, {:write_result, id, :ok})
      assert List.last(effects) == {:close, :sasl_required}
      refute Enum.any?(effects, &match?({:start_endpoint, _}, &1))
    end

    test "input is paused while a pipelined AMQP header is held" do
      machine = sasl_ready!(server!(mechanisms: ["PLAIN"], authenticate: true))
      refute Machine.input_paused?(machine)

      {machine, [_, {:authenticate, ref, _}]} =
        step(machine, {:frame, sasl_frame(plain_init("u", "p"))})

      refute Machine.input_paused?(machine)
      {machine, []} = step(machine, {:header, header(0)})
      assert Machine.input_paused?(machine)
      {machine, _effects} = step(machine, {:auth_result, ref, :ok})
      refute Machine.input_paused?(machine)
    end

    test "header and Open pipelined under a callback reach the Endpoint after success" do
      input = sasl_header_and_init("u", "p") <> @amqp <> open_frame()
      {machine, stream, rest, effects} = drive(sasl_ready_stream(), input)

      assert [{:expect_header, [0]}, {:authenticate, ref, %{username: "u"}}] = effects
      assert Machine.input_paused?(machine)
      # The held header was consumed; the Open is still unread input.
      assert rest == open_frame()

      {machine, effects} = step(machine, {:auth_result, ref, :ok})
      assert {:start_endpoint, %ProtocolHeader{protocol_id: 0}} = List.last(effects)
      refute Machine.active?(machine)

      # The shell resumes and hands the remaining bytes to the AMQP layer.
      assert {:ok, {:frame, %Frame{type: :amqp, body: body}}, _stream, <<>>} =
               Codec.Stream.next(stream, rest)

      assert {:ok, %Open{container_id: %Value{value: "pipelined"}}, <<>>} =
               Performative.decode(body)
    end

    test "header and Open pipelined under a callback are discarded after a failure" do
      input = sasl_header_and_init("u", "p") <> @amqp <> open_frame()
      {machine, _stream, rest, [_, {:authenticate, ref, _}]} = drive(sasl_ready_stream(), input)
      assert rest == open_frame()

      {machine, [{:outcome, :auth}, {:send_bytes, 0, id, outcome}]} =
        step(machine, {:auth_result, ref, {:error, :auth}})

      assert %Outcome{code: %Value{value: 1}} = decode_sasl(outcome)
      assert Machine.input_paused?(machine)
      {machine, effects} = step(machine, {:write_result, id, :ok})
      assert List.last(effects) == {:close, {:sasl, :auth}}
      refute Enum.any?(effects, &match?({:start_endpoint, _}, &1))
      refute Machine.active?(machine)
    end

    test "an unrecognised result for the current callback fails closed with sys" do
      machine = sasl_ready!(server!(mechanisms: ["PLAIN"], authenticate: true))

      {machine, [_, {:authenticate, ref, _}]} =
        step(machine, {:frame, sasl_frame(plain_init("u", "p"))})

      for result <- [:maybe, {:ok, :principal}, {:error, :teapot}, {:error, "auth"}] do
        {failed, [{:outcome, :sys}, {:send_bytes, 0, id, outcome}]} =
          step(machine, {:auth_result, ref, result})

        assert %Outcome{code: %Value{value: 2}} = decode_sasl(outcome)
        {_failed, effects} = step(failed, {:write_result, id, :ok})
        assert List.last(effects) == {:close, {:sasl, :sys}}
      end
    end

    test "a deadline during a failure flush keeps the decided close reason" do
      machine = sasl_ready!(server!(mechanisms: ["PLAIN"], authenticate: true))

      {machine, [_, {:authenticate, ref, _}]} =
        step(machine, {:frame, sasl_frame(plain_init("u", "p"))})

      {machine, _effects} = step(machine, {:auth_result, ref, {:error, :sys_temp}})

      assert {_, [{:close, {:sasl, :sys_temp}}]} =
               step(machine, {:timeout, :sasl_negotiation, 0})

      {machine, _effects} = step(server!(mode: :required), {:header, header(0)})
      assert {_, [{:close, :sasl_required}]} = step(machine, {:timeout, :sasl_negotiation, 0})
    end

    test "remaining server transitions" do
      # :await_amqp_header refuses anything but an AMQP header with the AMQP header.
      waiting = elem(step(sasl_ready!(server!()), {:frame, sasl_frame(anonymous_init(nil))}), 0)
      assert waiting.phase == :await_amqp_header
      error = Error.new(:protocol_header, :malformed, :invalid_protocol_magic)

      for event <- [
            {:decode_error, error},
            {:frame, sasl_frame(%Response{response: Value.binary("x")})},
            {:header, header(3)}
          ] do
        {machine, [{:send_bytes, 0, id, @amqp}]} = step(waiting, event)
        {_machine, effects} = step(machine, {:write_result, id, :ok})
        assert List.last(effects) == {:close, :unsupported_protocol_header}
      end

      plain = sasl_ready!(server!(mechanisms: ["PLAIN"], authenticate: true))

      {plain, [_, {:authenticate, ref, _}]} =
        step(plain, {:frame, sasl_frame(plain_init("u", "p"))})

      # A second early header while authenticating is a protocol error.
      {held, []} = step(plain, {:header, header(0)})

      assert {_,
              [
                {:cancel_authentication, ^ref},
                {:cancel_wake, _, _, _},
                {:close, :sasl_protocol_error}
              ]} =
               step(held, {:header, header(0)})

      # A failed earlier write while authenticating cancels the callback.
      assert {_, [{:cancel_authentication, ^ref}, {:cancel_wake, _, _, _}, {:close, :closed}]} =
               step(plain, {:write_result, {:sasl, 0}, {:error, :closed}})

      # An AMQP frame where sasl-init is expected is a protocol error.
      amqp = %{sasl_frame(anonymous_init(nil)) | type: :amqp}

      assert {_, [{:cancel_wake, _, _, _}, {:close, :sasl_protocol_error}]} =
               step(sasl_ready!(server!()), {:frame, amqp})
    end

    test "invalid server options are refused" do
      for options <- [
            [role: :server, timeout_ms: 0],
            [role: :server, timeout_ms: 10, mode: :sometimes],
            [role: :server, timeout_ms: 10, mechanisms: []],
            [role: :server, timeout_ms: 10, mechanisms: ["PLAIN"]],
            [role: :server, timeout_ms: 10, mechanisms: ["CRAM-MD5"], authenticate: true],
            [role: :server, timeout_ms: 10, mechanisms: ["PLAIN", "PLAIN"], authenticate: true],
            # A callback policy must not let a no-SASL peer bypass authentication.
            [role: :server, timeout_ms: 10, mechanisms: ["PLAIN"], authenticate: true],
            [
              role: :server,
              timeout_ms: 10,
              mechanisms: ["PLAIN"],
              authenticate: true,
              mode: :optional
            ],
            [role: :server, timeout_ms: 10, authenticate: true, mode: :disabled],
            [role: :observer, timeout_ms: 10],
            :not_a_keyword
          ] do
        assert {:error, :invalid_options} = Machine.new(options)
      end
    end
  end

  describe "client" do
    test "start sends the SASL header and arms the negotiation deadline" do
      {:ok, machine} = Machine.new(role: :client, credentials: :anonymous, timeout_ms: 500)
      {machine, effects} = step(machine, :start, 10)

      assert [{:wake_at, 0, :sasl_negotiation, 0, 510}, {:send_bytes, 0, {:sasl, 0}, @sasl}] =
               effects

      assert Machine.active?(machine)
    end

    test "PLAIN is selected from the server list and credentials leave the machine with sasl-init" do
      {:ok, plain} = Plain.new("graviton-user", "sentinel-password", "admin")
      machine = client!(credentials: plain, hostname: "broker.example")
      {machine, []} = step(machine, {:header, header(3)})
      offered = Value.array(:symbol, [Value.symbol("SCRAM-SHA-256"), Value.symbol("PLAIN")])

      {machine, effects} =
        step(machine, {:frame, sasl_frame(%Mechanisms{server_mechanisms: offered})})

      assert [{:send_bytes, 0, {:sasl, 1}, init}] = effects

      assert %Init{
               mechanism: %Value{type: :symbol, value: "PLAIN"},
               initial_response: %Value{type: :binary, value: response},
               hostname: %Value{type: :string, value: "broker.example"}
             } = decode_sasl(init)

      assert {:ok, ^plain} = Plain.parse(response)
      assert machine.credentials == nil
      refute_retained(machine, ["sentinel-password", "graviton-user"])

      {machine, effects} = step(machine, {:frame, sasl_frame(%Outcome{code: Value.ubyte(0)})})

      assert [
               {:cancel_wake, 0, :sasl_negotiation, 0},
               {:expect_header, [0]},
               {:start_endpoint, nil}
             ] = effects

      refute Machine.active?(machine)
      refute_retained(machine, ["sentinel-password", "graviton-user"])
    end

    test "credentials are dropped when negotiation ends before sasl-init" do
      {:ok, plain} = Plain.new("graviton-user", "sentinel-password")

      for event <- [
            {:header, header(0)},
            {:timeout, :sasl_negotiation, 0},
            {:transport_down, :closed}
          ] do
        {machine, _effects} = step(client!(credentials: plain), event)
        refute Machine.active?(machine)
        refute_retained(machine, ["sentinel-password", "graviton-user"])
      end

      {machine, []} = step(client!(credentials: plain), {:header, header(3)})

      {machine, [_, {:close, :sasl_mechanism_not_offered}]} =
        step(
          machine,
          {:frame, sasl_frame(%Mechanisms{server_mechanisms: Value.symbol("ANONYMOUS")})}
        )

      refute_retained(machine, ["sentinel-password", "graviton-user"])
    end

    test "ANONYMOUS sends its optional trace" do
      machine = client!(credentials: {:anonymous, "sirhc"})
      {machine, []} = step(machine, {:header, header(3)})

      {_machine, [{:send_bytes, 0, _, init}]} =
        step(
          machine,
          {:frame, sasl_frame(%Mechanisms{server_mechanisms: Value.symbol("ANONYMOUS")})}
        )

      assert %Init{initial_response: %Value{value: "sirhc"}, hostname: nil} = decode_sasl(init)
    end

    test "failure outcomes, refusals and challenges close with exact reasons" do
      for {code, reason} <- [
            {1, :auth},
            {2, :sys},
            {3, :sys_perm},
            {4, :sys_temp},
            {9, {:unknown, 9}}
          ] do
        machine = awaiting_outcome!()

        assert {_, [_, {:close, {:sasl, ^reason}}]} =
                 step(machine, {:frame, sasl_frame(%Outcome{code: Value.ubyte(code)})})
      end

      machine = client!(credentials: :anonymous)
      assert {_, [_, {:close, :sasl_not_offered}]} = step(machine, {:header, header(0)})

      {machine, []} = step(client!(credentials: :anonymous), {:header, header(3)})

      assert {_, [_, {:close, :sasl_mechanism_not_offered}]} =
               step(
                 machine,
                 {:frame, sasl_frame(%Mechanisms{server_mechanisms: Value.symbol("PLAIN")})}
               )

      {machine, []} = step(client!(credentials: :anonymous), {:header, header(3)})

      assert {_, [_, {:close, :sasl_protocol_error}]} =
               step(machine, {:frame, sasl_frame(%Outcome{code: Value.ubyte(0)})})

      challenge = %GravitonMQ.AMQP10.SASL.Challenge{challenge: Value.binary(<<>>)}

      assert {_, [_, {:close, :sasl_protocol_error}]} =
               step(awaiting_outcome!(), {:frame, sasl_frame(challenge)})

      assert {_, [{:close, :sasl_timeout}]} =
               step(awaiting_outcome!(), {:timeout, :sasl_negotiation, 0})
    end

    test "credentials whose sasl-init cannot fit a 512-octet frame are refused" do
      long = String.duplicate("a", 255)
      {:ok, plain} = Plain.new(long, long, long)

      assert {:error, :invalid_options} =
               Machine.new(role: :client, credentials: plain, timeout_ms: 10)

      assert {:error, :invalid_options} =
               Machine.new(role: :client, credentials: {:anonymous, <<0xFF>>}, timeout_ms: 10)

      assert {:error, :invalid_options} =
               Machine.new(role: :client, credentials: :token, timeout_ms: 10)

      assert {:error, :invalid_options} =
               Machine.new(role: :client, credentials: :anonymous, hostname: 7, timeout_ms: 10)
    end
  end

  defp client!(options) do
    {:ok, machine} = Machine.new(Keyword.merge([role: :client, timeout_ms: 1_000], options))
    {machine, _effects} = step(machine, :start)
    machine
  end

  defp awaiting_outcome! do
    {machine, []} = step(client!(credentials: :anonymous), {:header, header(3)})

    {machine, [{:send_bytes, _, _, _}]} =
      step(
        machine,
        {:frame, sasl_frame(%Mechanisms{server_mechanisms: Value.symbol("ANONYMOUS")})}
      )

    machine
  end

  # A minimal shell drain: feed stream items to the machine while it is active
  # and input is not paused; apply `{:expect_header, accept}` to the stream.
  defp drive({machine, stream}, input, effects \\ []) do
    if Machine.active?(machine) and not Machine.input_paused?(machine) do
      case Codec.Stream.next(stream, input) do
        {:ok, {kind, item}, stream, rest} ->
          event = if kind == :protocol_header, do: {:header, item}, else: {:frame, item}
          {machine, new} = step(machine, event)

          stream =
            Enum.reduce(new, stream, fn
              {:expect_header, accept}, stream ->
                {:ok, stream} = Codec.Stream.expect_header(stream, accept)
                stream

              _effect, stream ->
                stream
            end)

          drive({machine, stream}, rest, effects ++ new)

        {:more, stream} ->
          {machine, stream, <<>>, effects}
      end
    else
      {machine, stream, input, effects}
    end
  end

  defp sasl_ready_stream do
    machine = server!(mechanisms: ["PLAIN"], authenticate: true)
    {:ok, stream} = Codec.Stream.new(Limits.default(), accept: [0, 3])
    {machine, stream, <<>>, [_mechanisms]} = drive({machine, stream}, @sasl)
    {machine, stream}
  end

  defp sasl_header_and_init(username, password) do
    {:ok, frame} = SASLCodec.encode_frame(plain_init(username, password))
    frame
  end

  defp open_frame do
    {:ok, body} = Performative.encode(%Open{container_id: Value.string("pipelined")})
    <<8 + byte_size(body)::32, 2, 0, 0::16, body::binary>>
  end

  # The whole machine term, including what `inspect/1` would hide, holds none of
  # the secrets.
  defp refute_retained(machine, secrets) do
    raw = :erlang.term_to_binary(machine)
    printed = inspect(machine, structs: false, limit: :infinity, printable_limit: :infinity)

    for secret <- secrets do
      refute :binary.match(raw, secret) != :nomatch
      refute printed =~ secret
    end
  end

  defp step(machine, event, now \\ 0),
    do: Machine.step(machine, %{generation: 0, now_ms: now, event: event})

  defp header(id), do: %ProtocolHeader{protocol_id: id, major: 1, minor: 0, revision: 0}

  defp server!(options \\ []) do
    {:ok, machine} =
      Machine.new(Keyword.merge(server_defaults(options), options))

    {machine, [{:wake_at, 0, :sasl_negotiation, 0, 1_000}]} = step(machine, :start)
    machine
  end

  # A callback policy is valid only with mandatory SASL.
  defp server_defaults(options) do
    mode = if Keyword.get(options, :authenticate, false), do: :required, else: :optional
    [role: :server, timeout_ms: 1_000, mode: mode]
  end

  defp sasl_ready!(machine) do
    {machine, _effects} = step(machine, {:header, header(3)})
    machine
  end

  defp sasl_frame(body) do
    {:ok, bytes} = SASLCodec.encode(body)

    %Frame{
      declared_size: 8 + byte_size(bytes),
      data_offset_words: 2,
      type: :sasl,
      channel: 0,
      extended_header: <<>>,
      body: bytes
    }
  end

  defp decode_sasl(frame) do
    {:ok, %Frame{type: :sasl, body: body}, <<>>} =
      FrameCodec.decode(frame, Limits.default(), types: [:sasl])

    {:ok, decoded} = SASLCodec.decode(body)
    decoded
  end

  defp sent(effects), do: for({:send_bytes, 0, _id, bytes} <- effects, do: bytes)

  defp plain_init(username, password, hostname \\ nil) do
    {:ok, plain} = Plain.new(username, password)
    {:ok, message} = Plain.encode(plain)

    %Init{
      mechanism: Value.symbol("PLAIN"),
      initial_response: Value.binary(message),
      hostname: hostname && Value.string(hostname)
    }
  end

  defp anonymous_init(nil), do: %Init{mechanism: Value.symbol("ANONYMOUS")}

  defp anonymous_init(trace),
    do: %Init{mechanism: Value.symbol("ANONYMOUS"), initial_response: Value.binary(trace)}
end
