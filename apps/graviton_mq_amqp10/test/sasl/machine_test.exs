defmodule GravitonMQ.AMQP10.SASL.MachineTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.{Frame, Value}
  alias GravitonMQ.AMQP10.Codec.{Error, Limits, ProtocolHeader}
  alias GravitonMQ.AMQP10.Codec.Frame, as: FrameCodec
  alias GravitonMQ.AMQP10.Codec.SASL, as: SASLCodec
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

    test "invalid server options are refused" do
      for options <- [
            [role: :server, timeout_ms: 0],
            [role: :server, timeout_ms: 10, mode: :sometimes],
            [role: :server, timeout_ms: 10, mechanisms: []],
            [role: :server, timeout_ms: 10, mechanisms: ["PLAIN"]],
            [role: :server, timeout_ms: 10, mechanisms: ["CRAM-MD5"], authenticate: true],
            [role: :server, timeout_ms: 10, mechanisms: ["PLAIN", "PLAIN"], authenticate: true],
            [role: :observer, timeout_ms: 10],
            :not_a_keyword
          ] do
        assert {:error, :invalid_options} = Machine.new(options)
      end
    end
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
      Machine.new(Keyword.merge([role: :server, timeout_ms: 1_000], options))

    {machine, [{:wake_at, 0, :sasl_negotiation, 0, 1_000}]} = step(machine, :start)
    machine
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
