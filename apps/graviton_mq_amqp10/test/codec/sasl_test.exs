defmodule GravitonMQ.AMQP10.Codec.SASLTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Codec.{Error, Limits, Performative}
  alias GravitonMQ.AMQP10.Codec.Frame, as: FrameCodec
  alias GravitonMQ.AMQP10.Codec.SASL, as: Codec
  alias GravitonMQ.AMQP10.Codec.Value, as: ValueCodec
  alias GravitonMQ.AMQP10.Performative.Open
  alias GravitonMQ.AMQP10.SASL.{Challenge, Init, Mechanisms, Outcome, Response}

  @bodies [
    %Mechanisms{server_mechanisms: Value.symbol("ANONYMOUS")},
    %Mechanisms{
      server_mechanisms: Value.array(:symbol, [Value.symbol("PLAIN"), Value.symbol("ANONYMOUS")])
    },
    %Init{mechanism: Value.symbol("ANONYMOUS")},
    %Init{
      mechanism: Value.symbol("PLAIN"),
      initial_response: Value.binary(<<0, "user", 0, "pass">>),
      hostname: Value.string("broker.example")
    },
    %Challenge{challenge: Value.binary(<<>>)},
    %Response{response: Value.binary(<<1, 2, 3>>)},
    %Outcome{code: Value.ubyte(0)},
    %Outcome{code: Value.ubyte(1), additional_data: Value.binary(<<7>>)}
  ]

  test "every body round-trips with exact tagged values and a numeric descriptor" do
    for {body, code} <- Enum.zip(@bodies, [0x40, 0x40, 0x41, 0x41, 0x42, 0x43, 0x44, 0x44]) do
      assert {:ok, <<0x00, 0x53, ^code, _::binary>> = bytes} = Codec.encode(body)
      assert {:ok, ^body} = Codec.decode(bytes)
    end
  end

  test "symbolic descriptors decode to the same bodies" do
    for {symbol, fields, expected} <- [
          {"amqp:sasl-outcome:list", [Value.ubyte(1)], %Outcome{code: Value.ubyte(1)}},
          {"amqp:sasl-init:list", [Value.symbol("PLAIN")],
           %Init{mechanism: Value.symbol("PLAIN")}},
          {"amqp:sasl-mechanisms:list", [Value.symbol("PLAIN")],
           %Mechanisms{server_mechanisms: Value.symbol("PLAIN")}},
          {"amqp:sasl-challenge:list", [Value.binary("c")],
           %Challenge{challenge: Value.binary("c")}},
          {"amqp:sasl-response:list", [Value.binary("r")], %Response{response: Value.binary("r")}}
        ] do
      {:ok, bytes} =
        ValueCodec.encode(Value.described(Value.symbol(symbol), Value.list(fields)))

      assert {:ok, ^expected} = Codec.decode(bytes)
    end
  end

  test "interior nulls are absent optional fields and unknown sasl-codes are data" do
    {:ok, init} =
      ValueCodec.encode(
        Value.described(
          Value.ulong(0x41),
          Value.list([Value.symbol("PLAIN"), Value.null(), Value.string("h")])
        )
      )

    assert {:ok, %Init{initial_response: nil, hostname: %Value{type: :string, value: "h"}}} =
             Codec.decode(init)

    {:ok, outcome} = Codec.encode(%Outcome{code: Value.ubyte(9)})
    assert {:ok, %Outcome{code: %Value{type: :ubyte, value: 9}}} = Codec.decode(outcome)
  end

  test "mechanism lists must be a non-empty symbol or symbol array of at most 16 names" do
    for {value, reason, class} <- [
          {Value.null(), :mandatory_field_missing, :malformed},
          {Value.array(:symbol, []), :empty_mechanism_list, :malformed},
          {Value.string("PLAIN"), :field_type_mismatch, :malformed},
          {Value.array(:symbol, for(i <- 1..17, do: Value.symbol("M#{i}"))), :too_many_mechanisms,
           :limit_exceeded}
        ] do
      {:ok, bytes} = ValueCodec.encode(Value.described(Value.ulong(0x40), Value.list([value])))

      assert {:error,
              %Error{
                operation: :sasl_decode,
                class: ^class,
                reason: ^reason,
                details: %{body: :mechanisms, index: 0}
              }} = Codec.decode(bytes)
    end
  end

  test "frame bodies hold exactly one SASL value" do
    {:ok, outcome} = Codec.encode(%Outcome{code: Value.ubyte(0)})
    {:ok, open} = Performative.encode(%Open{container_id: Value.string("c")})

    assert {:error, %Error{reason: :empty_sasl_frame}} = Codec.decode(<<>>)
    assert {:error, %Error{reason: :trailing_bytes}} = Codec.decode(outcome <> <<0x40>>)

    assert {:error, %Error{reason: :truncated_sasl_body}} =
             Codec.decode(binary_part(outcome, 0, 4))

    assert {:error, %Error{reason: :not_a_sasl_body}} = Codec.decode(open)
    assert {:error, %Error{reason: :not_a_sasl_body}} = Codec.decode(<<0x41>>)

    {:ok, too_many} =
      ValueCodec.encode(
        Value.described(
          Value.ulong(0x44),
          Value.list([Value.ubyte(0), Value.null(), Value.null()])
        )
      )

    assert {:error, %Error{reason: :too_many_fields, details: %{body: :outcome}}} =
             Codec.decode(too_many)
  end

  test "errors for secret-bearing fields carry only the body and index" do
    secret = "secret-#{System.unique_integer([:positive])}"

    {:ok, bytes} =
      ValueCodec.encode(
        Value.described(
          Value.ulong(0x41),
          Value.list([Value.symbol("PLAIN"), Value.string(secret)])
        )
      )

    assert {:error, %Error{reason: :field_type_mismatch, details: details} = error} =
             Codec.decode(bytes)

    assert details == %{body: :init, index: 1}
    refute inspect(error) =~ secret

    assert {:error,
            %Error{
              operation: :sasl_encode,
              class: :invalid_value,
              details: %{body: :response, index: 0}
            }} =
             Codec.encode(%Response{response: Value.string(secret)})
  end

  test "encode_frame emits a bounded SASL frame and refuses an oversize body without its length" do
    assert {:ok, frame} = Codec.encode_frame(%Outcome{code: Value.ubyte(0)})

    assert {:ok, %GravitonMQ.AMQP10.Frame{type: :sasl, channel: 0, body: body}, <<>>} =
             FrameCodec.decode(frame, Limits.default(), types: [:sasl])

    assert {:ok, %Outcome{}} = Codec.decode(body)

    big = %Init{
      mechanism: Value.symbol("PLAIN"),
      initial_response: Value.binary(:binary.copy("x", 600))
    }

    assert {:error, %Error{class: :limit_exceeded} = error} = Codec.encode_frame(big)
    refute inspect(error) =~ "600"
  end
end
