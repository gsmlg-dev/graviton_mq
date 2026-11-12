defmodule GravitonMQ.AMQP10.SASLTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.{SASL, Value}
  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.SASL.{Challenge, Init, Mechanisms, Outcome, Response}

  test "sasl-code names are exact and unknown codes are retained, not whitelisted" do
    assert Enum.map(0..4, &SASL.code_name/1) == [:ok, :auth, :sys, :sys_perm, :sys_temp]
    assert SASL.code_name(9) == {:unknown, 9}
    assert SASL.code_name(255) == {:unknown, 255}
    assert Enum.map([:ok, :auth, :sys, :sys_perm, :sys_temp], &SASL.code/1) == [0, 1, 2, 3, 4]
  end

  test "mechanism names follow RFC 4422 section 3.1" do
    assert SASL.mechanism_name?("PLAIN")
    assert SASL.mechanism_name?("SCRAM-SHA-256")
    assert SASL.mechanism_name?("X_1")
    refute SASL.mechanism_name?("plain")
    refute SASL.mechanism_name?("")
    refute SASL.mechanism_name?(String.duplicate("A", 21))
    refute SASL.mechanism_name?(:PLAIN)
    assert SASL.supported_mechanisms() == ["PLAIN", "ANONYMOUS"]
  end

  test "mechanism lists expose names in wire order for a symbol or a symbol array" do
    assert Mechanisms.names(%Mechanisms{server_mechanisms: Value.symbol("ANONYMOUS")}) == [
             "ANONYMOUS"
           ]

    array = Value.array(:symbol, [Value.symbol("PLAIN"), Value.symbol("ANONYMOUS")])
    assert Mechanisms.names(%Mechanisms{server_mechanisms: array}) == ["PLAIN", "ANONYMOUS"]
  end

  test "bodies that may carry secrets never inspect their secret fields" do
    secret = "secret-#{System.unique_integer([:positive])}"

    for body <- [
          %Init{mechanism: Value.symbol("PLAIN"), initial_response: Value.binary(secret)},
          %Response{response: Value.binary(secret)},
          %Challenge{challenge: Value.binary(secret)},
          %Outcome{code: Value.ubyte(1), additional_data: Value.binary(secret)}
        ] do
      refute inspect(body) =~ secret
      assert inspect(body) =~ ":redacted"

      # Nested containers and unbounded/pretty options still dispatch to the
      # redacting Inspect implementation.
      for container <- [[body], %{body: body}, {:body, body}, [frame: [body: body]]],
          options <- [[], [pretty: true, limit: :infinity, printable_limit: :infinity]] do
        refute inspect(container, options) =~ secret
      end
    end

    assert inspect(%Init{mechanism: Value.symbol("PLAIN")}) =~ "\"PLAIN\""
    refute inspect(%Init{mechanism: Value.symbol(secret)}) =~ secret
  end

  test "redaction is limited to the Inspect protocol, as documented" do
    secret = "secret-#{System.unique_integer([:positive])}"
    body = %Response{response: Value.binary(secret)}

    # `structs: false` and Erlang term formatting bypass Inspect; callers must
    # not keep these bodies in printable process state.
    assert inspect(body, structs: false) =~ secret
    assert :io_lib.format(~c"~p", [body]) |> IO.iodata_to_binary() =~ secret

    for module <- [Init, Response, Challenge, Outcome, SASL.Plain] do
      {:docs_v1, _, _, _, %{"en" => doc}, _, _} = Code.fetch_docs(module)
      assert doc =~ "`inspect/1` only", "#{inspect(module)} must document the redaction limit"
    end
  end

  test "codec errors accept the SASL operations" do
    assert %Error{operation: :sasl_decode} = Error.new(:sasl_decode, :malformed, :x)
    assert %Error{operation: :sasl_encode} = Error.new(:sasl_encode, :invalid_value, :x)
  end
end
