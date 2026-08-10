defmodule GravitonMQ.AMQP10.BoundariesTest do
  use ExUnit.Case, async: true

  @modules [
    GravitonMQ.AMQP10,
    GravitonMQ.AMQP10.Types,
    GravitonMQ.AMQP10.Value,
    GravitonMQ.AMQP10.Codec,
    GravitonMQ.AMQP10.Codec.Error,
    GravitonMQ.AMQP10.Codec.Limits,
    GravitonMQ.AMQP10.Codec.ProtocolHeader,
    GravitonMQ.AMQP10.Codec.Frame,
    GravitonMQ.AMQP10.Codec.Value,
    GravitonMQ.AMQP10.Codec.Performative,
    GravitonMQ.AMQP10.Frame,
    GravitonMQ.AMQP10.Performative,
    GravitonMQ.AMQP10.Performative.Open,
    GravitonMQ.AMQP10.Performative.Begin,
    GravitonMQ.AMQP10.Message,
    GravitonMQ.AMQP10.ConnectionState,
    GravitonMQ.AMQP10.SessionState,
    GravitonMQ.AMQP10.Link,
    GravitonMQ.AMQP10.Settlement,
    GravitonMQ.AMQP10.Outcome,
    GravitonMQ.AMQP10.OutcomeMapper,
    GravitonMQ.AMQP10.SASL,
    GravitonMQ.AMQP10.Error
  ]

  test "AMQP 1.0 boundary modules load" do
    Enum.each(@modules, fn module ->
      assert Code.ensure_loaded?(module), "expected #{inspect(module)} to load"
    end)
  end

  test "Milestone 1 exposes only the bounded codec foundation" do
    for module <- [
          GravitonMQ.AMQP10.Codec.ProtocolHeader,
          GravitonMQ.AMQP10.Codec.Frame,
          GravitonMQ.AMQP10.Codec.Value,
          GravitonMQ.AMQP10.Codec.Performative,
          GravitonMQ.AMQP10.Performative.Open,
          GravitonMQ.AMQP10.Performative.Begin,
          GravitonMQ.AMQP10.Performative
        ] do
      assert Code.ensure_loaded?(module)
    end

    assert function_exported?(GravitonMQ.AMQP10.Codec.ProtocolHeader, :recognize, 1)
    assert function_exported?(GravitonMQ.AMQP10.Codec.Frame, :decode, 2)
    assert function_exported?(GravitonMQ.AMQP10.Codec.Value, :decode, 2)
    assert function_exported?(GravitonMQ.AMQP10.Codec.Value, :encode, 2)
    assert function_exported?(GravitonMQ.AMQP10.Codec.Performative, :decode, 2)
    assert function_exported?(GravitonMQ.AMQP10.Codec.Performative, :encode, 2)

    refute function_exported?(GravitonMQ.AMQP10.Performative, :parse, 1)
    refute function_exported?(GravitonMQ.AMQP10.Codec.Performative, :parse, 1)
    refute function_exported?(GravitonMQ.AMQP10.Codec.Performative, :transition, 2)
    refute function_exported?(GravitonMQ.AMQP10.Codec.Performative, :negotiate, 2)

    for module <- [
          GravitonMQ.AMQP10.Codec.Performative,
          GravitonMQ.AMQP10.Performative.Open,
          GravitonMQ.AMQP10.Performative.Begin
        ] do
      refute function_exported?(module, :start_link, 1)
      refute function_exported?(module, :child_spec, 1)
    end
  end
end
