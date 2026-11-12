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
    GravitonMQ.AMQP10.Performative.Detach,
    GravitonMQ.AMQP10.Performative.End,
    GravitonMQ.AMQP10.Performative.Close,
    GravitonMQ.AMQP10.Message,
    GravitonMQ.AMQP10.ConnectionState,
    GravitonMQ.AMQP10.SessionState,
    GravitonMQ.AMQP10.Link,
    GravitonMQ.AMQP10.Settlement,
    GravitonMQ.AMQP10.Outcome,
    GravitonMQ.AMQP10.OutcomeMapper,
    GravitonMQ.AMQP10.SASL,
    GravitonMQ.AMQP10.Codec.Stream,
    GravitonMQ.AMQP10.Codec.SASL,
    GravitonMQ.AMQP10.SASL.Mechanisms,
    GravitonMQ.AMQP10.SASL.Init,
    GravitonMQ.AMQP10.SASL.Challenge,
    GravitonMQ.AMQP10.SASL.Response,
    GravitonMQ.AMQP10.SASL.Outcome,
    GravitonMQ.AMQP10.SASL.Plain,
    GravitonMQ.AMQP10.SASL.Anonymous,
    GravitonMQ.AMQP10.SASL.Machine,
    GravitonMQ.AMQP10.Error
  ]

  test "AMQP 1.0 boundary modules load" do
    Enum.each(@modules, fn module ->
      assert Code.ensure_loaded?(module), "expected #{inspect(module)} to load"
    end)
  end

  test "current bounded codec foundation modules load" do
    for module <- [
          GravitonMQ.AMQP10.Codec.ProtocolHeader,
          GravitonMQ.AMQP10.Codec.Frame,
          GravitonMQ.AMQP10.Codec.Value,
          GravitonMQ.AMQP10.Codec.Performative,
          GravitonMQ.AMQP10.Performative.Open,
          GravitonMQ.AMQP10.Performative.Begin,
          GravitonMQ.AMQP10.Performative.Detach,
          GravitonMQ.AMQP10.Performative.End,
          GravitonMQ.AMQP10.Performative.Close,
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
          GravitonMQ.AMQP10.Performative.Begin,
          GravitonMQ.AMQP10.Performative.Detach,
          GravitonMQ.AMQP10.Performative.End,
          GravitonMQ.AMQP10.Performative.Close
        ] do
      # function_exported?/3 does not load modules; load them so the checks
      # do not depend on test order.
      assert {:module, ^module} = Code.ensure_loaded(module)
      refute function_exported?(module, :start_link, 1)
      refute function_exported?(module, :child_spec, 1)
    end
  end

  test "SASL layer modules are pure data and functions without process entry points" do
    for module <- [
          GravitonMQ.AMQP10.Codec.SASL,
          GravitonMQ.AMQP10.SASL.Machine,
          GravitonMQ.AMQP10.SASL.Plain,
          GravitonMQ.AMQP10.SASL.Anonymous
        ] do
      # function_exported?/3 does not load modules; load them so the checks
      # do not depend on test order.
      assert {:module, ^module} = Code.ensure_loaded(module)
      refute function_exported?(module, :start_link, 1)
      refute function_exported?(module, :child_spec, 1)
    end

    assert function_exported?(GravitonMQ.AMQP10.SASL.Machine, :step, 2)
    # SASL is composed by shells; the Endpoint option stays rejected.
    assert {:error, :sasl_not_supported} =
             GravitonMQ.AMQP10.Endpoint.new(container_id: "c", sasl: true)
  end
end
