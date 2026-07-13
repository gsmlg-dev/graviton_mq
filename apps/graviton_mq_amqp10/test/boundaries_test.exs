defmodule GravitonMQ.AMQP10.BoundariesTest do
  use ExUnit.Case, async: true

  @modules [
    GravitonMQ.AMQP10,
    GravitonMQ.AMQP10.Types,
    GravitonMQ.AMQP10.Frame,
    GravitonMQ.AMQP10.Performative,
    GravitonMQ.AMQP10.Message,
    GravitonMQ.AMQP10.ConnectionState,
    GravitonMQ.AMQP10.SessionState,
    GravitonMQ.AMQP10.Link,
    GravitonMQ.AMQP10.Settlement,
    GravitonMQ.AMQP10.SASL,
    GravitonMQ.AMQP10.Error
  ]

  test "AMQP 1.0 boundary modules load" do
    Enum.each(@modules, fn module ->
      assert Code.ensure_loaded?(module), "expected #{inspect(module)} to load"
    end)
  end

  test "a session data structure owns its link state" do
    link = struct(GravitonMQ.AMQP10.Link, name: "orders", role: :sender)
    session = struct(GravitonMQ.AMQP10.SessionState, links: %{"orders" => link})

    assert %{links: %{"orders" => ^link}} = session
  end

  test "codec and parser operations are not implemented in Milestone 0" do
    refute function_exported?(GravitonMQ.AMQP10.Frame, :decode, 1)
    refute function_exported?(GravitonMQ.AMQP10.Performative, :parse, 1)
  end
end
