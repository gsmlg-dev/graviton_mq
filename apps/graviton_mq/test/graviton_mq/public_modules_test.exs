defmodule GravitonMQ.PublicModulesTest do
  use ExUnit.Case, async: true

  test "the intentionally small public module surface loads" do
    for module <- [
          GravitonMQ,
          GravitonMQ.Application,
          GravitonMQ.Supervisor,
          GravitonMQ.Config,
          GravitonMQ.Architecture,
          GravitonMQ.MQTT.Frame,
          GravitonMQ.MQTT.Codec.Frame,
          GravitonMQ.MQTT.Codec.Packet
        ] do
      assert Code.ensure_loaded?(module), "expected #{inspect(module)} to load"
    end
  end

  test "no unimplemented queue operations are exposed" do
    for {function, arity} <- [consume: 3, declare_queue: 2] do
      refute function_exported?(GravitonMQ, function, arity)
    end
  end

  test "an instance without MQTT configuration cannot claim to accept host operations" do
    assert {:error, :mqtt_not_enabled} =
             GravitonMQ.publish(GravitonMQ.Supervisor, "unconfigured/value", "payload")

    assert {:error, :mqtt_not_enabled} =
             GravitonMQ.subscribe(GravitonMQ.Supervisor, "unconfigured/#")

    assert {:error, :mqtt_not_enabled} = GravitonMQ.status(GravitonMQ.Supervisor)
    assert {:error, :mqtt_not_enabled} = GravitonMQ.ack(GravitonMQ.Supervisor, make_ref())
  end

  test "the common instance lifecycle remains available" do
    assert function_exported?(GravitonMQ, :start_link, 1)
    assert function_exported?(GravitonMQ, :child_spec, 1)
  end
end
