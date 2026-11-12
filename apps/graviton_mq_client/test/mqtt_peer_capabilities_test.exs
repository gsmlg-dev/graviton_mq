Code.require_file("support/mosquitto.exs", __DIR__)
Code.require_file("support/mqtt_wire_tap.exs", __DIR__)

defmodule GravitonMQ.MQTTClient.PeerCapabilitiesTest do
  # S4 T0: re-verify pinned peer capabilities before relying on them.
  use ExUnit.Case, async: false

  alias GravitonMQ.Client.Test.{Mosquitto, MQTTWireTap}

  @moduletag :interop
  @moduletag timeout: 30_000

  setup_all do
    %{executables: Mosquitto.executables!()}
  end

  setup do
    directory = Mosquitto.directory!()
    on_exit(fn -> File.rm_rf!(directory) end)
    %{directory: directory}
  end

  test "Mosquitto 2.1.2 accepts max_topic_alias_broker", context do
    broker =
      capability!("Mosquitto 2.1.2 configuration variable max_topic_alias_broker", fn ->
        Mosquitto.broker!(context.executables, context.directory,
          configuration: "max_topic_alias_broker 10"
        )
      end)

    refute Mosquitto.snapshot(broker.owner).output =~ "Unknown configuration variable",
           "missing pinned peer capability: Mosquitto 2.1.2 configuration variable max_topic_alias_broker"
  end

  test "mosquitto_pub --repeat with topic-alias sends alias-only PUBLISH after the first",
       context do
    broker = Mosquitto.broker!(context.executables, context.directory)
    tap = start_supervised!({MQTTWireTap, upstream_port: broker.port, observer: self()})

    capability!("mosquitto_pub --repeat/--repeat-delay with -D publish topic-alias", fn ->
      Mosquitto.command!(
        context.executables.publisher,
        Mosquitto.cli_args(%{port: MQTTWireTap.port(tap)}) ++
          ["-t", "probe/a", "-m", "x", "-q", "1", "--repeat", "3", "--repeat-delay", "0"] ++
          ["-D", "publish", "topic-alias", "1"]
      )
    end)

    sent =
      for %{direction: :to_server} = fact <- MQTTWireTap.observed(tap),
          do: {fact.topic, fact.topic_alias}

    assert sent == [{"probe/a", 1}, {"", 1}, {"", 1}],
           "missing pinned peer capability: mosquitto_pub --repeat with -D publish topic-alias 1 " <>
             "does not send alias-only PUBLISH after the first; observed #{inspect(sent)}"
  end

  # Names the probed capability when the peer rejects it, keeping the original
  # failure (exit status, output or timeout diagnostic) in the message.
  defp capability!(capability, fun) do
    fun.()
  rescue
    error in ExUnit.AssertionError ->
      flunk("missing pinned peer capability: #{capability}: #{Exception.message(error)}")
  end
end
