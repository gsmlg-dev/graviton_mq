Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.FeaturePahoTest do
  use ExUnit.Case, async: false
  @moduletag :paho_interop
  @moduletag timeout: 60_000
  alias GravitonMQ.Client.Test.Mosquitto

  test "T0: Paho 2.1.0 exposes manual Topic Alias and Subscription Identifier properties" do
    assert run!("probe", 0) =~ "capability probe passed"
  end

  for {mode, extra, broker_options, expected} <- [
        {"alias-inbound", [], [], "inbound Topic Alias resolved"},
        {"alias-invalid", [], [], "DISCONNECT 0x94"},
        {"alias-unknown", [], [], "DISCONNECT 0x82"},
        {"alias-reconnect", [], [], "not carried across"},
        {"alias-outbound", ["3"], [max_outbound_topic_aliases: 10], "bounded by 3"},
        {"subid", [], [], "overlap, removal and retained"},
        {"shared", ["10"], [], "exactly once across two members"},
        {"shared-redistribute", [], [], "moved to the connected member"},
        {"shared-nolocal", [], [], "DISCONNECT 0x82"}
      ] do
    @mode mode
    @extra extra
    @broker_options broker_options
    @expected expected
    test "Paho → broker: #{mode}" do
      port = broker(@broker_options)
      assert run!(@mode, port, @extra) =~ @expected
    end
  end

  defp broker(extra) do
    instance =
      start_supervised!(
        {GravitonMQ,
         name: nil,
         runtime_supervisor_name: nil,
         mqtt:
           [
             auth: :anonymous,
             host_principal: :host,
             tick_interval: 10,
             listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
           ] ++ extra}
      )

    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    port
  end

  defp run!(mode, port, extra \\ []) do
    peer =
      Mosquitto.process!(
        System.fetch_env!("GRAVITON_PAHO_PYTHON"),
        [Path.expand("../support/mqtt_feature_peer.py", __DIR__), mode, to_string(port)] ++ extra
      )

    Mosquitto.eventually!(
      fn -> Mosquitto.snapshot(peer).exit_status != nil end,
      fn -> Mosquitto.snapshot(peer) end,
      30_000
    )

    assert %{exit_status: 0, output: output} = Mosquitto.snapshot(peer)
    output
  end
end
