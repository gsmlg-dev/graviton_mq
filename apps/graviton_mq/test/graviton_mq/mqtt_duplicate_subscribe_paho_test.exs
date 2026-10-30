Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.DuplicateSubscribePahoTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.Client.Test.Mosquitto, as: Peer

  @moduletag :paho_interop
  @moduletag :capture_log
  @moduletag timeout: 15_000

  for {first, second} <- [{2, 1}, {1, 2}] do
    @first first
    @second second
    test "Paho duplicate filters apply RH#{first} then RH#{second} sequentially" do
      instance =
        start_supervised!(
          {GravitonMQ,
           name: nil,
           runtime_supervisor_name: nil,
           mqtt: [
             auth: :anonymous,
             listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
           ]}
        )

      assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)

      peer =
        Peer.process!(System.fetch_env!("GRAVITON_PAHO_PYTHON"), [
          Path.expand("../support/mqtt_duplicate_subscribe_peer.py", __DIR__),
          to_string(port),
          to_string(@first),
          to_string(@second)
        ])

      Peer.eventually!(fn -> Peer.snapshot(peer).exit_status != nil end, fn ->
        Peer.snapshot(peer)
      end)

      assert %{exit_status: 0, output: output} = Peer.snapshot(peer)
      assert output =~ "PASS Paho2.1.0 ordered RH#{@first},RH#{@second}"

      Peer.eventually!(fn ->
        match?({:ok, %{connections: 0, pending_deliveries: 0}}, GravitonMQ.status(instance))
      end)
    end
  end
end
