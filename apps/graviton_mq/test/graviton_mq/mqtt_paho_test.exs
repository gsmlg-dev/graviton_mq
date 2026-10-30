Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.PahoTest do
  use ExUnit.Case, async: false
  @moduletag :paho_interop
  alias GravitonMQ.Client.Test.Mosquitto

  for reason <- [4, 0x80] do
    @reason reason
    test "Paho DISCONNECT #{reason} preserves Will and respects Will Delay" do
      instance = broker()
      assert {:ok, %{owner: owner, listeners: [%{port: port}]}} = GravitonMQ.status(instance)
      assert {:ok, operation} = GravitonMQ.subscribe(instance, "will/#", qos: 1)
      assert_receive {:graviton_mq, ^owner, {:operation, ^operation, {:ok, subscription}}}, 1_000
      assert :ok = GravitonMQ.request(instance, subscription, 1)
      peer = peer!(port, "will-#{@reason}")

      Mosquitto.eventually!(
        fn ->
          String.contains?(Mosquitto.snapshot(peer).output, "transmitted")
        end,
        fn -> Mosquitto.snapshot(peer) end
      )

      refute_receive {:graviton_mq, ^owner, {:delivery, ^subscription, _, _}}, 400

      assert_receive {:graviton_mq, ^owner,
                      {:delivery, ^subscription, delivery, %{topic: topic, payload: "delayed"}}},
                     3_000

      assert topic == "will/nonzero/#{@reason}"
      assert :ok = GravitonMQ.ack(instance, delivery)
      assert_peer!(peer)
    end
  end

  test "Paho receives DISCONNECT 0x9E after violating advertised Shared Subscriptions availability" do
    instance = broker()
    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    peer = peer!(port, "shared-off")
    assert assert_peer!(peer) =~ "DISCONNECT 0x9E"
  end

  test "an independent Paho peer transmits normal DISCONNECT and suppresses its Will" do
    instance =
      start_supervised!(
        {GravitonMQ,
         name: nil,
         runtime_supervisor_name: nil,
         mqtt: [
           auth: :anonymous,
           host_principal: :host,
           listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
         ]}
      )

    assert {:ok, %{owner: owner, listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    assert {:ok, operation} = GravitonMQ.subscribe(instance, "will/#", qos: 1)
    assert_receive {:graviton_mq, ^owner, {:operation, ^operation, {:ok, subscription}}}, 1_000
    assert :ok = GravitonMQ.request(instance, subscription, 1)

    python = System.fetch_env!("GRAVITON_PAHO_PYTHON")
    script = Path.expand("../support/mqtt_peer.py", __DIR__)
    peer = Mosquitto.process!(python, [script, "normal-will", Integer.to_string(port)])

    Mosquitto.eventually!(
      fn -> Mosquitto.snapshot(peer).exit_status != nil end,
      fn -> Mosquitto.snapshot(peer) end
    )

    assert %{exit_status: 0, output: output} = Mosquitto.snapshot(peer)
    assert output =~ "CONNACK, PUBACK, DISCONNECT completed"

    Mosquitto.eventually!(fn ->
      match?({:ok, %{connections: 0, sessions: 1}}, GravitonMQ.status(instance))
    end)

    refute_receive {:graviton_mq, ^owner, {:delivery, ^subscription, _, _}}, 1_500
  end

  defp broker do
    start_supervised!(
      {GravitonMQ,
       name: nil,
       runtime_supervisor_name: nil,
       mqtt: [
         auth: :anonymous,
         host_principal: :host,
         tick_interval: 10,
         listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
       ]}
    )
  end

  defp peer!(port, mode) do
    Mosquitto.process!(
      System.fetch_env!("GRAVITON_PAHO_PYTHON"),
      [Path.expand("../support/mqtt_peer.py", __DIR__), mode, to_string(port)]
    )
  end

  defp assert_peer!(peer) do
    Mosquitto.eventually!(
      fn -> Mosquitto.snapshot(peer).exit_status != nil end,
      fn -> Mosquitto.snapshot(peer) end
    )

    assert %{exit_status: 0, output: output} = Mosquitto.snapshot(peer)
    output
  end
end
