Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.QoS2PahoTest do
  use ExUnit.Case, async: false
  @moduletag :paho_interop
  @moduletag timeout: 20_000
  alias GravitonMQ.Client.Test.Mosquitto, as: Peer

  setup do
    instance =
      start_supervised!(
        {GravitonMQ,
         name: nil,
         runtime_supervisor_name: nil,
         mqtt: [
           auth: :anonymous,
           host_principal: :host,
           maximum_qos: 2,
           listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
         ]}
      )

    assert {:ok, %{owner: owner, listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    %{instance: instance, owner: owner, port: port}
  end

  for mode <- ["send-before-pubrec", "send-before-pubrel", "send-before-pubcomp"] do
    @mode mode
    test "independent publisher resumes #{@mode} without duplicate onward delivery", context do
      assert {:ok, op} = GravitonMQ.subscribe(context.instance, "qos2/#", qos: 2)
      assert_receive {:graviton_mq, _, {:operation, ^op, {:ok, subscription}}}, 1_000
      assert :ok = GravitonMQ.request(context.instance, subscription, 2)
      process = peer(context.port, @mode)

      assert_receive {:graviton_mq, owner,
                      {:delivery, ^subscription, delivery, %{payload: "once", qos: 2}}},
                     5_000

      assert owner == context.owner
      assert :ok = GravitonMQ.ack(context.instance, delivery)
      output = completed(process)
      assert output =~ "\"connections\": [false, true]"
      assert output =~ "\"losses\": 1"
      assert output =~ "\"reason\": #{if @mode == "send-before-pubcomp", do: 146, else: 0}"
      refute_receive {:graviton_mq, ^owner, {:delivery, ^subscription, _, _}}, 100
      assert {:ok, %{pending_deliveries: 0}} = GravitonMQ.status(context.instance)
    end
  end

  for mode <- ["receive-before-pubrec", "receive-before-pubcomp"] do
    @mode mode
    test "independent receiver resumes #{@mode} and observes the message once", context do
      process = peer(context.port, @mode)

      Peer.eventually!(
        fn -> Peer.snapshot(process).output =~ "SUBSCRIBED" end,
        fn -> Peer.snapshot(process) end
      )

      assert {:ok, op} = GravitonMQ.publish(context.instance, "qos2/value", "once", qos: 2)
      assert_receive {:graviton_mq, _, {:operation, ^op, {:ok, :accepted_in_memory}}}, 1_000
      output = completed(process)
      assert output =~ "\"connections\": [false, true]"
      assert output =~ "\"received\": 1"
      assert output =~ "\"losses\": 1"

      Peer.eventually!(fn ->
        match?({:ok, %{pending_deliveries: 0}}, GravitonMQ.status(context.instance))
      end)
    end
  end

  test "resumed PUBREL progresses before held QoS1 exhausts a smaller receive quota", context do
    process =
      Peer.process!(
        System.fetch_env!("GRAVITON_PAHO_PYTHON"),
        [
          Path.expand("../support/mqtt_pubrel_quota_peer.py", __DIR__),
          Integer.to_string(context.port)
        ]
      )

    Peer.eventually!(
      fn -> Peer.snapshot(process).output =~ "SUBSCRIBED" end,
      fn -> Peer.snapshot(process) end
    )

    for {topic, payload, qos} <- [{"one", "one", 1}, {"two", "two", 1}, {"three", "three", 2}] do
      assert {:ok, operation} =
               GravitonMQ.publish(context.instance, "qos2/quota/" <> topic, payload, qos: qos)

      assert_receive {:graviton_mq, _, {:operation, ^operation, {:ok, :accepted_in_memory}}}, 1000
    end

    output = completed(process)
    assert output =~ "\"connections\": [false, true]"
    assert output =~ "\"qos1_acks\": 2"
    assert output =~ "\"unique_messages\": 3"
    assert output =~ "\"resumed_receive_maximum\": 1"

    Peer.eventually!(fn ->
      match?({:ok, %{pending_deliveries: 0}}, GravitonMQ.status(context.instance))
    end)
  end

  test "two pending PUBRELs resume in order without repeating PUBLISH", context do
    process =
      Peer.process!(
        System.fetch_env!("GRAVITON_PAHO_PYTHON"),
        [
          Path.expand("../support/mqtt_pubrel_order_peer.py", __DIR__),
          Integer.to_string(context.port)
        ]
      )

    Peer.eventually!(
      fn -> Peer.snapshot(process).output =~ "SUBSCRIBED" end,
      fn -> Peer.snapshot(process) end
    )

    for payload <- ["first", "second"] do
      assert {:ok, operation} =
               GravitonMQ.publish(context.instance, "qos2/order", payload, qos: 2)

      assert_receive {:graviton_mq, _, {:operation, ^operation, {:ok, :accepted_in_memory}}}, 1000
    end

    output = completed(process)
    assert output =~ "\"connections\": [false, true]"
    assert output =~ "\"delivered\": [\"first\", \"second\"]"
    assert output =~ "\"pubcomp\": 2"
    assert output =~ "\"resumed_receive_maximum\": 1"

    Peer.eventually!(fn ->
      match?({:ok, %{pending_deliveries: 0}}, GravitonMQ.status(context.instance))
    end)
  end

  defp peer(port, mode) do
    Peer.process!(
      System.fetch_env!("GRAVITON_PAHO_PYTHON"),
      [Path.expand("../support/mqtt_qos2_peer.py", __DIR__), mode, Integer.to_string(port)]
    )
  end

  defp completed(process) do
    Peer.eventually!(fn -> Peer.snapshot(process).exit_status != nil end, fn ->
      Peer.snapshot(process)
    end)

    assert %{exit_status: 0, output: output} = Peer.snapshot(process)
    assert output =~ "PASS "
    output
  end
end
