Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)
Code.require_file("../../../graviton_mq_client/test/support/mqtt_wire_tap.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.FeatureMosquittoTest do
  use ExUnit.Case, async: false
  @moduletag :interop
  @moduletag timeout: 60_000
  alias GravitonMQ.Client.Test.{Mosquitto, MQTTWireTap}

  setup_all do
    %{executables: Mosquitto.executables!()}
  end

  defp broker do
    instance =
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

    {:ok, %{owner: owner, listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    {instance, owner, port}
  end

  test "two mosquitto_sub members split a shared group evenly", context do
    {instance, _owner, port} = broker()

    members =
      for name <- ["mosq-share-1", "mosq-share-2"] do
        Mosquitto.process!(
          context.executables.subscriber,
          Mosquitto.cli_args(%{port: port}) ++
            ["-i", name, "-t", "$share/g/m/#", "-q", "1", "-C", "3", "-W", "10", "-F", "%t|%p"]
        )
      end

    Mosquitto.eventually!(fn ->
      match?({:ok, %{shared_groups: 1, sessions: 2}}, GravitonMQ.status(instance))
    end)

    for index <- 1..6 do
      assert {:ok, _} = GravitonMQ.publish(instance, "m/#{index}", "p#{index}", qos: 1)
    end

    outputs =
      for member <- members do
        Mosquitto.eventually!(
          fn -> Mosquitto.snapshot(member).exit_status != nil end,
          fn -> Mosquitto.snapshot(member) end,
          10_000
        )

        assert %{exit_status: 0, output: output} = Mosquitto.snapshot(member)
        output |> String.split("\n", trim: true) |> Enum.filter(&String.starts_with?(&1, "m/"))
      end

    assert Enum.map(outputs, &length/1) == [3, 3]
    assert Enum.sort(List.flatten(outputs)) == Enum.map(1..6, &"m/#{&1}|p#{&1}")
  end

  test "mosquitto_pub topic-alias publications reach a host subscriber with full topics",
       context do
    {instance, owner, port} = broker()
    tap = start_supervised!({MQTTWireTap, upstream_port: port, observer: self()})
    {:ok, operation} = GravitonMQ.subscribe(instance, "a/#", qos: 1)
    assert_receive {:graviton_mq, ^owner, {:operation, ^operation, {:ok, subscription}}}, 2_000
    :ok = GravitonMQ.request(instance, subscription, 3)

    Mosquitto.command!(
      context.executables.publisher,
      Mosquitto.cli_args(%{port: MQTTWireTap.port(tap)}) ++
        ["-t", "a/m", "-m", "x", "-q", "1", "--repeat", "3", "--repeat-delay", "0"] ++
        ["-D", "publish", "topic-alias", "1"]
    )

    for _ <- 1..3 do
      assert_receive {:graviton_mq, ^owner,
                      {:delivery, ^subscription, delivery, %{topic: "a/m"}}},
                     3_000

      :ok = GravitonMQ.ack(instance, delivery)
    end

    assert Enum.any?(
             MQTTWireTap.observed(tap),
             &(&1.direction == :to_server and &1.topic == "" and &1.topic_alias == 1)
           )
  end
end
