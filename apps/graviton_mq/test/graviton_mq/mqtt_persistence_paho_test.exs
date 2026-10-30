Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.PersistencePahoTest do
  use ExUnit.Case, async: false
  @moduletag :paho_interop
  @moduletag :capture_log
  @moduletag timeout: 30_000

  alias GravitonMQ.Client.Test.{Mosquitto, PeerProcess}

  setup do
    parent =
      Path.join(
        System.tmp_dir!(),
        "graviton-mqtt-peer-recovery-#{System.unique_integer([:positive])}"
      )

    File.mkdir!(parent)
    on_exit(fn -> File.rm_rf!(parent) end)
    %{path: Path.join(parent, "broker"), parent: parent}
  end

  test "independent Paho resumes stored QoS1/2 subscriptions and offline deliveries after broker owner kill",
       context do
    instance =
      start_supervised!(
        {GravitonMQ,
         name: nil,
         runtime_supervisor_name: nil,
         mqtt: [
           auth: :anonymous,
           host_principal: :host,
           persistence: [directory: context.path, stream_id: "paho-owner"],
           listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
         ]}
      )

    {:ok, %{owner: original, listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    assert peer!(port, "seed") =~ "\"present\": false"
    publish(instance, "persist/one", "q1", 1)
    publish(instance, "persist/two", "q2", 2)
    monitor = Process.monitor(original)
    Process.exit(original, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^original, :killed}, 2_000

    Mosquitto.eventually!(fn ->
      match?(
        {:ok, %{owner: replacement}} when replacement != original,
        GravitonMQ.status(instance)
      )
    end)

    {:ok, %{listeners: [%{port: restarted_port}]}} = GravitonMQ.status(instance)
    assert peer!(restarted_port, "receive") =~ "\"present\": true"

    Mosquitto.eventually!(fn ->
      match?({:ok, %{pending_deliveries: 0}}, GravitonMQ.status(instance))
    end)
  end

  test "independent Paho resumes subscriptions and exact offline content after BEAM SIGKILL",
       context do
    {vm, pid, port} = vm!(context.path)
    assert peer!(port, "seed") =~ "\"present\": false"

    for {qos, topic, body} <- [{1, "persist/one", "q1"}, {2, "persist/two", "q2"}] do
      assert PeerProcess.command(vm, "PUBLISH #{qos} #{topic} #{body}\n")
      Mosquitto.eventually!(fn -> Mosquitto.snapshot(vm).output =~ "DURABLE #{topic} " end)
    end

    {_, 0} = System.cmd(System.find_executable("kill"), ["-KILL", pid])
    Mosquitto.eventually!(fn -> Mosquitto.snapshot(vm).exit_status == 137 end)
    {_restarted, _pid, restarted_port} = vm!(context.path)
    output = peer!(restarted_port, "receive")
    assert output =~ "\"present\": true"
    assert output =~ "persist/one" and output =~ "persist/two"
  end

  test "Paho-observed session, retained and scheduled Will expiry count broker downtime",
       context do
    options = [
      name: nil,
      runtime_supervisor_name: nil,
      mqtt: [
        auth: :anonymous,
        host_principal: :host,
        persistence: [directory: context.path, stream_id: "paho-expiry"],
        listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
      ]
    ]

    instance = start_supervised!({GravitonMQ, options})
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    assert peer!(port, "seed-expiry") =~ "PASS expiry seeded"

    Mosquitto.eventually!(fn ->
      match?({:ok, %{connections: 0, sessions: 1}}, GravitonMQ.status(instance))
    end)

    assert :ok = stop_supervised({GravitonMQ, nil})
    Process.sleep(2_200)
    restarted = start_supervised!({GravitonMQ, options})
    {:ok, %{listeners: [%{port: restarted_port}], sessions: 0}} = GravitonMQ.status(restarted)
    assert peer!(restarted_port, "check-expiry") =~ "will/state"
  end

  for mode <- [
        "send-before-pubrec",
        "send-before-pubrel",
        "send-before-pubcomp",
        "receive-before-pubrec",
        "receive-before-pubcomp"
      ] do
    @mode mode
    test "broker owner crash preserves Paho QoS2 #{@mode} responsibility", context do
      instance =
        start_supervised!(
          {GravitonMQ,
           name: nil,
           runtime_supervisor_name: nil,
           mqtt: [
             auth: :anonymous,
             host_principal: :host,
             persistence: [directory: context.path, stream_id: "paho-qos2"],
             listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
           ]}
        )

      {:ok, %{owner: original, listeners: [%{port: port}]}} = GravitonMQ.status(instance)
      if String.starts_with?(@mode, "send-"), do: peer!(port, "seed-qos2")
      control = Path.join(context.parent, "reconnect-port")

      peer =
        Mosquitto.process!(
          System.fetch_env!("GRAVITON_PAHO_PYTHON"),
          [
            Path.expand("../support/mqtt_persistence_qos2_peer.py", __DIR__),
            @mode,
            Integer.to_string(port),
            control
          ]
        )

      if String.starts_with?(@mode, "receive-") do
        Mosquitto.eventually!(fn -> Mosquitto.snapshot(peer).output =~ "SUBSCRIBED" end)
        publish(instance, "qos2/value", "once", 2)
      end

      Mosquitto.eventually!(fn -> Mosquitto.snapshot(peer).output =~ "CRASH_POINT " end, fn ->
        Mosquitto.snapshot(peer)
      end)

      monitor = Process.monitor(original)
      Process.exit(original, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^original, :killed}, 2_000

      Mosquitto.eventually!(fn ->
        match?(
          {:ok, %{owner: replacement}} when replacement != original,
          GravitonMQ.status(instance)
        )
      end)

      {:ok, %{listeners: [%{port: restarted_port}]}} = GravitonMQ.status(instance)
      File.write!(control, Integer.to_string(restarted_port))

      Mosquitto.eventually!(fn -> Mosquitto.snapshot(peer).exit_status != nil end, fn ->
        Mosquitto.snapshot(peer)
      end)

      assert %{exit_status: 0, output: output} = Mosquitto.snapshot(peer)
      assert output =~ "PASS " and output =~ "\"connections\": [false, true]"

      if String.starts_with?(@mode, "send-") do
        assert output =~ "\"reason\": #{if @mode == "send-before-pubcomp", do: 146, else: 0}"
        assert peer!(restarted_port, "receive-qos2") =~ "once"
      else
        assert output =~ "\"received\": 1"
      end

      Mosquitto.eventually!(fn ->
        match?({:ok, %{pending_deliveries: 0}}, GravitonMQ.status(instance))
      end)
    end
  end

  defp peer!(port, mode) do
    Mosquitto.command!(
      System.fetch_env!("GRAVITON_PAHO_PYTHON"),
      [Path.expand("../support/mqtt_persistence_peer.py", __DIR__), mode, Integer.to_string(port)]
    )
  end

  defp publish(instance, topic, body, qos) do
    {:ok, reference} = GravitonMQ.publish(instance, topic, body, qos: qos)

    assert_receive {:graviton_mq, _, {:operation, ^reference, {:ok, {:accepted_durable, _}}}},
                   3_000
  end

  defp vm!(path) do
    paths =
      [
        :graviton_mq_core,
        :graviton_mq_amqp10,
        :graviton_mq_mqtt,
        :graviton_mq_storage,
        :graviton_mq_runtime,
        :graviton_mq
      ]
      |> Enum.flat_map(fn app ->
        ["-pa", :code.lib_dir(app) |> List.to_string() |> Path.join("ebin")]
      end)

    vm =
      Mosquitto.process!(
        System.find_executable("elixir"),
        paths ++ [Path.expand("../support/mqtt_persistence_vm.exs", __DIR__), path]
      )

    Mosquitto.eventually!(fn -> Mosquitto.snapshot(vm).output =~ "READY " end, fn ->
      Mosquitto.snapshot(vm)
    end)

    [_, pid, port] = Regex.run(~r/READY (\d+) (\d+)/, Mosquitto.snapshot(vm).output)
    {vm, pid, String.to_integer(port)}
  end
end
