Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.MetadataPahoTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.Client.Test.Mosquitto, as: Peer

  @moduletag :paho_interop
  @moduletag :capture_log
  @moduletag timeout: 20_000

  setup do
    parent = Peer.directory!()
    on_exit(fn -> File.rm_rf!(parent) end)
    %{parent: parent}
  end

  test "independent Paho peers retain exact QoS1/2 metadata through live broker routing" do
    instance = broker!()
    %{port: port} = status(instance)
    receiver = peer!("receive", port, "metadata/live")
    Peer.eventually!(fn -> Peer.snapshot(receiver).output =~ "READY" end)
    publisher = peer!("publish", port, "metadata/live")
    completed!(publisher)
    assert completed!(receiver) =~ "00ff80002a7e"
    drained!(instance)
  end

  test "independent retained replay preserves metadata and reduces expiry" do
    instance = broker!()
    %{port: port} = status(instance)
    completed!(peer!("publish", port, "metadata/retained", ["--retained"]))
    assert status(instance).retained == 2
    Process.sleep(1_200)

    completed!(
      peer!("receive", port, "metadata/retained", ["--retained", "--expiry-upper", "59"])
    )

    drained!(instance)
  end

  test "independent retained replay preserves metadata after durable broker owner kill",
       context do
    instance =
      broker!(persistence: [directory: Path.join(context.parent, "store"), stream_id: "metadata"])

    %{port: port, owner: original} = status(instance)
    completed!(peer!("publish", port, "metadata/recovered", ["--retained"]))
    assert status(instance).retained == 2
    drained!(instance)
    monitor = Process.monitor(original)
    Process.exit(original, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^original, :killed}, 2_000

    Peer.eventually!(fn ->
      match?(
        {:ok, %{owner: replacement}} when replacement != original,
        GravitonMQ.status(instance)
      )
    end)

    assert %{port: restarted_port, profile: :local_durable, storage: :ready, retained: 2} =
             status(instance)

    Process.sleep(1_200)

    completed!(
      peer!("receive", restarted_port, "metadata/recovered", [
        "--retained",
        "--expiry-upper",
        "59"
      ])
    )

    drained!(instance)
  end

  defp broker!(options \\ []) do
    mqtt =
      Keyword.merge(
        [auth: :anonymous, listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]],
        options
      )

    start_supervised!({GravitonMQ, name: nil, runtime_supervisor_name: nil, mqtt: mqtt})
  end

  defp status(instance) do
    {:ok, %{listeners: [%{port: port}]} = status} = GravitonMQ.status(instance)
    Map.put(status, :port, port)
  end

  defp drained!(instance) do
    Peer.eventually!(fn ->
      match?({:ok, %{connections: 0, pending_deliveries: 0}}, GravitonMQ.status(instance))
    end)
  end

  defp peer!(mode, port, prefix, options \\ []) do
    Peer.process!(System.fetch_env!("GRAVITON_PAHO_PYTHON"), [
      Path.expand("../../../graviton_mq_client/test/support/mqtt_metadata_peer.py", __DIR__),
      mode,
      Integer.to_string(port),
      prefix
      | options
    ])
  end

  defp completed!(peer) do
    Peer.eventually!(fn -> Peer.snapshot(peer).exit_status != nil end, fn ->
      Peer.snapshot(peer)
    end)

    assert %{exit_status: 0, output: output} = Peer.snapshot(peer)
    assert output =~ "PASS "
    output
  end
end
