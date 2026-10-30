Code.require_file("../../../graviton_mq_client/test/support/peer_process.exs", __DIR__)

defmodule GravitonMQ.AMQP10.PersistenceProtonTest do
  use ExUnit.Case, async: false
  @moduletag :proton_interop
  @moduletag :tmp_dir
  @moduletag :capture_log
  @moduletag timeout: 40_000

  alias GravitonMQ.Client.Test.PeerProcess

  @peer Path.expand("../support/amqp_durable_peer.py", __DIR__)

  setup context do
    {:ok, Map.put(context, :python, System.fetch_env!("GRAVITON_PROTON_PYTHON"))}
  end

  test "Proton durable Accepted is backed by actual queue recovery and retirement", context do
    options = options(context)
    {broker, id} = broker!(options)
    path = body(context, <<0, 255, 128, 17>>)
    assert {_, 0} = peer(context, "send", url(broker), path)
    assert queue(broker).messages == 1
    assert queue(broker).durable_through != :none
    assert :ok = stop_supervised(id)
    {restored, id} = broker!(options)
    assert {output, 0} = peer(context, "receive", url(restored), path)
    assert output =~ digest(File.read!(path))
    eventually(fn -> queue(restored).messages == 0 end)
    assert :ok = stop_supervised(id)
    {empty, _} = broker!(options)
    assert queue(empty).messages == 0
  end

  test "failed synchronization closes publication without Proton Accepted", context do
    control = :ets.new(:amqp_sync_fault, [:public])

    fault = fn
      :wal_sync, _ ->
        case :ets.take(control, :armed) do
          [{:armed, value}] -> value
          [] -> :ok
        end

      _, _ ->
        :ok
    end

    {broker, _} = broker!(options(context, fault_injector: fault))
    path = body(context, "no-accepted-on-failed-sync")
    :ets.insert(control, {:armed, {:error, :eio}})
    {output, status} = peer(context, "send", url(broker), path)
    assert status != 0, output
    refute output =~ "\"accepted\": true"
    assert queue(broker).storage == :failed
  end

  test "the exact noncanonical message corpus survives WAL replay and durable retirement",
       context do
    options = options(context)
    {broker, id} = broker!(options)
    corpus = Path.expand("../support/amqp10_message_peer.py", __DIR__)

    {output, code} =
      System.cmd(
        context.python,
        [corpus, url(broker), "send-corpus", "--address", "orders", "--durable"],
        stderr_to_stdout: true
      )

    assert code == 0, output
    assert output =~ "\"sent_fixtures\": 12"
    assert queue(broker).messages == 12
    assert queue(broker).durable_through != :none
    assert :ok = stop_supervised(id)
    {restored, id} = broker!(options)
    assert queue(restored).messages == 12

    {output, code} =
      System.cmd(
        context.python,
        [corpus, url(restored), "receive-corpus", "--address", "orders", "--durable"],
        stderr_to_stdout: true
      )

    assert code == 0, output
    assert output =~ "\"received_fixtures\": 12"
    eventually(fn -> queue(restored).messages == 0 end)
    assert :ok = stop_supervised(id)
    {empty, _} = broker!(options)
    assert queue(empty).messages == 0
  end

  test "Modified annotations and rejection responsibility survive broker restart", context do
    options = options(context)
    {broker, id} = broker!(options)
    path = body(context, "modified-durable-content")
    modifier = receiver!(context, "modify", url(broker), path)
    assert {_, 0} = peer(context, "send", url(broker), path)
    eventually(fn -> PeerProcess.snapshot(modifier).output =~ "\"modified\": true" end)
    eventually(fn -> queue(broker).ready == 1 end)
    assert :ok = stop_supervised(id)
    {restored, id} = broker!(options)
    assert {output, 0} = peer(context, "receive", url(restored), path, ["--modified"])
    assert output =~ digest(File.read!(path))
    eventually(fn -> queue(restored).messages == 0 end)
    assert {_, 0} = peer(context, "send", url(restored), path)
    assert {_, 0} = peer(context, "reject", url(restored), path)
    eventually(fn -> queue(restored).messages == 0 end)
    assert :ok = stop_supervised(id)
    {empty, _} = broker!(options)
    assert queue(empty).messages == 0
  end

  test "whole BEAM SIGKILL restores a Proton receiver's unsettled delivery", context do
    options = options(context)

    script = """
    Application.ensure_all_started(:graviton_mq)
    {:ok, broker} = GravitonMQ.start_link(#{inspect(options)})
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    IO.puts("AMQP_PORT=" <> Integer.to_string(port))
    case IO.gets("") do
      "crash\\n" -> System.cmd("kill", ["-KILL", System.pid()])
      _ -> :ok
    end
    """

    paths =
      [
        :graviton_mq_core,
        :graviton_mq_storage,
        :graviton_mq_amqp10,
        :graviton_mq_mqtt,
        :graviton_mq_runtime,
        :graviton_mq
      ]
      |> Enum.flat_map(fn app ->
        ["-pa", Path.join(List.to_string(:code.lib_dir(app)), "ebin")]
      end)

    beam =
      start_supervised!(
        {PeerProcess,
         executable: System.find_executable("elixir"),
         args: paths ++ ["--erl", "+S 2:2", "-e", script]},
        id: make_ref()
      )

    beam_url = beam_url(beam)
    path = body(context, "unsettled-across-real-vm-death")
    receiver = receiver!(context, "hold", beam_url, path)
    assert {_, 0} = peer(context, "send", beam_url, path)
    eventually(fn -> PeerProcess.snapshot(receiver).output =~ "\"held\": true" end)
    assert PeerProcess.command(beam, "crash\n")
    eventually(fn -> PeerProcess.snapshot(beam).exit_status != nil end)
    assert PeerProcess.snapshot(beam).exit_status == 137
    assert :ok = PeerProcess.restart(beam)
    assert {output, 0} = peer(context, "receive", beam_url(beam), path)
    assert output =~ digest(File.read!(path))
  end

  test "Proton observes no Accepted while the actual WAL synchronization is gated", context do
    control = :ets.new(:sync_gate, [:public])
    parent = self()
    token = make_ref()

    fault = fn
      :wal_sync, _ ->
        case :ets.take(control, :armed) do
          [{:armed, true}] ->
            send(parent, {:waiting_for_sync, self(), token})

            receive do
              {:release_sync, ^token} -> :ok
            after
              3000 -> {:error, :gate_timeout}
            end

          [] ->
            :ok
        end

      _, _ ->
        :ok
    end

    {broker, _} = broker!(options(context, fault_injector: fault))
    path = body(context, "ack-requires-synchronized-boundary")
    :ets.insert(control, {:armed, true})

    sender =
      start_supervised!(
        {PeerProcess,
         executable: context.python, args: [@peer, "send", url(broker), "orders", path]},
        id: make_ref()
      )

    assert_receive {:waiting_for_sync, wal, ^token}, 3000
    refute PeerProcess.snapshot(sender).output =~ "\"accepted\": true"
    assert PeerProcess.snapshot(sender).exit_status == nil
    send(wal, {:release_sync, token})
    eventually(fn -> PeerProcess.snapshot(sender).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(sender)
    assert output =~ "\"accepted\": true"
    assert queue(broker).durable_through != :none
  end

  test "killing the supervised queue restores accepted content before admitting a new receiver",
       context do
    {broker, _} = broker!(options(context))
    path = body(context, "owner-kill-recovery")
    assert {_, 0} = peer(context, "send", url(broker), path)
    old = queue_owner(broker)
    Process.exit(old, :kill)

    eventually(fn ->
      case GravitonMQ.amqp10_status(broker) do
        {:ok, %{queues: %{"orders" => %{messages: 1}}, listeners: [_]}} ->
          queue_owner(broker) != old

        _ ->
          false
      end
    end)

    assert {output, 0} = peer(context, "receive", url(broker), path)
    assert output =~ digest(File.read!(path))
  end

  test "one broker owns the base directory across disjoint queue configurations", context do
    options = options(context)
    {broker, _} = broker!(options)
    other = update_in(options[:amqp10], &Keyword.put(&1, :queues, ["different-address"]))

    assert {:error, _} =
             start_supervised(Supervisor.child_spec({GravitonMQ, other}, id: make_ref()))

    assert queue(broker).profile == :local_durable
  end

  defp options(context, extra \\ []) do
    [
      name: nil,
      runtime_supervisor_name: nil,
      amqp10: [
        auth: :anonymous,
        container_id: "persistent-proton-broker",
        queues: ["orders"],
        persistence:
          [directory: Path.join(context.tmp_dir, "broker"), stream_id: "stable-proton"] ++ extra,
        listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
      ]
    ]
  end

  defp broker!(options) do
    id = make_ref()
    {start_supervised!(Supervisor.child_spec({GravitonMQ, options}, id: id)), id}
  end

  defp queue_owner(broker) do
    child = fn parent, id ->
      {^id, pid, _, _} = List.keyfind(Supervisor.which_children(parent), id, 0)
      pid
    end

    runtime = child.(broker, GravitonMQ.Runtime.Supervisor)
    amqp = child.(runtime, GravitonMQ.Runtime.AMQP10.Supervisor)
    child.(amqp, {GravitonMQ.Runtime.AMQP10.Queue, "orders"})
  end

  defp queue(broker) do
    {:ok, %{queues: %{"orders" => status}}} = GravitonMQ.amqp10_status(broker)
    status
  end

  defp url(broker) do
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    "amqp://127.0.0.1:#{port}"
  end

  defp peer(context, operation, url, path, extra \\ []),
    do:
      System.cmd(context.python, [@peer, operation, url, "orders", path] ++ extra,
        stderr_to_stdout: true
      )

  defp receiver!(context, operation, url, path) do
    receiver =
      start_supervised!(
        {PeerProcess, executable: context.python, args: [@peer, operation, url, "orders", path]},
        id: make_ref()
      )

    eventually(fn ->
      snapshot = PeerProcess.snapshot(receiver)
      if snapshot.exit_status, do: flunk(snapshot.output)
      snapshot.output =~ "\"ready\": true"
    end)

    receiver
  end

  defp beam_url(beam) do
    eventually(fn -> PeerProcess.snapshot(beam).output =~ "AMQP_PORT=" end)
    [_, port] = Regex.run(~r/AMQP_PORT=(\d+)/, PeerProcess.snapshot(beam).output)
    "amqp://127.0.0.1:#{port}"
  end

  defp body(context, bytes) do
    path = Path.join(context.tmp_dir, "body-#{System.unique_integer([:positive])}")
    File.write!(path, bytes)
    path
  end

  defp digest(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
  defp eventually(predicate), do: until(predicate, System.monotonic_time(:millisecond) + 10_000)

  defp until(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline,
             "independent durable peer condition timed out"

      Process.sleep(10)
      until(predicate, deadline)
    end
  end
end
