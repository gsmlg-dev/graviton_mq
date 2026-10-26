defmodule GravitonMQ.ArchitectureTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Architecture
  alias GravitonMQ.Architecture.Reference
  alias GravitonMQ.Architecture.SourceAnalyzer

  test "the declared source direction allows only the intended downward edges" do
    references = [
      ref(
        :graviton_mq,
        GravitonMQ.Supervisor,
        :graviton_mq_runtime,
        GravitonMQ.Runtime.Supervisor
      ),
      ref(:graviton_mq_runtime, Example.Runtime, :graviton_mq_core, GravitonMQ.Core.Message),
      ref(:graviton_mq_runtime, Example.Runtime, :graviton_mq_storage, GravitonMQ.Storage.Record),
      ref(:graviton_mq_runtime, Example.Runtime, :graviton_mq_amqp10, GravitonMQ.AMQP10.Value),
      ref(:graviton_mq_runtime, Example.Runtime, :graviton_mq_mqtt, GravitonMQ.MQTT.Frame),
      ref(:graviton_mq_mqtt, Example.MQTT, :graviton_mq_core, GravitonMQ.Core.Outcome),
      ref(:graviton_mq_storage, Example.Storage, :graviton_mq_core, GravitonMQ.Queue.Event),
      ref(:graviton_mq_amqp10, Example.Protocol, :graviton_mq_core, GravitonMQ.Core.Outcome)
    ]

    assert :ok = Architecture.check(references)
  end

  test "a core reference to AMQP, storage, runtime, or composition code is rejected" do
    for {target_app, target_module} <- [
          {:graviton_mq_amqp10, GravitonMQ.AMQP10.Value},
          {:graviton_mq_mqtt, GravitonMQ.MQTT.Frame},
          {:graviton_mq_storage, GravitonMQ.Storage.Record},
          {:graviton_mq_runtime, GravitonMQ.Runtime.Supervisor},
          {:graviton_mq, GravitonMQ}
        ] do
      reference = ref(:graviton_mq_core, GravitonMQ.Queue.Event, target_app, target_module)

      assert {:error, [{:forbidden_application_dependency, violation}]} =
               Architecture.check([reference])

      assert %{source_app: :graviton_mq_core, target_app: ^target_app} = violation
    end
  end

  test "storage cannot reference AMQP or runtime source" do
    references = [
      ref(
        :graviton_mq_storage,
        GravitonMQ.Storage.Record,
        :graviton_mq_amqp10,
        GravitonMQ.AMQP10.Value
      ),
      ref(
        :graviton_mq_storage,
        GravitonMQ.Storage.Record,
        :graviton_mq_runtime,
        GravitonMQ.Runtime.Supervisor
      )
    ]

    assert {:error, violations} = Architecture.check(references)
    assert Enum.count(violations, &match?({:forbidden_application_dependency, _}, &1)) == 2
  end

  test "AMQP type and codec source cannot reference socket modules" do
    for transport <- [
          :gen_sctp,
          :gen_tcp,
          :gen_udp,
          :inet,
          :prim_inet,
          :prim_socket,
          :socket,
          :ssl
        ] do
      reference = ref(:graviton_mq_amqp10, GravitonMQ.AMQP10.Value, nil, transport)

      assert {:error, [{:forbidden_transport_dependency, %{target_module: ^transport}}]} =
               Architecture.check([reference])
    end

    for transport <- [Ranch.Transport, ThousandIsland.Socket] do
      reference = ref(:graviton_mq_amqp10, GravitonMQ.AMQP10.Value, nil, transport)

      assert {:error, [{:forbidden_transport_dependency, %{target_module: ^transport}}]} =
               Architecture.check([reference])
    end
  end

  test "codec source cannot reference OTP or BEAM process modules" do
    for process_module <- [
          GenServer,
          Supervisor,
          DynamicSupervisor,
          PartitionSupervisor,
          Agent,
          Task,
          Task.Supervisor,
          Process,
          Registry
        ] do
      reference =
        ref(:graviton_mq_amqp10, GravitonMQ.AMQP10.Codec.Value, nil, process_module)

      assert {:error, [{:forbidden_process_dependency, %{target_module: ^process_module}}]} =
               Architecture.check([reference])
    end
  end

  test "process dependencies remain available outside the pure codec namespace" do
    reference =
      ref(:graviton_mq_amqp10, GravitonMQ.AMQP10.ConnectionState, nil, Process)

    assert :ok = Architecture.check([reference])
  end

  test "application cycles are rejected even when represented by otherwise arbitrary edges" do
    references = [
      ref(:graviton_mq_core, Example.Core, :graviton_mq_storage, Example.Storage),
      ref(:graviton_mq_storage, Example.Storage, :graviton_mq_core, Example.Core)
    ]

    assert {:error, violations} = Architecture.check(references)

    assert {:application_cycle, [:graviton_mq_core, :graviton_mq_storage]} in violations
  end

  test "module ownership is inferred from architectural namespaces" do
    assert :graviton_mq_core = Architecture.owner_for_module(GravitonMQ.Queue.Event)
    assert :graviton_mq_storage = Architecture.owner_for_module(GravitonMQ.Storage.Record)
    assert :graviton_mq_amqp10 = Architecture.owner_for_module(GravitonMQ.AMQP10.Value)
    assert :graviton_mq_mqtt = Architecture.owner_for_module(GravitonMQ.MQTT.Codec.Packet)
    assert :graviton_mq_runtime = Architecture.owner_for_module(GravitonMQ.Runtime.Supervisor)
    assert :graviton_mq = Architecture.owner_for_module(GravitonMQ.Application)
    assert nil == Architecture.owner_for_module(:gen_tcp)
  end

  test "parsed source analysis catches typespec-only and Erlang socket references" do
    source = """
    defmodule GravitonMQ.Core.BadBoundary do
      @type leaked :: GravitonMQ.Runtime.Supervisor.t()
      def connect(options), do: :gen_tcp.connect(~c"localhost", 5672, options)
    end
    """

    references = SourceAnalyzer.references(:graviton_mq_core, "lib/bad_boundary.ex", source)

    assert Enum.any?(references, &(&1.target_module == GravitonMQ.Runtime.Supervisor))
    assert Enum.any?(references, &(&1.target_module == :gen_tcp))
  end

  test "parsed codec source analysis catches direct process primitives" do
    for expression <- [
          "spawn(fun)",
          "spawn_link(fun)",
          "spawn_monitor(fun)",
          "send(pid, :message)",
          "receive do: (:message -> :ok)",
          "Kernel.spawn(fun)",
          ":erlang.spawn(fun)",
          "Kernel.send(pid, :message)",
          ":erlang.send(pid, :message)"
        ] do
      source = """
      defmodule GravitonMQ.AMQP10.Codec.BadProcessBoundary do
        def start(fun, pid), do: #{expression}
      end
      """

      references =
        SourceAnalyzer.references(:graviton_mq_amqp10, "lib/bad_process_boundary.ex", source)

      assert Enum.any?(references, &(&1.target_module == Process)),
             "expected to detect #{expression}"

      assert {:error, violations} = Architecture.check(references)
      assert Enum.any?(violations, &match?({:forbidden_process_dependency, _}, &1))
    end
  end

  test "MQTT depends only on core and protocol adapters never reference each other" do
    for {source_app, source_module, target_app, target_module} <- [
          {:graviton_mq_mqtt, GravitonMQ.MQTT.Frame, :graviton_mq_amqp10,
           GravitonMQ.AMQP10.Value},
          {:graviton_mq_mqtt, GravitonMQ.MQTT.Frame, :graviton_mq_storage,
           GravitonMQ.Storage.Record},
          {:graviton_mq_mqtt, GravitonMQ.MQTT.Frame, :graviton_mq_runtime,
           GravitonMQ.Runtime.Supervisor},
          {:graviton_mq_mqtt, GravitonMQ.MQTT.Frame, :graviton_mq, GravitonMQ},
          {:graviton_mq_amqp10, GravitonMQ.AMQP10.Value, :graviton_mq_mqtt,
           GravitonMQ.MQTT.Frame},
          {:graviton_mq_storage, GravitonMQ.Storage.Record, :graviton_mq_mqtt,
           GravitonMQ.MQTT.Frame}
        ] do
      assert {:error, [{:forbidden_application_dependency, _}]} =
               Architecture.check([ref(source_app, source_module, target_app, target_module)])
    end
  end

  test "all MQTT modules reject process, transport, and clock dependencies" do
    for source_module <- [GravitonMQ.MQTT.Frame, GravitonMQ.MQTT.Codec.Packet],
        {target, violation} <- [
          {Process, :forbidden_process_dependency},
          {GenServer, :forbidden_process_dependency},
          {:gen_statem, :forbidden_process_dependency},
          {:gen_tcp, :forbidden_transport_dependency},
          {Ranch.Transport, :forbidden_transport_dependency},
          {System, :forbidden_clock_dependency},
          {:timer, :forbidden_clock_dependency},
          {:os, :forbidden_clock_dependency}
        ] do
      assert {:error, [{^violation, _}]} =
               Architecture.check([ref(:graviton_mq_mqtt, source_module, nil, target)])
    end
  end

  test "parsed MQTT syntax catches typespec-only cross-protocol references" do
    source = """
    defmodule GravitonMQ.MQTT.Packet.Connect do
      @type leaked :: GravitonMQ.AMQP10.Value.t()
    end
    """

    references = SourceAnalyzer.references(:graviton_mq_mqtt, "lib/connect.ex", source)
    assert {:error, [{:forbidden_application_dependency, _}]} = Architecture.check(references)
  end

  test "parsed MQTT syntax catches direct Erlang process and clock primitives" do
    for {expression, kind} <- [
          {"spawn(fun)", :forbidden_process_dependency},
          {":erlang.spawn_opt(fun, [])", :forbidden_process_dependency},
          {":erlang.send_after(1, pid, :tick)", :forbidden_process_dependency},
          {":erlang.self()", :forbidden_process_dependency},
          {":erlang.monotonic_time()", :forbidden_clock_dependency},
          {":erlang.system_time(:millisecond)", :forbidden_clock_dependency},
          {":erlang.timestamp()", :forbidden_clock_dependency}
        ] do
      source = """
      defmodule GravitonMQ.MQTT.Frame do
        def impure(fun, pid), do: #{expression}
      end
      """

      references = SourceAnalyzer.references(:graviton_mq_mqtt, "lib/frame.ex", source)
      assert {:error, violations} = Architecture.check(references)
      assert Enum.any?(violations, fn {violation, _} -> violation == kind end), expression
    end
  end

  defp ref(source_app, source_module, target_app, target_module) do
    %Reference{
      source_app: source_app,
      source_module: source_module,
      target_app: target_app,
      target_module: target_module,
      source_file: "test/source.ex",
      line: 1,
      kind: :runtime
    }
  end
end
