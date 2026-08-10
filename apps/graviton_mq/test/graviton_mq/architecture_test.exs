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
      ref(:graviton_mq_storage, Example.Storage, :graviton_mq_core, GravitonMQ.Queue.Event),
      ref(:graviton_mq_amqp10, Example.Protocol, :graviton_mq_core, GravitonMQ.Core.Outcome)
    ]

    assert :ok = Architecture.check(references)
  end

  test "a core reference to AMQP, storage, runtime, or composition code is rejected" do
    for {target_app, target_module} <- [
          {:graviton_mq_amqp10, GravitonMQ.AMQP10.Value},
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
