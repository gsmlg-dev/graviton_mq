defmodule GravitonMQ.ArchitectureTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Architecture
  alias GravitonMQ.Architecture.Reference
  alias GravitonMQ.Architecture.SourceAnalyzer

  test "relative aliases resolve in the current and nested module scopes" do
    source = """
    defmodule GravitonMQ.Client.MQTT.Store do
      alias __MODULE__.IO, as: LocalIO
      @type adapter :: __MODULE__.IO.t()
      defmodule Nested do
        @type adapter :: __MODULE__.IO.t()
      end
    end
    """

    references = SourceAnalyzer.references(:graviton_mq_client, "lib/store.ex", source)

    assert Enum.any?(
             references,
             &(&1.source_module == GravitonMQ.Client.MQTT.Store and
                 &1.target_module == GravitonMQ.Client.MQTT.Store.IO)
           )

    assert Enum.any?(
             references,
             &(&1.source_module == GravitonMQ.Client.MQTT.Store.Nested and
                 &1.target_module == GravitonMQ.Client.MQTT.Store.Nested.IO)
           )

    assert :ok = Architecture.check(references)
  end

  test "relative aliases retain application boundary checks" do
    source = """
    defmodule GravitonMQ.AMQP10 do
      @type transport :: __MODULE__.Transport.t()
    end
    """

    references = SourceAnalyzer.references(:graviton_mq_core, "lib/bad_boundary.ex", source)
    assert Enum.any?(references, &(&1.target_module == GravitonMQ.AMQP10.Transport))
    assert {:error, violations} = Architecture.check(references)
    assert Enum.any?(violations, &match?({:forbidden_application_dependency, _}, &1))
  end

  test "independent client namespace and downward protocol edges are owned by the client app" do
    for module <- [
          GravitonMQ.Client.MQTT.Connection,
          GravitonMQ.MQTTClient,
          GravitonMQ.AMQP10Client
        ] do
      assert :graviton_mq_client == Architecture.owner_for_module(module)
    end

    for {target_app, module} <- [
          {:graviton_mq_core, GravitonMQ.Core.Message},
          {:graviton_mq_mqtt, GravitonMQ.MQTT.Client},
          {:graviton_mq_amqp10, GravitonMQ.AMQP10.ConnectionState}
        ] do
      assert :ok =
               Architecture.check([
                 ref(:graviton_mq_client, GravitonMQ.MQTTClient, target_app, module)
               ])
    end
  end

  test "clients cannot import broker runtime storage or product even in typespecs" do
    for module <- [GravitonMQ.Runtime.Supervisor, GravitonMQ.Storage.Record, GravitonMQ] do
      source = """
      defmodule GravitonMQ.Client.BadBoundary do
        @type leaked :: #{inspect(module)}.t()
      end
      """

      references = SourceAnalyzer.references(:graviton_mq_client, "lib/client.ex", source)
      assert {:error, violations} = Architecture.check(references)
      assert Enum.any?(violations, &match?({:forbidden_application_dependency, _}, &1))
    end
  end

  test "protocol and broker apps cannot depend on outgoing clients" do
    for source <- [
          :graviton_mq_core,
          :graviton_mq_storage,
          :graviton_mq_amqp10,
          :graviton_mq_mqtt,
          :graviton_mq_runtime,
          :graviton_mq
        ] do
      assert {:error, [{:forbidden_application_dependency, _}]} =
               Architecture.check([
                 ref(source, Example.Source, :graviton_mq_client, GravitonMQ.MQTTClient)
               ])
    end
  end

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

  test "the existing codec namespace safeguard remains active for a misplaced source module" do
    reference = ref(:graviton_mq_runtime, GravitonMQ.AMQP10.Codec.Value, nil, Process)

    assert {:error, [{:forbidden_process_dependency, _}]} = Architecture.check([reference])
  end

  test "running client and broker owners may use process and clock facilities" do
    for {app, module} <- [
          {:graviton_mq_client, GravitonMQ.Client.AMQP10.Connection},
          {:graviton_mq_runtime, GravitonMQ.Runtime.AMQP10.Connection}
        ] do
      source = """
      defmodule #{inspect(module)} do
        def schedule(pid) do
          Process.send_after(pid, :tick, 1)
          System.monotonic_time(:millisecond)
          :erlang.system_time(:millisecond)
          Kernel.self()
        end
      end
      """

      references = SourceAnalyzer.references(app, "lib/connection.ex", source)
      assert Enum.any?(references, &(&1.target_module == Process))
      assert Enum.any?(references, &(&1.target_module == System))
      assert :ok = Architecture.check(references)
    end
  end

  test "all AMQP protocol modules reject process and clock dependencies" do
    for source_module <- [
          GravitonMQ.AMQP10,
          GravitonMQ.AMQP10.ConnectionState,
          GravitonMQ.AMQP10.Endpoint,
          GravitonMQ.AMQP10.Codec.Value
        ],
        {target, violation} <- [
          {Process, :forbidden_process_dependency},
          {GenServer, :forbidden_process_dependency},
          {Supervisor, :forbidden_process_dependency},
          {DynamicSupervisor, :forbidden_process_dependency},
          {PartitionSupervisor, :forbidden_process_dependency},
          {Agent, :forbidden_process_dependency},
          {Task, :forbidden_process_dependency},
          {Task.Supervisor, :forbidden_process_dependency},
          {Registry, :forbidden_process_dependency},
          {:gen_server, :forbidden_process_dependency},
          {:gen_statem, :forbidden_process_dependency},
          {:supervisor, :forbidden_process_dependency},
          {:proc_lib, :forbidden_process_dependency},
          {System, :forbidden_clock_dependency},
          {DateTime, :forbidden_clock_dependency},
          {NaiveDateTime, :forbidden_clock_dependency},
          {Time, :forbidden_clock_dependency},
          {Date, :forbidden_clock_dependency},
          {:calendar, :forbidden_clock_dependency},
          {:timer, :forbidden_clock_dependency},
          {:os, :forbidden_clock_dependency}
        ] do
      assert {:error, [{^violation, _}]} =
               Architecture.check([ref(:graviton_mq_amqp10, source_module, nil, target)])
    end
  end

  test "parsed AMQP endpoints reject direct process and clock primitives" do
    for {expression, kind} <- [
          {"Process.send_after(pid, :tick, 1)", :forbidden_process_dependency},
          {"spawn(fun)", :forbidden_process_dependency},
          {"spawn_link(fun)", :forbidden_process_dependency},
          {"spawn_monitor(fun)", :forbidden_process_dependency},
          {"send(pid, :message)", :forbidden_process_dependency},
          {"receive do: (:message -> :ok)", :forbidden_process_dependency},
          {"self()", :forbidden_process_dependency},
          {"exit(:normal)", :forbidden_process_dependency},
          {"Kernel.spawn(fun)", :forbidden_process_dependency},
          {"Kernel.send(pid, :message)", :forbidden_process_dependency},
          {"Kernel.self()", :forbidden_process_dependency},
          {"Kernel.exit(:normal)", :forbidden_process_dependency},
          {"&Kernel.self/0", :forbidden_process_dependency},
          {":erlang.spawn(fun)", :forbidden_process_dependency},
          {":erlang.spawn_opt(fun, [])", :forbidden_process_dependency},
          {":erlang.spawn_request(fun, [])", :forbidden_process_dependency},
          {":erlang.send_after(1, pid, :tick)", :forbidden_process_dependency},
          {":erlang.start_timer(1, pid, :tick)", :forbidden_process_dependency},
          {":erlang.cancel_timer(pid)", :forbidden_process_dependency},
          {":erlang.read_timer(pid)", :forbidden_process_dependency},
          {":erlang.self()", :forbidden_process_dependency},
          {":erlang.monitor(:process, pid)", :forbidden_process_dependency},
          {":erlang.demonitor(pid)", :forbidden_process_dependency},
          {":erlang.link(pid)", :forbidden_process_dependency},
          {":erlang.unlink(pid)", :forbidden_process_dependency},
          {":erlang.register(:owner, pid)", :forbidden_process_dependency},
          {":erlang.unregister(:owner)", :forbidden_process_dependency},
          {":erlang.whereis(:owner)", :forbidden_process_dependency},
          {":erlang.process_flag(:trap_exit, true)", :forbidden_process_dependency},
          {":erlang.process_info(pid)", :forbidden_process_dependency},
          {"System.monotonic_time()", :forbidden_clock_dependency},
          {"DateTime.utc_now()", :forbidden_clock_dependency},
          {":timer.sleep(1)", :forbidden_clock_dependency},
          {":os.system_time()", :forbidden_clock_dependency},
          {":erlang.monotonic_time()", :forbidden_clock_dependency},
          {":erlang.system_time(:millisecond)", :forbidden_clock_dependency},
          {":erlang.time_offset()", :forbidden_clock_dependency},
          {":erlang.timestamp()", :forbidden_clock_dependency},
          {":erlang.now()", :forbidden_clock_dependency},
          {":erlang.date()", :forbidden_clock_dependency},
          {":erlang.time()", :forbidden_clock_dependency},
          {":erlang.localtime()", :forbidden_clock_dependency},
          {":erlang.universaltime()", :forbidden_clock_dependency},
          {"&:erlang.monotonic_time/0", :forbidden_clock_dependency}
        ] do
      source = """
      defmodule GravitonMQ.AMQP10.Endpoint do
        def impure(fun, pid), do: #{expression}
      end
      """

      references = SourceAnalyzer.references(:graviton_mq_amqp10, "lib/endpoint.ex", source)
      result = Architecture.check(references)
      assert match?({:error, _}, result), expression
      {:error, violations} = result
      assert Enum.any?(violations, fn {violation, _} -> violation == kind end), expression
    end
  end

  test "parsed AMQP endpoint clocks are rejected without a process reference" do
    source = """
    defmodule GravitonMQ.AMQP10.Endpoint do
      def clock, do: :erlang.monotonic_time(:millisecond)
    end
    """

    references = SourceAnalyzer.references(:graviton_mq_amqp10, "lib/endpoint.ex", source)
    assert Enum.any?(references, &(&1.target_module == System))
    assert {:error, violations} = Architecture.check(references)
    assert Enum.any?(violations, &match?({:forbidden_clock_dependency, _}, &1))
  end

  test "parsed Kernel process calls and captures are rejected in both protocol libraries" do
    for {app, module} <- [
          {:graviton_mq_mqtt, GravitonMQ.MQTT.Client},
          {:graviton_mq_amqp10, GravitonMQ.AMQP10.Endpoint}
        ],
        expression <- ["Kernel.self()", "Kernel.exit(:normal)", "&Kernel.self/0"] do
      source = """
      defmodule #{inspect(module)} do
        def impure, do: #{expression}
      end
      """

      references = SourceAnalyzer.references(app, "lib/endpoint.ex", source)
      assert Enum.any?(references, &(&1.target_module == Process)), expression
      assert {:error, violations} = Architecture.check(references)
      assert Enum.any?(violations, &match?({:forbidden_process_dependency, _}, &1))
    end
  end

  test "pure local helpers and their guarded default heads are not process primitives" do
    for {app, module} <- [
          {:graviton_mq_amqp10, GravitonMQ.AMQP10.PureHelpers},
          {:graviton_mq_mqtt, GravitonMQ.MQTT.PureHelpers}
        ] do
      source = ~S"""
      defmodule GravitonMQ.AMQP10.PureHelpers do
        def cancel(state), do: cancel_timer(state, :operation)
        def cancel_default(state), do: cancel_timer(state)
        def capture, do: &cancel_timer/2
        defp cancel_timer(state, key \\ :default)
        defp cancel_timer(state, key) when is_map(state), do: Map.delete(state, key)
      end
      """

      source = String.replace(source, "GravitonMQ.AMQP10.PureHelpers", inspect(module))
      references = SourceAnalyzer.references(app, "lib/pure_helpers.ex", source)
      refute Enum.any?(references, &(&1.target_module == Process))
      assert :ok = Architecture.check(references)
    end
  end

  test "local primitive-shaped helpers resolve by arity while real remote calls remain visible" do
    source = """
    defmodule GravitonMQ.AMQP10.LocalHelpers do
      defp cancel_timer(state, key), do: Map.delete(state, key)
      def pure(state), do: cancel_timer(state, :key)
      def different_arity, do: cancel_timer(:reference)
      def remote(reference), do: Process.cancel_timer(reference)
      def erlang(reference), do: :erlang.cancel_timer(reference, [])
      def kernel, do: Kernel.self()
    end
    """

    references = SourceAnalyzer.references(:graviton_mq_amqp10, "lib/helpers.ex", source)

    process_lines =
      for reference <- references, reference.target_module == Process, do: reference.line

    assert process_lines == [4, 5, 6, 7]
  end

  test "local primitive-shaped definitions never leak into nested or sibling module scopes" do
    source = """
    defmodule GravitonMQ.AMQP10.Outer do
      defp cancel_timer(state, key), do: Map.delete(state, key)
      def pure(state), do: cancel_timer(state, :key)
      defmodule Inner do
        def impure(reference, options), do: cancel_timer(reference, options)
      end
    end
    defmodule GravitonMQ.AMQP10.Sibling do
      def impure(reference, options), do: cancel_timer(reference, options)
    end
    """

    references = SourceAnalyzer.references(:graviton_mq_amqp10, "lib/scopes.ex", source)

    process_origins =
      for reference <- references,
          reference.target_module == Process,
          do: {reference.source_module, reference.line}

    assert process_origins == [{GravitonMQ.AMQP10.Outer.Inner, 5}, {GravitonMQ.AMQP10.Sibling, 9}]
  end

  test "unqualified process captures respect only matching local signatures" do
    source = """
    defmodule GravitonMQ.AMQP10.Captures do
      defp cancel_timer(state, key), do: Map.delete(state, key)
      def pure, do: &cancel_timer/2
      def impure, do: &self/0
    end
    """

    references = SourceAnalyzer.references(:graviton_mq_amqp10, "lib/captures.ex", source)
    assert Enum.any?(references, &(&1.target_module == Process and &1.line == 4))
    refute Enum.any?(references, &(&1.target_module == Process and &1.line in [2, 3]))
  end

  test "imported primitives remain visible beside different-arity local helpers" do
    source = """
    defmodule GravitonMQ.AMQP10.ImportedPrimitives do
      import :erlang, only: [cancel_timer: 1]
      defp cancel_timer(state, key), do: Map.delete(state, key)
      def pure(state), do: cancel_timer(state, :key)
      def impure(reference), do: cancel_timer(reference)
    end
    """

    references = SourceAnalyzer.references(:graviton_mq_amqp10, "lib/imports.ex", source)

    process_lines =
      for reference <- references, reference.target_module == Process, do: reference.line

    assert process_lines == [5]
  end

  test "clock diagnostics name the originating protocol" do
    for {app, module, protocol} <- [
          {:graviton_mq_amqp10, GravitonMQ.AMQP10.Endpoint, "AMQP"},
          {:graviton_mq_mqtt, GravitonMQ.MQTT.Client, "MQTT"}
        ] do
      assert {:error, [violation]} = Architecture.check([ref(app, module, nil, System)])

      assert Architecture.format_violation(violation) =~
               "pure #{protocol} source references clock"
    end
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
