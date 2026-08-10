defmodule GravitonMQ.Architecture do
  @moduledoc """
  Pure rules for validating GravitonMQ source-module dependencies.

  The Mix architecture task supplies references collected from compiler xref
  manifests and parsed source syntax. Keeping rule evaluation pure makes
  forbidden edges and cycle detection deterministic and directly testable.
  """

  defmodule Reference do
    @moduledoc "A source-level module reference and its owning applications."

    @enforce_keys [:source_app, :source_module, :target_module, :source_file, :kind]
    defstruct [
      :source_app,
      :source_module,
      :target_app,
      :target_module,
      :source_file,
      :line,
      :kind
    ]

    @type dependency_kind :: :compile | :export | :runtime | :syntax
    @type t :: %__MODULE__{
            source_app: atom(),
            source_module: module() | nil,
            target_app: atom() | nil,
            target_module: module(),
            source_file: binary(),
            line: pos_integer() | nil,
            kind: dependency_kind()
          }
  end

  @allowed_targets %{
    graviton_mq_core: [],
    graviton_mq_storage: [:graviton_mq_core],
    graviton_mq_amqp10: [:graviton_mq_core],
    graviton_mq_runtime: [
      :graviton_mq_core,
      :graviton_mq_storage,
      :graviton_mq_amqp10
    ],
    graviton_mq: [:graviton_mq_runtime]
  }

  @transport_modules [
    :gen_sctp,
    :gen_tcp,
    :gen_udp,
    :inet,
    :inet6_tcp,
    :inet_tcp,
    :prim_inet,
    :prim_socket,
    :ranch_ssl,
    :ranch_tcp,
    :socket,
    :ssl,
    Bandit,
    Ranch,
    Socket,
    ThousandIsland
  ]

  @transport_namespace_prefixes [
    "Elixir.Bandit",
    "Elixir.Ranch",
    "Elixir.Socket",
    "Elixir.ThousandIsland"
  ]

  @process_modules [
    GenServer,
    Supervisor,
    DynamicSupervisor,
    PartitionSupervisor,
    Agent,
    Task,
    Task.Supervisor,
    Process,
    Registry
  ]

  @type violation ::
          {:forbidden_application_dependency, map()}
          | {:forbidden_transport_dependency, map()}
          | {:forbidden_process_dependency, map()}
          | {:application_cycle, [atom()]}

  @spec check([Reference.t()]) :: :ok | {:error, [violation()]}
  def check(references) when is_list(references) do
    violations =
      references
      |> Enum.flat_map(&reference_violations/1)
      |> Kernel.++(cycle_violations(references))
      |> Enum.uniq()
      |> Enum.sort_by(&inspect/1)

    case violations do
      [] -> :ok
      violations -> {:error, violations}
    end
  end

  @spec owner_for_module(module()) :: atom() | nil
  def owner_for_module(module) when is_atom(module) do
    module_name = Atom.to_string(module)

    cond do
      in_namespace?(module_name, "Elixir.GravitonMQ.Core") -> :graviton_mq_core
      in_namespace?(module_name, "Elixir.GravitonMQ.Queue") -> :graviton_mq_core
      in_namespace?(module_name, "Elixir.GravitonMQ.Storage") -> :graviton_mq_storage
      in_namespace?(module_name, "Elixir.GravitonMQ.AMQP10") -> :graviton_mq_amqp10
      in_namespace?(module_name, "Elixir.GravitonMQ.Runtime") -> :graviton_mq_runtime
      in_namespace?(module_name, "Elixir.GravitonMQ") -> :graviton_mq
      true -> nil
    end
  end

  @spec format_violation(violation()) :: binary()
  def format_violation({:forbidden_application_dependency, reference}) do
    "#{location(reference)}: #{inspect(reference.source_app)} source references " <>
      "forbidden #{inspect(reference.target_app)} module #{inspect(reference.target_module)}"
  end

  def format_violation({:forbidden_transport_dependency, reference}) do
    "#{location(reference)}: pure AMQP source references transport module " <>
      inspect(reference.target_module)
  end

  def format_violation({:forbidden_process_dependency, reference}) do
    "#{location(reference)}: pure AMQP codec source references process module " <>
      inspect(reference.target_module)
  end

  def format_violation({:application_cycle, applications}) do
    "application dependency cycle contains: #{Enum.map_join(applications, ", ", &inspect/1)}"
  end

  defp reference_violations(%Reference{} = reference) do
    application_violation(reference) ++
      transport_violation(reference) ++ process_violation(reference)
  end

  defp application_violation(%Reference{target_app: nil}), do: []

  defp application_violation(%Reference{source_app: app, target_app: app}), do: []

  defp application_violation(%Reference{} = reference) do
    if reference.target_app in Map.get(@allowed_targets, reference.source_app, []) do
      []
    else
      [{:forbidden_application_dependency, violation_details(reference)}]
    end
  end

  defp transport_violation(
         %Reference{source_app: :graviton_mq_amqp10, target_module: target_module} = reference
       ) do
    if transport_module?(target_module) do
      [{:forbidden_transport_dependency, violation_details(reference)}]
    else
      []
    end
  end

  defp transport_violation(_reference), do: []

  defp process_violation(
         %Reference{source_module: source_module, target_module: target_module} = reference
       ) do
    if codec_module?(source_module) and target_module in @process_modules do
      [{:forbidden_process_dependency, violation_details(reference)}]
    else
      []
    end
  end

  defp transport_module?(module) when module in @transport_modules, do: true

  defp transport_module?(module) when is_atom(module) do
    module_name = Atom.to_string(module)

    Enum.any?(@transport_namespace_prefixes, fn prefix ->
      module_name == prefix or String.starts_with?(module_name, prefix <> ".")
    end)
  end

  defp codec_module?(module) when is_atom(module),
    do: in_namespace?(Atom.to_string(module), "Elixir.GravitonMQ.AMQP10.Codec")

  defp codec_module?(_module), do: false

  defp cycle_violations(references) do
    edges =
      references
      |> Enum.flat_map(fn
        %Reference{source_app: app, target_app: app} ->
          []

        %Reference{source_app: source, target_app: target} when is_atom(target) ->
          [{source, target}]

        %Reference{} ->
          []
      end)
      |> MapSet.new()

    case cyclic_nodes(edges) do
      [] -> []
      applications -> [{:application_cycle, applications}]
    end
  end

  defp cyclic_nodes(edges) do
    nodes =
      edges
      |> Enum.flat_map(fn {source, target} -> [source, target] end)
      |> MapSet.new()

    adjacency =
      Enum.reduce(edges, %{}, fn {source, target}, graph ->
        Map.update(graph, source, [target], &[target | &1])
      end)

    nodes
    |> Enum.filter(fn node ->
      Enum.any?(Map.get(adjacency, node, []), fn next ->
        reachable?(next, node, adjacency, MapSet.new())
      end)
    end)
    |> Enum.sort()
  end

  defp reachable?(target, target, _adjacency, _seen), do: true

  defp reachable?(node, target, adjacency, seen) do
    if MapSet.member?(seen, node) do
      false
    else
      seen = MapSet.put(seen, node)
      Enum.any?(Map.get(adjacency, node, []), &reachable?(&1, target, adjacency, seen))
    end
  end

  defp violation_details(reference) do
    reference
    |> Map.from_struct()
    |> Map.take([
      :source_app,
      :source_module,
      :target_app,
      :target_module,
      :source_file,
      :line,
      :kind
    ])
  end

  defp in_namespace?(module_name, namespace) do
    module_name == namespace or String.starts_with?(module_name, namespace <> ".")
  end

  defp location(%{source_file: file, line: nil}), do: file
  defp location(%{source_file: file, line: line}), do: "#{file}:#{line}"
end
