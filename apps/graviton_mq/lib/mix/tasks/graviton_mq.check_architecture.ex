defmodule Mix.Tasks.GravitonMq.CheckArchitecture do
  use Mix.Task

  @shortdoc "Checks GravitonMQ source and application dependency boundaries"
  @requirements ["compile"]

  @moduledoc """
  Checks actual compiled and parsed source references against GravitonMQ's
  umbrella dependency rules.

      mix graviton_mq.check_architecture

  Compiler xref manifests supply compile, export, and runtime references. A
  parsed-AST pass supplements them for typespec-only references and direct
  transport module calls. The task exits non-zero when it finds a forbidden
  edge or application cycle.
  """

  require Mix.Compilers.Elixir

  alias GravitonMQ.Architecture
  alias GravitonMQ.Architecture.Reference
  alias GravitonMQ.Architecture.SourceAnalyzer

  @apps [
    :graviton_mq_core,
    :graviton_mq_storage,
    :graviton_mq_amqp10,
    :graviton_mq_runtime,
    :graviton_mq
  ]

  @app_by_directory Map.new(@apps, &{Atom.to_string(&1), &1})

  @impl Mix.Task
  def run(_arguments) do
    root = umbrella_root!()
    manifests = read_manifests()
    module_owners = module_owners(manifests)

    references =
      (manifest_references(manifests, module_owners, root) ++ source_references(root))
      |> Enum.uniq()

    case Architecture.check(references) do
      :ok ->
        Mix.shell().info(
          "Architecture check passed (#{length(references)} source references across #{length(@apps)} applications)"
        )

      {:error, violations} ->
        details = Enum.map_join(violations, "\n", &"  - #{Architecture.format_violation(&1)}")
        Mix.raise("GravitonMQ architecture violations:\n#{details}")
    end
  end

  defp umbrella_root! do
    if Mix.Project.umbrella?() do
      File.cwd!()
    else
      Mix.raise("mix graviton_mq.check_architecture must run from the umbrella root")
    end
  end

  defp read_manifests do
    Map.new(@apps, fn app ->
      path =
        Path.join([
          Mix.Project.build_path(),
          "lib",
          Atom.to_string(app),
          ".mix",
          "compile.elixir"
        ])

      {app, Mix.Compilers.Elixir.read_manifest(path)}
    end)
  end

  defp module_owners(manifests) do
    for {app, {modules, _sources}} <- manifests,
        {module, _module_entry} <- modules,
        into: %{},
        do: {module, app}
  end

  defp manifest_references(manifests, module_owners, root) do
    for {source_app, {_modules, sources}} <- manifests,
        {source_file, source_entry} <- sources,
        source_module <- Mix.Compilers.Elixir.source(source_entry, :modules),
        {kind, target_modules} <- reference_groups(source_entry),
        target_module <- target_modules,
        source_module != target_module do
      %Reference{
        source_app: source_app,
        source_module: source_module,
        target_app:
          Map.get_lazy(module_owners, target_module, fn ->
            Architecture.owner_for_module(target_module)
          end),
        target_module: target_module,
        source_file: display_source_file(root, source_app, source_file),
        line: nil,
        kind: kind
      }
    end
  end

  defp reference_groups(source_entry) do
    [
      {:compile, Mix.Compilers.Elixir.source(source_entry, :compile_references)},
      {:export, Mix.Compilers.Elixir.source(source_entry, :export_references)},
      {:runtime, Mix.Compilers.Elixir.source(source_entry, :runtime_references)}
    ]
  end

  defp source_references(root) do
    root
    |> Path.join("apps/*/lib/**/*.ex")
    |> Path.wildcard()
    |> Enum.flat_map(fn path ->
      source_app = source_app!(root, path)
      relative_path = Path.relative_to(path, root)
      SourceAnalyzer.references(source_app, relative_path, File.read!(path))
    end)
  end

  defp source_app!(root, path) do
    case path |> Path.relative_to(root) |> Path.split() do
      ["apps", directory | _rest] -> Map.fetch!(@app_by_directory, directory)
      relative -> Mix.raise("source is outside an umbrella child: #{Path.join(relative)}")
    end
  end

  defp display_source_file(root, app, source_file) do
    if Path.type(source_file) == :absolute do
      Path.relative_to(source_file, root)
    else
      Path.join(["apps", Atom.to_string(app), source_file])
    end
  end
end
