defmodule GravitonMQ.DependencyGraphTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../..", __DIR__)

  @expected_dependencies %{
    graviton_mq_core: [],
    graviton_mq_storage: [:graviton_mq_core],
    graviton_mq_amqp10: [:graviton_mq_core],
    graviton_mq_runtime: [:graviton_mq_core, :graviton_mq_storage, :graviton_mq_amqp10],
    graviton_mq: [:graviton_mq_runtime]
  }

  test "child Mix projects declare exactly the required internal dependency graph" do
    expected_apps = @expected_dependencies |> Map.keys() |> Enum.map(&Atom.to_string/1)
    actual_apps = child_apps()

    assert MapSet.new(expected_apps) == MapSet.new(actual_apps)

    actual_dependencies =
      Map.new(actual_apps, fn app_name ->
        app = String.to_existing_atom(app_name)
        {app, declared_dependencies(app)}
      end)

    assert @expected_dependencies == actual_dependencies
  end

  test "umbrella and public child Mix project modules do not conflict" do
    project_modules =
      ["mix.exs" | Enum.map(Map.keys(@expected_dependencies), &"apps/#{&1}/mix.exs")]
      |> Enum.map(&Path.join(@repo_root, &1))
      |> Enum.map(&mix_project_module/1)

    assert GravitonMQ.Umbrella.MixProject in project_modules
    assert GravitonMQ.MixProject in project_modules
    assert length(project_modules) == length(Enum.uniq(project_modules))
  end

  defp child_apps do
    @repo_root
    |> Path.join("apps/*/mix.exs")
    |> Path.wildcard()
    |> Enum.map(&(&1 |> Path.dirname() |> Path.basename()))
  end

  defp declared_dependencies(app) do
    ast =
      @repo_root
      |> Path.join("apps/#{app}/mix.exs")
      |> File.read!()
      |> Code.string_to_quoted!()

    {_ast, dependencies_ast} =
      Macro.prewalk(ast, nil, fn
        {:defp, _meta, [{:deps, _function_meta, arguments}, [do: body]]} = node, nil
        when arguments in [nil, []] ->
          {node, body}

        node, accumulator ->
          {node, accumulator}
      end)

    assert dependencies_ast != nil
    {dependencies, []} = Code.eval_quoted(dependencies_ast)

    Enum.map(dependencies, fn
      {dependency, [in_umbrella: true]} -> dependency
      unexpected -> flunk("unexpected dependency declaration: #{inspect(unexpected)}")
    end)
  end

  defp mix_project_module(path) do
    ast = path |> File.read!() |> Code.string_to_quoted!()

    {_ast, module} =
      Macro.prewalk(ast, nil, fn
        {:defmodule, _meta, [{:__aliases__, _, aliases}, _body]} = node, nil ->
          {node, Module.concat(aliases)}

        node, accumulator ->
          {node, accumulator}
      end)

    module
  end
end
