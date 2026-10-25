defmodule GravitonMQ.ReleaseTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir
  @repo_root Path.expand("../../../..", __DIR__)
  @dependencies %{
    graviton_mq_core: [],
    graviton_mq_storage: [:graviton_mq_core],
    graviton_mq_amqp10: [:graviton_mq_core],
    graviton_mq_runtime: [:graviton_mq_core, :graviton_mq_storage, :graviton_mq_amqp10],
    graviton_mq: [:graviton_mq_runtime]
  }

  test "version updates accept prereleases and reject malformed input before writing", %{
    tmp_dir: root
  } do
    File.cp_r!(Path.join(@repo_root, "scripts"), Path.join(root, "scripts"))
    File.write!(Path.join(root, "VERSION"), "0.1.0\n")

    assert {_, 0} = run_script(root, "set_version.exs", ["2.3.4-rc.1"])
    assert File.read!(Path.join(root, "VERSION")) == "2.3.4-rc.1\n"

    for invalid <- ["v2.3.4", "2.3", "02.3.4", "2.3.4+build", "2.3.4\n", "$(exit 0)"] do
      {_, status} = run_script(root, "set_version.exs", [invalid])
      assert status != 0
      assert File.read!(Path.join(root, "VERSION")) == "2.3.4-rc.1\n"
    end
  end

  test "all packages follow a single version update and retain standalone Hex metadata", %{
    tmp_dir: root
  } do
    for file <- ["mix.exs", "VERSION", "LICENSE", "README.md", "build", "config", "scripts"] do
      File.cp_r!(Path.join(@repo_root, file), Path.join(root, file))
    end

    for app <- Map.keys(@dependencies) do
      source = Path.join(@repo_root, "apps/#{app}")
      target = Path.join(root, "apps/#{app}")
      File.mkdir_p!(target)
      File.cp!(Path.join(source, "mix.exs"), Path.join(target, "mix.exs"))
      File.cp_r!(Path.join(source, "lib"), Path.join(target, "lib"))
    end

    version = "2.3.4-rc.1"
    assert {_, 0} = run_script(root, "set_version.exs", [version])
    staging = Path.join(root, "hex")
    {output, status} = run_script(root, "stage_hex.exs", [staging])
    assert status == 0, output

    assert File.read!(Path.join(staging, "packages.txt")) ==
             "graviton_mq_core\ngraviton_mq_storage\ngraviton_mq_amqp10\ngraviton_mq_runtime\ngraviton_mq\n"

    for {app, dependencies} <- @dependencies do
      package = Path.join(staging, Atom.to_string(app))
      config = literal_function(Path.join(package, "mix.exs"), :project)

      assert config[:app] == app
      assert config[:version] == version
      assert config[:elixir] == "~> 1.18.4"
      assert config[:package][:licenses] == ["MIT"]
      assert config[:deps] == Enum.map(dependencies, &{&1, "== #{version}"})

      refute Enum.any?(
               [:build_path, :config_path, :deps_path, :lockfile],
               &Keyword.has_key?(config, &1)
             )

      assert File.read!(Path.join(package, "LICENSE")) == File.read!(Path.join(root, "LICENSE"))
      assert File.exists?(Path.join(package, "README.md"))
      assert File.dir?(Path.join(package, "lib"))

      application = literal_function(Path.join(package, "mix.exs"), :application)

      assert application ==
               if(app == :graviton_mq, do: [mod: {GravitonMQ.Application, []}], else: [])
    end

    File.write!(Path.join(staging, "preserve.txt"), "existing staging")
    {_, status} = run_script(root, "stage_hex.exs", [staging])
    assert status != 0
    assert File.read!(Path.join(staging, "preserve.txt")) == "existing staging"
  end

  defp run_script(root, script, args) do
    System.cmd("elixir", [Path.join(root, "scripts/#{script}") | args],
      env: [{"MIX_ENV", "prod"}],
      stderr_to_stdout: true
    )
  end

  defp literal_function(path, function) do
    ast = path |> File.read!() |> Code.string_to_quoted!()

    {_, body} =
      Macro.prewalk(ast, nil, fn
        {:def, _, [{^function, _, _}, [do: body]]} = node, nil -> {node, body}
        node, accumulator -> {node, accumulator}
      end)

    {value, []} = Code.eval_quoted(body)
    value
  end
end
