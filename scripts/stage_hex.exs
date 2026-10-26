Mix.start()

defmodule GravitonMQ.StageHex do
  @apps [
    :graviton_mq_core,
    :graviton_mq_storage,
    :graviton_mq_amqp10,
    :graviton_mq_mqtt,
    :graviton_mq_runtime,
    :graviton_mq
  ]

  def run(destination) do
    root = Path.expand("..", __DIR__)
    destination = Path.expand(destination)

    if File.exists?(destination), do: raise(ArgumentError, "staging destination already exists")

    Mix.Project.in_project(:graviton_mq_umbrella, root, fn _ ->
      paths = Mix.Project.apps_paths()

      if MapSet.new(Map.keys(paths)) != MapSet.new(@apps) do
        raise ArgumentError, "update the Hex publication order to include every umbrella app"
      end

      version = Mix.Project.config()[:version]
      File.mkdir_p!(destination)

      for app <- @apps do
        source = Path.join(root, Map.fetch!(paths, app))
        target = Path.join(destination, Atom.to_string(app))

        Mix.Project.in_project(app, source, fn project ->
          config = project.project()

          if config[:version] != version do
            raise ArgumentError, "#{app} version does not match the umbrella"
          end

          deps =
            Enum.map(config[:deps], fn {dependency, [in_umbrella: true]} ->
              true = dependency in @apps
              {dependency, "== #{version}"}
            end)

          config =
            config
            |> Keyword.drop([:build_path, :config_path, :deps_path, :lockfile])
            |> Keyword.put(:deps, deps)

          application =
            if function_exported?(project, :application, 0), do: project.application(), else: []

          ast =
            quote do
              defmodule unquote(project) do
                use Mix.Project
                def project, do: unquote(Macro.escape(config))
                def application, do: unquote(Macro.escape(application))
              end
            end

          File.mkdir!(target)
          File.cp_r!(Path.join(source, "lib"), Path.join(target, "lib"))

          for file <- ["README.md", "LICENSE"] do
            File.cp!(Path.join(root, file), Path.join(target, file))
          end

          mixfile = ast |> Macro.to_string() |> Code.format_string!()
          File.write!(Path.join(target, "mix.exs"), [mixfile, "\n"])
        end)
      end
    end)

    File.write!(
      Path.join(destination, "packages.txt"),
      Enum.map(@apps, &[Atom.to_string(&1), "\n"])
    )
  end
end

[destination] = System.argv()
GravitonMQ.StageHex.run(destination)
