[staging] = System.argv()
staging = Path.expand(staging)
packages = staging |> Path.join("packages.txt") |> File.read!() |> String.split("\n", trim: true)
artifacts = Path.join(staging, "artifacts")
unpacked = Path.join(staging, "unpacked")
File.mkdir!(artifacts)
File.mkdir!(unpacked)

run! = fn args, directory ->
  {_, status} =
    System.cmd("mix", args,
      cd: directory,
      into: IO.stream(),
      env: [{"MIX_ENV", "prod"}, {"MIX_BUILD_PATH", Path.join(directory, "_build")}]
    )

  if status != 0, do: raise("mix #{Enum.join(args, " ")} failed in #{directory}")
end

for package <- packages do
  archive = Path.join(artifacts, package <> ".tar")
  run!.(["hex.build", "--output", archive], Path.join(staging, package))

  {:ok, files} = :erl_tar.extract(String.to_charlist(archive), [:memory])
  {~c"contents.tar.gz", contents} = List.keyfind(files, ~c"contents.tar.gz", 0)
  target = Path.join(unpacked, package)
  File.mkdir!(target)
  :ok = :erl_tar.extract({:binary, contents}, [:compressed, {:cwd, String.to_charlist(target)}])
end

consumer = Path.join(staging, "consumer")
File.mkdir!(consumer)

deps =
  Enum.map(packages, fn package ->
    {String.to_atom(package), [path: Path.join(unpacked, package), override: true]}
  end)

project =
  quote do
    defmodule GravitonMQ.HexSmoke.MixProject do
      use Mix.Project

      def project do
        [app: :graviton_mq_hex_smoke, version: "0.0.0", deps: unquote(Macro.escape(deps))]
      end
    end
  end

File.write!(Path.join(consumer, "mix.exs"), Macro.to_string(project))
run!.(["deps.get"], consumer)
run!.(["compile", "--warnings-as-errors"], consumer)

version = Path.expand("../VERSION", __DIR__) |> File.read!() |> String.trim()

smoke =
  quote do
    for package <- unquote(packages) do
      app = String.to_existing_atom(package)
      unquote(version) = app |> Application.spec(:vsn) |> to_string()
    end

    true = is_pid(Process.whereis(GravitonMQ.Supervisor))
    true = Code.ensure_loaded?(GravitonMQ.MQTT.Codec.Packet)

    {:ok, %GravitonMQ.MQTT.Packet.Connect{version: :v3_1_1, client_id: "c"}, <<>>} =
      GravitonMQ.MQTT.Codec.Packet.decode(<<0x10, 0x0D, 0, 4, "MQTT", 4, 2, 0, 60, 0, 1, "c">>)

    for app <- [
          :graviton_mq_core,
          :graviton_mq_storage,
          :graviton_mq_amqp10,
          :graviton_mq_mqtt,
          :graviton_mq_runtime
        ] do
      [] = Application.spec(app, :mod)
    end
  end

run!.(["run", "--no-compile", "-e", Macro.to_string(smoke)], consumer)

# A separate host project proves runtime: false prevents the product's default
# instance from starting, while the host can supervise the same public API.
embedding_consumer = Path.join(staging, "embedding-consumer")
File.mkdir!(embedding_consumer)

embedding_deps =
  Enum.map(deps, fn
    {:graviton_mq, options} -> {:graviton_mq, Keyword.put(options, :runtime, false)}
    dependency -> dependency
  end)

embedding_project =
  quote do
    defmodule GravitonMQ.HexEmbeddingSmoke.MixProject do
      use Mix.Project

      def project do
        [
          app: :graviton_mq_hex_embedding_smoke,
          version: "0.0.0",
          deps: unquote(Macro.escape(embedding_deps))
        ]
      end
    end
  end

File.write!(Path.join(embedding_consumer, "mix.exs"), Macro.to_string(embedding_project))
run!.(["deps.get"], embedding_consumer)
run!.(["compile", "--warnings-as-errors"], embedding_consumer)

embedding_smoke =
  quote do
    nil = Process.whereis(GravitonMQ.Supervisor)
    nil = Process.whereis(GravitonMQ.Runtime.Supervisor)

    false =
      Enum.any?(Application.started_applications(), fn {app, _, _} -> app == :graviton_mq end)

    {:ok, host} =
      Supervisor.start_link(
        [
          {GravitonMQ,
           name: :hex_embedded_graviton_mq,
           runtime_supervisor_name: :hex_embedded_graviton_mq_runtime}
        ],
        strategy: :one_for_one
      )

    embedded = Process.whereis(:hex_embedded_graviton_mq)
    runtime = Process.whereis(:hex_embedded_graviton_mq_runtime)
    true = is_pid(embedded)
    true = is_pid(runtime)
    [] = Supervisor.which_children(runtime)
    nil = Process.whereis(GravitonMQ.Supervisor)

    {:ok, %GravitonMQ.MQTT.Packet.Connect{version: :v3_1_1, client_id: "c"}, <<>>} =
      GravitonMQ.MQTT.Codec.Packet.decode(<<0x10, 0x0D, 0, 4, "MQTT", 4, 2, 0, 60, 0, 1, "c">>)

    :ok = Supervisor.stop(host)
    false = Process.alive?(embedded)
    false = Process.alive?(runtime)
  end

run!.(["run", "--no-compile", "-e", Macro.to_string(embedding_smoke)], embedding_consumer)
