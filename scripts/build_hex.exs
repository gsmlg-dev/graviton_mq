[staging] = System.argv()
staging = Path.expand(staging)
packages = staging |> Path.join("packages.txt") |> File.read!() |> String.split("\n", trim: true)
artifacts = Path.join(staging, "artifacts")
unpacked = Path.join(staging, "unpacked")
File.mkdir!(artifacts)
File.mkdir!(unpacked)

run! = fn args, directory ->
  {_, status} =
    System.cmd("mix", args, cd: directory, into: IO.stream(), env: [{"MIX_ENV", "prod"}])

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
  end

run!.(["run", "--no-compile", "-e", Macro.to_string(smoke)], consumer)
