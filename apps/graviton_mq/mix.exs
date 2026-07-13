defmodule GravitonMQ.MixProject do
  use Mix.Project

  def project do
    [
      app: :graviton_mq,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {GravitonMQ.Application, []}
    ]
  end

  defp deps do
    [
      {:graviton_mq_runtime, in_umbrella: true}
    ]
  end
end
