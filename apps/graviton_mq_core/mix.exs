defmodule GravitonMQCore.MixProject do
  use Mix.Project

  def project do
    [
      app: :graviton_mq_core,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  defp deps, do: []
end
