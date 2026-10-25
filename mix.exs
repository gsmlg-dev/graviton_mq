Code.require_file("build/project.exs", __DIR__)

defmodule GravitonMQ.Umbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: GravitonMQ.Build.version(),
      elixir: GravitonMQ.Build.elixir_requirement(),
      start_permanent: Mix.env() == :prod,
      deps: []
    ]
  end
end
