Code.require_file("../../build/project.exs", __DIR__)

defmodule GravitonMQClient.MixProject do
  use Mix.Project

  def project do
    [
      app: :graviton_mq_client,
      version: GravitonMQ.Build.version(),
      description: "Independent AMQP 1.0 and MQTT 5.0 network clients for GravitonMQ",
      package: GravitonMQ.Build.package(),
      elixir: GravitonMQ.Build.elixir_requirement(),
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application, do: [extra_applications: [:ssl]]

  defp deps do
    [
      {:graviton_mq_core, in_umbrella: true},
      {:graviton_mq_amqp10, in_umbrella: true},
      {:graviton_mq_mqtt, in_umbrella: true}
    ]
  end
end
