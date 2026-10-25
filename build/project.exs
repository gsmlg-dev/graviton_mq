defmodule GravitonMQ.Build do
  @moduledoc false

  @elixir_requirement "~> 1.18.4"
  @elixir_version "1.18.4"
  @otp_version "28.5.0.1"

  def version, do: __DIR__ |> Path.join("../VERSION") |> File.read!() |> String.trim()

  def elixir_requirement, do: @elixir_requirement
  def elixir_version, do: @elixir_version
  def otp_version, do: @otp_version

  def package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/gsmlg-dev/graviton_mq"},
      files: ["lib", "mix.exs", "README.md", "LICENSE"]
    ]
  end
end
