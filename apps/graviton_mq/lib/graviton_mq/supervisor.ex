defmodule GravitonMQ.Supervisor do
  @moduledoc """
  Public top-level supervisor for the embedded broker application.

  Its only Milestone 0 child is `GravitonMQ.Runtime.Supervisor`.
  """

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(options \\ []) do
    {name, init_options} = Keyword.pop(options, :name, __MODULE__)
    start_options = if name, do: [name: name], else: []

    Supervisor.start_link(__MODULE__, init_options, start_options)
  end

  @impl true
  def init(options) do
    runtime_supervisor_name =
      Keyword.get(options, :runtime_supervisor_name, GravitonMQ.Runtime.Supervisor)

    children = [
      {GravitonMQ.Runtime.Supervisor, name: runtime_supervisor_name}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
