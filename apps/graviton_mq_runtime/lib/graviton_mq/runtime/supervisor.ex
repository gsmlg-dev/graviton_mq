defmodule GravitonMQ.Runtime.Supervisor do
  @moduledoc """
  Root fault-domain boundary for future broker runtime processes.

  It is intentionally empty in Milestone 0: no listeners, storage processes,
  queues, registries, connections, sessions, or effect executors are started.
  """

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(options \\ []) do
    {name, init_options} = Keyword.pop(options, :name, __MODULE__)
    start_options = if name, do: [name: name], else: []

    Supervisor.start_link(__MODULE__, init_options, start_options)
  end

  @impl true
  def init(_options) do
    Supervisor.init([], strategy: :one_for_one)
  end
end
