defmodule GravitonMQ.Runtime.ListenerSupervisor do
  @moduledoc """
  One-for-one fault domain for a protocol's network listeners.

  A listener failure restarts only that listener, so its siblings keep their
  listen sockets and connections. It takes the listeners' child specs and
  knows nothing about any protocol. Exceeding its restart intensity escalates
  to the parent supervisor.
  """
  use Supervisor

  def start_link(children), do: Supervisor.start_link(__MODULE__, children)

  def child_spec(children),
    do: %{
      id: __MODULE__,
      type: :supervisor,
      shutdown: :infinity,
      start: {__MODULE__, :start_link, [children]}
    }

  @impl true
  def init(children), do: Supervisor.init(children, strategy: :one_for_one)
end
