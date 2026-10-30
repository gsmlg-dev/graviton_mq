defmodule GravitonMQ.Runtime.AMQP10.Supervisor do
  @moduledoc false
  use Supervisor

  alias GravitonMQ.Runtime.AMQP10.{Broker, Listener, Queue, Store}

  def start_link(configuration), do: Supervisor.start_link(__MODULE__, configuration)

  def child_spec(config),
    do: %{id: __MODULE__, type: :supervisor, start: {__MODULE__, :start_link, [fn -> config end]}}

  @impl true
  def init(configuration) do
    config = configuration.()
    parent = self()

    queues =
      Enum.map(config.queues, fn address ->
        Supervisor.child_spec({Queue, {config, address}}, id: {Queue, address})
      end)

    storage = if config.persistence, do: [{Store, config.persistence}], else: []

    children =
      storage ++
        queues ++
        [
          Supervisor.child_spec({Task.Supervisor, max_children: config.max_auth_jobs},
            id: :auth_tasks
          ),
          {Broker, {config, parent}},
          Supervisor.child_spec(
            {DynamicSupervisor, strategy: :one_for_one, max_children: config.max_connections},
            id: :connections
          )
        ] ++
        Enum.map(config.listeners, fn listener ->
          Supervisor.child_spec({Listener, {config, listener, parent}},
            id: {Listener, listener.id}
          )
        end)

    # A queue failure closes later running owners before replacement admission.
    # Broker restart preserves surviving queues, whose owner monitors release leases.
    Supervisor.init(children, strategy: :rest_for_one)
  end
end
