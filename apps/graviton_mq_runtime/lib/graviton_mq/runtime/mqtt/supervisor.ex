defmodule GravitonMQ.Runtime.MQTT.Supervisor do
  @moduledoc false
  use Supervisor

  alias GravitonMQ.Runtime.ListenerSupervisor
  alias GravitonMQ.Runtime.MQTT.{Broker, Listener}

  def start_link(configuration), do: Supervisor.start_link(__MODULE__, configuration)

  def child_spec(config),
    do: %{id: __MODULE__, type: :supervisor, start: {__MODULE__, :start_link, [fn -> config end]}}

  @impl true
  def init(configuration) do
    config = configuration.()
    parent = self()

    # Listeners share one one-for-one fault domain, started only when any are
    # configured, so a failing listener does not restart its siblings; they
    # still wire through this supervisor.
    listeners =
      Enum.map(config.listeners, fn listener ->
        Supervisor.child_spec({Listener, {config, listener, parent}}, id: {Listener, listener.id})
      end)

    children =
      [
        Supervisor.child_spec({Task.Supervisor, max_children: config.auth_concurrency},
          id: :auth_tasks
        ),
        {Broker, {config, parent}},
        Supervisor.child_spec(
          {Task.Supervisor,
           max_children: Keyword.get(config.machine_options, :max_prepared, 128)},
          id: :operation_tasks
        ),
        Supervisor.child_spec(
          {DynamicSupervisor,
           strategy: :one_for_one,
           max_children: Keyword.get(config.machine_options, :max_connections, 128)},
          id: :connections
        )
      ] ++
        if listeners == [],
          do: [],
          else: [Supervisor.child_spec({ListenerSupervisor, listeners}, id: :listeners)]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
