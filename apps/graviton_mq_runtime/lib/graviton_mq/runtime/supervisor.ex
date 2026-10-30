defmodule GravitonMQ.Runtime.Supervisor do
  @moduledoc """
  Root fault-domain boundary for broker runtime processes.

  Without explicit protocol configuration it retains the empty Milestone 0
  boundary. Each enabled protocol owns independent memory state and opt-in listeners.
  """

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(options \\ []) do
    options = if is_function(options, 0), do: options.(), else: options
    {name, init_options} = Keyword.pop(options, :name, __MODULE__)
    start_options = if name, do: [name: name], else: []

    with {:ok, mqtt} <-
           configure(Keyword.get(init_options, :mqtt), GravitonMQ.Runtime.MQTT.Config),
         {:ok, amqp} <-
           configure(Keyword.get(init_options, :amqp10), GravitonMQ.Runtime.AMQP10.Config) do
      Supervisor.start_link(__MODULE__, fn -> {mqtt, amqp} end, start_options)
    end
  end

  defp configure(nil, _module), do: {:ok, nil}
  defp configure(options, module), do: module.build(options)

  @impl true
  def init(configuration) do
    {mqtt, amqp} = configuration.()

    children =
      if(mqtt, do: [{GravitonMQ.Runtime.MQTT.Supervisor, mqtt}], else: []) ++
        if amqp, do: [{GravitonMQ.Runtime.AMQP10.Supervisor, amqp}], else: []

    Supervisor.init(children, strategy: :one_for_one)
  end
end
