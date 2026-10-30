defmodule GravitonMQ.Supervisor do
  @moduledoc """
  Public top-level supervisor for one standalone or embedded instance.

  Its runtime boundary is `GravitonMQ.Runtime.Supervisor`. Both process
  names are exact options supplied through the public lifecycle; the module
  never derives atoms from external identifiers.
  """

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(options \\ []) do
    options = if is_function(options, 0), do: options.(), else: options
    {name, init_options} = Keyword.pop(options, :name, __MODULE__)
    start_options = if name, do: [name: name], else: []

    Supervisor.start_link(__MODULE__, fn -> init_options end, start_options)
  end

  @impl true
  def init(configuration) do
    options = configuration.()

    runtime_supervisor_name =
      Keyword.get(options, :runtime_supervisor_name, GravitonMQ.Runtime.Supervisor)

    children = [
      {GravitonMQ.Runtime.Supervisor,
       fn ->
         [
           name: runtime_supervisor_name,
           mqtt: Keyword.get(options, :mqtt),
           amqp10: Keyword.get(options, :amqp10)
         ]
       end}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
