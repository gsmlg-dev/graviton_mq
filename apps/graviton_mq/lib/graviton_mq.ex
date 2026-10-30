defmodule GravitonMQ do
  @moduledoc """
  Public namespace for the embeddable GravitonMQ application.

  `start_link/1` and `child_spec/1` are the common standalone and embedded
  lifecycle. Explicit `:mqtt` and `:amqp10` configurations enable their
  instance-local brokers. Memory is the default; local persistence is opt-in.
  Without either protocol, the historical empty boundary is preserved.
  Host topic operations are independent of the outgoing network client.
  """

  @doc """
  Starts one GravitonMQ instance supervision tree.

  `:name` and `:runtime_supervisor_name` are exact OTP registration names
  supplied by the caller. They are never converted into atoms.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(options \\ []) do
    GravitonMQ.Supervisor.start_link(options)
  end

  @doc """
  Returns a child specification for host-supervised embedding.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(options) do
    name = Keyword.get(options, :name, GravitonMQ.Supervisor)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [fn -> options end]},
      type: :supervisor
    }
  end

  @doc "Admits a host publication; completion identifies memory or synchronized local responsibility."
  def publish(instance, topic, payload, options \\ []),
    do: GravitonMQ.Runtime.MQTT.publish(instance, topic, payload, options)

  @doc "Admits a monitored host subscription with explicit demand and acknowledgements."
  def subscribe(instance, filter, options \\ []),
    do: GravitonMQ.Runtime.MQTT.subscribe(instance, filter, options)

  def request(instance, subscription, count),
    do: GravitonMQ.Runtime.MQTT.request(instance, subscription, count)

  def ack(instance, delivery), do: GravitonMQ.Runtime.MQTT.ack(instance, delivery)

  def unsubscribe(instance, subscription),
    do: GravitonMQ.Runtime.MQTT.unsubscribe(instance, subscription)

  def status(instance), do: GravitonMQ.Runtime.MQTT.status(instance)

  @doc "Returns the explicitly enabled AMQP broker's bounded status and responsibility profile."
  def amqp10_status(instance), do: GravitonMQ.Runtime.AMQP10.status(instance)
end
