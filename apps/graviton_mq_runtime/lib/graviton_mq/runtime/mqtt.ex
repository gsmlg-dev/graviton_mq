defmodule GravitonMQ.Runtime.MQTT do
  @moduledoc false

  alias GravitonMQ.Runtime.MQTT.{Broker, Listener}

  def publish(instance, topic, payload, options),
    do: host(instance, {:publish, topic, payload, options})

  def subscribe(instance, filter, options), do: host(instance, {:subscribe, filter, options})
  def request(instance, subscription, count), do: host(instance, {:demand, subscription, count})
  def ack(instance, delivery), do: host(instance, {:ack, delivery})
  def unsubscribe(instance, subscription), do: host(instance, {:unsubscribe, subscription})

  def status(instance) do
    with {:ok, mqtt} <- mqtt(instance),
         {:ok, broker} <- child(mqtt, Broker) do
      try do
        status = GenServer.call(broker, :status)

        listeners =
          Supervisor.which_children(mqtt)
          |> Enum.flat_map(fn
            {{Listener, _}, listener, _, _} when is_pid(listener) -> [Listener.info(listener)]
            _ -> []
          end)

        {:ok, Map.put(status, :listeners, listeners)}
      catch
        :exit, _reason -> {:error, :not_running}
      end
    end
  end

  defp host(instance, command) do
    with {:ok, mqtt} <- mqtt(instance), {:ok, broker} <- child(mqtt, Broker) do
      try do
        GenServer.call(broker, {:host, command})
      catch
        :exit, _reason -> {:error, :unknown_outcome}
      end
    end
  end

  defp mqtt(instance) do
    with {:ok, runtime} <- child(instance, GravitonMQ.Runtime.Supervisor),
         {:ok, mqtt} <- child(runtime, GravitonMQ.Runtime.MQTT.Supervisor),
         do: {:ok, mqtt}
  end

  defp child(supervisor, id) do
    try do
      case List.keyfind(Supervisor.which_children(supervisor), id, 0) do
        {^id, pid, _, _} when is_pid(pid) -> {:ok, pid}
        _ -> {:error, :mqtt_not_enabled}
      end
    catch
      :exit, _reason -> {:error, :not_running}
    end
  end
end
