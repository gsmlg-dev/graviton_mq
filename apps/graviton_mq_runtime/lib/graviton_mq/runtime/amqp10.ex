defmodule GravitonMQ.Runtime.AMQP10 do
  @moduledoc false

  alias GravitonMQ.Runtime.AMQP10.{Broker, Listener}

  def status(instance) do
    with {:ok, runtime} <- child(instance, GravitonMQ.Runtime.Supervisor),
         {:ok, amqp} <- child(runtime, GravitonMQ.Runtime.AMQP10.Supervisor),
         {:ok, broker} <- child(amqp, Broker) do
      try do
        listeners =
          Supervisor.which_children(amqp)
          |> Enum.flat_map(fn
            {{Listener, _}, pid, _, _} when is_pid(pid) -> [Listener.info(pid)]
            _ -> []
          end)

        {:ok, Map.put(Broker.status(broker), :listeners, listeners)}
      catch
        :exit, _ -> {:error, :not_running}
      end
    end
  end

  defp child(supervisor, id) do
    try do
      case List.keyfind(Supervisor.which_children(supervisor), id, 0) do
        {^id, pid, _, _} when is_pid(pid) -> {:ok, pid}
        _ -> {:error, :amqp10_not_enabled}
      end
    catch
      :exit, _ -> {:error, :not_running}
    end
  end
end
