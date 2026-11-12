defmodule GravitonMQ.Runtime.AMQP10 do
  @moduledoc false

  alias GravitonMQ.Runtime.AMQP10.{Broker, Listener}

  def status(instance) do
    with {:ok, runtime} <- child(instance, GravitonMQ.Runtime.Supervisor),
         {:ok, amqp} <- child(runtime, GravitonMQ.Runtime.AMQP10.Supervisor),
         {:ok, broker} <- child(amqp, Broker),
         {:ok, listeners} <- listeners(amqp) do
      try do
        listeners =
          Enum.flat_map(listeners, fn
            {{Listener, _}, pid, _, _} when is_pid(pid) -> [Listener.info(pid)]
            _ -> []
          end)

        {:ok, Map.put(Broker.status(broker), :listeners, listeners)}
      catch
        :exit, _ -> {:error, :not_running}
      end
    end
  end

  # No `:listeners` child means none are configured.
  defp listeners(amqp) do
    try do
      case List.keyfind(Supervisor.which_children(amqp), :listeners, 0) do
        nil -> {:ok, []}
        {:listeners, pid, _, _} when is_pid(pid) -> {:ok, Supervisor.which_children(pid)}
        _restarting -> {:error, :not_running}
      end
    catch
      :exit, _ -> {:error, :not_running}
    end
  end

  defp child(supervisor, id) do
    try do
      case List.keyfind(Supervisor.which_children(supervisor), id, 0) do
        {^id, pid, _, _} when is_pid(pid) -> {:ok, pid}
        nil -> {:error, :amqp10_not_enabled}
        _restarting -> {:error, :not_running}
      end
    catch
      :exit, _ -> {:error, :not_running}
    end
  end
end
