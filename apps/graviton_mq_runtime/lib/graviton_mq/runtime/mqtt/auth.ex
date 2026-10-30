defmodule GravitonMQ.Runtime.MQTT.Auth do
  @moduledoc """
  Host authentication and authorization policy for a memory MQTT broker.

  A policy is explicitly `:anonymous` or `{module, options}`. Anonymous access
  permits all actions in that instance. Module callbacks execute in temporary
  supervised tasks, outside protocol transitions, under a finite concurrency
  limit and deadline. Failure reasons never include callback data.

  `:subscribe` authorizes the entire filter; `:deliver` independently checks
  the concrete topic against the recipient principal before admission.
  """

  @type action :: :publish | :subscribe | :deliver
  @type policy :: :anonymous | {module(), keyword()}

  @callback authenticate(map(), keyword()) :: {:ok, term()} | {:error, term()}
  @callback authorize(term(), action(), binary(), keyword()) :: :ok | {:error, term()}

  @doc false
  def run(_supervisor, :anonymous, {:authenticate, _credentials}, _timeout),
    do: {:ok, :anonymous}

  def run(_supervisor, :anonymous, {:authorize, _principal, action, resource}, _timeout)
      when action in [:publish, :subscribe, :deliver] and is_binary(resource),
      do: :ok

  def run(supervisor, {module, options}, operation, timeout)
      when is_atom(module) and is_list(options) and is_integer(timeout) and timeout in 1..60_000 do
    caller = self()
    reference = make_ref()
    deadline = System.monotonic_time(:millisecond) + timeout

    case start_callback(supervisor, fn ->
           callback = self()
           spawn(fn -> watch(callback, caller, deadline) end)
           send(caller, {reference, invoke(module, options, operation)})
         end) do
      {:ok, worker} -> await(supervisor, worker, reference, deadline)
      {:error, :max_children} -> {:error, :callback_busy}
      {:error, _reason} -> {:error, :callback_failed}
    end
  end

  def run(_supervisor, _policy, _operation, _timeout), do: {:error, :invalid_policy}

  defp start_callback(supervisor, callback) do
    try do
      Task.Supervisor.start_child(supervisor, callback)
    catch
      :exit, _reason -> {:error, :callback_failed}
    end
  end

  # Supervise the actual callback, so its slot cannot disappear while host
  # code is still running. A separate monitor owns cancellation/deadline even
  # when the caller dies or the callback traps ordinary exit signals.
  defp watch(callback, caller, deadline) do
    callback_monitor = Process.monitor(callback)
    caller_monitor = Process.monitor(caller)

    receive do
      {:DOWN, ^callback_monitor, :process, ^callback, _reason} -> :ok
      {:DOWN, ^caller_monitor, :process, ^caller, _reason} -> Process.exit(callback, :kill)
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> Process.exit(callback, :kill)
    end
  end

  defp invoke(module, options, operation) do
    try do
      case operation do
        {:authenticate, credentials} ->
          case module.authenticate(credentials, options) do
            {:ok, principal} -> {:ok, principal}
            {:error, _reason} -> {:error, :denied}
            _invalid -> {:error, :invalid_callback_result}
          end

        {:authorize, principal, action, resource}
        when action in [:publish, :subscribe, :deliver] and is_binary(resource) ->
          case module.authorize(principal, action, resource, options) do
            :ok -> :ok
            {:error, _reason} -> {:error, :denied}
            _invalid -> {:error, :invalid_callback_result}
          end

        _invalid ->
          {:error, :invalid_operation}
      end
    catch
      _kind, _reason -> {:error, :callback_failed}
    end
  end

  defp await(supervisor, worker, reference, deadline) do
    monitor = Process.monitor(worker)

    result =
      receive do
        {^reference, result} ->
          result

        {:DOWN, ^monitor, :process, ^worker, _reason} ->
          receive do
            {^reference, result} -> result
          after
            0 ->
              if System.monotonic_time(:millisecond) >= deadline,
                do: {:error, :callback_timeout},
                else: {:error, :callback_failed}
          end
      after
        max(deadline - System.monotonic_time(:millisecond), 0) ->
          Process.exit(worker, :kill)

          receive do
            {^reference, _result} -> :ok
          after
            0 -> :ok
          end

          {:error, :callback_timeout}
      end

    try do
      Task.Supervisor.terminate_child(supervisor, worker)
    catch
      :exit, _reason -> :ok
    end

    Process.demonitor(monitor, [:flush])
    result
  end
end
