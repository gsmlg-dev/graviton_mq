defmodule GravitonMQ.Runtime.AMQP10.Authenticator do
  @moduledoc false
  # Holds the host authentication policy outside every published Connection
  # specification (Listener's ETS spec is readable by any process) and starts
  # each callback as a temporary child of `:sasl_tasks`. Credentials pass
  # through this sensitive process only as call arguments and are never
  # stored in its state; temporary Task.Supervisor children do not retain
  # their start arguments. The Broker/Manager never sees credentials.
  use GenServer

  alias GravitonMQ.Runtime.AMQP10.Auth

  @call_timeout 1_000

  def start_link(configuration), do: GenServer.start_link(__MODULE__, configuration)

  def child_spec(configuration),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [fn -> configuration end]}}

  @spec start(pid() | nil, map(), pid(), term(), pos_integer()) ::
          {:ok, pid()} | {:error, :busy | :unavailable}
  def start(nil, _credentials, _reply_to, _reference, _timeout), do: {:error, :unavailable}

  def start(authenticator, credentials, reply_to, reference, timeout) do
    GenServer.call(
      authenticator,
      {:start, credentials, reply_to, reference, timeout},
      @call_timeout
    )
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  @impl true
  def init(configuration) do
    Process.flag(:sensitive, true)
    {policy, parent} = configuration.()
    {:ok, %{policy: policy, parent: parent, tasks: nil}, {:continue, :wire}}
  end

  @impl true
  def handle_continue(:wire, state) do
    {_, tasks, _, _} = List.keyfind(Supervisor.which_children(state.parent), :sasl_tasks, 0)
    {:noreply, %{state | tasks: tasks}}
  end

  @impl true
  def handle_call(
        {:start, credentials, reply_to, reference, timeout},
        _from,
        %{policy: {module, options}, tasks: tasks} = state
      )
      when is_pid(tasks) and is_pid(reply_to) and is_integer(timeout) and timeout > 0 do
    deadline = System.monotonic_time(:millisecond) + timeout

    callback = fn ->
      Process.flag(:sensitive, true)
      task = self()
      spawn(fn -> Auth.watch(task, reply_to, deadline) end)
      send(reply_to, {:amqp10_sasl_result, reference, Auth.invoke(module, options, credentials)})
    end

    reply =
      try do
        case Task.Supervisor.start_child(tasks, callback) do
          {:ok, pid} -> {:ok, pid}
          {:error, :max_children} -> {:error, :busy}
          _other -> {:error, :unavailable}
        end
      catch
        :exit, _reason -> {:error, :unavailable}
      end

    {:reply, reply, state}
  end

  def handle_call(_request, _from, state), do: {:reply, {:error, :unavailable}, state}

  @impl true
  def format_status(status),
    do: Map.merge(status, %{state: :redacted, message: :redacted, reason: :redacted, log: []})
end
