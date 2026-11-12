defmodule GravitonMQ.Test.StandInQueue do
  @moduledoc false
  # Test-only `{Queue, address}` child (S3 B4). It forwards every request to a
  # real Queue it owns, except a request kind it is told to hold: it *takes*
  # that request from the caller's exchange and never answers it, and stops
  # taking later requests until released. This models "the queue accepted the
  # request but has not answered", which `:sys.suspend/1` cannot model.
  # Every request goes through this process, so the real queue sees one
  # consistent controller.
  use GenServer

  alias GravitonMQ.Runtime.AMQP10.Queue

  def start_link(argument), do: GenServer.start_link(__MODULE__, argument)
  def hold(stand_in, kind), do: GenServer.call(stand_in, {:stand_in_hold, kind})
  def release(stand_in), do: GenServer.call(stand_in, :stand_in_release, 10_000)
  def queue(stand_in), do: GenServer.call(stand_in, :stand_in_queue)

  @impl true
  def init({config, address, observer}) do
    {:ok, queue} = Queue.start_link(fn -> {config, address} end)
    {:ok, %{queue: queue, observer: observer, hold: nil, held: nil, backlog: :queue.new()}}
  end

  @impl true
  def handle_call({:stand_in_hold, kind}, _from, state), do: {:reply, :ok, %{state | hold: kind}}
  def handle_call(:stand_in_queue, _from, state), do: {:reply, state.queue, state}

  def handle_call(:stand_in_release, _from, state) do
    state =
      case state.held do
        nil -> state
        {from, message, table, token} -> forward(state, from, message, table, token)
      end

    {:reply, :ok, drain(%{state | hold: nil, held: nil})}
  end

  def handle_call({:execute, table, token}, from, %{held: nil} = state),
    do: execute(state, from, table, token)

  def handle_call({:execute, table, token}, from, state) do
    # Reports the kind of a request waiting behind a held one, without taking
    # it, so a test can observe a re-probe.
    send(state.observer, {:stand_in_queued, self(), peek(table, token)})
    {:noreply, %{state | backlog: :queue.in({from, table, token}, state.backlog)}}
  end

  defp execute(state, from, table, token) do
    case take(table, token, elem(from, 0)) do
      {:ok, message} ->
        if kind(message) == state.hold do
          send(state.observer, {:stand_in_held, self(), kind(message)})
          {:noreply, %{state | held: {from, message, table, token}}}
        else
          {:noreply, forward(state, from, message, table, token)}
        end

      :abandoned ->
        {:reply, :ok, state}
    end
  end

  defp forward(state, from, message, table, token) do
    result = Queue.request(state.queue, message)

    try do
      :ets.insert(table, {token, {:result, result}})
    rescue
      ArgumentError -> :ok
    end

    GenServer.reply(from, :ok)
    state
  end

  defp drain(state) do
    case :queue.out(state.backlog) do
      {{:value, {from, table, token}}, backlog} ->
        case execute(%{state | backlog: backlog}, from, table, token) do
          {:reply, :ok, state} ->
            GenServer.reply(from, :ok)
            drain(state)

          {:noreply, state} ->
            drain(state)
        end

      {:empty, _} ->
        state
    end
  end

  defp take(table, token, caller) do
    if :ets.info(table, :owner) == caller do
      case :ets.take(table, token) do
        [{^token, {:request, message}}] -> {:ok, message}
        _ -> :abandoned
      end
    else
      :abandoned
    end
  rescue
    ArgumentError -> :abandoned
  end

  defp peek(table, token) do
    case :ets.lookup(table, token) do
      [{^token, {:request, message}}] -> kind(message)
      _ -> :abandoned
    end
  rescue
    ArgumentError -> :abandoned
  end

  defp kind(message) when is_tuple(message), do: elem(message, 0)
  defp kind(message), do: message
end

defmodule GravitonMQ.Test.StandInAMQP do
  @moduledoc false
  # The production AMQP child list with one queue replaced by the stand-in.
  # The Broker reads `{Queue, address}` through `Supervisor.which_children/1`
  # in `handle_continue(:wire)`, so it talks to the stand-in.
  use Supervisor

  alias GravitonMQ.Runtime.AMQP10.{Config, Queue}

  def start_link(argument), do: Supervisor.start_link(__MODULE__, argument)

  @impl true
  def init({options, address, observer}) do
    {:ok, config} = Config.build(options)
    {:ok, {flags, children}} = GravitonMQ.Runtime.AMQP10.Supervisor.init(fn -> config end)
    queue_config = Map.delete(config, :auth)

    children =
      Enum.map(children, fn
        %{id: {Queue, ^address}} = child ->
          %{
            child
            | start:
                {GravitonMQ.Test.StandInQueue, :start_link, [{queue_config, address, observer}]}
          }

        child ->
          child
      end)

    {:ok, {flags, children}}
  end
end
