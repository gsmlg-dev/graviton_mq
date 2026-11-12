defmodule GravitonMQ.Runtime.AMQP10.QueueRequests do
  @moduledoc false

  # Pure bookkeeping for the Manager's asynchronous queue requests. It has no
  # process, clock or ETS access: callers pass `now` and the list returned by
  # `:ets.take/2`, and act on the entries this module returns.
  #
  # Each queue keeps a FIFO of pending entries. The Manager issues requests to
  # one queue in order and the queue serves its mailbox in order, so deadlines
  # within one queue never decrease and only the head needs a timer.
  #
  # An expired or abandoned entry moves to `waiting` and stays counted until
  # its late reply (or the queue's DOWN) arrives. A queue that never answers
  # therefore cannot make the Manager's bookkeeping grow without bound.
  #
  # Exchange wrappers (written by Queue): `{token, {:request, message}}` until
  # the queue takes it; `{token, {:result, result}}` once executed.

  @default_max 4_096

  defstruct pending: %{}, waiting: %{}, size: 0, next_id: 0, max: @default_max

  @type t :: %__MODULE__{}
  @type outcome :: :not_executed | :uncertain | {:known, term()}

  @spec new(pos_integer()) :: t()
  def new(max \\ @default_max) when is_integer(max) and max > 0, do: %__MODULE__{max: max}

  def size(%__MODULE__{size: size}), do: size
  def full?(%__MODULE__{size: size, max: max}), do: size >= max

  def add(%__MODULE__{} = requests, queue, %{token: _, kind: _} = fields, deadline)
      when is_integer(deadline) do
    if full?(requests) do
      {:error, :full}
    else
      entry =
        %{key: nil, route_token: nil, data: %{}}
        |> Map.merge(fields)
        |> Map.merge(%{id: requests.next_id, queue: queue, deadline: deadline})

      fifo = Map.get(requests.pending, queue, :queue.new())

      {:ok, entry,
       %{
         requests
         | pending: Map.put(requests.pending, queue, :queue.in(entry, fifo)),
           size: requests.size + 1,
           next_id: requests.next_id + 1
       }}
    end
  end

  # A reply or queue DOWN for request `id`.
  def resolve(%__MODULE__{} = requests, queue, id) do
    case Map.pop(requests.waiting, id) do
      {%{} = entry, waiting} ->
        {:waiting, entry, %{requests | waiting: waiting, size: requests.size - 1}}

      {nil, _waiting} ->
        case Enum.split_with(entries(requests, queue), &(&1.id == id)) do
          {[entry], rest} ->
            {:pending, entry, put(%{requests | size: requests.size - 1}, queue, rest)}

          {[], _rest} ->
            {:unknown, requests}
        end
    end
  end

  # Due head entries, oldest first. They wait for their late reply.
  def expire(%__MODULE__{} = requests, queue, now) when is_integer(now) do
    {expired, rest} = Enum.split_while(entries(requests, queue), &(&1.deadline <= now))
    {expired, requests |> put(queue, rest) |> wait(expired)}
  end

  # Every pending entry of `queue` (a fence), oldest first.
  def abandon_all(%__MODULE__{} = requests, queue) do
    abandoned = entries(requests, queue)
    {abandoned, requests |> put(queue, []) |> wait(abandoned)}
  end

  def next_deadline(%__MODULE__{} = requests, queue) do
    case :queue.peek(Map.get(requests.pending, queue, :queue.new())) do
      {:value, entry} -> entry.deadline
      :empty -> nil
    end
  end

  def pending?(%__MODULE__{} = requests, queue, kind),
    do: Enum.any?(entries(requests, queue), &(&1.kind == kind))

  @spec classify(list()) :: outcome()
  def classify([{_token, {:request, _message}}]), do: :not_executed
  def classify([{_token, {:result, result}}]), do: {:known, result}
  def classify([]), do: :uncertain

  defp entries(requests, queue),
    do: requests.pending |> Map.get(queue, :queue.new()) |> :queue.to_list()

  defp put(requests, queue, []), do: %{requests | pending: Map.delete(requests.pending, queue)}

  defp put(requests, queue, entries),
    do: %{requests | pending: Map.put(requests.pending, queue, :queue.from_list(entries))}

  defp wait(requests, entries),
    do: %{requests | waiting: Enum.reduce(entries, requests.waiting, &Map.put(&2, &1.id, &1))}
end
