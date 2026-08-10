defmodule GravitonMQ.Core.Storage do
  @moduledoc """
  Mandatory behaviour for protocol-independent logical event durability.

  `append/2` accepts a non-empty ordered batch and returns the inclusive commit
  reference assigned to its final event. Append success establishes ordering,
  not durability. `sync/2` requests durability through a previously returned
  reference, and its returned durable-through reference is the highest
  contiguous position known durable in that stream. Batches never reorder
  events, and references from different streams are not comparable.

  Expected failures return `{:error, reason}` and must not be converted into a
  successful durability acknowledgement. `fold/4` starts with the first
  record for `:origin`, or strictly after the supplied commit reference. It
  yields ascending commit references and logical queue events incrementally so
  checkpointed recovery can resume without replaying the checkpoint or loading
  a complete log into memory. Concrete storage encodes and decodes its own
  physical record representation behind this boundary.

  `backend`, error reasons, reducers, and accumulators are call-scoped opaque
  values. They are not durable broker identities and must not be written into
  logical events.
  """

  @type backend :: term()
  @type reason :: term()
  @type accumulator :: term()
  @type start_position :: :origin | GravitonMQ.Core.CommitRef.t()
  @type reducer ::
          (GravitonMQ.Core.CommitRef.t(), GravitonMQ.Queue.Event.t(), accumulator() ->
             {:cont, accumulator()} | {:halt, accumulator()})

  @callback append(backend(), nonempty_list(GravitonMQ.Queue.Event.t())) ::
              {:ok, GravitonMQ.Core.CommitRef.t()} | {:error, reason()}
  @callback sync(backend(), GravitonMQ.Core.CommitRef.t()) ::
              {:ok, GravitonMQ.Core.CommitRef.t()} | {:error, reason()}
  @callback durable_through(backend()) ::
              {:ok, GravitonMQ.Core.CommitRef.t() | :none} | {:error, reason()}
  @callback fold(backend(), start_position(), accumulator(), reducer()) ::
              {:ok, accumulator()} | {:error, reason()}
end
