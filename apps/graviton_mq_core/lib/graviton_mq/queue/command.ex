defmodule GravitonMQ.Queue.Command do
  @moduledoc """
  Protocol-independent commands for the memory queue profile.

  The running queue owner allocates strictly increasing node-wide uint64
  registration generations. Replacing a consumer lease requeues its unresolved
  claims; disconnected slots are removed. A generation cannot be reused, even
  for another consumer identity. Exhaustion requires a new queue incarnation.

  Claim byte budgets cover charged content and retained metadata, rather than
  promising that a protocol adapter's later encoding will fit a wire frame.
  """

  @type generation :: non_neg_integer()
  @type t ::
          {:enqueue, term(), GravitonMQ.Core.Message.t()}
          | {:register_consumer, term(), binary(), generation()}
          | {:claim, term(), binary(), generation(), pos_integer(), pos_integer()}
          | {:settle, term(), binary(), generation(), GravitonMQ.Core.DeliveryRef.t(),
             GravitonMQ.Core.Outcome.t()}
          | {:disconnect, term(), binary(), generation()}
end
