defmodule GravitonMQ.Queue.Effect do
  @moduledoc """
  Data returned by queue transitions for a running owner to interpret.

  Operations correlate local commands only. Enqueue returns `:enqueued`,
  registration `:registered`, settlement `:settled`, disconnect `:disconnected`,
  and claim the number of emitted delivery effects. Enqueue success is memory
  acceptance, with no storage or disk durability claim.

  A delivery transfers local responsibility to the named consumer lease, not
  proof of peer receipt. Metadata carries `generation`, `attempt`, cumulative
  `failure_count`, the last Modified flags `delivery_failed?` and
  `undeliverable_here?`, ordered opaque `mutations`, and `charged_bytes`.
  Mutation deltas must be applied in order by the protocol adapter; original
  message bytes remain authoritative. Availability is a wake-up hint, not a
  credit grant. Logical events contain no operation tokens or runtime identity.
  """

  @type t ::
          {:operation, term(), {:ok, atom() | non_neg_integer()} | {:error, atom()}}
          | {:delivery, GravitonMQ.Core.Delivery.t(), GravitonMQ.Core.Message.t(), map()}
          | {:available, GravitonMQ.Core.NodeId.t()}
          | {:event, GravitonMQ.Queue.Event.t()}
end
