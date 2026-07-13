defmodule GravitonMQ.Core.Delivery do
  @moduledoc """
  Describes a protocol-independent attempt to deliver an internal message.

  Protocol frontends will translate their own delivery identifiers and
  settlement state to this broker lifecycle in later milestones.
  """

  @enforce_keys [:message_id, :consumer_ref]
  defstruct [:message_id, :consumer_ref, attempt: 1]

  @type t :: %__MODULE__{
          message_id: GravitonMQ.Core.Message.id(),
          consumer_ref: term(),
          attempt: pos_integer()
        }
end
