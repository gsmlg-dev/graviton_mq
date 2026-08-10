defmodule GravitonMQ.Core.Delivery do
  @moduledoc """
  Describes a protocol-independent attempt to deliver an internal message.

  Protocol frontends will translate their own delivery identifiers and
  settlement state to this broker lifecycle in later milestones.
  """

  @enforce_keys [:id, :message_id, :consumer_id]
  defstruct [:id, :message_id, :consumer_id, attempt: 1]

  @type t :: %__MODULE__{
          id: GravitonMQ.Core.DeliveryRef.t(),
          message_id: GravitonMQ.Core.Message.id(),
          consumer_id: binary(),
          attempt: pos_integer()
        }
end
