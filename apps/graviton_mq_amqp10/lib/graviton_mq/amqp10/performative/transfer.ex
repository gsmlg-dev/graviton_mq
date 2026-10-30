defmodule GravitonMQ.AMQP10.Performative.Transfer do
  @moduledoc """
  Pure AMQP 1.0 Transfer fields, separate from the opaque frame payload.

  Tagged `settled` may be nil: omission on a continuation transfer inherits
  the delivery's previous settlement state. Only the four specified false
  defaults are materialized by the codec. First-transfer requirements,
  fragmentation and settlement transitions belong to the endpoint machine.
  """

  alias GravitonMQ.AMQP10.Value

  defstruct [
    :handle,
    :delivery_id,
    :delivery_tag,
    :message_format,
    :settled,
    :more,
    :rcv_settle_mode,
    :state,
    :resume,
    :aborted,
    :batchable
  ]

  @type t :: %__MODULE__{
          handle: Value.t(),
          delivery_id: Value.t() | nil,
          delivery_tag: Value.binary_value() | nil,
          message_format: Value.t() | nil,
          settled: Value.t() | nil,
          more: Value.t() | nil,
          rcv_settle_mode: Value.t() | nil,
          state: GravitonMQ.AMQP10.DeliveryState.t() | nil,
          resume: Value.t() | nil,
          aborted: Value.t() | nil,
          batchable: Value.t() | nil
        }
end
