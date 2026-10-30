defmodule GravitonMQ.AMQP10.Performative.Disposition do
  @moduledoc """
  Pure AMQP 1.0 Disposition data with exact tagged role and delivery numbers.

  An absent `last` means `first` in the endpoint. Sequence wraparound and
  delivery responsibility are interpreted there, not by this wire schema.
  """

  alias GravitonMQ.AMQP10.Value

  defstruct [:role, :first, :last, :settled, :state, :batchable]

  @type t :: %__MODULE__{
          role: Value.t(),
          first: Value.t(),
          last: Value.t() | nil,
          settled: Value.t() | nil,
          state: GravitonMQ.AMQP10.DeliveryState.t() | nil,
          batchable: Value.t() | nil
        }
end
