defmodule GravitonMQ.AMQP10.DeliveryState do
  @moduledoc """
  Pure AMQP 1.0 delivery-state data. Terminal states use the existing
  `GravitonMQ.AMQP10.Outcome` structs; Received describes nonterminal progress.
  No delivery, settlement, or runtime transition is performed here.
  """

  defmodule Received do
    @moduledoc "AMQP Received progress with exact tagged section number and offset."
    @enforce_keys [:section_number, :section_offset]
    defstruct [:section_number, :section_offset]

    @type t :: %__MODULE__{
            section_number: GravitonMQ.AMQP10.Value.t(),
            section_offset: GravitonMQ.AMQP10.Value.t()
          }
  end

  @type t :: Received.t() | GravitonMQ.AMQP10.Outcome.t()
end
