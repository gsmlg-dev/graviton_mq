defmodule GravitonMQ.AMQP10.Error do
  @moduledoc """
  Data representation of an AMQP 1.0 error condition.

  This is protocol data, not an exception policy for broker-core failures.
  """

  @enforce_keys [:condition]
  defstruct [:condition, :description, :info]

  @type t :: %__MODULE__{
          condition: GravitonMQ.AMQP10.Types.symbol(),
          description: GravitonMQ.AMQP10.Value.string_value() | nil,
          info: GravitonMQ.AMQP10.Value.map_value() | nil
        }
end
