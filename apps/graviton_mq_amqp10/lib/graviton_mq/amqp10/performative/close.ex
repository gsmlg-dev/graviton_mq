defmodule GravitonMQ.AMQP10.Performative.Close do
  @moduledoc """
  Immutable AMQP 1.0 Close performative data.

  This struct does not close a Connection. It retains only the optional
  protocol error decoded by the pure schema codec.
  """

  defstruct error: nil

  @type t :: %__MODULE__{
          error: GravitonMQ.AMQP10.Error.t() | nil
        }
end
