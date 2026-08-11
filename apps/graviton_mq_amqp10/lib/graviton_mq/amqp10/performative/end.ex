defmodule GravitonMQ.AMQP10.Performative.End do
  @moduledoc """
  Immutable AMQP 1.0 End performative data.

  This struct does not end a Session. It retains only the optional protocol
  error decoded by the pure schema codec.
  """

  defstruct error: nil

  @type t :: %__MODULE__{
          error: GravitonMQ.AMQP10.Error.t() | nil
        }
end
