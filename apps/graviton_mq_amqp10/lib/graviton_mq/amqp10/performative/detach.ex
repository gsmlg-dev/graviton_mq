defmodule GravitonMQ.AMQP10.Performative.Detach do
  @moduledoc """
  Immutable AMQP 1.0 Detach performative data.

  This struct does not detach a Link or interpret its handle in Session state.
  It retains exact tagged fields and an optional nested protocol error.
  """

  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  defstruct handle: nil, closed: nil, error: nil

  @type uint_value :: %AMQPValue{type: :uint, value: 0..4_294_967_295}
  @type boolean_value :: %AMQPValue{type: :boolean, value: boolean()}

  @type t :: %__MODULE__{
          handle: uint_value(),
          closed: boolean_value() | nil,
          error: GravitonMQ.AMQP10.Error.t() | nil
        }
end
