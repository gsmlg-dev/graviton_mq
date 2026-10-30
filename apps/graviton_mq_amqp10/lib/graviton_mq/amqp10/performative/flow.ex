defmodule GravitonMQ.AMQP10.Performative.Flow do
  @moduledoc """
  Immutable AMQP 1.0 Flow performative fields in specification order.

  Fields retain exact tagged AMQP values. `nil` represents an omitted or
  explicitly null optional field; decoding materializes the false defaults
  for `drain` and `echo`. Session windows, Link credit, and context-dependent
  requirements are interpreted by later protocol state machines.
  """

  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  defstruct [
    :next_incoming_id,
    :incoming_window,
    :next_outgoing_id,
    :outgoing_window,
    :handle,
    :delivery_count,
    :link_credit,
    :available,
    :drain,
    :echo,
    :properties
  ]

  @type uint_value :: %AMQPValue{type: :uint, value: 0..4_294_967_295}
  @type boolean_value :: %AMQPValue{type: :boolean, value: boolean()}
  @type properties_value :: %AMQPValue{
          type: :map,
          value: [{AMQPValue.symbol_value(), AMQPValue.t()}]
        }

  @type t :: %__MODULE__{
          next_incoming_id: uint_value() | nil,
          incoming_window: uint_value(),
          next_outgoing_id: uint_value(),
          outgoing_window: uint_value(),
          handle: uint_value() | nil,
          delivery_count: uint_value() | nil,
          link_credit: uint_value() | nil,
          available: uint_value() | nil,
          drain: boolean_value() | nil,
          echo: boolean_value() | nil,
          properties: properties_value() | nil
        }
end
