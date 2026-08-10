defmodule GravitonMQ.AMQP10.Performative.Begin do
  @moduledoc """
  Immutable AMQP 1.0 Begin performative fields in specification order.

  Fields retain their exact tagged AMQP semantic values. `nil` represents an
  omitted or explicitly null optional field; the schema decoder materializes
  the protocol default for `handle_max`. Session transition rules are outside
  this data model.
  """

  alias GravitonMQ.AMQP10.Value, as: AMQPValue
  alias GravitonMQ.AMQP10.Value.Array

  defstruct [
    :remote_channel,
    :next_outgoing_id,
    :incoming_window,
    :outgoing_window,
    :handle_max,
    :offered_capabilities,
    :desired_capabilities,
    :properties
  ]

  @type uint_value :: %AMQPValue{type: :uint, value: 0..4_294_967_295}
  @type ushort_value :: %AMQPValue{type: :ushort, value: 0..65_535}
  @type symbol_array_value :: %AMQPValue{
          type: :array,
          value: %Array{element_type: :symbol, values: [AMQPValue.symbol_value()]}
        }
  @type multiple_symbol_value :: AMQPValue.symbol_value() | symbol_array_value()
  @type properties_value :: %AMQPValue{
          type: :map,
          value: [{AMQPValue.symbol_value(), AMQPValue.t()}]
        }

  @type t :: %__MODULE__{
          remote_channel: ushort_value() | nil,
          next_outgoing_id: uint_value(),
          incoming_window: uint_value(),
          outgoing_window: uint_value(),
          handle_max: uint_value() | nil,
          offered_capabilities: multiple_symbol_value() | nil,
          desired_capabilities: multiple_symbol_value() | nil,
          properties: properties_value() | nil
        }
end
