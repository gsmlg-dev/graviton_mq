defmodule GravitonMQ.AMQP10.Performative.Open do
  @moduledoc """
  Immutable AMQP 1.0 Open performative fields in specification order.

  Fields retain their exact tagged AMQP semantic values. `nil` represents an
  omitted or explicitly null optional field; the schema decoder materializes
  the protocol defaults for `max_frame_size` and `channel_max`.
  """

  alias GravitonMQ.AMQP10.Value, as: AMQPValue
  alias GravitonMQ.AMQP10.Value.Array

  defstruct [
    :container_id,
    :hostname,
    :max_frame_size,
    :channel_max,
    :idle_time_out,
    :outgoing_locales,
    :incoming_locales,
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
          container_id: AMQPValue.string_value(),
          hostname: AMQPValue.string_value() | nil,
          max_frame_size: %AMQPValue{type: :uint, value: 512..4_294_967_295} | nil,
          channel_max: ushort_value() | nil,
          idle_time_out: uint_value() | nil,
          outgoing_locales: multiple_symbol_value() | nil,
          incoming_locales: multiple_symbol_value() | nil,
          offered_capabilities: multiple_symbol_value() | nil,
          desired_capabilities: multiple_symbol_value() | nil,
          properties: properties_value() | nil
        }
end
