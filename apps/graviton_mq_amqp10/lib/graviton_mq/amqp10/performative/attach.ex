defmodule GravitonMQ.AMQP10.Performative.Attach do
  @moduledoc """
  Immutable AMQP 1.0 Attach fields in specification order.

  Scalar and extension fields retain exact tagged AMQP values. Standard Source
  and Target have dedicated immutable structs; described extensions remain
  tagged values. No Link, handle, or settlement
  ownership is established by constructing or decoding this data.
  """

  alias GravitonMQ.AMQP10.Terminus.{Source, Target}
  alias GravitonMQ.AMQP10.Value

  defstruct [
    :name,
    :handle,
    :role,
    :snd_settle_mode,
    :rcv_settle_mode,
    :source,
    :target,
    :unsettled,
    :incomplete_unsettled,
    :initial_delivery_count,
    :max_message_size,
    :offered_capabilities,
    :desired_capabilities,
    :properties
  ]

  @type t :: %__MODULE__{
          name: Value.string_value(),
          handle: Value.t(),
          role: Value.t(),
          snd_settle_mode: Value.t() | nil,
          rcv_settle_mode: Value.t() | nil,
          source: Source.t() | Value.described_value() | nil,
          target: Target.t() | Value.described_value() | nil,
          unsettled: Value.map_value() | nil,
          incomplete_unsettled: Value.t() | nil,
          initial_delivery_count: Value.t() | nil,
          max_message_size: Value.ulong_value() | nil,
          offered_capabilities: Value.t() | nil,
          desired_capabilities: Value.t() | nil,
          properties: Value.map_value() | nil
        }
end
