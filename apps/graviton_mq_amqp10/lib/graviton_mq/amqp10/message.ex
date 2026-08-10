defmodule GravitonMQ.AMQP10.Message do
  @moduledoc """
  AMQP 1.0 message data at the protocol boundary.

  `encoded_content` retains the original message bytes and `message_format`
  retains the enclosing Transfer value. `sections` is a future parsed view of
  known sections and uses exact AMQP values. Parsing and encoding are excluded
  from Milestone 0.
  """

  @enforce_keys [:message_format, :encoded_content]
  defstruct [:message_format, :encoded_content, sections: []]

  @type section_name ::
          :header
          | :delivery_annotations
          | :message_annotations
          | :properties
          | :application_properties
          | :data
          | :amqp_sequence
          | :amqp_value
          | :footer

  @type section :: {section_name(), GravitonMQ.AMQP10.Value.t()}
  @type t :: %__MODULE__{
          message_format: GravitonMQ.AMQP10.Types.uint(),
          encoded_content: binary(),
          sections: [section()]
        }
end
