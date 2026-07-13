defmodule GravitonMQ.AMQP10.Message do
  @moduledoc """
  AMQP 1.0 message-section data at the protocol boundary.

  It remains distinct from `GravitonMQ.Core.Message`. Section parsing and
  binary encoding are excluded from Milestone 0.
  """

  defstruct sections: []

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

  @type section :: {section_name(), term()}
  @type t :: %__MODULE__{sections: [section()]}
end
