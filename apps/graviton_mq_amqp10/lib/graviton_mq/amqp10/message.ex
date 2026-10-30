defmodule GravitonMQ.AMQP10.Message do
  @moduledoc """
  AMQP 1.0 message data at the protocol boundary.

  `encoded_content` retains the original message bytes and `message_format`
  retains the enclosing Transfer value. `sections` is an optional parsed view
  of known sections using exact AMQP values. The G3 message codec adds that
  view without replacing the authoritative original bytes. Parsing remains
  excluded from the historical Milestone 0 record.
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

  @doc "Checks received annotation understanding without decoding or rewriting the body."
  @spec check_annotations(binary(), GravitonMQ.AMQP10.Codec.Limits.t()) ::
          :ok | {:error, :not_implemented | :malformed_annotations | :annotation_limit}
  def check_annotations(raw, limits \\ %GravitonMQ.AMQP10.Codec.Limits{}),
    do: GravitonMQ.AMQP10.AnnotationPolicy.check(raw, limits)
end
