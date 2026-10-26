defmodule GravitonMQ.MQTT.Codec.Limits do
  @moduledoc "Bounds for total packet size, individual fields, and property count."

  alias GravitonMQ.MQTT.Codec.Error

  defstruct max_packet_size: 1_048_576, max_field_bytes: 65_535, max_properties: 256

  @type t :: %__MODULE__{
          max_packet_size: pos_integer(),
          max_field_bytes: non_neg_integer(),
          max_properties: non_neg_integer()
        }

  @spec default() :: t()
  def default, do: %__MODULE__{}

  @doc false
  def validate(
        %__MODULE__{max_packet_size: packet, max_field_bytes: field, max_properties: count} =
          limits,
        _operation
      )
      when is_integer(packet) and packet in 1..268_435_460 and
             is_integer(field) and field in 0..65_535 and
             is_integer(count) and count in 0..268_435_455,
      do: {:ok, limits}

  def validate(_limits, operation),
    do: {:error, Error.new(operation, :invalid_value, :invalid_limits)}
end
