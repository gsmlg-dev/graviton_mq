defmodule GravitonMQ.AMQP10.Codec.Limits do
  @moduledoc """
  Resource bounds for the pure AMQP 1.0 codec foundation.

  Limits keep frame and value work bounded before Open and Begin values are
  interpreted by later protocol layers.
  """

  alias GravitonMQ.AMQP10.Codec.Error

  defstruct max_frame_size: 16_777_216,
            max_value_bytes: 16_777_216,
            max_compound_items: 65_536,
            max_nesting_depth: 32

  @type t :: %__MODULE__{
          max_frame_size: pos_integer(),
          max_value_bytes: pos_integer(),
          max_compound_items: pos_integer(),
          max_nesting_depth: pos_integer()
        }

  @spec default() :: t()
  def default, do: %__MODULE__{}

  @spec validate(t(), Error.operation()) :: {:ok, t()} | {:error, Error.t()}
  def validate(
        %__MODULE__{
          max_frame_size: max_frame_size,
          max_value_bytes: max_value_bytes,
          max_compound_items: max_compound_items,
          max_nesting_depth: max_nesting_depth
        } = limits,
        _operation
      )
      when is_integer(max_frame_size) and max_frame_size > 0 and
             is_integer(max_value_bytes) and max_value_bytes > 0 and
             max_value_bytes <= 0xFFFF_FFFF and
             is_integer(max_compound_items) and max_compound_items > 0 and
             max_compound_items <= 0xFFFF_FFFF and
             is_integer(max_nesting_depth) and max_nesting_depth > 0 do
    {:ok, limits}
  end

  def validate(%__MODULE__{} = limits, operation) do
    {:error,
     Error.new(operation, :invalid_value, :invalid_limits, details: Map.from_struct(limits))}
  end
end
