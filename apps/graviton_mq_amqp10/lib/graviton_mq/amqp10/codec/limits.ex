defmodule GravitonMQ.AMQP10.Codec.Limits do
  @moduledoc """
  Resource bounds for the pure AMQP 1.0 codec foundation.

  Limits keep frame and value work bounded before Open and Begin values are
  interpreted by later protocol layers.

  `max_zero_width_elements` bounds array elements whose constructor carries
  no element bytes (`0x41`-`0x45`). Every other decoded value consumes at
  least one input byte, so decode work is otherwise linear in the bounded
  input. The budget is cumulative across nesting within one top-level decode,
  and multi-value decoders share one budget per input. The default equals one
  maximal `max_compound_items` array of constants.
  """

  alias GravitonMQ.AMQP10.Codec.Error

  defstruct max_frame_size: 16_777_216,
            max_value_bytes: 16_777_216,
            max_compound_items: 65_536,
            max_nesting_depth: 32,
            max_zero_width_elements: 65_536

  @type t :: %__MODULE__{
          max_frame_size: pos_integer(),
          max_value_bytes: pos_integer(),
          max_compound_items: pos_integer(),
          max_nesting_depth: pos_integer(),
          max_zero_width_elements: non_neg_integer()
        }

  @spec default() :: t()
  def default, do: %__MODULE__{}

  @spec validate(t(), Error.operation()) :: {:ok, t()} | {:error, Error.t()}
  def validate(
        %__MODULE__{
          max_frame_size: max_frame_size,
          max_value_bytes: max_value_bytes,
          max_compound_items: max_compound_items,
          max_nesting_depth: max_nesting_depth,
          max_zero_width_elements: max_zero_width_elements
        } = limits,
        _operation
      )
      when is_integer(max_frame_size) and max_frame_size > 0 and
             is_integer(max_value_bytes) and max_value_bytes > 0 and
             max_value_bytes <= 0xFFFF_FFFF and
             is_integer(max_compound_items) and max_compound_items > 0 and
             max_compound_items <= 0xFFFF_FFFF and
             is_integer(max_nesting_depth) and max_nesting_depth > 0 and
             is_integer(max_zero_width_elements) and max_zero_width_elements >= 0 do
    {:ok, limits}
  end

  def validate(%__MODULE__{} = limits, operation) do
    {:error,
     Error.new(operation, :invalid_value, :invalid_limits, details: Map.from_struct(limits))}
  end
end
