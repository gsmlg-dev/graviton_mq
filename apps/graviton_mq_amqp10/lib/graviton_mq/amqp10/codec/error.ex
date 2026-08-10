defmodule GravitonMQ.AMQP10.Codec.Error do
  @moduledoc """
  Structured failures returned by the pure AMQP 1.0 codec.

  Codec errors are data rather than exceptions so malformed or unsupported
  peer input can be reported without escaping the caller's control flow.
  """

  @enforce_keys [:operation, :class, :reason]
  defstruct [:operation, :class, :reason, :offset, details: %{}]

  @operations [
    :protocol_header,
    :frame_decode,
    :value_decode,
    :value_encode,
    :performative_decode,
    :performative_encode
  ]
  @classes [:malformed, :unsupported, :limit_exceeded, :invalid_value]

  @type operation ::
          :protocol_header
          | :frame_decode
          | :value_decode
          | :value_encode
          | :performative_decode
          | :performative_encode
  @type class :: :malformed | :unsupported | :limit_exceeded | :invalid_value
  @type t :: %__MODULE__{
          operation: operation(),
          class: class(),
          reason: atom(),
          offset: non_neg_integer() | nil,
          details: map()
        }

  @spec new(operation(), class(), atom(), keyword()) :: t()
  def new(operation, class, reason, options \\ []) do
    unless operation in @operations do
      raise ArgumentError,
            "expected :operation to be one of #{inspect(@operations)}, got: #{inspect(operation)}"
    end

    unless class in @classes do
      raise ArgumentError,
            "expected :class to be one of #{inspect(@classes)}, got: #{inspect(class)}"
    end

    unless is_atom(reason) do
      raise ArgumentError, "expected :reason to be an atom, got: #{inspect(reason)}"
    end

    offset =
      case Keyword.get(options, :offset) do
        offset when is_nil(offset) or (is_integer(offset) and offset >= 0) ->
          offset

        offset ->
          raise ArgumentError,
                "expected :offset to be nil or a non-negative integer, got: #{inspect(offset)}"
      end

    details =
      case Keyword.get(options, :details, %{}) do
        details when is_map(details) ->
          details

        details ->
          raise ArgumentError, "expected :details to be a map, got: #{inspect(details)}"
      end

    %__MODULE__{
      operation: operation,
      class: class,
      reason: reason,
      offset: offset,
      details: details
    }
  end
end
