defmodule GravitonMQ.MQTT.Codec.Error do
  @moduledoc "Structured codec failures; peer input is never rescued into success."

  @enforce_keys [:operation, :class, :reason]
  defstruct [:operation, :class, :reason, :offset, details: %{}]

  @type operation :: :frame_decode | :frame_encode | :packet_decode | :packet_encode
  @type class :: :malformed | :unsupported | :limit_exceeded | :invalid_value
  @type t :: %__MODULE__{
          operation: operation(),
          class: class(),
          reason: atom(),
          offset: non_neg_integer() | nil,
          details: map()
        }

  @doc false
  def new(operation, class, reason, details \\ %{}) do
    %__MODULE__{operation: operation, class: class, reason: reason, details: details}
  end
end
