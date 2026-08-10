defmodule GravitonMQ.Core.MessageId do
  @moduledoc """
  Stable broker identity for a message, independent of protocol delivery IDs.
  """

  @enforce_keys [:value]
  defstruct [:value]

  @opaque t :: %__MODULE__{value: binary()}

  @spec new(binary()) :: t()
  def new(value) when is_binary(value), do: %__MODULE__{value: value}
end
