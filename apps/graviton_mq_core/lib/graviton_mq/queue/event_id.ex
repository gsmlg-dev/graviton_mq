defmodule GravitonMQ.Queue.EventId do
  @moduledoc """
  Stable identity for one protocol-independent logical queue event.
  """

  @enforce_keys [:value]
  defstruct [:value]

  @opaque t :: %__MODULE__{value: binary()}

  @spec new(binary()) :: t()
  def new(value) when is_binary(value), do: %__MODULE__{value: value}
end
