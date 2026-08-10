defmodule GravitonMQ.Core.NodeId do
  @moduledoc """
  Stable identity for a broker node such as a queue.
  """

  @enforce_keys [:value]
  defstruct [:value]

  @opaque t :: %__MODULE__{value: binary()}

  @spec new(binary()) :: t()
  def new(value) when is_binary(value), do: %__MODULE__{value: value}
end
