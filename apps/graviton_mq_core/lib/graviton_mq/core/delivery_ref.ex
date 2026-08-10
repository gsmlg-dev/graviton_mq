defmodule GravitonMQ.Core.DeliveryRef do
  @moduledoc """
  Stable internal reference for a broker delivery attempt or lifecycle.

  It is not an AMQP delivery ID, handle, channel, PID, or Erlang reference.
  """

  @enforce_keys [:value]
  defstruct [:value]

  @opaque t :: %__MODULE__{value: binary()}

  @spec new(binary()) :: t()
  def new(value) when is_binary(value), do: %__MODULE__{value: value}
end
