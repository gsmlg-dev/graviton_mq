defmodule GravitonMQ.Core.Address do
  @moduledoc """
  Identifies a broker node without embedding protocol-specific address types.

  Protocol frontends are responsible for resolving their addresses into this
  representation.
  """

  @enforce_keys [:value]
  defstruct [:value]

  @type t :: %__MODULE__{value: String.t()}
end
