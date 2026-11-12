defmodule GravitonMQ.AMQP10.SASL.Mechanisms do
  @moduledoc """
  Immutable `sasl-mechanisms` body (descriptor `0x00000000:0x00000040`).

  `server_mechanisms` keeps the exact tagged wire form — one `symbol` or an
  `array` of `symbol` — ordered by decreasing server preference.
  """

  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Value.Array

  @enforce_keys [:server_mechanisms]
  defstruct [:server_mechanisms]

  @type t :: %__MODULE__{server_mechanisms: Value.t()}

  @doc "Mechanism names in wire order; names are data and may be unselectable."
  @spec names(t()) :: [binary()]
  def names(%__MODULE__{server_mechanisms: %Value{type: :symbol, value: name}})
      when is_binary(name),
      do: [name]

  def names(%__MODULE__{server_mechanisms: %Value{type: :array, value: %Array{values: values}}}),
    do: for(%Value{type: :symbol, value: name} when is_binary(name) <- values, do: name)

  def names(%__MODULE__{}), do: []
end
