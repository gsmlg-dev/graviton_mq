defmodule GravitonMQ.AMQP10.SASL.Challenge do
  @moduledoc "Immutable `sasl-challenge` body (descriptor `0x00000000:0x00000042`). Never inspected."

  @enforce_keys [:challenge]
  defstruct [:challenge]

  @type t :: %__MODULE__{challenge: GravitonMQ.AMQP10.Value.t()}
end

defimpl Inspect, for: GravitonMQ.AMQP10.SASL.Challenge do
  def inspect(_challenge, _options), do: "#GravitonMQ.AMQP10.SASL.Challenge<challenge: :redacted>"
end
