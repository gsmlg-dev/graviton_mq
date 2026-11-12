defmodule GravitonMQ.AMQP10.SASL.Response do
  @moduledoc "Immutable `sasl-response` body (descriptor `0x00000000:0x00000043`). Never inspected."

  @enforce_keys [:response]
  defstruct [:response]

  @type t :: %__MODULE__{response: GravitonMQ.AMQP10.Value.t()}
end

defimpl Inspect, for: GravitonMQ.AMQP10.SASL.Response do
  def inspect(_response, _options), do: "#GravitonMQ.AMQP10.SASL.Response<response: :redacted>"
end
