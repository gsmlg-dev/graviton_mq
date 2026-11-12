defmodule GravitonMQ.AMQP10.SASL.Response do
  @moduledoc """
  Immutable `sasl-response` body (descriptor `0x00000000:0x00000043`).

  `response` may carry credentials; `Inspect` never prints it.

  Redaction covers `inspect/1` only. `inspect(body, structs: false)`, Erlang
  `~p` formatting, crash reports and `:sys.get_status/1` print the raw fields;
  callers must not keep this body in process state that can be printed.
  """

  @enforce_keys [:response]
  defstruct [:response]

  @type t :: %__MODULE__{response: GravitonMQ.AMQP10.Value.t()}
end

defimpl Inspect, for: GravitonMQ.AMQP10.SASL.Response do
  def inspect(_response, _options), do: "#GravitonMQ.AMQP10.SASL.Response<response: :redacted>"
end
