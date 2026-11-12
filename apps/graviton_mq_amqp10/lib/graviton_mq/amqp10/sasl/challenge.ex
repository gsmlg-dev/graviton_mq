defmodule GravitonMQ.AMQP10.SASL.Challenge do
  @moduledoc """
  Immutable `sasl-challenge` body (descriptor `0x00000000:0x00000042`).

  `challenge` may carry credentials; `Inspect` never prints it.

  Redaction covers `inspect/1` only. `inspect(body, structs: false)`, Erlang
  `~p` formatting, crash reports and `:sys.get_status/1` print the raw fields;
  callers must not keep this body in process state that can be printed.
  """

  @enforce_keys [:challenge]
  defstruct [:challenge]

  @type t :: %__MODULE__{challenge: GravitonMQ.AMQP10.Value.t()}
end

defimpl Inspect, for: GravitonMQ.AMQP10.SASL.Challenge do
  def inspect(_challenge, _options), do: "#GravitonMQ.AMQP10.SASL.Challenge<challenge: :redacted>"
end
