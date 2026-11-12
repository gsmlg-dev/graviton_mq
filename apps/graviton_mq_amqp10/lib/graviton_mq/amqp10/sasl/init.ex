defmodule GravitonMQ.AMQP10.SASL.Init do
  @moduledoc """
  Immutable `sasl-init` body (descriptor `0x00000000:0x00000041`).

  `initial_response` may carry credentials; `Inspect` never prints it.

  Redaction covers `inspect/1` only. `inspect(body, structs: false)`, Erlang
  `~p` formatting, crash reports and `:sys.get_status/1` print the raw fields;
  callers must not keep this body in process state that can be printed.
  """

  @enforce_keys [:mechanism]
  defstruct [:mechanism, initial_response: nil, hostname: nil]

  @type t :: %__MODULE__{
          mechanism: GravitonMQ.AMQP10.Value.t(),
          initial_response: GravitonMQ.AMQP10.Value.t() | nil,
          hostname: GravitonMQ.AMQP10.Value.t() | nil
        }
end

defimpl Inspect, for: GravitonMQ.AMQP10.SASL.Init do
  def inspect(%{mechanism: mechanism}, _options) do
    "#GravitonMQ.AMQP10.SASL.Init<mechanism: " <>
      GravitonMQ.AMQP10.SASL.inspect_name(mechanism) <> ", initial_response: :redacted>"
  end
end
