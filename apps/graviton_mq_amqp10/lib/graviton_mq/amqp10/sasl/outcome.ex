defmodule GravitonMQ.AMQP10.SASL.Outcome do
  @moduledoc """
  Immutable `sasl-outcome` body (descriptor `0x00000000:0x00000044`).

  `code` is a tagged `ubyte` and is not whitelisted (5..255 are data).

  Redaction covers `inspect/1` only. `inspect(body, structs: false)`, Erlang
  `~p` formatting, crash reports and `:sys.get_status/1` print the raw fields;
  callers must not keep this body in process state that can be printed.
  """

  @enforce_keys [:code]
  defstruct [:code, additional_data: nil]

  @type t :: %__MODULE__{
          code: GravitonMQ.AMQP10.Value.t(),
          additional_data: GravitonMQ.AMQP10.Value.t() | nil
        }
end

defimpl Inspect, for: GravitonMQ.AMQP10.SASL.Outcome do
  def inspect(%{code: code}, _options) do
    code =
      case code do
        %GravitonMQ.AMQP10.Value{type: :ubyte, value: value} -> Integer.to_string(value)
        _other -> ":invalid"
      end

    "#GravitonMQ.AMQP10.SASL.Outcome<code: #{code}, additional_data: :redacted>"
  end
end
