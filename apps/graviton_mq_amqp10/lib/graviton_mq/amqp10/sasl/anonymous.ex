defmodule GravitonMQ.AMQP10.SASL.Anonymous do
  @moduledoc """
  RFC 4505 ANONYMOUS trace: absent/empty, or valid UTF-8 of at most 255
  characters. The trace is informational only and never an identity.

  Following the intent of the RFC 4505 StringPrep "trace" profile, C0 controls
  (U+0000–U+001F, including NUL, TAB, LF and ESC), DEL (U+007F) and C1 controls
  (U+0080–U+009F) are refused, so a trace can never inject terminal or log
  control sequences. Full StringPrep tables are not applied.
  """

  @spec parse(term()) :: {:ok, binary() | nil} | {:error, :invalid_anonymous_message}
  def parse(nil), do: {:ok, nil}
  def parse(<<>>), do: {:ok, nil}

  def parse(trace) when is_binary(trace) and byte_size(trace) <= 1020 do
    if String.valid?(trace) and length(String.codepoints(trace)) <= 255 and
         not control?(trace),
       do: {:ok, trace},
       else: {:error, :invalid_anonymous_message}
  end

  def parse(_trace), do: {:error, :invalid_anonymous_message}

  defp control?(trace),
    do: Enum.any?(String.to_charlist(trace), &(&1 <= 0x1F or &1 in 0x7F..0x9F))
end
