defmodule GravitonMQ.AMQP10.SASL.Anonymous do
  @moduledoc """
  RFC 4505 ANONYMOUS trace: absent/empty, or valid UTF-8 of at most 255
  characters. The trace is informational only and never an identity.
  """

  @spec parse(term()) :: {:ok, binary() | nil} | {:error, :invalid_anonymous_message}
  def parse(nil), do: {:ok, nil}
  def parse(<<>>), do: {:ok, nil}

  def parse(trace) when is_binary(trace) and byte_size(trace) <= 1020 do
    if String.valid?(trace) and length(String.codepoints(trace)) <= 255,
      do: {:ok, trace},
      else: {:error, :invalid_anonymous_message}
  end

  def parse(_trace), do: {:error, :invalid_anonymous_message}
end
