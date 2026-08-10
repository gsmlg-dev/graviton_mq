defmodule GravitonMQ.AMQP10.Codec.ProtocolHeader do
  @moduledoc """
  Recognizes the fixed AMQP 1.0 protocol header without negotiating a protocol.
  """

  alias GravitonMQ.AMQP10.Codec
  alias GravitonMQ.AMQP10.Codec.Error

  @enforce_keys [:protocol_id, :major, :minor, :revision]
  defstruct [:protocol_id, :major, :minor, :revision]

  @type t :: %__MODULE__{
          protocol_id: 0..255,
          major: 0..255,
          minor: 0..255,
          revision: 0..255
        }

  @spec recognize(binary()) :: Codec.decode_result(t())
  def recognize(<<"AMQP", protocol_id, major, minor, revision, rest::binary>>) do
    protocol_header(protocol_id, major, minor, revision, rest)
  end

  def recognize(header)
      when byte_size(header) < 4 and header == binary_part("AMQP", 0, byte_size(header)) do
    {:more, 8 - byte_size(header)}
  end

  def recognize(<<"AMQP", rest::binary>>) when byte_size(rest) < 4 do
    {:more, 4 - byte_size(rest)}
  end

  def recognize(_header) do
    {:error,
     Error.new(:protocol_header, :malformed, :invalid_protocol_magic, offset: 0, details: %{})}
  end

  defp protocol_header(0, 1, 0, 0, rest) do
    {:ok, %__MODULE__{protocol_id: 0, major: 1, minor: 0, revision: 0}, rest}
  end

  defp protocol_header(3, _major, _minor, _revision, _rest) do
    {:error,
     Error.new(:protocol_header, :unsupported, :sasl_not_supported,
       offset: 4,
       details: %{protocol_id: 3}
     )}
  end

  defp protocol_header(protocol_id, _major, _minor, _revision, _rest) when protocol_id != 0 do
    {:error,
     Error.new(:protocol_header, :unsupported, :unsupported_protocol_id,
       offset: 4,
       details: %{protocol_id: protocol_id}
     )}
  end

  defp protocol_header(0, major, minor, revision, _rest) do
    {:error,
     Error.new(:protocol_header, :unsupported, :unsupported_version,
       offset: 5,
       details: %{protocol_id: 0, major: major, minor: minor, revision: revision}
     )}
  end
end
