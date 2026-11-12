defmodule GravitonMQ.AMQP10.Codec.ProtocolHeader do
  @moduledoc """
  Recognizes fixed AMQP 1.0 protocol headers without negotiating a protocol.

  `recognize/1` accepts only `AMQP 0 1 0 0` and keeps its historical results.
  `recognize/2` takes the protocol IDs a caller currently accepts — `[0]`,
  `[3]` or `[0, 3]` — where ID 3 is the SASL layer header `AMQP 3 1 0 0`
  (Part 5 §5.3). Recognition never selects or negotiates a layer.
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
  @type accept :: [0 | 3]

  @accepts [[0], [3], [0, 3], [3, 0]]

  @spec accept?(term()) :: boolean()
  def accept?(accept), do: accept in @accepts

  @spec recognize(binary()) :: Codec.decode_result(t())
  def recognize(input), do: recognize(input, [0])

  @spec recognize(binary(), accept()) :: Codec.decode_result(t())
  def recognize(input, accept) when is_binary(input) and accept in @accepts do
    case input do
      <<"AMQP", protocol_id, major, minor, revision, rest::binary>> ->
        protocol_header(protocol_id, major, minor, revision, rest, accept)

      header when byte_size(header) < 4 ->
        if header == binary_part("AMQP", 0, byte_size(header)),
          do: {:more, 8 - byte_size(header)},
          else: magic_error()

      <<"AMQP", rest::binary>> ->
        {:more, 4 - byte_size(rest)}

      _other ->
        magic_error()
    end
  end

  def recognize(_input, _accept),
    do: {:error, Error.new(:protocol_header, :invalid_value, :invalid_accept)}

  defp protocol_header(protocol_id, 1, 0, 0, rest, accept) when protocol_id in [0, 3] do
    if protocol_id in accept,
      do: {:ok, %__MODULE__{protocol_id: protocol_id, major: 1, minor: 0, revision: 0}, rest},
      else: not_accepted(protocol_id)
  end

  defp protocol_header(protocol_id, major, minor, revision, _rest, accept)
       when protocol_id in [0, 3] do
    if protocol_id in accept,
      do: version_error(protocol_id, major, minor, revision),
      else: not_accepted(protocol_id)
  end

  defp protocol_header(protocol_id, _major, _minor, _revision, _rest, _accept),
    do: unsupported_protocol_id(protocol_id)

  defp not_accepted(3) do
    {:error,
     Error.new(:protocol_header, :unsupported, :sasl_not_supported,
       offset: 4,
       details: %{protocol_id: 3}
     )}
  end

  defp not_accepted(protocol_id), do: unsupported_protocol_id(protocol_id)

  defp unsupported_protocol_id(protocol_id) do
    {:error,
     Error.new(:protocol_header, :unsupported, :unsupported_protocol_id,
       offset: 4,
       details: %{protocol_id: protocol_id}
     )}
  end

  defp version_error(protocol_id, major, minor, revision) do
    {:error,
     Error.new(:protocol_header, :unsupported, :unsupported_version,
       offset: 5,
       details: %{protocol_id: protocol_id, major: major, minor: minor, revision: revision}
     )}
  end

  defp magic_error do
    {:error,
     Error.new(:protocol_header, :malformed, :invalid_protocol_magic, offset: 0, details: %{})}
  end
end
