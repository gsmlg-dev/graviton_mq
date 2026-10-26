defmodule GravitonMQ.MQTT.Codec.Connack do
  @moduledoc false

  alias GravitonMQ.MQTT.Codec.{Properties, Wire}
  alias GravitonMQ.MQTT.Packet.Connack

  @v5_codes [
    0,
    0x80,
    0x81,
    0x82,
    0x83,
    0x84,
    0x85,
    0x86,
    0x87,
    0x88,
    0x89,
    0x8A,
    0x8C,
    0x90,
    0x95,
    0x97,
    0x99,
    0x9A,
    0x9B,
    0x9C,
    0x9D,
    0x9F
  ]

  def decode(_bytes, :auto, context), do: Wire.error(context, :unsupported, :version_required)

  def decode(bytes, version, context) do
    with {:ok, flags, rest} <- Wire.decode(:u8, bytes, context, :connack_flags),
         :ok <- flags(flags, context),
         {:ok, reason_code, rest} <- Wire.decode(:u8, rest, context, :reason_code),
         :ok <- reason_code(reason_code, version, context),
         :ok <- session_present(flags == 1, reason_code, context),
         {:ok, properties, rest} <- Properties.decode(rest, version, :connack, context),
         :ok <- exhausted(rest, context) do
      {:ok,
       %Connack{
         version: version,
         session_present: flags == 1,
         reason_code: reason_code,
         properties: properties
       }}
    end
  end

  def encode(%Connack{} = packet, context) do
    with :ok <- reason_code(packet.reason_code, packet.version, context),
         :ok <- session_present(packet.session_present, packet.reason_code, context),
         {:ok, properties} <-
           Properties.encode(packet.properties, packet.version, :connack, context) do
      flags = if packet.session_present, do: 1, else: 0
      {:ok, [<<flags, packet.reason_code>>, properties]}
    end
  end

  defp flags(value, _context) when value in [0, 1], do: :ok
  defp flags(_value, context), do: Wire.error(context, :malformed, :invalid_connack_flags)

  defp reason_code(code, :v3_1_1, _context) when is_integer(code) and code in 0..5, do: :ok
  defp reason_code(code, :v5, _context) when code in @v5_codes, do: :ok

  defp reason_code(_code, _version, context),
    do: Wire.error(context, Wire.value_class(context), :invalid_reason_code)

  defp session_present(value, _code, context) when not is_boolean(value),
    do: Wire.error(context, Wire.value_class(context), :invalid_session_present)

  defp session_present(true, code, context) when code != 0,
    do: Wire.error(context, Wire.value_class(context), :invalid_session_present)

  defp session_present(_value, _code, _context), do: :ok

  defp exhausted(<<>>, _context), do: :ok
  defp exhausted(_bytes, context), do: Wire.error(context, :malformed, :trailing_packet_data)
end
