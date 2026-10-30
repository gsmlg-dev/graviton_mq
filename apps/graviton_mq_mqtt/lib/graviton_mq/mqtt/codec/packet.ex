defmodule GravitonMQ.MQTT.Codec.Packet do
  @moduledoc """
  Bounded pure MQTT 5.0 packet codecs and historical 3.1.1 CONNECT/CONNACK codecs.

  CONNECT identifies its version on the wire. CONNACK requires an explicit
  `:v3_1_1` or `:v5`; `:auto` discovers CONNECT's protocol level and never
  guesses a version from a CONNACK body. MQTT 3.1 is unsupported.
  MQTT 5.0 adds PUBLISH, QoS 1/2 acknowledgements, subscriptions, ping and
  DISCONNECT. Protocol
  context, authentication, session assignment, and payload policy are deferred.
  """

  alias GravitonMQ.MQTT.Codec.{Connect, Connack, Error, Frame, Limits, Operational, Wire}
  alias GravitonMQ.MQTT.Packet.Connect, as: ConnectValue
  alias GravitonMQ.MQTT.Packet.Connack, as: ConnackValue

  @type packet :: ConnectValue.t() | ConnackValue.t() | struct()
  @type version :: GravitonMQ.MQTT.version() | :auto

  @spec decode(binary(), version(), Limits.t()) ::
          {:ok, packet(), binary()} | {:more, pos_integer()} | {:error, Error.t()}
  def decode(bytes, version \\ :auto, limits \\ Limits.default()) do
    result =
      with {:ok, limits} <- Limits.validate(limits, :packet_decode),
           :ok <- version_argument(version, :packet_decode, true),
           {:ok, frame, rest} <- Frame.decode(bytes, limits, version == :v5),
           context = %{operation: :packet_decode, limits: limits},
           {:ok, packet} <- decode_body(frame, version, context),
           :ok <- canonical_header(frame, packet.version, context) do
        {:ok, packet, rest}
      end

    operation(result, :packet_decode)
  end

  defp decode_body(%{type: 1, body: bytes}, version, context),
    do: Connect.decode(bytes, version, context)

  defp decode_body(%{type: 2, body: bytes}, version, context),
    do: Connack.decode(bytes, version, context)

  defp decode_body(frame, version, context), do: Operational.decode(frame, version, context)

  defp canonical_header(frame, :v5, context) do
    if frame.header_size == 1 + Wire.vbi_size(byte_size(frame.body)),
      do: :ok,
      else: Wire.error(context, :malformed, :nonminimal_variable_integer)
  end

  defp canonical_header(_frame, :v3_1_1, _context), do: :ok

  @spec encode(packet(), Limits.t()) :: {:ok, binary()} | {:error, Error.t()}
  def encode(packet, limits \\ Limits.default()) do
    result =
      with {:ok, limits} <- Limits.validate(limits, :packet_encode),
           context = %{operation: :packet_encode, limits: limits},
           {:ok, type, flags, body} <- encode_body(packet, context),
           size = :erlang.iolist_size(body),
           :ok <- packet_size(size, context) do
        Frame.encode(
          %GravitonMQ.MQTT.Frame{type: type, flags: flags, body: :erlang.iolist_to_binary(body)},
          limits
        )
      end

    operation(result, :packet_encode)
  end

  defp encode_body(%ConnectValue{} = packet, context) do
    with :ok <- version_argument(packet.version, :packet_encode, false),
         {:ok, body} <- Connect.encode(packet, context),
         do: {:ok, 1, 0, body}
  end

  defp encode_body(%ConnackValue{} = packet, context) do
    with :ok <- version_argument(packet.version, :packet_encode, false),
         {:ok, body} <- Connack.encode(packet, context),
         do: {:ok, 2, 0, body}
  end

  defp encode_body(packet, context) do
    if Operational.packet?(packet),
      do: Operational.encode(packet, context),
      else: Wire.error(context, :invalid_value, :invalid_packet)
  end

  defp version_argument(version, _operation, _auto?) when version in [:v3_1_1, :v5], do: :ok
  defp version_argument(:auto, _operation, true), do: :ok

  defp version_argument(_version, operation, _auto?),
    do: {:error, Error.new(operation, :invalid_value, :invalid_version)}

  defp packet_size(size, context) do
    if size > 268_435_455 or 1 + Wire.vbi_size(size) + size > context.limits.max_packet_size,
      do: Wire.error(context, :limit_exceeded, :packet_too_large),
      else: :ok
  end

  defp operation({:error, %Error{} = error}, operation),
    do: {:error, %{error | operation: operation}}

  defp operation(result, _operation), do: result
end
