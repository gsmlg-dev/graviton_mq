defmodule GravitonMQ.MQTT.Codec.FrameTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Codec.Frame, as: Codec

  @connect <<0x10, 0x0E, 0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c">>

  test "decodes an opaque packet envelope and preserves the exact suffix" do
    <<_header, _length, body::binary>> = @connect
    suffix = <<0x20, 2, 0, 0, 0xAA>>

    assert {:ok, %{type: 1, flags: 0, body: ^body, header_size: 2}, ^suffix} =
             Codec.decode(@connect <> suffix)
  end

  test "does not interpret PUBLISH body bytes" do
    assert {:ok, %{type: 3, flags: 0, body: <<0xFF>>}, <<>>} =
             Codec.decode(<<0x30, 1, 0xFF>>)
  end

  test "every strict prefix of a valid packet remains incomplete" do
    for length <- 0..(byte_size(@connect) - 1) do
      assert {:more, required} = Codec.decode(binary_part(@connect, 0, length))
      assert required > 0

      if length >= 2, do: assert(required == byte_size(@connect) - length)
    end
  end

  test "bounds Remaining Length to four octets without allocating a body" do
    limits = struct(GravitonMQ.MQTT.Codec.Limits, max_packet_size: 268_435_460)

    for {encoded, remaining} <- [
          {<<0>>, 0},
          {<<0x80, 1>>, 128},
          {<<0x80, 0x80, 1>>, 16_384},
          {<<0xFF, 0xFF, 0xFF, 0x7F>>, 268_435_455}
        ] do
      if remaining == 0 do
        assert {:ok, %{body: <<>>}, <<>>} = Codec.decode(<<0xC0, encoded::binary>>, limits)
      else
        assert {:more, ^remaining} = Codec.decode(<<0x10, encoded::binary>>, limits)
      end
    end

    assert {:error, %{class: :malformed, reason: :invalid_variable_integer}} =
             Codec.decode(<<0x10, 0x80, 0x80, 0x80, 0x80>>, limits)
  end

  test "rejects announced oversize packets before the body arrives" do
    limits = struct(GravitonMQ.MQTT.Codec.Limits, max_packet_size: 16)

    assert {:error, %{class: :limit_exceeded, reason: :packet_too_large}} =
             Codec.decode(<<0x10, 15>>, limits)

    assert {:more, 14} = Codec.decode(<<0x10, 14>>, limits)
  end

  test "rejects reserved packet types and invalid fixed header flags" do
    for bytes <- [<<0, 0>>, <<0x11, 0>>, <<0x21, 0>>, <<0x80, 0>>, <<0x36, 0>>] do
      assert {:error, %{class: :malformed}} = Codec.decode(bytes)
    end
  end

  test "records canonical header size and rejects nonminimal Remaining Length" do
    assert {:ok, frame, <<>>} = Codec.decode(<<0xC0, 0>>)
    assert %{header_size: 2, body: <<>>} = frame
    assert {:ok, <<0xC0, 0>>} = Codec.encode(frame)

    assert {:error, %{class: :malformed, reason: :nonminimal_variable_integer}} =
             Codec.decode(<<0xC0, 0x80, 0>>)
  end

  test "opaque frames encode without packet schema interpretation" do
    frame = struct(GravitonMQ.MQTT.Frame, type: 3, flags: 0, body: <<0xFF>>)
    assert {:ok, <<0x30, 1, 0xFF>>} = Codec.encode(frame)
  end

  test "invalid configuration and caller values return explicit errors" do
    for limits <- [nil, %{}, struct(GravitonMQ.MQTT.Codec.Limits, max_packet_size: 0)] do
      assert {:error, %{class: :invalid_value, reason: :invalid_limits}} =
               Codec.decode(@connect, limits)
    end

    assert {:error, %{class: :invalid_value}} = Codec.decode(:not_bytes)
    assert {:error, %{class: :invalid_value}} = Codec.encode(%{})
  end
end
