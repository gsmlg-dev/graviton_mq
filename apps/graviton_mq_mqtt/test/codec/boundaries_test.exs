defmodule GravitonMQ.MQTT.Codec.BoundariesTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias GravitonMQ.MQTT.Codec.{Error, Frame, Limits, Packet}

  @connect <<0x10, 14, 0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c">>

  test "field limits accept the boundary and reject the next byte on encode and decode" do
    limits = %Limits{max_field_bytes: 5}
    exact = connect("12345")
    excess = connect("123456")
    assert {:ok, packet, <<>>} = Packet.decode(exact, :auto, limits)
    assert {:ok, ^exact} = Packet.encode(packet, limits)

    assert {:error, %Error{class: :limit_exceeded, reason: :field_too_large}} =
             Packet.decode(excess, :auto, limits)

    assert {:error, %Error{class: :limit_exceeded, reason: :field_too_large}} =
             Packet.encode(%{packet | client_id: "123456"}, limits)
  end

  test "UTF-8 and binary fields retain the full 16-bit length boundary" do
    id = :binary.copy("c", 65_535)
    password = :binary.copy(<<0xFF>>, 65_535)

    body =
      <<0, 4, "MQTT", 5, 0x42, 0, 0, 0, 65_535::16, id::binary, 65_535::16, password::binary>>

    bytes = packet(0x10, body)
    assert {:ok, value, <<>>} = Packet.decode(bytes)
    assert %{client_id: ^id, password: ^password} = value
    assert {:ok, ^bytes} = Packet.encode(value)

    for value <- [%{value | client_id: id <> "c"}, %{value | password: password <> <<0xFF>>}] do
      assert {:error, %Error{class: :invalid_value, reason: :field_length_out_of_range}} =
               Packet.encode(value)
    end
  end

  test "Property Length crosses 127 to 128 canonically and keeps exact suffixes" do
    for size <- [127, 128, 16_383, 16_384] do
      value = :binary.copy("v", size - 5)
      properties = <<0x26, 0, 0, byte_size(value)::16, value::binary>>
      assert byte_size(properties) == size
      bytes = connect("c", properties)
      assert {:ok, packet, <<0xAA, 0xBB>>} = Packet.decode(bytes <> <<0xAA, 0xBB>>)
      assert [user_property: {"", ^value}] = packet.properties
      assert {:ok, ^bytes} = Packet.encode(packet)
    end
  end

  test "each strict prefix of a packet with multibyte lengths stays incomplete" do
    properties = <<0x26, 0, 0, 123::16, :binary.copy("v", 123)::binary>>
    bytes = connect("c", properties)

    for size <- 0..(byte_size(bytes) - 1) do
      assert {:more, count} = Packet.decode(binary_part(bytes, 0, size))
      assert count > 0
    end
  end

  test "all encode-side semantic shapes and scalar ranges fail explicitly" do
    assert {:ok, value, <<>>} = Packet.decode(@connect)

    for invalid <- [
          %{value | version: nil},
          %{value | clean_start: 1},
          %{value | client_id: nil},
          %{value | client_id: <<0>>},
          %{value | keep_alive: -1},
          %{value | keep_alive: 65_536},
          %{value | keep_alive: 1.0},
          %{value | username: <<0xFF>>},
          %{value | password: 1},
          %{value | will: %{}},
          %{value | will: struct(GravitonMQ.MQTT.Packet.Will, topic: "w", payload: "", qos: 3)},
          %{
            value
            | will: struct(GravitonMQ.MQTT.Packet.Will, topic: "w", payload: "", retain: 1)
          },
          %{value | will: struct(GravitonMQ.MQTT.Packet.Will, topic: "#", payload: "")},
          %{},
          nil
        ] do
      assert {:error, %Error{operation: :packet_encode, class: :invalid_value}} =
               Packet.encode(invalid)
    end
  end

  test "all fixed header combinations are either valid opaque envelopes or malformed" do
    for header <- 0..255 do
      case Frame.decode(<<header, 0>>) do
        {:ok, frame, <<>>} -> assert {:ok, <<^header, 0>>} = Frame.encode(frame)
        {:error, %Error{class: :malformed}} -> :ok
      end
    end
  end

  test "binary mutations and arbitrary complete envelopes never escape as exceptions" do
    for bytes <- [@connect, <<0x20, 3, 0, 0, 0>>],
        position <- 0..(byte_size(bytes) - 1),
        mask <- [1, 2, 4, 8, 16, 32, 64, 128] do
      <<prefix::binary-size(position), byte, rest::binary>> = bytes
      changed = <<prefix::binary, bxor(byte, mask), rest::binary>>
      explicit_result(Packet.decode(changed, :v5))
    end

    for header <- [0x10, 0x20, 0x30, 0xC0, 0xF0],
        size <- [0, 1, 2, 3, 7, 10, 14, 32],
        byte <- 0..255 do
      body = :binary.copy(<<byte>>, size)
      explicit_result(Packet.decode(packet(header, body), :v5))
    end
  end

  test "malformed inner property lengths remain errors even with a following valid packet" do
    for prefix <- [
          <<0x80>>,
          <<0x80, 0x80, 0x80, 0x80>>,
          <<0xFF, 0xFF, 0xFF, 0x7F>>,
          <<3, 0x26, 0, 1>>
        ] do
      body = <<0, 4, "MQTT", 5, 2, 0, 60, prefix::binary>>

      assert {:error, %Error{operation: :packet_decode, class: :malformed}} =
               Packet.decode(packet(0x10, body) <> @connect)
    end
  end

  test "zero properties and invalid limit shapes are validated on both operations" do
    assert {:ok, value, <<>>} = Packet.decode(@connect, :auto, %Limits{max_properties: 0})
    assert {:ok, @connect} = Packet.encode(value, %Limits{max_properties: 0})

    for limits <- [
          %Limits{max_packet_size: 268_435_461},
          %Limits{max_field_bytes: 65_536},
          %Limits{max_properties: -1},
          %Limits{max_packet_size: 1.5},
          %{}
        ] do
      assert {:error, %Error{class: :invalid_value, reason: :invalid_limits}} =
               Packet.decode(@connect, :auto, limits)

      assert {:error, %Error{class: :invalid_value, reason: :invalid_limits}} =
               Packet.encode(value, limits)
    end
  end

  defp explicit_result({:ok, value, rest}) when is_binary(rest) do
    assert {:ok, encoded} = Packet.encode(value)
    assert {:ok, ^value, <<>>} = Packet.decode(encoded, :v5)
  end

  defp explicit_result({:more, count}), do: assert(is_integer(count) and count > 0)

  defp explicit_result({:error, %Error{operation: :packet_decode, class: class}}),
    do: assert(class in [:malformed, :unsupported, :limit_exceeded, :invalid_value])

  defp connect(id, properties \\ <<>>) do
    packet(
      0x10,
      <<0, 4, "MQTT", 5, 2, 0, 60, length_bytes(byte_size(properties))::binary,
        properties::binary, byte_size(id)::16, id::binary>>
    )
  end

  defp packet(header, body), do: <<header, length_bytes(byte_size(body))::binary, body::binary>>
  defp length_bytes(n) when n < 128, do: <<n>>
  defp length_bytes(n), do: <<rem(n, 128) + 128, length_bytes(div(n, 128))::binary>>
end
