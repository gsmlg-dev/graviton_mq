defmodule GravitonMQ.AMQP10.Codec.ValueMessageCompletionTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.AMQP10.Codec.Message, as: Codec
  alias GravitonMQ.AMQP10.Codec.Value, as: Wire
  alias GravitonMQ.AMQP10.Value, as: V

  test "AMQP value bodies expose exact scalars and recursive arrays while retaining authoritative bytes" do
    values = [
      V.float(<<0x7F801234::32>>),
      V.double(<<0xFFF8000012345678::64>>),
      V.decimal32(<<0xFEDCBA98::32>>),
      V.decimal64(<<0xFEDCBA9876543210::64>>),
      V.decimal128(<<0x000102030405060708090A0B0C0D0E0F::128>>),
      V.char(0x1F600),
      V.array(:map, [V.map([{V.symbol("x"), V.double(<<0x8000000000000000::64>>)}])]),
      V.array(:array, [V.array(:binary, [V.binary(<<0, 255>>)])])
    ]

    for value <- values do
      {:ok, body} = Wire.encode(value)
      original = <<0, 0x80, 0x77::64, body::binary>>
      assert {:ok, message, <<>>} = Codec.decode(original)
      assert message.sections == [amqp_value: value]
      assert message.encoded_content == original
      assert {:ok, ^original} = Codec.encode(message)
      assert {:ok, canonical} = Codec.encode_sections(message.sections)
      assert {:ok, rebuilt, <<>>} = Codec.decode(canonical)
      assert rebuilt.sections == message.sections
    end
  end

  test "application properties accept new simple scalars and keep compound values outside the schema" do
    scalar = V.decimal128(<<0x0123456789ABCDEF::128>>)
    section = {:application_properties, V.map([{V.string("x"), scalar}])}
    assert {:ok, wire} = Codec.encode_section(section)
    assert {:ok, ^section, <<>>} = Codec.decode_section(wire)

    assert {:error, _} =
             Codec.encode_section(
               {:application_properties, V.map([{V.string("x"), V.array(:uint, [V.uint(1)])}])}
             )
  end
end
