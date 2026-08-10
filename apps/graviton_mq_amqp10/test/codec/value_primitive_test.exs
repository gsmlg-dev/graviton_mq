defmodule GravitonMQ.AMQP10.Codec.ValuePrimitiveTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  @supported_format_codes [
    0x00,
    0x40,
    0x60,
    0x43,
    0x52,
    0x70,
    0x44,
    0x53,
    0x80,
    0xA1,
    0xB1,
    0xA3,
    0xB3,
    0x45,
    0xC0,
    0xD0,
    0xC1,
    0xD1,
    0xE0,
    0xF0
  ]

  @primitive_fixtures [
    {<<0x40>>, AMQPValue.null()},
    {<<0x60, 0x12, 0x34>>, AMQPValue.ushort(0x1234)},
    {<<0x43>>, AMQPValue.uint(0)},
    {<<0x52, 0xFF>>, AMQPValue.uint(255)},
    {<<0x70, 0, 0, 1, 0>>, AMQPValue.uint(256)},
    {<<0x44>>, AMQPValue.ulong(0)},
    {<<0x53, 0xFF>>, AMQPValue.ulong(255)},
    {<<0x80, 0::56, 1::8>>, AMQPValue.ulong(1)},
    {<<0xA1, 3, "cat">>, AMQPValue.string("cat")},
    {<<0xB1, 3::32, "cat">>, AMQPValue.string("cat")},
    {<<0xA3, 3, "cat">>, AMQPValue.symbol("cat")},
    {<<0xB3, 3::32, "cat">>, AMQPValue.symbol("cat")}
  ]

  test "decodes independent primitive fixtures to semantic values and preserves suffixes" do
    suffix = <<0xDE, 0xAD>>

    for {encoded, semantic_value} <- @primitive_fixtures do
      assert {:ok, ^semantic_value, ^suffix} = Value.decode(encoded <> suffix)
      refute semantic_value.type in [:uint0, :smalluint, :ulong0, :smallulong]
    end
  end

  test "requests the exact minimum for every strict fixture prefix" do
    for {fixture, _semantic_value} <- @primitive_fixtures,
        prefix_size <- 0..(byte_size(fixture) - 1) do
      prefix = binary_part(fixture, 0, prefix_size)
      assert {:more, expected_more(fixture, prefix_size)} == Value.decode(prefix)
    end
  end

  test "canonically encodes integer width boundaries" do
    fixtures = [
      {AMQPValue.ushort(0), <<0x60, 0, 0>>},
      {AMQPValue.ushort(65_535), <<0x60, 0xFF, 0xFF>>},
      {AMQPValue.uint(0), <<0x43>>},
      {AMQPValue.uint(255), <<0x52, 0xFF>>},
      {AMQPValue.uint(256), <<0x70, 256::32>>},
      {AMQPValue.ulong(0), <<0x44>>},
      {AMQPValue.ulong(255), <<0x53, 0xFF>>},
      {AMQPValue.ulong(256), <<0x80, 256::64>>}
    ]

    for {semantic_value, encoded} <- fixtures do
      assert {:ok, ^encoded} = Value.encode(semantic_value)
    end
  end

  test "canonically encodes string and symbol length boundaries" do
    string255 = :binary.copy("x", 255)
    string256 = :binary.copy("x", 256)
    symbol255 = :binary.copy("y", 255)
    symbol256 = :binary.copy("y", 256)

    assert {:ok, <<0xA1, 0>>} = Value.encode(AMQPValue.string(""))
    assert {:ok, <<0xA1, 255, ^string255::binary>>} = Value.encode(AMQPValue.string(string255))

    assert {:ok, <<0xB1, 256::32, ^string256::binary>>} =
             Value.encode(AMQPValue.string(string256))

    assert {:ok, <<0xA3, 0>>} = Value.encode(AMQPValue.symbol(""))
    assert {:ok, <<0xA3, 255, ^symbol255::binary>>} = Value.encode(AMQPValue.symbol(symbol255))

    assert {:ok, <<0xB3, 256::32, ^symbol256::binary>>} =
             Value.encode(AMQPValue.symbol(symbol256))
  end

  test "rejects malformed UTF-8 in both directions" do
    invalid_utf8 = <<0xFF>>

    assert_error(
      Value.decode(<<0xA1, 1, invalid_utf8::binary>>),
      :value_decode,
      :malformed,
      :invalid_utf8
    )

    assert_error(
      Value.encode(AMQPValue.string(invalid_utf8)),
      :value_encode,
      :invalid_value,
      :invalid_utf8
    )
  end

  test "rejects non-ASCII symbols in both directions" do
    non_ascii = <<0x80>>

    assert_error(
      Value.decode(<<0xA3, 1, non_ascii::binary>>),
      :value_decode,
      :malformed,
      :invalid_symbol
    )

    assert_error(
      Value.encode(AMQPValue.symbol(non_ascii)),
      :value_encode,
      :invalid_value,
      :invalid_symbol
    )

    assert {:ok, <<0xA3, 1, 0x7F>>} = Value.encode(AMQPValue.symbol(<<0x7F>>))
  end

  test "enforces declared value size limits before waiting for payload" do
    limits = %{Limits.default() | max_value_bytes: 3}

    for format_code <- [0xA1, 0xA3] do
      assert_error(
        Value.decode(<<format_code, 4>>, limits),
        :value_decode,
        :limit_exceeded,
        :value_size_limit
      )
    end

    for format_code <- [0xB1, 0xB3] do
      assert_error(
        Value.decode(<<format_code, 4::32>>, limits),
        :value_decode,
        :limit_exceeded,
        :value_size_limit
      )
    end
  end

  test "enforces value size limits while encoding" do
    limits = %{Limits.default() | max_value_bytes: 3}

    for value <- [AMQPValue.string("four"), AMQPValue.symbol("four")] do
      assert_error(
        Value.encode(value, limits),
        :value_encode,
        :limit_exceeded,
        :value_size_limit
      )
    end
  end

  test "returns structured errors for invalid decode and encode limits" do
    limits = %{Limits.default() | max_value_bytes: 0}

    assert_error(Value.decode(<<>>, limits), :value_decode, :invalid_value, :invalid_limits)

    assert_error(
      Value.encode(AMQPValue.null(), limits),
      :value_encode,
      :invalid_value,
      :invalid_limits
    )
  end

  test "rejects unsupported wire format codes with numeric code details" do
    unsupported_codes = Enum.to_list(0..255) -- @supported_format_codes

    assert length(unsupported_codes) == 236

    for format_code <- unsupported_codes do
      assert {:error,
              %Error{
                operation: :value_decode,
                class: :unsupported,
                reason: :format_code,
                offset: 0,
                details: %{format_code: ^format_code}
              }} = Value.decode(<<format_code>>)
    end
  end

  test "rejects unsupported semantic types with type details" do
    unsupported_types =
      AMQPValue.semantic_types() --
        [:null, :ushort, :uint, :ulong, :string, :symbol, :list, :map, :array, :described]

    for type <- unsupported_types do
      value = %AMQPValue{type: type, value: nil}

      assert {:error,
              %Error{
                operation: :value_encode,
                class: :unsupported,
                reason: :semantic_type,
                offset: nil,
                details: %{type: ^type}
              }} = Value.encode(value)
    end
  end

  test "malformed supported semantic structs return errors instead of raising" do
    malformed_values = [
      %AMQPValue{type: :null, value: 0},
      %AMQPValue{type: :ushort, value: -1},
      %AMQPValue{type: :ushort, value: 65_536},
      %AMQPValue{type: :uint, value: :zero},
      %AMQPValue{type: :uint, value: 4_294_967_296},
      %AMQPValue{type: :ulong, value: -1},
      %AMQPValue{type: :ulong, value: 18_446_744_073_709_551_616},
      %AMQPValue{type: :string, value: :not_a_binary},
      %AMQPValue{type: :symbol, value: [:not, :a, :binary]}
    ]

    for malformed_value <- malformed_values do
      assert_error(
        Value.encode(malformed_value),
        :value_encode,
        :invalid_value,
        :invalid_semantic_value
      )
    end
  end

  defp expected_more(_fixture, 0), do: 1

  defp expected_more(<<format_code, _rest::binary>> = fixture, prefix_size)
       when format_code in [0x40, 0x43, 0x44, 0x52, 0x53, 0x60, 0x70, 0x80] do
    byte_size(fixture) - prefix_size
  end

  defp expected_more(<<format_code, _rest::binary>> = fixture, prefix_size)
       when format_code in [0xA1, 0xA3] do
    if prefix_size < 2, do: 2 - prefix_size, else: byte_size(fixture) - prefix_size
  end

  defp expected_more(<<format_code, _rest::binary>> = fixture, prefix_size)
       when format_code in [0xB1, 0xB3] do
    if prefix_size < 5, do: 5 - prefix_size, else: byte_size(fixture) - prefix_size
  end

  defp assert_error(result, operation, class, reason) do
    assert {:error,
            %Error{
              operation: ^operation,
              class: ^class,
              reason: ^reason
            }} = result
  end
end
