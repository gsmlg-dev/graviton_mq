defmodule GravitonMQ.AMQP10.Codec.ValueCompoundTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  @compound_fixtures [
    {<<0x45>>, AMQPValue.list([])},
    {<<0xC0, 4, 1, 0xA1, 1, "x">>, AMQPValue.list([AMQPValue.string("x")])},
    {<<0xD0, 7::32, 1::32, 0xA1, 1, "x">>, AMQPValue.list([AMQPValue.string("x")])},
    {<<0xC1, 5, 2, 0xA3, 1, "k", 0x43>>,
     AMQPValue.map([{AMQPValue.symbol("k"), AMQPValue.uint(0)}])},
    {<<0xD1, 8::32, 2::32, 0xA3, 1, "k", 0x43>>,
     AMQPValue.map([{AMQPValue.symbol("k"), AMQPValue.uint(0)}])}
  ]

  test "decodes independent list and map fixtures and preserves the top-level suffix" do
    suffix = <<0xDE, 0xAD>>

    for {encoded, semantic_value} <- @compound_fixtures do
      assert {:ok, ^semantic_value, ^suffix} = Value.decode(encoded <> suffix)
    end
  end

  test "requests the exact minimum for truncated size, count, and payload prefixes" do
    for {fixture, _semantic_value} <- @compound_fixtures,
        prefix_size <- 0..(byte_size(fixture) - 1) do
      prefix = binary_part(fixture, 0, prefix_size)
      assert {:more, expected_more(fixture, prefix_size)} == Value.decode(prefix)
    end
  end

  test "rejects compound sizes smaller than their count field" do
    for encoded <- [<<0xC0, 0>>, <<0xC1, 0>>, <<0xD0, 3::32>>, <<0xD1, 3::32>>] do
      assert_error(Value.decode(encoded), :value_decode, :malformed, :compound_size_too_small)
    end
  end

  test "enforces declared compound size limits before waiting for the payload" do
    limits = %{Limits.default() | max_value_bytes: 3}

    for encoded <- [<<0xC0, 4>>, <<0xC1, 4>>, <<0xD0, 4::32>>, <<0xD1, 4::32>>] do
      assert_error(
        Value.decode(encoded, limits),
        :value_decode,
        :limit_exceeded,
        :value_size_limit
      )
    end
  end

  test "rejects compound item counts over the configured limit" do
    limits = %{Limits.default() | max_compound_items: 1}

    assert_error(
      Value.decode(<<0xC0, 3, 2, 0x40, 0x40>>, limits),
      :value_decode,
      :limit_exceeded,
      :compound_item_limit
    )

    assert_error(
      Value.decode(<<0xC1, 3, 2, 0x43, 0x43>>, limits),
      :value_decode,
      :limit_exceeded,
      :compound_item_limit
    )
  end

  test "rejects odd map item counts" do
    assert_error(Value.decode(<<0xC1, 2, 1, 0x40>>), :value_decode, :malformed, :odd_map_count)
  end

  test "does not borrow a top-level suffix to complete a truncated compound item" do
    assert_error(
      Value.decode(<<0xC0, 2, 1, 0xA1, 1, "x">>),
      :value_decode,
      :malformed,
      :compound_item_truncated
    )
  end

  test "rejects item-count underflow and payload bytes left after the item count" do
    assert_error(
      Value.decode(<<0xC0, 2, 2, 0x40>>),
      :value_decode,
      :malformed,
      :compound_item_truncated
    )

    assert_error(
      Value.decode(<<0xC0, 2, 0, 0x40>>),
      :value_decode,
      :malformed,
      :compound_size_mismatch
    )
  end

  test "rejects null and duplicate typed map keys while preserving typed-key order" do
    assert_error(
      Value.decode(<<0xC1, 3, 2, 0x40, 0x43>>),
      :value_decode,
      :malformed,
      :null_map_key
    )

    assert_error(
      Value.decode(<<0xC1, 9, 4, 0xA1, 1, "k", 0x43, 0xA1, 1, "k", 0x44>>),
      :value_decode,
      :malformed,
      :duplicate_map_key
    )

    expected =
      AMQPValue.map([
        {AMQPValue.string("k"), AMQPValue.uint(0)},
        {AMQPValue.symbol("k"), AMQPValue.ulong(0)}
      ])

    assert {:ok, ^expected, <<>>} =
             Value.decode(<<0xC1, 9, 4, 0xA1, 1, "k", 0x43, 0xA3, 1, "k", 0x44>>)
  end

  test "preserves unsupported nested wire constructors" do
    assert_error(Value.decode(<<0xC0, 2, 1, 0x41>>), :value_decode, :unsupported, :format_code)
    assert_error(Value.decode(<<0x41>>), :value_decode, :unsupported, :format_code)
  end

  test "canonically encodes independent list and map fixtures" do
    encode_fixtures = [
      {AMQPValue.list([]), <<0x45>>},
      {AMQPValue.list([AMQPValue.string("x")]), <<0xC0, 4, 1, 0xA1, 1, "x">>},
      {AMQPValue.map([]), <<0xC1, 1, 0>>},
      {AMQPValue.map([{AMQPValue.symbol("k"), AMQPValue.uint(0)}]),
       <<0xC1, 5, 2, 0xA3, 1, "k", 0x43>>}
    ]

    for {semantic_value, encoded} <- encode_fixtures do
      assert {:ok, ^encoded} = Value.encode(semantic_value)
    end
  end

  test "selects 8-bit and 32-bit compound widths at the encoded-size boundary" do
    string252 = AMQPValue.string(:binary.copy("x", 252))
    string253 = AMQPValue.string(:binary.copy("x", 253))

    assert {:ok, <<0xC0, 255, 1, 0xA1, 252, _payload::binary-size(252)>>} =
             Value.encode(AMQPValue.list([string252]))

    assert {:ok, <<0xD0, 259::32, 1::32, 0xA1, 253, _payload::binary-size(253)>>} =
             Value.encode(AMQPValue.list([string253]))

    string251 = AMQPValue.string(:binary.copy("k", 251))

    assert {:ok, <<0xC1, 255, 2, 0xA1, 251, _payload::binary-size(251), 0x40>>} =
             Value.encode(AMQPValue.map([{string251, AMQPValue.null()}]))

    assert {:ok, <<0xD1, 259::32, 2::32, 0xA1, 252, _payload::binary-size(252), 0x40>>} =
             Value.encode(AMQPValue.map([{string252, AMQPValue.null()}]))
  end

  test "uses compound32 for 256 items, where minimum item bytes also cross the size boundary" do
    list = AMQPValue.list(List.duplicate(AMQPValue.null(), 256))

    assert {:ok, <<0xD0, 260::32, 256::32, list_payload::binary-size(256)>> = list_encoded} =
             Value.encode(list)

    assert list_payload == :binary.copy(<<0x40>>, 256)
    assert {:ok, ^list, <<>>} = Value.decode(list_encoded)

    entries =
      Enum.map(0..127, fn key ->
        {AMQPValue.uint(key), AMQPValue.null()}
      end)

    map = AMQPValue.map(entries)

    assert {:ok, <<0xD1, 387::32, 256::32, _map_payload::binary-size(383)>> = map_encoded} =
             Value.encode(map)

    assert {:ok, ^map, <<>>} = Value.decode(map_encoded)
  end

  test "enforces item, encoded-size, and nesting limits while encoding" do
    item_limits = %{Limits.default() | max_compound_items: 1}
    size_limits = %{Limits.default() | max_value_bytes: 3}
    depth_limits = %{Limits.default() | max_nesting_depth: 1}

    assert_error(
      Value.encode(AMQPValue.list([AMQPValue.null(), AMQPValue.null()]), item_limits),
      :value_encode,
      :limit_exceeded,
      :compound_item_limit
    )

    assert_error(
      Value.encode(AMQPValue.map([{AMQPValue.uint(0), AMQPValue.null()}]), item_limits),
      :value_encode,
      :limit_exceeded,
      :compound_item_limit
    )

    assert_error(
      Value.encode(AMQPValue.list([AMQPValue.string("x")]), size_limits),
      :value_encode,
      :limit_exceeded,
      :value_size_limit
    )

    nested = AMQPValue.list([AMQPValue.list([])])

    assert_error(
      Value.encode(nested, depth_limits),
      :value_encode,
      :limit_exceeded,
      :nesting_depth_limit
    )

    assert_error(
      Value.decode(<<0xC0, 2, 1, 0x45>>, depth_limits),
      :value_decode,
      :limit_exceeded,
      :nesting_depth_limit
    )

    assert {:ok, <<0xC0, 2, 1, 0x45>>} =
             Value.encode(nested, %{depth_limits | max_nesting_depth: 2})
  end

  test "rejects invalid manually constructed compound values instead of raising" do
    malformed_values = [
      %AMQPValue{type: :list, value: :not_a_list},
      %AMQPValue{type: :list, value: ["bare"]},
      %AMQPValue{type: :map, value: :not_a_list},
      %AMQPValue{type: :map, value: [{AMQPValue.string("k")}]},
      %AMQPValue{type: :map, value: [{AMQPValue.string("k"), "bare"}]},
      %AMQPValue{
        type: :list,
        value: [%AMQPValue{type: :uint, value: :not_an_integer}]
      }
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

  test "rejects null and duplicate map keys while keeping string and symbol keys distinct" do
    assert_error(
      Value.encode(AMQPValue.map([{AMQPValue.null(), AMQPValue.uint(0)}])),
      :value_encode,
      :invalid_value,
      :null_map_key
    )

    duplicate_key = AMQPValue.string("k")

    assert_error(
      Value.encode(
        AMQPValue.map([
          {duplicate_key, AMQPValue.uint(0)},
          {duplicate_key, AMQPValue.ulong(0)}
        ])
      ),
      :value_encode,
      :invalid_value,
      :duplicate_map_key
    )

    ordered =
      AMQPValue.map([
        {AMQPValue.string("k"), AMQPValue.uint(0)},
        {AMQPValue.symbol("k"), AMQPValue.ulong(0)}
      ])

    assert {:ok, <<0xC1, 9, 4, 0xA1, 1, "k", 0x43, 0xA3, 1, "k", 0x44>>} =
             Value.encode(ordered)
  end

  test "preserves unsupported nested semantic types" do
    nested_boolean = AMQPValue.boolean(true)

    assert_error(
      Value.encode(AMQPValue.list([nested_boolean])),
      :value_encode,
      :unsupported,
      :semantic_type
    )
  end

  defp expected_more(_fixture, 0), do: 1
  defp expected_more(<<0x45>>, _prefix_size), do: 0

  defp expected_more(<<format_code, _rest::binary>> = fixture, prefix_size)
       when format_code in [0xC0, 0xC1] do
    if prefix_size < 2, do: 2 - prefix_size, else: byte_size(fixture) - prefix_size
  end

  defp expected_more(<<format_code, _rest::binary>> = fixture, prefix_size)
       when format_code in [0xD0, 0xD1] do
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
