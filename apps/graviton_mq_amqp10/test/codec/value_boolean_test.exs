defmodule GravitonMQ.AMQP10.Codec.ValueBooleanTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  @fixtures [
    {<<0x41>>, AMQPValue.boolean(true), <<0x41>>},
    {<<0x42>>, AMQPValue.boolean(false), <<0x42>>},
    {<<0x56, 0>>, AMQPValue.boolean(false), <<0x42>>},
    {<<0x56, 1>>, AMQPValue.boolean(true), <<0x41>>}
  ]

  test "all Boolean wire forms retain semantic identity and exact suffixes" do
    suffix = <<0x56, 0xFF, 0xDE, 0xAD>>

    for {fixture, expected, _canonical} <- @fixtures do
      assert {:ok, ^expected, <<>>} = Value.decode(fixture)
      assert {:ok, ^expected, ^suffix} = Value.decode(fixture <> suffix)
    end
  end

  test "every strict Boolean prefix requests exactly one byte" do
    for {fixture, _expected, _canonical} <- @fixtures,
        prefix_size <- 0..(byte_size(fixture) - 1) do
      assert {:more, 1} = Value.decode(binary_part(fixture, 0, prefix_size))
    end
  end

  test "Boolean encoding uses canonical width-zero constructors" do
    for {fixture, expected, canonical} <- @fixtures do
      assert {:ok, ^canonical} = Value.encode(expected)
      assert {:ok, decoded, <<>>} = Value.decode(fixture)
      assert {:ok, ^canonical} = Value.encode(decoded)
    end
  end

  test "every non-Boolean payload octet is malformed with exact diagnostic details" do
    for octet <- 2..255 do
      expected =
        {:error,
         %Error{
           operation: :value_decode,
           class: :malformed,
           reason: :invalid_boolean,
           offset: 1,
           details: %{value: octet}
         }}

      assert Value.decode(<<0x56, octet>>) == expected
      assert Value.decode(<<0x56, octet, 0x41>>) == expected
    end
  end

  test "corrupt tagged Boolean values are rejected without truth-value coercion" do
    for payload <- [nil, 0, 1, -1, 0.0, :yes, "true", "false", <<>>, [], %{}] do
      assert {:error,
              %Error{
                operation: :value_encode,
                class: :invalid_value,
                reason: :invalid_semantic_value,
                offset: nil,
                details: %{type: :boolean}
              }} = Value.encode(%AMQPValue{type: :boolean, value: payload})
    end
  end

  test "invalid limits are validated before Boolean input or output" do
    for field <- [:max_frame_size, :max_value_bytes, :max_compound_items, :max_nesting_depth] do
      limits = Map.put(Limits.default(), field, 0)
      details = Map.from_struct(limits)

      for {fixture, expected, _canonical} <- @fixtures do
        assert {:error,
                %Error{
                  operation: :value_decode,
                  class: :invalid_value,
                  reason: :invalid_limits,
                  details: ^details
                }} = Value.decode(fixture, limits)

        assert {:error,
                %Error{
                  operation: :value_encode,
                  class: :invalid_value,
                  reason: :invalid_limits,
                  details: ^details
                }} = Value.encode(expected, limits)
      end

      assert {:error, %Error{reason: :invalid_limits}} = Value.decode(<<0x56>>, limits)
    end
  end

  test "Boolean values decode recursively and canonicalize in lists, maps, and described values" do
    list = AMQPValue.list([AMQPValue.boolean(true), AMQPValue.boolean(false)])
    map = AMQPValue.map([{AMQPValue.symbol("k"), list}])
    described = AMQPValue.described(AMQPValue.ulong(0x7F), map)

    fixtures = [
      {<<0xC0, 4, 2, 0x41, 0x56, 0>>, list, <<0xC0, 3, 2, 0x41, 0x42>>},
      {<<0xD0, 7::32, 2::32, 0x41, 0x56, 0>>, list, <<0xC0, 3, 2, 0x41, 0x42>>},
      {<<0xC1, 10, 2, 0xA3, 1, "k", 0xC0, 4, 2, 0x41, 0x56, 0>>, map,
       <<0xC1, 9, 2, 0xA3, 1, "k", 0xC0, 3, 2, 0x41, 0x42>>},
      {<<0xD1, 13::32, 2::32, 0xA3, 1, "k", 0xC0, 4, 2, 0x41, 0x56, 0>>, map,
       <<0xC1, 9, 2, 0xA3, 1, "k", 0xC0, 3, 2, 0x41, 0x42>>},
      {<<0x00, 0x53, 0x7F, 0xC1, 10, 2, 0xA3, 1, "k", 0xC0, 4, 2, 0x41, 0x56, 0>>, described,
       <<0x00, 0x53, 0x7F, 0xC1, 9, 2, 0xA3, 1, "k", 0xC0, 3, 2, 0x41, 0x42>>}
    ]

    suffix = <<0xDE, 0xAD>>

    for {fixture, expected, canonical} <- fixtures do
      assert {:ok, ^expected, ^suffix} = Value.decode(fixture <> suffix)
      assert {:ok, ^canonical} = Value.encode(expected)

      for prefix_size <- 0..(byte_size(fixture) - 1) do
        assert {:more, needed} = Value.decode(binary_part(fixture, 0, prefix_size))
        assert needed > 0
      end
    end
  end

  test "Boolean keys remain distinct from tagged unsigned integers and preserve order" do
    value =
      AMQPValue.map([
        {AMQPValue.boolean(false), AMQPValue.null()},
        {AMQPValue.uint(0), AMQPValue.null()},
        {AMQPValue.ulong(0), AMQPValue.null()},
        {AMQPValue.boolean(true), AMQPValue.null()},
        {AMQPValue.uint(1), AMQPValue.null()}
      ])

    fixture = <<0xC1, 12, 10, 0x42, 0x40, 0x43, 0x40, 0x44, 0x40, 0x41, 0x40, 0x52, 1, 0x40>>
    assert {:ok, ^value, <<>>} = Value.decode(fixture)
    assert {:ok, ^fixture} = Value.encode(value)
  end

  test "alternate Boolean constructors normalize before duplicate map-key validation" do
    for {compact, payload, boolean} <- [{0x42, 0, false}, {0x41, 1, true}] do
      assert_error(
        Value.decode(<<0xC1, 6, 4, compact, 0x40, 0x56, payload, 0x40>>),
        :value_decode,
        :malformed,
        :duplicate_map_key
      )

      duplicate = AMQPValue.boolean(boolean)

      assert_error(
        Value.encode(
          AMQPValue.map([{duplicate, AMQPValue.null()}, {duplicate, AMQPValue.null()}])
        ),
        :value_encode,
        :invalid_value,
        :duplicate_map_key
      )
    end
  end

  test "nested Boolean errors retain value-layer diagnostics and compound boundaries" do
    for fixture <- [
          <<0xC0, 3, 1, 0x56, 2>>,
          <<0xC1, 4, 2, 0x41, 0x56, 2>>,
          <<0x00, 0x53, 0x7F, 0x56, 2>>
        ] do
      assert {:error,
              %Error{
                operation: :value_decode,
                class: :malformed,
                reason: :invalid_boolean,
                offset: 1,
                details: %{value: 2}
              }} = Value.decode(fixture)
    end

    assert_error(
      Value.decode(<<0xC0, 2, 1, 0x56, 1>>),
      :value_decode,
      :malformed,
      :compound_item_truncated
    )

    corrupt = %AMQPValue{type: :boolean, value: 1}

    for value <- [
          AMQPValue.list([corrupt]),
          AMQPValue.map([{AMQPValue.symbol("k"), corrupt}]),
          AMQPValue.described(AMQPValue.ulong(0x7F), corrupt)
        ] do
      assert {:error, %Error{reason: :invalid_semantic_value, details: %{type: :boolean}}} =
               Value.encode(value)
    end
  end

  test "Boolean compounds retain size, count, and depth limits" do
    fixtures = [
      {<<0xC0, 3, 2, 0x41, 0x42>>,
       AMQPValue.list([AMQPValue.boolean(true), AMQPValue.boolean(false)]),
       %{Limits.default() | max_compound_items: 1}, :compound_item_limit},
      {<<0xC0, 3, 2, 0x41, 0x42>>,
       AMQPValue.list([AMQPValue.boolean(true), AMQPValue.boolean(false)]),
       %{Limits.default() | max_value_bytes: 2}, :value_size_limit},
      {<<0xC0, 5, 1, 0xC0, 2, 1, 0x41>>,
       AMQPValue.list([AMQPValue.list([AMQPValue.boolean(true)])]),
       %{Limits.default() | max_nesting_depth: 1}, :nesting_depth_limit}
    ]

    for {fixture, value, limits, reason} <- fixtures do
      assert_error(Value.decode(fixture, limits), :value_decode, :limit_exceeded, reason)
      assert_error(Value.encode(value, limits), :value_encode, :limit_exceeded, reason)
    end
  end

  test "Boolean arrays and descriptors remain unsupported" do
    for constructor <- [0x41, 0x42, 0x56] do
      payload = if constructor == 0x56, do: <<1>>, else: <<>>
      fixture = <<0xE0, 2 + byte_size(payload), 1, constructor, payload::binary>>

      assert {:error,
              %Error{
                operation: :value_decode,
                class: :unsupported,
                reason: :array_element_type,
                details: %{format_code: ^constructor}
              }} = Value.decode(fixture)
    end

    assert {:error, %Error{reason: :array_element_type, details: %{element_type: :boolean}}} =
             Value.encode(AMQPValue.array(:boolean, [AMQPValue.boolean(true)]))

    assert {:error, %Error{reason: :descriptor_type, details: %{type: :boolean}}} =
             Value.decode(<<0x00, 0x41, 0x42>>)

    assert {:error, %Error{reason: :descriptor_type, details: %{type: :boolean}}} =
             Value.encode(%AMQPValue{
               type: :described,
               value: %AMQPValue.Described{
                 descriptor: AMQPValue.boolean(true),
                 value: AMQPValue.boolean(false)
               }
             })
  end

  defp assert_error(result, operation, class, reason) do
    assert {:error, %Error{operation: ^operation, class: ^class, reason: ^reason}} = result
  end
end
