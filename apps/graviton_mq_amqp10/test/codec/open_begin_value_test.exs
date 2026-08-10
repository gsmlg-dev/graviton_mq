defmodule GravitonMQ.AMQP10.Codec.OpenBeginValueTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Frame
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  @array8 <<0xE0, 6, 2, 0xA3, 1, "a", 1, "b">>
  @array32 <<0xF0, 9::32, 2::32, 0xA3, 1, "a", 1, "b">>
  @empty_array <<0xE0, 2, 0, 0xA3>>
  @minimal_open <<0x00, 0x53, 0x10, 0xC0, 4, 1, 0xA1, 1, "c">>
  @minimal_begin <<
    0x00,
    0x53,
    0x11,
    0xC0,
    7,
    4,
    0x40,
    0x43,
    0x52,
    100,
    0x52,
    100
  >>

  test "decodes symbol arrays with either outer width and preserves element identity" do
    expected = AMQPValue.array(:symbol, [AMQPValue.symbol("a"), AMQPValue.symbol("b")])
    suffix = <<0xDE, 0xAD>>

    for fixture <- [@array8, @array32] do
      assert {:ok, ^expected, ^suffix} = Value.decode(fixture <> suffix)
    end

    assert {:ok,
            %AMQPValue{type: :array, value: %AMQPValue.Array{element_type: :symbol, values: []}},
            <<>>} = Value.decode(@empty_array)
  end

  test "every strict symbol-array prefix requests more input" do
    for fixture <- [@array8, @array32, @empty_array],
        prefix_size <- 0..(byte_size(fixture) - 1) do
      prefix = binary_part(fixture, 0, prefix_size)
      assert {:more, needed} = Value.decode(prefix)
      assert needed > 0
    end
  end

  test "canonically encodes one shared symbol constructor" do
    values = [AMQPValue.symbol("a"), AMQPValue.symbol("b")]

    assert {:ok, @array8} = Value.encode(AMQPValue.array(:symbol, values))
    assert {:ok, @empty_array} = Value.encode(AMQPValue.array(:symbol, []))

    long_symbol = :binary.copy("x", 256)
    array = AMQPValue.array(:symbol, [AMQPValue.symbol(long_symbol)])

    assert {:ok, <<0xF0, 265::32, 1::32, 0xB3, 256::32, ^long_symbol::binary>>} =
             Value.encode(array)
  end

  test "rejects unsupported or malformed array element data explicitly" do
    assert_error(
      Value.decode(<<0xE0, 2, 0, 0x40>>),
      :value_decode,
      :unsupported,
      :array_element_type
    )

    assert_error(
      Value.encode(AMQPValue.array(:uint, [AMQPValue.uint(1)])),
      :value_encode,
      :unsupported,
      :array_element_type
    )

    malformed = %AMQPValue{
      type: :array,
      value: %AMQPValue.Array{element_type: :symbol, values: [AMQPValue.string("wrong")]}
    }

    assert_error(Value.encode(malformed), :value_encode, :invalid_value, :invalid_semantic_value)
  end

  test "array elements cannot borrow bytes outside the declared array" do
    assert_error(
      Value.decode(<<0xE0, 3, 1, 0xA3, 2, "xy">>),
      :value_decode,
      :malformed,
      :array_element_truncated
    )

    assert_error(
      Value.decode(<<0xE0, 4, 0, 0xA3, 1, "a">>),
      :value_decode,
      :malformed,
      :array_size_mismatch
    )

    assert_error(
      Value.decode(<<0xE0, 1, 0>>),
      :value_decode,
      :malformed,
      :array_size_too_small
    )
  end

  test "validates array symbols and resource limits" do
    assert_error(
      Value.decode(<<0xE0, 4, 1, 0xA3, 1, 0x80>>),
      :value_decode,
      :malformed,
      :invalid_symbol
    )

    item_limits = %{Limits.default() | max_compound_items: 1}
    size_limits = %{Limits.default() | max_value_bytes: 5}

    assert_error(
      Value.decode(@array8, item_limits),
      :value_decode,
      :limit_exceeded,
      :compound_item_limit
    )

    assert_error(
      Value.decode(<<0xE0, 6>>, size_limits),
      :value_decode,
      :limit_exceeded,
      :value_size_limit
    )

    assert_error(
      Value.encode(
        AMQPValue.array(:symbol, [AMQPValue.symbol("a"), AMQPValue.symbol("b")]),
        item_limits
      ),
      :value_encode,
      :limit_exceeded,
      :compound_item_limit
    )
  end

  test "decodes and canonically encodes Open and Begin as generic described values" do
    open =
      AMQPValue.described(
        AMQPValue.ulong(0x10),
        AMQPValue.list([AMQPValue.string("c")])
      )

    begin =
      AMQPValue.described(
        AMQPValue.ulong(0x11),
        AMQPValue.list([
          AMQPValue.null(),
          AMQPValue.uint(0),
          AMQPValue.uint(100),
          AMQPValue.uint(100)
        ])
      )

    assert {:ok, ^open, <<>>} = Value.decode(@minimal_open)
    assert {:ok, @minimal_open} = Value.encode(open)
    assert {:ok, ^begin, <<>>} = Value.decode(@minimal_begin)
    assert {:ok, @minimal_begin} = Value.encode(begin)
  end

  test "every strict Open and Begin value prefix requests more input" do
    for fixture <- [@minimal_open, @minimal_begin],
        prefix_size <- 0..(byte_size(fixture) - 1) do
      prefix = binary_part(fixture, 0, prefix_size)
      assert {:more, needed} = Value.decode(prefix)
      assert needed > 0
    end
  end

  test "unknown numeric and symbolic descriptors remain lossless" do
    values = [
      AMQPValue.described(AMQPValue.ulong(0xFE), AMQPValue.string("value")),
      AMQPValue.described(AMQPValue.symbol("example:extension"), AMQPValue.uint(7))
    ]

    for described <- values do
      assert {:ok, encoded} = Value.encode(described)
      assert {:ok, ^described, <<>>} = Value.decode(encoded)
    end
  end

  test "reserved descriptor semantic types are unsupported rather than malformed" do
    assert_error(
      Value.decode(<<0x00, 0xA1, 1, "x", 0x40>>),
      :value_decode,
      :unsupported,
      :descriptor_type
    )

    described = %AMQPValue{
      type: :described,
      value: %AMQPValue.Described{
        descriptor: AMQPValue.string("x"),
        value: AMQPValue.null()
      }
    }

    assert_error(Value.encode(described), :value_encode, :unsupported, :descriptor_type)
  end

  test "frame envelopes and value decoding preserve opaque payload bytes" do
    payload = <<0x00, 0xFF, 0x10, 0x20>>
    body = @minimal_open <> payload
    size = 8 + byte_size(body)
    frame_bytes = <<size::32, 2, 0, 0::16, body::binary, 0xAA>>

    assert {:ok, %{body: ^body}, <<0xAA>>} = Frame.decode(frame_bytes)
    assert {:ok, %AMQPValue{type: :described}, ^payload} = Value.decode(body)
  end

  test "described values enforce nesting and total encoded-size limits" do
    inner = AMQPValue.described(AMQPValue.ulong(1), AMQPValue.null())
    nested = AMQPValue.described(AMQPValue.ulong(2), inner)
    depth_limits = %{Limits.default() | max_nesting_depth: 1}
    size_limits = %{Limits.default() | max_value_bytes: 3}

    assert_error(
      Value.encode(nested, depth_limits),
      :value_encode,
      :limit_exceeded,
      :nesting_depth_limit
    )

    assert_error(
      Value.decode(<<0x00, 0x53, 2, 0x00, 0x53, 1, 0x40>>, depth_limits),
      :value_decode,
      :limit_exceeded,
      :nesting_depth_limit
    )

    assert_error(
      Value.encode(inner, size_limits),
      :value_encode,
      :limit_exceeded,
      :value_size_limit
    )

    assert_error(
      Value.decode(<<0x00, 0x53, 1, 0x40>>, size_limits),
      :value_decode,
      :limit_exceeded,
      :value_size_limit
    )
  end

  test "malformed described structs return structured errors" do
    assert_error(
      Value.encode(%AMQPValue{type: :described, value: nil}),
      :value_encode,
      :invalid_value,
      :invalid_semantic_value
    )

    malformed = %AMQPValue{
      type: :described,
      value: %AMQPValue.Described{descriptor: AMQPValue.ulong(1), value: :bare}
    }

    assert_error(Value.encode(malformed), :value_encode, :invalid_value, :invalid_semantic_value)
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
