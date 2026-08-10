defmodule GravitonMQ.AMQP10.Codec.OpenPerformativeTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Performative
  alias GravitonMQ.AMQP10.Performative.Open
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  @minimal_open <<0x00, 0x53, 0x10, 0xC0, 4, 1, 0xA1, 1, "c">>
  @symbolic_open <<
    0x00,
    0xA3,
    14,
    "amqp:open:list",
    0xC0,
    4,
    1,
    0xA1,
    1,
    "c"
  >>
  @open_with_explicit_trailing_nulls <<
    0x00,
    0x53,
    0x10,
    0xC0,
    7,
    4,
    0xA1,
    1,
    "c",
    0x40,
    0x40,
    0x40
  >>
  @full_open <<
    0x00,
    0x53,
    0x10,
    0xC0,
    46,
    10,
    0xA1,
    1,
    "c",
    0xA1,
    1,
    "h",
    0x70,
    512::32,
    0x60,
    7::16,
    0x52,
    100,
    0xA3,
    2,
    "en",
    0xE0,
    8,
    2,
    0xA3,
    2,
    "fr",
    2,
    "de",
    0xE0,
    2,
    0,
    0xA3,
    0xA3,
    1,
    "x",
    0xC1,
    6,
    2,
    0xA3,
    1,
    "p",
    0x52,
    1
  >>
  @open_with_desired_capability <<
    0x00,
    0x53,
    0x10,
    0xC0,
    16,
    9,
    0xA1,
    1,
    "c",
    0x40,
    0x40,
    0x40,
    0x40,
    0x40,
    0x40,
    0x40,
    0xA3,
    3,
    "cap"
  >>

  test "numeric and symbolic Open descriptors normalize to the same typed performative" do
    expected = minimal_open()

    for fixture <- [@minimal_open, @symbolic_open] do
      assert {:ok, ^expected, <<>>} = Performative.decode(fixture)
    end
  end

  test "decodes all ordered Open fields without losing exact AMQP value identities" do
    outgoing = AMQPValue.symbol("en")

    incoming =
      AMQPValue.array(:symbol, [AMQPValue.symbol("fr"), AMQPValue.symbol("de")])

    offered = AMQPValue.array(:symbol, [])
    desired = AMQPValue.symbol("x")
    properties = AMQPValue.map([{AMQPValue.symbol("p"), AMQPValue.uint(1)}])

    assert {:ok,
            %Open{
              container_id: %AMQPValue{type: :string, value: "c"},
              hostname: %AMQPValue{type: :string, value: "h"},
              max_frame_size: %AMQPValue{type: :uint, value: 512},
              channel_max: %AMQPValue{type: :ushort, value: 7},
              idle_time_out: %AMQPValue{type: :uint, value: 100},
              outgoing_locales: ^outgoing,
              incoming_locales: ^incoming,
              offered_capabilities: ^offered,
              desired_capabilities: ^desired,
              properties: ^properties
            }, <<>>} = Performative.decode(@full_open)
  end

  test "materializes Open defaults for omitted and explicitly null trailing fields" do
    expected = minimal_open()

    assert {:ok, ^expected, <<>>} = Performative.decode(@minimal_open)
    assert {:ok, ^expected, <<>>} = Performative.decode(@open_with_explicit_trailing_nulls)
  end

  test "returns the exact suffix after one Open value" do
    suffix = <<0xDE, 0xAD, 0xBE, 0xEF>>
    expected = minimal_open()

    assert {:ok, ^expected, ^suffix} = Performative.decode(@minimal_open <> suffix)
  end

  test "every strict numeric and symbolic Open prefix requests more input" do
    for fixture <- [@minimal_open, @symbolic_open, @full_open],
        prefix_size <- 0..(byte_size(fixture) - 1) do
      prefix = binary_part(fixture, 0, prefix_size)
      assert {:more, needed} = Performative.decode(prefix)
      assert needed > 0
    end
  end

  test "rejects missing or null mandatory container-id" do
    for fields <- [[], [AMQPValue.null()]] do
      assert_schema_error(
        Performative.decode(encode_open_fields!(fields)),
        :performative_decode,
        :malformed,
        :mandatory_field_missing
      )
    end
  end

  test "rejects exact type mismatches in every Open field" do
    valid_fields = [
      AMQPValue.string("c"),
      AMQPValue.string("h"),
      AMQPValue.uint(512),
      AMQPValue.ushort(7),
      AMQPValue.uint(100),
      AMQPValue.symbol("en"),
      AMQPValue.array(:symbol, [AMQPValue.symbol("fr")]),
      AMQPValue.symbol("offered"),
      AMQPValue.symbol("desired"),
      AMQPValue.map([{AMQPValue.symbol("p"), AMQPValue.uint(1)}])
    ]

    wrong_values = [
      AMQPValue.symbol("not-a-string"),
      AMQPValue.symbol("not-a-string"),
      AMQPValue.ulong(512),
      AMQPValue.uint(7),
      AMQPValue.ulong(100),
      AMQPValue.string("not-a-symbol"),
      AMQPValue.string("not-a-symbol-array"),
      AMQPValue.string("not-a-symbol"),
      AMQPValue.string("not-a-symbol"),
      AMQPValue.list([])
    ]

    for {wrong_value, index} <- Enum.with_index(wrong_values) do
      encoded = valid_fields |> List.replace_at(index, wrong_value) |> encode_open_fields!()

      assert_schema_error(
        Performative.decode(encoded),
        :performative_decode,
        :malformed,
        :field_type_mismatch
      )
    end
  end

  test "rejects invalid Open constraints and known malformed shapes" do
    assert_schema_error(
      Performative.decode(
        encode_open_fields!([
          AMQPValue.string("c"),
          AMQPValue.null(),
          AMQPValue.uint(511)
        ])
      ),
      :performative_decode,
      :malformed,
      :field_value_out_of_range
    )

    assert_schema_error(
      Performative.decode(
        encode_open_fields!([
          AMQPValue.string("c"),
          AMQPValue.null(),
          AMQPValue.null(),
          AMQPValue.null(),
          AMQPValue.null(),
          AMQPValue.null(),
          AMQPValue.null(),
          AMQPValue.null(),
          AMQPValue.null(),
          AMQPValue.map([{AMQPValue.string("p"), AMQPValue.uint(1)}])
        ])
      ),
      :performative_decode,
      :malformed,
      :invalid_property_key
    )

    assert_schema_error(
      Performative.decode(encode_open_fields!([AMQPValue.string("c") | nulls(10)])),
      :performative_decode,
      :malformed,
      :too_many_fields
    )

    assert_schema_error(
      Performative.decode(<<0x00, 0x53, 0x10, 0x40>>),
      :performative_decode,
      :malformed,
      :invalid_performative_body
    )
  end

  test "reports a well-formed unknown performative descriptor as unsupported" do
    assert_schema_error(
      Performative.decode(<<0x00, 0x53, 0x12, 0x45>>),
      :performative_decode,
      :unsupported,
      :unknown_descriptor
    )
  end

  test "canonically encodes Open with numeric descriptor, omitted defaults, and interior nulls" do
    assert {:ok, @minimal_open} = Performative.encode(minimal_open())

    open = %Open{
      container_id: AMQPValue.string("c"),
      desired_capabilities: AMQPValue.symbol("cap")
    }

    assert {:ok, @open_with_desired_capability} = Performative.encode(open)
    assert {:ok, @full_open} = Performative.encode(full_open())
  end

  test "invalid outbound Open structs return schema errors instead of raising" do
    malformed_symbol_array = %AMQPValue{
      type: :array,
      value: %AMQPValue.Array{element_type: :symbol, values: :not_a_list}
    }

    cases = [
      {%Open{container_id: nil}, :mandatory_field_missing},
      {%Open{container_id: AMQPValue.symbol("c")}, :field_type_mismatch},
      {%Open{container_id: AMQPValue.string("c"), max_frame_size: AMQPValue.uint(511)},
       :field_value_out_of_range},
      {%Open{
         container_id: AMQPValue.string("c"),
         outgoing_locales: AMQPValue.string("not-a-symbol")
       }, :field_type_mismatch},
      {%Open{
         container_id: AMQPValue.string("c"),
         outgoing_locales: malformed_symbol_array
       }, :field_type_mismatch},
      {%Open{
         container_id: AMQPValue.string("c"),
         properties: AMQPValue.map([{AMQPValue.string("p"), AMQPValue.uint(1)}])
       }, :invalid_property_key}
    ]

    for {open, reason} <- cases do
      assert_schema_error(
        Performative.encode(open),
        :performative_encode,
        :invalid_value,
        reason
      )
    end
  end

  defp minimal_open do
    %Open{
      container_id: AMQPValue.string("c"),
      max_frame_size: AMQPValue.uint(4_294_967_295),
      channel_max: AMQPValue.ushort(65_535)
    }
  end

  defp full_open do
    %Open{
      container_id: AMQPValue.string("c"),
      hostname: AMQPValue.string("h"),
      max_frame_size: AMQPValue.uint(512),
      channel_max: AMQPValue.ushort(7),
      idle_time_out: AMQPValue.uint(100),
      outgoing_locales: AMQPValue.symbol("en"),
      incoming_locales:
        AMQPValue.array(:symbol, [AMQPValue.symbol("fr"), AMQPValue.symbol("de")]),
      offered_capabilities: AMQPValue.array(:symbol, []),
      desired_capabilities: AMQPValue.symbol("x"),
      properties: AMQPValue.map([{AMQPValue.symbol("p"), AMQPValue.uint(1)}])
    }
  end

  defp encode_open_fields!(fields) do
    value = AMQPValue.described(AMQPValue.ulong(0x10), AMQPValue.list(fields))
    {:ok, encoded} = GravitonMQ.AMQP10.Codec.Value.encode(value)
    encoded
  end

  defp nulls(count), do: List.duplicate(AMQPValue.null(), count)

  defp assert_schema_error(result, operation, class, reason) do
    assert {:error,
            %Error{
              operation: ^operation,
              class: ^class,
              reason: ^reason
            }} = result
  end
end
