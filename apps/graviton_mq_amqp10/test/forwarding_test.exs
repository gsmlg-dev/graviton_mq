defmodule GravitonMQ.AMQP10.ForwardingTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Message, as: MessageCodec
  alias GravitonMQ.AMQP10.Codec.Value, as: WireValue
  alias GravitonMQ.AMQP10.{Forwarding, Value}
  alias GravitonMQ.Core.Message.Mutation

  # Deliberately use long descriptor/list/map/scalar constructors throughout
  # the original message; canonical reconstruction would change these bytes.
  @header <<0, 0x80, 0x70::64, 0xD0, 13::32, 5::32, 0x40, 0x40, 0x40, 0x40, 0x70, 3::32>>
  @properties <<0, 0x80, 0x73::64, 0xD0, 11::32, 1::32, 0xB1, 2::32, "id">>
  @application <<0, 0x80, 0x74::64, 0xD1, 15::32, 2::32, 0xB1, 1::32, "k", 0x71, 7::32>>
  @data <<0, 0x80, 0x75::64, 0xB0, 3::32, 0, 0xFF, 1>>
  @footer <<0, 0x80, 0x78::64, 0xD1, 12::32, 2::32, 0xB3, 1::32, "f", 0x56, 1>>
  @tail @properties <> @application <> @data <> @data <> @footer
  @annotations <<0, 0x80, 0x72::64, 0xD1, 15::32, 2::32, 0xB3, 1::32, "a", 0x70, 9::32>>
  @delivery_annotations <<0, 0x80, 0x71::64, 0xD1, 15::32, 2::32, 0xB3, 1::32, "d", 0x70, 8::32>>

  test "an untouched message and both mutable sections retain exact original wire constructors" do
    raw = @header <> @annotations <> @tail
    assert {:ok, ^raw} = Forwarding.prepare(raw, metadata(), byte_size(raw))
    assert {:ok, @tail} = Forwarding.prepare(@tail, metadata(), byte_size(@tail))
    assert {:ok, <<>>} = Forwarding.prepare(<<>>, metadata(), 1)
  end

  test "each new hop removes delivery annotations even when original exceeds the output budget" do
    raw = @header <> @delivery_annotations <> @annotations <> @tail
    expected = @header <> @annotations <> @tail
    assert {:ok, ^expected} = Forwarding.prepare(raw, metadata(), byte_size(expected))
    assert raw == @header <> @delivery_annotations <> @annotations <> @tail

    assert {:error, :message_too_large} =
             Forwarding.prepare(raw, metadata(), byte_size(expected) - 1)

    assert {:ok, ^expected} = Forwarding.prepare(expected, metadata(), byte_size(expected))
  end

  test "onward delivery clears a true first-acquirer claim without changing the bare message" do
    header =
      <<0, 0x80, 0x70::64, 0xD0, 13::32, 5::32, 0x40, 0x40, 0x40, 0x41, 0x70, 3::32>>

    original = header <> @annotations <> @tail

    for failed <- [0, 2] do
      assert {:ok, forwarded} = Forwarding.prepare(original, metadata(failed), 4096)
      assert {:ok, parsed, <<>>} = MessageCodec.decode(forwarded)
      assert {:header, %Value{type: :list, value: fields}} = hd(parsed.sections)
      assert Enum.at(fields, 3) == Value.boolean(false)
      assert Enum.at(fields, 4) == Value.uint(3 + failed)
      assert tail(forwarded) == @tail
      assert byte_size(forwarded) < byte_size(original)

      assert {:ok, ^forwarded} =
               Forwarding.prepare(original, metadata(failed), byte_size(forwarded))

      assert {:error, :message_too_large} =
               Forwarding.prepare(original, metadata(failed), byte_size(forwarded) - 1)
    end

    assert original == header <> @annotations <> @tail
  end

  test "delivery count adds cumulative failed deliveries while bare-message bytes remain immutable" do
    raw = @header <> @annotations <> @tail
    header = <<0, 0x53, 0x70, 0xC0, 7, 5, 0x40, 0x40, 0x40, 0x40, 0x52, 5>>
    expected = header <> @annotations <> @tail
    assert {:ok, ^expected} = Forwarding.prepare(raw, metadata(2), byte_size(expected))
    assert raw == @header <> @annotations <> @tail
    assert tail(expected) == @tail
  end

  test "absent and null delivery counts default to zero and create only the required header" do
    expected = <<0, 0x53, 0x70, 0xC0, 7, 5, 0x40, 0x40, 0x40, 0x40, 0x52, 2>> <> @tail

    for raw <- [
          @tail,
          <<0, 0x53, 0x70, 0x45>> <> @tail,
          <<0, 0x53, 0x70, 0xC0, 6, 5, 0x40, 0x40, 0x40, 0x40, 0x40>> <> @tail
        ] do
      assert {:ok, ^expected} = Forwarding.prepare(raw, metadata(2), byte_size(expected))
    end
  end

  test "ordered annotation deltas replace exact tagged keys and preserve other entries" do
    existing =
      Value.map([
        {Value.symbol("1"), Value.string("symbol")},
        {Value.ulong(1), Value.string("numeric")},
        {Value.symbol("keep"), Value.binary(<<0, 255>>)}
      ])

    first =
      mutation([{Value.symbol("1"), Value.uint(4)}, {Value.symbol("new"), Value.string("first")}])

    second =
      mutation([{Value.ulong(1), Value.null()}, {Value.symbol("new"), Value.string("last")}])

    raw = @header <> section(:message_annotations, existing) <> @tail
    assert {:ok, forwarded} = Forwarding.prepare(raw, metadata(0, [first, second]), 4096)
    assert {:ok, parsed, <<>>} = MessageCodec.decode(forwarded)

    assert {:message_annotations, merged} =
             Enum.find(parsed.sections, &(elem(&1, 0) == :message_annotations))

    assert merged ==
             Value.map([
               {Value.symbol("1"), Value.uint(4)},
               {Value.ulong(1), Value.null()},
               {Value.symbol("keep"), Value.binary(<<0, 255>>)},
               {Value.symbol("new"), Value.string("last")}
             ])

    assert binary_part(forwarded, 0, byte_size(@header)) == @header
    assert tail(forwarded) == @tail
  end

  test "an empty or semantically identical annotation mutation preserves noncanonical bytes" do
    raw = @header <> @annotations <> @tail

    for mutations <- [[mutation([])], [mutation([{Value.symbol("a"), Value.uint(9)}])]] do
      assert {:ok, ^raw} = Forwarding.prepare(raw, metadata(0, mutations), byte_size(raw))
    end

    assert {:ok, @tail} = Forwarding.prepare(@tail, metadata(0, [mutation([])]), byte_size(@tail))
  end

  test "new annotations appear before properties while every bare-message byte remains exact" do
    delta = mutation([{Value.symbol("added"), Value.boolean(true)}])
    assert {:ok, forwarded} = Forwarding.prepare(@tail, metadata(0, [delta]), 4096)

    assert forwarded ==
             section(
               :message_annotations,
               Value.map([{Value.symbol("added"), Value.boolean(true)}])
             ) <> @tail
  end

  test "header and merged annotation growth share the final byte budget" do
    delta = mutation([{Value.symbol("a"), Value.binary(:binary.copy(<<7>>, 300))}])
    raw = @header <> @annotations <> @tail
    assert {:ok, forwarded} = Forwarding.prepare(raw, metadata(256, [delta]), 4096)
    size = byte_size(forwarded)
    assert {:ok, ^forwarded} = Forwarding.prepare(raw, metadata(256, [delta]), size)

    assert {:error, :message_too_large} =
             Forwarding.prepare(raw, metadata(256, [delta]), size - 1)

    assert {:error, :message_too_large} = Forwarding.prepare(raw, metadata(256, [delta]), 1)
  end

  test "overflow and invalid metadata fail explicitly without changing authoritative bytes" do
    raw = @header <> @tail

    assert {:error, :delivery_count_overflow} =
             Forwarding.prepare(raw, metadata(4_294_967_295), 4096)

    assert {:error, :delivery_count_overflow} =
             Forwarding.prepare(raw, metadata(4_294_967_296), 4096)

    for invalid <- [
          nil,
          %{},
          metadata(-1),
          metadata(18_446_744_073_709_551_616),
          metadata(:failed),
          %{failure_count: 0, mutations: :invalid},
          %{failure_count: 0, mutations: [nil | :tail]}
        ] do
      assert {:error, :invalid_metadata} = Forwarding.prepare(raw, invalid, 4096)
    end

    for limit <- [0, -1, :infinity, 4_294_967_296] do
      assert {:error, :invalid_arguments} = Forwarding.prepare(raw, metadata(), limit)
    end

    assert {:error, :invalid_arguments} = Forwarding.prepare(:not_bytes, metadata(), 4096)
  end

  test "wrong formats map keys trailing data and truncated mutation encodings are refused" do
    raw = @header <> @tail
    good = mutation([{Value.symbol("x"), Value.uint(0)}])

    for invalid <- [
          %{good | format: "foreign"},
          %{good | encoded_data: <<0x43>>},
          %{good | encoded_data: good.encoded_data <> <<0x40>>},
          %{good | encoded_data: <<0xC1, 5, 2, 0xA3>>},
          mutation([{Value.uint(1), Value.null()}]),
          %{},
          Map.put(good, :extra, :forged)
        ] do
      assert {:error, :invalid_mutation} = Forwarding.prepare(raw, metadata(0, [invalid]), 4096)
    end
  end

  test "message validation still rejects malformed ordering fields and incomplete content" do
    for raw <- [
          @header <> @header <> @tail,
          @data <> @properties,
          <<0, 0x53, 0x70, 0xC0, 2, 1, 0x43>>
        ] do
      assert {:error, :malformed_message} = Forwarding.prepare(raw, metadata(), 4096)
    end

    assert {:error, :incomplete_message} =
             Forwarding.prepare(<<0, 0x53, 0x75, 0xA0, 3, 1>>, metadata(), 4096)

    assert {:error, :unsupported_message} =
             Forwarding.prepare(<<0, 0x53, 0x79, 0x45>>, metadata(), 4096)
  end

  defp metadata(failed \\ 0, mutations \\ []), do: %{failure_count: failed, mutations: mutations}

  defp mutation(entries) do
    {:ok, bytes} = WireValue.encode(Value.map(entries))
    %Mutation{format: "amqp-1.0/message-annotations", encoded_data: bytes}
  end

  defp section(name, body) do
    {:ok, bytes} = MessageCodec.encode_section({name, body})
    bytes
  end

  defp tail(forwarded),
    do: binary_part(forwarded, byte_size(forwarded) - byte_size(@tail), byte_size(@tail))
end
