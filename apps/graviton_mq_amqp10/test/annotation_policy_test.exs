defmodule GravitonMQ.AMQP10.AnnotationPolicyTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.{Message, Value}
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value, as: Codec

  test "unknown mandatory annotation keys are rejected in each annotation section" do
    for section <- [0x71, 0x72, 0x78], key <- [Value.symbol("x-required"), Value.ulong(7)] do
      assert {:error, :not_implemented} = check(annotation(section, key, <<0x41>>))
    end
  end

  test "optional annotations retain unsupported values and authoritative bytes" do
    for value <- opaque_values() do
      raw =
        annotation(0x71, Value.symbol("x-opt-hop"), value) <>
          annotation(0x72, Value.symbol("x-opt-meta"), value) <>
          <<0, 0x53, 0x77, value::binary>> <>
          annotation(0x78, Value.symbol("x-opt-footer"), value)

      assert :ok = check(raw)
      message = %Message{message_format: Value.uint(0), encoded_content: raw}
      assert message.encoded_content == raw
    end
  end

  test "a mandatory footer is found after every bounded unsupported body constructor" do
    for value <- opaque_values() do
      raw =
        <<0, 0x53, 0x77, value::binary>> <>
          annotation(0x78, Value.symbol("must-understand"), <<0x40>>)

      assert {:error, :not_implemented} = check(raw)
    end
  end

  test "symbolic section and Error descriptors preserve the understood rejected annotation" do
    for descriptor <- [Value.ulong(0x1D), Value.symbol("amqp:error:list")] do
      error = Value.described(descriptor, Value.list([Value.symbol("custom:condition")]))
      {:ok, value} = Codec.encode(error)
      key = Value.symbol("rejected")
      numeric = annotation(0x71, key, value)
      <<0, 0x53, 0x71, body::binary>> = numeric
      {:ok, symbolic} = Codec.encode(Value.symbol("amqp:delivery-annotations:map"))
      assert :ok = check(<<0, symbolic::binary, body::binary>>)
      assert {:error, :not_implemented} = check(annotation(0x72, key, value))
      assert {:error, :not_implemented} = check(annotation(0x78, key, value))
    end

    for value <- [<<0x42>>, <<0, 0x53, 0x1D, 0x45>>] do
      assert {:error, :malformed_annotations} =
               check(annotation(0x71, Value.symbol("rejected"), value))
    end
  end

  test "annotation envelope, key and byte limits fail explicitly" do
    raw = annotation(0x72, Value.symbol("x-opt-a"), <<0x41>>)
    assert {:error, :annotation_limit} = check(raw, %Limits{max_value_bytes: byte_size(raw) - 1})
    assert {:error, :annotation_limit} = check(raw, %Limits{max_compound_items: 1})
    assert {:error, :malformed_annotations} = check(<<0, 0x53, 0x72, 0xC1, 2, 1, 0x40>>)
    assert {:error, :malformed_annotations} = check(<<0, 0x53, 0x72, 0xC1, 5, 2, 0xA3>>)

    assert {:error, :malformed_annotations} =
             check(annotation(0x72, Value.string("x-opt-a"), <<0x41>>))

    nested = <<0, 0x53, 9, 0, 0x53, 9, 0x41>>

    assert {:error, :annotation_limit} =
             check(
               annotation(0x72, Value.symbol("x-opt-a"), nested),
               %Limits{max_nesting_depth: 1}
             )
  end

  test "raw protocol payloads remain opaque and are not claimed to be decoded messages" do
    assert :ok = check(<<1, 2, 3>>)
    assert :ok = check(<<>>)
  end

  defp check(raw, limits \\ %Limits{}) do
    assert function_exported?(Message, :check_annotations, 2)
    apply(Message, :check_annotations, [raw, limits])
  end

  defp annotation(section, key, value) do
    {:ok, key} = Codec.encode(key)
    body = key <> value
    <<0, 0x53, section, 0xD1, byte_size(body) + 4::32, 2::32, body::binary>>
  end

  defp opaque_values do
    [
      <<0x72, 0x3F800000::32>>,
      <<0x82, 0x3FF0000000000000::64>>,
      <<0x74, 0::32>>,
      <<0x84, 0::64>>,
      <<0x94, 0::128>>,
      <<0x73, 65::32>>,
      <<0xE0, 3, 1, 0x52, 1>>,
      <<0xF0, 9::32, 1::32, 0x70, 1::32>>,
      <<0, 0x53, 9, 0xC0, 6, 1, 0x72, 0x3F800000::32>>
    ]
  end
end
