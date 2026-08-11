defmodule GravitonMQ.AMQP10.Codec.EndCloseErrorTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Performative
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.AMQP10.Performative.Close
  alias GravitonMQ.AMQP10.Performative.End
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  test "decodes a numeric Error with an extension condition inside End" do
    fixture = end_with_error(numeric_error(<<0xA3, 16, "vendor:condition">>, 1))

    assert {:ok,
            %End{
              error: %ProtocolError{
                condition: %AMQPValue{type: :symbol, value: "vendor:condition"},
                description: nil,
                info: nil
              }
            }, <<>>} = Performative.decode(fixture)
  end

  test "decodes a symbolic Error descriptor inside Close" do
    fixture = close_with_error(symbolic_error(<<0xA3, 1, "x">>, 1))

    assert {:ok,
            %Close{
              error: %ProtocolError{
                condition: %AMQPValue{type: :symbol, value: "x"}
              }
            }, <<>>} = Performative.decode(fixture)
  end

  test "retains full Error fields and ordered info entries" do
    fields =
      <<
        0xA3,
        1,
        "x",
        0xA1,
        1,
        "d",
        0xC1,
        11,
        4,
        0xA3,
        1,
        "a",
        0x52,
        1,
        0xA3,
        1,
        "b",
        0x52,
        2
      >>

    assert {:ok, %End{error: %ProtocolError{} = error}, <<>>} =
             fields |> numeric_error(3) |> end_with_error() |> Performative.decode()

    assert error.condition == AMQPValue.symbol("x")
    assert error.description == AMQPValue.string("d")

    assert error.info ==
             AMQPValue.map([
               {AMQPValue.symbol("a"), AMQPValue.uint(1)},
               {AMQPValue.symbol("b"), AMQPValue.uint(2)}
             ])
  end

  test "canonically encodes an extension condition with numeric descriptors" do
    value = %End{error: %ProtocolError{condition: AMQPValue.symbol("x")}}

    expected =
      <<
        0x00,
        0x53,
        0x17,
        0xC0,
        10,
        1,
        0x00,
        0x53,
        0x1D,
        0xC0,
        4,
        1,
        0xA3,
        1,
        "x"
      >>

    assert {:ok, ^expected} = Performative.encode(value)
    assert {:ok, ^value, <<>>} = Performative.decode(expected)
  end

  test "preserves the interior description null when info is present" do
    value = %Close{
      error: %ProtocolError{
        condition: AMQPValue.symbol("x"),
        info: AMQPValue.map([{AMQPValue.symbol("k"), AMQPValue.uint(1)}])
      }
    }

    expected =
      <<
        0x00,
        0x53,
        0x18,
        0xC0,
        19,
        1,
        0x00,
        0x53,
        0x1D,
        0xC0,
        13,
        3,
        0xA3,
        1,
        "x",
        0x40,
        0xC1,
        6,
        2,
        0xA3,
        1,
        "k",
        0x52,
        1
      >>

    assert {:ok, ^expected} = Performative.encode(value)
    assert {:ok, ^value, <<>>} = Performative.decode(expected)
  end

  test "rejects invalid outbound termination and Error structs at the schema boundary" do
    raw_error =
      AMQPValue.described(
        AMQPValue.ulong(0x1D),
        AMQPValue.list([AMQPValue.symbol("x")])
      )

    invalid_values = [
      {%End{error: raw_error}, :field_type_mismatch,
       %{
         performative: :end,
         field: :error,
         index: 0,
         expected: :error,
         actual: :described
       }},
      {%End{error: %ProtocolError{condition: nil}}, :mandatory_field_missing,
       %{
         performative: :end,
         nested_schema: :error,
         field: :condition,
         index: 0
       }},
      {%Close{
         error: %ProtocolError{
           condition: AMQPValue.symbol("x"),
           description: AMQPValue.uint(1)
         }
       }, :field_type_mismatch,
       %{
         performative: :close,
         nested_schema: :error,
         field: :description,
         index: 1,
         expected: :string,
         actual: :uint
       }},
      {%Close{
         error: %ProtocolError{
           condition: AMQPValue.symbol("x"),
           info: AMQPValue.map([{AMQPValue.string("k"), AMQPValue.uint(1)}])
         }
       }, :invalid_property_key,
       %{
         performative: :close,
         nested_schema: :error,
         field: :info,
         index: 2
       }}
    ]

    for {value, reason, details} <- invalid_values do
      assert {:error,
              %Error{
                operation: :performative_encode,
                class: :invalid_value,
                reason: ^reason,
                details: ^details
              }} = Performative.encode(value)
    end
  end

  test "propagates unsupported Error info values from the value encoder" do
    value = %End{
      error: %ProtocolError{
        condition: AMQPValue.symbol("x"),
        info: AMQPValue.map([{AMQPValue.symbol("flag"), AMQPValue.boolean(true)}])
      }
    }

    assert {:error,
            %Error{
              operation: :value_encode,
              class: :unsupported,
              reason: :semantic_type,
              details: %{type: :boolean}
            }} = Performative.encode(value)
  end

  test "rejects an unknown nested Error descriptor as malformed End data" do
    fixture = end_with_error(<<0x00, 0x53, 0x7F, 0x45>>)

    assert {:error,
            %Error{
              operation: :performative_decode,
              class: :malformed,
              reason: :invalid_error_descriptor,
              details: %{
                performative: :end,
                field: :error,
                index: 0,
                expected: :error,
                actual: :described,
                descriptor: %AMQPValue{}
              }
            }} = Performative.decode(fixture)
  end

  test "requires the nested Error condition" do
    for error_value <- [numeric_error(<<>>, 0), numeric_error(<<0x40>>, 1)] do
      assert {:error,
              %Error{
                operation: :performative_decode,
                class: :malformed,
                reason: :mandatory_field_missing,
                details: %{
                  performative: :end,
                  nested_schema: :error,
                  field: :condition,
                  index: 0
                }
              }} = error_value |> end_with_error() |> Performative.decode()
    end
  end

  test "validates exact Error field types and info keys" do
    wrong_condition = numeric_error(<<0xA1, 1, "x">>, 1)
    wrong_description = numeric_error(<<0xA3, 1, "x", 0x52, 1>>, 2)
    wrong_info = numeric_error(<<0xA3, 1, "x", 0x40, 0x52, 1>>, 3)

    bad_info_key =
      numeric_error(
        <<0xA3, 1, "x", 0x40, 0xC1, 6, 2, 0xA1, 1, "k", 0x52, 1>>,
        3
      )

    assert {:error,
            %Error{
              operation: :performative_decode,
              class: :malformed,
              reason: :field_type_mismatch,
              details: %{
                performative: :end,
                nested_schema: :error,
                field: :condition,
                index: 0,
                expected: :symbol,
                actual: :string
              }
            }} = wrong_condition |> end_with_error() |> Performative.decode()

    assert {:error,
            %Error{
              operation: :performative_decode,
              class: :malformed,
              reason: :field_type_mismatch,
              details: %{
                performative: :end,
                nested_schema: :error,
                field: :description,
                index: 1,
                expected: :string,
                actual: :uint
              }
            }} = wrong_description |> end_with_error() |> Performative.decode()

    assert {:error,
            %Error{
              operation: :performative_decode,
              class: :malformed,
              reason: :field_type_mismatch,
              details: %{
                performative: :end,
                nested_schema: :error,
                field: :info,
                index: 2,
                expected: :map,
                actual: :uint
              }
            }} = wrong_info |> end_with_error() |> Performative.decode()

    assert {:error,
            %Error{
              operation: :performative_decode,
              class: :malformed,
              reason: :invalid_property_key,
              details: %{
                performative: :end,
                nested_schema: :error,
                field: :info,
                index: 2
              }
            }} = bad_info_key |> end_with_error() |> Performative.decode()
  end

  test "rejects non-list and overlong Error bodies" do
    non_list = end_with_error(<<0x00, 0x53, 0x1D, 0x40>>)

    overlong =
      <<0xA3, 1, "x", 0x40, 0x40, 0x40>>
      |> numeric_error(4)
      |> end_with_error()

    assert {:error,
            %Error{
              operation: :performative_decode,
              class: :malformed,
              reason: :invalid_error_body,
              details: %{
                performative: :end,
                nested_schema: :error,
                field: :error,
                index: 0,
                expected: :list,
                actual: :null
              }
            }} = Performative.decode(non_list)

    assert {:error,
            %Error{
              operation: :performative_decode,
              class: :malformed,
              reason: :too_many_fields,
              details: %{
                performative: :end,
                nested_schema: :error,
                actual: 4,
                maximum: 3
              }
            }} = Performative.decode(overlong)
  end

  test "does not dispatch Error as a top-level performative" do
    fixture = numeric_error(<<0xA3, 1, "x">>, 1)

    assert {:error,
            %Error{
              operation: :performative_decode,
              class: :unsupported,
              reason: :unknown_descriptor
            }} = Performative.decode(fixture)
  end

  test "every strict prefix remains incomplete" do
    full_fields =
      <<0xA3, 1, "x", 0xA1, 1, "d", 0xC1, 6, 2, 0xA3, 1, "k", 0x52, 1>>

    fixtures = [
      <<0x00, 0x53, 0x17, 0x45>>,
      <<0x00, 0x53, 0x18, 0x45>>,
      end_with_error(numeric_error(<<0xA3, 1, "x">>, 1)),
      close_with_error(symbolic_error(<<0xA3, 1, "x">>, 1)),
      end_with_error(numeric_error(full_fields, 3))
    ]

    for fixture <- fixtures,
        length <- 0..(byte_size(fixture) - 1) do
      assert {:more, needed} =
               fixture
               |> binary_part(0, length)
               |> Performative.decode()

      assert needed > 0
    end
  end

  test "returns the exact suffix after a nested Error" do
    suffix = <<0xFE, 0xED, 0xFA, 0xCE>>
    fixture = end_with_error(numeric_error(<<0xA3, 1, "x">>, 1))

    assert {:ok, %End{}, ^suffix} = Performative.decode(fixture <> suffix)
  end

  test "applies the existing item limit independently to nested compounds" do
    fields = <<0xA3, 1, "x", 0xA1, 1, "d", 0xC1, 1, 0>>
    fixture = fields |> numeric_error(3) |> end_with_error()
    limits = %{Limits.default() | max_compound_items: 2}

    assert {:error,
            %Error{
              operation: :value_decode,
              class: :limit_exceeded,
              reason: :compound_item_limit,
              offset: nil,
              details: %{}
            }} = Performative.decode(fixture, limits)
  end

  test "propagates malformed UTF-8 and unsupported nested constructors" do
    invalid_utf8 =
      <<0xA3, 1, "x", 0xA1, 1, 0xFF>>
      |> numeric_error(2)
      |> end_with_error()

    unsupported_boolean =
      <<0xA3, 1, "x", 0x40, 0xC1, 5, 2, 0xA3, 1, "k", 0x41>>
      |> numeric_error(3)
      |> end_with_error()

    assert {:error,
            %Error{
              operation: :value_decode,
              class: :malformed,
              reason: :invalid_utf8,
              offset: 2,
              details: %{}
            }} = Performative.decode(invalid_utf8)

    assert {:error,
            %Error{
              operation: :value_decode,
              class: :unsupported,
              reason: :format_code,
              offset: 0,
              details: %{format_code: 0x41}
            }} = Performative.decode(unsupported_boolean)
  end

  test "propagates duplicate ordered-map keys from the value decoder" do
    duplicate_info =
      <<
        0xA3,
        1,
        "x",
        0x40,
        0xC1,
        11,
        4,
        0xA3,
        1,
        "k",
        0x52,
        1,
        0xA3,
        1,
        "k",
        0x52,
        2
      >>
      |> numeric_error(3)
      |> end_with_error()

    assert {:error,
            %Error{
              operation: :value_decode,
              class: :malformed,
              reason: :duplicate_map_key,
              offset: nil,
              details: %{}
            }} = Performative.decode(duplicate_info)
  end

  defp numeric_error(fields, count) do
    size = byte_size(fields) + 1
    <<0x00, 0x53, 0x1D, 0xC0, size, count, fields::binary>>
  end

  defp symbolic_error(fields, count) do
    size = byte_size(fields) + 1
    <<0x00, 0xA3, 15, "amqp:error:list", 0xC0, size, count, fields::binary>>
  end

  defp end_with_error(error_value) do
    size = byte_size(error_value) + 1
    <<0x00, 0x53, 0x17, 0xC0, size, 1, error_value::binary>>
  end

  defp close_with_error(error_value) do
    size = byte_size(error_value) + 1
    <<0x00, 0x53, 0x18, 0xC0, size, 1, error_value::binary>>
  end
end
