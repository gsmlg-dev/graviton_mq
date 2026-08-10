defmodule GravitonMQ.AMQP10.ValueTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Value

  alias GravitonMQ.AMQP10.Error, as: AMQPError
  alias GravitonMQ.AMQP10.Message, as: AMQPMessage

  test "binary, string, and symbol retain distinct semantic identities" do
    binary = Value.binary("orders")
    string = Value.string("orders")
    symbol = Value.symbol("orders")

    assert %Value{type: :binary, value: "orders"} = binary
    assert %Value{type: :string, value: "orders"} = string
    assert %Value{type: :symbol, value: "orders"} = symbol
    assert MapSet.size(MapSet.new([binary, string, symbol])) == 3
  end

  test "float and double retain exact IEEE bit patterns including special values" do
    float_nan = <<0x7FC00001::32>>
    double_positive_zero = <<0::64>>
    double_negative_zero = <<0x8000000000000000::64>>

    assert %Value{type: :float, value: ^float_nan} = Value.float(float_nan)

    assert %Value{type: :double, value: ^double_positive_zero} =
             Value.double(double_positive_zero)

    assert %Value{type: :double, value: ^double_negative_zero} =
             Value.double(double_negative_zero)

    refute Value.double(double_positive_zero) == Value.double(double_negative_zero)
  end

  test "signed and unsigned integer widths remain distinct semantic values" do
    values = [
      Value.ubyte(1),
      Value.ushort(1),
      Value.uint(1),
      Value.ulong(1),
      Value.byte(1),
      Value.short(1),
      Value.int(1),
      Value.long(1)
    ]

    assert MapSet.size(MapSet.new(values)) == 8
  end

  test "nested compound values keep the exact type of every value and map key" do
    nested =
      Value.map([
        {Value.symbol("key"),
         Value.list([
           Value.string("same bytes"),
           Value.binary("same bytes"),
           Value.map([{Value.string("inner"), Value.ushort(7)}])
         ])}
      ])

    assert %Value{
             type: :map,
             value: [
               {%Value{type: :symbol},
                %Value{
                  type: :list,
                  value: [
                    %Value{type: :string},
                    %Value{type: :binary},
                    %Value{
                      type: :map,
                      value: [{%Value{type: :string}, %Value{type: :ushort, value: 7}}]
                    }
                  ]
                }}
             ]
           } = nested
  end

  test "compound constructors reject untagged nested values" do
    assert_raise ArgumentError, "AMQP list members must be tagged values", fn ->
      Value.list(["bare"])
    end

    assert_raise ArgumentError, "AMQP map entries must contain tagged keys and values", fn ->
      Value.map([{Value.string("key"), "bare"}])
    end
  end

  test "described values preserve both an extension descriptor and its value" do
    descriptor = Value.symbol("com.example:extension")
    value = Value.map([{Value.string("payload"), Value.uint(42)}])

    assert %Value{
             type: :described,
             value: %Value.Described{descriptor: ^descriptor, value: ^value}
           } = Value.described(descriptor, value)
  end

  test "arrays retain an explicit semantic element type" do
    one = Value.uint(1)
    two = Value.uint(2)

    assert %Value{
             type: :array,
             value: %Value.Array{element_type: :uint, values: [^one, ^two]}
           } = Value.array(:uint, [one, two])
  end

  test "arrays reject values whose semantic type differs from the element type" do
    assert_raise ArgumentError,
                 "AMQP array values do not match element type :uint",
                 fn -> Value.array(:uint, [Value.uint(1), Value.string("two")]) end
  end

  test "described arrays preserve and enforce the shared descriptor and underlying type" do
    descriptor = Value.symbol("com.example:counter")
    element_type = {:described, descriptor, :uint}

    one = Value.described(descriptor, Value.uint(1))
    two = Value.described(descriptor, Value.uint(2))

    assert %Value{
             type: :array,
             value: %Value.Array{element_type: ^element_type, values: [^one, ^two]}
           } = Value.array(element_type, [one, two])

    assert_raise ArgumentError, fn ->
      Value.array(element_type, [one, Value.described(descriptor, Value.string("wrong"))])
    end
  end

  test "compact wire constructors are not semantic value types" do
    assert %Value{type: :uint, value: 0} = Value.uint(0)
    assert %Value{type: :uint, value: 12} = Value.uint(12)

    refute :uint0 in Value.semantic_types()
    refute :smalluint in Value.semantic_types()
    refute :smallulong in Value.semantic_types()
  end

  test "AMQP errors and message sections use exact AMQP values" do
    condition = Value.symbol("amqp:not-found")
    description = Value.string("missing")
    info = Value.map([{Value.symbol("key"), Value.binary("value")}])

    assert %AMQPError{
             condition: ^condition,
             description: ^description,
             info: ^info
           } = %AMQPError{condition: condition, description: description, info: info}

    encoded_content = <<0, 83, 119, 161, 5, "hello">>
    section = {:amqp_value, Value.string("hello")}

    assert %AMQPMessage{
             message_format: 9,
             encoded_content: ^encoded_content,
             sections: [^section]
           } = %AMQPMessage{
             message_format: 9,
             encoded_content: encoded_content,
             sections: [section]
           }
  end
end
