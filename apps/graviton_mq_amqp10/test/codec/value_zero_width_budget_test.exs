defmodule GravitonMQ.AMQP10.Codec.ValueZeroWidthBudgetTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.{Error, Limits, Performative, Value}
  alias GravitonMQ.AMQP10.Codec.Message, as: MessageCodec
  alias GravitonMQ.AMQP10.Message
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  @limit %Error{
    operation: :value_decode,
    class: :limit_exceeded,
    reason: :zero_width_element_limit
  }

  # A 10-byte array that names 65,536 zero-width `list0` elements.
  defp zero_width_array(count, constructor \\ 0x45),
    do: <<0xF0, 5::32, count::32, constructor>>

  defp list32(items, count),
    do: <<0xD0, byte_size(items) + 4::32, count::32, items::binary>>

  test "default limits keep one maximal array of every zero-width constructor" do
    assert Limits.default().max_zero_width_elements == 65_536

    for constructor <- [0x41, 0x42, 0x43, 0x44, 0x45] do
      assert {:ok, %AMQPValue{type: :array, value: %AMQPValue.Array{values: values}}, <<>>} =
               Value.decode(zero_width_array(65_536, constructor))

      assert length(values) == 65_536
    end
  end

  test "zero-width elements beyond the budget fail before allocation" do
    limits = %{Limits.default() | max_zero_width_elements: 1_000}
    assert {:ok, _, <<>>} = Value.decode(zero_width_array(1_000), limits)
    assert {:error, @limit} = Value.decode(zero_width_array(1_001), limits)

    described = <<0xF0, 8::32, 1_001::32, 0x00, 0x53, 0x01, 0x41>>
    assert {:error, @limit} = Value.decode(described, limits)

    # Byte-carrying element constructors are bounded by their input bytes.
    assert {:ok, _, <<>>} =
             Value.decode(
               <<0xF0, 5 + 1_001::32, 1_001::32, 0x50, :binary.copy(<<1>>, 1_001)::binary>>,
               %{
                 limits
                 | max_zero_width_elements: 0
               }
             )
  end

  test "the budget is cumulative across sibling and nested containers" do
    pair = zero_width_array(40_000) <> zero_width_array(40_000)
    assert {:error, @limit} = Value.decode(list32(pair, 2))

    nested_element = <<5::32, 40_000::32, 0x41>>
    nested = nested_element <> nested_element
    outer = <<0xF0, 4 + 1 + byte_size(nested)::32, 2::32, 0xF0, nested::binary>>
    assert {:error, @limit} = Value.decode(outer)

    assert {:ok, %AMQPValue{type: :array}, <<>>, 10_000} =
             Value.decode_bounded(zero_width_array(40_000), Limits.default(), 50_000)

    assert {:error, @limit} =
             Value.decode_bounded(zero_width_array(40_000), Limits.default(), 39_999)
  end

  test "an amplifying first frame is rejected quickly within a finite heap" do
    items = :binary.copy(zero_width_array(65_536), 4_000)
    body = <<0x00, 0x53, 0x10>> <> list32(items, 4_000)
    parent = self()

    {pid, monitor} =
      :erlang.spawn_opt(
        fn -> send(parent, {self(), Performative.decode(body)}) end,
        [
          :monitor,
          {:max_heap_size,
           %{
             size: div(64_000_000, :erlang.system_info(:wordsize)),
             kill: true,
             error_logger: false
           }}
        ]
      )

    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    assert_receive {^pid, {:error, @limit}}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
  end

  test "all sections of one message share a single budget" do
    section = <<0x00, 0x53, 0x76>> <> list32(zero_width_array(40_000), 1)
    assert {:ok, %Message{}, <<>>} = MessageCodec.decode(section)
    assert {:error, @limit} = MessageCodec.decode(section <> section)
  end

  test "rejected delivery annotations share a single budget" do
    info = <<0xD1, 4 + 3 + 10::32, 2::32, 0xA3, 1, "k">> <> zero_width_array(40_000, 0x41)
    error_list = list32(<<0xA3, 1, "c", 0x40>> <> info, 3)
    rejected = <<0x00, 0x53, 0x1D>> <> error_list
    {:ok, key} = Value.encode(AMQPValue.symbol("rejected"))

    one = key <> rejected
    two = one <> one

    assert :ok = Message.check_annotations(annotations(one, 2))
    assert {:error, :annotation_limit} = Message.check_annotations(annotations(two, 4))
  end

  defp annotations(body, count),
    do: <<0x00, 0x53, 0x71, 0xD1, byte_size(body) + 4::32, count::32, body::binary>>
end
