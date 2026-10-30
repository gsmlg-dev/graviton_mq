defmodule GravitonMQ.AMQP10.Codec.ValueBudgetTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.{Error, Limits, Value}
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  test "an oversized aggregate is rejected within a finite process memory budget" do
    value = AMQPValue.list(List.duplicate(AMQPValue.string(:binary.copy("x", 262_144)), 64))
    parent = self()

    {pid, monitor} =
      :erlang.spawn_opt(
        fn ->
          result = Value.encode(value, %{Limits.default() | max_value_bytes: 262_144})
          send(parent, {self(), result})
        end,
        [
          :monitor,
          {:max_heap_size,
           %{
             size: div(24_000_000, :erlang.system_info(:wordsize)),
             kill: true,
             error_logger: false,
             include_shared_binaries: true
           }}
        ]
      )

    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)

    assert_receive {^pid,
                    {:error,
                     %Error{
                       operation: :value_encode,
                       class: :limit_exceeded,
                       reason: :value_size_limit
                     }}},
                   5_000

    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
  end

  test "list byte exhaustion stops before processing later unsupported values" do
    value =
      AMQPValue.list([
        AMQPValue.string("abc"),
        AMQPValue.string("abc"),
        %AMQPValue{type: :extension, value: nil}
      ])

    assert_size_limit(value, 8)
  end

  test "map byte exhaustion stops before processing later unsupported values" do
    value =
      AMQPValue.map([
        {AMQPValue.uint(0), AMQPValue.string("abc")},
        {AMQPValue.uint(1), AMQPValue.string("abc")},
        {AMQPValue.uint(2), %AMQPValue{type: :extension, value: nil}}
      ])

    assert_size_limit(value, 8)
  end

  test "map byte exhaustion stops before building the remaining key index" do
    value =
      AMQPValue.map([
        {AMQPValue.uint(0), AMQPValue.string("abc")},
        {AMQPValue.uint(1), AMQPValue.string("abc")},
        {AMQPValue.uint(1), AMQPValue.null()}
      ])

    assert_size_limit(value, 8)
  end

  test "map item admission is bounded before flattening or validating child entries" do
    value = %AMQPValue{type: :map, value: [{AMQPValue.uint(0), AMQPValue.null()}, :invalid]}
    limits = %{Limits.default() | max_compound_items: 2}

    assert {:error, %Error{class: :limit_exceeded, reason: :compound_item_limit}} =
             Value.encode(value, limits)
  end

  test "symbol array byte exhaustion stops before processing later invalid symbols" do
    value =
      AMQPValue.array(:symbol, [
        AMQPValue.symbol("a"),
        AMQPValue.symbol("b"),
        AMQPValue.symbol("c"),
        AMQPValue.symbol(<<0x80>>)
      ])

    assert_size_limit(value, 6)
  end

  test "described byte exhaustion includes the descriptor before processing its body" do
    value =
      AMQPValue.described(AMQPValue.symbol("123456"), %AMQPValue{type: :extension, value: nil})

    assert_size_limit(value, 8)
  end

  test "a nested compound exhausts its containing budget before later values" do
    nested = AMQPValue.list([AMQPValue.string("abc")])
    value = AMQPValue.list([nested, nested, %AMQPValue{type: :extension, value: nil}])
    assert_size_limit(value, 12)
  end

  test "compound size limits retain their exact declared-size and constructor boundaries" do
    string253 = :binary.copy("x", 253)
    symbol256 = :binary.copy("s", 256)

    cases = [
      {AMQPValue.list([AMQPValue.string("abc")]), 6, <<0xC0, 6, 1, 0xA1, 3, "abc">>},
      {AMQPValue.map([{AMQPValue.null(), AMQPValue.string("abc")}]), 7,
       <<0xC1, 7, 2, 0x40, 0xA1, 3, "abc">>},
      {AMQPValue.array(:symbol, [AMQPValue.symbol("a"), AMQPValue.symbol("b")]), 6,
       <<0xE0, 6, 2, 0xA3, 1, "a", 1, "b">>},
      {AMQPValue.described(AMQPValue.ulong(1), AMQPValue.null()), 4, <<0x00, 0x53, 1, 0x40>>},
      {AMQPValue.list([AMQPValue.string(string253)]), 259,
       <<0xD0, 259::32, 1::32, 0xA1, 253, string253::binary>>},
      {AMQPValue.list(List.duplicate(AMQPValue.null(), 256)), 260,
       <<0xD0, 260::32, 256::32, :binary.copy(<<0x40>>, 256)::binary>>},
      {AMQPValue.array(:symbol, [AMQPValue.symbol("a"), AMQPValue.symbol(symbol256)]), 270,
       <<0xF0, 270::32, 2::32, 0xB3, 1::32, "a", 256::32, symbol256::binary>>},
      {AMQPValue.array(:symbol, List.duplicate(AMQPValue.symbol(""), 256)), 261,
       <<0xF0, 261::32, 256::32, 0xA3, :binary.copy(<<0>>, 256)::binary>>}
    ]

    for {value, budget, bytes} <- cases do
      limits = %{Limits.default() | max_value_bytes: budget}
      assert {:ok, ^bytes} = Value.encode(value, limits)
      assert_size_limit(value, budget - 1)
    end
  end

  defp assert_size_limit(value, bytes) do
    limits = %{Limits.default() | max_value_bytes: bytes}

    assert {:error,
            %Error{
              operation: :value_encode,
              class: :limit_exceeded,
              reason: :value_size_limit
            }} = Value.encode(value, limits)
  end
end
