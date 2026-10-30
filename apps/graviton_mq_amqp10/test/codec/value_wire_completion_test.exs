defmodule GravitonMQ.AMQP10.Codec.ValueWireCompletionTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.AMQP10.Codec.{Error, Limits, Value}
  alias GravitonMQ.AMQP10.Value, as: V

  # OASIS AMQP 1.0 Part 1 §§1.2.4, 1.6.11–16. Exact binary payloads
  # preserve IEEE NaN payload/signaling bits, signed zero and BID decimals.
  @scalars [
    {0x72, V.float(<<0x7FC01234::32>>)},
    {0x72, V.float(<<0x7F801234::32>>)},
    {0x72, V.float(<<0x80000000::32>>)},
    {0x82, V.double(<<0x7FF8000012345678::64>>)},
    {0x82, V.double(<<0xFFF0000012345678::64>>)},
    {0x82, V.double(<<0x8000000000000000::64>>)},
    {0x74, V.decimal32(<<0xFEDCBA98::32>>)},
    {0x84, V.decimal64(<<0xFEDCBA9876543210::64>>)},
    {0x94, V.decimal128(<<0x000102030405060708090A0B0C0D0E0F::128>>)}
  ]

  test "floating and decimal wire scalars preserve exact bits, suffixes and every strict prefix" do
    for {code, value} <- @scalars do
      wire = <<code, value.value::binary>>
      assert {:ok, ^value, <<0x40>>} = Value.decode(wire <> <<0x40>>)
      assert {:ok, ^wire} = Value.encode(value)
      assert {:ok, size} = Value.encoded_size(value)
      assert size == byte_size(wire)

      for prefix <- 1..(byte_size(wire) - 1) do
        assert {:more, missing} = Value.decode(binary_part(wire, 0, prefix))
        assert missing == byte_size(wire) - prefix
      end

      limit = %{Limits.default() | max_value_bytes: byte_size(value.value) - 1}
      assert {:error, %Error{class: :limit_exceeded}} = Value.decode(wire, limit)
      assert {:error, %Error{class: :limit_exceeded}} = Value.encode(value, limit)
    end
  end

  test "char is a Unicode scalar in UTF-32BE and rejects surrogate/out-of-range words" do
    for char <- [0, 0xD7FF, 0xE000, 0x1F600, 0x10FFFF] do
      value = V.char(char)
      assert {:ok, ^value, <<>>} = Value.decode(<<0x73, char::32>>)
      assert {:ok, <<0x73, ^char::32>>} = Value.encode(value)
    end

    for char <- [0xD800, 0xDFFF, 0x110000, 0xFFFFFFFF] do
      assert {:error, %Error{class: :malformed, reason: :invalid_char}} =
               Value.decode(<<0x73, char::32>>)

      assert {:error, %Error{class: :invalid_value}} = Value.encode(%V{type: :char, value: char})
    end
  end

  # Produced independently by python-qpid-proton 0.40.0 Data.put_object/
  # Data.encode; numeric constructors in this fixture are not our encoder's
  # oracle. Producer invocation and version are requalified in task evidence.
  @proton_vectors [
    {"f00000000d00000002700000000000000100", V.array(:uint, [V.uint(0), V.uint(256)])},
    {"f00000000700000002560100", V.array(:boolean, [V.boolean(true), V.boolean(false)])},
    {"f00000001a00000002d0000000040000000000000009000000025207a30178",
     V.array(:list, [V.list([]), V.list([V.uint(7), V.symbol("x")])])},
    {"f00000001a00000002d100000004000000000000000900000002a3016b5208",
     V.array(:map, [V.map([]), V.map([{V.symbol("k"), V.uint(8)}])])},
    {"f00000002000000002f0000000090000000170000000070000000a00000001b30000000178",
     V.array(:array, [V.array(:uint, [V.uint(7)]), V.array(:symbol, [V.symbol("x")])])},
    {"f0000000160000000200a306783a747970657100000007ffffff7f",
     V.array({:described, V.symbol("x:type"), :int}, [
       V.described(V.symbol("x:type"), V.int(7)),
       V.described(V.symbol("x:type"), V.int(-129))
     ])}
  ]

  test "independent Proton arrays decode with homogeneous constructor identity, recursively" do
    for {hex, expected} <- @proton_vectors do
      wire = Base.decode16!(hex, case: :lower)
      assert {:ok, ^expected, <<0x40>>} = Value.decode(wire <> <<0x40>>)
      assert {:ok, encoded} = Value.encode(expected)
      assert {:ok, ^expected, <<>>} = Value.decode(encoded)
      assert {:ok, measured} = Value.encoded_size(expected)
      assert measured == byte_size(encoded)
    end
  end

  test "all scalar families and empty homogeneous arrays retain semantic element types" do
    values =
      [
        V.boolean(true),
        V.ubyte(255),
        V.ushort(65535),
        V.uint(256),
        V.ulong(256),
        V.byte(-128),
        V.short(-32768),
        V.int(-129),
        V.long(-129),
        V.char(0x1F600),
        V.timestamp(-1),
        V.uuid(<<1::128>>),
        V.binary(<<0, 255>>),
        V.string("λ"),
        V.symbol("x")
      ] ++
        Enum.map(@scalars, &elem(&1, 1))

    for value <- values, members <- [[], [value, value]] do
      array = V.array(value.type, members)
      assert {:ok, wire} = Value.encode(array)
      assert {:ok, ^array, <<>>} = Value.decode(wire)
    end
  end

  test "compact shared constructors and nested described constructor chains are legal" do
    for {wire, expected} <- [
          {<<0xE0, 2, 3, 0x43>>, V.array(:uint, List.duplicate(V.uint(0), 3))},
          {<<0xE0, 2, 2, 0x41>>, V.array(:boolean, [V.boolean(true), V.boolean(true)])},
          {<<0xE0, 2, 2, 0x45>>, V.array(:list, [V.list([]), V.list([])])},
          {<<0xE0, 8, 1, 0, 0x53, 1, 0, 0x53, 2, 0x43>>,
           V.array(
             {:described, V.ulong(1), {:described, V.ulong(2), :uint}},
             [V.described(V.ulong(1), V.described(V.ulong(2), V.uint(0)))]
           )}
        ] do
      assert {:ok, ^expected, <<>>} = Value.decode(wire)
      assert {:ok, encoded} = Value.encode(expected)
      assert {:ok, ^expected, <<>>} = Value.decode(encoded)
    end
  end

  test "array malformed bodies, null constructors, mismatched descriptors and duplicate keys fail explicitly" do
    for wire <- [
          <<0xE0, 2, 1, 0x40>>,
          <<0xE0, 3, 1, 0x56, 2>>,
          <<0xE0, 3, 0, 0x50, 1>>,
          <<0xE0, 3, 1, 0x70, 0>>,
          <<0xE0, 4, 1, 0, 0x43, 0x50>>
        ] do
      assert {:error, %Error{}} = Value.decode(wire)
    end

    forged = %V{
      type: :array,
      value: %V.Array{
        element_type: {:described, V.ulong(1), :uint},
        values: [V.described(V.ulong(2), V.uint(1))]
      }
    }

    assert {:error, %Error{class: :invalid_value}} = Value.encode(forged)
    duplicate = V.array(:map, [V.map([{V.uint(0), V.null()}, {V.uint(0), V.null()}])])
    assert {:error, %Error{reason: :duplicate_map_key}} = Value.encode(duplicate)
  end

  test "array item, nesting and byte budgets precede allocation and later invalid children" do
    limits = %{Limits.default() | max_compound_items: 2}

    assert {:error, %Error{reason: :compound_item_limit}} =
             Value.decode(<<0xF0, 5::32, 0xFFFFFFFF::32, 0x43>>, limits)

    value = %V{
      type: :array,
      value: %V.Array{element_type: :uint, values: [V.uint(1), V.uint(2), :invalid]}
    }

    assert {:error, %Error{reason: :compound_item_limit}} = Value.encode(value, limits)
    array = V.array(:array, [V.array(:uint, [V.uint(1)])])
    {:ok, wire} = Value.encode(array)
    limits = %{Limits.default() | max_nesting_depth: 1}
    assert {:error, %Error{reason: :nesting_depth_limit}} = Value.encode(array, limits)
    assert {:error, %Error{reason: :nesting_depth_limit}} = Value.decode(wire, limits)

    value = %V{
      type: :array,
      value: %V.Array{
        element_type: :binary,
        values: [V.binary("abc"), V.binary("abc"), V.binary(:binary.copy("x", 256)), :invalid]
      }
    }

    assert {:error, %Error{reason: :value_size_limit}} =
             Value.encode(value, %{Limits.default() | max_value_bytes: 16})
  end

  test "every strict prefix of independent and canonical recursive arrays remains incomplete" do
    for {hex, expected} <- @proton_vectors do
      independent = Base.decode16!(hex, case: :lower)
      {:ok, canonical} = Value.encode(expected)

      for wire <- [independent, canonical], prefix_size <- 0..(byte_size(wire) - 1) do
        assert {:more, missing} = Value.decode(binary_part(wire, 0, prefix_size))
        width = if :binary.first(wire) == 0xE0, do: 1, else: 4

        expected_missing =
          if prefix_size < width + 1,
            do: width + 1 - max(1, prefix_size),
            else: byte_size(wire) - prefix_size

        assert missing == if(prefix_size == 0, do: 1, else: expected_missing)
      end
    end
  end

  test "forced compound constructor overhead exhausts the parent before a later invalid element" do
    value = %V{
      type: :array,
      value: %V.Array{element_type: :list, values: [V.list([V.uint(0)]), V.list([]), :invalid]}
    }

    assert {:error, %Error{reason: :value_size_limit}} =
             Value.encode(value, %{Limits.default() | max_value_bytes: 18})

    described = V.array({:described, V.symbol("long-descriptor"), :uint}, [])

    assert {:error, %Error{reason: :value_size_limit}} =
             Value.encode(described, %{Limits.default() | max_value_bytes: 8})
  end

  test "oversized recursive array preflight stays within a finite process memory budget" do
    value = V.array(:list, List.duplicate(V.list([V.binary(:binary.copy("x", 262_144))]), 64))
    owner = self()

    {pid, monitor} =
      :erlang.spawn_opt(
        fn ->
          send(
            owner,
            {self(), Value.encode(value, %{Limits.default() | max_value_bytes: 262_144})}
          )
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
    assert_receive {^pid, {:error, %Error{reason: :value_size_limit}}}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000
  end
end
