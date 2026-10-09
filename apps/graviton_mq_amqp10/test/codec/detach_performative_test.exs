defmodule GravitonMQ.AMQP10.Codec.DetachPerformativeTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Performative
  alias GravitonMQ.AMQP10.Codec.Value
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  @detach GravitonMQ.AMQP10.Performative.Detach
  @minimal <<0x00, 0x53, 0x16, 0xC0, 2, 1, 0x43>>
  @closed_true <<0x00, 0x53, 0x16, 0xC0, 3, 2, 0x43, 0x41>>
  @error <<0x00, 0x53, 0x16, 0xC0, 12, 3, 0x43, 0x40, 0x00, 0x53, 0x1D, 0xC0, 4, 1, 0xA3, 1, "x">>

  test "Detach is immutable unset data with no process or transition API" do
    assert Code.ensure_loaded?(@detach)
    assert Map.from_struct(struct(@detach)) == %{handle: nil, closed: nil, error: nil}

    for {function, arity} <- [start_link: 1, child_spec: 1, transition: 2] do
      refute function_exported?(@detach, function, arity)
    end
  end

  test "facade decodes independently assembled minimal Detach" do
    assert {:ok, %{__struct__: @detach, handle: handle, closed: closed, error: nil}, <<>>} =
             Performative.decode(@minimal)

    assert handle == AMQPValue.uint(0)
    assert closed == AMQPValue.boolean(false)
  end

  test "numeric and symbolic descriptors and list widths canonicalize identically" do
    expected = detach(handle: AMQPValue.uint(0), closed: AMQPValue.boolean(false))

    for descriptor <- [
          <<0x53, 0x16>>,
          <<0x80, 0x16::64>>,
          <<0xA3, 16, "amqp:detach:list">>,
          <<0xB3, 16::32, "amqp:detach:list">>
        ],
        body <- [<<0xC0, 2, 1, 0x43>>, <<0xD0, 5::32, 1::32, 0x43>>] do
      fixture = <<0x00, descriptor::binary, body::binary>>
      assert {:ok, ^expected, <<>>} = Performative.decode(fixture)
      assert {:ok, @minimal} = Performative.encode(expected)
    end
  end

  test "all closed Boolean constructors and null normalize to exact tagged defaults" do
    for {wire, flag, canonical} <- [
          {<<0x41>>, true, @closed_true},
          {<<0x42>>, false, @minimal},
          {<<0x56, 1>>, true, @closed_true},
          {<<0x56, 0>>, false, @minimal},
          {<<0x40>>, false, @minimal}
        ] do
      expected = detach(handle: AMQPValue.uint(0), closed: AMQPValue.boolean(flag))
      assert {:ok, ^expected, <<>>} = Performative.decode(wrap(<<0x43, wire::binary>>, 2))
      assert {:ok, ^canonical} = Performative.encode(expected)
    end
  end

  test "encoder accepts unset and explicitly null closed and removes trailing null fields" do
    for closed <- [nil, AMQPValue.null(), AMQPValue.boolean(false)] do
      assert {:ok, @minimal} =
               Performative.encode(detach(handle: AMQPValue.uint(0), closed: closed))
    end

    assert {:ok, decoded, <<>>} = Performative.decode(wrap(<<0x43, 0x40, 0x40>>, 3))
    assert {:ok, @minimal} = Performative.encode(decoded)
  end

  test "full uint range remains independent of negotiated handle limits" do
    for {wire, integer} <- [
          {<<0x43>>, 0},
          {<<0x52, 255>>, 255},
          {<<0x70, 256::32>>, 256},
          {<<0x70, 0xFFFFFFFF::32>>, 0xFFFFFFFF}
        ] do
      expected = detach(handle: AMQPValue.uint(integer), closed: AMQPValue.boolean(false))
      fixture = wrap(wire, 1)
      assert {:ok, ^expected, <<>>} = Performative.decode(fixture)
      assert {:ok, ^fixture} = Performative.encode(expected)
    end
  end

  test "handle is mandatory on decode and encode" do
    details = %{performative: :detach, field: :handle, index: 0}

    for fixture <- [<<0x00, 0x53, 0x16, 0x45>>, wrap(<<0x40>>, 1)] do
      assert_schema(
        Performative.decode(fixture),
        :performative_decode,
        :malformed,
        :mandatory_field_missing,
        details
      )
    end

    for handle <- [nil, AMQPValue.null()] do
      assert_schema(
        Performative.encode(detach(handle: handle)),
        :performative_encode,
        :invalid_value,
        :mandatory_field_missing,
        details
      )
    end
  end

  test "handle and closed retain exact tagged types in both directions" do
    for {field, index, expected, wire, value} <- [
          {:handle, 0, :uint, <<0x60, 0::16>>, AMQPValue.ushort(0)},
          {:handle, 0, :uint, <<0x44>>, AMQPValue.ulong(0)},
          {:closed, 1, :boolean, <<0x43>>, AMQPValue.uint(0)},
          {:closed, 1, :boolean, <<0xA3, 1, "x">>, AMQPValue.symbol("x")}
        ] do
      fields = if index == 0, do: wire, else: <<0x43, wire::binary>>

      details = %{
        performative: :detach,
        field: field,
        index: index,
        expected: expected,
        actual: value.type
      }

      assert_schema(
        Performative.decode(wrap(fields, index + 1)),
        :performative_decode,
        :malformed,
        :field_type_mismatch,
        details
      )

      assert_schema(
        Performative.encode(detach(Keyword.put([handle: AMQPValue.uint(0)], field, value))),
        :performative_encode,
        :invalid_value,
        :field_type_mismatch,
        details
      )
    end

    for {field, index, type} <- [{:handle, 0, :uint}, {:closed, 1, :boolean}] do
      assert_schema(
        Performative.encode(detach([{:handle, AMQPValue.uint(0)}, {field, 0}])),
        :performative_encode,
        :invalid_value,
        :field_type_mismatch,
        %{performative: :detach, field: field, index: index, expected: type, actual: :invalid}
      )
    end
  end

  test "wrong body and excess fields identify Detach" do
    assert_schema(
      Performative.decode(<<0x00, 0x53, 0x16, 0x40>>),
      :performative_decode,
      :malformed,
      :invalid_performative_body,
      %{performative: :detach, expected: :list, actual: :null}
    )

    assert_schema(
      Performative.decode(wrap(<<0x43, 0x40, 0x40, 0x40>>, 4)),
      :performative_decode,
      :malformed,
      :too_many_fields,
      %{performative: :detach, maximum: 3, actual: 4}
    )
  end

  test "nested numeric and symbolic Errors preserve the closed positional hole" do
    error = %ProtocolError{condition: AMQPValue.symbol("x")}
    expected = detach(handle: AMQPValue.uint(0), closed: AMQPValue.boolean(false), error: error)

    for descriptor <- [<<0x53, 0x1D>>, <<0x80, 0x1D::64>>, <<0xA3, 15, "amqp:error:list">>] do
      fixture = with_error(error_wire(<<0xA3, 1, "x">>, 1, descriptor))
      assert {:ok, ^expected, <<>>} = Performative.decode(fixture)
      assert {:ok, @error} = Performative.encode(expected)
    end

    assert {:ok, @error} = Performative.encode(detach(handle: AMQPValue.uint(0), error: error))
  end

  test "full nested Error retains ordered Boolean info and extension condition" do
    fields = <<0xA3, 1, "x", 0xA1, 1, "d", 0xC1, 9, 4, 0xA3, 1, "a", 0x41, 0xA3, 1, "b", 0x42>>
    fixture = with_error(error_wire(fields, 3), <<0x41>>)

    error = %ProtocolError{
      condition: AMQPValue.symbol("x"),
      description: AMQPValue.string("d"),
      info:
        AMQPValue.map([
          {AMQPValue.symbol("a"), AMQPValue.boolean(true)},
          {AMQPValue.symbol("b"), AMQPValue.boolean(false)}
        ])
    }

    expected = detach(handle: AMQPValue.uint(0), closed: AMQPValue.boolean(true), error: error)
    assert {:ok, ^expected, <<>>} = Performative.decode(fixture)
    assert {:ok, ^fixture} = Performative.encode(expected)
  end

  test "all Boolean forms in Error info canonicalize and retain exact suffixes" do
    suffix = <<0xDE, 0xAD>>

    for {wire, flag, canonical} <- [
          {<<0x41>>, true, <<0x41>>},
          {<<0x42>>, false, <<0x42>>},
          {<<0x56, 0>>, false, <<0x42>>},
          {<<0x56, 1>>, true, <<0x41>>}
        ] do
      fields = <<0xA3, 1, "x", 0x40, 0xC1, 4 + byte_size(wire), 2, 0xA3, 1, "k", wire::binary>>
      canonical_fields = <<0xA3, 1, "x", 0x40, 0xC1, 5, 2, 0xA3, 1, "k", canonical::binary>>
      fixture = with_error(error_wire(fields, 3))
      canonical_fixture = with_error(error_wire(canonical_fields, 3))

      error = %ProtocolError{
        condition: AMQPValue.symbol("x"),
        info: AMQPValue.map([{AMQPValue.symbol("k"), AMQPValue.boolean(flag)}])
      }

      expected = detach(handle: AMQPValue.uint(0), closed: AMQPValue.boolean(false), error: error)
      assert {:ok, ^expected, ^suffix} = Performative.decode(fixture <> suffix)
      assert {:ok, ^canonical_fixture} = Performative.encode(expected)
    end
  end

  test "Error info preserves its own description hole" do
    fields = <<0xA3, 1, "x", 0x40, 0xC1, 5, 2, 0xA3, 1, "k", 0x41>>
    fixture = with_error(error_wire(fields, 3))

    error = %ProtocolError{
      condition: AMQPValue.symbol("x"),
      info: AMQPValue.map([{AMQPValue.symbol("k"), AMQPValue.boolean(true)}])
    }

    assert {:ok, ^fixture} = Performative.encode(detach(handle: AMQPValue.uint(0), error: error))
    assert {:ok, %{error: ^error}, <<>>} = Performative.decode(fixture)
  end

  test "outer Error mismatch descriptor and body diagnostics use field index two" do
    for {wire, reason, extra} <- [
          {<<0x43>>, :field_type_mismatch, %{expected: :error, actual: :uint}},
          {<<0x00, 0x53, 0x7F, 0x45>>, :invalid_error_descriptor,
           %{expected: :error, actual: :described, descriptor: AMQPValue.ulong(0x7F)}},
          {<<0x00, 0x53, 0x1D, 0x40>>, :invalid_error_body,
           %{expected: :list, actual: :null, nested_schema: :error}}
        ] do
      details = Map.merge(%{performative: :detach, field: :error, index: 2}, extra)

      assert_schema(
        Performative.decode(with_error(wire)),
        :performative_decode,
        :malformed,
        reason,
        details
      )
    end

    assert_schema(
      Performative.encode(detach(handle: AMQPValue.uint(0), error: AMQPValue.uint(0))),
      :performative_encode,
      :invalid_value,
      :field_type_mismatch,
      %{performative: :detach, field: :error, index: 2, expected: :error, actual: :uint}
    )
  end

  test "nested Error internal indexes and symbol map keys remain unchanged" do
    for {fields, count, error, reason, extra} <- [
          {<<>>, 0, %ProtocolError{condition: nil}, :mandatory_field_missing,
           %{field: :condition, index: 0}},
          {<<0x40>>, 1, %ProtocolError{condition: nil}, :mandatory_field_missing,
           %{field: :condition, index: 0}},
          {<<0xA1, 1, "x">>, 1, %ProtocolError{condition: AMQPValue.string("x")},
           :field_type_mismatch,
           %{field: :condition, index: 0, expected: :symbol, actual: :string}},
          {<<0xA3, 1, "x", 0x43>>, 2,
           %ProtocolError{condition: AMQPValue.symbol("x"), description: AMQPValue.uint(0)},
           :field_type_mismatch,
           %{field: :description, index: 1, expected: :string, actual: :uint}},
          {<<0xA3, 1, "x", 0x40, 0x43>>, 3,
           %ProtocolError{condition: AMQPValue.symbol("x"), info: AMQPValue.uint(0)},
           :field_type_mismatch, %{field: :info, index: 2, expected: :map, actual: :uint}},
          {<<0xA3, 1, "x", 0x40, 0xC1, 5, 2, 0xA1, 1, "k", 0x41>>, 3,
           %ProtocolError{
             condition: AMQPValue.symbol("x"),
             info: AMQPValue.map([{AMQPValue.string("k"), AMQPValue.boolean(true)}])
           }, :invalid_property_key, %{field: :info, index: 2}}
        ] do
      details = Map.merge(%{performative: :detach, nested_schema: :error}, extra)

      assert_schema(
        Performative.decode(with_error(error_wire(fields, count))),
        :performative_decode,
        :malformed,
        reason,
        details
      )

      assert_schema(
        Performative.encode(detach(handle: AMQPValue.uint(0), error: error)),
        :performative_encode,
        :invalid_value,
        reason,
        details
      )
    end

    assert_schema(
      Performative.decode(with_error(error_wire(<<0xA3, 1, "x", 0x40, 0x40, 0x40>>, 4))),
      :performative_decode,
      :malformed,
      :too_many_fields,
      %{performative: :detach, nested_schema: :error, actual: 4, maximum: 3}
    )
  end

  test "top-level Error remains unsupported" do
    assert {:error,
            %Error{
              operation: :performative_decode,
              class: :unsupported,
              reason: :unknown_descriptor
            }} =
             Performative.decode(error_wire(<<0xA3, 1, "x">>, 1))
  end

  test "strict prefixes are incomplete and suffixes are byte exact" do
    fixtures = [
      @minimal,
      @closed_true,
      @error,
      with_error(
        error_wire(
          <<0xA3, 1, "x", 0xA1, 1, "d", 0xC1, 10, 4, 0xA3, 1, "a", 0x56, 1, 0xA3, 1, "b", 0x42>>,
          3
        )
      ),
      <<0x00, 0x80, 0x16::64, 0xD0, 6::32, 2::32, 0x43, 0x41>>,
      <<0x00, 0xA3, 16, "amqp:detach:list", 0xC0, 2, 1, 0x43>>,
      with_error(error_wire(<<0xA3, 1, "x">>, 1, <<0xA3, 15, "amqp:error:list">>))
    ]

    suffix = <<0xFE, 0xED, 0x00, 0x56, 2>>

    for fixture <- fixtures do
      assert {:ok, %{__struct__: @detach}, ^suffix} = Performative.decode(fixture <> suffix)

      for length <- 0..(byte_size(fixture) - 1) do
        assert {:more, needed} = Performative.decode(binary_part(fixture, 0, length))
        assert needed > 0
      end
    end
  end

  test "malformed Boolean and unsupported primitive errors propagate unchanged" do
    for fields <- [<<0x43, 0x56, 2>>, <<0x50, 1>>] do
      fixture = wrap(fields, if(byte_size(fields) == 3, do: 2, else: 1))
      assert {:error, %Error{operation: :value_decode}} = expected = Value.decode(fixture)
      assert Performative.decode(fixture) == expected
    end

    assert {:error, %Error{class: :malformed, reason: :compound_item_truncated}} =
             Performative.decode(wrap(<<0x43, 0x56>>, 2) <> <<1>>)
  end

  test "malformed tagged uint and Boolean payloads retain value encoder errors" do
    for {field, type, payload} <- [
          {:handle, :uint, -1},
          {:handle, :uint, 0x100000000},
          {:handle, :uint, "0"},
          {:closed, :boolean, 1},
          {:closed, :boolean, :yes}
        ] do
      value = %AMQPValue{type: type, value: payload}
      fields = if field == :handle, do: [value], else: [AMQPValue.uint(0), value]
      semantic = AMQPValue.described(AMQPValue.ulong(0x16), AMQPValue.list(fields))
      expected = Value.encode(semantic)

      assert {:error,
              %Error{
                operation: :value_encode,
                class: :invalid_value,
                reason: :invalid_semantic_value
              }} = expected

      assert Performative.encode(detach([{:handle, AMQPValue.uint(0)}, {field, value}])) ==
               expected
    end
  end

  test "unsupported and malformed Error info values retain value layer errors" do
    for {wire, value} <- [
          {<<0x50, 1>>, AMQPValue.ubyte(1)},
          {<<0x56, 2>>, %AMQPValue{type: :boolean, value: 2}}
        ] do
      fields = <<0xA3, 1, "x", 0x40, 0xC1, 4 + byte_size(wire), 2, 0xA3, 1, "k", wire::binary>>
      fixture = with_error(error_wire(fields, 3))
      assert {:error, %Error{operation: :value_decode}} = expected = Value.decode(fixture)
      assert Performative.decode(fixture) == expected
      info = AMQPValue.map([{AMQPValue.symbol("k"), value}])
      error = %ProtocolError{condition: AMQPValue.symbol("x"), info: info}

      semantic_error =
        AMQPValue.described(
          AMQPValue.ulong(0x1D),
          AMQPValue.list([error.condition, AMQPValue.null(), info])
        )

      semantic =
        AMQPValue.described(
          AMQPValue.ulong(0x16),
          AMQPValue.list([AMQPValue.uint(0), AMQPValue.null(), semantic_error])
        )

      assert {:error, %Error{operation: :value_encode}} = expected = Value.encode(semantic)
      assert Performative.encode(detach(handle: AMQPValue.uint(0), error: error)) == expected
    end
  end

  test "size count and nesting limits apply in both directions" do
    expected =
      detach(
        handle: AMQPValue.uint(0),
        closed: AMQPValue.boolean(false),
        error: %ProtocolError{condition: AMQPValue.symbol("x")}
      )

    for {limits, reason} <- [
          {%{Limits.default() | max_value_bytes: 3}, :value_size_limit},
          {%{Limits.default() | max_compound_items: 2}, :compound_item_limit},
          {%{Limits.default() | max_nesting_depth: 1}, :nesting_depth_limit}
        ] do
      for {result, operation} <- [
            {Performative.decode(@error, limits), :value_decode},
            {Performative.encode(expected, limits), :value_encode}
          ] do
        assert {:error, %Error{operation: ^operation, class: :limit_exceeded, reason: ^reason}} =
                 result
      end
    end
  end

  test "item limits independently bound the nested Error info map" do
    fields = <<0xA3, 1, "x", 0xA1, 1, "d", 0xC1, 9, 4, 0xA3, 1, "a", 0x41, 0xA3, 1, "b", 0x42>>
    fixture = with_error(error_wire(fields, 3))

    error = %ProtocolError{
      condition: AMQPValue.symbol("x"),
      description: AMQPValue.string("d"),
      info:
        AMQPValue.map([
          {AMQPValue.symbol("a"), AMQPValue.boolean(true)},
          {AMQPValue.symbol("b"), AMQPValue.boolean(false)}
        ])
    }

    limits = %{Limits.default() | max_compound_items: 3}

    for {result, operation} <- [
          {Performative.decode(fixture, limits), :value_decode},
          {Performative.encode(detach(handle: AMQPValue.uint(0), error: error), limits),
           :value_encode}
        ] do
      assert {:error,
              %Error{operation: ^operation, class: :limit_exceeded, reason: :compound_item_limit}} =
               result
    end
  end

  defp detach(fields), do: struct(@detach, fields)

  defp wrap(fields, count),
    do: <<0x00, 0x53, 0x16, 0xC0, byte_size(fields) + 1, count, fields::binary>>

  defp error_wire(fields, count, descriptor \\ <<0x53, 0x1D>>),
    do: <<0x00, descriptor::binary, 0xC0, byte_size(fields) + 1, count, fields::binary>>

  defp with_error(error, closed \\ <<0x40>>), do: wrap(<<0x43, closed::binary, error::binary>>, 3)

  defp assert_schema(result, operation, class, reason, details) do
    assert {:error,
            %Error{operation: ^operation, class: ^class, reason: ^reason, details: ^details}} =
             result
  end
end
