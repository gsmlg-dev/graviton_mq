defmodule GravitonMQ.AMQP10.Codec.TerminusTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.{Error, Limits, Terminus, Value}
  alias GravitonMQ.AMQP10.Terminus.{Source, Target}
  alias GravitonMQ.AMQP10.Value, as: V

  setup do
    assert Code.ensure_loaded?(Terminus)
    :ok
  end

  test "minimal numeric and symbolic Source/Target materialize exact defaults" do
    for {module, code, symbol} <- [
          {Source, 0x28, "amqp:source:list"},
          {Target, 0x29, "amqp:target:list"}
        ],
        wire <- [<<0, 0x53, code, 0x45>>, <<0, 0xA3, byte_size(symbol), symbol::binary, 0x45>>] do
      assert {:ok, terminus, <<>>} = Terminus.decode(wire)
      assert terminus.__struct__ == module
      assert terminus.durable == V.uint(0)
      assert terminus.expiry_policy == V.symbol("session-end")
      assert terminus.timeout == V.uint(0)
      assert terminus.dynamic == V.boolean(false)
      assert {:ok, <<0, 0x53, ^code, 0x45>>} = Terminus.encode(terminus)
    end
  end

  test "literal addresses preserve interior holes before final capabilities" do
    source_wire =
      <<0, 0x53, 0x28, 0xC0, 15, 11, 0xA1, 1, "q", 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,
        0x40, 0xA3, 0>>

    target_wire =
      <<0, 0x53, 0x29, 0xC0, 11, 7, 0xA1, 1, "q", 0x40, 0x40, 0x40, 0x40, 0x40, 0xA3, 0>>

    for {module, wire} <- [{Source, source_wire}, {Target, target_wire}] do
      assert {:ok, terminus, <<>>} = Terminus.decode(wire)
      assert terminus.address == V.string("q")
      assert terminus.capabilities == V.symbol("")
      assert terminus.__struct__ == module
      assert {:ok, ^wire} = Terminus.encode(terminus)
    end
  end

  test "strict prefixes are incomplete and the suffix remains exact" do
    for code <- [0x28, 0x29] do
      wire = <<0, 0x53, code, 0xC0, 4, 1, 0xA1, 1, "q">>

      for size <- 0..(byte_size(wire) - 1) do
        assert {:more, needed} = Terminus.decode(binary_part(wire, 0, size))
        assert needed > 0
      end

      assert {:ok, _, <<0xDE, 0xAD>>} = Terminus.decode(wire <> <<0xDE, 0xAD>>)
    end
  end

  test "null fields normalize and false or zero defaults are omitted canonically" do
    for {module, code, count} <- [{Source, 0x28, 11}, {Target, 0x29, 7}] do
      assert {:ok, terminus, <<>>} = Terminus.decode(wire(code, List.duplicate(V.null(), count)))
      assert {:ok, <<0, 0x53, ^code, 0x45>>} = Terminus.encode(terminus)
      assert {:ok, <<0, 0x53, ^code, 0x45>>} = Terminus.encode(struct(module))
    end
  end

  test "retains full Source schema including filters, outcomes and described extensions" do
    filter = V.described(V.symbol("example:filter"), V.list([V.boolean(true)]))
    outcome = V.described(V.symbol("example:outcome"), V.map([{V.null(), V.uint(1)}]))
    properties = V.map([{V.symbol("x"), V.list([V.boolean(false)])}])

    fields = [
      V.string("q"),
      V.uint(2),
      V.symbol("never"),
      V.uint(100),
      V.boolean(true),
      properties,
      V.symbol("copy"),
      V.map([{V.symbol("f"), filter}, {V.symbol("none"), V.null()}]),
      outcome,
      V.array(:symbol, [V.symbol("amqp:accepted:list"), V.symbol("example:outcome")]),
      V.array(:symbol, [V.symbol("cap")])
    ]

    assert {:ok, source, <<>>} = Terminus.decode(wire(0x28, fields))
    assert source.filter == Enum.at(fields, 7)
    assert source.default_outcome == outcome
    assert source.outcomes == Enum.at(fields, 9)
    assert source.dynamic_node_properties == properties
    assert {:ok, encoded} = Terminus.encode(source)
    assert {:ok, ^source, <<>>} = Terminus.decode(encoded)
    assert {:ok, tagged} = Terminus.encode_value(source, :performative_encode, :invalid_value)
    assert {:ok, ^source} = Terminus.decode_value(tagged, :performative_decode, :malformed)
  end

  test "checks every Source and Target field type on encode and decode" do
    for {module, code, names} <- [
          {Source, 0x28,
           [
             :address,
             :durable,
             :expiry_policy,
             :timeout,
             :dynamic,
             :dynamic_node_properties,
             :distribution_mode,
             :filter,
             :default_outcome,
             :outcomes,
             :capabilities
           ]},
          {Target, 0x29,
           [
             :address,
             :durable,
             :expiry_policy,
             :timeout,
             :dynamic,
             :dynamic_node_properties,
             :capabilities
           ]}
        ],
        {field, index} <- Enum.with_index(names) do
      fields = List.duplicate(V.null(), length(names)) |> List.replace_at(index, V.int(1))
      assert_error(Terminus.decode(wire(code, fields)), :field_type_mismatch, field)

      assert_error(
        Terminus.encode(struct(module, [{field, V.int(1)}])),
        :field_type_mismatch,
        field
      )
    end
  end

  test "rejects excess fields, non-list bodies and invalid descriptor forms" do
    for {code, count} <- [{0x28, 11}, {0x29, 7}] do
      assert_error(
        Terminus.decode(wire(code, List.duplicate(V.null(), count + 1))),
        :too_many_fields
      )

      assert_error(Terminus.decode(<<0, 0x53, code, 0x40>>), :invalid_terminus_body)
    end

    assert_error(Terminus.decode(<<0, 0x53, 0x30, 0x45>>), :transactions_unsupported)

    assert_error(
      Terminus.decode(<<0, 0xA3, 21, "amqp:coordinator:list", 0x45>>),
      :transactions_unsupported
    )

    assert_error(Terminus.decode(<<0, 0x53, 0x7F, 0x45>>), :unknown_terminus)
  end

  test "validates durable and expiry enumerations and standard distribution modes" do
    for module <- [Source, Target] do
      for value <- 0..2 do
        assert {:ok, _} = Terminus.encode(struct(module, durable: V.uint(value)))
      end

      assert_error(
        Terminus.encode(struct(module, durable: V.uint(3))),
        :field_value_out_of_range,
        :durable
      )

      for policy <- ["link-detach", "session-end", "connection-close", "never"] do
        assert {:ok, _} = Terminus.encode(struct(module, expiry_policy: V.symbol(policy)))
      end

      assert_error(
        Terminus.encode(struct(module, expiry_policy: V.symbol("invalid"))),
        :field_value_out_of_range,
        :expiry_policy
      )
    end

    for mode <- ["copy", "move"] do
      assert {:ok, _} = Terminus.encode(struct(Source, distribution_mode: V.symbol(mode)))
    end
  end

  test "node properties require dynamic true, symbol keys and valid standard values" do
    for module <- [Source, Target] do
      properties = V.map([{V.symbol("x"), V.boolean(true)}])

      assert_error(
        Terminus.encode(struct(module, dynamic_node_properties: properties)),
        :dynamic_node_properties_without_dynamic,
        :dynamic_node_properties
      )

      assert {:ok, _} =
               Terminus.encode(
                 struct(module,
                   dynamic: V.boolean(true),
                   dynamic_node_properties: properties
                 )
               )

      for key <- [V.null(), V.string("x")] do
        assert_error(
          Terminus.encode(
            struct(module,
              dynamic: V.boolean(true),
              dynamic_node_properties: V.map([{key, V.boolean(true)}])
            )
          ),
          :invalid_property_key
        )
      end

      assert_error(
        Terminus.encode(
          struct(module,
            dynamic: V.boolean(true),
            dynamic_node_properties: V.map([{V.symbol("lifetime-policy"), V.uint(1)}])
          )
        ),
        :invalid_node_property
      )

      assert_error(
        Terminus.encode(
          struct(module,
            dynamic: V.boolean(true),
            dynamic_node_properties: V.map([{V.symbol("supported-dist-modes"), V.boolean(true)}])
          )
        ),
        :invalid_node_property
      )
    end
  end

  @tag :extension_distribution
  test "custom distribution mode symbols are retained without claiming endpoint support" do
    # Messaging §3.5.2 permits distribution modes beyond the two standard ones.
    custom = V.symbol("example:mode")

    bytes =
      <<0, 0x53, 0x28, 0xC0, 21, 7, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0xA3, 12, "example:mode">>

    assert {:ok, source, <<>>} = Terminus.decode(bytes)
    assert source.distribution_mode == custom
    assert {:ok, ^bytes} = Terminus.encode(source)
  end

  @tag :extension_distribution
  test "supported-dist-modes includes custom scalar and multiple symbol forms" do
    for modes <- [
          V.symbol("example:mode"),
          V.array(:symbol, [V.symbol("copy"), V.symbol("example:mode")])
        ],
        module <- [Source, Target] do
      properties = V.map([{V.symbol("supported-dist-modes"), modes}])
      terminus = struct(module, dynamic: V.boolean(true), dynamic_node_properties: properties)
      assert {:ok, bytes} = Terminus.encode(terminus)
      assert {:ok, decoded, <<>>} = Terminus.decode(bytes)
      assert decoded.dynamic_node_properties == properties
    end
  end

  test "supported-dist-modes requires at least one symbol" do
    properties = V.map([{V.symbol("supported-dist-modes"), V.array(:symbol, [])}])

    for module <- [Source, Target] do
      terminus = struct(module, dynamic: V.boolean(true), dynamic_node_properties: properties)
      assert_error(Terminus.encode(terminus), :invalid_node_property)
    end

    fields = List.duplicate(V.null(), 4) ++ [V.boolean(true), properties]
    assert_error(Terminus.decode(wire(0x28, fields)), :invalid_node_property)
  end

  test "filters accept described extension or null values only and forbid non-symbol keys" do
    for {key, value, reason} <- [
          {V.null(), V.null(), :invalid_property_key},
          {V.string("f"), V.null(), :invalid_property_key},
          {V.symbol("f"), V.boolean(true), :invalid_filter_value}
        ] do
      assert_error(Terminus.encode(struct(Source, filter: V.map([{key, value}]))), reason)
      fields = List.duplicate(V.null(), 7) ++ [V.map([{key, value}])]
      assert_error(Terminus.decode(wire(0x28, fields)), reason)
    end
  end

  test "default outcome accepts standard outcomes and rejects Received and transactions" do
    for code <- [0x24, 0x25, 0x26, 0x27] do
      outcome = V.described(V.ulong(code), V.list([]))
      source = struct(Source, default_outcome: outcome)
      assert {:ok, encoded} = Terminus.encode(source)
      assert {:ok, decoded, <<>>} = Terminus.decode(encoded)
      assert decoded.default_outcome == outcome
    end

    received = V.described(V.ulong(0x23), V.list([V.uint(0), V.ulong(0)]))
    assert_error(Terminus.encode(struct(Source, default_outcome: received)), :invalid_outcome)
    transactional = V.described(V.ulong(0x34), V.list([]))

    assert_error(
      Terminus.encode(struct(Source, default_outcome: transactional)),
      :transactions_unsupported
    )

    assert_error(
      Terminus.encode(
        struct(Source, default_outcome: V.described(V.ulong(0x24), V.list([V.uint(0)])))
      ),
      :too_many_delivery_state_fields
    )

    assert_error(
      Terminus.encode(struct(Source, default_outcome: V.described(V.ulong(0x28), V.list([])))),
      :invalid_outcome
    )

    assert_error(
      Terminus.encode(struct(Source, outcomes: V.symbol("amqp:received:list"))),
      :invalid_outcome
    )
  end

  @tag :canonical_nested
  test "decoding retains a symbolic default outcome and encoding uses its numeric descriptor" do
    symbolic = V.described(V.symbol("amqp:released:list"), V.list([]))
    fields = List.duplicate(V.null(), 8) ++ [symbolic]
    assert {:ok, source, <<>>} = Terminus.decode(wire(0x28, fields))
    assert source.default_outcome == symbolic
    assert {:ok, encoded} = Terminus.encode(source)
    assert {:ok, decoded, <<>>} = Terminus.decode(encoded)
    assert decoded.default_outcome == V.described(V.ulong(0x26), V.list([]))
  end

  test "helper inputs fail explicitly and shared byte/count/depth limits propagate" do
    assert_error(
      Terminus.encode_value(%{}, :performative_encode, :invalid_value),
      :invalid_terminus
    )

    assert_error(
      Terminus.decode_value(V.string("q"), :performative_decode, :malformed),
      :invalid_terminus
    )

    source = struct(Source, address: V.string("queue"), capabilities: V.symbol("cap"))
    assert {:ok, wire} = Terminus.encode(source)

    for limits <- [
          %{Limits.default() | max_value_bytes: 3},
          %{Limits.default() | max_compound_items: 1},
          %{Limits.default() | max_nesting_depth: 1}
        ] do
      assert {:error, %Error{class: :limit_exceeded}} = Terminus.encode(source, limits)
      assert {:error, %Error{class: :limit_exceeded}} = Terminus.decode(wire, limits)
    end
  end

  defp wire(code, fields) do
    {:ok, wire} = Value.encode(V.described(V.ulong(code), V.list(fields)))
    wire
  end

  defp assert_error(result, reason, field \\ nil) do
    assert {:error, %Error{reason: ^reason} = error} = result
    if field, do: assert(error.details.field == field)
    error
  end
end
