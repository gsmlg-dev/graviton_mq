defmodule GravitonMQ.AMQP10.Codec.PerformativeContractTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Performative
  alias GravitonMQ.AMQP10.Codec.Value
  alias GravitonMQ.AMQP10.Performative.Begin
  alias GravitonMQ.AMQP10.Performative.Open
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  test "Open names its ordered schema fields and retains exact AMQP values" do
    assert Code.ensure_loaded?(Open)

    fields = %{
      container_id: AMQPValue.string("node-a"),
      hostname: AMQPValue.string("broker.example"),
      max_frame_size: AMQPValue.uint(65_536),
      channel_max: AMQPValue.ushort(32),
      idle_time_out: AMQPValue.uint(30_000),
      outgoing_locales: AMQPValue.symbol("en-US"),
      incoming_locales: AMQPValue.array(:symbol, [AMQPValue.symbol("en-US")]),
      offered_capabilities: AMQPValue.symbol("example:offered"),
      desired_capabilities: AMQPValue.array(:symbol, [AMQPValue.symbol("example:desired")]),
      properties: AMQPValue.map([{AMQPValue.symbol("product"), AMQPValue.string("GravitonMQ")}])
    }

    module = Open
    open = struct(module, fields)

    assert %{__struct__: ^module} = open
    assert Map.from_struct(open) == fields
  end

  test "Begin names its ordered schema fields and retains exact AMQP values" do
    assert Code.ensure_loaded?(Begin)

    fields = %{
      remote_channel: AMQPValue.ushort(7),
      next_outgoing_id: AMQPValue.uint(1),
      incoming_window: AMQPValue.uint(100),
      outgoing_window: AMQPValue.uint(200),
      handle_max: AMQPValue.uint(4_294_967_295),
      offered_capabilities: AMQPValue.symbol("example:offered"),
      desired_capabilities: AMQPValue.array(:symbol, [AMQPValue.symbol("example:desired")]),
      properties: AMQPValue.map([{AMQPValue.symbol("product"), AMQPValue.string("GravitonMQ")}])
    }

    module = Begin
    begin_performative = struct(module, fields)

    assert %{__struct__: ^module} = begin_performative
    assert Map.from_struct(begin_performative) == fields
  end

  test "structured errors identify the performative schema boundary" do
    for operation <- [:performative_decode, :performative_encode] do
      assert %Error{
               operation: ^operation,
               class: :invalid_value,
               reason: :invalid_performative,
               offset: nil,
               details: %{}
             } = Error.new(operation, :invalid_value, :invalid_performative)
    end

    assert {:error,
            %Error{
              operation: :performative_encode,
              class: :invalid_value,
              reason: :invalid_performative
            }} = Performative.encode(%{})
  end

  test "value-codec malformed, limit, and unsupported errors propagate unchanged" do
    invalid_utf8_open = <<0x00, 0x53, 0x10, 0xC0, 4, 1, 0xA1, 1, 0xFF>>

    assert {:error,
            %Error{
              operation: :value_decode,
              class: :malformed,
              reason: :invalid_utf8
            }} = Performative.decode(invalid_utf8_open)

    begin_value =
      AMQPValue.described(
        AMQPValue.ulong(0x11),
        AMQPValue.list([
          AMQPValue.null(),
          AMQPValue.uint(0),
          AMQPValue.uint(100),
          AMQPValue.uint(100)
        ])
      )

    {:ok, encoded_begin} = Value.encode(begin_value)
    limits = %{Limits.default() | max_compound_items: 3}

    assert {:error,
            %Error{
              operation: :value_decode,
              class: :limit_exceeded,
              reason: :compound_item_limit
            }} = Performative.decode(encoded_begin, limits)

    open = %Open{
      container_id: AMQPValue.string("c"),
      properties: AMQPValue.map([{AMQPValue.symbol("flag"), AMQPValue.boolean(true)}])
    }

    assert {:error,
            %Error{
              operation: :value_encode,
              class: :unsupported,
              reason: :semantic_type,
              details: %{type: :boolean}
            }} = Performative.encode(open)
  end
end
