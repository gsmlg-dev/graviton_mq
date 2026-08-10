defmodule GravitonMQ.AMQP10.Codec.ContractsTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits

  test "default limits bound codec resource use" do
    assert %Limits{
             max_frame_size: 16_777_216,
             max_value_bytes: 16_777_216,
             max_compound_items: 65_536,
             max_nesting_depth: 32
           } = Limits.default()
  end

  test "invalid limits return a structured value decode error" do
    limits = %{Limits.default() | max_nesting_depth: -1}

    assert {:error,
            %Error{
              operation: :value_decode,
              class: :invalid_value,
              reason: :invalid_limits,
              offset: nil,
              details: details
            } = error} = Limits.validate(limits, :value_decode)

    assert details == Map.from_struct(limits)
    refute is_exception(error)

    assert_raise ArgumentError, fn ->
      struct!(Error, class: :invalid_value, reason: :invalid_limits)
    end
  end

  test "value byte limits cannot exceed the AMQP uint32 length field" do
    limits = %{Limits.default() | max_value_bytes: 0x1_0000_0000}
    expected_details = Map.from_struct(limits)

    assert {:error,
            %Error{
              operation: :value_decode,
              class: :invalid_value,
              reason: :invalid_limits,
              offset: nil,
              details: ^expected_details
            }} = Limits.validate(limits, :value_decode)
  end

  test "compound item limits cannot exceed the AMQP uint32 count field" do
    limits = %{Limits.default() | max_compound_items: 0x1_0000_0000}
    expected_details = Map.from_struct(limits)

    assert {:error,
            %Error{
              operation: :value_encode,
              class: :invalid_value,
              reason: :invalid_limits,
              offset: nil,
              details: ^expected_details
            }} = Limits.validate(limits, :value_encode)
  end

  test "error construction rejects invalid offsets" do
    for offset <- [-1, :unknown] do
      assert_raise ArgumentError,
                   "expected :offset to be nil or a non-negative integer, got: #{inspect(offset)}",
                   fn ->
                     Error.new(:value_decode, :invalid_value, :invalid_offset, offset: offset)
                   end
    end
  end

  test "error construction rejects non-map details" do
    assert_raise ArgumentError, "expected :details to be a map, got: :unknown", fn ->
      Error.new(:value_decode, :invalid_value, :invalid_details,
        offset: 0,
        details: :unknown
      )
    end
  end

  test "error construction rejects unsupported operations" do
    assert_raise ArgumentError,
                 "expected :operation to be one of [:protocol_header, :frame_decode, :value_decode, :value_encode, :performative_decode, :performative_encode], got: :transfer_decode",
                 fn ->
                   Error.new(:transfer_decode, :invalid_value, :invalid_operation)
                 end
  end

  test "error construction rejects unsupported classes" do
    assert_raise ArgumentError,
                 "expected :class to be one of [:malformed, :unsupported, :limit_exceeded, :invalid_value], got: :internal_error",
                 fn ->
                   Error.new(:value_decode, :internal_error, :invalid_class)
                 end
  end

  test "error construction rejects non-atom reasons" do
    assert_raise ArgumentError, ~s(expected :reason to be an atom, got: "not_an_atom"), fn ->
      Error.new(:value_decode, :invalid_value, "not_an_atom")
    end
  end
end
