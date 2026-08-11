defmodule GravitonMQ.AMQP10.Codec.EndClosePerformativeTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Performative
  alias GravitonMQ.AMQP10.Performative.Close
  alias GravitonMQ.AMQP10.Performative.End

  @empty_end <<0x00, 0x53, 0x17, 0x45>>
  @symbolic_end <<0x00, 0xA3, 13, "amqp:end:list", 0x45>>
  @null_end <<0x00, 0x53, 0x17, 0xC0, 2, 1, 0x40>>
  @empty_close <<0x00, 0x53, 0x18, 0x45>>
  @symbolic_close <<0x00, 0xA3, 15, "amqp:close:list", 0x45>>

  test "numeric and symbolic End descriptors normalize to End" do
    for fixture <- [@empty_end, @symbolic_end, @null_end] do
      assert {:ok, %End{error: nil}, <<>>} = Performative.decode(fixture)
    end
  end

  test "numeric and symbolic Close descriptors normalize to Close" do
    for fixture <- [@empty_close, @symbolic_close] do
      assert {:ok, %Close{error: nil}, <<>>} = Performative.decode(fixture)
    end
  end

  test "canonically encodes empty End and Close with numeric descriptors" do
    assert {:ok, @empty_end} = Performative.encode(%End{})
    assert {:ok, @empty_close} = Performative.encode(%Close{})
  end

  test "symbolic inputs re-encode with numeric descriptors" do
    assert {:ok, end_performative, <<>>} = Performative.decode(@symbolic_end)
    assert {:ok, close_performative, <<>>} = Performative.decode(@symbolic_close)
    assert {:ok, @empty_end} = Performative.encode(end_performative)
    assert {:ok, @empty_close} = Performative.encode(close_performative)
  end

  test "returns the exact suffix after one termination performative" do
    suffix = <<0xDE, 0xAD, 0xBE, 0xEF>>
    assert {:ok, %End{}, ^suffix} = Performative.decode(@empty_end <> suffix)
    assert {:ok, %Close{}, ^suffix} = Performative.decode(@empty_close <> suffix)
  end

  test "reports exact outer schema errors for End and Close" do
    for {performative, descriptor} <- [end: 0x17, close: 0x18],
        {body, reason, details} <- [
          {<<0x40>>, :invalid_performative_body, %{expected: :list, actual: :null}},
          {<<0xC0, 3, 2, 0x40, 0x40>>, :too_many_fields, %{actual: 2, maximum: 1}},
          {<<0xC0, 3, 1, 0x52, 1>>, :field_type_mismatch,
           %{field: :error, index: 0, expected: :error, actual: :uint}}
        ] do
      fixture = <<0x00, 0x53, descriptor>> <> body
      expected_details = Map.put(details, :performative, performative)

      assert {:error,
              %Error{
                operation: :performative_decode,
                class: :malformed,
                reason: ^reason,
                details: ^expected_details
              }} = Performative.decode(fixture)
    end
  end
end
