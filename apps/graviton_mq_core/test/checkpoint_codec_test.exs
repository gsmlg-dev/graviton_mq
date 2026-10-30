defmodule GravitonMQ.Core.CheckpointCodecTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.Core.CheckpointCodec, as: Codec

  test "portable values round trip with exact identities and integer boundaries" do
    for value <- [
          %{version: 1, data: [nil, true, false, {"x", <<0, 255>>}]},
          -9_223_372_036_854_775_808,
          18_446_744_073_709_551_615,
          [],
          {}
        ] do
      assert {:ok, bytes} = Codec.encode(value)
      assert {:ok, ^value} = Codec.decode(bytes)
    end
  end

  test "runtime handles, improper lists, floats and excessive integers are rejected" do
    for value <- [
          self(),
          make_ref(),
          fn -> :ok end,
          %{nested: self()},
          [1 | 2],
          1.0,
          18_446_744_073_709_551_616,
          -9_223_372_036_854_775_809
        ] do
      assert {:error, _} = Codec.encode(value)
      assert {:error, _} = Codec.decode(:erlang.term_to_binary(value))
    end
  end

  test "preflight rejects compressed, huge declared allocation, unsafe atoms and trailing bytes" do
    assert {:error, _} =
             Codec.decode(
               :erlang.term_to_binary(%{data: :binary.copy("long", 1000)}, compressed: 9)
             )

    for bytes <- [
          <<131, 108, 0xFFFFFFFF::32, 106>>,
          <<131, 116, 0xFFFFFFFF::32>>,
          <<131, 105, 0xFFFFFFFF::32>>,
          <<131, 109, 0xFFFFFFFF::32>>,
          <<131, 111, 0xFFFFFFFF::32, 0>>,
          <<131, 97, 1, 97, 2>>,
          <<131, 119, 27, "uncreated_checkpoint_atom_42">>
        ] do
      assert {:error, _} = Codec.decode(bytes, max_items: 32)
    end
  end

  test "byte, recursive depth and cumulative item budgets apply in both directions" do
    for {value, limits} <- [
          {"123456789", [max_bytes: 8]},
          {[[[[1]]]], [max_depth: 2]},
          {%{data: [1, 2, 3]}, [max_items: 4]}
        ] do
      assert {:error, _} = Codec.encode(value, limits)
      assert {:error, _} = Codec.decode(:erlang.term_to_binary(value), limits)
    end

    assert {:error, :invalid_limits} = Codec.encode(:ok, max_depth: 0)
  end
end
