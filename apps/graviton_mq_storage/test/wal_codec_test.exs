defmodule GravitonMQ.Storage.WALCodecTest do
  # Pure on-disk format checks; they need no file system or platform profile.
  use ExUnit.Case, async: true

  alias GravitonMQ.Storage.WAL.Codec

  @stream "codec/stream"

  test "generation 0 keeps the original version 1 header and checkpoint bytes" do
    header = crc(<<"GMWL", 1, 1, byte_size(@stream)::16, @stream::binary>>)
    assert Codec.header(@stream, :opaque) == header
    assert {:ok, byte_size(header)} == Codec.header_size(binary_part(header, 0, 8))

    assert {:ok, %{generation: 0, base_position: 0, base_end: base, header_size: base}} =
             Codec.decode_header(header, @stream, :opaque)

    assert base == byte_size(header)

    checkpoint =
      crc(<<"GMCP", 1, 0, byte_size(@stream)::16, @stream::binary, 7::64, 900::64>>)

    assert Codec.checkpoint(@stream, :events, 0, 7, 900) == checkpoint
    assert {:ok, 0} = Codec.checkpoint_generation(checkpoint)
    assert {:ok, 0, 7, 900} = Codec.decode_checkpoint(checkpoint, @stream, :events)
  end

  test "compacted generations round trip their base position and size" do
    header = Codec.header(@stream, :events, 3, 41, 4096)
    assert byte_size(header) == Codec.compacted_header_size(@stream)
    assert {:ok, byte_size(header)} == Codec.header_size(binary_part(header, 0, 8))

    assert {:ok, %{generation: 3, base_position: 41, base_end: 4096}} =
             Codec.decode_header(header, @stream, :events)

    assert {:error, :stream_mode} = Codec.decode_header(header, @stream, :opaque)
    assert {:error, :stream_id} = Codec.decode_header(header, "other", :events)

    checkpoint = Codec.checkpoint(@stream, :events, 3, 45, 5000)
    assert {:ok, 3} = Codec.checkpoint_generation(checkpoint)
    assert {:ok, 3, 45, 5000} = Codec.decode_checkpoint(checkpoint, @stream, :events)
    assert {:error, :checkpoint_corrupt} = Codec.decode_checkpoint(checkpoint, @stream, :opaque)
  end

  test "malformed version 2 headers and checkpoints are rejected" do
    zero =
      crc(<<"GMWL", 2, 0, byte_size(@stream)::16, @stream::binary, 0::64, 0::64, 4096::64>>)

    assert {:error, :wal_header} = Codec.decode_header(zero, @stream, :events)

    short_base =
      crc(<<"GMWL", 2, 0, byte_size(@stream)::16, @stream::binary, 1::64, 0::64, 10::64>>)

    assert {:error, :wal_header} = Codec.decode_header(short_base, @stream, :events)

    header = Codec.header(@stream, :events, 1, 0, 4096)
    <<head::binary-size(20), octet, tail::binary>> = header
    flipped = <<head::binary, Bitwise.bxor(octet, 1), tail::binary>>
    assert {:error, :wal_header} = Codec.decode_header(flipped, @stream, :events)

    zero_checkpoint =
      crc(<<"GMCP", 2, 0, byte_size(@stream)::16, @stream::binary, 0::64, 1::64, 2::64>>)

    assert {:error, :checkpoint_corrupt} = Codec.checkpoint_generation(zero_checkpoint)
    assert {:error, :checkpoint_corrupt} = Codec.checkpoint_generation(<<0, 1, 2>>)
  end

  defp crc(body), do: <<body::binary, :erlang.crc32(body)::32>>
end
