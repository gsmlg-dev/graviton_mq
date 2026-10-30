defmodule GravitonMQ.AMQP10.Codec.StreamTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.{Error, Limits, ProtocolHeader, Stream}
  alias GravitonMQ.AMQP10.Frame

  @header <<"AMQP", 0, 1, 0, 0>>
  @heartbeat <<8::32, 2, 0, 0::16>>
  @opaque_frame <<12::32, 2, 0, 19::16, 0xFF, 0, 1, 2>>

  test "one item at a time retains coalesced input without interpreting frame bodies" do
    assert {:ok, stream} = Stream.new()

    assert {:ok, {:protocol_header, %ProtocolHeader{}}, stream, rest} =
             Stream.next(stream, @header <> @heartbeat <> @opaque_frame)

    assert rest == @heartbeat <> @opaque_frame
    assert stream.buffer == <<>>
    assert {:ok, {:frame, %Frame{body: <<>>}}, stream, @opaque_frame} = Stream.next(stream, rest)

    assert {:ok, {:frame, %Frame{channel: 19, body: <<0xFF, 0, 1, 2>>}}, stream, <<>>} =
             Stream.next(stream, @opaque_frame)

    assert {:more, ^stream} = Stream.next(stream, <<>>)
  end

  test "every network fragmentation boundary preserves the same envelope" do
    for item <- [@header, @opaque_frame], split <- 0..(byte_size(item) - 1) do
      {:ok, stream} = Stream.new()

      stream =
        if item == @opaque_frame do
          {:ok, _, after_header, <<>>} = Stream.next(stream, @header)
          after_header
        else
          stream
        end

      <<prefix::binary-size(split), tail::binary>> = item
      assert {:more, incomplete} = Stream.next(stream, prefix)
      assert incomplete.buffer == prefix
      assert {:ok, _value, complete, <<>>} = Stream.next(incomplete, tail)
      assert complete.buffer == <<>>
    end
  end

  test "octet-at-a-time header and frame assembly handles concatenated input" do
    {:ok, stream} = Stream.new()

    {stream, items} =
      for <<octet <- @header <> @heartbeat <> @opaque_frame>>, reduce: {stream, []} do
        {state, items} ->
          case Stream.next(state, <<octet>>) do
            {:more, next} -> {next, items}
            {:ok, item, next, <<>>} -> {next, items ++ [item]}
          end
      end

    assert length(items) == 3
    assert stream.buffer == <<>>
  end

  test "declared size and header failures propagate before buffering a peer body" do
    {:ok, stream} = Stream.new(%{Limits.default() | max_frame_size: 16})
    {:ok, _, stream, <<>>} = Stream.next(stream, @header)

    assert {:error, %Error{class: :limit_exceeded}} =
             Stream.next(stream, <<1_000_000::32, 2, 0, 0::16>> <> :binary.copy("x", 1_000))

    assert {:error, %Error{class: :malformed}} = Stream.next(stream, <<7::32, 2, 0, 0::16>>)
    {:ok, fresh} = Stream.new()

    assert {:error, %Error{operation: :protocol_header, class: :unsupported}} =
             Stream.next(fresh, <<"AMQP", 3, 1, 0, 0>>)

    assert {:error, %Error{operation: :protocol_header, class: :malformed}} =
             Stream.next(fresh, "WRONG DATA")
  end

  test "a coalesced batch may exceed the per-frame limit while each retained buffer stays bounded" do
    {:ok, stream} = Stream.new(%{Limits.default() | max_frame_size: 8})
    {:ok, _, stream, rest} = Stream.next(stream, @header <> :binary.copy(@heartbeat, 1_000))

    {stream, <<>>} =
      Enum.reduce(1..1_000, {stream, rest}, fn _, {current, input} ->
        assert {:ok, {:frame, %Frame{}}, next, tail} = Stream.next(current, input)
        assert next.buffer == <<>>
        {next, tail}
      end)

    assert stream.buffer == <<>>
  end

  test "invalid limits and invalid manually constructed stream state fail explicitly" do
    assert {:error, %Error{reason: :invalid_limits}} =
             Stream.new(%{Limits.default() | max_frame_size: 0})

    {:ok, stream} = Stream.new()

    for invalid <- [%{stream | phase: :unknown}, %{stream | buffer: []}] do
      assert {:error, %Error{class: :invalid_value}} = Stream.next(invalid, <<>>)
    end

    assert {:error, %Error{class: :invalid_value}} = Stream.next(stream, :invalid)
  end
end
