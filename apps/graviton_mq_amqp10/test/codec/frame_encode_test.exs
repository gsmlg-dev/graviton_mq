defmodule GravitonMQ.AMQP10.Codec.FrameEncodeTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.{Error, Frame, Limits}
  alias GravitonMQ.AMQP10.Frame, as: AMQPFrame

  # AMQP 1.0 Transport §2.3: uint32 SIZE, ubyte DOFF (four-octet words),
  # ubyte TYPE=0, ushort CHANNEL, followed by opaque extended header/body.
  @vectors [
    {<<0, 0, 0, 8, 2, 0, 0, 0>>,
     %AMQPFrame{
       declared_size: 8,
       data_offset_words: 2,
       type: :amqp,
       channel: 0,
       extended_header: <<>>,
       body: <<>>
     }},
    {<<0, 0, 0, 12, 2, 0, 0xFF, 0xFF, 0x00, 0xFF, 0x72, 0x80>>,
     %AMQPFrame{
       declared_size: 12,
       data_offset_words: 2,
       type: :amqp,
       channel: 65_535,
       extended_header: <<>>,
       body: <<0x00, 0xFF, 0x72, 0x80>>
     }},
    {<<0, 0, 0, 16, 3, 0, 0, 7, 1, 2, 3, 4, 9, 8, 7, 6>>,
     %AMQPFrame{
       declared_size: 16,
       data_offset_words: 3,
       type: :amqp,
       channel: 7,
       extended_header: <<1, 2, 3, 4>>,
       body: <<9, 8, 7, 6>>
     }},
    {<<0, 0, 0, 12, 3, 0, 0, 0, 0x00, 0xFF, 0x00, 0xFF>>,
     %AMQPFrame{
       declared_size: 12,
       data_offset_words: 3,
       type: :amqp,
       channel: 0,
       extended_header: <<0x00, 0xFF, 0x00, 0xFF>>,
       body: <<>>
     }},
    {<<0, 0, 3, 0xFC, 0xFF, 0, 0, 1, :binary.copy(<<0xFF>>, 1_012)::binary>>,
     %AMQPFrame{
       declared_size: 1_020,
       data_offset_words: 255,
       type: :amqp,
       channel: 1,
       extended_header: :binary.copy(<<0xFF>>, 1_012),
       body: <<>>
     }}
  ]

  test "encodes independent envelope vectors with exact opaque byte preservation" do
    for {wire, frame} <- @vectors do
      assert {:ok, ^wire} = Frame.encode(frame)
      assert {:ok, ^frame, <<>>} = Frame.decode(wire)
    end
  end

  test "encoded envelopes retain exact decoder prefixes, suffixes and adjacent frame boundaries" do
    heartbeat = <<0, 0, 0, 8, 2, 0, 0, 0>>
    suffix = heartbeat <> <<0xFF, 0xDE, 0xAD>>

    for {wire, frame} <- @vectors do
      assert {:ok, ^wire} = Frame.encode(frame)

      for size <- 0..(byte_size(wire) - 1) do
        expected = if size < 8, do: 8 - size, else: byte_size(wire) - size
        assert {:more, ^expected} = Frame.decode(binary_part(wire, 0, size))
      end

      assert {:ok, ^frame, ^suffix} = Frame.decode(wire <> suffix)
      assert {:ok, adjacent, <<0xFF, 0xDE, 0xAD>>} = Frame.decode(suffix)
      assert {:ok, ^heartbeat} = Frame.encode(adjacent)
    end
  end

  test "frame encoding never invokes value or performative parsing or their limits" do
    limits = %{Limits.default() | max_value_bytes: 1, max_compound_items: 1, max_nesting_depth: 1}

    for body <- [
          <<0x00, 0xFF>>,
          <<0xC0, 1, 0>>,
          <<0x98>>,
          <<0xFF, 0, 0xFF>>,
          <<0x00, 0x53, 0x10>>
        ] do
      frame = envelope(body)
      wire = <<byte_size(body) + 8::32, 2, 0, 0::16, body::binary>>
      assert {:ok, ^wire} = Frame.encode(frame, limits)
      assert {:ok, ^frame, <<>>} = Frame.decode(wire, limits)
    end
  end

  test "configured frame size is inclusive and applied to the whole envelope" do
    for {wire, frame} <- @vectors do
      limits = %{Limits.default() | max_frame_size: byte_size(wire)}
      assert {:ok, ^wire} = Frame.encode(frame, limits)

      assert_error(
        Frame.encode(frame, %{limits | max_frame_size: byte_size(wire) - 1}),
        :limit_exceeded,
        :frame_size_limit
      )
    end
  end

  test "malformed scalar header fields return structured errors without truncation or coercion" do
    frame = envelope(<<>>)

    for {field, bad_values, reason} <- [
          {:declared_size, [-1, 0, 7, 8.0, nil, "8", :auto, 0x1_0000_0000],
           :invalid_declared_size},
          {:data_offset_words, [-1, 0, 1, 256, 2.0, nil, :auto], :invalid_data_offset},
          {:channel, [-1, 65_536, 0.0, nil, "0", :zero], :invalid_channel}
        ],
        bad <- bad_values do
      assert_error(Frame.encode(Map.put(frame, field, bad)), :invalid_value, reason)
    end

    for invalid <- [nil, %{}, <<>>, []] do
      assert_error(Frame.encode(invalid), :invalid_value, :invalid_frame)
    end

    for field <- [:declared_size, :data_offset_words, :type, :channel, :extended_header, :body] do
      assert_error(Frame.encode(Map.delete(frame, field)), :invalid_value, :invalid_frame)
    end
  end

  test "body and extended header require binaries including byte-aligned empty binaries" do
    for invalid <- [nil, [], [0, 1], :binary, <<0::1>>] do
      assert_error(Frame.encode(%{envelope(<<>>) | body: invalid}), :invalid_value, :invalid_body)

      assert_error(
        Frame.encode(%{envelope(<<>>) | extended_header: invalid}),
        :invalid_value,
        :invalid_extended_header
      )
    end
  end

  test "extended header must be aligned to four octets" do
    for size <- [1, 2, 3, 5, 6, 7] do
      frame = %AMQPFrame{
        declared_size: 8 + size,
        data_offset_words: div(8 + size, 4),
        type: :amqp,
        channel: 0,
        extended_header: :binary.copy(<<0xFF>>, size),
        body: <<>>
      }

      assert_error(Frame.encode(frame), :invalid_value, :unaligned_extended_header)
    end
  end

  test "an aligned extended header cannot exceed the ubyte data-offset field" do
    frame = %AMQPFrame{
      declared_size: 1_024,
      data_offset_words: 255,
      type: :amqp,
      channel: 0,
      extended_header: :binary.copy(<<0xFF>>, 1_016),
      body: <<>>
    }

    assert_error(Frame.encode(frame), :limit_exceeded, :data_offset_limit)
  end

  test "supplied data offset must exactly match the actual extended header" do
    for frame <- [
          %{envelope(<<1, 2, 3, 4>>) | data_offset_words: 3},
          %AMQPFrame{
            declared_size: 12,
            data_offset_words: 2,
            type: :amqp,
            channel: 0,
            extended_header: <<1, 2, 3, 4>>,
            body: <<>>
          }
        ] do
      assert_error(Frame.encode(frame), :invalid_value, :data_offset_mismatch)
    end
  end

  test "supplied declared size must exactly match actual bytes even under permissive limits" do
    limits = %{Limits.default() | max_frame_size: 0x1_0000_0000}

    for size <- [8, 11, 13, 0xFFFF_FFFF] do
      assert_error(
        Frame.encode(%{envelope(<<1, 2, 3, 4>>) | declared_size: size}, limits),
        :invalid_value,
        :declared_size_mismatch
      )
    end

    assert_error(
      Frame.encode(%{envelope(<<>>) | declared_size: 0x1_0000_0000}, limits),
      :invalid_value,
      :invalid_declared_size
    )
  end

  test "SASL remains explicitly unsupported and other outbound frame types are invalid" do
    frame = envelope(<<>>)
    assert_error(Frame.encode(%{frame | type: :sasl}), :unsupported, :sasl_frame_not_supported)

    for type <- [0, 1, 2, nil, :mqtt, "amqp"] do
      assert_error(Frame.encode(%{frame | type: type}), :invalid_value, :invalid_frame_type)
    end
  end

  test "invalid limits are reported with the frame encode operation before inspecting input" do
    for field <- [:max_frame_size, :max_value_bytes, :max_compound_items, :max_nesting_depth] do
      limits = Map.put(Limits.default(), field, 0)
      details = Map.from_struct(limits)

      for frame <- [envelope(<<>>), nil] do
        assert {:error,
                %Error{
                  operation: :frame_encode,
                  class: :invalid_value,
                  reason: :invalid_limits,
                  offset: nil,
                  details: ^details
                }} = Frame.encode(frame, limits)
      end
    end
  end

  test "oversized body is rejected before concatenation within a finite process heap budget" do
    frame = envelope(:binary.copy(<<0xFF>>, 16_777_216))
    parent = self()

    {pid, monitor} =
      :erlang.spawn_opt(
        fn ->
          send(parent, {self(), Frame.encode(frame, %{Limits.default() | max_frame_size: 8})})
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
                       operation: :frame_encode,
                       class: :limit_exceeded,
                       reason: :frame_size_limit
                     }}},
                   5_000

    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
  end

  defp envelope(body),
    do: %AMQPFrame{
      declared_size: byte_size(body) + 8,
      data_offset_words: 2,
      type: :amqp,
      channel: 0,
      extended_header: <<>>,
      body: body
    }

  defp assert_error(result, class, reason) do
    assert {:error,
            %Error{
              operation: :frame_encode,
              class: ^class,
              reason: ^reason,
              offset: nil,
              details: %{}
            }} = result
  end
end
