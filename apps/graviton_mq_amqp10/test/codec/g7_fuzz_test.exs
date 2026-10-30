defmodule GravitonMQ.AMQP10.Codec.G7FuzzTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.{
    Error,
    Frame,
    Limits,
    Message,
    Performative,
    ProtocolHeader,
    Stream,
    Value
  }

  alias GravitonMQ.AMQP10.Value, as: V

  @seed {2026, 10, 30}
  @limits %Limits{
    max_frame_size: 64,
    max_value_bytes: 64,
    max_compound_items: 8,
    max_nesting_depth: 4
  }
  @classes [:malformed, :unsupported, :limit_exceeded, :invalid_value]
  @header <<"AMQP", 0, 1, 0, 0>>
  @open <<0, 0x53, 0x10, 0xC0, 4, 1, 0xA1, 1, "c">>
  @data <<0, 0x80, 0x75::64, 0xB0, 3::32, 0, 0xFF, 1>>

  test "seeded byte corpus has explicit bounded results across six public decoders" do
    :rand.seed(:exsss, @seed)

    fixed = [
      <<>>,
      <<0x40>>,
      <<0x56, 2>>,
      <<0x73, 0xD800::32>>,
      @header,
      @open,
      @data,
      <<0xF0, 5::32, 0xFFFFFFFF::32, 0x43>>,
      <<0xD0, 4::32, 0xFFFFFFFF::32>>,
      <<0xB0, 0xFFFFFFFF::32>>,
      <<0xE0, 2, 255, 0x45>>,
      <<0xC1, 5, 4, 0x40, 0x43, 0x40, 0x44>>,
      <<0, 0x53, 1, 0x40>>
    ]

    samples = for _ <- 1..1_024, do: random_bytes(:rand.uniform(49) - 1)

    mutations =
      for wire <- [@open, @data, <<0xE0, 6, 2, 0xA3, 1, "a", 1, "b">>],
          index <- 0..(byte_size(wire) - 1),
          replacement <- [0, 255] do
        <<before::binary-size(index), _old, after_bytes::binary>> = wire
        <<before::binary, replacement, after_bytes::binary>>
      end

    corpus = fixed ++ samples ++ mutations

    decoders = [
      value: &Value.decode(&1, @limits),
      frame: &Frame.decode(&1, @limits),
      performative: &Performative.decode(&1, @limits),
      message: &Message.decode(&1, @limits),
      section: &Message.decode_section(&1, @limits),
      header: &ProtocolHeader.recognize/1
    ]

    bounded(fn ->
      for {wire, index} <- Enum.with_index(corpus), {name, decode} <- decoders do
        result = decode_case(decode, wire, {name, index})
        assert_result(result, wire, name)
      end
    end)

    IO.puts(
      "G7 AMQP corpus: seed=#{inspect(@seed)} inputs=#{length(corpus)} decodes=#{length(corpus) * length(decoders)}"
    )
  end

  test "generated independent vectors retain type identity, exact float bits, suffix and truncation boundaries" do
    :rand.seed(:exsss, @seed)

    vectors =
      for n <- 1..96 do
        number = :rand.uniform(0x100000000) - 1
        bits = <<number::32>>
        members = for index <- 0..rem(n, 3), do: rem(number + index, 0x100000000)
        count = length(members)
        bodies = for member <- members, into: <<>>, do: <<member::32>>
        array = V.array(:uint, Enum.map(members, &V.uint/1))

        [
          {<<0x70, number::32>>, V.uint(number)},
          {<<0x72, bits::binary>>, V.float(bits)},
          {<<0xE0, 2 + byte_size(bodies), count, 0x70, bodies::binary>>, array}
        ]
      end
      |> List.flatten()

    prefixes = Enum.sum(for {wire, _} <- vectors, do: byte_size(wire))

    bounded(fn ->
      for {wire, expected} <- vectors do
        assert {:ok, ^expected, <<0xFF, 0x40>>} = Value.decode(wire <> <<0xFF, 0x40>>, @limits)

        for size <- 0..(byte_size(wire) - 1) do
          assert {:more, needed} = Value.decode(binary_part(wire, 0, size), @limits)
          assert needed in 1..64
        end
      end
    end)

    IO.puts("G7 AMQP semantic vectors=#{length(vectors)} strict prefixes=#{prefixes}")
  end

  test "opaque frame streaming is independent of fragmentation and coalesced size" do
    for n <- 0..32 do
      opaque = <<0xFF, 0x56, 2, n>>
      frame = <<12::32, 2, 0, n::16, opaque::binary>>
      stream_bytes = @header <> :binary.copy(frame, 8)
      # Aggregate input is larger than the individual frame bound. Every split
      # must yield the same nine envelopes, without interpreting invalid bodies.
      for split <- 0..byte_size(stream_bytes) do
        <<first::binary-size(split), second::binary>> = stream_bytes
        {:ok, stream} = Stream.new(@limits)
        {stream, first_items} = feed(stream, first)
        {stream, second_items} = feed(stream, second)
        items = first_items ++ second_items
        assert stream.buffer == <<>>
        assert length(items) == 9
        assert [{:protocol_header, _} | frames] = items

        assert Enum.all?(frames, fn {:frame, value} ->
                 value.body == opaque and value.channel == n
               end)
      end
    end
  end

  test "independent frame size/offset/type matrix enforces envelope boundaries" do
    for size <- 0..80, offset <- 0..5, type <- [0, 1, 2] do
      body = :binary.copy(<<0xFF>>, max(size - 8, 0))
      wire = <<size::32, offset, type, 17::16, body::binary>>

      if size >= 8 and offset >= 2 and offset * 4 <= size and size <= 64 and type == 0 do
        assert {:ok, frame, <<0x40>>} = Frame.decode(wire <> <<0x40>>, @limits)
        assert frame.channel == 17
        assert frame.extended_header == :binary.copy(<<0xFF>>, offset * 4 - 8)
        assert frame.body == :binary.copy(<<0xFF>>, size - offset * 4)
      else
        assert {:error, %Error{}} = Frame.decode(wire <> <<0x40>>, @limits)
      end
    end

    for {wire, decode} <- [
          {@header, &ProtocolHeader.recognize/1},
          {@open, &Performative.decode(&1, @limits)},
          {@data, &Message.decode_section(&1, @limits)}
        ] do
      assert {:ok, _, <<0xFF>>} = decode.(wire <> <<0xFF>>)

      for size <- 0..(byte_size(wire) - 1) do
        assert {:more, needed} = decode.(binary_part(wire, 0, size))
        assert needed in 1..64
      end
    end

    IO.puts("G7 AMQP frame boundary cases=1458; fragmented streams=3465")
  end

  test "malformed complete containers cannot borrow a following valid value and announced excess fails early" do
    for wire <- [<<0xC0, 2, 1, 0x70>>, <<0xC1, 2, 2, 0x43>>, <<0xE0, 3, 1, 0x70, 0>>] do
      assert {:error, %Error{class: :malformed}} = Value.decode(wire <> <<0x43>>, @limits)
    end

    for wire <- [<<0xB0, 65::32>>, <<0xF0, 65::32>>, <<0xD0, 65::32>>] do
      assert {:error, %Error{class: :limit_exceeded}} = Value.decode(wire, @limits)
    end

    assert {:error, %Error{class: :limit_exceeded}} =
             Frame.decode(<<65::32, 2, 0, 0::16>>, @limits)

    assert {:ok, message, <<>>} = Message.decode(@data, @limits)
    assert message.encoded_content == @data
    assert message.sections == [data: V.binary(<<0, 0xFF, 1>>)]
    assert {:ok, @data} = Message.encode(%{message | sections: []}, @limits)
  end

  defp assert_result({:ok, value, rest}, input, name) do
    assert is_binary(rest)
    consumed = byte_size(input) - byte_size(rest)
    assert consumed >= 0
    assert binary_part(input, consumed, byte_size(rest)) == rest

    case name do
      :frame ->
        assert value.declared_size == consumed
        assert consumed in 8..64
        offset = value.data_offset_words * 4
        assert value.body == binary_part(input, offset, consumed - offset)

      :message ->
        assert value.encoded_content == input
        assert rest == <<>>
        assert length(value.sections) <= @limits.max_compound_items

      _ ->
        assert consumed > 0
    end
  end

  defp assert_result({:more, needed}, _input, _name),
    do: assert(is_integer(needed) and needed in 1..64)

  defp assert_result({:error, %Error{} = error}, _input, _name) do
    assert error.class in @classes
    assert is_atom(error.reason)
  end

  defp assert_result(other, input, name),
    do: flunk("unexpected #{name} result #{inspect(other)} for #{Base.encode16(input)}")

  defp decode_case(decode, wire, context) do
    decode.(wire)
  rescue
    error ->
      flunk(
        "AMQP seed=#{inspect(@seed)} case=#{inspect(context)} hex=#{Base.encode16(wire)} raised #{Exception.message(error)}"
      )
  end

  defp feed(stream, input) do
    case Stream.next(stream, input) do
      {:more, stream} ->
        {stream, []}

      {:ok, item, stream, rest} ->
        {stream, following} = feed(stream, rest)
        {stream, [item | following]}

      {:error, error} ->
        flunk("valid fragmented stream failed: #{inspect(error)}")
    end
  end

  defp random_bytes(0), do: <<>>
  defp random_bytes(size), do: for(_ <- 1..size, into: <<>>, do: <<:rand.uniform(256) - 1>>)

  defp bounded(fun) do
    owner = self()

    {pid, monitor} =
      :erlang.spawn_opt(
        fn ->
          {:reductions, before} = Process.info(self(), :reductions)
          fun.()
          {:reductions, after_count} = Process.info(self(), :reductions)
          send(owner, {self(), :passed, after_count - before})
        end,
        [
          :monitor,
          {:max_heap_size,
           %{
             size: div(32_000_000, :erlang.system_info(:wordsize)),
             kill: true,
             error_logger: false,
             include_shared_binaries: true
           }}
        ]
      )

    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)

    receive do
      {^pid, :passed, reductions} ->
        assert reductions < 30_000_000
        assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 1_000

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        flunk("bounded AMQP corpus failed: #{inspect(reason)}")
    after
      10_000 -> flunk("bounded AMQP corpus exceeded 10 seconds")
    end
  end
end
