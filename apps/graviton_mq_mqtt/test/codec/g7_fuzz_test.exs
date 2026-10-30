defmodule GravitonMQ.MQTT.Codec.G7FuzzTest do
  use ExUnit.Case, async: true
  import Bitwise

  alias GravitonMQ.MQTT.Codec.{Error, Frame, Limits, Packet}
  alias GravitonMQ.MQTT.Packet.Publish

  @seed {2026, 30, 10}
  @limits %Limits{max_packet_size: 64, max_field_bytes: 16, max_properties: 2}
  @classes [:malformed, :unsupported, :limit_exceeded, :invalid_value]
  @connect <<0x10, 14, 0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c">>
  @publish <<0x32, 7, 0, 1, "t", 0, 7, 0, "x">>

  test "seeded byte corpus and every fixed header produce explicit bounded results" do
    :rand.seed(:exsss, @seed)

    fixed =
      for header <- 0..255,
          body <- [<<0>>, <<0x80, 0>>, <<0xFF, 0xFF, 0xFF, 0x7F>>],
          do: <<header, body::binary>>

    samples = for _ <- 1..1_024, do: random_bytes(:rand.uniform(49) - 1)

    mutations =
      for wire <- [@connect, @publish, <<0x82, 7, 0, 9, 0, 0, 1, "t", 1>>],
          index <- 0..(byte_size(wire) - 1),
          replacement <- [0, 255] do
        <<before::binary-size(index), _old, after_bytes::binary>> = wire
        <<before::binary, replacement, after_bytes::binary>>
      end

    corpus = [<<>> | fixed ++ samples ++ mutations]

    decoders = [
      frame: &Frame.decode(&1, @limits),
      v5: &Packet.decode(&1, :v5, @limits),
      legacy: &Packet.decode(&1, :v3_1_1, @limits),
      auto: &Packet.decode(&1, :auto, @limits)
    ]

    bounded(fn ->
      for {wire, index} <- Enum.with_index(corpus), {name, decode} <- decoders do
        result = decode_case(decode, wire, {name, index})
        assert_result(result, wire, name)
      end
    end)

    IO.puts(
      "G7 MQTT corpus: seed=#{inspect(@seed)} inputs=#{length(corpus)} decodes=#{length(corpus) * length(decoders)}"
    )
  end

  test "generated wire publications preserve QoS ownership fields, opaque payload and exact boundaries" do
    :rand.seed(:exsss, @seed)

    vectors =
      for n <- 1..96 do
        qos = rem(n, 3)
        topic = "t/#{n}"
        payload = random_bytes(rem(n, 9))
        id = if qos == 0, do: <<>>, else: <<n::16>>
        # Independent MQTT 5 §3.3 layout; no production encoder is used to
        # create the expected fields or the packet boundary.
        body = <<byte_size(topic)::16, topic::binary, id::binary, 0, payload::binary>>
        wire = <<0x30 ||| qos <<< 1, byte_size(body), body::binary>>

        {wire,
         %{topic: topic, payload: payload, qos: qos, packet_id: if(qos == 0, do: nil, else: n)}}
      end

    prefixes = Enum.sum(for {wire, _} <- vectors, do: byte_size(wire))

    bounded(fn ->
      for {wire, expected} <- vectors do
        assert {:ok, %Publish{} = packet, <<0xC0, 0>>} =
                 Packet.decode(wire <> <<0xC0, 0>>, :v5, @limits)

        assert Map.take(packet, Map.keys(expected)) == expected
        assert packet.properties == []
        assert packet.version == :v5

        for size <- 0..(byte_size(wire) - 1) do
          prefix = binary_part(wire, 0, size)
          assert {:more, needed} = Packet.decode(prefix, :v5, @limits)
          assert needed in 1..64
          assert {:more, _} = Frame.decode(prefix, @limits)
        end
      end
    end)

    IO.puts(
      "G7 MQTT semantic vectors=#{length(vectors)} strict prefixes=#{prefixes} (packet and frame)"
    )
  end

  test "all 256 fixed headers agree with independent MQTT flag admissibility" do
    # MQTT 5 §§2.2.2 and 3.3.1: reserved type zero, mandatory flags,
    # reserved QoS3 and QoS0 DUP are rejected at the opaque envelope boundary.
    valid = [
      0x10,
      0x20,
      0x30,
      0x31,
      0x32,
      0x33,
      0x34,
      0x35,
      0x3A,
      0x3B,
      0x3C,
      0x3D,
      0x40,
      0x50,
      0x62,
      0x70,
      0x82,
      0x90,
      0xA2,
      0xB0,
      0xC0,
      0xD0,
      0xE0,
      0xF0
    ]

    for header <- 0..255 do
      wire = <<header, 0>>

      if header in valid do
        assert {:ok, frame, <<0xFF>>} = Frame.decode(wire <> <<0xFF>>, @limits)
        assert frame.type == header >>> 4
        assert frame.flags == (header &&& 15)
        assert frame.body == <<>>
      else
        assert {:error, %Error{class: :malformed, reason: :invalid_fixed_header}} =
                 Frame.decode(wire, @limits)
      end
    end

    for remaining <- 0..63 do
      assert {:error, %Error{class: :malformed, reason: :nonminimal_variable_integer}} =
               Frame.decode(<<0x30, remaining + 128, 0>>, @limits)
    end
  end

  test "complete malformed fields cannot borrow the next packet and limits reject before allocation" do
    for wire <- [
          <<0x30, 2, 0, 1>>,
          <<0x32, 5, 0, 1, "t", 0, 7>>,
          <<0x40, 2, 0, 0>>,
          <<0x82, 7, 0, 9, 0, 0, 1, "t", 3>>,
          <<0xC0, 1, 0>>
        ] do
      assert {:error, %Error{class: :malformed}} =
               Packet.decode(wire <> <<0xC0, 0>>, :v5, @limits)
    end

    for wire <- [<<0x30, 63>>, <<0x30, 0xFF, 0xFF, 0xFF, 0x7F>>] do
      assert {:error, %Error{class: :limit_exceeded, reason: :packet_too_large}} =
               Frame.decode(wire, @limits)

      assert {:error, %Error{class: :limit_exceeded, reason: :packet_too_large}} =
               Packet.decode(wire, :v5, @limits)
    end

    assert {:error, %Error{class: :limit_exceeded, reason: :field_too_large}} =
             Packet.decode(<<0x30, 4, 0, 255, 0, 0>>, :v5, @limits)

    properties = :binary.copy(<<0x26, 0, 1, "k", 0, 1, "v">>, 3)

    wire =
      <<0x30, 4 + byte_size(properties), 0, 1, "t", byte_size(properties), properties::binary>>

    assert {:error, %Error{class: :limit_exceeded}} = Packet.decode(wire, :v5, @limits)

    for continuation_count <- 4..16 do
      wire = <<0x30, :binary.copy(<<0x80>>, continuation_count)::binary, 0>>

      assert {:error, %Error{class: :malformed, reason: :invalid_variable_integer}} =
               Frame.decode(wire, @limits)
    end
  end

  defp assert_result({:ok, value, rest}, input, name) do
    assert is_binary(rest)
    consumed = byte_size(input) - byte_size(rest)
    assert consumed in 2..64
    assert binary_part(input, consumed, byte_size(rest)) == rest

    if name == :frame do
      assert consumed == value.header_size + byte_size(value.body)
      assert value.body == binary_part(input, value.header_size, byte_size(value.body))
    else
      assert value.version in [:v5, :v3_1_1]
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
        "MQTT seed=#{inspect(@seed)} case=#{inspect(context)} hex=#{Base.encode16(wire)} raised #{Exception.message(error)}"
      )
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
        flunk("bounded MQTT corpus failed: #{inspect(reason)}")
    after
      10_000 -> flunk("bounded MQTT corpus exceeded 10 seconds")
    end
  end
end
