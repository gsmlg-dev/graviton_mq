Code.require_file("support/peer_process.exs", __DIR__)

defmodule GravitonMQ.AMQP10Client.MessageArtemisTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Client.Test.PeerProcess

  @moduletag :artemis_interop
  @moduletag :tmp_dir
  @moduletag timeout: 60_000
  @peer Path.join(__DIR__, "support/amqp10_message_artemis_peer.py")

  setup context do
    host = System.fetch_env!("ARTEMIS_HOST")
    port = System.fetch_env!("ARTEMIS_PORT") |> String.to_integer()
    python = System.fetch_env!("GRAVITON_PROTON_PYTHON")
    directory = Path.join(context.tmp_dir, "corpus")
    {output, status} = System.cmd(python, [@peer, "fixtures", directory], stderr_to_stdout: true)
    assert status == 0, output
    names = File.read!(Path.join(directory, "manifest")) |> String.split("\n", trim: true)
    assert length(names) == 12

    %{
      host: host,
      port: port,
      python: python,
      url: "amqp://#{host}:#{port}",
      directory: directory,
      names: names
    }
  end

  test "client raw corpus reaches Artemis and Proton with exact immutable bytes", context do
    client = connected!(context)
    address = address("out")
    sender = attach!(client, "corpus-out", :sender, address)
    receiver = peer!(context, "receive", address)
    eventually!(fn -> PeerProcess.snapshot(receiver).output =~ "READY" end, receiver)

    for name <- context.names do
      original = File.read!(Path.join(context.directory, name <> ".amqp"))
      assert_semantics!(name, original)
      assert {:ok, operation} = Client.send_message(client, 1, sender, original)

      assert_receive {:graviton_mq, ^client,
                      {:operation, ^operation, {:ok, %Outcome.Accepted{}}}},
                     10_000
    end

    assert_peer!(receiver)

    for name <- context.names do
      original = File.read!(Path.join(context.directory, name <> ".amqp"))
      actual = File.read!(Path.join(context.directory, name <> ".received"))
      assert bare(actual) == bare(original), name
      assert_semantics!(name, actual)
    end

    assert Client.status(client).phase == :active
  end

  test "Proton raw corpus reaches Artemis and client with exact immutable bytes", context do
    client = connected!(context)
    address = address("in")
    receiver = attach!(client, "corpus-in", :receiver, address)
    sender = peer!(context, "send", address)
    assert_peer!(sender)
    refute_receive {:graviton_mq, ^client, {:delivery, _, _, _, _, _}}, 50
    count = length(context.names)
    assert {:ok, request} = Client.request(client, 1, receiver, count, count * 16_384)
    assert_receive {:graviton_mq, ^client, {:operation, ^request, {:ok, ^count}}}, 5_000

    for name <- context.names do
      assert_receive {:graviton_mq, ^client,
                      {:delivery, reference, 1, ^receiver, actual, %{message_format: 0}}},
                     10_000

      original = File.read!(Path.join(context.directory, name <> ".amqp"))
      assert bare(actual) == bare(original), name
      assert_semantics!(name, actual)
      assert {:ok, settlement} = Client.settle(client, reference, %Outcome.Accepted{})

      assert_receive {:graviton_mq, ^client,
                      {:operation, ^settlement, {:ok, %Outcome.Accepted{}}}},
                     5_000
    end

    assert Client.status(client).phase == :active
  end

  defp assert_semantics!(name, raw) do
    assert {:ok, decoded, <<>>} = Message.decode(raw)
    [kind, index] = String.split(name, "-")
    index = String.to_integer(index)

    identifiers = [
      Value.ulong(123),
      Value.uuid(
        <<0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x81, 0x23, 0x45, 0x67, 0x89, 0xAB,
          0xCD, 0xEF>>
      ),
      Value.binary(<<0, 255, 128, "-id">>),
      Value.string("message-id-雪")
    ]

    expected_properties = [
      Enum.at(identifiers, index),
      Value.binary(<<0, 255, "-user">>),
      Value.string("unchanged-to"),
      Value.string("subject-雪"),
      Value.string("reply-to"),
      Enum.at(identifiers, rem(index + 1, 4)),
      Value.symbol("application/x-graviton"),
      if(kind == "data", do: Value.symbol("gzip"), else: Value.null()),
      Value.null(),
      Value.timestamp(123_456_789),
      Value.string("group-id"),
      Value.uint(9),
      Value.string("reply-group-id")
    ]

    assert {:properties, Value.list(expected_properties)} in decoded.sections

    application =
      Value.map([
        {Value.string("null"), Value.null()},
        {Value.string("boolean"), Value.boolean(true)},
        {Value.string("binary"), Value.binary(<<0, 255, 128>>)},
        {Value.string("byte"), Value.byte(-128)},
        {Value.string("int"), Value.int(-129)},
        {Value.string("ulong"), Value.ulong(1_099_511_627_776)},
        {Value.string("text"), Value.string("application-雪")}
      ])

    assert {:application_properties, application} in decoded.sections

    assert {:message_annotations, %Value{type: :map, value: annotations}} =
             List.keyfind(decoded.sections, :message_annotations, 0)

    assert {Value.symbol("x-opt-sequence"), Value.uint(17)} in annotations
    assert {Value.symbol("x-opt-null"), Value.null()} in annotations

    assert {Value.symbol("x-opt-array"), Value.array(:uint, [Value.uint(0), Value.uint(256)])} in annotations

    body = Enum.filter(decoded.sections, &(elem(&1, 0) in [:data, :amqp_sequence, :amqp_value]))

    case kind do
      "data" ->
        assert [
                 {:data, %Value{type: :binary, value: first}},
                 {:data, %Value{type: :binary, value: second}}
               ] = body

        expected =
          for number <- 0..127, into: <<>>, do: :crypto.hash(:sha256, Integer.to_string(number))

        assert :zlib.gunzip(first <> second) == expected

      "sequence" ->
        assert body == [
                 {:amqp_sequence,
                  Value.list([
                    Value.uint(1),
                    Value.string("sequence-雪"),
                    Value.list([Value.null(), Value.boolean(false)])
                  ])},
                 {:amqp_sequence,
                  Value.list([Value.binary(<<0, 255>>), Value.ulong(1_099_511_627_776)])}
               ]

      "value" ->
        assert body == [
                 {:amqp_value,
                  Value.map([
                    {Value.null(), Value.string("generic-null-map-key")},
                    {Value.string("nested"),
                     Value.list([
                       Value.uint(1),
                       Value.symbol("distinct-symbol"),
                       Value.binary("distinct-binary"),
                       Value.string("distinct-string"),
                       Value.array(:uint, [Value.uint(0), Value.uint(256)])
                     ])}
                  ])}
               ]
    end

    assert {:footer, %Value{type: :map}} = List.keyfind(decoded.sections, :footer, 0)
    assert {:ok, ^raw} = Message.encode(decoded)
  end

  defp bare(raw) do
    assert {:ok, {name, _value}, rest} = Message.decode_section(raw)
    if name in [:header, :delivery_annotations, :message_annotations], do: bare(rest), else: raw
  end

  defp connected!(context) do
    client =
      start_supervised!(
        {Client,
         container_id: address("client"),
         host: context.host,
         port: context.port,
         max_frame_size: 512,
         session_options: [max_message_bytes: 16_384]}
      )

    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    assert {:ok, opening} = Client.open_session(client, 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^opening, {:ok, 1}}}, 5_000
    client
  end

  defp attach!(client, name, role, address) do
    assert {:ok, operation} =
             Client.attach(client, 1, name, role,
               address: Value.string(address),
               max_message_bytes: 16_384
             )

    identity = {Value.string(name), role}
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^identity}}}, 5_000
    identity
  end

  defp peer!(context, mode, address) do
    start_supervised!(
      {PeerProcess,
       executable: context.python,
       args: [@peer, mode, context.directory, "--url", context.url, "--address", address]},
      id: make_ref()
    )
  end

  defp assert_peer!(peer) do
    eventually!(fn -> PeerProcess.snapshot(peer).exit_status != nil end, peer)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(peer)
    assert output =~ "PASS "
    assert output =~ ~s("fixtures": 12)
  end

  defp eventually!(predicate, peer),
    do: until!(predicate, peer, System.monotonic_time(:millisecond) + 20_000)

  defp until!(predicate, peer, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline, inspect(PeerProcess.snapshot(peer))
      Process.sleep(10)
      until!(predicate, peer, deadline)
    end
  end

  defp address(suffix), do: "graviton-message-#{suffix}-#{System.unique_integer([:positive])}"
end
