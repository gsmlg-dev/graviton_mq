Code.require_file("support/peer_process.exs", __DIR__)
Code.require_file("support/amqp_outcome_loss_proxy.exs", __DIR__)

defmodule GravitonMQ.AMQP10Client.ArtemisTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Client.Test.PeerProcess
  alias GravitonMQ.Client.Test.AMQPOutcomeLossProxy

  @moduletag :artemis_interop
  @moduletag timeout: 30_000
  @moduletag :tmp_dir
  @peer Path.join(__DIR__, "support/amqp_peer.py")

  setup_all do
    host = System.fetch_env!("ARTEMIS_HOST")
    port = System.fetch_env!("ARTEMIS_PORT") |> String.to_integer()
    python = System.fetch_env!("GRAVITON_PROTON_PYTHON")
    %{host: host, port: port, python: python, url: "amqp://#{host}:#{port}"}
  end

  test "a real Artemis outcome lost in transit produces unknown instead of success", context do
    proxy =
      start_supervised!({AMQPOutcomeLossProxy, upstream_port: context.port, observer: self()})

    client = connected!(context, port: AMQPOutcomeLossProxy.port(proxy))
    session!(client)
    sender = attach!(client, "unknown", :sender, address("unknown"))
    {:ok, raw} = Message.encode_sections([{:data, Value.binary("accepted-outcome-will-be-lost")}])
    assert {:ok, operation} = Client.send_message(client, 1, sender, raw, timeout: 200)
    assert_receive {:amqp_outcome_dropped, ^proxy}, 5000
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:unknown, _reason}}}, 5000
    refute_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, _}}}, 50
  end

  test "outgoing transfer reaches Artemis and an independent Proton receiver", context do
    client = connected!(context)
    session!(client)
    address = address("send")
    sender = attach!(client, "sender", :sender, address)
    body = :binary.copy(<<0, 1, 255, 2>>, 40_000)
    {:ok, raw} = Message.encode_sections([{:data, Value.binary(body)}])
    path = Path.join(context.tmp_dir, "body")
    File.write!(path, body)

    receiver =
      start_supervised!(
        {PeerProcess,
         executable: context.python, args: [@peer, "receive", context.url, address, path]}
      )

    eventually!(fn -> PeerProcess.snapshot(receiver).output =~ "\"ready\": true" end)
    assert {:ok, operation} = Client.send_message(client, 1, sender, raw)

    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, %Outcome.Accepted{}}}},
                   10_000

    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    %{output: output, exit_status: status} = PeerProcess.snapshot(receiver)
    assert status == 0, output
    assert output =~ "\"received_bytes\": #{byte_size(body)}"
    assert output =~ digest(body)
    assert Client.status(client).phase == :active
  end

  test "independent Proton publication arrives through Artemis under explicit host credit",
       context do
    client =
      connected!(context,
        max_frame_size: 512,
        session_options: [max_message_bytes: 262_144, max_incoming_bytes: 262_144]
      )

    session!(client)
    address = address("receive")
    receiver = attach!(client, "receiver", :receiver, address, max_message_bytes: 262_144)
    body = :binary.copy(<<254, 0, 3, 4>>, 25_000)
    path = Path.join(context.tmp_dir, "body")
    File.write!(path, body)
    {output, status} = proton(context, "send", address, path)
    assert status == 0, output
    refute_receive {:graviton_mq, ^client, {:delivery, _, _, _, _, _}}, 50
    assert {:ok, operation} = Client.request(client, 1, receiver, 1, 262_144)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, 1}}}, 5_000

    assert_receive {:graviton_mq, ^client,
                    {:delivery, reference, 1, ^receiver, raw, %{message_format: 0}}},
                   10_000

    assert {:ok, decoded, <<>>} = Message.decode(raw)
    assert {:amqp_value, Value.binary(body)} in decoded.sections
    assert {:ok, settlement} = Client.settle(client, reference, %Outcome.Accepted{})

    assert_receive {:graviton_mq, ^client, {:operation, ^settlement, {:ok, %Outcome.Accepted{}}}},
                   5_000

    assert Client.status(client).phase == :active
  end

  test "release causes actual Artemis redelivery before explicit acceptance", context do
    client = connected!(context, session_options: [max_message_bytes: 1024])
    session!(client)
    address = address("release")
    receiver = attach!(client, "release-receiver", :receiver, address, max_message_bytes: 1024)
    path = Path.join(context.tmp_dir, "body")
    File.write!(path, "release-redelivery")
    {output, status} = proton(context, "send", address, path)
    assert status == 0, output
    request!(client, receiver, 1, 1024)
    assert_receive {:graviton_mq, ^client, {:delivery, first, 1, ^receiver, original, _}}, 5_000
    assert {:ok, released} = Client.settle(client, first, %Outcome.Released{})

    assert_receive {:graviton_mq, ^client, {:operation, ^released, {:ok, %Outcome.Released{}}}},
                   5_000

    request!(client, receiver, 1, 1024)

    assert_receive {:graviton_mq, ^client, {:delivery, second, 1, ^receiver, redelivered, _}},
                   5_000

    refute second == first
    assert {:ok, first_message, <<>>} = Message.decode(original)
    assert {:ok, second_message, <<>>} = Message.decode(redelivered)

    assert {:amqp_value, Value.binary("release-redelivery")} in first_message.sections
    assert {:amqp_value, Value.binary("release-redelivery")} in second_message.sections

    assert {:ok, accepted} = Client.settle(client, second, %Outcome.Accepted{})

    assert_receive {:graviton_mq, ^client, {:operation, ^accepted, {:ok, %Outcome.Accepted{}}}},
                   5_000
  end

  test "peer idle negotiation, detach, End and Close complete over the actual network", context do
    client = connected!(context, idle_timeout_ms: 2000)
    session!(client)
    sender = attach!(client, "lifecycle", :sender, address("lifecycle"))
    assert {:ok, detached} = Client.detach(client, 1, sender)
    assert_receive {:graviton_mq, ^client, {:operation, ^detached, {:ok, _}}}, 5_000
    assert {:ok, ended} = Client.end_session(client, 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^ended, {:ok, 1}}}, 5_000
    assert {:ok, closed} = Client.close(client)
    assert_receive {:graviton_mq, ^client, {:operation, ^closed, {:ok, _}}}, 5_000
    assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 5_000
    assert Client.status(client).phase == :closed
  end

  test "Transfer and delivery identifiers cross uint32 wrap against Artemis", context do
    client = connected!(context)

    assert {:ok, opening} =
             Client.open_session(client, 1,
               initial_outgoing_id: 0xFFFF_FFFE,
               initial_delivery_id: 0xFFFF_FFFE
             )

    assert_receive {:graviton_mq, ^client, {:operation, ^opening, {:ok, 1}}}, 5_000
    address = address("serial-wrap")
    sender = attach!(client, "serial-wrap", :sender, address)

    script = """
    import json, sys, proton
    from proton.utils import BlockingConnection
    from proton.reactor import AtLeastOnce
    assert proton.VERSION == (0, 40, 0), proton.VERSION
    connection = BlockingConnection(sys.argv[1], timeout=10, sasl_enabled=False)
    try:
        receiver = connection.create_receiver(sys.argv[2], credit=3, options=AtLeastOnce())
        print('READY', flush=True)
        bodies = []
        for _ in range(3):
            message = receiver.receive(timeout=10)
            bodies.append(bytes(message.body).decode('ascii'))
            receiver.accept()
        assert bodies == ['before-wrap', 'last-uint32', 'after-wrap'], bodies
        print(json.dumps(bodies), flush=True)
    finally:
        connection.close()
    """

    receiver =
      start_supervised!(
        {PeerProcess, executable: context.python, args: ["-c", script, context.url, address]},
        id: make_ref()
      )

    eventually!(fn -> PeerProcess.snapshot(receiver).output =~ "READY" end)

    for body <- ["before-wrap", "last-uint32", "after-wrap"] do
      {:ok, raw} = Message.encode_sections([{:data, Value.binary(body)}])
      assert {:ok, operation} = Client.send_message(client, 1, sender, raw)

      assert_receive {:graviton_mq, ^client,
                      {:operation, ^operation, {:ok, %Outcome.Accepted{}}}},
                     5_000
    end

    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(receiver)
    assert output =~ "after-wrap"
    assert Client.status(client).phase == :active
  end

  @tag :artemis_tls_interop
  test "verified Artemis TLS carries publications and deliveries through independent Proton peers",
       context do
    tls_port = System.fetch_env!("ARTEMIS_TLS_PORT") |> String.to_integer()
    ca = System.fetch_env!("ARTEMIS_TLS_CA")
    tls_context = %{context | url: "amqps://localhost:#{tls_port}"}

    client =
      connected!(context,
        host: "localhost",
        port: tls_port,
        transport: :tls,
        tls_options: [cacertfile: ca]
      )

    session!(client)
    outgoing_address = address("tls-out")
    sender = attach!(client, "tls-out", :sender, outgoing_address)
    body = :binary.copy(<<0, 255, 1, 254>>, 512)
    path = Path.join(context.tmp_dir, "tls-body")
    File.write!(path, body)

    receiver =
      start_supervised!(
        {PeerProcess,
         executable: context.python,
         args: [@peer, "receive", tls_context.url, outgoing_address, path, "--cafile", ca]}
      )

    eventually!(fn -> PeerProcess.snapshot(receiver).output =~ "\"ready\": true" end)
    {:ok, raw} = Message.encode_sections([{:data, Value.binary(body)}])
    assert {:ok, operation} = Client.send_message(client, 1, sender, raw)

    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, %Outcome.Accepted{}}}},
                   5000

    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(receiver)
    assert output =~ digest(body)

    incoming_address = address("tls-in")
    link = attach!(client, "tls-in", :receiver, incoming_address, max_message_bytes: 4096)

    {output, status} =
      System.cmd(
        context.python,
        [@peer, "send", tls_context.url, incoming_address, path, "--cafile", ca],
        stderr_to_stdout: true
      )

    assert status == 0, output
    request!(client, link, 1, 4096)
    assert_receive {:graviton_mq, ^client, {:delivery, reference, 1, ^link, encoded, _}}, 5000
    assert {:ok, decoded, <<>>} = Message.decode(encoded)
    assert {:amqp_value, Value.binary(body)} in decoded.sections
    assert {:ok, settlement} = Client.settle(client, reference, %Outcome.Accepted{})

    assert_receive {:graviton_mq, ^client, {:operation, ^settlement, {:ok, %Outcome.Accepted{}}}},
                   5000

    assert Client.status(client).phase == :active
  end

  @tag :artemis_tls_interop
  test "Artemis TLS rejects the wrong trust root and wrong hostname before AMQP negotiation" do
    tls_port = System.fetch_env!("ARTEMIS_TLS_PORT") |> String.to_integer()

    for {host, ca} <- [
          {"localhost", System.fetch_env!("ARTEMIS_TLS_UNTRUSTED_CA")},
          {"127.0.0.1", System.fetch_env!("ARTEMIS_TLS_CA")}
        ] do
      client =
        start_supervised!(
          {Client,
           container_id: address("tls-refused"),
           host: host,
           port: tls_port,
           transport: :tls,
           tls_options: [cacertfile: ca]},
          id: make_ref()
        )

      assert_receive {:graviton_mq, ^client, {:disconnected, {:connect, _}}}, 5000
      refute_receive {:graviton_mq, ^client, {:connected, _}}, 50
      assert %{phase: :closed, sessions: 0, pending: 0, writes: 0} = Client.status(client)
    end
  end

  defp connected!(context, options \\ []) do
    options =
      Keyword.merge(
        [container_id: address("client"), host: context.host, port: context.port],
        options
      )

    client = start_supervised!({Client, options})
    assert_receive {:graviton_mq, ^client, {:connected, peer}}, 5_000
    assert is_binary(peer) and peer != ""
    client
  end

  defp session!(client) do
    assert {:ok, operation} = Client.open_session(client, 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, 1}}}, 5_000
  end

  defp attach!(client, name, role, address, options \\ []) do
    assert {:ok, operation} =
             Client.attach(
               client,
               1,
               name,
               role,
               Keyword.put(options, :address, Value.string(address))
             )

    identity = {Value.string(name), role}
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^identity}}}, 5_000
    identity
  end

  defp request!(client, identity, slots, bytes) do
    assert {:ok, operation} = Client.request(client, 1, identity, slots, bytes)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^slots}}}, 5_000
  end

  defp address(prefix), do: "graviton-#{prefix}-#{System.unique_integer([:positive])}"

  defp proton(context, operation, address, path),
    do:
      System.cmd(context.python, [@peer, operation, context.url, address, path],
        stderr_to_stdout: true
      )

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp eventually!(predicate), do: until!(predicate, System.monotonic_time(:millisecond) + 12_000)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline,
             "independent peer condition timed out"

      Process.sleep(10)
      until!(predicate, deadline)
    end
  end
end
