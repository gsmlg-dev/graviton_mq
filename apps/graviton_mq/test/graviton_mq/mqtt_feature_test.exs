defmodule GravitonMQ.MQTTBroker.FeatureTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.MQTT.Codec.Packet

  alias GravitonMQ.MQTT.Packet.{
    Connack,
    Connect,
    Disconnect,
    Puback,
    Pubcomp,
    Publish,
    Pubrec,
    Pubrel,
    Suback,
    Subscribe
  }

  @moduletag timeout: 30_000

  # Raw-socket evidence for S4 (Topic Alias, Subscription Identifiers, shared
  # subscriptions) against the running broker. Packets are built and read with
  # the pure codec, which does not resolve aliases, so alias-only PUBLISH
  # packets are observed exactly as sent on the wire.

  defp broker(options \\ []) do
    mqtt =
      Keyword.merge(
        [
          auth: :anonymous,
          host_principal: :host,
          tick_interval: 10,
          listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
        ],
        options
      )

    instance =
      start_supervised!({GravitonMQ, name: nil, runtime_supervisor_name: nil, mqtt: mqtt},
        id: make_ref()
      )

    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    {instance, port}
  end

  defp connect(port, client_id, properties \\ []) do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    on_exit(fn -> :gen_tcp.close(socket) end)

    send_packet(socket, %Connect{
      version: :v5,
      client_id: client_id,
      clean_start: true,
      properties: properties
    })

    [%Connack{reason_code: 0} = connack] = recv(socket, 1)
    {socket, connack}
  end

  # A fresh packet identifier per SUBSCRIBE: the broker releases an identifier
  # only after the SUBACK write result, so reuse could race into 0x82.
  defp subscribe(socket, filters, properties \\ []) do
    packet_id = rem(System.unique_integer([:positive]), 65_535) + 1

    send_packet(socket, %Subscribe{
      packet_id: packet_id,
      subscriptions: filters,
      properties: properties
    })

    [%Suback{packet_id: ^packet_id} = suback] = recv(socket, 1)
    suback
  end

  defp send_packet(socket, packet) do
    {:ok, encoded} = Packet.encode(packet)
    :ok = :gen_tcp.send(socket, encoded)
  end

  defp recv(socket, count), do: Enum.map(recv_frames(socket, count), &elem(&1, 0))

  # Returns `{packet, wire_bytes}` pairs; bytes after the last packet are kept
  # for the next read.
  defp recv_frames(socket, count),
    do: recv_frames(socket, count, Process.get({:mqtt_buffer, socket}, <<>>), [])

  defp recv_frames(socket, 0, buffer, acc) do
    Process.put({:mqtt_buffer, socket}, buffer)
    Enum.reverse(acc)
  end

  defp recv_frames(socket, count, buffer, acc) do
    case Packet.decode(buffer, :v5) do
      {:ok, packet, rest} ->
        raw = binary_part(buffer, 0, byte_size(buffer) - byte_size(rest))
        recv_frames(socket, count - 1, rest, [{packet, raw} | acc])

      {:more, _} ->
        {:ok, bytes} = :gen_tcp.recv(socket, 0, 2_000)
        recv_frames(socket, count, buffer <> bytes, acc)
    end
  end

  # Nothing more arrives, and nothing is left over from an earlier read: a
  # duplicate PUBLISH can share the TCP read of the packet before it.
  defp quiet(socket) do
    assert Process.get({:mqtt_buffer, socket}, <<>>) == <<>>
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 100)
  end

  defp status(instance) do
    {:ok, status} = GravitonMQ.status(instance)
    status
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts == 0 -> false
      true -> Process.sleep(20) && eventually(fun, attempts - 1)
    end
  end

  describe "CONNACK capabilities" do
    test "defaults advertise shared subscriptions, Subscription Identifiers and 10 aliases" do
      {_instance, port} = broker()
      {_socket, %Connack{properties: properties}} = connect(port, "caps-default")
      assert properties[:shared_subscription_available] == 1
      assert properties[:subscription_identifier_available] == 1
      assert properties[:wildcard_subscription_available] == 1
      assert properties[:topic_alias_maximum] == 10
    end

    test "each switch turns off only its own capability" do
      for {options, shared, sid, aliases} <- [
            {[shared_subscriptions: false], 0, 1, 10},
            {[subscription_identifiers: false], 1, 0, 10},
            {[topic_alias_maximum: 0], 1, 1, 0}
          ] do
        {_instance, port} = broker(options)
        {_socket, %Connack{properties: properties}} = connect(port, "caps-#{inspect(options)}")
        assert properties[:shared_subscription_available] == shared, inspect(options)
        assert properties[:subscription_identifier_available] == sid, inspect(options)
        assert properties[:topic_alias_maximum] == aliases, inspect(options)
      end
    end
  end

  describe "inbound Topic Alias" do
    test "aliases resolve for QoS0/1/2 and subscribers receive full topics" do
      {_instance, port} = broker()
      {sub, _} = connect(port, "raw-alias-sub")
      subscribe(sub, [{"alias/#", 2}])
      {pub, %Connack{properties: properties}} = connect(port, "raw-alias-pub")
      assert properties[:topic_alias_maximum] == 10

      send_packet(pub, %Publish{topic: "alias/t", payload: "0", properties: [topic_alias: 1]})

      send_packet(pub, %Publish{
        topic: "",
        payload: "1",
        qos: 1,
        packet_id: 1,
        properties: [topic_alias: 1]
      })

      assert [%Puback{packet_id: 1, reason_code: 0}] = recv(pub, 1)

      send_packet(pub, %Publish{
        topic: "",
        payload: "2",
        qos: 2,
        packet_id: 2,
        properties: [topic_alias: 1]
      })

      assert [%Pubrec{packet_id: 2, reason_code: 0}] = recv(pub, 1)
      send_packet(pub, %Pubrel{packet_id: 2})
      assert [%Pubcomp{packet_id: 2, reason_code: 0}] = recv(pub, 1)

      received = recv(sub, 3)

      assert Enum.sort(Enum.map(received, &{&1.topic, &1.payload})) ==
               [{"alias/t", "0"}, {"alias/t", "1"}, {"alias/t", "2"}]

      refute Enum.any?(received, &Keyword.has_key?(&1.properties, :topic_alias))
    end

    test "an out-of-range alias receives DISCONNECT 0x94 and an unknown alias 0x82" do
      {_instance, port} = broker()

      for {publication, code} <- [
            {%Publish{topic: "a", payload: "x", properties: [topic_alias: 11]}, 0x94},
            {%Publish{topic: "", payload: "x", properties: [topic_alias: 4]}, 0x82}
          ] do
        {socket, _} = connect(port, "raw-alias-bad-#{code}")
        send_packet(socket, publication)
        assert [%Disconnect{reason_code: ^code}] = recv(socket, 1)
        assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
      end
    end

    test "a Topic Alias Maximum of 0 refuses every alias with 0x94" do
      {_instance, port} = broker(topic_alias_maximum: 0)
      {socket, _} = connect(port, "raw-alias-off")
      send_packet(socket, %Publish{topic: "a", payload: "x", properties: [topic_alias: 1]})
      assert [%Disconnect{reason_code: 0x94}] = recv(socket, 1)
    end

    # Maximum Packet Size bounds the wire packet only (§3.1.2.11.4): restoring
    # the full topic may exceed the broker's receive limit.
    test "a resolved publication larger than the receive limit reaches the subscriber" do
      {_instance, port} = broker(max_packet_size: 100)
      topic = String.duplicate("t", 40)
      payload = String.duplicate("p", 60)
      {sub, _} = connect(port, "raw-alias-large-sub")
      subscribe(sub, [{topic, 1}])
      {pub, _} = connect(port, "raw-alias-large-pub")
      send_packet(pub, %Publish{topic: topic, payload: "", properties: [topic_alias: 1]})

      aliased = %Publish{
        topic: "",
        payload: payload,
        qos: 1,
        packet_id: 1,
        properties: [topic_alias: 1]
      }

      {:ok, wire} = Packet.encode(aliased)
      assert byte_size(wire) <= 100
      send_packet(pub, aliased)
      assert [%Puback{packet_id: 1, reason_code: 0}] = recv(pub, 1)

      assert [
               %Publish{topic: ^topic, payload: ""},
               %Publish{topic: ^topic, payload: ^payload} = large
             ] =
               recv(sub, 2)

      send_packet(sub, %Puback{packet_id: large.packet_id})
      quiet(pub)
    end

    test "a mapping is not carried across a reconnect" do
      {_instance, port} = broker()
      {first, _} = connect(port, "raw-alias-again")

      send_packet(first, %Publish{
        topic: "a",
        payload: "x",
        qos: 1,
        packet_id: 1,
        properties: [topic_alias: 1]
      })

      assert [%Puback{packet_id: 1}] = recv(first, 1)
      :ok = :gen_tcp.close(first)

      {second, _} = connect(port, "raw-alias-again")
      send_packet(second, %Publish{topic: "", payload: "y", properties: [topic_alias: 1]})
      assert [%Disconnect{reason_code: 0x82}] = recv(second, 1)
    end
  end

  describe "outbound Topic Alias" do
    test "is off by default even when the subscriber advertises a Topic Alias Maximum" do
      {_instance, port} = broker()
      {sub, _} = connect(port, "raw-out-default-sub", topic_alias_maximum: 4)
      subscribe(sub, [{"o/#", 0}])
      {pub, _} = connect(port, "raw-out-default-pub")

      for topic <- ["o/a", "o/a", "o/b"],
          do: send_packet(pub, %Publish{topic: topic, payload: topic})

      assert Enum.map(recv(sub, 3), &{&1.topic, &1.properties[:topic_alias]}) ==
               [{"o/a", nil}, {"o/a", nil}, {"o/b", nil}]
    end

    test "configured aliases are bounded by the subscriber's maximum and sent alias-only" do
      {instance, port} = broker(max_outbound_topic_aliases: 4)
      {sub, _} = connect(port, "raw-out-sub", topic_alias_maximum: 1)
      subscribe(sub, [{"o/#", 1}])
      {pub, _} = connect(port, "raw-out-pub")

      for {topic, id} <- Enum.with_index(["o/a", "o/a", "o/b"], 1) do
        send_packet(pub, %Publish{topic: topic, payload: topic, qos: 1, packet_id: id})
        assert [%Puback{packet_id: ^id}] = recv(pub, 1)
      end

      frames = recv_frames(sub, 3)

      assert Enum.map(frames, fn {packet, _} ->
               {packet.topic, packet.properties[:topic_alias]}
             end) ==
               [{"o/a", 1}, {"", 1}, {"o/b", nil}]

      # The alias-only PUBLISH carries a zero-length topic and Topic Alias 1.
      {_, alias_only} = Enum.at(frames, 1)
      assert <<0x32, _remaining_length, 0, 0, _::binary>> = alias_only
      assert :binary.match(alias_only, <<0x23, 0, 1>>) != :nomatch

      for {%Publish{packet_id: id}, _} <- frames, do: send_packet(sub, %Puback{packet_id: id})
      assert %{topic_aliases: %{inbound: 0, outbound: 1}} = status(instance)
    end
  end

  describe "Subscription Identifiers" do
    test "identifiers of overlapping subscriptions arrive on one delivered PUBLISH" do
      {_instance, port} = broker()
      {sub, _} = connect(port, "raw-sid-sub")
      subscribe(sub, [{"sid/#", 1}], subscription_identifier: 7)
      subscribe(sub, [{"sid/+/x", 1}], subscription_identifier: 9)
      {pub, _} = connect(port, "raw-sid-pub")
      send_packet(pub, %Publish{topic: "sid/a/x", payload: "x"})
      [{%Publish{properties: properties}, raw}] = recv_frames(sub, 1)
      assert Enum.sort(for {:subscription_identifier, id} <- properties, do: id) == [7, 9]
      # Property 0x0B with one-byte Variable Byte Integers 7 and 9 on the wire.
      assert :binary.match(raw, <<0x0B, 7>>) != :nomatch
      assert :binary.match(raw, <<0x0B, 9>>) != :nomatch
      quiet(sub)
    end

    test "the switch refuses a SUBSCRIBE carrying an identifier with DISCONNECT 0xA1" do
      {_instance, port} = broker(subscription_identifiers: false)
      {socket, _} = connect(port, "raw-sid-off")

      send_packet(socket, %Subscribe{
        packet_id: 1,
        subscriptions: [{"a", 1}],
        properties: [subscription_identifier: 1]
      })

      assert [%Disconnect{reason_code: 0xA1}] = recv(socket, 1)
    end
  end

  describe "shared subscriptions" do
    test "a group splits messages round-robin and status reports groups and alias tables" do
      {instance, port} = broker()
      {m1, _} = connect(port, "raw-m1")
      {m2, _} = connect(port, "raw-m2")
      subscribe(m1, [{"$share/g/s", 0}])
      subscribe(m2, [{"$share/g/s", 0}])
      {pub, _} = connect(port, "raw-share-pub")
      send_packet(pub, %Publish{topic: "s", payload: "seed", properties: [topic_alias: 3]})
      for payload <- ["1", "2", "3"], do: send_packet(pub, %Publish{topic: "s", payload: payload})

      # Members are ordered by Client Identifier after the round-robin cursor.
      assert Enum.map(recv(m1, 2), & &1.payload) == ["seed", "2"]
      assert Enum.map(recv(m2, 2), & &1.payload) == ["1", "3"]
      quiet(m1)
      quiet(m2)

      assert %{
               shared_groups: 1,
               shared_discarded: 0,
               shared_qos2_discarded: 0,
               topic_aliases: %{inbound: 1, outbound: 0}
             } = status(instance)
    end

    test "never-sent QoS1 copies of a disconnected member move to the connected member" do
      {instance, port} = broker()
      properties = [receive_maximum: 1, session_expiry_interval: 60]
      {m1, _} = connect(port, "raw-r1", properties)
      {m2, _} = connect(port, "raw-r2", properties)
      subscribe(m1, [{"$share/g/r", 1}])
      subscribe(m2, [{"$share/g/r", 1}])
      {pub, _} = connect(port, "raw-r-pub")

      for {payload, id} <- Enum.with_index(["x1", "x2", "x3", "x4"], 1) do
        send_packet(pub, %Publish{topic: "r", payload: payload, qos: 1, packet_id: id})
        assert [%Puback{packet_id: ^id, reason_code: 0}] = recv(pub, 1)
      end

      # Receive Maximum 1: each member holds one sent and one bound copy.
      assert [%Publish{payload: "x1"}] = recv(m1, 1)
      assert [%Publish{payload: "x2", packet_id: x2}] = recv(m2, 1)
      send_packet(m2, %Puback{packet_id: x2})
      assert [%Publish{payload: "x4", packet_id: x4}] = recv(m2, 1)
      send_packet(m2, %Puback{packet_id: x4})
      quiet(m2)

      # m1's sent x1 stays with its session (MQTT-4.8.2-4); unsent x3 moves.
      :ok = :gen_tcp.close(m1)
      assert [%Publish{payload: "x3", packet_id: x3, dup: false}] = recv(m2, 1)
      send_packet(m2, %Puback{packet_id: x3})
      quiet(m2)

      assert eventually(fn ->
               match?(%{connections: 2, pending_deliveries: 1}, status(instance))
             end)

      assert %{shared_groups: 1, shared_discarded: 0} = status(instance)
    end

    test "the shared switch keeps DISCONNECT 0x9E and the host API boundary" do
      {instance, port} = broker(shared_subscriptions: false)
      {socket, %Connack{properties: properties}} = connect(port, "raw-off")
      assert properties[:shared_subscription_available] == 0
      send_packet(socket, %Subscribe{packet_id: 1, subscriptions: [{"$share/g/a", 1}]})
      assert [%Disconnect{reason_code: 0x9E}] = recv(socket, 1)

      assert {:error, :invalid_subscription} =
               GravitonMQ.subscribe(instance, "$share/g/a", qos: 1)
    end

    test "No Local on a shared filter receives DISCONNECT 0x82" do
      {_instance, port} = broker()
      {socket, _} = connect(port, "raw-nolocal")
      filter = "$share/g/t"
      body = <<0, 1, 0, byte_size(filter)::16, filter::binary, 0b0000_0101>>
      :ok = :gen_tcp.send(socket, <<0x82, byte_size(body)>> <> body)
      assert [%Disconnect{reason_code: 0x82}] = recv(socket, 1)
    end
  end
end
