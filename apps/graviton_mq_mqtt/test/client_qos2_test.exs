defmodule GravitonMQ.MQTT.ClientQoS2Test do
  use ExUnit.Case, async: true
  alias GravitonMQ.MQTT.Client

  alias GravitonMQ.MQTT.Packet.{
    Connect,
    Connack,
    Publish,
    Pubrec,
    Pubrel,
    Pubcomp,
    Subscribe,
    Will
  }

  defp online(properties \\ [], options \\ []) do
    connect = %Connect{
      version: :v5,
      client_id: "qos2",
      clean_start: true,
      keep_alive: 10,
      properties: [session_expiry_interval: 60] ++ Keyword.get(options, :connect_properties, [])
    }

    {:ok, state} = Client.new(connect, Keyword.put_new(options, :reconnect_max, 2))
    {state, _} = Client.transition(state, :start)
    {state, _} = Client.transition(state, {:transport_up, 1, 0})

    {state, _} =
      Client.transition(
        state,
        {:packet, 1,
         %Connack{version: :v5, session_present: false, reason_code: 0, properties: properties},
         1}
      )

    state
  end

  defp packet(state, packet, now \\ 10),
    do: Client.transition(state, {:packet, state.generation, packet, now})

  defp publish(state, ref \\ :op, properties \\ []) do
    Client.transition(
      state,
      {:publish, ref, %Publish{topic: "a", payload: "x", qos: 2, properties: properties}, 2}
    )
  end

  defp resume(state, properties \\ [], now \\ 20) do
    {state, _} = Client.transition(state, {:transport_down, state.generation, :closed, now})
    {state, _} = Client.transition(state, {:retry, now + 1})

    {state, [{:send, connect, :connect}]} =
      Client.transition(state, {:transport_up, state.generation, now + 2})

    refute connect.clean_start

    packet(
      state,
      %Connack{version: :v5, session_present: true, reason_code: 0, properties: properties},
      now + 3
    )
  end

  test "QoS2 completes at PUBCOMP and positive PUBREC holds receive quota" do
    {state, [{:send, %Publish{qos: 2, packet_id: 1}, _}]} = publish(online(receive_maximum: 1))
    {state, [{:send, release, _}]} = packet(state, struct!(Pubrec, packet_id: 1))
    assert release == struct!(Pubrel, packet_id: 1)
    {^state, [{:notify, {:operation, :two, {:error, :inflight_full}}}]} = publish(state, :two)

    {state, [{:notify, {:operation, :op, {:ok, :pubcomp}}}]} =
      packet(state, struct!(Pubcomp, packet_id: 1))

    assert state.outbound == %{}
    assert {_, [{:send, %Publish{packet_id: 2}, _}]} = publish(state, :two)
  end

  test "negative PUBREC releases identifier quota without PUBREL or success" do
    {state, _} = publish(online(receive_maximum: 1))

    {state, [{:notify, {:operation, :op, {:error, {:pubrec, 0x97}}}}]} =
      packet(state, struct!(Pubrec, packet_id: 1, reason_code: 0x97))

    assert {_, [{:send, %Publish{}, _}]} = publish(state, :two)
  end

  test "reconnect before PUBREC resends original PUBLISH with DUP and after PUBREC only PUBREL" do
    {state, _} = publish(online(), :op, message_expiry_interval: 1)
    {state, effects} = resume(state, [], 2_000)
    assert Enum.any?(effects, &match?({:send, %Publish{qos: 2, packet_id: 1, dup: true}, _}, &1))
    {state, _} = packet(state, struct!(Pubrec, packet_id: 1))
    {_state, effects} = resume(state, [receive_maximum: 1, maximum_qos: 1], 3_000)
    assert Enum.any?(effects, &match?({:send, %{__struct__: Pubrel, packet_id: 1}, _}, &1))
    refute Enum.any?(effects, &match?({:send, %Publish{}, _}, &1))
  end

  test "PUBREL resumption and duplicate PUBREC proceed while fresh PUBLISH quota is exhausted" do
    {state, _} = publish(online(receive_maximum: 1))
    {state, _} = packet(state, struct!(Pubrec, packet_id: 1))
    {state, effects} = packet(state, struct!(Pubrec, packet_id: 1))
    assert [{:send, %{__struct__: Pubrel}, _}] = effects
    {state, effects} = resume(state, receive_maximum: 1)
    assert Enum.any?(effects, &match?({:send, %{__struct__: Pubrel}, _}, &1))
    assert {_, [{:send, %Publish{packet_id: 2}, _}]} = publish(state, :two)
  end

  test "inbound manual ownership ACK emits PUBREC and duplicate PUBLISH cannot redeliver" do
    state = online()
    {state, _} = Client.transition(state, {:demand, 2, 2})
    incoming = %Publish{topic: "a", payload: "x", qos: 2, packet_id: 9}
    {state, [{:notify, {:delivery, id, ^incoming}}]} = packet(state, incoming)
    {^state, []} = packet(state, %{incoming | dup: true})
    {state, [{:send, rec, _}]} = Client.transition(state, {:ack, id, 11})
    assert rec == struct!(Pubrec, packet_id: 9)
    {state, [{:send, ^rec, _}]} = packet(state, %{incoming | dup: true})
    {state, [{:send, comp, token}]} = packet(state, struct!(Pubrel, packet_id: 9))
    assert comp == struct!(Pubcomp, packet_id: 9)
    {state, _} = Client.transition(state, {:sent, state.generation, token, :ok, 12})
    assert state.incoming == %{}
    assert state.deliveries == %{}
    {state, [{:send, unknown, _}]} = packet(state, struct!(Pubrel, packet_id: 9))
    assert unknown == struct!(Pubcomp, packet_id: 9, reason_code: 0x92)
    assert {_, [{:notify, {:delivery, next, ^incoming}}]} = packet(state, incoming)
    assert next != id
  end

  test "identifier reuse is fenced against a late PUBCOMP write receipt" do
    {state, _} = Client.transition(online(), {:demand, 2, 2})
    incoming = %Publish{topic: "a", payload: "first", qos: 2, packet_id: 9}
    {state, [{:notify, {:delivery, first_id, _}}]} = packet(state, incoming)
    {state, _} = Client.transition(state, {:ack, first_id, 11})
    {state, [{:send, _, old_token}]} = packet(state, struct!(Pubrel, packet_id: 9))
    {state, [{:notify, {:delivery, new_id, _}}]} = packet(state, %{incoming | payload: "second"})
    {state, []} = Client.transition(state, {:sent, state.generation, old_token, :ok, 12})
    assert state.incoming[9] == new_id
    assert state.deliveries[new_id].packet.payload == "second"
  end

  test "expiry still finishes inbound QoS2 without an application delivery" do
    incoming = %Publish{
      topic: "a",
      payload: "expired",
      qos: 2,
      packet_id: 9,
      properties: [message_expiry_interval: 0]
    }

    {state, [{:send, %{__struct__: Pubrec}, _}]} = packet(online(), incoming)
    {_state, [{:send, %{__struct__: Pubcomp}, _}]} = packet(state, struct!(Pubrel, packet_id: 9))
  end

  test "inbound QoS2 ledger survives reconnect without delivering twice" do
    {state, _} = Client.transition(online(), {:demand, 1, 2})
    incoming = %Publish{topic: "a", payload: "x", qos: 2, packet_id: 9}
    {state, [{:notify, {:delivery, id, _}}]} = packet(state, incoming)
    {state, _} = Client.transition(state, {:ack, id, 11})
    {state, _} = resume(state)
    {state, effects} = packet(state, %{incoming | dup: true})
    assert [{:send, %{__struct__: Pubrec}, _}] = effects

    assert {_, [{:send, %{__struct__: Pubcomp}, _}]} =
             packet(state, struct!(Pubrel, packet_id: 9))
  end

  test "Receive Maximum counts QoS1 and QoS2 together until final acknowledgement" do
    state = online([], max_deliveries: 1)
    {state, _} = packet(state, %Publish{topic: "a", payload: "x", qos: 2, packet_id: 1})
    {_state, effects} = packet(state, %Publish{topic: "a", payload: "y", qos: 1, packet_id: 2})
    assert {:close, :receive_maximum_exceeded} in effects
  end

  test "QoS2 subscribe and Will are allowed but negotiated Maximum QoS still constrains publish" do
    connect = %Connect{
      version: :v5,
      client_id: "qos2-will",
      clean_start: true,
      will: %Will{topic: "a", payload: "x", qos: 2}
    }

    assert {:ok, _} = Client.new(connect)

    assert {_, [{:send, %Subscribe{}, _}]} =
             Client.transition(
               online(),
               {:subscribe, :sub, %Subscribe{packet_id: 0, subscriptions: [{"a", 2}]}, 2}
             )

    assert {_, [{:notify, {:operation, :op, {:error, :qos_not_supported}}}]} =
             publish(online(maximum_qos: 1))
  end

  test "lost session and operation timeout report unknown outcome rather than success" do
    {state, _} = publish(online())
    {_state, effects} = Client.transition(state, {:operation_timeout, :op, 100})
    assert {:notify, {:operation, :op, {:error, :unknown_outcome}}} in effects
    {state, _} = packet(state, struct!(Pubrec, packet_id: 1))
    {state, _} = Client.transition(state, {:transport_down, 1, :closed, 20})
    {state, _} = Client.transition(state, {:retry, 21})
    {state, _} = Client.transition(state, {:transport_up, 2, 22})

    {_state, effects} =
      packet(state, %Connack{version: :v5, session_present: false, reason_code: 0})

    assert {:notify, {:operation, :op, {:error, :unknown_outcome}}} in effects
  end

  test "old incoming QoS2 handshakes do not consume the new connection receive quota" do
    {state, _} =
      Client.transition(
        online([], max_deliveries: 3, connect_properties: [receive_maximum: 1]),
        {:demand, 3, 2}
      )

    first = %Publish{topic: "a", payload: "first", qos: 2, packet_id: 1}
    {state, [{:notify, {:delivery, first_id, _}}]} = packet(state, first)
    {state, _} = Client.transition(state, {:ack, first_id, 11})
    {state, _} = resume(state)
    # A new Receive Maximum applies to packets received on this connection.
    # An old PUBREL completes session state without reserving a PUBLISH slot.
    {state, [{:notify, {:delivery, _, _}}]} = packet(state, %{first | packet_id: 2})
    {state, [{:send, %{__struct__: Pubcomp}, _}]} = packet(state, struct!(Pubrel, packet_id: 1))
    {_state, effects} = packet(state, %{first | packet_id: 3})
    assert {:close, :receive_maximum_exceeded} in effects
  end

  test "replayed PUBLISH reserves fresh receive quota even when the delivery already exists" do
    {state, _} =
      packet(online([], max_deliveries: 3, connect_properties: [receive_maximum: 1]), %Publish{
        topic: "a",
        payload: "x",
        qos: 2,
        packet_id: 1
      })

    {state, _} = resume(state)

    {state, []} =
      packet(state, %Publish{topic: "a", payload: "x", qos: 2, packet_id: 1, dup: true})

    {_state, effects} = packet(state, %Publish{topic: "a", payload: "x", qos: 2, packet_id: 2})
    assert {:close, :receive_maximum_exceeded} in effects
  end

  test "a QoS1 publication may reuse an identifier after PUBCOMP before its late write receipt" do
    {state, _} = Client.transition(online(), {:demand, 2, 2})

    {state, [{:notify, {:delivery, first_id, _}}]} =
      packet(state, %Publish{topic: "a", payload: "first", qos: 2, packet_id: 9})

    {state, _} = Client.transition(state, {:ack, first_id, 11})
    {state, [{:send, _, old_token}]} = packet(state, struct!(Pubrel, packet_id: 9))

    {state, [{:notify, {:delivery, new_id, _}}]} =
      packet(state, %Publish{topic: "a", payload: "second", qos: 1, packet_id: 9})

    {state, []} = Client.transition(state, {:sent, state.generation, old_token, :ok, 12})
    assert state.incoming[9] == new_id
  end

  test "PUBREC and PUBREL ordering follow their corresponding incoming packets" do
    {state, _} = Client.transition(online(), {:demand, 2, 2})

    {state, [{:notify, {:delivery, first, _}}]} =
      packet(state, %Publish{topic: "a", payload: "1", qos: 2, packet_id: 1})

    {state, [{:notify, {:delivery, second, _}}]} =
      packet(state, %Publish{topic: "a", payload: "2", qos: 2, packet_id: 2})

    {state, []} = Client.transition(state, {:ack, second, 12})

    {state, []} =
      packet(state, %Publish{topic: "a", payload: "2", qos: 2, packet_id: 2, dup: true})

    {_state, effects} = Client.transition(state, {:ack, first, 13})

    assert [
             {:send, %{__struct__: Pubrec, packet_id: 1}, _},
             {:send, %{__struct__: Pubrec, packet_id: 2}, _}
           ] = effects

    {state, _} = publish(online(), :first)
    {state, _} = publish(state, :second)
    {state, _} = packet(state, struct!(Pubrec, packet_id: 2))
    {state, _} = packet(state, struct!(Pubrec, packet_id: 1))
    {_state, effects} = resume(state)
    releases = for {:send, %{__struct__: Pubrel, packet_id: id}, _} <- effects, do: id
    assert releases == [2, 1]
  end

  test "PUBCOMP negative result and illegal early PUBCOMP cannot report success" do
    {state, _} = publish(online())
    {_stopped, effects} = packet(state, struct!(Pubcomp, packet_id: 1))
    assert {:close, :invalid_pubcomp} in effects
    {state, _} = packet(state, struct!(Pubrec, packet_id: 1))
    {_state, effects} = packet(state, struct!(Pubcomp, packet_id: 1, reason_code: 0x92))
    assert {:notify, {:operation, :op, {:error, {:pubcomp, 0x92}}}} in effects
  end

  test "incoming QoS2 responses honor peer packet size" do
    {state, _} = Client.transition(online(maximum_packet_size: 3), {:demand, 1, 2})

    {state, [{:notify, {:delivery, id, _}}]} =
      packet(state, %Publish{topic: "a", payload: "x", qos: 2, packet_id: 1})

    {_state, effects} = Client.transition(state, {:ack, id, 11})
    assert {:close, :peer_packet_size} in effects
    {_state, effects} = packet(online(maximum_packet_size: 4), struct!(Pubrel, packet_id: 99))
    assert {:close, :peer_packet_size} in effects
  end
end
