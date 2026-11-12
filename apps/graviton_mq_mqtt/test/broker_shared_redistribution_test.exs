defmodule GravitonMQ.MQTT.BrokerSharedRedistributionTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.{Broker, Broker.Checkpoint}
  alias GravitonMQ.MQTT.Packet.{Connect, Puback, Pubcomp, Publish, Pubrec, Pubrel}
  alias GravitonMQ.MQTT.Packet.{Subscribe, Unsubscribe}

  defp allowed(state, effects, now) do
    Enum.reduce(effects, {state, []}, fn
      {:authorize, op, request, _deadline}, {state, effects} ->
        result = if is_list(request), do: Enum.map(request, fn _ -> :ok end), else: {:ok, :user}

        {state, more} =
          Broker.transition(
            state,
            {:auth_result, op, Map.fetch!(state.operations, op).generation, result, now}
          )

        {state, effects ++ more}

      effect, {state, effects} ->
        {state, effects ++ [effect]}
    end)
    |> then(fn {state, effects} ->
      if Enum.any?(effects, &match?({:authorize, _, _, _}, &1)),
        do: allowed(state, effects, now),
        else: {state, effects}
    end)
  end

  defp attach_as(state, id, client_id, properties, clean_start \\ true, now \\ 0) do
    {state, []} = Broker.transition(state, {:connection_open, id, 1, now})

    connect = %Connect{
      version: :v5,
      client_id: client_id,
      clean_start: clean_start,
      properties: properties
    }

    {state, effects} = Broker.transition(state, {:packet, id, 1, connect, <<16, 0>>, now})
    allowed(state, effects, now)
  end

  defp packet(state, id, packet, now \\ 1) do
    {state, effects} = Broker.transition(state, {:packet, id, 1, packet, <<1, 2>>, now})
    allowed(state, effects, now)
  end

  defp payloads(state, client_id),
    do: Enum.map(state.sessions[client_id].deliveries, & &1.message.payload) |> Enum.sort()

  defp publishes(effects, id), do: for({:send, ^id, _, _, %Publish{} = p} <- effects, do: p)

  # m1: x1 sent and unacknowledged, x3 bound but unsent.
  # m2: x2 sent and unacknowledged, x4 bound but unsent.
  defp two_members(m1_expiry, qos \\ [1, 1, 1, 1]) do
    {:ok, state} = Broker.new()
    {state, _} = attach_as(state, :pub, "pub", [])

    {state, _} =
      attach_as(state, :m1, "m1", receive_maximum: 1, session_expiry_interval: m1_expiry)

    {state, _} = attach_as(state, :m2, "m2", receive_maximum: 1, session_expiry_interval: 60)

    {state, _} =
      packet(state, :m1, %Subscribe{packet_id: 1, subscriptions: [{"$share/g/t", 2}]})

    {state, _} =
      packet(state, :m2, %Subscribe{packet_id: 1, subscriptions: [{"$share/g/t", 2}]})

    Enum.zip(["x1", "x2", "x3", "x4"], qos)
    |> Enum.with_index(1)
    |> Enum.reduce(state, fn {{payload, qos}, index}, state ->
      publication = %Publish{topic: "t", payload: payload, qos: qos, packet_id: index}
      {state, _} = packet(state, :pub, publication)
      state
    end)
  end

  # Acts as m2: acknowledges every PUBLISH/PUBREL addressed to it until the
  # broker sends nothing more, and returns every PUBLISH in arrival order.
  defp drain_m2(state, effects, now, received \\ []) do
    replies =
      for {:send, :m2, _, _, packet} <- effects,
          reply =
            (case packet do
               %Publish{qos: 1, packet_id: id} -> %Puback{packet_id: id}
               %Publish{qos: 2, packet_id: id} -> %Pubrec{packet_id: id}
               %Pubrel{packet_id: id} -> %Pubcomp{packet_id: id}
               _ -> nil
             end),
          reply != nil,
          do: reply

    received = received ++ publishes(effects, :m2)

    if replies == [] do
      {state, received}
    else
      {state, effects} =
        Enum.reduce(replies, {state, []}, fn reply, {state, acc} ->
          {state, more} = packet(state, :m2, reply, now)
          {state, acc ++ more}
        end)

      drain_m2(state, effects, now, received)
    end
  end

  test "the fixture binds two sent and two unsent entries across the group" do
    state = two_members(60)
    assert payloads(state, "m1") == ["x1", "x3"]
    assert payloads(state, "m2") == ["x2", "x4"]
    assert Enum.find(state.sessions["m1"].deliveries, &(&1.message.payload == "x1")).ever_sent
    refute Enum.find(state.sessions["m1"].deliveries, &(&1.message.payload == "x3")).ever_sent
  end

  test "connection loss moves only never-sent entries to a connected member (Q6)" do
    {state, effects} =
      Broker.transition(two_members(60), {:connection_down, :m1, 1, :abnormal, 2})

    # D5: a pure state change; the next pump dispatches the moved entry.
    assert effects == []
    assert payloads(state, "m1") == ["x1"]
    assert payloads(state, "m2") == ["x2", "x3", "x4"]

    moved = Enum.find(state.sessions["m2"].deliveries, &(&1.message.payload == "x3"))
    assert %{packet_id: nil, ever_sent: false, shared: "$share/g/t", phase: :publish} = moved
  end

  # m2 drops first: its never-sent x4 moves to the still-connected m1. When m1
  # then drops no member is connected, so its never-sent entries stay.
  test "with no connected member a never-sent entry stays with the disconnected member" do
    state = two_members(60)
    {state, _} = Broker.transition(state, {:connection_down, :m2, 1, :abnormal, 2})
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 3})
    assert payloads(state, "m1") == ["x1", "x3", "x4"]
    assert payloads(state, "m2") == ["x2"]
    assert state.shared_discarded == 0
  end

  for {name, trigger} <- [
        {"expiry-0 connection loss", :expiry_zero_loss},
        {"session expiry on tick", :expiry_tick},
        {"clean-start reconnect", :clean_start},
        {"expiry-0 takeover", :takeover}
      ] do
    @trigger trigger
    test "session end by #{name} moves all QoS1 entries to another member (§4.8.2)" do
      state = end_m1(@trigger)
      assert payloads(state, "m2") == ["x1", "x2", "x3", "x4"]
      assert state.shared_discarded == 0
    end
  end

  defp end_m1(:expiry_zero_loss) do
    {state, _} = Broker.transition(two_members(0), {:connection_down, :m1, 1, :abnormal, 2})
    state
  end

  defp end_m1(:expiry_tick) do
    {state, _} = Broker.transition(two_members(1), {:connection_down, :m1, 1, :abnormal, 2})
    {state, _} = Broker.transition(state, {:tick, 5_000})
    state
  end

  defp end_m1(:clean_start) do
    {state, _} = Broker.transition(two_members(60), {:connection_down, :m1, 1, :abnormal, 2})
    {state, _} = attach_as(state, :m1_again, "m1", [session_expiry_interval: 60], true, 3)
    state
  end

  defp end_m1(:takeover) do
    {state, _} = attach_as(two_members(0), :m1_again, "m1", [], false, 3)
    state
  end

  test "an already-sent QoS2 entry is never sent to another member (MQTT-4.8.2-5)" do
    state = two_members(0, [2, 2, 1, 2])
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    refute "x1" in payloads(state, "m2")
    assert payloads(state, "m2") == ["x2", "x3", "x4"]
  end

  test "a sent QoS2 entry in PUBREL phase is discarded at session end, never moved" do
    state = two_members(0, [2, 1, 1, 1])
    [x1] = Enum.filter(state.sessions["m1"].deliveries, &(&1.message.payload == "x1"))
    {state, _} = packet(state, :m1, %Pubrec{packet_id: x1.packet_id}, 2)
    assert [%{phase: :pubrel}] = Enum.filter(state.sessions["m1"].deliveries, &(&1.id == x1.id))
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 3})
    assert payloads(state, "m2") == ["x2", "x3", "x4"]
  end

  test "an entry nobody can take is deleted and counted" do
    {:ok, state} = Broker.new()
    {state, _} = attach_as(state, :pub, "pub", [])
    {state, _} = attach_as(state, :m1, "m1", receive_maximum: 1, session_expiry_interval: 0)

    {state, _} =
      packet(state, :m1, %Subscribe{packet_id: 1, subscriptions: [{"$share/g/t", 1}]})

    {state, _} = packet(state, :pub, %Publish{topic: "t", payload: "a", qos: 1, packet_id: 1})
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    refute Map.has_key?(state.sessions, "m1")
    assert state.shared_discarded == 1
  end

  test "UNSUBSCRIBE moves never-sent entries and keeps sent ones with the member" do
    state = two_members(60)

    {state, effects} =
      packet(state, :m1, %Unsubscribe{packet_id: 9, filters: ["$share/g/t"]}, 2)

    assert payloads(state, "m1") == ["x1"]
    assert payloads(state, "m2") == ["x2", "x3", "x4"]
    assert is_list(effects)

    # The member that left still completes its sent entry (§3.10.4).
    [x1] = state.sessions["m1"].deliveries
    {state, _} = packet(state, :m1, %Puback{packet_id: x1.packet_id}, 3)
    assert state.sessions["m1"].deliveries == []
  end

  test "a negative PUBACK discards the entry and never re-routes it (MQTT-4.8.2-6)" do
    state = two_members(60)
    [x1] = Enum.filter(state.sessions["m1"].deliveries, &(&1.message.payload == "x1"))
    {state, _} = packet(state, :m1, %Puback{packet_id: x1.packet_id, reason_code: 0x80}, 2)
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 3})
    refute "x1" in payloads(state, "m2")
  end

  # Ordering guarantee: a moved entry is appended to the receiving member's
  # queue with a fresh entry identifier, so it is sent after every entry
  # already bound to that member; moved entries keep their relative order.
  # No order is promised across members (MQTT-4.6.0-6 covers non-shared only).
  test "moved entries are sent after the receiver's own entries, one at a time within Receive Maximum" do
    {state, []} = Broker.transition(two_members(60), {:connection_down, :m1, 1, :abnormal, 2})
    {state, effects} = Broker.transition(state, {:tick, 3})
    assert publishes(effects, :m2) == []

    [x2] = Enum.filter(state.sessions["m2"].deliveries, &(&1.message.payload == "x2"))
    {state, effects} = packet(state, :m2, %Puback{packet_id: x2.packet_id}, 4)
    assert [%Publish{payload: "x4", dup: false} = x4] = publishes(effects, :m2)

    {state, effects} = packet(state, :m2, %Puback{packet_id: x4.packet_id}, 5)
    assert [%Publish{payload: "x3", dup: false, qos: 1}] = publishes(effects, :m2)
    assert Enum.count(state.sessions["m2"].deliveries, & &1.ever_sent) == 1
  end

  test "a sent entry resumes with the same member and the DUP flag after reconnect (MQTT-4.8.2-4)" do
    {state, _} = Broker.transition(two_members(60), {:connection_down, :m1, 1, :abnormal, 2})
    [x1] = state.sessions["m1"].deliveries
    {_state, effects} = attach_as(state, :m1_again, "m1", [session_expiry_interval: 60], false, 3)

    assert [%Publish{payload: "x1", dup: true, packet_id: packet_id}] =
             publishes(effects, :m1_again)

    assert packet_id == x1.packet_id
  end

  test "at-least-once: every QoS1/2 publication reaches a member across a session end" do
    state = two_members(0, [1, 2, 2, 1])

    state =
      Enum.reduce(5..8, state, fn index, state ->
        publication = %Publish{topic: "t", payload: "x#{index}", qos: 1, packet_id: index}
        {state, _} = packet(state, :pub, publication)
        state
      end)

    sent_to_m1 = for entry <- state.sessions["m1"].deliveries, entry.ever_sent, do: entry

    {state, []} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 10})
    refute Map.has_key?(state.sessions, "m1")
    {state, effects} = Broker.transition(state, {:tick, 11})

    # m2's already-sent x2 is still awaiting its PUBREC from the fixture.
    [x2] = Enum.filter(state.sessions["m2"].deliveries, &(&1.message.payload == "x2"))
    {state, more} = packet(state, :m2, %Pubrec{packet_id: x2.packet_id}, 12)
    {state, received} = drain_m2(state, effects ++ more, 12)

    assert state.sessions["m2"].deliveries == []
    assert state.shared_discarded == 0
    # Only m1's sent QoS1 x1 was in flight there; it moves (no QoS2 was sent to m1).
    assert Enum.map(sent_to_m1, & &1.message.payload) == ["x1"]

    assert Enum.sort(Enum.map(received, & &1.payload)) ==
             ["x1", "x3", "x4", "x5", "x6", "x7", "x8"]
  end

  test "redistributed responsibility survives a checkpoint and recovery is effect-free" do
    {state, _} = Broker.transition(two_members(60), {:connection_down, :m1, 1, :abnormal, 2})
    assert {:ok, snapshot} = Checkpoint.capture(state, 2, 100_000)
    {:ok, template} = Broker.new()
    # Both sessions recover offline; m2's loss is detected at recovery with no
    # connected member, so its never-sent entries stay with it.
    assert {:ok, recovered} = Checkpoint.restore(template, snapshot, 1_000, 100_100)
    assert payloads(recovered, "m1") == ["x1"]
    assert payloads(recovered, "m2") == ["x2", "x3", "x4"]

    assert Enum.all?(recovered.sessions["m2"].deliveries, &(&1.shared == "$share/g/t"))
    assert recovered.shared_discarded == 0
  end
end
