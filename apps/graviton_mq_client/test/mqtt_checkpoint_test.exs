defmodule GravitonMQ.MQTTClient.CheckpointTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.Client.MQTT.Checkpoint
  alias GravitonMQ.MQTT.Client, as: Machine
  alias GravitonMQ.MQTT.Packet.{Connect, Publish}

  test "checkpoint excludes credentials and rebases all retained deadlines through downtime" do
    assert Code.ensure_loaded?(Checkpoint)

    connect = %Connect{
      version: :v5,
      clean_start: true,
      client_id: "persistent",
      username: "private-user",
      password: "private-pass",
      properties: [session_expiry_interval: 60]
    }

    {:ok, fresh} = Machine.new(connect)
    packet = %Publish{qos: 2, packet_id: 7, topic: "a", payload: "body"}

    entry = %{
      ref: 1,
      kind: :publish,
      packet: packet,
      order: 1,
      sent: true,
      qos2_phase: :await_pubcomp,
      release_order: 2,
      expiry_deadline: 9_000
    }

    delivery = %{
      packet: packet,
      delivered: true,
      ack_pending: true,
      ack_sent: true,
      received_at: 500,
      qos2_phase: :await_pubrel
    }

    machine = %{
      fresh
      | phase: :connected,
        session_available: true,
        ever_connected: true,
        outbound: %{1 => entry},
        incoming: %{7 => 1},
        deliveries: %{1 => delivery},
        subscriptions: %{"a/#" => 2},
        next_delivery_id: 2,
        next_order: 3
    }

    meta = %{next_operation: 2, deadlines: %{1 => 8_000}, results: %{}}
    assert {:ok, snapshot} = Checkpoint.capture(machine, meta, 1_000, 100_000)
    refute inspect(snapshot) =~ "private-user"
    refute inspect(snapshot) =~ "private-pass"
    assert {:ok, recovered, meta} = Checkpoint.restore(snapshot, fresh, -10_000, 103_000)
    assert recovered.phase == :idle
    assert recovered.outbound[1].qos2_phase == :await_pubcomp
    assert recovered.outbound[1].expiry_deadline == -5_000
    assert recovered.deliveries[1].received_at == -13_500
    assert recovered.deliveries[1].delivered
    assert recovered.subscriptions == %{"a/#" => 2}
    assert meta.deadlines[1] == -6_000
    assert recovered.disconnected_at == -10_000
  end

  test "checkpoint rejects runtime operation identities and mismatched client ownership" do
    assert Code.ensure_loaded?(Checkpoint)
    {:ok, fresh} = Machine.new(%Connect{version: :v5, clean_start: true, client_id: "one"})

    assert {:error, _} =
             Checkpoint.capture(
               %{fresh | outbound: %{make_ref() => %{}}},
               %{next_operation: 1, deadlines: %{}, results: %{}},
               0,
               0
             )

    assert {:ok, snapshot} =
             Checkpoint.capture(fresh, %{next_operation: 1, deadlines: %{}, results: %{}}, 0, 0)

    {:ok, other} = Machine.new(%Connect{version: :v5, clean_start: true, client_id: "two"})
    assert {:error, :client_identity_mismatch} = Checkpoint.restore(snapshot, other, 0, 0)
  end

  test "protocol obligations without a live host request survive a checkpoint" do
    alias GravitonMQ.MQTT.Packet.{Connack, Pubrec}

    {:ok, fresh} =
      Machine.new(%Connect{
        version: :v5,
        clean_start: true,
        client_id: "terminal",
        properties: [session_expiry_interval: 60]
      })

    {state, _} = Machine.transition(fresh, :start)
    {state, _} = Machine.transition(state, {:transport_up, 1, 0})

    {state, _} =
      Machine.transition(
        state,
        {:packet, 1, %Connack{version: :v5, session_present: false, reason_code: 0}, 1}
      )

    {state, _} =
      Machine.transition(state, {:publish, 1, %Publish{topic: "a", payload: "x", qos: 2}, 2})

    {state, _} = Machine.transition(state, {:packet, 1, %Pubrec{packet_id: 1}, 3})
    {state, _} = Machine.transition(state, {:stop, 4})
    assert %{host_completed: true} = state.outbound[1]

    meta = %{
      next_operation: 2,
      deadlines: %{},
      results: %{1 => %{kind: :publish, result: {:error, :unknown_outcome}}}
    }

    assert {:ok, snapshot} = Checkpoint.capture(state, meta, 4, 1000)
    assert {:ok, restored, _} = Checkpoint.restore(snapshot, fresh, 100, 1001)
    assert %{host_completed: true, qos2_phase: :await_pubcomp} = restored.outbound[1]

    assert {:error, :invalid_checkpoint_schema} =
             Checkpoint.restore(
               put_in(snapshot.machine.outbound[1].host_completed, :forged),
               fresh,
               100,
               1001
             )
  end

  test "an old online snapshot does not invent an earlier network disconnect" do
    alias GravitonMQ.MQTT.Packet.{Connack, Pubrec, Pubrel}

    {:ok, fresh} =
      Machine.new(%Connect{
        version: :v5,
        clean_start: true,
        client_id: "online",
        properties: [session_expiry_interval: 1]
      })

    {state, _} = Machine.transition(fresh, :start)
    {state, _} = Machine.transition(state, {:transport_up, 1, 0})

    {state, _} =
      Machine.transition(
        state,
        {:packet, 1, %Connack{version: :v5, session_present: false, reason_code: 0}, 1}
      )

    {state, _} =
      Machine.transition(state, {:publish, 1, %Publish{topic: "a", payload: "x", qos: 2}, 2})

    {state, _} = Machine.transition(state, {:packet, 1, %Pubrec{packet_id: 1}, 3})
    meta = %{next_operation: 2, deadlines: %{1 => 60_000}, results: %{}}
    assert {:ok, snapshot} = Checkpoint.capture(state, meta, 4, 1_000)
    assert snapshot.machine.disconnected_at == nil
    assert {:ok, restored, meta} = Checkpoint.restore(snapshot, fresh, 10_000, 6_000)
    assert meta.deadlines[1] == 64_996
    assert restored.disconnected_at == 10_000
    {restored, _} = Machine.transition(restored, :start)

    {restored, [{:send, connect, :connect}]} =
      Machine.transition(restored, {:transport_up, 1, 10_001})

    refute connect.clean_start

    {_state, effects} =
      Machine.transition(
        restored,
        {:packet, 1, %Connack{version: :v5, session_present: true, reason_code: 0}, 10_002}
      )

    assert Enum.any?(effects, &match?({:send, %Pubrel{packet_id: 1}, _}, &1))

    # A known offline instant still includes all downtime and expires normally.
    assert {:ok, offline} = Checkpoint.capture(%{state | disconnected_at: 3}, meta, 4, 1_000)
    assert {:ok, restored, _} = Checkpoint.restore(offline, fresh, 10_000, 6_000)
    {restored, _} = Machine.transition(restored, :start)
    {_, [{:send, connect, :connect}]} = Machine.transition(restored, {:transport_up, 1, 10_001})
    assert connect.clean_start
  end

  test "recovery rejects forged schema fields and inconsistent ownership indexes" do
    {:ok, fresh} = Machine.new(%Connect{version: :v5, clean_start: true, client_id: "one"})

    assert {:ok, snapshot} =
             Checkpoint.capture(
               fresh,
               %{next_operation: 1, deadlines: %{}, results: %{}},
               0,
               1000
             )

    for malformed <- [
          Map.put(snapshot, :credentials, "secret"),
          put_in(snapshot.machine.next_packet_id, 0),
          put_in(snapshot.machine.incoming, %{7 => 1}),
          put_in(snapshot.machine.subscriptions, %{"bad/#/tail" => 2}),
          put_in(snapshot.meta.deadlines, %{1 => 1000}),
          put_in(snapshot.meta.results, %{1 => %{kind: :publish, result: {:ok, :sent}}})
        ] do
      assert {:error, :invalid_checkpoint_schema} = Checkpoint.restore(malformed, fresh, 0, 1000)
    end
  end

  test "wall-clock rollback explicitly rejects recovery" do
    {:ok, fresh} = Machine.new(%Connect{version: :v5, clean_start: true, client_id: "one"})

    assert {:ok, snapshot} =
             Checkpoint.capture(
               fresh,
               %{next_operation: 1, deadlines: %{}, results: %{}},
               100,
               1000
             )

    assert {:error, :wall_clock_regressed} = Checkpoint.restore(snapshot, fresh, -100, 500)
  end
end
