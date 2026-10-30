defmodule GravitonMQ.MQTT.BrokerCheckpointTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Core.CheckpointCodec
  alias GravitonMQ.MQTT.{Broker, Broker.Checkpoint}
  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Connect, Publish, Pubrec, Pubrel, Subscribe, Will}

  defp new do
    {:ok, machine} = Broker.new()
    machine
  end

  defp allow(machine, effects, now) do
    Enum.reduce(effects, {machine, []}, fn
      {:authorize, id, request, _}, {machine, output} ->
        decision = if is_list(request), do: Enum.map(request, fn _ -> :ok end), else: {:ok, :user}

        {machine, effects} =
          Broker.transition(
            machine,
            {:auth_result, id, machine.operations[id].generation, decision, now}
          )

        {machine, effects} = allow(machine, effects, now)
        {machine, output ++ effects}

      effect, {machine, output} ->
        {machine, output ++ [effect]}
    end)
  end

  defp packet(machine, id, packet, now) do
    {:ok, bytes} = Packet.encode(packet)
    {machine, effects} = Broker.transition(machine, {:packet, id, 1, packet, bytes, now})
    allow(machine, effects, now)
  end

  defp connect(machine, id, options \\ [], now \\ 0) do
    {machine, []} = Broker.transition(machine, {:connection_open, id, 1, now})

    connect =
      struct!(
        Connect,
        Keyword.merge(
          [
            version: :v5,
            client_id: to_string(id),
            clean_start: true,
            properties: [session_expiry_interval: 60]
          ],
          options
        )
      )

    packet(machine, id, connect, now)
  end

  test "recovers session-owned QoS2 phases and exact content without live resources or credentials" do
    {machine, _} = connect(new(), :reader)

    {machine, _} =
      packet(machine, :reader, %Subscribe{packet_id: 1, subscriptions: [{"a", 2}]}, 0)

    {machine, _} =
      connect(machine, :writer, username: "private-name", password: "private-password")

    publication = %Publish{
      topic: "a",
      payload: "content",
      packet_id: 77,
      qos: 2,
      retain: true,
      properties: [message_expiry_interval: 10]
    }

    {machine, _} = packet(machine, :writer, publication, 1_000)
    [delivery] = machine.sessions["reader"].deliveries
    {machine, _} = packet(machine, :reader, %Pubrec{packet_id: delivery.packet_id}, 1_001)

    assert {:ok, snapshot} = Checkpoint.capture(machine, 2_000, 100_000)
    assert {:ok, bytes} = CheckpointCodec.encode(snapshot, max_bytes: 1_048_576)
    refute bytes =~ "private-name"
    refute bytes =~ "private-password"
    assert {:ok, recovered} = Checkpoint.restore(new(), snapshot, 40_000, 103_000)
    assert recovered.connections == %{}
    assert recovered.hosts == %{}
    assert recovered.operations == %{}
    assert recovered.writes == %{}
    assert recovered.sessions["reader"].subscriptions == machine.sessions["reader"].subscriptions
    assert recovered.sessions["writer"].incoming_qos2 == %{77 => true}

    assert [%{phase: :pubrel, packet_id: id, sending: false, dispatch: nil}] =
             recovered.sessions["reader"].deliveries

    assert id == delivery.packet_id
    assert recovered.retained["a"].encoded == machine.retained["a"].encoded
    assert recovered.retained["a"].expires_at == 46_000
    # A previously live connection is detected lost on recovery. It had no
    # offline deadline to resume; its expiry begins at that explicit detection.
    assert recovered.sessions["reader"].expires_at == 100_000
  end

  test "offline session, retained, and scheduled Will deadlines count downtime once" do
    will = %Will{
      topic: "will",
      payload: "gone",
      qos: 1,
      retain: true,
      properties: [will_delay_interval: 1]
    }

    {machine, _} =
      connect(new(), :writer, [properties: [session_expiry_interval: 2], will: will], 1_000)

    publication = %Publish{
      topic: "a",
      payload: "short",
      qos: 1,
      packet_id: 3,
      retain: true,
      properties: [message_expiry_interval: 1]
    }

    {machine, _} = packet(machine, :writer, publication, 1_000)

    {machine, _} =
      Broker.transition(machine, {:connection_down, :writer, 1, :transport_loss, 1_000})

    assert {:ok, snapshot} = Checkpoint.capture(machine, 1_200, 100_000)
    assert {:ok, recovered} = Checkpoint.restore(new(), snapshot, 50_000, 103_000)
    assert recovered.sessions["writer"].expires_at == 48_800
    assert [%{due: 47_800}] = Map.values(recovered.wills)
    {recovered, effects} = Broker.transition(recovered, {:tick, 50_000})
    assert recovered.sessions == %{}
    refute Map.has_key?(recovered.retained, "a")
    assert Enum.any?(effects, &match?({:authorize, _, _, _}, &1))
  end

  test "checkpoint restores reverse PUBREC order and rejects missing or forged release order" do
    {machine, _} = connect(new(), :reader)

    {machine, _} =
      packet(machine, :reader, %Subscribe{packet_id: 1, subscriptions: [{"a", 2}]}, 0)

    {machine, _} = connect(machine, :writer)

    machine =
      Enum.reduce(1..3, machine, fn id, machine ->
        publication = %Publish{topic: "a", payload: Integer.to_string(id), packet_id: id, qos: 2}
        elem(packet(machine, :writer, publication, 1), 0)
      end)

    {machine, _} = packet(machine, :reader, %Pubrec{packet_id: 2}, 2)
    {machine, _} = packet(machine, :reader, %Pubrec{packet_id: 1}, 3)
    assert {:ok, snapshot} = Checkpoint.capture(machine, 4, 100_000)
    assert {:ok, encoded} = CheckpointCodec.encode(snapshot)
    assert {:ok, snapshot} = CheckpointCodec.decode(encoded)
    assert {:ok, recovered} = Checkpoint.restore(new(), snapshot, 100, 100_010)

    {_recovered, effects} = connect(recovered, :reader, [clean_start: false], 101)
    releases = for {:send, :reader, _, _, %Pubrel{packet_id: id}} <- effects, do: id
    publications = for {:send, :reader, _, _, %Publish{packet_id: id}} <- effects, do: id
    assert releases == [2, 1]
    assert publications == [3]

    [first, second, third] = snapshot.state.sessions["reader"].deliveries

    for deliveries <- [
          [Map.delete(first, :release_order), second, third],
          [Map.put(first, :release_order, 0), second, third],
          [Map.put(first, :release_order, snapshot.state.next_id), second, third],
          [first, Map.put(second, :release_order, Map.get(first, :release_order)), third],
          [first, second, Map.put(third, :release_order, 1)]
        ] do
      forged = put_in(snapshot.state.sessions["reader"].deliveries, deliveries)

      assert {:error, :invalid_broker_checkpoint} =
               Checkpoint.restore(new(), forged, 100, 100_010)
    end
  end

  test "retries an interrupted Will authorization with its original message expiry" do
    will = %Will{
      topic: "will",
      payload: "gone",
      qos: 1,
      retain: true,
      properties: [message_expiry_interval: 2]
    }

    {machine, _} =
      connect(new(), :writer, [properties: [session_expiry_interval: 60], will: will], 0)

    {machine, _} = Broker.transition(machine, {:connection_down, :writer, 1, :transport_loss, 0})
    {machine, [_ | _]} = Broker.transition(machine, {:tick, 100})
    assert {:ok, snapshot} = Checkpoint.capture(machine, 200, 100_000)
    assert {:ok, recovered} = Checkpoint.restore(new(), snapshot, 50_000, 101_000)
    assert [%{status: :due, message: %{expires_at: 50_900}}] = Map.values(recovered.wills)
    assert recovered.operations == %{}
    {_recovered, effects} = Broker.transition(recovered, {:tick, 50_000})
    assert Enum.any?(effects, &match?({:authorize, _, _, _}, &1))
  end

  test "rejects schema corruption, unsafe principals, incompatible limits, and backward wall clocks" do
    {machine, _} = connect(new(), :writer)
    assert {:ok, snapshot} = Checkpoint.capture(machine, 0, 100_000)
    assert {:error, :wall_clock_regressed} = Checkpoint.restore(new(), snapshot, 1_000, 99_999)

    assert {:error, :invalid_broker_checkpoint} =
             Checkpoint.restore(new(), %{snapshot | version: 2}, 1_000, 100_000)

    assert {:error, :invalid_broker_checkpoint} =
             Checkpoint.restore(new(), Map.put(snapshot, :connections, %{}), 1_000, 100_000)

    {:ok, smaller} = Broker.new(max_sessions: 0)
    assert {:error, :checkpoint_limits} = Checkpoint.restore(smaller, snapshot, 1_000, 100_000)
    session = %{machine.sessions["writer"] | principal: self()}
    machine = %{machine | sessions: Map.put(machine.sessions, "writer", session)}
    assert {:error, :invalid_broker_checkpoint} = Checkpoint.capture(machine, 0, 100_000)
  end

  test "subscription byte admission uses the same metric during recovery" do
    {:ok, machine} = Broker.new(max_subscription_bytes: 1)
    {machine, _} = connect(machine, :reader)

    {machine, _} =
      packet(machine, :reader, %Subscribe{packet_id: 1, subscriptions: [{"a", 1}]}, 0)

    assert Map.has_key?(machine.sessions["reader"].subscriptions, "a")
    assert {:ok, snapshot} = Checkpoint.capture(machine, 0, 100_000)
    assert {:ok, _} = Checkpoint.restore(machine, snapshot, 1_000, 100_100)
  end
end
