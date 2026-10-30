defmodule GravitonMQ.MQTT.ClientTerminalTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.MQTT.Client

  alias GravitonMQ.MQTT.Packet.{
    Connect,
    Connack,
    Disconnect,
    Puback,
    Pubcomp,
    Pubrec,
    Pubrel,
    Publish
  }

  for event <- [:stop, :transport_down, :disconnect, :timeout, :retry_exhaustion],
      qos <- [1, 2] do
    @event event
    @qos qos
    test "#{event} preserves QoS#{qos} protocol ownership after the host result" do
      state = online()

      {state, _} =
        Client.transition(
          state,
          {:publish, :op, %Publish{topic: "a", payload: "x", qos: @qos}, 2}
        )

      {state, _} = Client.transition(state, {:sent, 1, {:operation, :op}, :ok, 3})

      state =
        if @qos == 2,
          do: elem(Client.transition(state, {:packet, 1, %Pubrec{packet_id: 1}, 4}), 0),
          else: state

      event =
        case @event do
          :stop -> {:stop, 10}
          :transport_down -> {:transport_down, 1, :closed, 10}
          :disconnect -> {:packet, 1, %Disconnect{reason_code: 0x80}, 10}
          :timeout -> {:operation_timeout, :op, 10}
          :retry_exhaustion -> {:transport_down, 2, :refused, 10}
        end

      state =
        if @event == :retry_exhaustion do
          {state, _} =
            Client.transition(%{state | reconnect_max: 1}, {:transport_down, 1, :closed, 8})

          {state, _} = Client.transition(state, {:retry, 9})
          state
        else
          state
        end

      {state, effects} = Client.transition(state, event)
      assert state.phase == :stopped
      assert %{host_completed: true} = state.outbound[:op]
      assert {:notify, {:operation, :op, {:error, :unknown_outcome}}} in effects
      {state, effects} = Client.transition(state, {:stop, 11})
      refute Enum.any?(effects, &match?({:notify, {:operation, _, _}}, &1))

      # Checkpoint recovery supplies fresh transport ownership, preserving only session state.
      state = %{state | phase: :idle, generation: 0, disconnected_at: 10}
      {state, _} = Client.transition(state, :start)
      {state, [{:send, connect, :connect}]} = Client.transition(state, {:transport_up, 1, 12})
      refute connect.clean_start

      {state, effects} =
        Client.transition(
          state,
          {:packet, 1, %Connack{version: :v5, session_present: true, reason_code: 0}, 13}
        )

      if @qos == 2 do
        assert Enum.any?(effects, &match?({:send, %Pubrel{packet_id: 1}, _}, &1))
        refute Enum.any?(effects, &match?({:send, %Publish{}, _}, &1))
      else
        assert Enum.any?(effects, &match?({:send, %Publish{packet_id: 1, dup: true}, _}, &1))
      end

      acknowledgement = if @qos == 2, do: %Pubcomp{packet_id: 1}, else: %Puback{packet_id: 1}
      result = if @qos == 2, do: :pubcomp, else: :puback
      {state, effects} = Client.transition(state, {:packet, 1, acknowledgement, 14})
      assert state.outbound == %{}
      assert {:notify, {:protocol_complete, :op, {:ok, result}}} in effects
      refute Enum.any?(effects, &match?({:notify, {:operation, _, _}}, &1))
    end
  end

  defp online do
    {:ok, state} =
      Client.new(%Connect{
        version: :v5,
        client_id: "terminal",
        clean_start: true,
        properties: [session_expiry_interval: 60]
      })

    {state, _} = Client.transition(state, :start)
    {state, _} = Client.transition(state, {:transport_up, 1, 0})

    {state, _} =
      Client.transition(
        state,
        {:packet, 1, %Connack{version: :v5, session_present: false, reason_code: 0}, 1}
      )

    state
  end

  test "a terminal protocol obligation does not outlive the negotiated session" do
    state = online()

    {state, _} =
      Client.transition(state, {:publish, :op, %Publish{topic: "a", payload: "x", qos: 2}, 2})

    {state, _} = Client.transition(state, {:packet, 1, %Pubrec{packet_id: 1}, 3})
    {state, _} = Client.transition(state, {:stop, 10})
    state = %{state | phase: :idle, generation: 0}
    {state, _} = Client.transition(state, :start)
    {state, [{:send, connect, :connect}]} = Client.transition(state, {:transport_up, 1, 60_010})
    assert connect.clean_start

    {state, effects} =
      Client.transition(
        state,
        {:packet, 1, %Connack{version: :v5, session_present: false, reason_code: 0}, 60_011}
      )

    assert state.outbound == %{}
    refute Enum.any?(effects, &match?({:send, _, _}, &1))
    refute Enum.any?(effects, &match?({:notify, {:operation, _, _}}, &1))
  end

  test "protocol-only identifiers remain reserved and late host deadlines cannot stop recovery" do
    state = online()

    {state, _} =
      Client.transition(state, {:publish, :old, %Publish{topic: "a", payload: "x", qos: 2}, 2})

    {state, _} = Client.transition(state, {:packet, 1, %Pubrec{packet_id: 1}, 3})
    {state, _} = Client.transition(state, {:stop, 4})
    {state, _} = Client.transition(%{state | phase: :idle, generation: 0}, :start)
    {state, _} = Client.transition(state, {:transport_up, 1, 5})

    {state, _} =
      Client.transition(
        state,
        {:packet, 1, %Connack{version: :v5, session_present: true, reason_code: 0}, 6}
      )

    assert {^state, []} = Client.transition(state, {:operation_timeout, :old, 7})

    {state, [{:send, first, _}]} =
      Client.transition(
        %{state | next_packet_id: 1},
        {:publish, :fresh, %Publish{topic: "a", payload: "fresh", qos: 1}, 8}
      )

    assert first.packet_id == 2
    {state, _} = Client.transition(state, {:packet, 1, %Pubcomp{packet_id: 1}, 9})

    {_state, [{:send, second, _}]} =
      Client.transition(
        %{state | next_packet_id: 1},
        {:publish, :next, %Publish{topic: "a", payload: "next", qos: 1}, 10}
      )

    assert second.packet_id == 1
  end
end
