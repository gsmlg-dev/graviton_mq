defmodule GravitonMQ.MQTT.ClientProblemInformationTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.MQTT.Client

  alias GravitonMQ.MQTT.Packet.{
    Connect,
    Connack,
    Disconnect,
    Publish,
    Puback,
    Pubrec,
    Pubrel,
    Pubcomp,
    Suback,
    Unsuback
  }

  defp online do
    {:ok, state} =
      Client.new(%Connect{
        version: :v5,
        client_id: "problem-info",
        clean_start: true,
        properties: [request_problem_information: 0]
      })

    {state, _} = Client.transition(state, :start)
    {state, _} = Client.transition(state, {:transport_up, state.generation, 0})

    {state, _} =
      Client.transition(
        state,
        {:packet, state.generation,
         %Connack{version: :v5, session_present: false, reason_code: 0}, 1}
      )

    state
  end

  test "Request Problem Information=0 rejects forbidden properties on every received ACK" do
    for module <- [Puback, Pubrec, Pubrel, Pubcomp, Suback, Unsuback],
        properties <- [[reason_string: "not requested"], [user_property: {"key", "value"}]] do
      state = online()
      packet = struct!(module, packet_id: 1, properties: properties)
      {stopped, effects} = Client.transition(state, {:packet, state.generation, packet, 2})
      assert stopped.phase == :stopped

      assert Enum.any?(
               effects,
               &match?({:send, %Disconnect{reason_code: 0x82}, :disconnect}, &1)
             ),
             inspect({module, effects})

      refute Enum.any?(effects, &match?({:notify, {:operation, _, {:ok, _}}}, &1))
    end
  end

  test "forbidden PUBACK properties cannot become successful publication completion" do
    state = online()

    {state, [{:send, publication, token}]} =
      Client.transition(
        state,
        {:publish, :operation, %Publish{topic: "a", payload: "b", qos: 1}, 2}
      )

    {state, _} = Client.transition(state, {:sent, state.generation, token, :ok, 3})

    {state, effects} =
      Client.transition(
        state,
        {:packet, state.generation,
         %Puback{packet_id: publication.packet_id, properties: [reason_string: "forbidden"]}, 4}
      )

    assert state.phase == :stopped
    refute {:notify, {:operation, :operation, {:ok, :puback}}} in effects
    assert {:send, %Disconnect{reason_code: 0x82}, :disconnect} in effects
  end

  test "PUBLISH User Properties remain allowed when problem information is disabled" do
    state = online()
    {state, _} = Client.transition(state, {:demand, 1, 2})

    {state, effects} =
      Client.transition(
        state,
        {:packet, state.generation,
         %Publish{topic: "a", payload: "b", properties: [user_property: {"key", "value"}]}, 3}
      )

    assert state.phase == :connected
    assert Enum.any?(effects, &match?({:notify, {:delivery, _, %Publish{}}}, &1))
  end
end
