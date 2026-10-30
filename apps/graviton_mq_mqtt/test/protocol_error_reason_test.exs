defmodule GravitonMQ.MQTT.ProtocolErrorReasonTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.{Broker, Client}
  alias GravitonMQ.MQTT.Codec.{Limits, Packet}
  alias GravitonMQ.MQTT.Packet.{Connect, Connack, Disconnect}

  defp client do
    {:ok, state} = Client.new(%Connect{version: :v5, client_id: "size-client", clean_start: true})
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

  defp broker do
    {:ok, state} = Broker.new()
    {state, []} = Broker.transition(state, {:connection_open, :peer, 1, 0})
    connect = %Connect{version: :v5, client_id: "size-peer", clean_start: true}
    {:ok, bytes} = Packet.encode(connect)

    {state, [{:authorize, operation, _, _}]} =
      Broker.transition(state, {:packet, :peer, 1, connect, bytes, 0})

    {state, _} = Broker.transition(state, {:auth_result, operation, 1, {:ok, :user}, 1})
    state
  end

  test "client preserves oversized packet diagnosis from the real decoder error" do
    assert {:error, %{class: :limit_exceeded, reason: :packet_too_large} = error} =
             Packet.decode(<<0x30, 127>>, :v5, %Limits{max_packet_size: 128})

    state = client()

    {state, effects} =
      Client.transition(
        state,
        {:protocol_error, state.generation, {:decode, error.class, error.reason}, 2}
      )

    assert state.phase == :stopped
    assert {:send, %Disconnect{reason_code: 0x95}, :disconnect} in effects
  end

  test "broker preserves oversized packet diagnosis from the real decoder error" do
    assert {:error, %{class: :limit_exceeded, reason: :packet_too_large} = error} =
             Packet.decode(<<0x30, 127>>, :v5, %Limits{max_packet_size: 128})

    {state, effects} =
      Broker.transition(broker(), {:protocol_error, :peer, 1, {error.class, error.reason}, 2})

    assert state.connections == %{}
    assert Enum.any?(effects, &match?({:send, :peer, 1, _, %Disconnect{reason_code: 0x95}}, &1))
    assert Enum.any?(effects, &match?({:close, :peer, 1, _}, &1))
  end

  test "malformed and unknown protocol errors retain their existing diagnosis" do
    for reason <- [
          {:malformed, :invalid_fixed_header},
          {:limit_exceeded, :field_too_large},
          :unknown
        ] do
      state = client()

      client_reason =
        case reason do
          {class, detail} -> {:decode, class, detail}
          other -> other
        end

      {state, effects} =
        Client.transition(state, {:protocol_error, state.generation, client_reason, 2})

      assert state.phase == :stopped
      assert {:send, %Disconnect{reason_code: 0x81}, :disconnect} in effects

      {state, effects} = Broker.transition(broker(), {:protocol_error, :peer, 1, reason, 2})
      assert state.connections == %{}
      assert Enum.any?(effects, &match?({:send, :peer, 1, _, %Disconnect{reason_code: 0x81}}, &1))
    end
  end
end
