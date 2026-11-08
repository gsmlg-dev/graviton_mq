defmodule GravitonMQ.MQTT.BrokerConnectHoldTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.MQTT.Broker

  alias GravitonMQ.MQTT.Packet.{
    Connect,
    Connack,
    Disconnect,
    Pingreq,
    Publish,
    Puback,
    Subscribe,
    Suback
  }

  defp new(options \\ []) do
    {:ok, state} = Broker.new(options)
    state
  end

  defp allowed(state, effects, now \\ 0) do
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

  defp attach(state, id, generation, options \\ []) do
    {state, []} = Broker.transition(state, {:connection_open, id, generation, 0})

    connect = %Connect{
      version: :v5,
      client_id: Keyword.get(options, :client_id, to_string(id)),
      clean_start: Keyword.get(options, :clean_start, true),
      properties: Keyword.get(options, :properties, [])
    }

    {state, effects} = Broker.transition(state, {:packet, id, generation, connect, <<16>>, 0})
    allowed(state, effects)
  end

  defp packet(state, id, generation, packet, now \\ 1) do
    {state, effects} = Broker.transition(state, {:packet, id, generation, packet, <<1, 2>>, now})
    allowed(state, effects, now)
  end

  defp index(effects, fun), do: Enum.find_index(effects, fun)

  defp pending_connect(state, id) do
    {state, []} = Broker.transition(state, {:connection_open, id, 1, 0})
    connect = %Connect{version: :v5, client_id: to_string(id), clean_start: true}

    {state, [{:authorize, op, {:authenticate, _}, _}]} =
      Broker.transition(state, {:packet, id, 1, connect, <<16>>, 1})

    {state, op}
  end

  describe "packets pipelined after CONNECT" do
    test "are held during authentication and replayed in order after CONNACK" do
      {state, op} = pending_connect(new(), :one)

      {state, []} =
        Broker.transition(
          state,
          {:packet, :one, 1, %Subscribe{packet_id: 1, subscriptions: [{"a", 1}]}, <<1>>, 2}
        )

      {state, []} =
        Broker.transition(
          state,
          {:packet, :one, 1, %Publish{topic: "a", payload: "x", qos: 1, packet_id: 2}, <<2>>, 3}
        )

      assert Map.has_key?(state.connections, :one)
      assert state.sessions == %{}

      {state, effects} = Broker.transition(state, {:auth_result, op, 1, {:ok, :user}, 4})
      {state, effects} = allowed(state, effects, 4)

      connack = index(effects, &match?({:send, :one, 1, _, %Connack{reason_code: 0}}, &1))
      suback = index(effects, &match?({:send, :one, 1, _, %Suback{reason_codes: [1]}}, &1))
      puback = index(effects, &match?({:send, :one, 1, _, %Puback{packet_id: 2}}, &1))

      assert connack < suback and suback < puback
      refute Enum.any?(effects, &match?({:close, _, _, _}, &1))
      assert state.connections[:one].phase == :connected
      assert state.connections[:one].held == []
      assert Map.has_key?(state.sessions["one"].subscriptions, "a")
    end

    test "are discarded unprocessed when CONNECT is rejected" do
      {state, op} = pending_connect(new(), :one)

      {state, []} =
        Broker.transition(
          state,
          {:packet, :one, 1, %Subscribe{packet_id: 1, subscriptions: [{"a", 1}]}, <<1>>, 2}
        )

      {state, []} =
        Broker.transition(
          state,
          {:packet, :one, 1, %Publish{topic: "a", payload: "x", qos: 1, packet_id: 2}, <<2>>, 3}
        )

      {state, effects} = Broker.transition(state, {:auth_result, op, 1, {:error, :denied}, 4})

      assert [
               {:send, :one, 1, _, %Connack{reason_code: 0x87}},
               {:close, :one, 1, :denied}
             ] = effects

      assert state.connections == %{}
      assert state.operations == %{}
      assert state.sessions == %{}
      assert state.retained == %{}
    end

    test "a held second CONNECT is a protocol error once the first is accepted" do
      {state, op} = pending_connect(new(), :one)
      connect = %Connect{version: :v5, client_id: "one", clean_start: true}
      {state, []} = Broker.transition(state, {:packet, :one, 1, connect, <<16>>, 2})
      {state, effects} = Broker.transition(state, {:auth_result, op, 1, {:ok, :user}, 3})

      assert [
               {:send, :one, 1, _, %Connack{reason_code: 0}},
               {:send, :one, 1, _, %Disconnect{reason_code: 0x82}},
               {:close, :one, 1, :unexpected_connect}
             ] = effects

      refute Map.has_key?(state.connections, :one)
    end

    test "overflowing the held packet count refuses CONNECT with Quota exceeded" do
      {state, op} = pending_connect(new(receive_maximum: 2), :one)
      {state, []} = Broker.transition(state, {:packet, :one, 1, %Pingreq{}, <<192, 0>>, 2})
      {state, []} = Broker.transition(state, {:packet, :one, 1, %Pingreq{}, <<192, 0>>, 3})
      {state, effects} = Broker.transition(state, {:packet, :one, 1, %Pingreq{}, <<192, 0>>, 4})

      assert [
               {:send, :one, 1, _, %Connack{reason_code: 0x97, session_present: false}},
               {:close, :one, 1, :held_packet_limit}
             ] = effects

      refute Enum.any?(effects, &match?({:send, _, _, _, %Disconnect{}}, &1))
      assert state.connections == %{}
      assert state.operations == %{}
      assert {^state, []} = Broker.transition(state, {:auth_result, op, 1, {:ok, :user}, 5})
    end

    test "overflowing the held packet bytes refuses CONNECT with Quota exceeded" do
      {state, _op} = pending_connect(new(max_packet_size: 100), :one)
      bytes = :binary.copy(<<0>>, 60)
      {state, []} = Broker.transition(state, {:packet, :one, 1, %Pingreq{}, bytes, 2})
      {state, effects} = Broker.transition(state, {:packet, :one, 1, %Pingreq{}, bytes, 3})

      assert [
               {:send, :one, 1, _, %Connack{reason_code: 0x97}},
               {:close, :one, 1, :held_packet_limit}
             ] = effects

      assert state.connections == %{}
    end

    test "the aggregate held byte budget spans connections" do
      state = new(max_incoming_bytes: 5)
      {state, _} = pending_connect(state, :one)
      {state, _} = pending_connect(state, :two)
      {state, []} = Broker.transition(state, {:packet, :one, 1, %Pingreq{}, <<1, 2, 3>>, 2})
      {state, effects} = Broker.transition(state, {:packet, :two, 1, %Pingreq{}, <<1, 2, 3>>, 3})
      assert [{:send, :two, 1, _, %Connack{reason_code: 0x97}}, {:close, :two, 1, _}] = effects
      assert Map.has_key?(state.connections, :one)
    end

    test "packets before CONNECT still close the connection" do
      {state, []} = Broker.transition(new(), {:connection_open, :one, 1, 0})
      {state, effects} = Broker.transition(state, {:packet, :one, 1, %Pingreq{}, <<192, 0>>, 1})
      assert [{:close, :one, 1, :expected_connect}] = effects
      assert state.connections == %{}
    end
  end

  describe "session takeover" do
    defp pending_dispatch(state) do
      {state, [{:authorize, publication, _, _}]} =
        Broker.transition(
          state,
          {:packet, :sender, 1, %Publish{topic: "a", payload: "x", qos: 1, packet_id: 9}, <<1>>,
           2}
        )

      {state, effects} = Broker.transition(state, {:auth_result, publication, 1, [:ok, :ok], 3})

      assert [{:authorize, dispatch, _, _}] =
               Enum.filter(effects, &match?({:authorize, _, _, _}, &1))

      assert [%{dispatch: ^dispatch}] = state.sessions["original"].deliveries
      state
    end

    test "of an expiry-zero session starts fresh and never resumes stale dispatch fences" do
      {state, _} = attach(new(), :sender, 1)
      {state, _} = attach(state, :original, 1)

      {state, _} =
        packet(state, :original, 1, %Subscribe{packet_id: 1, subscriptions: [{"a", 1}]})

      state = pending_dispatch(state)

      {state, effects} =
        attach(state, :replacement, 2, client_id: "original", clean_start: false)

      assert Enum.any?(effects, &match?({:close, :original, 1, :session_taken_over}, &1))

      assert Enum.any?(
               effects,
               &match?(
                 {:send, :replacement, 2, _, %Connack{reason_code: 0, session_present: false}},
                 &1
               )
             )

      session = state.sessions["original"]
      assert session.connection == {:replacement, 2}
      assert session.subscriptions == %{}
      assert session.deliveries == []

      {state, _} =
        packet(state, :replacement, 2, %Subscribe{packet_id: 1, subscriptions: [{"a", 1}]}, 4)

      {_state, effects} =
        packet(state, :sender, 1, %Publish{topic: "a", payload: "y", qos: 1, packet_id: 10}, 5)

      assert Enum.any?(
               effects,
               &match?({:send, :replacement, 2, _, %Publish{payload: "y", dup: false}}, &1)
             )
    end

    test "of a persistent session resumes the post-takeover state and delivers pending messages" do
      {state, _} = attach(new(), :sender, 1)
      {state, _} = attach(state, :original, 1, properties: [session_expiry_interval: 60])

      {state, _} =
        packet(state, :original, 1, %Subscribe{packet_id: 1, subscriptions: [{"a", 1}]})

      state = pending_dispatch(state)

      {state, effects} = Broker.transition(state, {:connection_open, :replacement, 2, 4})
      [] = effects

      connect = %Connect{
        version: :v5,
        client_id: "original",
        clean_start: false,
        properties: [session_expiry_interval: 60]
      }

      {state, [{:authorize, op, _, _}]} =
        Broker.transition(state, {:packet, :replacement, 2, connect, <<16>>, 4})

      {state, effects} = Broker.transition(state, {:auth_result, op, 2, {:ok, :user}, 5})

      assert Enum.any?(effects, &match?({:close, :original, 1, :session_taken_over}, &1))

      assert Enum.any?(
               effects,
               &match?(
                 {:send, :replacement, 2, _, %Connack{reason_code: 0, session_present: true}},
                 &1
               )
             )

      assert [{:authorize, dispatch, _, _}] =
               Enum.filter(effects, &match?({:authorize, _, _, _}, &1))

      assert [%{dispatch: ^dispatch, sending: false}] = state.sessions["original"].deliveries

      {state, effects} =
        allowed(state, effects |> Enum.filter(&match?({:authorize, _, _, _}, &1)), 6)

      assert Enum.any?(effects, &match?({:send, :replacement, 2, _, %Publish{payload: "x"}}, &1))
      assert Map.has_key?(state.sessions["original"].subscriptions, "a")
    end

    test "principal mismatch is refused before takeover in either session lifetime" do
      for properties <- [[], [session_expiry_interval: 60]] do
        {state, _} = attach(new(), :original, 1, properties: properties)
        {state, []} = Broker.transition(state, {:connection_open, :attacker, 2, 1})
        connect = %Connect{version: :v5, client_id: "original", clean_start: false}

        {state, [{:authorize, op, _, _}]} =
          Broker.transition(state, {:packet, :attacker, 2, connect, <<16>>, 1})

        {state, effects} = Broker.transition(state, {:auth_result, op, 2, {:ok, :other}, 2})

        assert Enum.any?(
                 effects,
                 &match?({:send, :attacker, 2, _, %Connack{reason_code: 0x87}}, &1)
               )

        refute Enum.any?(effects, &match?({:close, :original, _, _}, &1))
        assert state.sessions["original"].connection == {:original, 1}
      end
    end

    test "takeover at the session limit replaces rather than adds a session" do
      for properties <- [[], [session_expiry_interval: 60]] do
        {state, _} = attach(new(max_sessions: 1), :original, 1, properties: properties)

        {state, effects} =
          attach(state, :replacement, 2, client_id: "original", clean_start: false)

        assert Enum.any?(
                 effects,
                 &match?({:send, :replacement, 2, _, %Connack{reason_code: 0}}, &1)
               )

        assert map_size(state.sessions) == 1
        assert state.sessions["original"].connection == {:replacement, 2}
      end
    end
  end
end
