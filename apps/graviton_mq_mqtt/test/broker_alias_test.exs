defmodule GravitonMQ.MQTT.BrokerAliasTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.{Broker, Broker.Checkpoint}
  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Disconnect, Publish, Puback, Subscribe}

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

  defp attach_as(state, id, client_id, options \\ []) do
    {state, []} = Broker.transition(state, {:connection_open, id, 1, 0})

    connect = %Connect{
      version: :v5,
      client_id: client_id,
      clean_start: Keyword.get(options, :clean_start, true),
      properties: Keyword.get(options, :properties, [])
    }

    {state, effects} = Broker.transition(state, {:packet, id, 1, connect, <<16, 0>>, 0})
    allowed(state, effects)
  end

  defp attach(state, id, options \\ []), do: attach_as(state, id, to_string(id), options)

  defp packet(state, id, packet, now \\ 1) do
    {state, effects} = Broker.transition(state, {:packet, id, 1, packet, <<1, 2>>, now})
    allowed(state, effects, now)
  end

  defp subscribe(state, id, filters) do
    packet(state, id, %Subscribe{
      packet_id: rem(state.next_id, 65_535) + 1,
      subscriptions: filters
    })
  end

  defp publishes(effects, id), do: for({:send, ^id, _, _, %Publish{} = p} <- effects, do: p)

  defp disconnected?(effects, id, code),
    do: Enum.any?(effects, &match?({:send, ^id, _, _, %Disconnect{reason_code: ^code}}, &1))

  test "CONNACK advertises the configured inbound Topic Alias Maximum" do
    for {options, expected} <- [
          {[], 10},
          {[topic_alias_maximum: 0], 0},
          {[topic_alias_maximum: 65_535, max_alias_bytes: 9_000_000_000_000], 65_535}
        ] do
      {_state, effects} = attach(new(options), :one)

      assert [{:send, :one, 1, _, %Connack{properties: properties}}] =
               Enum.filter(effects, &match?({:send, _, _, _, %Connack{}}, &1))

      assert properties[:topic_alias_maximum] == expected
    end
  end

  test "alias limits are validated and statically bounded by max_alias_bytes" do
    assert {:error, :invalid_options} = Broker.new(topic_alias_maximum: 65_536)
    assert {:error, :invalid_options} = Broker.new(max_outbound_topic_aliases: 65_536)
    assert {:error, :invalid_options} = Broker.new(max_alias_bytes: 1)
    assert {:ok, _} = Broker.new(topic_alias_maximum: 0, max_alias_bytes: 0)

    bounded = [max_connections: 1, topic_alias_maximum: 1, max_packet_size: 100]
    assert {:ok, _} = Broker.new(bounded ++ [max_alias_bytes: 100])
    assert {:error, :invalid_options} = Broker.new(bounded ++ [max_alias_bytes: 99])
  end
end
