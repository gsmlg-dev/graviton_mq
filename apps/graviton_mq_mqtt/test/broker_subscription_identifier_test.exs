defmodule GravitonMQ.MQTT.BrokerSubscriptionIdentifierTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.{Broker, Broker.Checkpoint}
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Disconnect, Publish, Subscribe}

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

  defp attach_as(state, id, client_id, options) do
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

  defp subscribe(state, id, filters, identifier \\ nil) do
    properties = if identifier, do: [subscription_identifier: identifier], else: []

    packet(state, id, %Subscribe{
      packet_id: rem(state.next_id, 65_535) + 1,
      subscriptions: filters,
      properties: properties
    })
  end

  defp publishes(effects, id), do: for({:send, ^id, _, _, %Publish{} = p} <- effects, do: p)

  defp ids(%Publish{properties: properties}),
    do: for({:subscription_identifier, id} <- properties, do: id) |> Enum.sort()

  test "CONNACK advertises Subscription Identifiers and the switch restores 0xA1" do
    {_state, effects} = attach(new(), :one)

    [{:send, :one, 1, _, %Connack{properties: on}}] =
      Enum.filter(effects, &match?({:send, _, _, _, %Connack{}}, &1))

    assert on[:subscription_identifier_available] == 1

    {state, effects} = attach(new(subscription_identifiers: 0), :one)

    [{:send, :one, 1, _, %Connack{properties: off}}] =
      Enum.filter(effects, &match?({:send, _, _, _, %Connack{}}, &1))

    assert off[:subscription_identifier_available] == 0
    {state, effects} = subscribe(state, :one, [{"a", 1}], 7)
    assert Enum.any?(effects, &match?({:send, :one, 1, _, %Disconnect{reason_code: 0xA1}}, &1))
    refute Map.has_key?(state.connections, :one)
    assert {:error, :invalid_options} = Broker.new(subscription_identifiers: 2)
  end

  test "one copy carries the IDs of every matching subscription (MQTT-3.3.4-3/4)" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :sub)
    {state, _} = subscribe(state, :sub, [{"s/#", 1}], 7)
    {state, _} = subscribe(state, :sub, [{"s/+/x", 1}], 9)
    {state, effects} = packet(state, :pub, %Publish{topic: "s/a/x", payload: "1"})
    assert [copy] = publishes(effects, :sub)
    assert ids(copy) == [7, 9]

    {state, _} = subscribe(state, :sub, [{"s/+/x", 1}])
    {_state, effects} = packet(state, :pub, %Publish{topic: "s/a/x", payload: "2"})
    assert [copy] = publishes(effects, :sub)
    assert ids(copy) == [7]
  end

  test "identical identifiers are repeated and one SUBSCRIBE applies its ID to every filter" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :sub)
    {state, _} = subscribe(state, :sub, [{"i/#", 0}, {"i/+", 0}], 3)
    assert state.sessions["sub"].subscriptions["i/#"].subscription_identifier == 3
    assert state.sessions["sub"].subscriptions["i/+"].subscription_identifier == 3
    {_state, effects} = packet(state, :pub, %Publish{topic: "i/a", payload: "x"})
    assert [copy] = publishes(effects, :sub)
    assert ids(copy) == [3, 3]
  end

  test "retained-on-subscribe carries the subscribing filter's identifier" do
    {state, _} = attach(new(), :pub)
    {state, _} = packet(state, :pub, %Publish{topic: "r/x", payload: "kept", retain: true})
    {state, _} = attach(state, :sub)
    {_state, effects} = subscribe(state, :sub, [{"r/+", 1}], 11)
    assert [%Publish{retain: true} = copy] = publishes(effects, :sub)
    assert ids(copy) == [11]
  end

  test "inbound PUBLISH carrying an identifier is a protocol error (MQTT-3.3.4-6)" do
    {state, _} = attach(new(), :pub)

    {_state, effects} =
      packet(state, :pub, %Publish{
        topic: "a",
        payload: "x",
        properties: [subscription_identifier: 1]
      })

    assert Enum.any?(effects, &match?({:send, :pub, 1, _, %Disconnect{reason_code: 0x82}}, &1))
  end

  test "a retransmission after reconnect reuses the snapshotted identifiers" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :sub, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :sub, [{"q", 1}], 5)
    {state, _} = packet(state, :pub, %Publish{topic: "q", payload: "x", qos: 1, packet_id: 1})
    {state, _} = Broker.transition(state, {:connection_down, :sub, 1, :abnormal, 2})

    {_state, effects} =
      attach_as(state, :sub_again, "sub",
        clean_start: false,
        properties: [session_expiry_interval: 60]
      )

    assert [%Publish{dup: true} = copy] = publishes(effects, :sub_again)
    assert ids(copy) == [5]
  end

  test "identifiers survive a checkpoint and fail closed when the feature is disabled" do
    {state, _} = attach(new(), :sub, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :sub, [{"c", 1}], 21)
    assert {:ok, snapshot} = Checkpoint.capture(state, 0, 100_000)
    assert {:ok, recovered} = Checkpoint.restore(new(), snapshot, 1_000, 100_100)
    assert recovered.sessions["sub"].subscriptions["c"].subscription_identifier == 21

    assert {:error, :checkpoint_capability_mismatch} =
             Checkpoint.restore(new(subscription_identifiers: 0), snapshot, 1_000, 100_100)
  end
end
