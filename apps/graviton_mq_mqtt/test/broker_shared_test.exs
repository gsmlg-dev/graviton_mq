defmodule GravitonMQ.MQTT.BrokerSharedTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.{Broker, Broker.Checkpoint}
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Disconnect, Publish, Suback, Subscribe}
  alias GravitonMQ.MQTT.Packet.{Puback, Unsubscribe}

  defp new(options \\ []) do
    {:ok, state} = Broker.new(options)
    state
  end

  # Each client's principal is its Client Identifier as an atom (a fixed test
  # set). A `{:authorize, principal, :deliver, topic}` request is denied when
  # `principal` is in `deny`; every other request is allowed.
  defp allowed(state, effects, now \\ 0, deny \\ []) do
    Enum.reduce(effects, {state, []}, fn
      {:authorize, op, request, _deadline}, {state, effects} ->
        result =
          if is_list(request),
            do:
              Enum.map(request, fn
                {:authorize, principal, :deliver, _topic} ->
                  if principal in deny, do: {:error, :denied}, else: :ok

                _ ->
                  :ok
              end),
            else: {:ok, request |> elem(1) |> Map.fetch!(:client_id) |> String.to_atom()}

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
        do: allowed(state, effects, now, deny),
        else: {state, effects}
    end)
  end

  defp attach(state, id, options \\ []) do
    {state, []} = Broker.transition(state, {:connection_open, id, 1, 0})

    connect = %Connect{
      version: :v5,
      client_id: to_string(id),
      clean_start: true,
      properties: Keyword.get(options, :properties, [])
    }

    {state, effects} = Broker.transition(state, {:packet, id, 1, connect, <<16, 0>>, 0})
    allowed(state, effects)
  end

  defp packet(state, id, packet, now \\ 1, deny \\ []) do
    {state, effects} = Broker.transition(state, {:packet, id, 1, packet, <<1, 2>>, now})
    allowed(state, effects, now, deny)
  end

  defp subscribe(state, id, filters, properties \\ []) do
    packet(state, id, %Subscribe{
      packet_id: rem(state.next_id, 65_535) + 1,
      subscriptions: filters,
      properties: properties
    })
  end

  defp publishes(effects, id), do: for({:send, ^id, _, _, %Publish{} = p} <- effects, do: p)

  defp publish_all(state, payloads, qos \\ 0) do
    Enum.reduce(Enum.with_index(payloads, 1), {state, []}, fn {payload, index}, {state, acc} ->
      publication = %Publish{
        topic: "t",
        payload: payload,
        qos: qos,
        packet_id: if(qos == 0, do: nil, else: index)
      }

      {state, effects} = packet(state, :pub, publication)
      {state, acc ++ effects}
    end)
  end

  defp group(options \\ [], member \\ []) do
    {state, _} = attach(new(options), :pub)
    {state, _} = attach(state, :m1, member)
    {state, _} = attach(state, :m2, member)
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {state, _} = subscribe(state, :m2, [{"$share/g/t", 1}])
    state
  end

  test "CONNACK advertises shared subscriptions; the switch keeps DISCONNECT 0x9E" do
    {_state, effects} = attach(new(), :one)

    [{:send, :one, 1, _, %Connack{properties: on}}] =
      Enum.filter(effects, &match?({:send, _, _, _, %Connack{}}, &1))

    assert on[:shared_subscription_available] == 1
    assert {:error, :invalid_options} = Broker.new(shared_subscriptions: 2)

    {state, effects} = attach(new(shared_subscriptions: 0), :one)

    [{:send, :one, 1, _, %Connack{properties: off}}] =
      Enum.filter(effects, &match?({:send, _, _, _, %Connack{}}, &1))

    assert off[:shared_subscription_available] == 0
    {_state, effects} = subscribe(state, :one, [{"$share/g/t", 1}])
    assert Enum.any?(effects, &match?({:send, :one, 1, _, %Disconnect{reason_code: 0x9E}}, &1))
  end

  test "each message goes to exactly one member, round-robin over Client Identifiers" do
    {_state, effects} = publish_all(group(), ["1", "2", "3", "4"])
    assert Enum.map(publishes(effects, :m1), & &1.payload) == ["1", "3"]
    assert Enum.map(publishes(effects, :m2), & &1.payload) == ["2", "4"]
  end

  test "subscribe authorizes the full shared filter and sends no retained messages (§3.8.4)" do
    {state, _} = attach(new(), :pub)
    {state, _} = packet(state, :pub, %Publish{topic: "t", payload: "kept", retain: true})
    {state, _} = attach(state, :m1)

    subscribe = %Subscribe{
      packet_id: 1,
      subscriptions: [{"$share/g/t", %{qos: 1, retain_handling: 0}}]
    }

    {state, effects} = Broker.transition(state, {:packet, :m1, 1, subscribe, <<1, 2>>, 1})
    assert [{:authorize, _, [{:authorize, :m1, :subscribe, "$share/g/t"}], _}] = effects
    {_state, effects} = allowed(state, effects, 1)
    assert Enum.any?(effects, &match?({:send, :m1, _, _, %Suback{reason_codes: [1]}}, &1))
    assert publishes(effects, :m1) == []
  end

  test "membership is keyed by session and full filter; UNSUBSCRIBE leaves the group" do
    state = group()
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 0}])
    assert map_size(state.sessions["m1"].subscriptions) == 1
    assert state.sessions["m1"].subscriptions["$share/g/t"].shared == {"g", "t"}
    {state, _} = packet(state, :m1, %Unsubscribe{packet_id: 77, filters: ["$share/g/t"]})
    {_state, effects} = publish_all(state, ["1", "2"])
    assert publishes(effects, :m1) == []
    assert Enum.map(publishes(effects, :m2), & &1.payload) == ["1", "2"]
  end

  test "a member's granted QoS and its own identifier apply; an overlapping ordinary subscription gets its own copy" do
    {state, _} = attach(new(maximum_qos: 1), :pub)
    {state, _} = attach(state, :m1)
    {state, effects} = subscribe(state, :m1, [{"$share/g/t", 2}], subscription_identifier: 4)
    assert Enum.any?(effects, &match?({:send, :m1, _, _, %Suback{reason_codes: [1]}}, &1))
    {state, _} = subscribe(state, :m1, [{"t", 0}], subscription_identifier: 8)

    {_state, effects} =
      packet(state, :pub, %Publish{topic: "t", payload: "x", qos: 1, packet_id: 1})

    copies = publishes(effects, :m1)

    assert Enum.sort(Enum.map(copies, &{&1.qos, &1.properties[:subscription_identifier]})) ==
             [{0, 8}, {1, 4}]
  end

  test "every matching group gets exactly one copy; a member of two groups gets one per group" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1)
    {state, _} = attach(state, :m2)
    {state, _} = subscribe(state, :m1, [{"$share/a/t", 0}, {"$share/b/#", 0}])
    {state, _} = subscribe(state, :m2, [{"$share/a/t", 0}, {"$share/c/+", 0}])
    {state, _} = subscribe(state, :m2, [{"$share/d/other", 0}])

    {_state, effects} = publish_all(state, ["1", "2"])
    copies = publishes(effects, :m1) ++ publishes(effects, :m2)

    # Per publication: group a (one of m1/m2), b (m1), c (m2); d does not match.
    assert length(copies) == 6
    assert Enum.frequencies(Enum.map(copies, & &1.payload)) == %{"1" => 3, "2" => 3}
    assert length(publishes(effects, :m1)) == 3
    assert length(publishes(effects, :m2)) == 3
  end

  test ":deliver is authorized for the chosen member only; a denial drops the copy without fallback" do
    state = group()
    publication = %Publish{topic: "t", payload: "x", qos: 1, packet_id: 1}

    {pending, effects} =
      Broker.transition(state, {:packet, :pub, 1, publication, <<1, 2>>, 1})

    assert [{:authorize, _, request, _}] = effects

    assert [{:authorize, :pub, :publish, "t"}, {:authorize, :m1, :deliver, "t"}] = request

    {state, effects} = allowed(pending, effects, 1, [:m1])
    assert publishes(effects, :m1) == []
    assert publishes(effects, :m2) == []
    assert state.sessions["m1"].deliveries == []
    assert state.sessions["m2"].deliveries == []
    assert Enum.any?(effects, &match?({:send, :pub, _, _, %Puback{}}, &1))
  end

  test "a member without send quota is skipped while another can send" do
    state = group([], properties: [receive_maximum: 1])
    {state, effects} = publish_all(state, ["1", "2", "3"], 1)
    assert Enum.map(publishes(effects, :m1), & &1.payload) == ["1"]
    assert Enum.map(publishes(effects, :m2), & &1.payload) == ["2"]
    # Every member is at its Receive Maximum: the third message is bound
    # (queued) to a connected member rather than sent or lost.
    queued = Enum.flat_map(["m1", "m2"], &state.sessions[&1].deliveries)
    assert Enum.count(queued, &(&1.message.payload == "3" and not &1.ever_sent)) == 1
  end

  test "an oversized member is skipped and a QoS0 message nobody can receive is discarded" do
    # 64 bytes admits CONNACK and SUBACK but not a 100-byte payload.
    large = String.duplicate("p", 100)
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [maximum_packet_size: 64])
    {state, _} = attach(state, :m2)
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 0}])
    {state, _} = subscribe(state, :m2, [{"$share/g/t", 0}])
    {_state, effects} = publish_all(state, [large])
    assert publishes(effects, :m1) == []
    assert [%Publish{payload: ^large}] = publishes(effects, :m2)

    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [maximum_packet_size: 64])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 0}])
    {state, effects} = publish_all(state, [large])
    assert publishes(effects, :m1) == []
    assert state.sessions["m1"].deliveries == []
  end

  test "QoS1 binds to an offline persistent member; QoS0 is not queued offline" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    {state, _} = publish_all(state, ["q0"], 0)
    assert state.sessions["m1"].deliveries == []
    {state, _} = publish_all(state, ["q1"], 1)

    assert [%{shared: "$share/g/t", qos: 1, message: %{payload: "q1"}}] =
             state.sessions["m1"].deliveries
  end

  test "a connected member is preferred over an offline one" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [session_expiry_interval: 60])
    {state, _} = attach(state, :m2)
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {state, _} = subscribe(state, :m2, [{"$share/g/t", 1}])
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    {state, effects} = publish_all(state, ["1", "2", "3"], 1)
    assert state.sessions["m1"].deliveries == []
    assert Enum.map(publishes(effects, :m2), & &1.payload) == ["1", "2", "3"]
  end

  test "shared memberships and entries survive a checkpoint and fail closed when disabled" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    {state, _} = publish_all(state, ["kept"], 1)
    assert {:ok, snapshot} = Checkpoint.capture(state, 0, 100_000)
    refute Map.has_key?(snapshot.state, :share_cursor)
    assert {:ok, recovered} = Checkpoint.restore(new(), snapshot, 1_000, 100_100)
    assert recovered.sessions["m1"].subscriptions["$share/g/t"].shared == {"g", "t"}
    assert [%{shared: "$share/g/t"}] = recovered.sessions["m1"].deliveries
    assert recovered.share_cursor == %{}

    assert {:error, :checkpoint_capability_mismatch} =
             Checkpoint.restore(new(shared_subscriptions: 0), snapshot, 1_000, 100_100)

    # A membership alone (no bound entry) also fails closed.
    {state, _} = attach(new(), :m1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {:ok, snapshot} = Checkpoint.capture(state, 0, 100_000)

    assert {:error, :checkpoint_capability_mismatch} =
             Checkpoint.restore(new(shared_subscriptions: 0), snapshot, 1_000, 100_100)
  end

  test "a forged shared field that disagrees with its filter is rejected" do
    {state, _} = attach(new(), :m1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {:ok, snapshot} = Checkpoint.capture(state, 0, 100_000)

    forged =
      put_in(snapshot, [:state, :sessions, "m1", :subscriptions, "$share/g/t", :shared], nil)

    assert {:error, :invalid_broker_checkpoint} =
             Checkpoint.restore(new(), forged, 1_000, 100_100)
  end
end
