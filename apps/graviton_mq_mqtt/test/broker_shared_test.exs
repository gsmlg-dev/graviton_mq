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

  # Review fixes: commit-time rebinding, offline-limit parity, discards, cost.

  defp race_group(m1_expiry) do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [session_expiry_interval: m1_expiry])
    {state, _} = attach(state, :m2, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {state, _} = subscribe(state, :m2, [{"$share/g/t", 1}])
    state
  end

  # A QoS1 publication bound to m1 (cursor empty) and awaiting authorization.
  defp pending_publication(state) do
    publication = %Publish{topic: "t", payload: "x", qos: 1, packet_id: 1}

    {state, [{:authorize, op, request, _}]} =
      Broker.transition(state, {:packet, :pub, 1, publication, <<1, 2>>, 1})

    assert {:authorize, :m1, :deliver, "t"} in request
    {state, op, request}
  end

  defp complete(state, op, request, now \\ 3) do
    {state, effects} =
      Broker.transition(
        state,
        {:auth_result, op, state.operations[op].generation, Enum.map(request, fn _ -> :ok end),
         now}
      )

    allowed(state, effects, now)
  end

  defp holders(state, payload),
    do:
      for(
        {client_id, session} <- state.sessions,
        entry <- session.deliveries,
        entry.message.payload == payload,
        do: client_id
      )

  defp pubacks(effects), do: for({:send, :pub, _, _, %Puback{} = p} <- effects, do: p)

  for {name, race} <- [
        {"an expiry-0 member's session ends", :expiry_zero_loss},
        {"the member re-SUBSCRIBEs to the same shared filter", :resubscribe},
        {"the member UNSUBSCRIBEs", :unsubscribe},
        {"the member reconnects with Clean Start", :clean_start},
        {"the member's connection is lost (session kept)", :connection_loss}
      ] do
    @race race
    test "a stale binding is re-selected at commit when #{name} during authorization" do
      {state, op, request} =
        pending_publication(race_group(if @race == :expiry_zero_loss, do: 0, else: 60))

      state = race(state, @race)
      {state, effects} = complete(state, op, request)
      assert [%Puback{reason_code: 0}] = pubacks(effects)
      assert holders(state, "x") == ["m2"]
      assert [%Publish{payload: "x"}] = publishes(effects, :m2)
      assert state.shared_discarded == 0
    end
  end

  defp race(state, :expiry_zero_loss) do
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    refute Map.has_key?(state.sessions, "m1")
    state
  end

  defp race(state, :resubscribe) do
    {state, _} =
      packet(state, :m1, %Subscribe{packet_id: 5, subscriptions: [{"$share/g/t", 1}]}, 2)

    state
  end

  defp race(state, :unsubscribe) do
    {state, _} = packet(state, :m1, %Unsubscribe{packet_id: 5, filters: ["$share/g/t"]}, 2)
    state
  end

  defp race(state, :clean_start) do
    {state, []} = Broker.transition(state, {:connection_open, :m1_again, 1, 2})
    connect = %Connect{version: :v5, client_id: "m1", clean_start: true}
    {state, effects} = Broker.transition(state, {:packet, :m1_again, 1, connect, <<16, 0>>, 2})
    {state, _} = allowed(state, effects, 2)
    assert state.sessions["m1"].subscriptions == %{}
    state
  end

  defp race(state, :connection_loss) do
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    state
  end

  test "a commit-time binding with no member left is counted, never silent" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [session_expiry_interval: 0])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {state, op, request} = pending_publication(state)
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    {state, effects} = complete(state, op, request)
    assert [%Puback{reason_code: 0}] = pubacks(effects)
    assert state.shared_discarded == 1
  end

  test "a publication-time group without an eligible member is counted" do
    large = String.duplicate("p", 100)
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [maximum_packet_size: 64])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 0}])
    {state, _} = publish_all(state, [large])
    assert state.shared_discarded == 1

    # QoS0 while the only member is offline: no member can take it.
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 0}])
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    {state, _} = publish_all(state, ["nobody"])
    assert state.sessions["m1"].deliveries == []
    assert state.shared_discarded == 1
  end

  test "full offline limits refuse a shared publication with 0x97, like an ordinary one" do
    acks =
      for filter <- ["$share/g/t", "t"] do
        {state, _} = attach(new(max_offline_deliveries: 1), :pub)
        {state, _} = attach(state, :m1, properties: [session_expiry_interval: 60])
        {state, _} = subscribe(state, :m1, [{filter, 1}])
        {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
        {state, effects} = publish_all(state, ["a", "b"], 1)
        assert Enum.map(state.sessions["m1"].deliveries, & &1.message.payload) == ["a"]
        assert state.shared_discarded == 0
        for p <- pubacks(effects), do: {p.packet_id, p.reason_code}
      end

    assert acks == [[{1, 0}, {2, 0x97}], [{1, 0}, {2, 0x97}]]
  end

  # Structural cost check: the number of Packet.encode calls for one
  # publication must not grow with the number of group members.
  defp encodes(fun) do
    mfa = {GravitonMQ.MQTT.Codec.Packet, :encode, :_}
    parent = self()

    # A process cannot be its own call tracer: count in a separate one.
    tracer =
      spawn_link(fn ->
        count = fn count, n ->
          receive do
            {:trace, ^parent, :call, _} -> count.(count, n + 1)
            {:count, ref} -> send(parent, {ref, n})
          end
        end

        count.(count, 0)
      end)

    :erlang.trace_pattern(mfa, true, [:local])
    :erlang.trace(parent, true, [:call, {:tracer, tracer}])

    try do
      fun.()
    after
      :erlang.trace(parent, false, [:call])
      :erlang.trace_pattern(mfa, false, [:local])
    end

    delivered = :erlang.trace_delivered(parent)
    receive do: ({:trace_delivered, ^parent, ^delivered} -> :ok)
    ref = make_ref()
    send(tracer, {:count, ref})
    receive do: ({^ref, n} -> n)
  end

  defp wide_group(members) do
    {state, _} = attach(new(max_connections: 128), :pub)

    Enum.reduce(1..members, state, fn index, state ->
      id = String.to_atom("w#{index}")
      {state, _} = attach(state, id, properties: [receive_maximum: 1])
      {state, _} = subscribe(state, id, [{"$share/g/t", 1}])
      state
    end)
  end

  test "member selection does not encode the publication once per member" do
    counts =
      for members <- [2, 40] do
        # Every member is at its Receive Maximum, so selection scans them all.
        state = wide_group(members)
        {state, _} = publish_all(state, Enum.map(1..members, &"fill#{&1}"), 1)
        publication = %Publish{topic: "t", payload: "probe", qos: 1, packet_id: 999}
        encodes(fn -> packet(state, :pub, publication) end)
      end

    assert [same, same] = counts
  end

  # Review minor: a binding to a connected member is live only if the copy
  # still fits that member's current Maximum Packet Size (D2 full topic).
  test "a member that resumes with a smaller Maximum Packet Size during authorization loses the binding" do
    large = String.duplicate("p", 100)
    publication = %Publish{topic: "t", payload: large, qos: 1, packet_id: 1}
    state = race_group(60)

    {state, [{:authorize, op, request, _}]} =
      Broker.transition(state, {:packet, :pub, 1, publication, <<1, 2>>, 1})

    assert {:authorize, :m1, :deliver, "t"} in request

    {state, []} = Broker.transition(state, {:connection_open, :m1_again, 1, 2})

    connect = %Connect{
      version: :v5,
      client_id: "m1",
      clean_start: false,
      properties: [session_expiry_interval: 60, maximum_packet_size: 64]
    }

    {state, effects} = Broker.transition(state, {:packet, :m1_again, 1, connect, <<16, 0>>, 2})
    {state, _} = allowed(state, effects, 2)
    assert state.sessions["m1"].connection == {:m1_again, 1}

    {state, effects} = complete(state, op, request)
    assert [%Puback{reason_code: 0}] = pubacks(effects)
    assert holders(state, large) == ["m2"]
    assert [%Publish{payload: ^large}] = publishes(effects, :m2)
    assert state.shared_discarded == 0
  end

  test "a resumed member's never-sent copy that no longer fits moves to a fitting member" do
    large = String.duplicate("p", 100)
    state = race_group(60)
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    {state, _} = Broker.transition(state, {:connection_down, :m2, 1, :abnormal, 2})
    # Both members offline: the copy is bound to m1's session, never sent.
    {state, _} =
      packet(state, :pub, %Publish{topic: "t", payload: large, qos: 1, packet_id: 1}, 3)

    assert holders(state, large) == ["m1"]

    {state, []} = Broker.transition(state, {:connection_open, :m2_again, 1, 4})

    connect_m2 = %Connect{
      version: :v5,
      client_id: "m2",
      clean_start: false,
      properties: [session_expiry_interval: 60]
    }

    {state, effects} =
      Broker.transition(state, {:packet, :m2_again, 1, connect_m2, <<16, 0>>, 4})

    {state, _} = allowed(state, effects, 4)

    {state, []} = Broker.transition(state, {:connection_open, :m1_again, 1, 5})

    connect_m1 = %Connect{
      version: :v5,
      client_id: "m1",
      clean_start: false,
      properties: [session_expiry_interval: 60, maximum_packet_size: 64]
    }

    {state, effects} =
      Broker.transition(state, {:packet, :m1_again, 1, connect_m1, <<16, 0>>, 5})

    {state, _} = allowed(state, effects, 5)
    {state, effects} = Broker.transition(state, {:tick, 6})
    {state, _} = allowed(state, effects, 6)
    assert holders(state, large) == ["m2"]
    assert state.shared_discarded == 0
  end

  # Review minor: the dispatch-time discard when no other member can take the
  # never-sent copy (the other member is connected but too small, or gone).
  for {name, other} <- [
        {"connected but too small", :too_small},
        {"no longer a member", :gone}
      ] do
    @other other
    test "a never-sent copy that fits no member is deleted and counted (other member #{name})" do
      large = String.duplicate("p", 100)
      state = race_group(60)
      {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
      {state, _} = Broker.transition(state, {:connection_down, :m2, 1, :abnormal, 2})

      {state, _} =
        packet(state, :pub, %Publish{topic: "t", payload: large, qos: 1, packet_id: 1}, 3)

      assert holders(state, large) == ["m1"]

      {state, []} = Broker.transition(state, {:connection_open, :m2_again, 1, 4})

      connect_m2 =
        case @other do
          :too_small ->
            %Connect{
              version: :v5,
              client_id: "m2",
              clean_start: false,
              properties: [session_expiry_interval: 60, maximum_packet_size: 64]
            }

          :gone ->
            %Connect{version: :v5, client_id: "m2", clean_start: true}
        end

      {state, effects} =
        Broker.transition(state, {:packet, :m2_again, 1, connect_m2, <<16, 0>>, 4})

      {state, _} = allowed(state, effects, 4)
      {state, []} = Broker.transition(state, {:connection_open, :m1_again, 1, 5})

      connect_m1 = %Connect{
        version: :v5,
        client_id: "m1",
        clean_start: false,
        properties: [session_expiry_interval: 60, maximum_packet_size: 64]
      }

      {state, effects} =
        Broker.transition(state, {:packet, :m1_again, 1, connect_m1, <<16, 0>>, 5})

      {state, _} = allowed(state, effects, 5)
      {state, effects} = Broker.transition(state, {:tick, 6})
      {state, more} = allowed(state, effects, 6)
      assert holders(state, large) == []
      assert state.shared_discarded == 1

      refute Enum.any?(effects ++ more, &match?({:send, _, _, _, %Publish{payload: ^large}}, &1))
    end
  end
end
