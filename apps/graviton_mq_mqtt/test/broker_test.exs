defmodule GravitonMQ.MQTT.BrokerTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.MQTT.Broker

  alias GravitonMQ.MQTT.Packet.{
    Connect,
    Connack,
    Publish,
    Subscribe,
    Suback,
    Puback,
    Disconnect,
    Will
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

  defp attach(state, id, generation \\ 1, options \\ []) do
    {state, []} = Broker.transition(state, {:connection_open, id, generation, 0})

    connect = %Connect{
      version: :v5,
      client_id: to_string(id),
      clean_start: Keyword.get(options, :clean_start, true),
      properties: Keyword.get(options, :properties, []),
      will: Keyword.get(options, :will)
    }

    {state, effects} = Broker.transition(state, {:packet, id, generation, connect, <<16, 0>>, 0})
    allowed(state, effects)
  end

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

  test "CONNECT advertises bounded QoS1 capabilities and attaches only after authentication" do
    state = new(maximum_qos: 1)
    {state, []} = Broker.transition(state, {:connection_open, :one, 1, 0})
    connect = %Connect{version: :v5, client_id: "one", clean_start: true}

    {state, [{:authorize, op, {:authenticate, credentials}, _}]} =
      Broker.transition(state, {:packet, :one, 1, connect, <<16, 0>>, 1})

    assert credentials.client_id == "one"
    assert state.sessions == %{}
    {state, effects} = Broker.transition(state, {:auth_result, op, 1, {:ok, :user}, 2})
    assert state.sessions["one"].principal == :user
    assert [{:send, :one, 1, _, %Connack{reason_code: 0, properties: properties}}] = effects
    assert properties[:maximum_qos] == 1
    assert properties[:shared_subscription_available] == 0
    assert properties[:subscription_identifier_available] == 0
    assert properties[:topic_alias_maximum] == 0
  end

  test "publication admission installs every recipient and retained atomically before PUBACK" do
    {state, _} = attach(new(max_pending_deliveries: 1), :sender)
    {state, _} = attach(state, :one)
    {state, _} = attach(state, :two)
    {state, _} = subscribe(state, :one, [{"a", 1}])
    {state, _} = subscribe(state, :two, [{"a", 1}])

    {state, effects} =
      packet(state, :sender, %Publish{
        topic: "a",
        payload: "x",
        qos: 1,
        packet_id: 9,
        retain: true
      })

    assert state.retained == %{}
    assert state.sessions["one"].deliveries == []
    assert state.sessions["two"].deliveries == []
    assert Enum.any?(effects, &match?({:send, :sender, 1, _, %Puback{reason_code: 0x97}}, &1))
  end

  test "outbound identifiers are independently allocated and overlapping subscriptions coalesce" do
    {state, _} = attach(new(), :sender)
    {state, _} = attach(state, :receiver)
    {state, _} = subscribe(state, :receiver, [{"a/#", 0}, {"a/+", 1}])

    {_state, effects} =
      packet(state, :sender, %Publish{
        topic: "a/b",
        payload: "x",
        qos: 1,
        packet_id: 44,
        dup: true
      })

    assert [{:send, :receiver, 1, _, %Publish{packet_id: 1, qos: 1, dup: false}}] =
             Enum.filter(effects, &match?({:send, :receiver, _, _, %Publish{}}, &1))
  end

  test "persistent offline QoS1 resumes with DUP and stale generations cannot remove the session" do
    {state, _} = attach(new(), :sender)
    {state, _} = attach(state, :receiver, 1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :receiver, [{"a", 1}])
    {state, _} = packet(state, :sender, %Publish{topic: "a", payload: "x", qos: 1, packet_id: 4})
    {state, _} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 2})
    {state, _} = Broker.transition(state, {:connection_open, :new_receiver, 2, 3})

    connect = %Connect{
      version: :v5,
      client_id: "receiver",
      clean_start: false,
      properties: [session_expiry_interval: 60]
    }

    {state, effects} =
      Broker.transition(state, {:packet, :new_receiver, 2, connect, <<16, 0>>, 3})

    {state, effects} = allowed(state, effects, 3)

    assert Enum.any?(
             effects,
             &match?({:send, :new_receiver, 2, _, %Connack{session_present: true}}, &1)
           )

    assert Enum.any?(
             effects,
             &match?({:send, :new_receiver, 2, _, %Publish{packet_id: 1, dup: true}}, &1)
           )

    {same, []} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 4})
    assert same == state
  end

  test "retained replacement deletion and Retain Handling obey subscription incarnation" do
    {state, _} = attach(new(), :sender)
    {state, _} = packet(state, :sender, %Publish{topic: "a", payload: "retained", retain: true})
    {state, _} = attach(state, :receiver)
    {state, effects} = subscribe(state, :receiver, [{"a", %{qos: 0, retain_handling: 0}}])

    assert Enum.any?(
             effects,
             &match?({:send, :receiver, 1, _, %Publish{retain: true, payload: "retained"}}, &1)
           )

    {state, effects} = subscribe(state, :receiver, [{"a", %{qos: 0, retain_handling: 1}}])
    refute Enum.any?(effects, &match?({:send, :receiver, 1, _, %Publish{}}, &1))
    {state, _} = packet(state, :sender, %Publish{topic: "a", payload: "", retain: true})
    assert state.retained == %{}
  end

  test "duplicate SUBSCRIBE filters apply Retain Handling sequentially and keep replay QoS" do
    for {first, second, replay_count} <- [{2, 1, 0}, {1, 2, 1}] do
      {state, _} = attach(new(), :sender)

      {state, _} =
        packet(state, :sender, %Publish{
          topic: "a",
          payload: "retained",
          qos: 1,
          packet_id: 40,
          retain: true
        })

      {state, _} = attach(state, :receiver)

      {state, effects} =
        subscribe(state, :receiver, [
          {"a", %{qos: 1, retain_handling: first}},
          {"a", %{qos: 0, retain_handling: second}}
        ])

      assert [%Suback{reason_codes: [1, 0]}] =
               for({:send, :receiver, _, _, %Suback{} = ack} <- effects, do: ack)

      replays = for({:send, :receiver, _, _, %Publish{} = replay} <- effects, do: replay)
      assert length(replays) == replay_count
      assert Enum.all?(replays, &(&1.qos == 1 and &1.retain and &1.payload == "retained"))
      assert state.sessions["receiver"].subscriptions["a"].options.qos == 0
      assert state.sessions["receiver"].subscriptions["a"].options.retain_handling == second
    end
  end

  test "denied duplicate filters do not create subscriptions or authorize another occurrence" do
    for {grants, expected_codes, replay_count, expected_qos} <- [
          {[{:error, :denied}, :ok], [0x87, 0], 1, 0},
          {[:ok, {:error, :denied}], [1, 0x87], 0, 1}
        ] do
      {state, _} = attach(new(), :sender)

      {state, _} =
        packet(state, :sender, %Publish{
          topic: "a",
          payload: "retained",
          qos: 1,
          packet_id: 40,
          retain: true
        })

      {state, _} = attach(state, :receiver)

      request = %Subscribe{
        packet_id: 9,
        subscriptions: [
          {"a", %{qos: 1, retain_handling: 2}},
          {"a", %{qos: 0, retain_handling: 1}}
        ]
      }

      {state, effects} = Broker.transition(state, {:packet, :receiver, 1, request, <<1>>, 1})
      {state, effects} = subscription_decisions(state, effects, grants)

      assert [%Suback{reason_codes: ^expected_codes}] =
               for({:send, :receiver, _, _, %Suback{} = ack} <- effects, do: ack)

      replays = for({:send, :receiver, _, _, %Publish{} = replay} <- effects, do: replay)
      assert length(replays) == replay_count
      assert Enum.all?(replays, &(&1.qos == expected_qos))
      assert state.sessions["receiver"].subscriptions["a"].options.qos == expected_qos
    end
  end

  test "duplicate SUBSCRIBE replacement preserves existing inflight responsibility" do
    {state, _} = attach(new(), :sender)
    {state, _} = attach(state, :receiver)
    {state, _} = subscribe(state, :receiver, [{"a", 1}])

    {state, _} =
      packet(state, :sender, %Publish{topic: "a", payload: "inflight", qos: 1, packet_id: 40})

    [inflight] = state.sessions["receiver"].deliveries
    assert inflight.packet_id != nil

    {state, effects} =
      subscribe(state, :receiver, [
        {"a", %{qos: 1, retain_handling: 2}},
        {"a", %{qos: 0, retain_handling: 1}}
      ])

    assert state.sessions["receiver"].deliveries == [inflight]
    refute Enum.any?(effects, &match?({:send, :receiver, _, _, %Publish{}}, &1))
    {state, _} = packet(state, :receiver, %Puback{packet_id: inflight.packet_id})
    assert state.sessions["receiver"].deliveries == []
  end

  defp subscription_decisions(state, effects, grants) do
    Enum.reduce(effects, {state, []}, fn
      {:authorize, op, requests, _}, {state, output} ->
        decisions =
          requests
          |> Enum.with_index()
          |> Enum.map(fn
            {{:authorize, _, :subscribe, _}, index} -> Enum.fetch!(grants, index)
            _ -> :ok
          end)

        {state, more} = Broker.transition(state, {:auth_result, op, 1, decisions, 2})
        {state, more} = subscription_decisions(state, more, grants)
        {state, output ++ more}

      effect, {state, output} ->
        {state, output ++ [effect]}
    end)
  end

  test "No Local excludes the publishing session and dollar topics require explicit filters" do
    {state, _} = attach(new(), :sender)
    {state, _} = subscribe(state, :sender, [{"#", %{qos: 1, no_local: true}}])
    {state, _} = attach(state, :receiver)
    {state, _} = subscribe(state, :receiver, [{"#", 1}])
    {state, effects} = packet(state, :sender, %Publish{topic: "a", payload: "x"})
    refute Enum.any?(effects, &match?({:send, :sender, _, _, %Publish{}}, &1))
    assert Enum.any?(effects, &match?({:send, :receiver, _, _, %Publish{}}, &1))
    {_state, effects} = packet(state, :sender, %Publish{topic: "$SYS/a", payload: "x"})
    refute Enum.any?(effects, &match?({:send, _, _, _, %Publish{}}, &1))
  end

  test "auth infrastructure failure never partially routes or mutates retained state" do
    {state, _} = attach(new(), :sender)
    {state, _} = attach(state, :receiver)
    {state, _} = subscribe(state, :receiver, [{"a", 1}])

    {state, [{:authorize, op, requests, _}]} =
      Broker.transition(
        state,
        {:packet, :sender, 1,
         %Publish{topic: "a", payload: "x", qos: 1, packet_id: 3, retain: true}, <<1>>, 2}
      )

    assert length(requests) == 2

    {state, effects} =
      Broker.transition(state, {:auth_result, op, 1, [:ok, {:error, :callback_timeout}], 3})

    assert state.retained == %{}
    assert state.sessions["receiver"].deliveries == []
    refute Enum.any?(effects, &match?({:send, :sender, _, _, %Puback{reason_code: 0}}, &1))
  end

  test "publisher order survives reverse asynchronous authorization completion" do
    {state, _} = attach(new(), :sender)
    {state, _} = attach(state, :receiver)
    {state, _} = subscribe(state, :receiver, [{"a", 0}])
    publish = %Publish{topic: "a", payload: "1"}

    {state, [{:authorize, first, _, _}]} =
      Broker.transition(state, {:packet, :sender, 1, publish, <<1>>, 1})

    {state, [{:authorize, second, _, _}]} =
      Broker.transition(state, {:packet, :sender, 1, %{publish | payload: "2"}, <<2>>, 2})

    {state, []} = Broker.transition(state, {:auth_result, second, 1, [:ok, :ok], 3})
    {state, effects} = Broker.transition(state, {:auth_result, first, 1, [:ok, :ok], 4})

    assert Enum.any?(
             effects,
             &match?({:authorize, _, [{:authorize, :user, :deliver, "a"}], _}, &1)
           )

    {_state, effects} = allowed(state, effects, 4)

    assert ["1", "2"] ==
             for({:send, :receiver, _, _, %Publish{payload: payload}} <- effects, do: payload)
  end

  test "Will fires at the earlier session expiry and delay, reconnect cancels it" do
    will = %Will{topic: "a", payload: "will", qos: 1, properties: [will_delay_interval: 10]}
    {state, _} = attach(new(), :receiver)
    {state, _} = subscribe(state, :receiver, [{"a", 1}])
    {state, _} = attach(state, :sender, 1, will: will, properties: [session_expiry_interval: 1])
    {state, _} = Broker.transition(state, {:connection_down, :sender, 1, :abnormal, 10})
    {state, []} = Broker.transition(state, {:tick, 1009})
    {state, effects} = Broker.transition(state, {:tick, 1010})
    {_state, effects} = allowed(state, effects, 1010)
    assert Enum.any?(effects, &match?({:send, :receiver, _, _, %Publish{payload: "will"}}, &1))
  end

  test "Will forwarding preserves repeated User Properties in their original order" do
    users = [{"key", "first"}, {"key", "first"}, {"key", "last"}]

    will = %Will{
      topic: "will/properties",
      payload: "will-雪",
      qos: 1,
      properties: [
        user_property: Enum.at(users, 0),
        payload_format_indicator: 1,
        user_property: Enum.at(users, 1),
        will_delay_interval: 1,
        user_property: Enum.at(users, 2)
      ]
    }

    {state, _} = attach(new(), :receiver)
    {state, _} = subscribe(state, :receiver, [{will.topic, 1}])
    {state, _} = attach(state, :sender, 1, will: will)
    {state, _effects} = Broker.transition(state, {:connection_down, :sender, 1, :abnormal, 10})
    {state, effects} = Broker.transition(state, {:tick, 10})
    {_state, effects} = allowed(state, effects, 10)

    assert [packet] = for({:send, :receiver, 1, _, %Publish{} = packet} <- effects, do: packet)
    assert packet.payload == will.payload
    assert packet.properties[:payload_format_indicator] == 1
    assert Keyword.get_values(packet.properties, :user_property) == users
    refute Keyword.has_key?(packet.properties, :will_delay_interval)
  end

  test "only normal DISCONNECT suppresses Will and nonzero reasons retain its delay" do
    for reason <- [0, 4, 0x80, 0x82, 0x99], expiry <- [1, 20] do
      will = %Will{topic: "a", payload: "will", qos: 1, properties: [will_delay_interval: 10]}
      {state, _} = attach(new(), :receiver)
      {state, _} = subscribe(state, :receiver, [{"a", 1}])

      {state, _} =
        attach(state, :sender, 1, will: will, properties: [session_expiry_interval: expiry])

      {state, effects} = packet(state, :sender, %Disconnect{reason_code: reason}, 100)
      assert Enum.any?(effects, &match?({:close, :sender, 1, _}, &1))

      if reason == 0 do
        assert state.wills == %{}
      else
        due = 100 + min(10, expiry) * 1_000
        assert [%{due: ^due}] = Map.values(state.wills)
        {state, effects} = Broker.transition(state, {:tick, due - 1})
        refute Enum.any?(effects, &match?({:authorize, _, _, _}, &1))
        {state, effects} = Broker.transition(state, {:tick, due})
        {_state, effects} = allowed(state, effects, due)

        assert Enum.any?(
                 effects,
                 &match?({:send, :receiver, _, _, %Publish{payload: "will"}}, &1)
               )
      end
    end
  end

  test "host demand ownership references and capacity are finite" do
    state = new(max_demand: 1)

    {state, effects} =
      Broker.transition(state, {:host_operation, :owner, :sub, {:subscribe, "a", %{qos: 1}}, 0})

    {state, effects} = allowed(state, effects)
    assert [{:complete, :sub, {:ok, subscription}}] = effects

    {state, [{:complete, :demand, {:ok, :ok}}]} =
      Broker.transition(state, {:host_operation, :owner, :demand, {:demand, subscription, 1}, 0})

    {state, [{:complete, :excess, {:error, :demand_limit}}]} =
      Broker.transition(state, {:host_operation, :owner, :excess, {:demand, subscription, 1}, 0})

    {state, effects} =
      Broker.transition(
        state,
        {:host_operation, :publisher, :pub,
         {:publish, %Publish{topic: "a", payload: "x", qos: 1}, <<1>>}, 1}
      )

    {state, effects} = allowed(state, effects, 1)
    assert {:complete, :pub, {:ok, :accepted_memory}} in effects

    assert [{:host_delivery, ^subscription, delivery, message}] =
             Enum.filter(effects, &match?({:host_delivery, _, _, _}, &1))

    assert message.encoded == <<1>>

    {state, [{:complete, :wrong, {:error, :not_owner}}]} =
      Broker.transition(state, {:host_operation, :other, :wrong, {:ack, delivery}, 2})

    {state, [{:complete, :ack, {:ok, :ok}}]} =
      Broker.transition(state, {:host_operation, :owner, :ack, {:ack, delivery}, 2})

    {state, _} = Broker.transition(state, {:host_down, :owner, 3})
    assert state.hosts == %{}
  end

  test "Will retry retains a bounded due job under route pressure" do
    will = %Will{topic: "a", payload: "will"}
    {state, _} = attach(new(max_pending_deliveries: 0), :receiver)
    {state, _} = subscribe(state, :receiver, [{"a", 0}])
    {state, _} = attach(state, :sender, 1, will: will)
    {state, _} = Broker.transition(state, {:connection_down, :sender, 1, :abnormal, 10})
    {state, effects} = Broker.transition(state, {:tick, 10})
    {state, _} = allowed(state, effects, 10)
    assert [{_, %{status: :due}}] = Map.to_list(state.wills)
    {state, effects} = Broker.transition(state, {:tick, 11})
    {state, _} = allowed(state, effects, 11)
    assert map_size(state.wills) == 1
    assert state.sessions["receiver"].deliveries == []
  end

  test "reconnect before Will delay cancels the old Will and normal disconnect suppresses Will" do
    will = %Will{topic: "a", payload: "will", properties: [will_delay_interval: 10]}
    {state, _} = attach(new(), :sender, 1, will: will, properties: [session_expiry_interval: 60])
    {state, _} = Broker.transition(state, {:connection_down, :sender, 1, :abnormal, 10})
    {state, _} = Broker.transition(state, {:connection_open, :resumed, 2, 11})

    connect = %Connect{
      version: :v5,
      client_id: "sender",
      clean_start: false,
      properties: [session_expiry_interval: 60]
    }

    {state, effects} = Broker.transition(state, {:packet, :resumed, 2, connect, <<16>>, 11})
    {state, _} = allowed(state, effects, 11)
    assert state.wills == %{}
    {state, _} = attach(state, :normal, 1, will: will)
    {state, _} = packet(state, :normal, %Disconnect{})
    assert state.wills == %{}
  end

  test "resumed sessions exclude offline deliveries whose granted QoS is zero" do
    {state, _} = attach(new(), :sender)
    {state, _} = attach(state, :receiver, 1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :receiver, [{"a", 0}])
    {state, _} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 2})

    {state, _} =
      packet(state, :sender, %Publish{topic: "a", payload: "x", qos: 1, packet_id: 9}, 3)

    assert state.sessions["receiver"].deliveries == []
  end

  test "takeover cannot change principal and old auth results are fenced" do
    {state, _} = attach(new(), :original, 1, properties: [session_expiry_interval: 60])
    {state, _} = Broker.transition(state, {:connection_open, :attacker, 2, 1})
    connect = %Connect{version: :v5, client_id: "original", clean_start: false}

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(state, {:packet, :attacker, 2, connect, <<16>>, 1})

    {state, effects} = Broker.transition(state, {:auth_result, op, 2, {:ok, :other}, 2})
    assert state.sessions["original"].connection == {:original, 1}
    refute Enum.any?(effects, &match?({:close, :original, _, _}, &1))
    {state, []} = Broker.transition(state, {:auth_result, op, 2, {:ok, :user}, 3})
    assert state.sessions["original"].connection == {:original, 1}
  end

  test "Retain As Published applies to live routing and Retain Handling two suppresses replay" do
    {state, _} = attach(new(), :sender)
    {state, _} = attach(state, :receiver)
    {state, _} = subscribe(state, :receiver, [{"a", %{qos: 0, retain_as_published: true}}])
    {state, effects} = packet(state, :sender, %Publish{topic: "a", payload: "x", retain: true})
    assert Enum.any?(effects, &match?({:send, :receiver, _, _, %Publish{retain: true}}, &1))
    {_state, effects} = subscribe(state, :receiver, [{"a", %{qos: 0, retain_handling: 2}}])
    assert Enum.any?(effects, &match?({:send, :receiver, _, _, %Suback{reason_codes: [0]}}, &1))
    refute Enum.any?(effects, &match?({:send, :receiver, _, _, %Publish{}}, &1))
  end

  test "expired offline unsent QoS1 and retained messages release their byte capacity" do
    {state, _} = attach(new(), :sender)
    {state, _} = attach(state, :receiver, 1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :receiver, [{"a", 1}])
    {state, _} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 2})

    {state, _} =
      packet(
        state,
        :sender,
        %Publish{
          topic: "a",
          payload: "x",
          qos: 1,
          packet_id: 9,
          retain: true,
          properties: [message_expiry_interval: 1]
        },
        10
      )

    assert length(state.sessions["receiver"].deliveries) == 1
    {state, _} = Broker.transition(state, {:tick, 1010})
    assert state.sessions["receiver"].deliveries == []
    assert state.retained == %{}
  end

  test "revoked subscription incarnations cannot receive a late authorized publication" do
    {state, _} = attach(new(), :sender)
    {state, _} = attach(state, :receiver)
    {state, _} = subscribe(state, :receiver, [{"a", 1}])

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(
        state,
        {:packet, :sender, 1, %Publish{topic: "a", payload: "x"}, <<1>>, 2}
      )

    {state, _} =
      packet(
        state,
        :receiver,
        %GravitonMQ.MQTT.Packet.Unsubscribe{packet_id: 2, filters: ["a"]},
        3
      )

    {state, effects} = Broker.transition(state, {:auth_result, op, 1, [:ok, :ok], 4})
    refute Enum.any?(effects, &match?({:send, :receiver, _, _, %Publish{}}, &1))
    assert state.sessions["receiver"].deliveries == []
  end

  test "QoS2 subscriptions grant one with a Maximum QoS one profile" do
    {state, _} = attach(new(maximum_qos: 1), :one)
    {_state, effects} = subscribe(state, :one, [{"a", 2}])
    assert Enum.any?(effects, &match?({:send, :one, _, _, %Suback{reason_codes: [1]}}, &1))
  end

  test "a shared subscription after advertising unavailable disconnects before authorization" do
    {state, _} = attach(new(), :one, 1, properties: [session_expiry_interval: 60])
    packet = %Subscribe{packet_id: 9, subscriptions: [{"a", 1}, {"$share/g/a", 1}]}
    {state, effects} = Broker.transition(state, {:packet, :one, 1, packet, <<1, 2>>, 1})
    assert Enum.any?(effects, &match?({:send, :one, 1, _, %Disconnect{reason_code: 0x9E}}, &1))
    assert Enum.any?(effects, &match?({:close, :one, 1, _}, &1))
    refute Enum.any?(effects, &match?({:authorize, _, _, _}, &1))
    refute Enum.any?(effects, &match?({:send, :one, 1, _, %Suback{}}, &1))
    assert state.sessions["one"].subscriptions == %{}
  end

  test "failed replacement CONNECT capacity does not take over or erase the old session" do
    {state, _} =
      attach(new(max_wills: 0), :original, 1, properties: [session_expiry_interval: 60])

    {state, _} = subscribe(state, :original, [{"a", 1}])
    {state, _} = Broker.transition(state, {:connection_open, :replacement, 2, 1})

    connect = %Connect{
      version: :v5,
      client_id: "original",
      clean_start: true,
      will: %Will{topic: "a", payload: "will"}
    }

    {state, effects} = Broker.transition(state, {:packet, :replacement, 2, connect, <<16>>, 1})
    {state, effects} = allowed(state, effects, 1)
    assert state.sessions["original"].connection == {:original, 1}
    assert Map.has_key?(state.sessions["original"].subscriptions, "a")
    refute Enum.any?(effects, &match?({:close, :original, _, _}, &1))
  end

  test "unconfirmed socket writes have a finite count and byte budget" do
    {state, _} = attach(new(max_writes: 2), :one)
    {state, _} = packet(state, :one, %GravitonMQ.MQTT.Packet.Pingreq{})
    {state, effects} = packet(state, :one, %GravitonMQ.MQTT.Packet.Pingreq{})
    assert Enum.any?(effects, &match?({:close, :one, _, :write_limit}, &1))
    assert state.writes == %{}
  end

  test "revoking an overlapping high grant lowers late publication delivery QoS" do
    {state, _} = attach(new(), :sender)
    {state, _} = attach(state, :receiver)
    {state, _} = subscribe(state, :receiver, [{"a/#", 0}, {"a/+", 1}])

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(
        state,
        {:packet, :sender, 1, %Publish{topic: "a/b", payload: "x", qos: 1, packet_id: 9}, <<1>>,
         2}
      )

    {state, _} =
      packet(
        state,
        :receiver,
        %GravitonMQ.MQTT.Packet.Unsubscribe{packet_id: 2, filters: ["a/+"]},
        3
      )

    {state, effects} = Broker.transition(state, {:auth_result, op, 1, [:ok, :ok], 4})

    assert Enum.any?(
             effects,
             &match?({:authorize, _, [{:authorize, :user, :deliver, "a/b"}], _}, &1)
           )

    {_state, effects} = allowed(state, effects, 4)
    assert Enum.any?(effects, &match?({:send, :receiver, _, _, %Publish{qos: 0}}, &1))
  end

  test "CONNECT will authorization uses a bounded authenticated principal" do
    {state, _} = Broker.transition(new(max_session_bytes: 64), {:connection_open, :one, 1, 0})

    connect = %Connect{
      version: :v5,
      client_id: "one",
      clean_start: true,
      will: %Will{topic: "a", payload: "will"}
    }

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(state, {:packet, :one, 1, connect, <<16>>, 1})

    {state, effects} =
      Broker.transition(state, {:auth_result, op, 1, {:ok, String.duplicate("p", 128)}, 2})

    refute Enum.any?(effects, &match?({:authorize, _, _, _}, &1))
    assert state.operations == %{}
    assert state.sessions == %{}
  end

  test "UNSUBSCRIBE cannot be overtaken by earlier pending SUBSCRIBE authorization" do
    {state, _} = attach(new(), :one)

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(
        state,
        {:packet, :one, 1, %Subscribe{packet_id: 5, subscriptions: [{"a", 1}]}, <<1>>, 1}
      )

    {state, []} =
      Broker.transition(
        state,
        {:packet, :one, 1, %GravitonMQ.MQTT.Packet.Unsubscribe{packet_id: 6, filters: ["a"]},
         <<2>>, 2}
      )

    {state, effects} = Broker.transition(state, {:auth_result, op, 1, [:ok], 3})
    assert state.sessions["one"].subscriptions == %{}

    assert [%Suback{}, %GravitonMQ.MQTT.Packet.Unsuback{}] =
             for({:send, :one, _, _, packet} <- effects, do: packet)
  end

  test "pending client packet identifiers cannot be reused for a different request" do
    {state, _} = attach(new(), :one)

    {state, _} =
      Broker.transition(
        state,
        {:packet, :one, 1, %Subscribe{packet_id: 5, subscriptions: [{"a", 1}]}, <<1>>, 1}
      )

    {_state, effects} =
      Broker.transition(
        state,
        {:packet, :one, 1, %Publish{topic: "a", payload: "x", qos: 1, packet_id: 5}, <<2>>, 2}
      )

    assert Enum.any?(effects, &match?({:close, :one, _, :packet_identifier_in_use}, &1))
  end

  test "expired session is discarded on CONNECT even before the next timer tick" do
    {state, _} = attach(new(max_sessions: 1), :one, 1, properties: [session_expiry_interval: 1])
    {state, _} = subscribe(state, :one, [{"a", 1}])
    {state, _} = Broker.transition(state, {:connection_down, :one, 1, :normal, 10})
    {state, _} = Broker.transition(state, {:connection_open, :resumed, 2, 2010})
    connect = %Connect{version: :v5, client_id: "one", clean_start: false}
    {state, effects} = Broker.transition(state, {:packet, :resumed, 2, connect, <<16>>, 2010})
    {state, effects} = allowed(state, effects, 2010)

    assert Enum.any?(
             effects,
             &match?(
               {:send, :resumed, _, _, %Connack{reason_code: 0, session_present: false}},
               &1
             )
           )

    assert state.sessions["one"].subscriptions == %{}
  end

  test "pressure retries do not reset a due Will message expiry" do
    will = %Will{topic: "a", payload: "will", properties: [message_expiry_interval: 1]}
    {state, _} = attach(new(max_pending_deliveries: 0), :receiver)
    {state, _} = subscribe(state, :receiver, [{"a", 0}])
    {state, _} = attach(state, :sender, 1, will: will)
    {state, _} = Broker.transition(state, {:connection_down, :sender, 1, :abnormal, 10})
    {state, effects} = Broker.transition(state, {:tick, 10})
    {state, _} = allowed(state, effects, 10)
    {state, effects} = Broker.transition(state, {:tick, 1010})
    {state, effects} = allowed(state, effects, 1010)
    assert state.wills == %{}
    refute Enum.any?(effects, &match?({:send, _, _, _, %Publish{}}, &1))
  end

  test "Will payload format indication is validated before authentication" do
    {state, _} = Broker.transition(new(), {:connection_open, :one, 1, 0})

    connect = %Connect{
      version: :v5,
      client_id: "one",
      clean_start: true,
      will: %Will{topic: "a", payload: <<255>>, properties: [payload_format_indicator: 1]}
    }

    {_state, effects} = Broker.transition(state, {:packet, :one, 1, connect, <<16>>, 1})
    assert Enum.any?(effects, &match?({:send, :one, _, _, %Connack{reason_code: 0x99}}, &1))
    refute Enum.any?(effects, &match?({:authorize, _, _, _}, &1))
  end

  test "stale packet and authorization generations cannot advance expiry state" do
    {state, _} = attach(new(), :sender)

    {state, _} =
      packet(
        state,
        :sender,
        %Publish{
          topic: "a",
          payload: "x",
          retain: true,
          properties: [message_expiry_interval: 1]
        },
        10
      )

    {same, []} =
      Broker.transition(
        state,
        {:packet, :sender, 0, %GravitonMQ.MQTT.Packet.Pingreq{}, <<192, 0>>, 9999}
      )

    assert same == state
    {same, []} = Broker.transition(state, {:auth_result, 999, 0, [:ok], 9999})
    assert same == state
  end

  test "retained and prepared byte limits reject without side effects" do
    for limits <- [[max_retained_bytes: 0], [max_prepared_bytes: 1]] do
      {state, effects} =
        Broker.transition(
          new(limits),
          {:host_operation, :owner, :pub,
           {:publish, %Publish{topic: "a", payload: "x", retain: true}, <<1>>}, 1}
        )

      {state, effects} = allowed(state, effects, 1)
      assert state.retained == %{}
      assert state.operations == %{}
      assert Enum.any?(effects, &match?({:complete, :pub, {:error, _}}, &1))
    end
  end

  test "denied recipient delivery is distinct from failed authorization infrastructure" do
    {state, _} = attach(new(), :sender)
    {state, _} = attach(state, :receiver)
    {state, _} = subscribe(state, :receiver, [{"a", 1}])

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(
        state,
        {:packet, :sender, 1,
         %Publish{topic: "a", payload: "x", qos: 1, packet_id: 3, retain: true}, <<1>>, 2}
      )

    {state, effects} =
      Broker.transition(state, {:auth_result, op, 1, [:ok, {:error, :denied}], 3})

    assert state.retained["a"].payload == "x"
    assert state.sessions["receiver"].deliveries == []
    assert Enum.any?(effects, &match?({:send, :sender, _, _, %Puback{reason_code: 0}}, &1))
  end

  test "Will byte budget accounts for retained authenticated principal and job data" do
    {state, _} = Broker.transition(new(max_will_bytes: 128), {:connection_open, :one, 1, 0})

    connect = %Connect{
      version: :v5,
      client_id: "one",
      clean_start: true,
      will: %Will{topic: "a", payload: "x"}
    }

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(state, {:packet, :one, 1, connect, <<16>>, 1})

    {state, effects} =
      Broker.transition(state, {:auth_result, op, 1, {:ok, String.duplicate("p", 512)}, 2})

    {state, effects} = allowed(state, effects, 2)
    assert state.wills == %{}
    assert state.sessions == %{}
    assert Enum.any?(effects, &match?({:send, :one, _, _, %Connack{reason_code: 0x97}}, &1))
  end

  test "host subscription byte budget includes the bound principal" do
    {state, effects} =
      Broker.transition(
        new(max_subscription_bytes: 128),
        {:host_operation, :owner, :sub, {:as, String.duplicate("p", 512), {:subscribe, "a", 1}},
         0}
      )

    {state, effects} = allowed(state, effects)
    assert state.hosts == %{}
    assert {:complete, :sub, {:error, :capacity}} in effects
  end

  test "incoming unacknowledged publications have an aggregate byte budget" do
    {state, _} = attach(new(max_incoming_bytes: 0), :one)

    {state, effects} =
      Broker.transition(
        state,
        {:packet, :one, 1, %Publish{topic: "a", payload: "x", qos: 1, packet_id: 9}, <<1>>, 1}
      )

    refute Enum.any?(effects, &match?({:authorize, _, _, _}, &1))
    refute Enum.any?(effects, &match?({:send, :one, _, _, %Puback{reason_code: 0}}, &1))
    assert state.operations == %{}
  end

  test "active session takeover with expiry zero safely resumes the existing subscriptions" do
    {state, _} = attach(new(), :original)
    {state, _} = subscribe(state, :original, [{"a", 1}])
    {state, _} = Broker.transition(state, {:connection_open, :replacement, 2, 1})
    connect = %Connect{version: :v5, client_id: "original", clean_start: false}
    {state, effects} = Broker.transition(state, {:packet, :replacement, 2, connect, <<16>>, 1})
    {state, effects} = allowed(state, effects, 1)
    assert state.sessions["original"].connection == {:replacement, 2}
    assert Map.has_key?(state.sessions["original"].subscriptions, "a")
    assert Enum.any?(effects, &match?({:close, :original, _, :session_taken_over}, &1))
  end

  test "expired retained replay does not consume capacity while subscription authorization waits" do
    {state, _} = attach(new(max_pending_deliveries: 0), :sender)

    {state, _} =
      packet(
        state,
        :sender,
        %Publish{
          topic: "a",
          payload: "x",
          retain: true,
          properties: [message_expiry_interval: 1]
        },
        1
      )

    {state, _} = attach(state, :receiver)

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(
        state,
        {:packet, :receiver, 1, %Subscribe{packet_id: 9, subscriptions: [{"a", 1}]}, <<1>>, 2}
      )

    {state, effects} = Broker.transition(state, {:auth_result, op, 1, [:ok, :ok], 1001})
    assert [{:authorize, ^op, [{:authorize, :user, :subscribe, "a"}], _}] = effects
    {_state, effects} = allowed(state, effects, 1001)
    assert Enum.any?(effects, &match?({:send, :receiver, _, _, %Suback{reason_codes: [1]}}, &1))
  end

  test "negative CONNACK also respects the CONNECT maximum packet size" do
    {state, _} = Broker.transition(new(), {:connection_open, :one, 1, 0})

    connect = %Connect{
      version: :v5,
      client_id: "one",
      clean_start: true,
      properties: [maximum_packet_size: 1, authentication_method: "unsupported"]
    }

    {state, effects} = Broker.transition(state, {:packet, :one, 1, connect, <<16>>, 1})
    assert state.connections == %{}
    refute Enum.any?(effects, &match?({:send, _, _, _, _}, &1))
    assert Enum.any?(effects, &match?({:close, :one, 1, _}, &1))
  end

  defp flush_writes(state) do
    Enum.reduce(state.writes, state, fn {sendid, write}, state ->
      {id, generation} = write.connection
      elem(Broker.transition(state, {:write_result, id, generation, sendid, :ok, 0}), 0)
    end)
  end

  test "resuming several offline entries halts safely when CONNACK occupies the write budget" do
    {state, _} =
      attach(new(max_writes: 1), :receiver, 1, properties: [session_expiry_interval: 60])

    state = flush_writes(state)
    {state, _} = subscribe(state, :receiver, [{"a", 1}])
    state = flush_writes(state)
    {state, _} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 1})

    state =
      Enum.reduce(1..3, state, fn n, state ->
        {state, effects} =
          Broker.transition(
            state,
            {:host_operation, :publisher, n,
             {:publish, %Publish{topic: "a", payload: to_string(n), qos: 1}, <<1>>}, 2}
          )

        elem(allowed(state, effects, 2), 0)
      end)

    {state, _} = Broker.transition(state, {:connection_open, :resumed, 2, 3})
    connect = %Connect{version: :v5, client_id: "receiver", clean_start: false}
    {state, effects} = Broker.transition(state, {:packet, :resumed, 2, connect, <<16>>, 3})
    {state, effects} = allowed(state, effects, 3)
    assert state.sessions == %{}
    assert Enum.any?(effects, &match?({:close, :resumed, 2, :write_limit}, &1))
  end

  test "queued host delivery reauthorizes the actual topic and respects revocation" do
    {state, effects} =
      Broker.transition(new(), {:host_operation, :owner, :sub, {:subscribe, "a/#", 1}, 0})

    {state, [{:complete, :sub, {:ok, subscription}}]} = allowed(state, effects)

    {state, effects} =
      Broker.transition(
        state,
        {:host_operation, :publisher, :pub,
         {:publish, %Publish{topic: "a/b", payload: "queued", qos: 1}, <<1>>}, 1}
      )

    {state, _} = allowed(state, effects, 1)

    {state, effects} =
      Broker.transition(state, {:host_operation, :owner, :demand, {:demand, subscription, 1}, 2})

    refute Enum.any?(effects, &match?({:host_delivery, _, _, _}, &1))

    assert [{:authorize, dispatch, [{:authorize, {:host, :owner}, :deliver, "a/b"}], _}] =
             Enum.filter(effects, &match?({:authorize, _, _, _}, &1))

    {state, effects} =
      Broker.transition(state, {:auth_result, dispatch, 0, [{:error, :denied}], 3})

    assert state.hosts[subscription].deliveries == []
    refute Enum.any?(effects, &match?({:host_delivery, _, _, _}, &1))
  end

  test "dispatch infrastructure failure preserves accepted ownership and retries with finite backoff" do
    {state, effects} =
      Broker.transition(new(), {:host_operation, :owner, :sub, {:subscribe, "a", 1}, 0})

    {state, [{:complete, :sub, {:ok, subscription}}]} = allowed(state, effects)

    {state, effects} =
      Broker.transition(
        state,
        {:host_operation, :publisher, :pub,
         {:publish, %Publish{topic: "a", payload: "queued", qos: 1}, <<1>>}, 1}
      )

    {state, effects} = allowed(state, effects, 1)
    assert {:complete, :pub, {:ok, :accepted_memory}} in effects

    {state, effects} =
      Broker.transition(state, {:host_operation, :owner, :demand, {:demand, subscription, 1}, 2})

    assert [{:authorize, dispatch, _, _}] =
             Enum.filter(effects, &match?({:authorize, _, _, _}, &1))

    {state, []} =
      Broker.transition(state, {:auth_result, dispatch, 0, [{:error, :callback_timeout}], 3})

    assert [%{message: %{payload: "queued"}, delivered: false, retry_at: retry_at}] =
             state.hosts[subscription].deliveries

    assert retry_at > 3
    {state, []} = Broker.transition(state, {:tick, retry_at - 1})
    {state, effects} = Broker.transition(state, {:tick, retry_at})
    assert Enum.any?(effects, &match?({:authorize, _, _, _}, &1))
    {_state, effects} = allowed(state, effects, retry_at)

    assert Enum.any?(
             effects,
             &match?({:host_delivery, ^subscription, _, %{payload: "queued"}}, &1)
           )
  end

  test "retained replacement during SUBSCRIBE authorization refreshes and reauthorizes the snapshot" do
    {state, effects} =
      Broker.transition(
        new(),
        {:host_operation, :publisher, :old,
         {:publish, %Publish{topic: "a", payload: "old", retain: true}, <<1>>}, 0}
      )

    {state, _} = allowed(state, effects)
    {state, _} = attach(state, :receiver)

    {state, [{:authorize, op, _, deadline}]} =
      Broker.transition(
        state,
        {:packet, :receiver, 1, %Subscribe{packet_id: 9, subscriptions: [{"a", 1}]}, <<1>>, 1}
      )

    {state, effects} =
      Broker.transition(
        state,
        {:host_operation, :publisher, :new,
         {:publish, %Publish{topic: "a", payload: "new", retain: true}, <<2>>}, 2}
      )

    {state, _} = allowed(state, effects, 2)
    {state, effects} = Broker.transition(state, {:auth_result, op, 1, [:ok, :ok], 3})
    assert [{:authorize, ^op, _, ^deadline}] = effects
    {state, effects} = allowed(state, effects, 3)
    assert state.retained["a"].payload == "new"

    assert ["new"] ==
             for({:send, :receiver, _, _, %Publish{payload: payload}} <- effects, do: payload)
  end

  test "assigned client identifiers skip existing and pending explicit identifiers" do
    {state, _} = Broker.transition(new(), {:connection_open, :victim, 1, 0})
    connect = %Connect{version: :v5, client_id: "graviton-5", clean_start: true}
    {state, effects} = Broker.transition(state, {:packet, :victim, 1, connect, <<16>>, 0})
    {state, _} = allowed(state, effects)
    {state, _} = Broker.transition(state, {:connection_open, :anonymous, 1, 0})

    {state, effects} =
      Broker.transition(state, {:packet, :anonymous, 1, %{connect | client_id: ""}, <<16>>, 0})

    {state, effects} = allowed(state, effects)
    assert state.sessions["graviton-5"].connection == {:victim, 1}
    assert [{:send, :anonymous, _, _, %Connack{properties: properties}}] = effects
    assert properties[:assigned_client_identifier] != "graviton-5"
    refute Enum.any?(effects, &match?({:close, :victim, _, _}, &1))
  end

  test "offline network admission reauthorizes before first delivery on resume" do
    {state, _} = attach(new(), :receiver, 1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :receiver, [{"a", 1}])
    {state, _} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 1})

    {state, effects} =
      Broker.transition(
        state,
        {:host_operation, :publisher, :pub,
         {:publish, %Publish{topic: "a", payload: "offline", qos: 1}, <<1>>}, 2}
      )

    {state, _} = allowed(state, effects, 2)
    {state, _} = Broker.transition(state, {:connection_open, :resumed, 2, 3})

    connect = %Connect{
      version: :v5,
      client_id: "receiver",
      clean_start: false,
      properties: [session_expiry_interval: 60]
    }

    {state, [{:authorize, connect_op, _, _}]} =
      Broker.transition(state, {:packet, :resumed, 2, connect, <<16>>, 3})

    {state, effects} = Broker.transition(state, {:auth_result, connect_op, 2, {:ok, :user}, 4})

    assert [{:authorize, dispatch, [{:authorize, :user, :deliver, "a"}], _}] =
             Enum.filter(effects, &match?({:authorize, _, _, _}, &1))

    refute Enum.any?(effects, &match?({:send, :resumed, _, _, %Publish{}}, &1))

    {state, effects} =
      Broker.transition(state, {:auth_result, dispatch, 2, [{:error, :denied}], 5})

    assert state.sessions["receiver"].deliveries == []
    refute Enum.any?(effects, &match?({:send, :resumed, _, _, %Publish{}}, &1))
  end

  test "dispatch authorization is fenced by connection lifetime and entry expiry" do
    {state, _} = attach(new(), :receiver, 1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :receiver, [{"a", 1}])

    {state, [{:authorize, publication, _, _}]} =
      Broker.transition(
        state,
        {:host_operation, :publisher, :pub,
         {:publish,
          %Publish{topic: "a", payload: "x", qos: 1, properties: [message_expiry_interval: 1]},
          <<1>>}, 2}
      )

    {state, effects} = Broker.transition(state, {:auth_result, publication, 0, [:ok, :ok], 3})

    assert [{:authorize, dispatch, _, _}] =
             Enum.filter(effects, &match?({:authorize, _, _, _}, &1))

    {state, _} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 4})
    {same, []} = Broker.transition(state, {:auth_result, dispatch, 1, [:ok], 5})
    assert same == state
    {state, _} = Broker.transition(state, {:tick, 1002})
    assert state.sessions["receiver"].deliveries == []
    assert state.operations == %{}
  end

  test "already-sent QoS1 is neither periodically retransmitted nor retired on resumed denial" do
    {state, _} = attach(new(), :receiver, 1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :receiver, [{"a", 1}])

    {state, effects} =
      Broker.transition(
        state,
        {:host_operation, :publisher, :pub,
         {:publish, %Publish{topic: "a", payload: "responsibility", qos: 1}, <<1>>}, 1}
      )

    {state, effects} = allowed(state, effects, 1)

    assert Enum.any?(
             effects,
             &match?({:send, :receiver, _, _, %Publish{packet_id: 1, dup: false}}, &1)
           )

    {state, effects} = Broker.transition(state, {:tick, 2})
    refute Enum.any?(effects, &match?({:authorize, _, _, _}, &1))
    refute Enum.any?(effects, &match?({:send, _, _, _, %Publish{}}, &1))
    {state, _} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 3})
    {state, _} = Broker.transition(state, {:connection_open, :resumed, 2, 4})

    connect = %Connect{
      version: :v5,
      client_id: "receiver",
      clean_start: false,
      properties: [session_expiry_interval: 60]
    }

    {state, [{:authorize, connect_op, _, _}]} =
      Broker.transition(state, {:packet, :resumed, 2, connect, <<16>>, 4})

    {state, effects} = Broker.transition(state, {:auth_result, connect_op, 2, {:ok, :user}, 5})

    assert [{:authorize, dispatch, _, _}] =
             Enum.filter(effects, &match?({:authorize, _, _, _}, &1))

    {state, effects} =
      Broker.transition(state, {:auth_result, dispatch, 2, [{:error, :denied}], 6})

    assert [%{packet_id: 1, ever_sent: true, message: %{payload: "responsibility"}}] =
             state.sessions["receiver"].deliveries

    assert state.sessions["receiver"].connection == nil
    assert Enum.any?(effects, &match?({:close, :resumed, 2, :delivery_denied}, &1))
  end

  test "retained replay requires a separate dispatch authorization after SUBACK" do
    {state, effects} =
      Broker.transition(
        new(),
        {:host_operation, :publisher, :pub,
         {:publish, %Publish{topic: "a", payload: "retained", retain: true}, <<1>>}, 0}
      )

    {state, _} = allowed(state, effects)
    {state, _} = attach(state, :receiver)

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(
        state,
        {:packet, :receiver, 1, %Subscribe{packet_id: 9, subscriptions: [{"a", 1}]}, <<1>>, 1}
      )

    {state, effects} = Broker.transition(state, {:auth_result, op, 1, [:ok, :ok], 2})
    assert Enum.any?(effects, &match?({:send, :receiver, _, _, %Suback{}}, &1))

    assert [{:authorize, dispatch, _, _}] =
             Enum.filter(effects, &match?({:authorize, _, _, _}, &1))

    {state, effects} =
      Broker.transition(state, {:auth_result, dispatch, 1, [{:error, :denied}], 3})

    refute Enum.any?(effects, &match?({:send, :receiver, _, _, %Publish{}}, &1))
    assert state.retained["a"].payload == "retained"
    assert state.sessions["receiver"].deliveries == []
  end

  test "host retained subscribe refreshes its authorization snapshot before admission" do
    {state, effects} =
      Broker.transition(
        new(),
        {:host_operation, :publisher, :old,
         {:publish, %Publish{topic: "a", payload: "old", retain: true}, <<1>>}, 0}
      )

    {state, _} = allowed(state, effects)

    {state, [{:authorize, op, _, deadline}]} =
      Broker.transition(state, {:host_operation, :owner, :sub, {:subscribe, "a", 1}, 1})

    {state, effects} =
      Broker.transition(
        state,
        {:host_operation, :publisher, :new,
         {:publish, %Publish{topic: "a", payload: "new", retain: true}, <<2>>}, 2}
      )

    {state, _} = allowed(state, effects, 2)

    {state, [{:authorize, ^op, _, ^deadline}]} =
      Broker.transition(state, {:auth_result, op, 0, [:ok, :ok], 3})

    {state, [{:complete, :sub, {:ok, subscription}}]} =
      Broker.transition(state, {:auth_result, op, 0, [:ok, :ok], 4})

    assert [%{message: %{payload: "new"}}] = state.hosts[subscription].deliveries

    {state, effects} =
      Broker.transition(state, {:host_operation, :owner, :demand, {:demand, subscription, 1}, 5})

    {_state, effects} = allowed(state, effects, 5)
    assert Enum.any?(effects, &match?({:host_delivery, ^subscription, _, %{payload: "new"}}, &1))
  end

  test "assigned identifiers also skip an explicit identifier still awaiting authentication" do
    {state, _} = Broker.transition(new(), {:connection_open, :reserved, 1, 0})
    connect = %Connect{version: :v5, client_id: "graviton-3", clean_start: true}

    {state, [{:authorize, pending, _, _}]} =
      Broker.transition(state, {:packet, :reserved, 1, connect, <<16>>, 0})

    {state, _} = Broker.transition(state, {:connection_open, :anonymous, 1, 0})

    {state, [{:authorize, anonymous, _, _}]} =
      Broker.transition(state, {:packet, :anonymous, 1, %{connect | client_id: ""}, <<16>>, 0})

    {state, [{:send, :anonymous, _, _, %Connack{properties: properties}}]} =
      Broker.transition(state, {:auth_result, anonymous, 1, {:ok, :user}, 1})

    assert properties[:assigned_client_identifier] != "graviton-3"
    {state, effects} = Broker.transition(state, {:auth_result, pending, 1, {:ok, :user}, 2})
    assert map_size(state.sessions) == 2
    refute Enum.any?(effects, &match?({:close, :anonymous, _, _}, &1))
  end
end
