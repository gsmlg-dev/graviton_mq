defmodule GravitonMQ.MQTT.BrokerQoS2Test do
  use ExUnit.Case, async: true
  alias GravitonMQ.MQTT.Broker

  alias GravitonMQ.MQTT.Packet.{
    Connect,
    Connack,
    Publish,
    Subscribe,
    Suback,
    Pubrec,
    Pubrel,
    Pubcomp,
    Puback
  }

  defp new(options \\ []) do
    {:ok, state} = Broker.new(options)
    state
  end

  defp allow(state, effects, now) do
    Enum.reduce(effects, {state, []}, fn
      {:authorize, op, request, _}, {state, output} ->
        decision = if is_list(request), do: Enum.map(request, fn _ -> :ok end), else: {:ok, :user}

        {state, more} =
          Broker.transition(
            state,
            {:auth_result, op, state.operations[op].generation, decision, now}
          )

        {state, more} = allow(state, more, now)
        {state, output ++ more}

      effect, {state, output} ->
        {state, output ++ [effect]}
    end)
  end

  defp connect(state, id, client_id \\ nil, options \\ []) do
    generation = Keyword.get(options, :generation, 1)
    {state, []} = Broker.transition(state, {:connection_open, id, generation, 0})

    packet = %Connect{
      version: :v5,
      client_id: client_id || to_string(id),
      clean_start: Keyword.get(options, :clean_start, true),
      properties: Keyword.get(options, :properties, [])
    }

    event(state, id, packet, 0)
  end

  defp event(state, id, packet, now \\ 1) do
    {state, effects} =
      Broker.transition(
        state,
        {:packet, id, state.connections[id].generation, packet, <<1>>, now}
      )

    allow(state, effects, now)
  end

  defp subscribe(state, id, qos \\ 2),
    do: event(state, id, %Subscribe{packet_id: 90, subscriptions: [{"a", qos}]})

  defp publication(state, payload \\ "body", qos \\ 2) do
    packet = %Publish{topic: "a", payload: payload, qos: qos}

    {state, effects} =
      Broker.transition(
        state,
        {:host_operation, :host, make_ref(), {:publish, packet, payload}, 1}
      )

    allow(state, effects, 1)
  end

  defp sent(effects, id, module),
    do: for({:send, ^id, _, _, %{__struct__: ^module} = packet} <- effects, do: packet)

  test "QoS2 capability omits Maximum QoS and QoS1 profile still negotiates down" do
    {_state, effects} = connect(new(), :peer)
    assert [%Connack{properties: properties}] = sent(effects, :peer, Connack)
    refute Keyword.has_key?(properties, :maximum_qos)
    {state, effects} = connect(new(maximum_qos: 1), :limited)
    assert [%Connack{properties: properties}] = sent(effects, :limited, Connack)
    assert properties[:maximum_qos] == 1
    {_state, effects} = subscribe(state, :limited)
    assert [%Suback{reason_codes: [1]}] = sent(effects, :limited, Suback)
  end

  test "incoming QoS2 forwards once, repeats PUBREC, completes and permits identifier reuse" do
    {state, _} = connect(new(), :publisher)
    {state, _} = connect(state, :receiver)
    {state, _} = subscribe(state, :receiver)
    packet = %Publish{topic: "a", payload: "one", qos: 2, packet_id: 7}
    {state, effects} = event(state, :publisher, packet)
    assert [%Pubrec{packet_id: 7, reason_code: 0}] = sent(effects, :publisher, Pubrec)
    assert [%Publish{qos: 2, payload: "one"}] = sent(effects, :receiver, Publish)
    {state, effects} = event(state, :publisher, %{packet | dup: true})
    assert [%Pubrec{packet_id: 7}] = sent(effects, :publisher, Pubrec)
    assert sent(effects, :receiver, Publish) == []
    assert length(state.sessions["receiver"].deliveries) == 1
    {state, effects} = event(state, :publisher, %Pubrel{packet_id: 7})
    assert [%Pubcomp{reason_code: 0}] = sent(effects, :publisher, Pubcomp)
    {state, effects} = event(state, :publisher, %Pubrel{packet_id: 7})
    assert [%Pubcomp{reason_code: 0x92}] = sent(effects, :publisher, Pubcomp)
    {state, _} = event(state, :publisher, %{packet | payload: "two"})
    assert length(state.sessions["receiver"].deliveries) == 2
  end

  test "Receive Maximum is held through PUBCOMP rather than PUBREC" do
    {state, _} = connect(new(receive_maximum: 1), :publisher)
    {state, _} = event(state, :publisher, %Publish{topic: "a", payload: "", qos: 2, packet_id: 1})

    {_closed, effects} =
      event(state, :publisher, %Publish{topic: "a", payload: "", qos: 1, packet_id: 2})

    assert Enum.any?(effects, &match?({:close, :publisher, _, :receive_maximum_exceeded}, &1))
    {state, _} = event(state, :publisher, %Pubrel{packet_id: 1})

    {_state, effects} =
      event(state, :publisher, %Publish{topic: "a", payload: "", qos: 1, packet_id: 2})

    assert [%Puback{packet_id: 2}] = sent(effects, :publisher, Puback)
  end

  test "accepted incoming QoS2 survives connection resumption without duplicate forwarding" do
    {state, _} = connect(new(), :publisher, nil, properties: [session_expiry_interval: 60])
    {state, _} = connect(state, :receiver)
    {state, _} = subscribe(state, :receiver)
    packet = %Publish{topic: "a", payload: "", qos: 2, packet_id: 5}
    {state, _} = event(state, :publisher, packet)
    {state, _} = Broker.transition(state, {:connection_down, :publisher, 1, :abnormal, 2})

    {state, _} =
      connect(state, :resumed, "publisher",
        clean_start: false,
        generation: 2,
        properties: [session_expiry_interval: 60]
      )

    {state, effects} = event(state, :resumed, %{packet | dup: true})
    assert [%Pubrec{packet_id: 5}] = sent(effects, :resumed, Pubrec)
    assert sent(effects, :receiver, Publish) == []
    assert length(state.sessions["receiver"].deliveries) == 1
    {_state, effects} = event(state, :resumed, %Pubrel{packet_id: 5})
    assert [%Pubcomp{reason_code: 0}] = sent(effects, :resumed, Pubcomp)
  end

  test "outgoing QoS2 keeps credit until PUBCOMP and negative PUBREC releases it" do
    {state, _} = connect(new(), :receiver, nil, properties: [receive_maximum: 1])
    {state, _} = subscribe(state, :receiver)
    {state, effects} = publication(state, "first")
    assert [%Publish{packet_id: first, qos: 2}] = sent(effects, :receiver, Publish)
    {state, effects} = publication(state, "second")
    assert sent(effects, :receiver, Publish) == []
    {state, effects} = event(state, :receiver, %Pubrec{packet_id: first})
    assert [%Pubrel{packet_id: ^first}] = sent(effects, :receiver, Pubrel)
    assert sent(effects, :receiver, Publish) == []
    {state, effects} = event(state, :receiver, %Pubrec{packet_id: first})
    assert [%Pubrel{packet_id: ^first}] = sent(effects, :receiver, Pubrel)
    {state, effects} = event(state, :receiver, %Pubcomp{packet_id: first})
    assert [%Publish{payload: "second", packet_id: second}] = sent(effects, :receiver, Publish)
    {state, _} = event(state, :receiver, %Pubrec{packet_id: second, reason_code: 0x87})
    assert state.sessions["receiver"].deliveries == []
    assert state.connections.receiver.send_quota == 1
  end

  test "resumed PUBREL needs no PUBLISH quota and never replays the application message" do
    {state, _} =
      connect(new(), :receiver, nil,
        properties: [receive_maximum: 1, session_expiry_interval: 60]
      )

    {state, _} = subscribe(state, :receiver)
    {state, _} = publication(state, "first")
    {state, _} = event(state, :receiver, %Pubrec{packet_id: 1})
    {state, _} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 2})
    {state, _} = publication(state, "second")

    {state, effects} =
      connect(state, :resumed, "receiver",
        clean_start: false,
        generation: 2,
        properties: [receive_maximum: 1, session_expiry_interval: 60]
      )

    assert [%Pubrel{packet_id: 1}] = sent(effects, :resumed, Pubrel)
    assert [%Publish{payload: "second", dup: false}] = sent(effects, :resumed, Publish)
    refute Enum.any?(sent(effects, :resumed, Publish), &(&1.payload == "first"))
    assert state.connections.resumed.send_quota == 0
  end

  test "resumed unacknowledged QoS2 PUBLISH keeps its identifier and DUP" do
    {state, _} = connect(new(), :receiver, nil, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :receiver)
    {state, _} = publication(state)
    {state, _} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 2})

    {_state, effects} =
      connect(state, :resumed, "receiver",
        clean_start: false,
        generation: 2,
        properties: [session_expiry_interval: 60]
      )

    assert [%Publish{packet_id: 1, qos: 2, dup: true}] = sent(effects, :resumed, Publish)
    assert sent(effects, :resumed, Pubrel) == []
  end

  test "resumed PUBREL retains PUBREC order rather than original PUBLISH order" do
    {state, _} = connect(new(), :receiver, nil, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :receiver)
    {state, _} = publication(state, "first")
    {state, _} = publication(state, "second")
    {state, _} = event(state, :receiver, %Pubrec{packet_id: 2})
    {state, _} = event(state, :receiver, %Pubrec{packet_id: 1})
    # A repeated PUBREC must not move the established release order.
    {state, _} = event(state, :receiver, %Pubrec{packet_id: 2})
    {state, _} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 2})

    {state, effects} =
      connect(state, :resumed, "receiver",
        clean_start: false,
        generation: 2,
        properties: [session_expiry_interval: 60]
      )

    assert Enum.map(sent(effects, :resumed, Pubrel), & &1.packet_id) == [2, 1]
    assert sent(effects, :resumed, Publish) == []
    {state, effects} = publication(state, "third")
    assert [%Publish{packet_id: 3}] = sent(effects, :resumed, Publish)
    assert Enum.map(state.sessions["receiver"].deliveries, & &1.packet_id) == [1, 2, 3]
  end

  test "resumed PUBREL bypasses exhausted PUBLISH quota without reordering PUBLISH" do
    {state, _} = connect(new(), :receiver, nil, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :receiver)
    {state, _} = publication(state, "first")
    {state, _} = publication(state, "second")
    {state, _} = publication(state, "third")
    {state, _} = event(state, :receiver, %Pubrec{packet_id: 3})
    {state, _} = Broker.transition(state, {:connection_down, :receiver, 1, :abnormal, 2})

    {state, effects} =
      connect(state, :resumed, "receiver",
        clean_start: false,
        generation: 2,
        properties: [session_expiry_interval: 60, receive_maximum: 1]
      )

    assert [%Pubrel{packet_id: 3}] = sent(effects, :resumed, Pubrel)

    assert [%Publish{packet_id: 1, payload: "first", dup: true}] =
             sent(effects, :resumed, Publish)

    {state, effects} = event(state, :resumed, %Pubcomp{packet_id: 3})

    assert [%Publish{packet_id: 2, payload: "second", dup: true}] =
             sent(effects, :resumed, Publish)

    assert Enum.map(state.sessions["receiver"].deliveries, & &1.packet_id) == [1, 2]
  end

  test "oversized outbound application messages are discarded without disconnecting the peer" do
    {state, _} = connect(new(), :receiver, nil, properties: [maximum_packet_size: 100])
    {state, _} = subscribe(state, :receiver)
    {state, effects} = publication(state, String.duplicate("x", 200))
    assert sent(effects, :receiver, Publish) == []
    refute Enum.any?(effects, &match?({:close, :receiver, _, _}, &1))
    assert state.sessions["receiver"].deliveries == []
    {_state, effects} = publication(state, "small")
    assert [%Publish{payload: "small"}] = sent(effects, :receiver, Publish)
  end

  test "QoS1-only profile rejects QoS2 publication instead of accepting unsupported ownership" do
    {state, _} = connect(new(maximum_qos: 1), :publisher)

    {_state, effects} =
      event(state, :publisher, %Publish{topic: "a", payload: "", qos: 2, packet_id: 1})

    assert Enum.any?(effects, &match?({:close, :publisher, _, :qos_not_supported}, &1))
    assert sent(effects, :publisher, Pubrec) == []
  end

  test "negative PUBREC releases the identifier without retaining or forwarding the message" do
    {state, _} = connect(new(), :publisher)
    packet = %Publish{topic: "a", payload: "denied", retain: true, qos: 2, packet_id: 7}

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(state, {:packet, :publisher, 1, packet, <<1>>, 1})

    {state, effects} = Broker.transition(state, {:auth_result, op, 1, [{:error, :denied}], 1})
    assert [%Pubrec{reason_code: 0x87}] = sent(effects, :publisher, Pubrec)
    assert state.retained == %{}
    {state, effects} = event(state, :publisher, packet)
    assert [%Pubrec{reason_code: 0}] = sent(effects, :publisher, Pubrec)
    assert state.retained["a"].payload == "denied"
  end

  test "new-connection Receive Maximum excludes an older pending PUBREL" do
    {state, _} =
      connect(new(receive_maximum: 1), :publisher, nil, properties: [session_expiry_interval: 60])

    {state, _} = event(state, :publisher, %Publish{topic: "a", payload: "", qos: 2, packet_id: 1})
    {state, _} = Broker.transition(state, {:connection_down, :publisher, 1, :abnormal, 2})

    {state, _} =
      connect(state, :resumed, "publisher",
        clean_start: false,
        generation: 2,
        properties: [session_expiry_interval: 60]
      )

    {state, effects} =
      event(state, :resumed, %Publish{topic: "a", payload: "", qos: 2, packet_id: 2})

    assert [%Pubrec{packet_id: 2}] = sent(effects, :resumed, Pubrec)
    {state, _} = event(state, :resumed, %Pubrel{packet_id: 1})

    {_state, effects} =
      event(state, :resumed, %Publish{topic: "a", payload: "", qos: 2, packet_id: 3})

    assert Enum.any?(effects, &match?({:close, :resumed, _, :receive_maximum_exceeded}, &1))
  end

  test "expiry does not interrupt an already-started QoS2 acknowledgement sequence" do
    {state, _} = connect(new(), :receiver)
    {state, _} = subscribe(state, :receiver)

    packet = %Publish{
      topic: "a",
      payload: "expires",
      qos: 2,
      properties: [message_expiry_interval: 1]
    }

    {state, effects} =
      Broker.transition(state, {:host_operation, :host, :expiry, {:publish, packet, <<1>>}, 1})

    {state, _} = allow(state, effects, 1)
    {state, _} = Broker.transition(state, {:tick, 2_000})
    {state, effects} = event(state, :receiver, %Pubrec{packet_id: 1}, 2_001)
    assert [%Pubrel{packet_id: 1}] = sent(effects, :receiver, Pubrel)
    {state, _} = event(state, :receiver, %Pubcomp{packet_id: 1}, 2_002)
    assert state.sessions["receiver"].deliveries == []
  end
end
