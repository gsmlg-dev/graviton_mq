defmodule GravitonMQ.MQTT.ClientTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Client
  alias GravitonMQ.MQTT.Packet.{Connect, Connack, Disconnect, Pingreq, Pingresp, Puback, Publish}
  alias GravitonMQ.MQTT.Packet.{Suback, Subscribe, Unsuback, Unsubscribe, Will}

  defp connect(properties \\ []) do
    %Connect{
      version: :v5,
      client_id: "client",
      clean_start: true,
      keep_alive: 2,
      properties: properties
    }
  end

  defp online(properties \\ [], options \\ []) do
    {:ok, state} = Client.new(connect(Keyword.get(options, :connect_properties, [])), options)
    {state, [{:open, 1}]} = Client.transition(state, :start)
    {state, [{:send, %Connect{}, :connect}]} = Client.transition(state, {:transport_up, 1, 0})

    {state, effects} =
      Client.transition(
        state,
        {:packet, 1,
         %Connack{version: :v5, session_present: false, reason_code: 0, properties: properties},
         1}
      )

    assert {:notify, {:connected, state.peer}} in effects
    state
  end

  test "MQTT runtime profile rejects legacy version and enhanced authentication explicitly" do
    assert {:error, :unsupported_version} = Client.new(%{connect() | version: :v3_1_1})

    assert {:error, :enhanced_authentication_unsupported} =
             Client.new(connect(authentication_method: "method"))
  end

  test "a Will declaring UTF-8 rejects malformed character data before client admission" do
    for payload <- [<<0xFF>>, <<0xC0, 0x80>>, <<0xED, 0xA0, 0x80>>, <<0xE2, 0x82>>] do
      will = %Will{
        topic: "will/topic",
        payload: payload,
        properties: [payload_format_indicator: 1]
      }

      assert {:error, :payload_format_invalid} = Client.new(%{connect() | will: will})
    end
  end

  test "Will character data permits NUL and binary Will payloads retain arbitrary bytes" do
    for {payload, properties} <- [
          {"will-雪", [payload_format_indicator: 1]},
          {<<0>>, [payload_format_indicator: 1]},
          {<<>>, [payload_format_indicator: 1]},
          {<<0xFF>>, [payload_format_indicator: 0]},
          {<<0xFF>>, []}
        ] do
      will = %Will{topic: "will/topic", payload: payload, properties: properties}
      assert {:ok, state} = Client.new(%{connect() | will: will})
      {state, [{:open, 1}]} = Client.transition(state, :start)

      assert {_state, [{:send, %Connect{will: ^will}, :connect}]} =
               Client.transition(state, {:transport_up, 1, 0})
    end
  end

  test "Session Present cannot resume a session the client does not own" do
    connect = %{connect(session_expiry_interval: 60) | clean_start: false}
    {:ok, state} = Client.new(connect, reconnect_max: 2)
    {state, [{:open, 1}]} = Client.transition(state, :start)

    {state, [{:send, %Connect{clean_start: false}, :connect}]} =
      Client.transition(state, {:transport_up, 1, 0})

    {state, effects} =
      Client.transition(
        state,
        {:packet, 1, %Connack{version: :v5, session_present: true, reason_code: 0}, 1}
      )

    assert state.phase == :stopped
    assert {:close, :local_session_missing} in effects
    refute Enum.any?(effects, &match?({:notify, {:connected, _}}, &1))
    refute Enum.any?(effects, &match?({:retry, _}, &1))
  end

  test "QoS1 completes only on matching PUBACK, retaining negative rejection" do
    state = online()
    packet = %Publish{topic: "a", payload: "payload", qos: 1}

    {state, [{:send, sent, {:operation, :op}}]} =
      Client.transition(state, {:publish, :op, packet, 10})

    assert sent.packet_id == 1
    {state, []} = Client.transition(state, {:sent, 1, {:operation, :op}, :ok, 10})

    {state, [{:notify, {:operation, :op, {:error, {:puback, 0x87}}}}]} =
      Client.transition(state, {:packet, 1, %Puback{packet_id: 1, reason_code: 0x87}, 20})

    assert state.outbound == %{}
  end

  test "QoS0 completion reports only socket acceptance" do
    state = online()

    {state, [{:send, %Publish{packet_id: nil}, {:operation, :op}}]} =
      Client.transition(state, {:publish, :op, %Publish{topic: "a", payload: "x"}, 10})

    {_state, [{:notify, {:operation, :op, {:ok, :sent}}}]} =
      Client.transition(state, {:sent, 1, {:operation, :op}, :ok, 10})
  end

  test "negotiated QoS, retained availability, packet size and receive maximum constrain sends" do
    packet = %Publish{topic: "a", payload: "x", qos: 1}
    state = online(maximum_qos: 0, retain_available: 0)

    {^state, [{:notify, {:operation, :op, {:error, :qos_not_supported}}}]} =
      Client.transition(state, {:publish, :op, packet, 10})

    {^state, [{:notify, {:operation, :op, {:error, :retain_not_available}}}]} =
      Client.transition(state, {:publish, :op, %{packet | qos: 0, retain: true}, 10})

    state = online(maximum_packet_size: 5)

    {^state, [{:notify, {:operation, :op, {:error, :peer_packet_size}}}]} =
      Client.transition(state, {:publish, :op, %{packet | qos: 0}, 10})

    state = online(receive_maximum: 1)
    {state, _} = Client.transition(state, {:publish, :one, packet, 10})

    {^state, [{:notify, {:operation, :two, {:error, :inflight_full}}}]} =
      Client.transition(state, {:publish, :two, packet, 10})
  end

  test "SUBACK grants must match requested count and cannot exceed requested QoS" do
    state = online()

    {state, [{:send, %Subscribe{packet_id: 1}, _}]} =
      Client.transition(
        state,
        {:subscribe, :sub, %Subscribe{packet_id: 0, subscriptions: [{"a/#", 1}]}, 10}
      )

    {state, [{:notify, {:operation, :sub, {:ok, [0]}}}]} =
      Client.transition(state, {:packet, 1, %Suback{packet_id: 1, reason_codes: [0]}, 11})

    assert state.subscriptions == %{"a/#" => 0}

    {state, [{:send, %Unsubscribe{packet_id: 2}, _}]} =
      Client.transition(
        state,
        {:unsubscribe, :unsub, %Unsubscribe{packet_id: 0, filters: ["a/#"]}, 12}
      )

    {state, [{:notify, {:operation, :unsub, {:ok, [0]}}}]} =
      Client.transition(state, {:packet, 1, %Unsuback{packet_id: 2, reason_codes: [0]}, 13})

    assert state.subscriptions == %{}

    state = online()

    {state, _} =
      Client.transition(
        state,
        {:subscribe, :sub, %Subscribe{packet_id: 0, subscriptions: [{"a", 0}]}, 10}
      )

    {state, effects} =
      Client.transition(state, {:packet, 1, %Suback{packet_id: 1, reason_codes: [1]}, 11})

    assert state.phase == :stopped
    assert {:close, :invalid_suback} in effects
  end

  test "inbound QoS1 is bounded, demand driven and acknowledged only after host ownership" do
    state = online([], max_deliveries: 1)
    publish = %Publish{topic: "a", payload: "x", qos: 1, packet_id: 7}
    {state, []} = Client.transition(state, {:packet, 1, publish, 10})
    {state, []} = Client.transition(state, {:packet, 1, %{publish | dup: true}, 11})

    {state, [{:notify, {:delivery, 1, ^publish}}]} =
      Client.transition(state, {:demand, 1, 12})

    {state, [{:send, %Puback{packet_id: 7}, {:ack, 7}}]} =
      Client.transition(state, {:ack, 1, 13})

    {state, []} = Client.transition(state, {:sent, 1, {:ack, 7}, :ok, 13})
    assert state.incoming == %{}
    assert state.deliveries == %{}
  end

  test "a saturated receiver closes with an explicit quota error instead of acknowledging loss" do
    state = online([], max_deliveries: 1)
    packet = %Publish{topic: "a", payload: "x", qos: 1, packet_id: 1}
    {state, []} = Client.transition(state, {:packet, 1, packet, 10})
    {state, effects} = Client.transition(state, {:packet, 1, %{packet | packet_id: 2}, 11})
    assert state.phase == :stopped
    assert {:close, :receive_maximum_exceeded} in effects
    refute Enum.any?(effects, &match?({:send, %Puback{}, _}, &1))
  end

  test "keep alive obeys server interval and fences stale timer generations" do
    state = online(server_keep_alive: 1)
    {^state, []} = Client.transition(state, {:tick, 0, 5000})
    {state, [{:send, %Pingreq{}, :ping}]} = Client.transition(state, {:tick, 1, 1001})
    {state, []} = Client.transition(state, {:sent, 1, :ping, :ok, 1001})

    {state, [{:notify, :ping_response}]} =
      Client.transition(state, {:packet, 1, %Pingresp{}, 1002})

    assert state.ping_at == nil
    {state, _} = Client.transition(state, {:tick, 1, 2002})
    {_state, effects} = Client.transition(state, {:tick, 1, 3003})
    assert {:close, :keep_alive_timeout} in effects
  end

  test "reconnect retransmits a QoS1 only with an actual remote and local session" do
    state = online([], reconnect_max: 2, connect_properties: [session_expiry_interval: 60])

    {state, _} =
      Client.transition(state, {:publish, :op, %Publish{topic: "a", payload: "x", qos: 1}, 10})

    {state, []} = Client.transition(state, {:sent, 1, {:operation, :op}, :ok, 10})
    {state, effects} = Client.transition(state, {:transport_down, 1, :closed, 11})
    assert {:retry, 1} in effects
    {state, [{:open, 2}]} = Client.transition(state, {:retry, 12})

    {state, [{:send, %Connect{clean_start: false}, :connect}]} =
      Client.transition(state, {:transport_up, 2, 13})

    {state, effects} =
      Client.transition(
        state,
        {:packet, 2, %Connack{version: :v5, session_present: true, reason_code: 0}, 14}
      )

    assert {:send, %Publish{topic: "a", payload: "x", qos: 1, packet_id: 1, dup: true},
            {:operation, :op}} in effects

    {^state, []} = Client.transition(state, {:packet, 1, %Puback{packet_id: 1}, 15})
  end

  test "missing remote session reports unknown completion and clears lost subscription state" do
    state = online([], reconnect_max: 1, connect_properties: [session_expiry_interval: 60])

    {state, _} =
      Client.transition(state, {:publish, :op, %Publish{topic: "a", payload: "x", qos: 1}, 10})

    {state, _} = Client.transition(state, {:sent, 1, {:operation, :op}, :ok, 10})
    {state, _} = Client.transition(state, {:transport_down, 1, :closed, 11})
    {state, _} = Client.transition(state, {:retry, 12})
    {state, _} = Client.transition(state, {:transport_up, 2, 13})

    {state, effects} =
      Client.transition(
        state,
        {:packet, 2, %Connack{version: :v5, session_present: false, reason_code: 0}, 14}
      )

    assert {:notify, {:operation, :op, {:error, :unknown_outcome}}} in effects
    assert state.outbound == %{}
  end

  test "a CONNACK without Session Expiry uses the current CONNECT after a prior override" do
    for {requested, previous} <- [{60, 1}, {1, 60}] do
      state =
        online([session_expiry_interval: previous],
          reconnect_max: 2,
          connect_properties: [session_expiry_interval: requested]
        )

      {state, _} = Client.transition(state, {:transport_down, 1, :closed, 100})
      {state, _} = Client.transition(state, {:retry, 101})

      {state, [{:send, connect, :connect}]} =
        Client.transition(state, {:transport_up, 2, 102})

      assert connect.properties[:session_expiry_interval] == requested

      {state, _} =
        Client.transition(
          state,
          {:packet, 2, %Connack{version: :v5, session_present: true, reason_code: 0}, 103}
        )

      assert state.session_expiry_interval == requested
      assert state.peer.session_expiry_interval == requested

      {state, _} = Client.transition(state, {:transport_down, 2, :closed, 104})
      {state, _} = Client.transition(state, {:retry, 2_104})

      {_state, [{:send, next_connect, :connect}]} =
        Client.transition(state, {:transport_up, 3, 2_105})

      assert next_connect.clean_start == (requested == 1)
    end
  end

  test "operation timeout cannot report success or reuse an identifier while the peer might ACK" do
    state = online()

    {state, _} =
      Client.transition(state, {:publish, :op, %Publish{topic: "a", payload: "x", qos: 1}, 10})

    {state, _} = Client.transition(state, {:sent, 1, {:operation, :op}, :ok, 10})
    {state, effects} = Client.transition(state, {:operation_timeout, :op, 20})
    assert state.phase == :stopped
    assert {:notify, {:operation, :op, {:error, :unknown_outcome}}} in effects
    assert {:close, :operation_timeout} in effects
  end

  test "negative CONNACK and intentional DISCONNECT do not create retry loops" do
    {:ok, state} = Client.new(connect(), reconnect_max: 2)
    {state, _} = Client.transition(state, :start)
    {state, _} = Client.transition(state, {:transport_up, 1, 0})

    {state, effects} =
      Client.transition(
        state,
        {:packet, 1, %Connack{version: :v5, session_present: false, reason_code: 0x87}, 1}
      )

    assert state.phase == :stopped
    refute Enum.any?(effects, &match?({:retry, _}, &1))

    state = online([], reconnect_max: 2)
    {state, effects} = Client.transition(state, {:stop, 10})
    assert {:send, %Disconnect{}, :disconnect} in effects
    {state, _} = Client.transition(state, {:transport_down, 1, :closed, 11})
    assert state.phase == :stopped
  end

  test "malformed host property shapes are explicit rejections without a transition" do
    state = online()

    for packet <- [
          %Publish{topic: "a", payload: "x", properties: %{}},
          %Publish{topic: "a", payload: "x", properties: [nil]}
        ] do
      {^state, [{:notify, {:operation, :bad, {:error, _}}}]} =
        Client.transition(state, {:publish, :bad, packet, 10})
    end

    {^state, [{:notify, {:operation, :bad, {:error, _}}}]} =
      Client.transition(
        state,
        {:subscribe, :bad, %Subscribe{packet_id: 0, subscriptions: [{"a", 1}], properties: %{}},
         10}
      )
  end

  test "new validates the actual CONNECT after adding its bounded receive capabilities" do
    assert {:error, _} = Client.new(connect(), max_deliveries: 0)
    assert {:error, _} = Client.new(connect(), reconnect_max: -1)

    assert {:error, _} =
             Client.new(connect(), limits: %GravitonMQ.MQTT.Codec.Limits{max_properties: 0})

    assert {:error, _} = Client.new(connect(receive_maximum: 2), max_deliveries: 1)
    assert {:error, :topic_aliases_unsupported} = Client.new(connect(topic_alias_maximum: 1))
  end

  test "a resumed session schedules retransmits within the newly negotiated receive maximum" do
    state = online([], reconnect_max: 1, connect_properties: [session_expiry_interval: 60])

    {state, _} =
      Client.transition(state, {:publish, :one, %Publish{topic: "a", payload: "1", qos: 1}, 10})

    {state, _} = Client.transition(state, {:sent, 1, {:operation, :one}, :ok, 10})

    {state, _} =
      Client.transition(state, {:publish, :two, %Publish{topic: "a", payload: "2", qos: 1}, 11})

    {state, _} = Client.transition(state, {:sent, 1, {:operation, :two}, :ok, 11})
    {state, _} = Client.transition(state, {:transport_down, 1, :closed, 12})
    {state, _} = Client.transition(state, {:retry, 13})
    {state, _} = Client.transition(state, {:transport_up, 2, 14})

    {state, effects} =
      Client.transition(
        state,
        {:packet, 2,
         %Connack{
           version: :v5,
           session_present: true,
           reason_code: 0,
           properties: [receive_maximum: 1]
         }, 15}
      )

    assert [{:send, %Publish{payload: "1", dup: true}, {:operation, :one}}] =
             Enum.filter(effects, &match?({:send, %Publish{}, _}, &1))

    {state, effects} = Client.transition(state, {:packet, 2, %Puback{packet_id: 1}, 16})
    assert {:notify, {:operation, :one, {:ok, :puback}}} in effects

    assert {:send, %Publish{topic: "a", payload: "2", qos: 1, packet_id: 2, dup: true},
            {:operation, :two}} in effects

    assert map_size(state.outbound) == 1
  end

  test "a smaller peer packet limit cannot cause an oversized resumed publication" do
    state = online([], reconnect_max: 1, connect_properties: [session_expiry_interval: 60])

    {state, _} =
      Client.transition(
        state,
        {:publish, :op, %Publish{topic: "a", payload: "large", qos: 1}, 10}
      )

    {state, _} = Client.transition(state, {:sent, 1, {:operation, :op}, :ok, 10})
    {state, _} = Client.transition(state, {:transport_down, 1, :closed, 11})
    {state, _} = Client.transition(state, {:retry, 12})
    {state, _} = Client.transition(state, {:transport_up, 2, 13})

    {state, effects} =
      Client.transition(
        state,
        {:packet, 2,
         %Connack{
           version: :v5,
           session_present: true,
           reason_code: 0,
           properties: [maximum_packet_size: 5]
         }, 14}
      )

    assert state.phase == :stopped
    refute Enum.any?(effects, &match?({:send, %Publish{}, _}, &1))
    assert {:notify, {:operation, :op, {:error, :unknown_outcome}}} in effects
  end

  test "reuse of an outstanding incoming identifier without DUP is a protocol error" do
    state = online()
    packet = %Publish{topic: "a", payload: "x", qos: 1, packet_id: 1}
    {state, []} = Client.transition(state, {:packet, 1, packet, 10})
    {state, effects} = Client.transition(state, {:packet, 1, packet, 11})
    assert state.phase == :stopped
    assert {:close, :packet_identifier_in_use} in effects
  end

  test "incoming expiry discards only queued deliveries and acknowledges expired QoS1" do
    state = online()

    packet = %Publish{
      topic: "a",
      payload: "x",
      qos: 1,
      packet_id: 1,
      properties: [message_expiry_interval: 1]
    }

    {state, []} = Client.transition(state, {:packet, 1, packet, 10})
    {state, effects} = Client.transition(state, {:demand, 1, 1010})
    assert {:send, %Puback{packet_id: 1}, {:ack, 1}} in effects
    refute Enum.any?(effects, &match?({:notify, {:delivery, _, _}}, &1))
    {state, []} = Client.transition(state, {:sent, 1, {:ack, 1}, :ok, 1010})
    assert state.deliveries == %{}
    assert state.incoming == %{}
    assert state.demand == 1
  end

  test "zero-expiry QoS0 is not delivered and delivered publications require explicit ownership" do
    state = online()

    {state, []} =
      Client.transition(
        state,
        {:packet, 1, %Publish{topic: "a", payload: "x", properties: [message_expiry_interval: 0]},
         10}
      )

    assert state.deliveries == %{}
    packet = %Publish{topic: "a", payload: "x", qos: 1, packet_id: 1}
    {state, []} = Client.transition(state, {:packet, 1, packet, 11})

    {^state, [{:notify, {:ack, 2, {:error, :delivery_not_owned}}}]} =
      Client.transition(state, {:ack, 2, 12})
  end

  test "inbound aliases and invalid indicated payloads disconnect explicitly" do
    for {packet, code} <- [
          {%Publish{topic: "a", payload: "x", properties: [topic_alias: 1]}, 0x94},
          {%Publish{topic: "a", payload: <<255>>, properties: [payload_format_indicator: 1]},
           0x99}
        ] do
      {state, effects} = Client.transition(online(), {:packet, 1, packet, 10})
      assert state.phase == :stopped

      assert Enum.any?(
               effects,
               &match?({:send, %Disconnect{reason_code: ^code}, :disconnect}, &1)
             )
    end
  end

  test "failed writes never imply QoS0 completion and retries remain finite" do
    state = online([], reconnect_max: 1)
    {state, _} = Client.transition(state, {:publish, :op, %Publish{topic: "a", payload: "x"}, 10})

    {state, effects} =
      Client.transition(state, {:sent, 1, {:operation, :op}, {:error, :closed}, 11})

    assert {:notify, {:operation, :op, {:error, :unknown_outcome}}} in effects
    refute {:notify, {:operation, :op, {:ok, :sent}}} in effects
    {state, _} = Client.transition(state, {:retry, 12})
    {state, effects} = Client.transition(state, {:handshake_timeout, 2, 13})
    assert state.phase == :stopped
    refute Enum.any?(effects, &match?({:retry, _}, &1))
  end

  test "negative signed clock values, stale callbacks and disabled keep alive are safe" do
    {:ok, state} = Client.new(%{connect() | keep_alive: 0})
    {state, _} = Client.transition(state, :start)
    {state, _} = Client.transition(state, {:transport_up, 1, -10_000})

    {state, _} =
      Client.transition(
        state,
        {:packet, 1, %Connack{version: :v5, session_present: false, reason_code: 0}, -9_999}
      )

    {^state, []} = Client.transition(state, {:tick, 1, 1_000_000})
    {^state, []} = Client.transition(state, {:sent, 0, :connect, :ok, 1_000_000})
    {^state, []} = Client.transition(state, {:transport_down, 0, :closed, 1_000_000})
  end

  test "an unknown peer identifier cannot complete another operation" do
    state = online()

    {state, _} =
      Client.transition(state, {:publish, :op, %Publish{topic: "a", payload: "x", qos: 1}, 10})

    {state, effects} = Client.transition(state, {:packet, 1, %Puback{packet_id: 9}, 11})
    assert state.phase == :stopped
    assert {:close, :invalid_puback} in effects
    refute {:notify, {:operation, :op, {:ok, :puback}}} in effects
  end

  test "subscription state is bounded across completed and concurrent requests" do
    state = online([], max_subscriptions: 1)

    {state, _} =
      Client.transition(
        state,
        {:subscribe, :one, %Subscribe{packet_id: 0, subscriptions: [{"a", 1}]}, 10}
      )

    {^state, [{:notify, {:operation, :two, {:error, :subscriptions_full}}}]} =
      Client.transition(
        state,
        {:subscribe, :two, %Subscribe{packet_id: 0, subscriptions: [{"b", 1}]}, 11}
      )

    {state, _} =
      Client.transition(state, {:packet, 1, %Suback{packet_id: 1, reason_codes: [1]}, 12})

    {^state, [{:notify, {:operation, :two, {:error, :subscriptions_full}}}]} =
      Client.transition(
        state,
        {:subscribe, :two, %Subscribe{packet_id: 0, subscriptions: [{"b", 1}]}, 13}
      )
  end

  test "new publications cannot forge DUP or declare invalid UTF8 payloads" do
    state = online()

    for {packet, reason} <- [
          {%Publish{topic: "a", payload: "x", qos: 1, dup: true}, :invalid_publish_flags},
          {%Publish{topic: "a", payload: <<255>>, properties: [payload_format_indicator: 1]},
           :payload_format_invalid}
        ] do
      {^state, [{:notify, {:operation, :op, {:error, ^reason}}}]} =
        Client.transition(state, {:publish, :op, packet, 10})
    end
  end

  test "CONNACK cannot silently replace a nonempty requested identifier" do
    {:ok, state} = Client.new(connect())
    {state, _} = Client.transition(state, :start)
    {state, _} = Client.transition(state, {:transport_up, 1, 0})

    {state, effects} =
      Client.transition(
        state,
        {:packet, 1,
         %Connack{
           version: :v5,
           session_present: false,
           reason_code: 0,
           properties: [assigned_client_identifier: "different"]
         }, 1}
      )

    assert state.phase == :stopped
    assert {:close, :unexpected_assigned_client_identifier} in effects
  end

  test "SUBACK and UNSUBACK cardinality mismatches are explicit protocol failures" do
    for {request, ack, kind, reason} <- [
          {%Subscribe{packet_id: 0, subscriptions: [{"a", 1}]},
           %Suback{packet_id: 1, reason_codes: [0, 1]}, :subscribe, :invalid_suback},
          {%Subscribe{packet_id: 0, subscriptions: [{"a", 1}]},
           %Suback{packet_id: 1, reason_codes: []}, :subscribe, :invalid_suback},
          {%Unsubscribe{packet_id: 0, filters: ["a"]},
           %Unsuback{packet_id: 1, reason_codes: [0, 0]}, :unsubscribe, :invalid_unsuback}
        ] do
      {state, _} = Client.transition(online(), {kind, :op, request, 10})
      {state, effects} = Client.transition(state, {:packet, 1, ack, 11})
      assert state.phase == :stopped
      assert {:close, reason} in effects
    end
  end

  defp resume_at(state, now, properties \\ []) do
    {state, _} = Client.transition(state, {:transport_down, 1, :closed, 20})
    {state, _} = Client.transition(state, {:retry, now - 2})
    {state, _} = Client.transition(state, {:transport_up, 2, now - 1})

    Client.transition(
      state,
      {:packet, 2,
       %Connack{version: :v5, session_present: true, reason_code: 0, properties: properties}, now}
    )
  end

  test "resumed expiry preserves the original lifetime and unacknowledged identifier" do
    for {now, remaining} <- [{2010, 3}, {6010, 0}] do
      state = online([], reconnect_max: 1, connect_properties: [session_expiry_interval: 60])

      packet = %Publish{
        topic: "a",
        payload: "x",
        qos: 1,
        properties: [user_property: {"a", "b"}, message_expiry_interval: 5]
      }

      {state, _} = Client.transition(state, {:publish, :op, packet, 10})
      {state, effects} = resume_at(state, now)

      assert [{:send, resent, {:operation, :op}}] =
               Enum.filter(effects, &match?({:send, %Publish{}, _}, &1))

      assert resent.packet_id == 1 and resent.dup
      assert resent.properties == [user_property: {"a", "b"}, message_expiry_interval: remaining]
      assert Map.has_key?(state.outbound, :op)
      {_state, effects} = Client.transition(state, {:packet, 2, %Puback{packet_id: 1}, now + 1})
      assert {:notify, {:operation, :op, {:ok, :puback}}} in effects
    end
  end

  test "resumed expiry is recalculated when receive maximum later releases a slot" do
    state = online([], reconnect_max: 1, connect_properties: [session_expiry_interval: 60])
    packet = %Publish{topic: "a", payload: "x", qos: 1, properties: [message_expiry_interval: 5]}
    {state, _} = Client.transition(state, {:publish, :one, packet, 10})
    {state, _} = Client.transition(state, {:publish, :two, packet, 10})
    {state, _} = resume_at(state, 2010, receive_maximum: 1)
    {_state, effects} = Client.transition(state, {:packet, 2, %Puback{packet_id: 1}, 4010})

    assert {:send, %{packet | packet_id: 2, dup: true, properties: [message_expiry_interval: 1]},
            {:operation, :two}} in effects
  end

  test "host acknowledgements emit PUBACK in received PUBLISH order across reconnect" do
    state = online([], reconnect_max: 1, connect_properties: [session_expiry_interval: 60])
    first = %Publish{topic: "a", payload: "1", qos: 1, packet_id: 9}
    second = %{first | payload: "2", packet_id: 3}
    {state, _} = Client.transition(state, {:demand, 2, 9})
    {state, _} = Client.transition(state, {:packet, 1, first, 10})
    {state, _} = Client.transition(state, {:packet, 1, second, 11})
    {state, []} = Client.transition(state, {:ack, 2, 12})

    {state, []} =
      resume_at(state, 30)
      |> then(fn {state, effects} ->
        {state, Enum.filter(effects, &match?({:send, %Puback{}, _}, &1))}
      end)

    {_state, effects} = Client.transition(state, {:ack, 1, 31})

    assert effects == [
             {:send, %Puback{packet_id: 9}, {:ack, 9}},
             {:send, %Puback{packet_id: 3}, {:ack, 3}}
           ]
  end

  test "expired later incoming publications wait for earlier PUBACK order" do
    state = online()
    first = %Publish{topic: "a", payload: "1", qos: 1, packet_id: 9}
    second = %{first | payload: "2", packet_id: 3, properties: [message_expiry_interval: 0]}
    {state, _} = Client.transition(state, {:demand, 1, 9})
    {state, _} = Client.transition(state, {:packet, 1, first, 10})
    {state, []} = Client.transition(state, {:packet, 1, second, 11})
    {_state, effects} = Client.transition(state, {:ack, 1, 12})

    assert effects == [
             {:send, %Puback{packet_id: 9}, {:ack, 9}},
             {:send, %Puback{packet_id: 3}, {:ack, 3}}
           ]
  end

  test "server DISCONNECT cannot use client-only reason or session expiry property" do
    for packet <- [
          %Disconnect{reason_code: 4},
          %Disconnect{properties: [session_expiry_interval: 1]}
        ] do
      {state, effects} = Client.transition(online(), {:packet, 1, packet, 10})
      assert state.phase == :stopped
      assert {:close, :invalid_disconnect_direction} in effects
    end
  end

  test "automatic PUBACK and terminal DISCONNECT obey peer maximum packet size" do
    state = online(maximum_packet_size: 3)
    {state, _} = Client.transition(state, {:demand, 1, 9})

    {state, _} =
      Client.transition(
        state,
        {:packet, 1, %Publish{topic: "a", payload: "x", qos: 1, packet_id: 1}, 10}
      )

    {state, effects} = Client.transition(state, {:ack, 1, 11})
    assert state.phase == :stopped
    assert {:close, :peer_packet_size} in effects
    refute Enum.any?(effects, &match?({:send, %Puback{}, _}, &1))

    {state, effects} =
      Client.transition(online(maximum_packet_size: 1), {:protocol_error, 1, :malformed, 10})

    assert state.phase == :stopped
    refute Enum.any?(effects, &match?({:send, _, _}, &1))
  end

  test "automatic expiry PUBACK, resumed PUBACK and keepalive cannot exceed peer limits" do
    first = %Publish{topic: "a", payload: "x", qos: 1, packet_id: 1}

    {state, effects} =
      Client.transition(
        online(maximum_packet_size: 3),
        {:packet, 1, %{first | properties: [message_expiry_interval: 0]}, 10}
      )

    assert state.phase == :stopped
    assert {:close, :peer_packet_size} in effects
    refute Enum.any?(effects, &match?({:send, %Puback{}, _}, &1))

    state = online([], reconnect_max: 1, connect_properties: [session_expiry_interval: 60])
    {state, _} = Client.transition(state, {:demand, 1, 9})
    {state, _} = Client.transition(state, {:packet, 1, first, 10})
    {state, [{:send, %Puback{}, _}]} = Client.transition(state, {:ack, 1, 11})
    {state, effects} = resume_at(state, 30, maximum_packet_size: 3)
    assert state.phase == :stopped
    refute Enum.any?(effects, &match?({:send, %Puback{}, _}, &1))
    assert {:close, :peer_packet_size} in effects
    refute Enum.any?(effects, &match?({:notify, {:connected, _}}, &1))

    {state, effects} = Client.transition(online(maximum_packet_size: 1), {:tick, 1, 3000})
    assert state.phase == :stopped
    assert {:close, :peer_packet_size} in effects
    refute Enum.any?(effects, &match?({:send, _, _}, &1))
  end
end
