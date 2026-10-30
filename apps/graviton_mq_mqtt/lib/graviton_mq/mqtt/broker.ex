defmodule GravitonMQ.MQTT.Broker do
  @moduledoc """
  Deterministic single-instance MQTT 5.0 QoS 0/1/2 broker.

  Callers supply monotonic milliseconds and opaque connection/host identities.
  Authentication, authorization, writes, closes, timers and host notifications
  are effects; this module invokes no callbacks and owns no running resources.
  Effects describe memory admission. A persistent running owner synchronizes
  the durable projection before interpreting them; this machine does no I/O.
  """
  alias GravitonMQ.MQTT.Codec.{Limits, Packet}
  alias GravitonMQ.MQTT.Packet.{Connect, Connack, Publish, Puback, Subscribe, Suback}
  alias GravitonMQ.MQTT.Packet.{Unsubscribe, Unsuback, Pingreq, Pingresp, Disconnect}
  alias GravitonMQ.MQTT.Packet.{Pubrec, Pubrel, Pubcomp}
  alias GravitonMQ.MQTT.Topic

  @defaults %{
    max_connections: 128,
    max_sessions: 128,
    max_session_bytes: 8_388_608,
    max_subscriptions: 1_024,
    max_subscription_bytes: 1_048_576,
    max_pending_deliveries: 2_048,
    max_pending_bytes: 8_388_608,
    max_incoming_bytes: 8_388_608,
    max_offline_deliveries: 2_048,
    max_offline_bytes: 8_388_608,
    max_retained: 256,
    max_retained_bytes: 4_194_304,
    max_wills: 128,
    max_will_bytes: 1_048_576,
    max_prepared: 128,
    max_prepared_bytes: 4_194_304,
    max_writes: 4_096,
    max_write_bytes: 4_194_304,
    max_inflight: 16,
    maximum_qos: 2,
    max_packet_size: 1_048_576,
    receive_maximum: 16,
    max_host_subscriptions: 128,
    max_host_outstanding: 128,
    max_demand: 128,
    auth_timeout: 5_000,
    dispatch_retry_interval: 1_000
  }

  defstruct [
    :limits,
    :codec,
    connections: %{},
    sessions: %{},
    retained: %{},
    hosts: %{},
    operations: %{},
    wills: %{},
    writes: %{},
    next_id: 1
  ]

  @spec new(keyword() | map()) :: {:ok, %__MODULE__{}} | {:error, atom()}
  def new(options \\ []) do
    options =
      if is_list(options) and Keyword.keyword?(options), do: Map.new(options), else: options

    if is_map(options) and
         Enum.all?(options, fn {key, value} ->
           Map.has_key?(@defaults, key) and is_integer(value) and value >= 0
         end) do
      limits = Map.merge(@defaults, options)

      if limits.max_packet_size in 1..268_435_460 and limits.receive_maximum in 1..65_535 and
           limits.max_inflight in 1..65_535 and limits.maximum_qos in 0..2 and
           limits.auth_timeout > 0 and
           limits.dispatch_retry_interval > 0 do
        {:ok,
         %__MODULE__{limits: limits, codec: %Limits{max_packet_size: limits.max_packet_size}}}
      else
        {:error, :invalid_options}
      end
    else
      {:error, :invalid_options}
    end
  end

  @spec transition(%__MODULE__{}, term()) :: {%__MODULE__{}, list()}
  def transition(state, {:connection_open, id, generation, now}) do
    cond do
      Map.has_key?(state.connections, id) ->
        {state, []}

      map_size(state.connections) >= state.limits.max_connections ->
        {state, [{:close, id, generation, :connection_limit}]}

      true ->
        conn = %{
          generation: generation,
          phase: :connect,
          client_id: nil,
          last_seen: now,
          keep_alive: 0,
          peer_maximum: 268_435_460,
          receive_maximum: 65_535,
          send_quota: state.limits.max_inflight,
          incoming: %{},
          requests: %{}
        }

        {%{state | connections: Map.put(state.connections, id, conn)}, []}
    end
  end

  def transition(state, {:packet, id, generation, packet, bytes, now}) do
    case Map.get(state.connections, id) do
      %{generation: ^generation} = conn ->
        state = prune_messages(state, now)
        state = put_conn(state, id, %{conn | last_seen: now})

        if is_binary(bytes) and byte_size(bytes) <= state.limits.max_packet_size and
             valid_packet?(state, packet) do
          receive_packet(state, id, packet, bytes, now)
        else
          disconnect(state, id, :malformed_packet, 0x81, now)
        end

      _ ->
        {state, []}
    end
  end

  def transition(state, {:connection_down, id, generation, reason, now}) do
    if current?(state, id, generation), do: down(state, id, reason, now), else: {state, []}
  end

  def transition(state, {:protocol_error, id, generation, reason, now}) do
    code = if reason == {:limit_exceeded, :packet_too_large}, do: 0x95, else: 0x81

    if current?(state, id, generation),
      do: disconnect(state, id, reason, code, now),
      else: {state, []}
  end

  def transition(state, {:auth_result, opid, generation, decision, now}) do
    case Map.get(state.operations, opid) do
      %{generation: ^generation} = op ->
        if live?(state, op) do
          state = prune_messages(state, now)
          decision = if now >= op.deadline, do: {:error, :callback_timeout}, else: decision

          state = %{
            state
            | operations: Map.put(state.operations, opid, %{op | decision: decision})
          }

          drain(state, op.source, now)
        else
          {%{state | operations: Map.delete(state.operations, opid)}, []}
        end

      _ ->
        {state, []}
    end
  end

  def transition(state, {:write_result, id, generation, sendid, result, now}) do
    case Map.get(state.writes, sendid) do
      %{connection: {^id, ^generation}, kind: kind} ->
        state = %{state | writes: Map.delete(state.writes, sendid)}

        if current?(state, id, generation) do
          if result == :ok,
            do: wrote(state, id, kind, now),
            else: disconnect(state, id, :write_failed, nil, now)
        else
          {state, []}
        end

      _ ->
        {state, []}
    end
  end

  def transition(state, {:host_operation, caller, opid, {:as, principal, command}, now}),
    do: host_operation(state, caller, principal, opid, command, now)

  def transition(state, {:host_operation, caller, opid, command, now}),
    do: host_operation(state, caller, {:host, caller}, opid, command, now)

  def transition(state, {:host_down, caller, _now}) do
    hosts = Map.reject(state.hosts, fn {_id, host} -> host.caller == caller end)
    removed = Map.keys(state.hosts) -- Map.keys(hosts)

    operations =
      Map.reject(state.operations, fn {_id, op} ->
        op.source == {:host, caller} or
          (op.kind == :dispatch and op.delivery_kind == :host and op.recipient in removed)
      end)

    {%{state | hosts: hosts, operations: operations}, []}
  end

  def transition(state, {:tick, now}) do
    {state, effects} =
      Enum.reduce(state.operations, {state, []}, fn {id, op}, {state, effects} ->
        if op.deadline <= now and Map.has_key?(state.operations, id) do
          {state, more} =
            transition(state, {:auth_result, id, op.generation, {:error, :callback_timeout}, now})

          {state, effects ++ more}
        else
          {state, effects}
        end
      end)

    {state, more} = expire(state, now)
    {state, effects ++ more}
  end

  def transition(state, _event), do: {state, []}

  defp receive_packet(state, id, %Connect{} = packet, bytes, now) do
    conn = %{
      state.connections[id]
      | peer_maximum: Keyword.get(packet.properties, :maximum_packet_size, 268_435_460)
    }

    state = put_conn(state, id, conn)

    cond do
      conn.phase != :connect ->
        disconnect(state, id, :unexpected_connect, 0x82, now)

      packet.version != :v5 ->
        reject_connect(state, id, 0x84, :unsupported_version, now)

      Keyword.has_key?(packet.properties, :authentication_method) or
          Keyword.has_key?(packet.properties, :authentication_data) ->
        reject_connect(state, id, 0x8C, :enhanced_authentication_unsupported, now)

      packet.will != nil and packet.will.qos > state.limits.maximum_qos ->
        reject_connect(state, id, 0x9B, :qos_not_supported, now)

      packet.will != nil and
        Keyword.get(packet.will.properties, :payload_format_indicator, 0) == 1 and
          not String.valid?(packet.will.payload) ->
        reject_connect(state, id, 0x99, :payload_format_invalid, now)

      true ->
        conn = %{
          conn
          | phase: :authenticating,
            peer_maximum: Keyword.get(packet.properties, :maximum_packet_size, 268_435_460),
            receive_maximum: Keyword.get(packet.properties, :receive_maximum, 65_535),
            send_quota:
              min(
                state.limits.max_inflight,
                Keyword.get(packet.properties, :receive_maximum, 65_535)
              ),
            keep_alive: packet.keep_alive
        }

        state = put_conn(state, id, conn)

        credentials = %{
          client_id: packet.client_id,
          username: packet.username,
          password: packet.password
        }

        prepare(
          state,
          %{
            kind: :connect,
            source: {:network, id, conn.generation},
            generation: conn.generation,
            packet: packet,
            encoded: bytes
          },
          {:authenticate, credentials},
          now
        )
    end
  end

  defp receive_packet(state, id, packet, bytes, now) do
    conn = state.connections[id]

    if conn.phase != :connected do
      disconnect(state, id, :expected_connect, 0x82, now)
    else
      case packet do
        %Publish{qos: qos} when qos > state.limits.maximum_qos ->
          disconnect(state, id, :qos_not_supported, 0x9B, now)

        %Publish{} ->
          incoming_publish(state, id, packet, bytes, now)

        %Subscribe{} ->
          incoming_subscribe(state, id, packet, now)

        %Unsubscribe{} ->
          incoming_unsubscribe(state, id, packet, now)

        %Puback{} ->
          outgoing_ack(state, id, packet, now)

        %Pubrec{} ->
          outgoing_pubrec(state, id, packet, now)

        %Pubcomp{} ->
          outgoing_pubcomp(state, id, packet, now)

        %Pubrel{} ->
          incoming_pubrel(state, id, packet, now)

        %Pingreq{} ->
          send_packet(state, id, %Pingresp{}, :control, now)

        %Disconnect{} ->
          client_disconnect(state, id, packet, now)

        _ ->
          disconnect(state, id, :unexpected_packet, 0x82, now)
      end
    end
  end

  defp incoming_publish(state, id, packet, bytes, now) do
    conn = state.connections[id]
    session = state.sessions[conn.client_id]
    previous = Map.get(conn.incoming, packet.packet_id)

    cond do
      Keyword.has_key?(packet.properties, :topic_alias) ->
        disconnect(state, id, :topic_alias_invalid, 0x94, now)

      Keyword.has_key?(packet.properties, :subscription_identifier) ->
        disconnect(state, id, :invalid_property_direction, 0x82, now)

      Keyword.get(packet.properties, :payload_format_indicator, 0) == 1 and
          not String.valid?(packet.payload) ->
        disconnect(state, id, :payload_format_invalid, 0x99, now)

      packet.qos > 0 and Map.has_key?(conn.requests, packet.packet_id) ->
        disconnect(state, id, :packet_identifier_in_use, 0x82, now)

      packet.qos == 2 and Map.has_key?(session.incoming_qos2, packet.packet_id) ->
        if previous == nil and map_size(conn.incoming) >= state.limits.receive_maximum do
          disconnect(state, id, :receive_maximum_exceeded, 0x93, now)
        else
          state =
            put_conn(state, id, %{
              conn
              | incoming: Map.put(conn.incoming, packet.packet_id, :qos2)
            })

          send_packet(state, id, %Pubrec{packet_id: packet.packet_id}, :control, now)
        end

      packet.qos > 0 and Map.has_key?(session.incoming_qos2, packet.packet_id) ->
        disconnect(state, id, :packet_identifier_in_use, 0x82, now)

      previous != nil ->
        if is_struct(previous, Publish) and packet.dup and same_publish?(previous, packet),
          do: {state, []},
          else: disconnect(state, id, :packet_identifier_in_use, 0x82, now)

      packet.qos > 0 and map_size(conn.incoming) >= state.limits.receive_maximum ->
        disconnect(state, id, :receive_maximum_exceeded, 0x93, now)

      packet.qos > 0 and
          incoming_bytes(state) + :erlang.external_size(packet) > state.limits.max_incoming_bytes ->
        disconnect(state, id, :incoming_byte_limit, 0x97, now)

      true ->
        message = message(packet, bytes, {:network, conn.client_id}, now)
        source = {:network, id, conn.generation}

        state =
          if packet.qos > 0,
            do:
              put_conn(state, id, %{
                conn
                | incoming: Map.put(conn.incoming, packet.packet_id, packet)
              }),
            else: state

        publication(
          state,
          source,
          conn.generation,
          state.sessions[conn.client_id].principal,
          message,
          %{packet_id: packet.packet_id, qos: packet.qos},
          now
        )
    end
  end

  defp incoming_subscribe(state, id, packet, now) do
    case claim_request(state, id, packet.packet_id) do
      {:ok, state} -> prepare_subscribe(state, id, packet, now)
      :busy -> disconnect(state, id, :packet_identifier_in_use, 0x82, now)
    end
  end

  defp prepare_subscribe(state, id, packet, now) do
    conn = state.connections[id]

    cond do
      Keyword.has_key?(packet.properties, :subscription_identifier) ->
        disconnect(state, id, :subscription_identifiers_not_supported, 0xA1, now)

      Enum.any?(packet.subscriptions, &match?({<<"$share/", _::binary>>, _}, &1)) ->
        disconnect(state, id, :shared_subscriptions_not_supported, 0x9E, now)

      true ->
        subscriptions =
          Enum.map(packet.subscriptions, fn {filter, options} -> {filter, options(options)} end)

        session = state.sessions[conn.client_id]

        requests =
          Enum.map(subscriptions, fn {filter, _} ->
            {:authorize, session.principal, :subscribe, filter}
          end)

        retained = retained_candidates(state, {:network, conn.client_id}, subscriptions, now)

        requests =
          requests ++
            Enum.map(retained, fn target ->
              {:authorize, session.principal, :deliver, target.message.topic}
            end)

        prepare(
          state,
          %{
            kind: :subscribe,
            source: {:network, id, conn.generation},
            generation: conn.generation,
            client_id: conn.client_id,
            packet_id: packet.packet_id,
            subscriptions: subscriptions,
            retained: retained,
            encoded: <<>>
          },
          requests,
          now
        )
    end
  end

  defp incoming_unsubscribe(state, id, packet, now) do
    case claim_request(state, id, packet.packet_id) do
      {:ok, state} ->
        conn = state.connections[id]

        prepare(
          state,
          %{
            kind: :unsubscribe,
            source: {:network, id, conn.generation},
            generation: conn.generation,
            packet: packet,
            encoded: <<>>
          },
          [],
          now
        )

      :busy ->
        disconnect(state, id, :packet_identifier_in_use, 0x82, now)
    end
  end

  defp claim_request(state, id, packet_id) do
    conn = state.connections[id]

    if Map.has_key?(conn.incoming, packet_id) or Map.has_key?(conn.requests, packet_id) or
         Map.has_key?(state.sessions[conn.client_id].incoming_qos2, packet_id),
       do: :busy,
       else:
         {:ok, put_conn(state, id, %{conn | requests: Map.put(conn.requests, packet_id, true)})}
  end

  defp commit_unsubscribe(state, id, packet, now) do
    conn = state.connections[id]
    session = state.sessions[conn.client_id]

    codes =
      Enum.map(packet.filters, fn filter ->
        if Map.has_key?(session.subscriptions, filter), do: 0, else: 0x11
      end)

    session = %{session | subscriptions: Map.drop(session.subscriptions, packet.filters)}
    state = put_session(state, conn.client_id, session)

    send_packet(
      state,
      id,
      %Unsuback{packet_id: packet.packet_id, reason_codes: codes},
      {:request_ack, packet.packet_id},
      now
    )
  end

  defp outgoing_ack(state, id, packet, now) do
    conn = state.connections[id]
    session = state.sessions[conn.client_id]

    if Enum.any?(session.deliveries, &(&1.packet_id == packet.packet_id and &1.qos == 1)) do
      session = %{
        session
        | deliveries: Enum.reject(session.deliveries, &(&1.packet_id == packet.packet_id))
      }

      state = put_session(state, conn.client_id, session) |> replenish_quota(id)
      pump_session(state, conn.client_id, now)
    else
      disconnect(state, id, :unknown_packet_identifier, 0x92, now)
    end
  end

  defp outgoing_pubrec(state, id, packet, now) do
    conn = state.connections[id]
    session = state.sessions[conn.client_id]

    case Enum.find(session.deliveries, &(&1.packet_id == packet.packet_id and &1.qos == 2)) do
      nil ->
        disconnect(state, id, :unknown_packet_identifier, 0x82, now)

      %{phase: :pubrel} ->
        send_packet(state, id, %Pubrel{packet_id: packet.packet_id}, :control, now)

      entry when packet.reason_code >= 0x80 ->
        state = remove_delivery(state, :network, conn.client_id, entry.id) |> replenish_quota(id)
        pump_session(state, conn.client_id, now)

      entry ->
        entry = %{entry | phase: :pubrel, release_order: state.next_id}
        state = %{state | next_id: state.next_id + 1}
        state = put_delivery(state, :network, conn.client_id, entry)
        send_packet(state, id, %Pubrel{packet_id: packet.packet_id}, :control, now)
    end
  end

  defp outgoing_pubcomp(state, id, packet, now) do
    conn = state.connections[id]
    session = state.sessions[conn.client_id]

    case Enum.find(
           session.deliveries,
           &(&1.packet_id == packet.packet_id and &1.qos == 2 and &1.phase == :pubrel)
         ) do
      nil ->
        disconnect(state, id, :unknown_packet_identifier, 0x82, now)

      entry ->
        state = remove_delivery(state, :network, conn.client_id, entry.id) |> replenish_quota(id)
        pump_session(state, conn.client_id, now)
    end
  end

  defp replenish_quota(state, id) do
    conn = state.connections[id]

    put_conn(state, id, %{
      conn
      | send_quota: min(conn.send_quota + 1, min(conn.receive_maximum, state.limits.max_inflight))
    })
  end

  defp incoming_pubrel(state, id, packet, now) do
    conn = state.connections[id]
    session = state.sessions[conn.client_id]
    code = if Map.has_key?(session.incoming_qos2, packet.packet_id), do: 0, else: 0x92

    state =
      put_session(state, conn.client_id, %{
        session
        | incoming_qos2: Map.delete(session.incoming_qos2, packet.packet_id)
      })

    state = release_incoming(state, id, packet.packet_id)

    send_packet(
      state,
      id,
      %Pubcomp{packet_id: packet.packet_id, reason_code: code},
      :control,
      now
    )
  end

  defp release_incoming(state, id, packet_id) do
    conn = state.connections[id]
    put_conn(state, id, %{conn | incoming: Map.delete(conn.incoming, packet_id)})
  end

  defp client_disconnect(state, id, packet, now) do
    conn = state.connections[id]
    session = state.sessions[conn.client_id]
    expiry = Keyword.get(packet.properties, :session_expiry_interval, session.expiry)

    cond do
      session.expiry == 0 and expiry > 0 ->
        disconnect(state, id, :invalid_session_expiry, 0x82, now)

      packet.reason_code not in [
        0,
        4,
        0x80,
        0x81,
        0x82,
        0x83,
        0x87,
        0x90,
        0x93,
        0x94,
        0x95,
        0x97,
        0x99
      ] ->
        disconnect(state, id, :invalid_disconnect_direction, 0x82, now)

      true ->
        state = put_session(state, conn.client_id, %{session | expiry: expiry})

        {state, effects} =
          down(state, id, if(packet.reason_code == 0, do: :normal, else: :with_will), now)

        {state, effects ++ [{:close, id, conn.generation, :normal}]}
    end
  end

  defp prepare(state, data, request, now) do
    data =
      Map.merge(data, %{
        id: state.next_id,
        decision: :pending,
        request: request,
        deadline: now + state.limits.auth_timeout
      })

    size = :erlang.external_size(data)

    if map_size(state.operations) < state.limits.max_prepared and
         prepared_bytes(state) + size <= state.limits.max_prepared_bytes do
      op = Map.put(data, :size, size)
      op = if request == [], do: %{op | decision: []}, else: op

      state = %{
        state
        | operations: Map.put(state.operations, op.id, op),
          next_id: state.next_id + 1
      }

      if request == [],
        do: drain(state, op.source, now),
        else: {state, [{:authorize, op.id, request, op.deadline}]}
    else
      preparation_failed(state, data, :prepared_limit, now)
    end
  end

  defp drain(state, source, now) do
    next =
      state.operations
      |> Map.values()
      |> Enum.filter(&(&1.source == source))
      |> Enum.min_by(& &1.id, fn -> nil end)

    if next != nil and next.decision != :pending do
      state = %{state | operations: Map.delete(state.operations, next.id)}
      {state, effects} = commit(state, next, now)
      {state, more} = drain(state, source, now)
      {state, effects ++ more}
    else
      {state, []}
    end
  end

  defp commit(state, %{kind: :connect} = op, now) do
    case op.decision do
      {:ok, principal} ->
        cond do
          :erlang.external_size(principal) > state.limits.max_session_bytes ->
            reject_connect_source(state, op.source, 0x97, :principal_limit, now)

          op.packet.will == nil ->
            attach(state, op, principal, now)

          true ->
            op = %{
              op
              | kind: :connect_will,
                decision: :pending,
                request: [{:authorize, principal, :publish, op.packet.will.topic}]
            }

            op = Map.put(op, :principal, principal)
            size = :erlang.external_size(op)

            if prepared_bytes(state) + size <= state.limits.max_prepared_bytes do
              op = %{op | size: size}

              {%{state | operations: Map.put(state.operations, op.id, op)},
               [{:authorize, op.id, op.request, op.deadline}]}
            else
              reject_connect_source(state, op.source, 0x97, :prepared_limit, now)
            end
        end

      {:error, :denied} ->
        reject_connect_source(state, op.source, 0x87, :denied, now)

      _ ->
        reject_connect_source(state, op.source, 0x88, :authentication_failed, now)
    end
  end

  defp commit(state, %{kind: :connect_will} = op, now) do
    case decisions(op) do
      {:ok, [:ok]} -> attach(state, op, op.principal, now)
      {:ok, _} -> reject_connect_source(state, op.source, 0x87, :will_denied, now)
      {:error, _} -> reject_connect_source(state, op.source, 0x88, :authorization_failed, now)
    end
  end

  defp commit(state, %{kind: :dispatch} = op, now), do: commit_dispatch(state, op, now)

  defp commit(state, %{kind: :publication} = op, now) do
    case decisions(op) do
      {:error, reason} ->
        publication_failed(state, op, {:auth_failed, reason}, now)

      {:ok, [{:error, :denied} | _]} ->
        publication_failed(state, op, :denied, now)

      {:ok, [:ok | deliveries]} ->
        targets =
          Enum.zip(op.targets, deliveries)
          |> Enum.filter(fn {target, decision} ->
            decision == :ok and target_current?(state, target)
          end)
          |> Enum.map(&elem(&1, 0))

        targets = Enum.map(targets, &refresh_target(state, &1))

        targets =
          Enum.reject(
            targets,
            &(&1.kind == :network and state.sessions[&1.recipient].connection == nil and
                &1.qos == 0)
          )

        targets = if expired_message?(op.message, now), do: [], else: targets
        retained = retained_after(state.retained, op.message, now)

        if capacity?(state, targets, retained) do
          state = %{state | retained: retained}
          state = install_targets(state, targets)
          state = finish_will(state, op)
          {state, accepted} = publication_accepted(state, op, now)
          {state, sends} = pump(state, now)
          {state, accepted ++ sends}
        else
          publication_failed(state, op, :capacity, now)
        end
    end
  end

  defp commit(state, %{kind: :unsubscribe, source: {:network, id, _}} = op, now),
    do: commit_unsubscribe(state, id, op.packet, now)

  defp commit(state, %{kind: :subscribe} = op, now), do: commit_subscribe(state, op, now)

  defp commit(state, %{kind: :host_subscribe} = op, now),
    do: commit_host_subscribe(state, op, now)

  defp attach(state, op, principal, now) do
    {:network, id, generation} = op.source
    packet = op.packet

    client_id =
      if packet.client_id == "",
        do: available_client_id(state, state.next_id),
        else: packet.client_id

    state =
      case Map.get(state.sessions, client_id) do
        nil ->
          state

        old ->
          if session_expired?(old, now),
            do: elem(end_session(state, client_id, now), 0),
            else: state
      end

    original_state = state
    existing = Map.get(state.sessions, client_id)

    cond do
      existing != nil and existing.principal != principal ->
        reject_connect(state, id, 0x87, :principal_mismatch, now)

      existing == nil and map_size(state.sessions) >= state.limits.max_sessions ->
        reject_connect(state, id, 0x97, :session_limit, now)

      true ->
        {state, close_effects} =
          case existing do
            %{connection: {old_id, _}} when old_id != id ->
              disconnect(state, old_id, :session_taken_over, 0x8E, now)

            _ ->
              {state, []}
          end

        present = existing != nil and not packet.clean_start

        {state, _} =
          if existing != nil and not present,
            do: end_session(state, client_id, now),
            else: {state, []}

        session =
          if present,
            do: state.sessions[client_id] || %{existing | connection: nil},
            else: fresh_session(principal, state.next_id)

        state = %{state | next_id: state.next_id + 1}
        state = if present, do: cancel_delayed_will(state, session, now), else: state

        {state, will_id} =
          register_will(state, packet.will, principal, client_id, session.token, now)

        session = %{
          session
          | principal: principal,
            connection: {id, generation},
            expiry: Keyword.get(packet.properties, :session_expiry_interval, 0),
            expires_at: nil,
            will_id: will_id,
            deliveries: Enum.map(session.deliveries, &%{&1 | sending: false})
        }

        session_bytes =
          :erlang.external_size({client_id, principal}) +
            Enum.sum(
              for {key, value} <- state.sessions,
                  key != client_id,
                  do: :erlang.external_size({key, value.principal})
            )

        if will_id == :limit or session_bytes > state.limits.max_session_bytes do
          reject_connect(original_state, id, 0x97, :session_limit, now)
        else
          conn = %{state.connections[id] | phase: :connected, client_id: client_id}
          state = state |> put_session(client_id, session) |> put_conn(id, conn)

          properties = [
            retain_available: 1,
            wildcard_subscription_available: 1,
            shared_subscription_available: 0,
            subscription_identifier_available: 0,
            topic_alias_maximum: 0,
            maximum_packet_size: state.limits.max_packet_size,
            receive_maximum: state.limits.receive_maximum
          ]

          properties =
            if state.limits.maximum_qos < 2,
              do: [{:maximum_qos, state.limits.maximum_qos} | properties],
              else: properties

          properties =
            if packet.client_id == "",
              do: properties ++ [assigned_client_identifier: client_id],
              else: properties

          {state, effects} =
            send_packet(
              state,
              id,
              %Connack{
                version: :v5,
                session_present: present,
                reason_code: 0,
                properties: properties
              },
              :control,
              now
            )

          {state, sends} = pump_session(state, client_id, now)
          {state, close_effects ++ effects ++ sends}
        end
    end
  end

  defp fresh_session(principal, token),
    do: %{
      principal: principal,
      token: token,
      connection: nil,
      expiry: 0,
      expires_at: nil,
      will_id: nil,
      subscriptions: %{},
      incoming_qos2: %{},
      deliveries: [],
      next_packet_id: 1
    }

  defp publication(state, source, generation, principal, message, reply, now) do
    targets = route(state, message)

    requests =
      [{:authorize, principal, :publish, message.topic}] ++
        Enum.map(targets, fn target -> {:authorize, target.principal, :deliver, message.topic} end)

    prepare(
      state,
      %{
        kind: :publication,
        source: source,
        generation: generation,
        message: message,
        targets: targets,
        reply: reply,
        encoded: message.encoded
      },
      requests,
      now
    )
  end

  defp message(packet, bytes, origin, now) do
    expiry =
      case Keyword.get(packet.properties, :message_expiry_interval) do
        nil -> nil
        seconds -> now + seconds * 1_000
      end

    %{
      topic: packet.topic,
      payload: packet.payload,
      qos: packet.qos,
      retain: packet.retain,
      properties: packet.properties,
      encoded: bytes,
      origin: origin,
      expires_at: expiry
    }
  end

  defp route(state, message) do
    network =
      Enum.flat_map(state.sessions, fn {id, session} ->
        matches =
          Enum.filter(session.subscriptions, fn {filter, sub} ->
            Topic.matches?(filter, message.topic) and
              not (sub.options.no_local and message.origin == {:network, id})
          end)

        if matches == [] or (session.connection == nil and message.qos == 0) do
          []
        else
          qos = min(message.qos, Enum.max(Enum.map(matches, fn {_, sub} -> sub.options.qos end)))

          retain =
            message.retain and
              Enum.any?(matches, fn {_, sub} -> sub.options.retain_as_published end)

          if session.connection == nil and qos == 0 do
            []
          else
            [
              %{
                kind: :network,
                recipient: id,
                token: session.token,
                principal: session.principal,
                incarnations: Enum.map(matches, fn {filter, sub} -> {filter, sub.id} end),
                message: message,
                qos: qos,
                retain: retain
              }
            ]
          end
        end
      end)

    host =
      Enum.flat_map(state.hosts, fn {id, host} ->
        if Topic.matches?(host.filter, message.topic) and
             not (host.options.no_local and message.origin == {:host, host.caller}) do
          [
            %{
              kind: :host,
              recipient: id,
              token: host.id,
              principal: host.principal,
              incarnations: [],
              message: message,
              qos: min(message.qos, host.options.qos),
              retain: message.retain and host.options.retain_as_published
            }
          ]
        else
          []
        end
      end)

    Enum.sort_by(network ++ host, &{&1.kind, &1.recipient})
  end

  defp target_current?(state, %{kind: :network} = target) do
    case Map.get(state.sessions, target.recipient) do
      %{token: token} = session when token == target.token ->
        Enum.any?(target.incarnations, fn {filter, incarnation} ->
          match?(%{id: ^incarnation}, Map.get(session.subscriptions, filter))
        end)

      _ ->
        false
    end
  end

  defp target_current?(state, %{kind: :host} = target),
    do: Map.has_key?(state.hosts, target.recipient)

  defp refresh_target(state, %{kind: :network} = target) do
    session = state.sessions[target.recipient]

    surviving =
      Enum.flat_map(target.incarnations, fn {filter, id} ->
        case Map.get(session.subscriptions, filter) do
          %{id: ^id} = sub -> [sub]
          _ -> []
        end
      end)

    %{
      target
      | qos: min(target.message.qos, Enum.max(Enum.map(surviving, & &1.options.qos))),
        retain: target.message.retain and Enum.any?(surviving, & &1.options.retain_as_published)
    }
  end

  defp refresh_target(_state, target), do: target

  defp retained_after(retained, %{retain: false}, _now), do: retained

  defp retained_after(retained, message, now) do
    if message.payload == "" or expired_message?(message, now),
      do: Map.delete(retained, message.topic),
      else: Map.put(retained, message.topic, message)
  end

  defp capacity?(state, targets, retained) do
    entries = pending_entries(state)

    offline =
      Enum.flat_map(state.sessions, fn {_id, session} ->
        if session.connection == nil, do: session.deliveries, else: []
      end)

    new_offline =
      Enum.filter(
        targets,
        &(&1.kind == :network and state.sessions[&1.recipient].connection == nil)
      )

    length(entries) + length(targets) <= state.limits.max_pending_deliveries and
      pending_bytes(entries) + pending_bytes(targets) <= state.limits.max_pending_bytes and
      length(offline) + length(new_offline) <= state.limits.max_offline_deliveries and
      pending_bytes(offline) + pending_bytes(new_offline) <= state.limits.max_offline_bytes and
      map_size(retained) <= state.limits.max_retained and
      Enum.sum(Enum.map(retained, fn {_topic, message} -> message_bytes(message) end)) <=
        state.limits.max_retained_bytes
  end

  defp install_targets(state, targets) do
    Enum.reduce(targets, state, fn target, state ->
      entry = %{
        id: state.next_id,
        message: target.message,
        qos: target.qos,
        phase: :publish,
        release_order: nil,
        retain: target.retain,
        packet_id: nil,
        sending: false,
        ever_sent: false,
        delivered: false,
        dispatch: nil,
        retry_at: nil
      }

      state = %{state | next_id: state.next_id + 1}

      case target.kind do
        :network ->
          session = state.sessions[target.recipient]

          put_session(state, target.recipient, %{
            session
            | deliveries: session.deliveries ++ [entry]
          })

        :host ->
          host = state.hosts[target.recipient]

          %{
            state
            | hosts:
                Map.put(state.hosts, host.id, %{host | deliveries: host.deliveries ++ [entry]})
          }
      end
    end)
  end

  defp pump(state, now) do
    {state, effects} =
      Enum.reduce(Map.keys(state.sessions), {state, []}, fn id, {state, effects} ->
        {state, more} = pump_session(state, id, now)
        {state, effects ++ more}
      end)

    Enum.reduce(Map.keys(state.hosts), {state, effects}, fn id, {state, effects} ->
      {state, more} = pump_host(state, id, now)
      {state, effects ++ more}
    end)
  end

  defp pump_session(state, client_id, now) do
    case Map.get(state.sessions, client_id) do
      %{connection: {id, generation}} = session ->
        case Map.get(state.connections, id) do
          %{generation: ^generation} = conn ->
            # PUBREL is independent of PUBLISH quota and follows PUBREC order,
            # which can differ from the original publication order on resume.
            next =
              session.deliveries
              |> Enum.reject(& &1.sending)
              |> Enum.min_by(
                fn
                  %{phase: :pubrel, release_order: order} -> {0, order}
                  entry -> {1, entry.id}
                end,
                fn -> nil end
              )

            case next do
              nil ->
                {state, []}

              entry ->
                cond do
                  entry.dispatch != nil ->
                    {state, []}

                  entry.retry_at != nil and entry.retry_at > now ->
                    {state, []}

                  entry.qos == 2 and entry.phase == :pubrel ->
                    state =
                      put_delivery(state, :network, client_id, %{
                        entry
                        | sending: true,
                          dispatch: nil,
                          retry_at: nil
                      })

                    {state, effects} =
                      send_packet(state, id, %Pubrel{packet_id: entry.packet_id}, :control, now)

                    {state, more} = pump_session(state, client_id, now)
                    {state, effects ++ more}

                  entry.qos > 0 and conn.send_quota == 0 ->
                    {state, []}

                  true ->
                    request_dispatch(state, :network, client_id, session, entry, now)
                end
            end

          _ ->
            {state, []}
        end

      _ ->
        {state, []}
    end
  end

  defp recipient_packet(entry, packet_id, now) do
    properties =
      Enum.reject(entry.message.properties, fn {key, _} ->
        key in [:topic_alias, :subscription_identifier]
      end)

    properties =
      Enum.map(properties, fn
        {:message_expiry_interval, _} ->
          {:message_expiry_interval, max(0, div(entry.message.expires_at - now, 1_000))}

        pair ->
          pair
      end)

    %Publish{
      topic: entry.message.topic,
      payload: entry.message.payload,
      qos: entry.qos,
      retain: entry.retain,
      packet_id: packet_id,
      dup: entry.qos > 0 and entry.ever_sent,
      properties: properties
    }
  end

  defp free_packet_id(session) do
    used = MapSet.new(Enum.map(session.deliveries, & &1.packet_id))

    Enum.find(0..65_534, fn offset ->
      not MapSet.member?(used, rem(session.next_packet_id + offset - 1, 65_535) + 1)
    end)
    |> then(fn offset -> rem(session.next_packet_id + offset - 1, 65_535) + 1 end)
  end

  defp send_packet(state, id, packet, kind, now) do
    case Map.get(state.connections, id) do
      nil ->
        {state, []}

      conn ->
        case Packet.encode(packet, state.codec) do
          {:ok, bytes} when byte_size(bytes) <= conn.peer_maximum ->
            write_bytes = Enum.sum(Enum.map(state.writes, fn {_id, write} -> write.bytes end))

            if map_size(state.writes) < state.limits.max_writes and
                 write_bytes + byte_size(bytes) <= state.limits.max_write_bytes do
              sendid = state.next_id

              state = %{
                state
                | next_id: sendid + 1,
                  writes:
                    Map.put(state.writes, sendid, %{
                      connection: {id, conn.generation},
                      kind: kind,
                      bytes: byte_size(bytes)
                    })
              }

              {state, [{:send, id, conn.generation, sendid, packet}]}
            else
              disconnect(state, id, :write_limit, nil, now)
            end

          _ ->
            disconnect(state, id, :peer_packet_size, nil, now)
        end
    end
  end

  defp wrote(state, id, {:request_ack, packet_id}, _now) do
    conn = state.connections[id]
    {put_conn(state, id, %{conn | requests: Map.delete(conn.requests, packet_id)}), []}
  end

  defp wrote(state, _id, {:delivery, client_id, delivery_id, 0}, now) do
    case Map.get(state.sessions, client_id) do
      nil ->
        {state, []}

      session ->
        session = %{
          session
          | deliveries: Enum.reject(session.deliveries, &(&1.id == delivery_id))
        }

        pump_session(put_session(state, client_id, session), client_id, now)
    end
  end

  defp wrote(state, _id, _kind, _now), do: {state, []}

  defp publication_accepted(
         state,
         %{source: {:network, id, _}, reply: %{qos: 1, packet_id: packet_id}},
         now
       ) do
    state = release_incoming(state, id, packet_id)
    send_packet(state, id, %Puback{packet_id: packet_id}, :control, now)
  end

  defp publication_accepted(
         state,
         %{source: {:network, id, _}, reply: %{qos: 2, packet_id: packet_id}},
         now
       ) do
    conn = state.connections[id]
    session = state.sessions[conn.client_id]

    state =
      put_session(state, conn.client_id, %{
        session
        | incoming_qos2: Map.put(session.incoming_qos2, packet_id, true)
      })

    state = put_conn(state, id, %{conn | incoming: Map.put(conn.incoming, packet_id, :qos2)})
    send_packet(state, id, %Pubrec{packet_id: packet_id}, :control, now)
  end

  defp publication_accepted(state, %{source: {:host, _}, reply: reply}, _now),
    do: {state, [{:complete, reply, {:ok, :accepted_memory}}]}

  defp publication_accepted(state, _op, _now), do: {state, []}

  defp publication_failed(
         state,
         %{source: {:network, id, _}, reply: %{qos: qos, packet_id: packet_id}},
         reason,
         now
       )
       when qos in [1, 2] and reason in [:denied, :capacity, :prepared_limit] do
    code = if reason == :denied, do: 0x87, else: 0x97

    state = release_incoming(state, id, packet_id)

    packet =
      if qos == 1,
        do: %Puback{packet_id: packet_id, reason_code: code},
        else: %Pubrec{packet_id: packet_id, reason_code: code}

    send_packet(state, id, packet, :control, now)
  end

  defp publication_failed(state, %{source: {:network, id, _}}, reason, now),
    do: disconnect(state, id, reason, if(reason == :denied, do: 0x87, else: 0x83), now)

  defp publication_failed(state, %{source: {:host, _}, reply: reply}, reason, _now),
    do: {state, [{:complete, reply, {:error, reason}}]}

  defp publication_failed(state, %{source: {:will, will_id}} = op, reason, _now) do
    state =
      case Map.get(state.wills, will_id) do
        nil ->
          state

        will ->
          if reason == :denied,
            do: %{state | wills: Map.delete(state.wills, will_id)},
            else: %{state | wills: Map.put(state.wills, will_id, %{will | status: :due})}
      end

    {state, [{:complete, {:will, will_id, op.id}, {:error, reason}}]}
  end

  defp preparation_failed(state, %{kind: :dispatch} = op, _reason, now),
    do: retry_dispatch(state, op, now)

  defp preparation_failed(state, %{kind: :publication} = op, reason, now),
    do: publication_failed(state, op, reason, now)

  defp preparation_failed(state, %{source: {:network, id, _}}, reason, now),
    do: disconnect(state, id, reason, 0x97, now)

  defp preparation_failed(state, %{source: {:host, _}, reply: reply}, reason, _now),
    do: {state, [{:complete, reply, {:error, reason}}]}

  defp decisions(op) do
    list = if is_list(op.decision), do: op.decision, else: [op.decision]

    cond do
      length(list) != length(op.request) ->
        {:error, :invalid_callback_result}

      Enum.any?(list, &(&1 != :ok and &1 != {:error, :denied})) ->
        {:error, Enum.find(list, &(&1 != :ok and &1 != {:error, :denied}))}

      true ->
        {:ok, list}
    end
  end

  defp disconnect(state, id, reason, code, now) do
    case Map.get(state.connections, id) do
      nil ->
        {state, []}

      conn ->
        packet = %Disconnect{reason_code: code || 0}

        wire =
          if code != nil and conn.phase == :connected,
            do: Packet.encode(packet, state.codec),
            else: :none

        sends =
          case wire do
            {:ok, bytes} when byte_size(bytes) <= conn.peer_maximum ->
              [{:send, id, conn.generation, state.next_id, packet}]

            _ ->
              []
          end

        {state, effects} = down(%{state | next_id: state.next_id + 1}, id, :abnormal, now)
        {state, sends ++ [{:close, id, conn.generation, reason}] ++ effects}
    end
  end

  defp reject_connect_source(state, {:network, id, _}, code, reason, now),
    do: reject_connect(state, id, code, reason, now)

  defp reject_connect(state, id, code, reason, now) do
    {state, effects} =
      send_packet(
        state,
        id,
        %Connack{version: :v5, session_present: false, reason_code: code},
        :control,
        now
      )

    generation =
      case Map.get(state.connections, id) do
        nil -> nil
        conn -> conn.generation
      end

    {state, more} = down(state, id, :normal, now)

    {state,
     effects ++ if(generation == nil, do: [], else: [{:close, id, generation, reason}]) ++ more}
  end

  defp down(state, id, reason, now) do
    if Map.has_key?(state.connections, id),
      do: down_existing(state, id, reason, now),
      else: {state, []}
  end

  defp down_existing(state, id, reason, now) do
    conn = state.connections[id]

    operations =
      Map.reject(state.operations, fn {_opid, op} ->
        op.source == {:network, id, conn.generation} or
          (op.kind == :dispatch and Map.get(op, :connection) == {id, conn.generation})
      end)

    writes =
      Map.reject(state.writes, fn {_sendid, write} ->
        write.connection == {id, conn.generation}
      end)

    state = %{
      state
      | connections: Map.delete(state.connections, id),
        operations: operations,
        writes: writes
    }

    case Map.get(state.sessions, conn.client_id) do
      %{connection: {^id, generation}} = session when generation == conn.generation ->
        state = schedule_will(state, session, reason, now)
        expires_at = if session.expiry == 0xFFFF_FFFF, do: nil, else: now + session.expiry * 1_000

        session = %{
          session
          | connection: nil,
            expires_at: expires_at,
            deliveries:
              Enum.filter(session.deliveries, &(&1.qos > 0)) |> Enum.map(&%{&1 | dispatch: nil})
        }

        state = put_session(state, conn.client_id, session)
        if session.expiry == 0, do: end_session(state, conn.client_id, now), else: {state, []}

      _ ->
        {state, []}
    end
  end

  defp register_will(state, nil, _principal, _client_id, _token, _now), do: {state, nil}

  defp register_will(state, will, principal, client_id, token, now) do
    properties = Keyword.delete(will.properties, :will_delay_interval)

    packet = %Publish{
      topic: will.topic,
      payload: will.payload,
      qos: will.qos,
      retain: will.retain,
      properties: properties,
      packet_id: if(will.qos == 0, do: nil, else: 1)
    }

    {:ok, encoded} = Packet.encode(packet, state.codec)

    job = %{
      id: state.next_id,
      principal: principal,
      client_id: client_id,
      token: token,
      packet: packet,
      encoded: encoded,
      delay: Keyword.get(will.properties, :will_delay_interval, 0),
      due: nil,
      status: :armed,
      registered_at: now,
      message: nil
    }

    weight =
      :erlang.external_size({job, message(packet, encoded, {:network, client_id}, now)}) + 32

    job = Map.put(job, :bytes, weight)

    if map_size(state.wills) < state.limits.max_wills and
         Enum.sum(Enum.map(state.wills, fn {_id, job} -> job.bytes end)) + weight <=
           state.limits.max_will_bytes do
      {%{state | wills: Map.put(state.wills, job.id, job), next_id: state.next_id + 1}, job.id}
    else
      {state, :limit}
    end
  end

  defp schedule_will(state, %{will_id: nil}, _reason, _now), do: state

  defp schedule_will(state, session, :normal, _now),
    do: %{state | wills: Map.delete(state.wills, session.will_id)}

  defp schedule_will(state, session, _reason, now) do
    case Map.get(state.wills, session.will_id) do
      nil ->
        state

      will ->
        delay = min(will.delay, session.expiry)

        %{
          state
          | wills: Map.put(state.wills, will.id, %{will | due: now + delay * 1_000, status: :due})
        }
    end
  end

  defp cancel_delayed_will(state, session, now) do
    case Map.get(state.wills, session.will_id) do
      %{due: due} when due == nil or due > now ->
        %{state | wills: Map.delete(state.wills, session.will_id)}

      _ ->
        state
    end
  end

  defp end_session(state, client_id, now) do
    case Map.get(state.sessions, client_id) do
      nil ->
        {state, []}

      session ->
        state =
          case Map.get(state.wills, session.will_id) do
            nil ->
              state

            will ->
              %{
                state
                | wills:
                    Map.put(state.wills, will.id, %{
                      will
                      | due: now,
                        status: if(will.status == :prepared, do: :prepared, else: :due)
                    })
              }
          end

        {%{state | sessions: Map.delete(state.sessions, client_id)}, []}
    end
  end

  defp finish_will(state, %{source: {:will, id}}),
    do: %{state | wills: Map.delete(state.wills, id)}

  defp finish_will(state, _op), do: state

  defp expire(state, now) do
    state = prune_messages(state, now)

    retained =
      Map.reject(state.retained, fn {_topic, message} -> expired_message?(message, now) end)

    state = %{state | retained: retained}

    state =
      Enum.reduce(state.sessions, state, fn {id, session}, state ->
        if session_expired?(session, now), do: elem(end_session(state, id, now), 0), else: state
      end)

    {state, effects} =
      Enum.reduce(state.connections, {state, []}, fn {id, conn}, {state, effects} ->
        if conn.phase == :connected and conn.keep_alive > 0 and
             now - conn.last_seen >= conn.keep_alive * 1_500 do
          {state, more} = disconnect(state, id, :keep_alive_timeout, 0x8D, now)
          {state, effects ++ more}
        else
          {state, effects}
        end
      end)

    {state, effects} =
      Enum.reduce(state.wills, {state, effects}, fn {id, will}, {state, effects} ->
        if will.status == :due and will.due <= now do
          message =
            will.message || message(will.packet, will.encoded, {:network, will.client_id}, now)

          state = %{
            state
            | wills: Map.put(state.wills, id, %{will | status: :prepared, message: message})
          }

          {state, more} = publication(state, {:will, id}, 0, will.principal, message, nil, now)
          {state, effects ++ more}
        else
          {state, effects}
        end
      end)

    {state, more} = pump(state, now)
    {state, effects ++ more}
  end

  defp prune_messages(state, now) do
    retained =
      Map.reject(state.retained, fn {_topic, message} -> expired_message?(message, now) end)

    sessions =
      Map.new(state.sessions, fn {id, session} ->
        deliveries =
          Enum.reject(
            session.deliveries,
            &(&1.packet_id == nil and expired_message?(&1.message, now))
          )

        {id, %{session | deliveries: deliveries}}
      end)

    hosts =
      Map.new(state.hosts, fn {id, host} ->
        deliveries =
          Enum.reject(host.deliveries, &(not &1.delivered and expired_message?(&1.message, now)))

        {id, %{host | deliveries: deliveries}}
      end)

    %{state | retained: retained, sessions: sessions, hosts: hosts}
  end

  defp session_expired?(session, now),
    do: session.connection == nil and session.expires_at != nil and session.expires_at <= now

  defp expired_message?(message, now), do: message.expires_at != nil and message.expires_at <= now

  defp incoming_bytes(state),
    do:
      Enum.sum(
        Enum.map(state.connections, fn {_id, conn} ->
          Enum.sum(Enum.map(conn.incoming, fn {_id, packet} -> :erlang.external_size(packet) end))
        end)
      ) +
        Enum.sum(
          Enum.map(state.sessions, fn {_id, session} ->
            :erlang.external_size(session.incoming_qos2)
          end)
        )

  defp prepared_bytes(state),
    do: Enum.sum(Enum.map(state.operations, fn {_id, op} -> op.size end))

  defp message_bytes(message),
    do: byte_size(message.encoded) + byte_size(message.payload) + byte_size(message.topic)

  defp pending_entries(state),
    do:
      Enum.flat_map(state.sessions, fn {_id, session} -> session.deliveries end) ++
        Enum.flat_map(state.hosts, fn {_id, host} -> host.deliveries end)

  defp pending_bytes(entries), do: Enum.sum(Enum.map(entries, &message_bytes(&1.message)))

  defp replace_entry(entries, entry),
    do: Enum.map(entries, fn old -> if old.id == entry.id, do: entry, else: old end)

  defp put_conn(state, id, conn), do: %{state | connections: Map.put(state.connections, id, conn)}

  defp put_session(state, id, session),
    do: %{state | sessions: Map.put(state.sessions, id, session)}

  defp current?(state, id, generation),
    do: match?(%{generation: ^generation}, Map.get(state.connections, id))

  defp live?(state, %{kind: :dispatch} = op), do: dispatch_live?(state, op)
  defp live?(state, %{source: {:network, id, generation}}), do: current?(state, id, generation)
  defp live?(_state, %{source: {:host, _}}), do: true
  defp live?(state, %{source: {:will, id}}), do: Map.has_key?(state.wills, id)
  defp valid_packet?(state, packet), do: match?({:ok, _}, Packet.encode(packet, state.codec))

  defp same_publish?(old, packet) do
    %{old | dup: true, properties: Keyword.delete(old.properties, :message_expiry_interval)} ==
      %{packet | properties: Keyword.delete(packet.properties, :message_expiry_interval)}
  end

  defp options(qos) when is_integer(qos), do: options(%{qos: qos})

  defp options(options),
    do:
      Map.merge(
        %{qos: 0, no_local: false, retain_as_published: false, retain_handling: 0},
        options
      )

  defp retained_candidates(state, {:network, client_id}, subscriptions, now) do
    grants = Enum.map(subscriptions, fn _ -> :ok end)
    network_retained_candidates(state, client_id, subscriptions, grants, now)
  end

  defp network_retained_candidates(state, client_id, subscriptions, grants, now) do
    # Each successful occurrence changes the subscription seen by the next one.
    # Keep replay QoS from that occurrence even if a later duplicate replaces it.
    {targets, _existing} =
      Enum.zip(subscriptions, grants)
      |> Enum.flat_map_reduce(state.sessions[client_id].subscriptions, fn
        {{filter, opts}, :ok}, existing ->
          if Topic.valid_filter?(filter) do
            targets =
              retained_candidates(state, [{filter, opts}], existing, {:network, client_id}, now)

            {targets, Map.put(existing, filter, true)}
          else
            {[], existing}
          end

        _, existing ->
          {[], existing}
      end)

    targets
  end

  defp retained_candidates(state, subscriptions, existing, origin, now) do
    state.retained
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.flat_map(fn {_topic, message} ->
      matches =
        Enum.filter(subscriptions, fn {filter, opts} ->
          Topic.valid_filter?(filter) and Topic.matches?(filter, message.topic) and
            opts.retain_handling != 2 and
            not (opts.retain_handling == 1 and Map.has_key?(existing, filter)) and
            not (opts.no_local and message.origin == origin)
        end)

      if matches == [] or expired_message?(message, now),
        do: [],
        else: [
          %{
            message: message,
            filters: Enum.map(matches, &elem(&1, 0)),
            qos:
              min(
                message.qos,
                Enum.max(
                  Enum.map(matches, fn {_, opts} -> min(opts.qos, state.limits.maximum_qos) end)
                )
              ),
            retain: true
          }
        ]
    end)
  end

  defp commit_subscribe(state, op, now) do
    case decisions(op) do
      {:ok, all} ->
        grants = Enum.take(all, length(op.subscriptions))
        fresh = network_retained_candidates(state, op.client_id, op.subscriptions, grants, now)

        if fresh != op.retained,
          do: refresh_subscription(state, op, fresh, now),
          else: commit_subscribe_snapshot(state, op, now)

      _ ->
        commit_subscribe_snapshot(state, op, now)
    end
  end

  defp commit_subscribe_snapshot(state, op, now) do
    {:network, id, _generation} = op.source

    case decisions(op) do
      {:error, reason} ->
        disconnect(state, id, {:auth_failed, reason}, 0x83, now)

      {:ok, all} ->
        {grants, delivery_decisions} = Enum.split(all, length(op.subscriptions))
        session = state.sessions[op.client_id]

        {state, subscriptions, codes} =
          Enum.zip(op.subscriptions, grants)
          |> Enum.reduce({state, session.subscriptions, []}, fn {{filter, opts}, decision},
                                                                {state, subscriptions, codes} ->
            code =
              cond do
                not Topic.valid_filter?(filter) -> 0x9E
                decision == {:error, :denied} -> 0x87
                true -> min(opts.qos, state.limits.maximum_qos)
              end

            if code < 0x80 do
              sub = %{id: state.next_id, options: %{opts | qos: code}}

              {%{state | next_id: state.next_id + 1}, Map.put(subscriptions, filter, sub),
               codes ++ [code]}
            else
              {state, subscriptions, codes ++ [code]}
            end
          end)

        prospective = put_session(state, op.client_id, %{session | subscriptions: subscriptions})

        targets =
          Enum.zip(op.retained, delivery_decisions)
          |> Enum.flat_map(fn {target, decision} ->
            filters = target.filters

            if decision == :ok and not expired_message?(target.message, now) do
              [
                %{
                  kind: :network,
                  recipient: op.client_id,
                  token: session.token,
                  principal: session.principal,
                  incarnations: Enum.map(filters, &{&1, subscriptions[&1].id}),
                  message: target.message,
                  qos: target.qos,
                  retain: true
                }
              ]
            else
              []
            end
          end)

        if subscription_capacity?(prospective) and capacity?(prospective, targets, state.retained) do
          state = install_targets(prospective, targets)

          {state, effects} =
            send_packet(
              state,
              id,
              %Suback{packet_id: op.packet_id, reason_codes: codes},
              {:request_ack, op.packet_id},
              now
            )

          {state, more} = pump_session(state, op.client_id, now)
          {state, effects ++ more}
        else
          send_packet(
            state,
            id,
            %Suback{
              packet_id: op.packet_id,
              reason_codes: Enum.map(codes, fn code -> if code < 0x80, do: 0x97, else: code end)
            },
            {:request_ack, op.packet_id},
            now
          )
        end
    end
  end

  defp subscription_capacity?(state) do
    subs =
      Enum.flat_map(state.sessions, fn {_id, session} -> Map.to_list(session.subscriptions) end)

    host_bytes =
      Enum.sum(
        Enum.map(state.hosts, fn {_id, host} ->
          :erlang.external_size({host.caller, host.principal, host.filter})
        end)
      )

    length(subs) <= state.limits.max_subscriptions and
      Enum.sum(Enum.map(subs, fn {filter, _sub} -> byte_size(filter) end)) + host_bytes <=
        state.limits.max_subscription_bytes
  end

  defp host_operation(state, caller, principal, opid, command, now) do
    state = prune_messages(state, now)

    duplicate =
      Enum.any?(state.operations, fn {_id, op} ->
        op.source == {:host, caller} and Map.get(op, :reply) == opid
      end)

    if duplicate do
      {state, [{:complete, opid, {:error, :operation_in_use}}]}
    else
      host_command(state, caller, principal, opid, command, now)
    end
  end

  defp host_command(state, caller, principal, opid, {:publish, %Publish{} = packet, bytes}, now) do
    checked = %{packet | packet_id: if(packet.qos == 0, do: nil, else: 1), dup: false}

    cond do
      packet.qos not in 0..state.limits.maximum_qos ->
        host_error(state, opid, :qos_not_supported)

      not is_binary(bytes) or not valid_packet?(state, checked) ->
        host_error(state, opid, :invalid_publication)

      byte_size(bytes) > state.limits.max_packet_size ->
        host_error(state, opid, :packet_size)

      Keyword.has_key?(packet.properties, :topic_alias) or
          Keyword.has_key?(packet.properties, :subscription_identifier) ->
        host_error(state, opid, :unsupported_property)

      Keyword.get(packet.properties, :payload_format_indicator, 0) == 1 and
          not String.valid?(packet.payload) ->
        host_error(state, opid, :payload_format_invalid)

      true ->
        publication(
          state,
          {:host, caller},
          0,
          principal,
          message(packet, bytes, {:host, caller}, now),
          opid,
          now
        )
    end
  end

  defp host_command(state, caller, principal, opid, {:subscribe, filter, opts}, now) do
    if Topic.valid_filter?(filter) and (is_integer(opts) or is_map(opts)) and
         valid_host_options?(filter, opts, state) do
      opts = options(opts)
      retained = retained_candidates(state, [{filter, opts}], %{}, {:host, caller}, now)

      request =
        [{:authorize, principal, :subscribe, filter}] ++
          Enum.map(retained, fn target ->
            {:authorize, principal, :deliver, target.message.topic}
          end)

      prepare(
        state,
        %{
          kind: :host_subscribe,
          source: {:host, caller},
          generation: 0,
          principal: principal,
          filter: filter,
          options: %{opts | qos: min(opts.qos, state.limits.maximum_qos)},
          retained: retained,
          reply: opid,
          encoded: <<>>
        },
        request,
        now
      )
    else
      host_error(state, opid, :invalid_subscription)
    end
  end

  defp host_command(state, caller, _principal, opid, {:demand, subscription, count}, now) do
    case Map.get(state.hosts, subscription) do
      %{caller: ^caller} = host ->
        total = Enum.sum(Enum.map(state.hosts, fn {_id, host} -> host.demand end))

        if is_integer(count) and count > 0 and total + count <= state.limits.max_demand do
          state = %{
            state
            | hosts: Map.put(state.hosts, subscription, %{host | demand: host.demand + count})
          }

          {state, effects} = pump_host(state, subscription, now)
          {state, [{:complete, opid, {:ok, :ok}}] ++ effects}
        else
          host_error(state, opid, :demand_limit)
        end

      _ ->
        host_error(state, opid, :not_owner)
    end
  end

  defp host_command(state, caller, _principal, opid, {:ack, delivery}, now) do
    found =
      Enum.find(state.hosts, fn {_id, host} ->
        host.caller == caller and
          Enum.any?(host.deliveries, &(&1.id == delivery and &1.delivered))
      end)

    case found do
      nil ->
        host_error(state, opid, :not_owner)

      {id, host} ->
        host = %{host | deliveries: Enum.reject(host.deliveries, &(&1.id == delivery))}
        {state, effects} = pump(%{state | hosts: Map.put(state.hosts, id, host)}, now)
        {state, [{:complete, opid, {:ok, :ok}}] ++ effects}
    end
  end

  defp host_command(state, caller, _principal, opid, {:unsubscribe, subscription}, _now) do
    case Map.get(state.hosts, subscription) do
      %{caller: ^caller} ->
        {%{state | hosts: Map.delete(state.hosts, subscription)}, [{:complete, opid, {:ok, :ok}}]}

      _ ->
        host_error(state, opid, :not_owner)
    end
  end

  defp host_command(state, _caller, _principal, opid, _command, _now),
    do: host_error(state, opid, :invalid_command)

  defp valid_host_options?(filter, opts, state) do
    valid_packet?(state, %Subscribe{packet_id: 1, subscriptions: [{filter, opts}]})
  end

  defp commit_host_subscribe(state, op, now) do
    {:host, caller} = op.source
    fresh = retained_candidates(state, [{op.filter, op.options}], %{}, {:host, caller}, now)

    case decisions(op) do
      {:ok, _} when fresh != op.retained -> refresh_subscription(state, op, fresh, now)
      _ -> commit_host_subscribe_snapshot(state, op, now)
    end
  end

  defp commit_host_subscribe_snapshot(state, op, now) do
    case decisions(op) do
      {:error, reason} ->
        host_error(state, op.reply, {:auth_failed, reason})

      {:ok, [{:error, :denied} | _]} ->
        host_error(state, op.reply, :denied)

      {:ok, [:ok | results]} ->
        {:host, caller} = op.source
        id = state.next_id

        host = %{
          id: id,
          caller: caller,
          principal: op.principal,
          filter: op.filter,
          options: op.options,
          demand: 0,
          deliveries: []
        }

        targets =
          Enum.zip(op.retained, results)
          |> Enum.flat_map(fn {target, decision} ->
            if decision == :ok and not expired_message?(target.message, now),
              do: [
                %{
                  kind: :host,
                  recipient: id,
                  token: id,
                  principal: op.principal,
                  incarnations: [],
                  message: target.message,
                  qos: target.qos,
                  retain: true
                }
              ],
              else: []
          end)

        candidate = %{state | hosts: Map.put(state.hosts, id, host)}

        if map_size(state.hosts) < state.limits.max_host_subscriptions and
             subscription_capacity?(candidate) and
             capacity?(state, targets, state.retained) do
          state = %{state | hosts: Map.put(state.hosts, id, host), next_id: id + 1}
          state = install_targets(state, targets)
          {state, effects} = pump_host(state, id, now)
          {state, [{:complete, op.reply, {:ok, id}}] ++ effects}
        else
          host_error(state, op.reply, :capacity)
        end
    end
  end

  defp pump_host(state, subscription, now) do
    case Map.get(state.hosts, subscription) do
      nil ->
        {state, []}

      host ->
        outstanding = Enum.count(pending_entries(state), & &1.delivered)

        case Enum.find(host.deliveries, &(not &1.delivered)) do
          nil ->
            {state, []}

          entry ->
            if host.demand > 0 and outstanding < state.limits.max_host_outstanding and
                 entry.dispatch == nil and (entry.retry_at == nil or entry.retry_at <= now),
               do: request_dispatch(state, :host, subscription, host, entry, now),
               else: {state, []}
        end
    end
  end

  defp request_dispatch(state, kind, recipient, owner, entry, now) do
    {generation, connection, token} =
      case kind do
        :network -> {elem(owner.connection, 1), owner.connection, owner.token}
        :host -> {0, nil, owner.id}
      end

    op = %{
      kind: :dispatch,
      source: {:dispatch, kind, recipient},
      generation: generation,
      delivery_kind: kind,
      recipient: recipient,
      entry_id: entry.id,
      token: token,
      connection: connection,
      encoded: <<>>
    }

    opid = state.next_id
    request = [{:authorize, owner.principal, :deliver, entry.message.topic}]
    {state, effects} = prepare(state, op, request, now)

    if Map.has_key?(state.operations, opid) do
      entry = %{entry | dispatch: opid}
      {put_delivery(state, kind, recipient, entry), effects}
    else
      {state, effects}
    end
  end

  defp commit_dispatch(state, op, now) do
    case dispatch_entry(state, op) do
      nil ->
        pump_recipient(state, op, now)

      entry ->
        case decisions(op) do
          {:ok, [:ok]} ->
            deliver_authorized(state, op, entry, now)

          {:ok, [{:error, :denied}]} ->
            if op.delivery_kind == :network and entry.qos > 0 and entry.ever_sent do
              {state, _} = retry_dispatch(state, op, now)
              disconnect(state, elem(op.connection, 0), :delivery_denied, 0x87, now)
            else
              state = remove_delivery(state, op.delivery_kind, op.recipient, entry.id)
              pump_recipient(state, op, now)
            end

          {:error, _reason} ->
            retry_dispatch(state, op, now)
        end
    end
  end

  defp deliver_authorized(state, %{delivery_kind: :network} = op, entry, now) do
    session = state.sessions[op.recipient]
    {id, generation} = op.connection
    conn = state.connections[id]

    cond do
      entry.packet_id == nil and expired_message?(entry.message, now) ->
        state = remove_delivery(state, :network, op.recipient, entry.id)
        pump_recipient(state, op, now)

      entry.qos > 0 and conn.send_quota == 0 ->
        retry_dispatch(state, op, now)

      true ->
        packet_id = if entry.qos > 0, do: entry.packet_id || free_packet_id(session), else: nil
        packet = recipient_packet(entry, packet_id, now)

        if outbound_fits?(state, conn, packet) do
          {state, effects} =
            send_packet(state, id, packet, {:delivery, op.recipient, entry.id, entry.qos}, now)

          if current?(state, id, generation) and Map.has_key?(state.sessions, op.recipient) do
            entry = %{
              entry
              | packet_id: packet_id,
                sending: true,
                ever_sent: true,
                dispatch: nil,
                retry_at: nil
            }

            state = put_delivery(state, :network, op.recipient, entry)
            session = state.sessions[op.recipient]
            conn = state.connections[id]

            state =
              if entry.qos > 0,
                do: put_conn(state, id, %{conn | send_quota: conn.send_quota - 1}),
                else: state

            state =
              put_session(state, op.recipient, %{
                session
                | next_packet_id:
                    if(packet_id == nil,
                      do: session.next_packet_id,
                      else: rem(packet_id, 65_535) + 1
                    )
              })

            {state, more} = pump_recipient(state, op, now)
            {state, effects ++ more}
          else
            # A failed write reservation may close the connection and end its session.
            # No following entry may dereference that removed owner.
            {state, effects}
          end
        else
          # MQTT-3.1.2-25 completes an oversized Application Message locally.
          # It must not disconnect an otherwise healthy recipient.
          state = remove_delivery(state, :network, op.recipient, entry.id)
          pump_recipient(state, op, now)
        end
    end
  end

  defp deliver_authorized(state, %{delivery_kind: :host} = op, entry, now) do
    host = state.hosts[op.recipient]
    outstanding = Enum.count(pending_entries(state), & &1.delivered)

    cond do
      expired_message?(entry.message, now) ->
        state = remove_delivery(state, :host, op.recipient, entry.id)
        pump_recipient(state, op, now)

      host.demand == 0 or outstanding >= state.limits.max_host_outstanding ->
        retry_dispatch(state, op, now)

      true ->
        entry = %{entry | delivered: true, dispatch: nil, retry_at: nil}
        state = put_delivery(state, :host, op.recipient, entry)
        host = state.hosts[op.recipient]

        state = %{
          state
          | hosts: Map.put(state.hosts, op.recipient, %{host | demand: host.demand - 1})
        }

        message = %{entry.message | qos: entry.qos, retain: entry.retain}
        {state, more} = pump_recipient(state, op, now)
        {state, [{:host_delivery, op.recipient, entry.id, message}] ++ more}
    end
  end

  defp outbound_fits?(state, conn, packet) do
    case Packet.encode(packet, state.codec) do
      {:ok, bytes} -> byte_size(bytes) <= conn.peer_maximum
      _ -> false
    end
  end

  defp retry_dispatch(state, op, now) do
    case dispatch_entry(state, op, false) do
      nil ->
        {state, []}

      entry ->
        entry = %{entry | dispatch: nil, retry_at: now + state.limits.dispatch_retry_interval}
        {put_delivery(state, op.delivery_kind, op.recipient, entry), []}
    end
  end

  defp dispatch_entry(state, op, require_marker \\ true) do
    owner =
      case op.delivery_kind do
        :network -> Map.get(state.sessions, op.recipient)
        :host -> Map.get(state.hosts, op.recipient)
      end

    if owner == nil,
      do: nil,
      else:
        Enum.find(owner.deliveries, fn entry ->
          entry.id == op.entry_id and (not require_marker or entry.dispatch == op.id)
        end)
  end

  defp dispatch_live?(state, op) do
    case op.delivery_kind do
      :network ->
        case Map.get(state.sessions, op.recipient) do
          %{token: token, connection: connection}
          when token == op.token and connection == op.connection ->
            current?(state, elem(connection, 0), op.generation) and
              dispatch_entry(state, op) != nil

          _ ->
            false
        end

      :host ->
        Map.has_key?(state.hosts, op.recipient) and dispatch_entry(state, op) != nil
    end
  end

  defp put_delivery(state, :network, recipient, entry) do
    session = state.sessions[recipient]

    put_session(state, recipient, %{
      session
      | deliveries: replace_entry(session.deliveries, entry)
    })
  end

  defp put_delivery(state, :host, recipient, entry) do
    host = state.hosts[recipient]

    %{
      state
      | hosts:
          Map.put(state.hosts, recipient, %{
            host
            | deliveries: replace_entry(host.deliveries, entry)
          })
    }
  end

  defp remove_delivery(state, :network, recipient, id) do
    session = state.sessions[recipient]

    put_session(state, recipient, %{
      session
      | deliveries: Enum.reject(session.deliveries, &(&1.id == id))
    })
  end

  defp remove_delivery(state, :host, recipient, id) do
    host = state.hosts[recipient]

    %{
      state
      | hosts:
          Map.put(state.hosts, recipient, %{
            host
            | deliveries: Enum.reject(host.deliveries, &(&1.id == id))
          })
    }
  end

  defp pump_recipient(state, %{delivery_kind: :network, recipient: id}, now),
    do: pump_session(state, id, now)

  defp pump_recipient(state, %{delivery_kind: :host, recipient: id}, now),
    do: pump_host(state, id, now)

  defp refresh_subscription(state, op, fresh, now) do
    {principal, subscribe_requests} =
      case op.kind do
        :subscribe ->
          principal = state.sessions[op.client_id].principal

          {principal,
           Enum.map(op.subscriptions, fn {filter, _} ->
             {:authorize, principal, :subscribe, filter}
           end)}

        :host_subscribe ->
          {op.principal, [{:authorize, op.principal, :subscribe, op.filter}]}
      end

    request =
      subscribe_requests ++
        Enum.map(fresh, fn target -> {:authorize, principal, :deliver, target.message.topic} end)

    op = %{op | retained: fresh, request: request, decision: :pending}
    size = :erlang.external_size(op)

    if now < op.deadline and prepared_bytes(state) + size <= state.limits.max_prepared_bytes do
      op = %{op | size: size}

      {%{state | operations: Map.put(state.operations, op.id, op)},
       [{:authorize, op.id, request, op.deadline}]}
    else
      preparation_failed(state, op, :prepared_limit, now)
    end
  end

  defp available_client_id(state, counter) do
    candidate = "graviton-#{counter}"

    pending =
      Enum.any?(state.operations, fn {_id, op} ->
        match?(%{packet: %Connect{client_id: ^candidate}}, op)
      end)

    if Map.has_key?(state.sessions, candidate) or pending,
      do: available_client_id(state, counter + 1),
      else: candidate
  end

  defp host_error(state, opid, reason), do: {state, [{:complete, opid, {:error, reason}}]}
end
