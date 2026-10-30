defmodule GravitonMQ.MQTT.Client do
  @moduledoc """
  Pure, bounded MQTT 5.0 QoS 0/1/2 client transitions.

  Time inputs are signed monotonic milliseconds supplied by the owner. Effects
  describe transport, notification and reconnect work; this module performs no
  IO, clock reads, authentication callbacks or process operations. A successful
  QoS 0 send means socket acceptance; a successful QoS 1 PUBACK means remote
  MQTT acceptance. QoS 2 completes at PUBCOMP, without a business exactly-once,
  subscriber-processing or disk-durability claim.
  """

  alias GravitonMQ.MQTT.Codec.{Limits, Packet}
  alias GravitonMQ.MQTT.Packet.{Connect, Connack, Disconnect, Pingreq, Pingresp, Puback, Publish}
  alias GravitonMQ.MQTT.Packet.{Pubrec, Pubrel, Pubcomp, Suback, Subscribe, Unsuback, Unsubscribe}

  defstruct [
    :connect,
    :last_sent,
    :ping_at,
    :disconnected_at,
    :handshake_connect,
    phase: :idle,
    generation: 0,
    peer: %{},
    outbound: %{},
    incoming: %{},
    received_inflight: MapSet.new(),
    deliveries: %{},
    subscriptions: %{},
    demand: 0,
    next_packet_id: 1,
    next_delivery_id: 1,
    next_order: 1,
    max_inflight: 16,
    max_operations: 32,
    max_subscriptions: 1_024,
    max_deliveries: 32,
    receive_maximum: 32,
    send_quota: 0,
    initial_send_quota: 0,
    reconnect_max: 0,
    reconnect_attempts: 0,
    session_expiry_interval: 0,
    session_available: false,
    ever_connected: false,
    limits: %Limits{}
  ]

  @type t :: %__MODULE__{}
  @type effect ::
          {:open, pos_integer()}
          | {:send, struct(), term()}
          | {:notify, term()}
          | {:close, term()}
          | {:retry, pos_integer()}

  @spec new(Connect.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(connect, options \\ [])

  def new(%Connect{version: version}, _options) when version != :v5,
    do: {:error, :unsupported_version}

  def new(%Connect{} = connect, options) when is_list(options) do
    with true <- Keyword.keyword?(options),
         {:ok, limits} <-
           Limits.validate(Keyword.get(options, :limits, Limits.default()), :packet_encode),
         {:ok, _bytes} <- Packet.encode(connect, limits),
         :ok <- connect_profile(connect),
         :ok <- options_valid(options) do
      max_deliveries = Keyword.get(options, :max_deliveries, 32)

      receive_maximum =
        Keyword.get(connect.properties, :receive_maximum, min(max_deliveries, 65_535))

      if receive_maximum > max_deliveries do
        {:error, :receive_maximum_exceeds_deliveries}
      else
        properties = Keyword.put(connect.properties, :receive_maximum, receive_maximum)
        properties = Keyword.put_new(properties, :topic_alias_maximum, 0)
        connect = %{connect | properties: properties}

        case Packet.encode(connect, limits) do
          {:ok, _bytes} ->
            {:ok,
             %__MODULE__{
               connect: connect,
               limits: limits,
               max_inflight: Keyword.get(options, :max_inflight, 16),
               max_operations: Keyword.get(options, :max_operations, 32),
               max_subscriptions: Keyword.get(options, :max_subscriptions, 1_024),
               max_deliveries: max_deliveries,
               receive_maximum: receive_maximum,
               reconnect_max: Keyword.get(options, :reconnect_max, 0),
               session_expiry_interval: Keyword.get(properties, :session_expiry_interval, 0)
             }}

          {:error, error} ->
            {:error, {:invalid_connect, error.reason}}
        end
      end
    else
      false -> {:error, :invalid_options}
      {:error, %{reason: reason}} -> {:error, {:invalid_connect, reason}}
      {:error, _reason} = error -> error
    end
  end

  def new(_connect, _options), do: {:error, :invalid_connect}

  defp connect_profile(connect) do
    cond do
      Keyword.has_key?(connect.properties, :authentication_method) or
          Keyword.has_key?(connect.properties, :authentication_data) ->
        {:error, :enhanced_authentication_unsupported}

      Keyword.get(connect.properties, :topic_alias_maximum, 0) != 0 ->
        {:error, :topic_aliases_unsupported}

      connect.will != nil and
        Keyword.get(connect.will.properties, :payload_format_indicator, 0) == 1 and
          not String.valid?(connect.will.payload) ->
        {:error, :payload_format_invalid}

      true ->
        :ok
    end
  end

  defp options_valid(options) do
    positive = [:max_inflight, :max_operations, :max_deliveries, :max_subscriptions]

    values_valid? =
      Enum.all?(positive, fn key ->
        value = Keyword.get(options, key, 1)
        is_integer(value) and value > 0
      end)

    retries = Keyword.get(options, :reconnect_max, 0)

    if values_valid? and is_integer(retries) and retries >= 0,
      do: :ok,
      else: {:error, :invalid_options}
  end

  @spec transition(t(), term()) :: {t(), [effect()]}
  def transition(%__MODULE__{phase: :idle} = state, :start),
    do: {%{state | phase: :connecting, generation: 1}, [{:open, 1}]}

  def transition(state, {:transport_up, generation, now}) do
    if current?(state, generation) and state.phase == :connecting do
      resuming? = local_session?(state, now)
      clean_start = if state.ever_connected, do: not resuming?, else: state.connect.clean_start
      connect = %{state.connect | clean_start: clean_start}

      {%{
         state
         | phase: :awaiting_connack,
           handshake_connect: connect,
           last_sent: now,
           ping_at: nil
       }, [{:send, connect, :connect}]}
    else
      {state, []}
    end
  end

  def transition(state, {:transport_down, generation, reason, now}) do
    if current?(state, generation) and state.phase not in [:stopped, :offline, :idle],
      do: transport_loss(state, reason, now, false),
      else: {state, []}
  end

  def transition(%{phase: :offline} = state, {:retry, _now}) do
    generation = state.generation + 1
    {%{state | phase: :connecting, generation: generation}, [{:open, generation}]}
  end

  def transition(state, {:handshake_timeout, generation, now}) do
    if current?(state, generation) and state.phase in [:connecting, :awaiting_connack],
      do: transport_loss(state, :handshake_timeout, now, true),
      else: {state, []}
  end

  def transition(state, {:protocol_error, generation, reason, _now}) do
    code = if reason == {:decode, :limit_exceeded, :packet_too_large}, do: 0x95, else: 0x81

    if current?(state, generation) and state.phase not in [:stopped, :offline, :idle],
      do: terminal(state, {:protocol_error, reason}, code),
      else: {state, []}
  end

  def transition(state, {:packet, generation, packet, now}) do
    cond do
      not current?(state, generation) ->
        {state, []}

      state.phase in [:stopped, :offline, :idle, :connecting] ->
        {state, []}

      forbidden_problem_information?(state, packet) ->
        terminal(state, :problem_information_not_requested, 0x82)

      true ->
        receive_packet(state, packet, now)
    end
  end

  def transition(state, {:sent, generation, token, result, now}) do
    if current?(state, generation), do: sent(state, token, result, now), else: {state, []}
  end

  def transition(state, {:publish, ref, packet, now}),
    do: operation(state, :publish, ref, packet, now)

  def transition(state, {:subscribe, ref, packet, now}),
    do: operation(state, :subscribe, ref, packet, now)

  def transition(state, {:unsubscribe, ref, packet, now}),
    do: operation(state, :unsubscribe, ref, packet, now)

  def transition(state, {:operation_timeout, ref, _now}) do
    if Map.has_key?(state.outbound, ref) and
         not Map.get(state.outbound[ref], :host_completed, false),
       do: terminal(state, :operation_timeout, 0x80),
       else: {state, []}
  end

  def transition(state, {:demand, count, now}) do
    cond do
      not is_integer(count) or count <= 0 ->
        {state, [{:notify, {:demand, {:error, :invalid_demand}}}]}

      count + state.demand > state.max_deliveries ->
        {state, [{:notify, {:demand, {:error, :demand_limit}}}]}

      state.phase != :connected ->
        {state, [{:notify, {:demand, {:error, :not_connected}}}]}

      true ->
        flush_deliveries(%{state | demand: state.demand + count}, now)
    end
  end

  def transition(state, {:ack, id, _now}) do
    case Map.fetch(state.deliveries, id) do
      _ when state.phase != :connected ->
        ack_error(state, id, :not_connected)

      {:ok, %{delivered: true, ack_pending: false, packet: %Publish{qos: 0}}} ->
        {%{state | deliveries: Map.delete(state.deliveries, id)}, []}

      {:ok, %{delivered: true, ack_pending: false, packet: %Publish{qos: qos}} = entry}
      when qos in [1, 2] ->
        state = %{state | deliveries: Map.put(state.deliveries, id, %{entry | ack_pending: true})}
        flush_acknowledgements(state)

      {:ok, %{ack_pending: true}} ->
        ack_error(state, id, :ack_pending)

      {:ok, _entry} ->
        ack_error(state, id, :delivery_not_owned)

      :error ->
        ack_error(state, id, :unknown_delivery)
    end
  end

  def transition(state, {:tick, generation, now}) do
    if current?(state, generation) and state.phase == :connected do
      {state, effects} = flush_deliveries(state, now)
      {state, timer_effects} = keep_alive(state, now)
      {state, effects ++ timer_effects}
    else
      {state, []}
    end
  end

  def transition(%{phase: :stopped} = state, {:stop, _now}), do: {state, []}

  def transition(state, {:stop, now}) do
    {state, notifications} = finish_host_operations(state)

    effects =
      if state.phase == :connected and peer_fits?(state, %Disconnect{}),
        do: [{:send, %Disconnect{}, :disconnect}],
        else: [{:close, :normal}]

    {%{state | phase: :stopped, ping_at: nil, disconnected_at: state.disconnected_at || now},
     notifications ++ effects ++ [{:notify, {:disconnected, :normal}}]}
  end

  def transition(state, _event), do: {state, []}

  defp forbidden_problem_information?(state, %{__struct__: module, properties: properties})
       when module in [Puback, Pubrec, Pubrel, Pubcomp, Suback, Unsuback] do
    Keyword.get(state.connect.properties, :request_problem_information, 1) == 0 and
      is_list(properties) and
      Enum.any?(properties, fn
        {key, _} when key in [:reason_string, :user_property] -> true
        _ -> false
      end)
  end

  defp forbidden_problem_information?(_state, _packet), do: false

  defp receive_packet(%{phase: :awaiting_connack} = state, %Connack{} = packet, now) do
    cond do
      packet.reason_code != 0 ->
        terminal(state, {:connack, packet.reason_code}, nil)

      not valid_packet?(packet, state) ->
        terminal(state, :invalid_connack, nil)

      Keyword.has_key?(packet.properties, :authentication_method) or
          Keyword.has_key?(packet.properties, :authentication_data) ->
        terminal(state, :enhanced_authentication_unsupported, nil)

      packet.session_present and state.handshake_connect.clean_start ->
        terminal(state, :invalid_session_present, nil)

      packet.session_present and not local_session?(state, now) ->
        terminal(state, :local_session_missing, nil)

      true ->
        connected(state, packet, now)
    end
  end

  defp receive_packet(%{phase: :awaiting_connack} = state, _packet, _now),
    do: terminal(state, :expected_connack, nil)

  defp receive_packet(state, %Publish{} = packet, now) do
    cond do
      not valid_packet?(packet, state) ->
        terminal(state, :malformed_publish, 0x81)

      Keyword.has_key?(packet.properties, :topic_alias) ->
        terminal(state, :topic_alias_invalid, 0x94)

      Keyword.get(packet.properties, :payload_format_indicator, 0) == 1 and
          not String.valid?(packet.payload) ->
        terminal(state, :payload_format_invalid, 0x99)

      true ->
        incoming_publication(state, packet, now)
    end
  end

  defp receive_packet(state, %Puback{} = packet, now) do
    case find_operation(state, packet.packet_id, :publish) do
      {ref, %{packet: %Publish{qos: 1}}} ->
        if valid_packet?(packet, state) do
          result =
            if packet.reason_code < 0x80,
              do: {:ok, :puback},
              else: {:error, {:puback, packet.reason_code}}

          complete_publication(state, ref, result, now)
        else
          terminal(state, :invalid_puback, 0x82)
        end

      _ ->
        terminal(state, :invalid_puback, 0x82)
    end
  end

  defp receive_packet(state, %Pubrec{} = packet, now) do
    case find_operation(state, packet.packet_id, :publish) do
      {ref, %{packet: %Publish{qos: 2}, qos2_phase: phase} = entry} ->
        cond do
          not valid_packet?(packet, state) ->
            terminal(state, :invalid_pubrec, 0x82)

          phase == :await_pubrec and packet.reason_code >= 0x80 ->
            complete_publication(state, ref, {:error, {:pubrec, packet.reason_code}}, now)

          packet.reason_code < 0x80 ->
            {state, entry} =
              if phase == :await_pubrec do
                entry =
                  Map.merge(entry, %{qos2_phase: :await_pubcomp, release_order: state.next_order})

                {%{
                   state
                   | outbound: Map.put(state.outbound, ref, entry),
                     next_order: state.next_order + 1
                 }, entry}
              else
                {state, entry}
              end

            send_release(state, ref, entry)

          true ->
            terminal(state, :invalid_pubrec, 0x82)
        end

      _ ->
        terminal(state, :invalid_pubrec, 0x82)
    end
  end

  defp receive_packet(state, %Pubcomp{} = packet, now) do
    case find_operation(state, packet.packet_id, :publish) do
      {ref, %{packet: %Publish{qos: 2}, qos2_phase: :await_pubcomp}} ->
        if valid_packet?(packet, state) do
          result =
            if packet.reason_code == 0,
              do: {:ok, :pubcomp},
              else: {:error, {:pubcomp, packet.reason_code}}

          complete_publication(state, ref, result, now)
        else
          terminal(state, :invalid_pubcomp, 0x82)
        end

      _ ->
        terminal(state, :invalid_pubcomp, 0x82)
    end
  end

  defp receive_packet(state, %Pubrel{} = packet, _now) do
    if valid_packet?(packet, state) do
      case Map.fetch(state.incoming, packet.packet_id) do
        {:ok, id} ->
          case state.deliveries[id] do
            %{packet: %Publish{qos: 2}, qos2_phase: phase} = entry
            when phase in [:await_pubrel, :completing] ->
              entry = %{entry | qos2_phase: :completing}
              state = %{state | deliveries: Map.put(state.deliveries, id, entry)}

              qos2_response(
                state,
                %Pubcomp{packet_id: packet.packet_id},
                {:qos2_pubcomp, packet.packet_id, id}
              )

            _ ->
              unknown_release(state, packet.packet_id)
          end

        :error ->
          unknown_release(state, packet.packet_id)
      end
    else
      terminal(state, :invalid_pubrel, 0x82)
    end
  end

  defp receive_packet(state, %Suback{} = packet, now) do
    case find_operation(state, packet.packet_id, :subscribe) do
      {ref, %{packet: request}} ->
        requests = request.subscriptions

        if valid_packet?(packet, state) and length(packet.reason_codes) == length(requests) and
             Enum.zip(requests, packet.reason_codes)
             |> Enum.all?(fn {{_filter, options}, grant} ->
               grant >= 0x80 or grant <= requested_qos(options)
             end) do
          subscriptions =
            Enum.zip(requests, packet.reason_codes)
            |> Enum.reduce(state.subscriptions, fn {{filter, _options}, grant}, subscriptions ->
              if grant < 0x80, do: Map.put(subscriptions, filter, grant), else: subscriptions
            end)

          complete(%{state | subscriptions: subscriptions}, ref, {:ok, packet.reason_codes}, now)
        else
          terminal(state, :invalid_suback, 0x82)
        end

      _ ->
        terminal(state, :invalid_suback, 0x82)
    end
  end

  defp receive_packet(state, %Unsuback{} = packet, now) do
    case find_operation(state, packet.packet_id, :unsubscribe) do
      {ref, %{packet: request}} ->
        if valid_packet?(packet, state) and length(packet.reason_codes) == length(request.filters) do
          subscriptions =
            Enum.zip(request.filters, packet.reason_codes)
            |> Enum.reduce(state.subscriptions, fn {filter, code}, subscriptions ->
              if code in [0, 0x11], do: Map.delete(subscriptions, filter), else: subscriptions
            end)

          complete(%{state | subscriptions: subscriptions}, ref, {:ok, packet.reason_codes}, now)
        else
          terminal(state, :invalid_unsuback, 0x82)
        end

      _ ->
        terminal(state, :invalid_unsuback, 0x82)
    end
  end

  defp receive_packet(%{ping_at: nil} = state, %Pingresp{}, _now),
    do: terminal(state, :unexpected_pingresp, 0x82)

  defp receive_packet(state, %Pingresp{}, _now),
    do: {%{state | ping_at: nil}, [{:notify, :ping_response}]}

  defp receive_packet(state, %Disconnect{} = packet, _now) do
    cond do
      not valid_packet?(packet, state) ->
        terminal(state, :invalid_disconnect, 0x82)

      packet.reason_code == 0x04 or Keyword.has_key?(packet.properties, :session_expiry_interval) ->
        terminal(state, :invalid_disconnect_direction, 0x82)

      true ->
        terminal(state, {:disconnect, packet.reason_code}, nil)
    end
  end

  defp receive_packet(state, _packet, _now), do: terminal(state, :unexpected_packet, 0x82)

  defp connected(state, packet, now) do
    properties = packet.properties
    assigned = Keyword.get(properties, :assigned_client_identifier)

    cond do
      state.connect.client_id == "" and assigned == nil ->
        terminal(state, :assigned_client_identifier_required, nil)

      state.handshake_connect.client_id != "" and assigned != nil ->
        terminal(state, :unexpected_assigned_client_identifier, nil)

      true ->
        connect =
          if assigned == nil, do: state.connect, else: %{state.connect | client_id: assigned}

        peer = %{
          maximum_qos: Keyword.get(properties, :maximum_qos, 2),
          receive_maximum: Keyword.get(properties, :receive_maximum, 65_535),
          maximum_packet_size: Keyword.get(properties, :maximum_packet_size, 268_435_460),
          retain_available: Keyword.get(properties, :retain_available, 1),
          wildcard_subscription_available:
            Keyword.get(properties, :wildcard_subscription_available, 1),
          subscription_identifier_available:
            Keyword.get(properties, :subscription_identifier_available, 1),
          shared_subscription_available:
            Keyword.get(properties, :shared_subscription_available, 1),
          server_keep_alive: Keyword.get(properties, :server_keep_alive, connect.keep_alive),
          session_expiry_interval:
            Keyword.get(
              properties,
              :session_expiry_interval,
              Keyword.get(state.handshake_connect.properties, :session_expiry_interval, 0)
            ),
          topic_alias_maximum: Keyword.get(properties, :topic_alias_maximum, 0)
        }

        state = %{state | peer: peer, connect: connect}

        if packet.session_present and not resumable_packets?(state) do
          terminal(state, :resume_negotiation_conflict, nil)
        else
          state = %{
            state
            | phase: :connected,
              peer: peer,
              connect: connect,
              ping_at: nil,
              session_expiry_interval: peer.session_expiry_interval,
              session_available: true,
              ever_connected: true,
              reconnect_attempts: 0,
              disconnected_at: nil,
              initial_send_quota: min(state.max_inflight, peer.receive_maximum),
              send_quota: min(state.max_inflight, peer.receive_maximum),
              received_inflight: MapSet.new()
          }

          {state, resumed_effects} =
            if packet.session_present, do: resume(state, now), else: reset_session(state)

          if state.phase == :connected do
            {state, delivery_effects} = flush_deliveries(state, now)
            {state, [{:notify, {:connected, peer}}] ++ resumed_effects ++ delivery_effects}
          else
            {state, resumed_effects}
          end
        end
    end
  end

  defp resume(state, now) do
    {outbound, effects} =
      state.outbound
      |> Enum.sort_by(fn {_ref, entry} -> entry.order end)
      |> Enum.reduce({%{}, []}, fn {ref, entry}, {outbound, effects} ->
        case entry.packet do
          %Publish{qos: qos} = packet when qos in [1, 2] ->
            packet = %{packet | dup: true}

            entry =
              entry |> Map.put(:packet, packet) |> Map.put(:sent, false) |> Map.put(:resend, true)

            {Map.put(outbound, ref, entry), effects}

          _ ->
            {outbound, effects ++ [{:notify, {:operation, ref, {:error, :unknown_outcome}}}]}
        end
      end)

    deliveries = Map.new(state.deliveries, fn {id, entry} -> {id, %{entry | ack_sent: false}} end)
    {state, sends} = send_resumptions(%{state | outbound: outbound, deliveries: deliveries}, now)
    {state, ack_effects} = flush_acknowledgements(state)
    {state, effects ++ sends ++ ack_effects}
  end

  defp resumable_packets?(state) do
    Enum.all?(state.outbound, fn {_ref, entry} ->
      case entry do
        %{packet: %Publish{qos: 2, packet_id: id}, qos2_phase: :await_pubcomp} ->
          peer_fits?(state, %Pubrel{packet_id: id})

        %{packet: %Publish{qos: qos} = packet} when qos in [1, 2] ->
          state.peer.maximum_qos >= qos and
            (not packet.retain or state.peer.retain_available == 1) and peer_fits?(state, packet)

        _ ->
          true
      end
    end)
  end

  defp send_resumptions(state, now) do
    state.outbound
    |> Enum.filter(fn {_ref, entry} -> Map.get(entry, :resend, false) end)
    |> Enum.sort_by(fn {_ref, entry} ->
      case entry do
        %{qos2_phase: :await_pubcomp, release_order: order} -> {0, order}
        _ -> {1, entry.order}
      end
    end)
    |> Enum.reduce({state, []}, fn {ref, entry}, {state, effects} ->
      cond do
        Map.get(entry, :qos2_phase) == :await_pubcomp ->
          entry = %{entry | resend: false}
          state = %{state | outbound: Map.put(state.outbound, ref, entry)}
          {state, more} = send_release(state, ref, entry)
          {state, effects ++ more}

        state.send_quota > 0 and state.phase == :connected ->
          packet = remaining_expiry(entry, now)
          entry = %{entry | resend: false, packet: packet}

          state = %{
            state
            | outbound: Map.put(state.outbound, ref, entry),
              send_quota: state.send_quota - 1
          }

          {state, effects ++ [{:send, packet, publication_token(ref, entry)}]}

        true ->
          {state, effects}
      end
    end)
  end

  defp send_release(state, ref, entry) do
    qos2_response(
      state,
      %Pubrel{packet_id: entry.packet.packet_id},
      {:qos2_pubrel, ref, entry.release_order}
    )
  end

  defp publication_token(ref, %{packet: %Publish{qos: 2}, order: order}),
    do: {:qos2_publish, ref, order}

  defp publication_token(ref, _entry), do: {:operation, ref}

  defp qos2_response(state, packet, token) do
    if peer_fits?(state, packet),
      do: {state, [{:send, packet, token}]},
      else: terminal(state, :peer_packet_size, nil)
  end

  defp unknown_release(state, packet_id),
    do:
      qos2_response(
        state,
        %Pubcomp{packet_id: packet_id, reason_code: 0x92},
        :qos2_unknown_pubcomp
      )

  defp reset_session(state) do
    effects = failed_operations(state, :unknown_outcome)

    effects =
      if map_size(state.subscriptions) > 0,
        do: effects ++ [{:notify, {:subscriptions_lost, state.subscriptions}}],
        else: effects

    effects =
      effects ++
        Enum.map(Map.keys(state.deliveries), fn id ->
          {:notify, {:delivery_cancelled, id, :session_lost}}
        end)

    {%{
       state
       | outbound: %{},
         incoming: %{},
         deliveries: %{},
         subscriptions: %{},
         received_inflight: MapSet.new()
     }, effects}
  end

  defp operation(state, _kind, ref, _packet, _now) when state.phase != :connected,
    do: reject(state, ref, :not_connected)

  defp operation(state, kind, ref, packet, now) do
    cond do
      Map.has_key?(state.outbound, ref) -> reject(state, ref, :operation_in_use)
      map_size(state.outbound) >= state.max_operations -> reject(state, ref, :operations_full)
      not host_shape?(kind, packet) -> reject(state, ref, :invalid_packet)
      true -> admit_operation(state, kind, ref, packet, now)
    end
  end

  defp admit_operation(state, :publish, ref, %Publish{} = packet, now) do
    cond do
      packet.qos not in [0, 1, 2] or packet.qos > state.peer.maximum_qos ->
        reject(state, ref, :qos_not_supported)

      packet.dup != false ->
        reject(state, ref, :invalid_publish_flags)

      Keyword.get(packet.properties, :payload_format_indicator, 0) == 1 and
          (not is_binary(packet.payload) or not String.valid?(packet.payload)) ->
        reject(state, ref, :payload_format_invalid)

      packet.retain == true and state.peer.retain_available == 0 ->
        reject(state, ref, :retain_not_available)

      Keyword.has_key?(packet.properties, :subscription_identifier) ->
        reject(state, ref, :invalid_property_direction)

      Keyword.has_key?(packet.properties, :topic_alias) ->
        reject(state, ref, :topic_aliases_unsupported)

      packet.qos > 0 and
          (publication_count(state) >= state.max_inflight or state.send_quota == 0) ->
        reject(state, ref, :inflight_full)

      true ->
        allocate_operation(state, :publish, ref, packet, now)
    end
  end

  defp admit_operation(state, :subscribe, ref, %Subscribe{} = packet, now) do
    cond do
      state.peer.subscription_identifier_available == 0 and
          Keyword.has_key?(packet.properties, :subscription_identifier) ->
        reject(state, ref, :subscription_identifiers_not_available)

      not is_list(packet.subscriptions) ->
        reject(state, ref, :invalid_subscriptions)

      Enum.any?(packet.subscriptions, fn
        {filter, _options} when is_binary(filter) ->
          String.starts_with?(filter, "$share/") and state.peer.shared_subscription_available == 0

        _ ->
          false
      end) ->
        reject(state, ref, :shared_subscriptions_not_available)

      Enum.any?(packet.subscriptions, fn
        {filter, _options} when is_binary(filter) ->
          :binary.match(filter, ["+", "#"]) != :nomatch and
              state.peer.wildcard_subscription_available == 0

        _ ->
          false
      end) ->
        reject(state, ref, :wildcards_not_available)

      Enum.any?(packet.subscriptions, fn
        {_filter, options} -> requested_qos(options) not in [0, 1, 2]
        _ -> false
      end) ->
        reject(state, ref, :qos_not_supported)

      not subscription_capacity?(state, packet.subscriptions) ->
        reject(state, ref, :subscriptions_full)

      true ->
        allocate_operation(state, :subscribe, ref, packet, now)
    end
  end

  defp admit_operation(state, :unsubscribe, ref, %Unsubscribe{} = packet, now),
    do: allocate_operation(state, :unsubscribe, ref, packet, now)

  defp admit_operation(state, _kind, ref, _packet, _now), do: reject(state, ref, :invalid_packet)

  defp allocate_operation(state, kind, ref, packet, now) do
    needs_id? = kind != :publish or packet.qos != 0
    id = if needs_id?, do: next_id(state, state.next_packet_id, 0), else: nil
    packet = %{packet | packet_id: id}

    case Packet.encode(packet, state.limits) do
      {:ok, bytes} when byte_size(bytes) > state.peer.maximum_packet_size ->
        reject(state, ref, :peer_packet_size)

      {:ok, _bytes} ->
        next_packet_id = if needs_id?, do: rem(id, 65_535) + 1, else: state.next_packet_id

        entry = %{
          ref: ref,
          packet: packet,
          kind: kind,
          sent: false,
          order: state.next_order,
          expiry_deadline: expiry_deadline(packet, now)
        }

        entry =
          if match?(%Publish{qos: 2}, packet),
            do: Map.put(entry, :qos2_phase, :await_pubrec),
            else: entry

        quota =
          if match?(%Publish{qos: qos} when qos > 0, packet),
            do: state.send_quota - 1,
            else: state.send_quota

        {%{
           state
           | outbound: Map.put(state.outbound, ref, entry),
             next_packet_id: next_packet_id,
             next_order: state.next_order + 1,
             send_quota: quota
         }, [{:send, packet, publication_token(ref, entry)}]}

      {:error, error} ->
        reject(state, ref, {:invalid_packet, error.reason})
    end
  end

  defp next_id(_state, _id, 65_535), do: nil

  defp next_id(state, id, tried) do
    if Enum.any?(state.outbound, fn {_ref, entry} -> entry.packet.packet_id == id end),
      do: next_id(state, rem(id, 65_535) + 1, tried + 1),
      else: id
  end

  defp publication_count(state),
    do:
      Enum.count(state.outbound, fn
        {_ref, %{packet: %Publish{qos: qos}}} when qos > 0 -> true
        _ -> false
      end)

  defp subscription_capacity?(state, subscriptions) do
    pending =
      Enum.flat_map(state.outbound, fn
        {_ref, %{packet: %Subscribe{subscriptions: subscriptions}}} -> subscriptions
        _ -> []
      end)

    filters = MapSet.new(Map.keys(state.subscriptions))

    case Enum.reduce_while(pending ++ subscriptions, filters, fn
           {filter, _options}, filters ->
             filters = MapSet.put(filters, filter)

             if MapSet.size(filters) > state.max_subscriptions,
               do: {:halt, :full},
               else: {:cont, filters}

           _invalid, filters ->
             {:cont, filters}
         end) do
      :full -> false
      _filters -> true
    end
  end

  defp host_shape?(:publish, %Publish{properties: properties}), do: property_shape?(properties)

  defp host_shape?(:subscribe, %Subscribe{properties: properties}),
    do: property_shape?(properties)

  defp host_shape?(:unsubscribe, %Unsubscribe{properties: properties}),
    do: property_shape?(properties)

  defp host_shape?(_kind, _packet), do: false
  defp property_shape?(properties), do: is_list(properties) and Keyword.keyword?(properties)

  defp requested_qos(qos) when is_integer(qos), do: qos
  defp requested_qos(options) when is_map(options), do: Map.get(options, :qos, 0)
  defp requested_qos(_options), do: nil

  defp incoming_publication(state, %Publish{qos: qos} = packet, now) when qos > 0 do
    # A peer may already have received PUBCOMP before its local write receipt.
    # Retire only that incarnation; old receipts cannot erase the next delivery.
    state = retire_completed_identifier(state, packet.packet_id)

    case Map.fetch(state.incoming, packet.packet_id) do
      {:ok, id} ->
        entry = state.deliveries[id]

        cond do
          entry.packet.qos != qos ->
            terminal(state, :packet_identifier_in_use, 0x82)

          qos == 1 and not (packet.dup and same_publication?(entry.packet, packet)) ->
            terminal(state, :packet_identifier_in_use, 0x82)

          true ->
            case reserve_received(state, id) do
              {:ok, state} ->
                if qos == 2 and entry.qos2_phase == :await_pubrel,
                  do:
                    qos2_response(
                      state,
                      %Pubrec{packet_id: packet.packet_id},
                      {:qos2_pubrec, packet.packet_id, id}
                    ),
                  else: {state, []}

              :full ->
                terminal(state, :receive_maximum_exceeded, 0x93)
            end
        end

      :error ->
        store_publication(state, packet, now)
    end
  end

  defp incoming_publication(state, packet, now), do: store_publication(state, packet, now)

  defp retire_completed_identifier(state, packet_id) do
    case Map.fetch(state.incoming, packet_id) do
      {:ok, id} ->
        if state.deliveries[id].qos2_phase == :completing do
          %{
            state
            | incoming: Map.delete(state.incoming, packet_id),
              deliveries: Map.delete(state.deliveries, id),
              received_inflight: MapSet.delete(state.received_inflight, id)
          }
        else
          state
        end

      :error ->
        state
    end
  end

  defp reserve_received(state, id) do
    if MapSet.member?(state.received_inflight, id) or
         MapSet.size(state.received_inflight) < state.receive_maximum,
       do: {:ok, %{state | received_inflight: MapSet.put(state.received_inflight, id)}},
       else: :full
  end

  defp store_publication(state, packet, now) do
    cond do
      packet.qos > 0 and MapSet.size(state.received_inflight) >= state.receive_maximum ->
        terminal(state, :receive_maximum_exceeded, 0x93)

      map_size(state.deliveries) >= state.max_deliveries ->
        terminal(state, :delivery_quota_exceeded, 0x97)

      true ->
        id = state.next_delivery_id

        entry = %{
          packet: packet,
          delivered: false,
          ack_pending: false,
          ack_sent: false,
          received_at: now,
          qos2_phase: if(packet.qos == 2, do: :await_host, else: nil)
        }

        incoming =
          if packet.qos > 0,
            do: Map.put(state.incoming, packet.packet_id, id),
            else: state.incoming

        state = %{
          state
          | incoming: incoming,
            deliveries: Map.put(state.deliveries, id, entry),
            next_delivery_id: id + 1,
            received_inflight:
              if(packet.qos > 0,
                do: MapSet.put(state.received_inflight, id),
                else: state.received_inflight
              )
        }

        flush_deliveries(state, now)
    end
  end

  defp flush_deliveries(state, now) do
    {state, effects} =
      state.deliveries
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.reduce({state, []}, fn {id, entry}, {state, effects} ->
        cond do
          entry.delivered or entry.ack_pending ->
            {state, effects}

          expired?(entry, now) ->
            if entry.packet.qos > 0 do
              entry = %{entry | ack_pending: true}
              {%{state | deliveries: Map.put(state.deliveries, id, entry)}, effects}
            else
              {%{state | deliveries: Map.delete(state.deliveries, id)}, effects}
            end

          state.demand > 0 ->
            state = %{
              state
              | deliveries: Map.put(state.deliveries, id, %{entry | delivered: true}),
                demand: state.demand - 1
            }

            {state, effects ++ [{:notify, {:delivery, id, entry.packet}}]}

          true ->
            {state, effects}
        end
      end)

    {state, acknowledgements} = flush_acknowledgements(state)
    {state, effects ++ acknowledgements}
  end

  # MQTT-4.6.0-2/3: host ownership may arrive out of order, PUBACK/PUBREC may not.
  defp flush_acknowledgements(state) do
    Enum.reduce([1, 2], {state, []}, fn qos, {state, effects} ->
      {state, more} = flush_acknowledgements(state, qos)
      {state, effects ++ more}
    end)
  end

  defp flush_acknowledgements(state, qos) do
    state.deliveries
    |> Enum.filter(fn {_id, entry} -> entry.packet.qos == qos end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce_while({state, []}, fn {id, entry}, {state, effects} ->
      cond do
        state.phase != :connected ->
          {:halt, {state, effects}}

        entry.qos2_phase == :completing ->
          {:cont, {state, effects}}

        not entry.ack_pending ->
          {:halt, {state, effects}}

        entry.ack_sent ->
          {:cont, {state, effects}}

        true ->
          {packet, token} =
            if qos == 1,
              do: {%Puback{packet_id: entry.packet.packet_id}, {:ack, entry.packet.packet_id}},
              else:
                {%Pubrec{packet_id: entry.packet.packet_id},
                 {:qos2_pubrec, entry.packet.packet_id, id}}

          if peer_fits?(state, packet) do
            entry = %{
              entry
              | ack_sent: true,
                qos2_phase: if(qos == 2, do: :await_pubrel, else: nil)
            }

            state = %{state | deliveries: Map.put(state.deliveries, id, entry)}
            {:cont, {state, effects ++ [{:send, packet, token}]}}
          else
            {state, failure} = terminal(state, :peer_packet_size, nil)
            {:halt, {state, effects ++ failure}}
          end
      end
    end)
  end

  defp expiry_deadline(%Publish{} = packet, now) do
    case Keyword.get(packet.properties, :message_expiry_interval) do
      nil -> nil
      seconds -> now + seconds * 1_000
    end
  end

  defp expiry_deadline(_packet, _now), do: nil

  defp remaining_expiry(%{expiry_deadline: nil, packet: packet}, _now), do: packet

  defp remaining_expiry(%{expiry_deadline: deadline, packet: packet}, now) do
    remaining = max(0, div(deadline - now, 1_000))

    properties =
      Enum.map(packet.properties, fn
        {:message_expiry_interval, _seconds} -> {:message_expiry_interval, remaining}
        property -> property
      end)

    %{packet | properties: properties}
  end

  defp expired?(entry, now) do
    case Keyword.get(entry.packet.properties, :message_expiry_interval) do
      nil -> false
      seconds -> now - entry.received_at >= seconds * 1_000
    end
  end

  defp same_publication?(left, right) do
    Map.take(left, [:topic, :payload, :qos, :retain]) ==
      Map.take(right, [:topic, :payload, :qos, :retain]) and
      Keyword.delete(left.properties, :message_expiry_interval) ==
        Keyword.delete(right.properties, :message_expiry_interval)
  end

  defp sent(state, :disconnect, _result, _now), do: {state, [{:close, :normal}]}

  defp sent(%{phase: phase} = state, _token, _result, _now)
       when phase in [:stopped, :offline, :idle],
       do: {state, []}

  defp sent(state, _token, {:error, reason}, now),
    do: transport_loss(state, {:send_failed, reason}, now, true)

  defp sent(state, {:operation, ref}, :ok, now) do
    case Map.fetch(state.outbound, ref) do
      {:ok, %{packet: %Publish{qos: 0}}} ->
        complete(%{state | last_sent: now}, ref, {:ok, :sent}, now)

      {:ok, entry} ->
        {%{state | last_sent: now, outbound: Map.put(state.outbound, ref, %{entry | sent: true})},
         []}

      :error ->
        {state, []}
    end
  end

  defp sent(state, {:qos2_publish, ref, order}, :ok, now) do
    case Map.get(state.outbound, ref) do
      %{order: ^order} = entry ->
        {%{state | last_sent: now, outbound: Map.put(state.outbound, ref, %{entry | sent: true})},
         []}

      _ ->
        {state, []}
    end
  end

  defp sent(state, {:qos2_pubrel, ref, order}, :ok, now) do
    case Map.get(state.outbound, ref) do
      %{release_order: ^order, qos2_phase: :await_pubcomp} -> {%{state | last_sent: now}, []}
      _ -> {state, []}
    end
  end

  defp sent(state, {:qos2_pubrec, packet_id, id}, :ok, now) do
    if Map.get(state.incoming, packet_id) == id,
      do: {%{state | last_sent: now}, []},
      else: {state, []}
  end

  defp sent(state, {:qos2_pubcomp, packet_id, id}, :ok, now) do
    if Map.get(state.incoming, packet_id) == id and state.deliveries[id].qos2_phase == :completing do
      {%{
         state
         | last_sent: now,
           incoming: Map.delete(state.incoming, packet_id),
           deliveries: Map.delete(state.deliveries, id),
           received_inflight: MapSet.delete(state.received_inflight, id)
       }, []}
    else
      {state, []}
    end
  end

  defp sent(state, :qos2_unknown_pubcomp, :ok, now), do: {%{state | last_sent: now}, []}

  defp sent(state, {:ack, packet_id}, :ok, now) do
    case Map.fetch(state.incoming, packet_id) do
      {:ok, id} ->
        {%{
           state
           | last_sent: now,
             incoming: Map.delete(state.incoming, packet_id),
             deliveries: Map.delete(state.deliveries, id),
             received_inflight: MapSet.delete(state.received_inflight, id)
         }, []}

      :error ->
        {state, []}
    end
  end

  defp sent(state, token, :ok, now) when token in [:connect, :ping],
    do: {%{state | last_sent: now}, []}

  defp sent(state, _token, _result, _now), do: {state, []}

  defp keep_alive(state, now) do
    interval = state.peer.server_keep_alive * 1_000

    cond do
      interval == 0 ->
        {state, []}

      state.ping_at != nil and now - state.ping_at >= interval ->
        transport_loss(state, :keep_alive_timeout, now, true)

      state.ping_at != nil ->
        {state, []}

      now - state.last_sent >= interval ->
        if peer_fits?(state, %Pingreq{}),
          do: {%{state | ping_at: now}, [{:send, %Pingreq{}, :ping}]},
          else: terminal(state, :peer_packet_size, nil)

      true ->
        {state, []}
    end
  end

  defp transport_loss(state, reason, now, close?) do
    close_effects = if close?, do: [{:close, reason}], else: []
    can_retry? = state.reconnect_attempts < state.reconnect_max
    preserve? = state.session_available and state.session_expiry_interval > 0
    {state, failures} = if preserve?, do: {state, []}, else: reset_session(state)
    state = %{state | ping_at: nil, disconnected_at: state.disconnected_at || now}

    if can_retry? do
      attempt = state.reconnect_attempts + 1

      {%{state | phase: :offline, reconnect_attempts: attempt},
       failures ++ close_effects ++ [{:notify, {:disconnected, reason}}, {:retry, attempt}]}
    else
      {state, terminal_failures} = finish_host_operations(state)

      {%{state | phase: :stopped},
       failures ++ terminal_failures ++ close_effects ++ [{:notify, {:disconnected, reason}}]}
    end
  end

  defp terminal(state, reason, code) do
    disconnect =
      if code != nil and state.phase == :connected and
           peer_fits?(state, %Disconnect{reason_code: code}),
         do: [{:send, %Disconnect{reason_code: code}, :disconnect}],
         else: []

    {state, failures} = finish_host_operations(state)

    {%{state | phase: :stopped, ping_at: nil},
     failures ++
       disconnect ++ [{:close, reason}, {:notify, {:disconnected, reason}}]}
  end

  # A terminal host result does not settle the MQTT session's outstanding
  # PUBLISH/PUBREL. Keep those bounded obligations until acknowledgement,
  # session loss or expiry; recovery must not create another host success.
  defp finish_host_operations(state) do
    notifications = failed_operations(state, :unknown_outcome)

    outbound =
      if state.session_available and state.session_expiry_interval > 0 do
        state.outbound
        |> Enum.flat_map(fn
          {ref, %{packet: %Publish{qos: qos}} = entry} when qos in [1, 2] ->
            [{ref, Map.put(entry, :host_completed, true)}]

          _ ->
            []
        end)
        |> Map.new()
      else
        %{}
      end

    {%{state | outbound: outbound}, notifications}
  end

  defp local_session?(%{session_available: false}, _now), do: false
  defp local_session?(%{session_expiry_interval: 0}, _now), do: false
  defp local_session?(%{disconnected_at: nil}, _now), do: true

  defp local_session?(state, now),
    do:
      state.session_expiry_interval == 0xFFFF_FFFF or
        now - state.disconnected_at < state.session_expiry_interval * 1_000

  defp find_operation(state, packet_id, kind),
    do:
      Enum.find(state.outbound, fn
        {_ref, %{kind: ^kind, packet: %{packet_id: ^packet_id}}} -> true
        _ -> false
      end)

  defp complete_publication(state, ref, result, now) do
    state = %{state | send_quota: min(state.initial_send_quota, state.send_quota + 1)}
    complete(state, ref, result, now)
  end

  defp complete(state, ref, result, now) do
    event =
      if Map.get(state.outbound[ref], :host_completed, false),
        do: {:protocol_complete, ref, result},
        else: {:operation, ref, result}

    {state, sends} = send_resumptions(%{state | outbound: Map.delete(state.outbound, ref)}, now)
    {state, [{:notify, event}] ++ sends}
  end

  defp reject(state, ref, reason), do: {state, [{:notify, {:operation, ref, {:error, reason}}}]}
  defp ack_error(state, id, reason), do: {state, [{:notify, {:ack, id, {:error, reason}}}]}

  defp failed_operations(state, reason),
    do:
      state.outbound
      |> Enum.reject(fn {_ref, entry} -> Map.get(entry, :host_completed, false) end)
      |> Enum.sort_by(fn {_ref, entry} -> entry.order end)
      |> Enum.map(fn {ref, _entry} -> {:notify, {:operation, ref, {:error, reason}}} end)

  defp peer_fits?(state, packet) do
    case Packet.encode(packet, state.limits) do
      {:ok, bytes} -> byte_size(bytes) <= state.peer.maximum_packet_size
      {:error, _error} -> false
    end
  end

  defp valid_packet?(packet, state),
    do: match?({:ok, _bytes}, Packet.encode(packet, state.limits))

  defp current?(state, generation), do: state.generation == generation
end
