defmodule GravitonMQ.MQTT.Broker.Checkpoint do
  @moduledoc """
  Pure, versioned persistence projection for one MQTT 5.0 broker.

  Only protocol session responsibility is captured. Connections, authentication
  requests and credentials, write receipts and host process ownership are
  transient. The running owner must synchronize a checkpoint before interpreting
  the corresponding effects. This module performs no I/O and reads no clock.

  Existing offline deadlines count explicit wall-clock downtime. A formerly
  online connection is detected lost at recovery, when its offline expiry and
  Will delay start. Backward wall-clock movement rejects recovery. QoS0 live
  deliveries are transient; retained messages and QoS1/2 responsibility survive.
  """

  alias GravitonMQ.Core.CheckpointCodec
  alias GravitonMQ.MQTT.{Broker, Topic}
  alias GravitonMQ.MQTT.Codec.{Packet, Wire}
  alias GravitonMQ.MQTT.Packet.Publish

  @session ~w(principal token expiry expires_at will_id subscriptions incoming_qos2 deliveries next_packet_id online)a
  @delivery ~w(id message qos phase release_order retain packet_id sending ever_sent delivered dispatch retry_at)a
  @message ~w(topic payload qos retain properties encoded origin expires_at)a
  @will ~w(id principal client_id token packet encoded delay due status registered_at message bytes)a
  @uint64 18_446_744_073_709_551_615

  def capture(%Broker{} = machine, monotonic, wall)
      when is_integer(monotonic) and is_integer(wall) do
    sessions =
      Map.new(machine.sessions, fn {id, session} ->
        deliveries =
          session.deliveries
          |> Enum.filter(&(&1.qos > 0))
          |> Enum.map(&%{&1 | sending: false, dispatch: nil, retry_at: nil})

        saved =
          session
          |> Map.take(@session -- [:online])
          |> Map.put(:online, session.connection != nil)
          |> Map.put(:deliveries, deliveries)

        {id, saved}
      end)

    wills =
      Map.new(machine.wills, fn {id, will} ->
        {id, %{will | status: if(will.status == :prepared, do: :due, else: will.status)}}
      end)

    snapshot = %{
      version: 1,
      at_monotonic: monotonic,
      at_wall: wall,
      state: %{
        sessions: sessions,
        retained: machine.retained,
        wills: wills,
        next_id: machine.next_id
      }
    }

    case validate(snapshot, machine) do
      :ok -> {:ok, snapshot}
      error -> error
    end
  end

  def capture(_, _, _), do: invalid()

  def restore(%Broker{} = template, snapshot, monotonic, wall)
      when is_integer(monotonic) and is_integer(wall) do
    with :ok <- validate(snapshot, template),
         true <- wall >= snapshot.at_wall do
      shift = monotonic - snapshot.at_monotonic - (wall - snapshot.at_wall)

      sessions =
        Map.new(snapshot.state.sessions, fn {id, saved} ->
          session =
            saved
            |> Map.delete(:online)
            |> Map.put(:connection, nil)
            |> Map.update!(:expires_at, &deadline(&1, shift))
            |> Map.update!(:deliveries, fn entries ->
              Enum.map(entries, &%{&1 | message: rebase_message(&1.message, shift)})
            end)

          {id, session}
        end)

      wills =
        Map.new(snapshot.state.wills, fn {id, will} ->
          {id,
           %{
             will
             | due: deadline(will.due, shift),
               registered_at: will.registered_at + shift,
               message: if(will.message, do: rebase_message(will.message, shift), else: nil)
           }}
        end)

      machine = %{
        template
        | sessions: sessions,
          retained:
            Map.new(snapshot.state.retained, fn {topic, message} ->
              {topic, rebase_message(message, shift)}
            end),
          wills: wills,
          next_id: snapshot.state.next_id,
          connections: %{},
          operations: %{},
          writes: %{},
          hosts: %{}
      }

      # Reuse the role's existing loss transition. Synthetic identities are
      # local pure data; neither the old socket identity nor a PID is restored.
      machine =
        Enum.reduce(snapshot.state.sessions, machine, fn {id, saved}, machine ->
          if saved.online do
            connection = {:recovered_connection, saved.token}
            session = %{machine.sessions[id] | connection: {connection, 0}}

            machine = %{
              machine
              | sessions: Map.put(machine.sessions, id, session),
                connections:
                  Map.put(machine.connections, connection, %{client_id: id, generation: 0})
            }

            {machine, []} =
              Broker.transition(
                machine,
                {:connection_down, connection, 0, :recovery_detected, monotonic}
              )

            machine
          else
            machine
          end
        end)

      {:ok, machine}
    else
      false -> {:error, :wall_clock_regressed}
      error -> error
    end
  end

  def restore(_, _, _, _), do: invalid()

  defp validate(snapshot, template) do
    with true <- keys?(snapshot, [:version, :at_monotonic, :at_wall, :state]),
         true <-
           snapshot.version == 1 and is_integer(snapshot.at_monotonic) and
             is_integer(snapshot.at_wall),
         data = snapshot.state,
         true <- keys?(data, [:sessions, :retained, :wills, :next_id]),
         true <- positive?(data.next_id),
         true <- is_map(data.sessions) and is_map(data.retained) and is_map(data.wills),
         true <-
           Enum.all?(data.sessions, fn {id, session} -> session?(id, session, data, template) end),
         true <-
           Enum.all?(data.retained, fn {topic, message} ->
             message?(message, template) and topic == message.topic and message.retain
           end),
         true <- Enum.all?(data.wills, fn {id, will} -> will?(id, will, data, template) end),
         {:ok, _} <- CheckpointCodec.encode(snapshot, max_bytes: 67_108_800, max_items: 500_000),
         :ok <- limits(data, template.limits) do
      :ok
    else
      {:error, :checkpoint_limits} = error -> error
      _ -> invalid()
    end
  end

  defp session?(id, session, data, template) do
    is_binary(id) and byte_size(id) in 1..65_535 and Wire.valid_utf8?(id) and
      keys?(session, @session) and positive?(session.token) and session.token < data.next_id and
      is_boolean(session.online) and uint32?(session.expiry) and timestamp?(session.expires_at) and
      (not session.online or session.expires_at == nil) and
      (session.will_id == nil or Map.has_key?(data.wills, session.will_id)) and
      is_map(session.subscriptions) and
      Enum.all?(session.subscriptions, fn {filter, sub} ->
        subscription?(filter, sub, data.next_id)
      end) and
      is_map(session.incoming_qos2) and
      Enum.all?(session.incoming_qos2, fn {id, marker} -> packet_id?(id) and marker == true end) and
      packet_id?(session.next_packet_id) and is_list(session.deliveries) and
      Enum.all?(session.deliveries, &delivery?(&1, data.next_id, template)) and
      unique?(Enum.map(session.deliveries, & &1.id)) and
      unique?(for entry <- session.deliveries, entry.packet_id != nil, do: entry.packet_id) and
      unique?(
        for entry <- session.deliveries, entry.release_order != nil, do: entry.release_order
      )
  end

  defp subscription?(filter, sub, next) do
    Topic.valid_filter?(filter) and keys?(sub, [:id, :options]) and positive?(sub.id) and
      sub.id < next and
      keys?(sub.options, [:qos, :no_local, :retain_as_published, :retain_handling]) and
      sub.options.qos in 0..2 and is_boolean(sub.options.no_local) and
      is_boolean(sub.options.retain_as_published) and sub.options.retain_handling in 0..2
  end

  defp delivery?(entry, next, template) do
    keys?(entry, @delivery) and positive?(entry.id) and entry.id < next and
      message?(entry.message, template) and entry.qos in [1, 2] and is_boolean(entry.retain) and
      entry.phase in [:publish, :pubrel] and
      ((entry.phase == :publish and entry.release_order == nil) or
         (entry.phase == :pubrel and entry.qos == 2 and packet_id?(entry.packet_id) and
            positive?(entry.release_order) and entry.release_order < next)) and
      (entry.packet_id == nil or packet_id?(entry.packet_id)) and
      is_boolean(entry.ever_sent) and (entry.packet_id == nil or entry.ever_sent) and
      entry.sending == false and entry.delivered == false and entry.dispatch == nil and
      entry.retry_at == nil
  end

  defp message?(message, template) do
    keys?(message, @message) and timestamp?(message.expires_at) and
      is_binary(message.encoded) and byte_size(message.encoded) <= template.limits.max_packet_size and
      origin?(message.origin) and
      match?(
        {:ok, _},
        Packet.encode(
          %Publish{
            topic: message.topic,
            payload: message.payload,
            qos: message.qos,
            packet_id: if(message.qos == 0, do: nil, else: 1),
            retain: message.retain,
            properties: message.properties
          },
          template.codec
        )
      )
  end

  defp will?(id, will, data, template) do
    positive?(id) and id < data.next_id and keys?(will, @will) and will.id == id and
      is_binary(will.client_id) and will.client_id != "" and positive?(will.token) and
      will.token < data.next_id and
      is_struct(will.packet, Publish) and
      match?({:ok, _}, Packet.encode(will.packet, template.codec)) and
      is_binary(will.encoded) and byte_size(will.encoded) <= template.limits.max_packet_size and
      uint32?(will.delay) and timestamp?(will.due) and is_integer(will.registered_at) and
      will.status in [:armed, :due] and
      ((will.status == :armed and will.due == nil) or
         (will.status == :due and is_integer(will.due))) and
      (will.message == nil or message?(will.message, template)) and positive?(will.bytes)
  end

  defp limits(data, limits) do
    entries = Enum.flat_map(data.sessions, fn {_, session} -> session.deliveries end)

    subscriptions =
      Enum.flat_map(data.sessions, fn {_, session} -> Map.to_list(session.subscriptions) end)

    bytes = fn messages ->
      Enum.sum(
        Enum.map(messages, &(byte_size(&1.encoded) + byte_size(&1.payload) + byte_size(&1.topic)))
      )
    end

    if map_size(data.sessions) <= limits.max_sessions and
         Enum.sum(
           Enum.map(data.sessions, fn {id, session} ->
             :erlang.external_size({id, session.principal})
           end)
         ) <= limits.max_session_bytes and
         length(subscriptions) <= limits.max_subscriptions and
         Enum.sum(Enum.map(subscriptions, fn {filter, _} -> byte_size(filter) end)) <=
           limits.max_subscription_bytes and
         length(entries) <= limits.max_pending_deliveries and
         bytes.(Enum.map(entries, & &1.message)) <= limits.max_pending_bytes and
         Enum.sum(
           Enum.map(data.sessions, fn {_, session} ->
             :erlang.external_size(session.incoming_qos2)
           end)
         ) <= limits.max_incoming_bytes and
         map_size(data.retained) <= limits.max_retained and
         bytes.(Map.values(data.retained)) <= limits.max_retained_bytes and
         map_size(data.wills) <= limits.max_wills and
         Enum.sum(Enum.map(data.wills, fn {_, will} -> will.bytes end)) <= limits.max_will_bytes,
       do: :ok,
       else: {:error, :checkpoint_limits}
  end

  defp rebase_message(message, shift),
    do: %{message | expires_at: deadline(message.expires_at, shift)}

  defp deadline(nil, _), do: nil
  defp deadline(value, shift), do: value + shift
  defp origin?({:network, id}), do: is_binary(id)
  defp origin?({:host, _}), do: true
  defp origin?(_), do: false
  defp timestamp?(nil), do: true
  defp timestamp?(value), do: is_integer(value)
  defp positive?(value), do: is_integer(value) and value in 1..@uint64
  defp uint32?(value), do: is_integer(value) and value in 0..0xFFFF_FFFF
  defp packet_id?(value), do: is_integer(value) and value in 1..65_535
  defp unique?(values), do: length(values) == MapSet.size(MapSet.new(values))

  defp keys?(value, keys),
    do:
      is_map(value) and not is_struct(value) and map_size(value) == length(keys) and
        Enum.all?(keys, &Map.has_key?(value, &1))

  defp invalid, do: {:error, :invalid_broker_checkpoint}
end
