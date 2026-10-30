defmodule GravitonMQ.Client.MQTT.Checkpoint do
  @moduledoc false
  alias GravitonMQ.MQTT.Client, as: Machine
  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Publish, Subscribe, Unsubscribe}
  alias GravitonMQ.MQTT.Topic

  @fields [
    :outbound,
    :incoming,
    :deliveries,
    :subscriptions,
    :next_packet_id,
    :next_delivery_id,
    :next_order,
    :session_expiry_interval,
    :session_available,
    :ever_connected,
    :disconnected_at
  ]
  @snapshot_fields [:version, :client_id, :mono, :wall, :machine, :meta]

  def capture(%Machine{} = machine, meta, mono, wall) do
    # CONNECT (including credentials and Will) and all transport/host ownership
    # are deliberately absent. Fresh configuration supplies those on restart.
    retained = Map.take(machine, @fields)

    snapshot = %{
      version: 1,
      client_id: machine.connect.client_id,
      mono: mono,
      wall: wall,
      machine: retained,
      meta: meta
    }

    case validate(snapshot, machine) do
      :ok -> {:ok, snapshot}
      error -> error
    end
  end

  def restore(snapshot, %Machine{} = fresh, mono, wall) do
    with :ok <- validate(snapshot, fresh),
         true <- wall >= snapshot.wall do
      downtime = wall - snapshot.wall
      offset = mono - snapshot.mono - downtime
      retained = snapshot.machine

      outbound =
        Map.new(retained.outbound, fn {id, entry} ->
          {id, Map.update!(entry, :expiry_deadline, &shift(&1, offset))}
        end)

      deliveries =
        Map.new(retained.deliveries, fn {id, entry} ->
          {id, Map.update!(entry, :received_at, &shift(&1, offset))}
        end)

      retained = %{
        retained
        | outbound: outbound,
          deliveries: deliveries,
          # An online snapshot does not reveal when its later connection died.
          # Keep a finite recovery window from now and let Session Present
          # decide whether the peer still owns the session. Known disconnects
          # retain their exact offline age, independently of other deadlines.
          disconnected_at:
            if(is_nil(retained.disconnected_at),
              do: mono,
              else: shift(retained.disconnected_at, offset)
            )
      }

      machine = struct!(fresh, retained)
      deadlines = Map.new(snapshot.meta.deadlines, fn {id, time} -> {id, time + offset} end)
      {:ok, machine, %{snapshot.meta | deadlines: deadlines}}
    else
      false -> {:error, :wall_clock_regressed}
      error -> error
    end
  end

  defp shift(nil, _offset), do: nil
  defp shift(time, offset), do: time + offset

  defp validate(snapshot, fresh) do
    cond do
      not exact?(snapshot, @snapshot_fields) ->
        {:error, :invalid_checkpoint_schema}

      snapshot.client_id != fresh.connect.client_id ->
        {:error, :client_identity_mismatch}

      snapshot.version != 1 or not is_integer(snapshot.mono) or not is_integer(snapshot.wall) ->
        {:error, :invalid_checkpoint_schema}

      not retained?(snapshot.machine, snapshot.meta, fresh) ->
        {:error, :invalid_checkpoint_schema}

      true ->
        :ok
    end
  end

  defp retained?(state, meta, fresh) do
    exact?(state, @fields) and exact?(meta, [:next_operation, :deadlines, :results]) and
      positive?(state.next_delivery_id) and positive?(state.next_order) and
      is_integer(state.next_packet_id) and state.next_packet_id in 1..65_535 and
      is_integer(state.session_expiry_interval) and
      state.session_expiry_interval in 0..4_294_967_295 and
      is_boolean(state.session_available) and is_boolean(state.ever_connected) and
      (is_nil(state.disconnected_at) or is_integer(state.disconnected_at)) and
      bounded_map?(state.outbound, fresh.max_operations) and
      bounded_map?(state.deliveries, fresh.max_deliveries) and
      bounded_map?(state.incoming, fresh.max_deliveries) and
      bounded_map?(state.subscriptions, fresh.max_subscriptions) and
      positive?(meta.next_operation) and bounded_map?(meta.deadlines, fresh.max_operations) and
      bounded_map?(meta.results, fresh.max_operations) and
      Enum.all?(state.outbound, fn {id, entry} ->
        positive?(id) and id < meta.next_operation and
          outbound?(id, entry, state.next_order, fresh)
      end) and
      unique_packet_ids?(state.outbound) and
      Enum.all?(state.deliveries, fn {id, entry} ->
        positive?(id) and id < state.next_delivery_id and delivery?(entry, fresh)
      end) and
      Enum.all?(state.incoming, fn {packet_id, id} ->
        is_integer(packet_id) and packet_id in 1..65_535 and
          match?(
            %{packet: %Publish{qos: qos, packet_id: ^packet_id}} when qos > 0,
            Map.get(state.deliveries, id)
          )
      end) and
      Enum.all?(state.deliveries, fn {id, entry} ->
        entry.packet.qos == 0 or Map.get(state.incoming, entry.packet.packet_id) == id
      end) and
      Enum.all?(state.subscriptions, fn {filter, qos} ->
        Topic.valid_filter?(filter) and qos in [0, 1, 2]
      end) and
      Enum.sort(Map.keys(meta.deadlines)) ==
        Enum.sort(
          for {id, entry} <- state.outbound, not Map.get(entry, :host_completed, false), do: id
        ) and
      Enum.all?(meta.deadlines, fn {id, time} ->
        Map.has_key?(state.outbound, id) and is_integer(time)
      end) and
      Enum.all?(meta.results, fn {id, entry} ->
        positive?(id) and id < meta.next_operation and
          exact?(entry, [:kind, :result]) and entry.kind in [:publish, :subscribe, :unsubscribe] and
          result?(entry.result)
      end)
  end

  defp outbound?(id, entry, next_order, fresh) do
    required = [:ref, :packet, :kind, :sent, :order, :expiry_deadline]

    is_map(entry) and Enum.all?(required, &Map.has_key?(entry, &1)) and
      Enum.all?(
        Map.keys(entry),
        &(&1 in (required ++ [:qos2_phase, :release_order, :resend, :host_completed]))
      ) and
      entry.ref == id and is_boolean(entry.sent) and positive?(entry.order) and
      entry.order < next_order and
      (is_nil(entry.expiry_deadline) or is_integer(entry.expiry_deadline)) and
      (not Map.has_key?(entry, :resend) or is_boolean(entry.resend)) and
      (not Map.has_key?(entry, :host_completed) or
         (is_boolean(entry.host_completed) and
            match?(%Publish{qos: qos} when qos in [1, 2], entry.packet))) and
      packet?(entry.packet, entry.kind, fresh) and qos_phase?(entry, next_order)
  end

  defp qos_phase?(%{packet: %Publish{qos: 2}, qos2_phase: :await_pubrec} = entry, _),
    do: not Map.has_key?(entry, :release_order)

  defp qos_phase?(
         %{packet: %Publish{qos: 2}, qos2_phase: :await_pubcomp, release_order: order},
         next
       ),
       do: positive?(order) and order < next

  defp qos_phase?(%{packet: %Publish{qos: 2}}, _), do: false

  defp qos_phase?(entry, _),
    do: not Map.has_key?(entry, :qos2_phase) and not Map.has_key?(entry, :release_order)

  defp delivery?(entry, fresh) do
    exact?(entry, [:packet, :delivered, :ack_pending, :ack_sent, :received_at, :qos2_phase]) and
      packet?(entry.packet, :publish, fresh) and
      Enum.all?([entry.delivered, entry.ack_pending, entry.ack_sent], &is_boolean/1) and
      is_integer(entry.received_at) and
      (not entry.ack_sent or entry.ack_pending) and
      delivery_phase?(entry)
  end

  defp delivery_phase?(%{packet: %Publish{qos: 2}, qos2_phase: :await_host} = entry),
    do: not entry.ack_sent

  defp delivery_phase?(%{packet: %Publish{qos: 2}, qos2_phase: phase} = entry)
       when phase in [:await_pubrel, :completing],
       do: entry.ack_pending and entry.ack_sent

  defp delivery_phase?(%{packet: %Publish{qos: 2}}), do: false
  defp delivery_phase?(entry), do: is_nil(entry.qos2_phase)

  defp packet?(%Publish{} = packet, :publish, fresh), do: valid_packet?(packet, fresh)
  defp packet?(%Subscribe{} = packet, :subscribe, fresh), do: valid_packet?(packet, fresh)
  defp packet?(%Unsubscribe{} = packet, :unsubscribe, fresh), do: valid_packet?(packet, fresh)
  defp packet?(_, _, _), do: false

  defp valid_packet?(packet, fresh) do
    exact?(packet, Map.keys(packet.__struct__.__struct__())) and
      match?({:ok, _}, Packet.encode(packet, fresh.limits))
  end

  defp unique_packet_ids?(outbound) do
    ids =
      Enum.flat_map(outbound, fn {_id, entry} ->
        if entry.packet.packet_id, do: [entry.packet.packet_id], else: []
      end)

    length(ids) == length(Enum.uniq(ids))
  end

  defp result?({:ok, value}) when value in [:sent, :puback, :pubcomp], do: true

  defp result?({:ok, codes}) when is_list(codes),
    do: Enum.all?(codes, &(is_integer(&1) and &1 in 0..255))

  defp result?({:error, reason}) when is_atom(reason), do: true

  defp result?({:error, {kind, code}}) when kind in [:puback, :pubrec, :pubcomp],
    do: is_integer(code) and code in 0..255

  defp result?(_), do: false

  defp exact?(map, keys),
    do:
      is_map(map) and map_size(map) == length(keys) and
        Enum.sort(Map.keys(map)) == Enum.sort(keys)

  defp bounded_map?(map, maximum), do: is_map(map) and map_size(map) <= maximum
  defp positive?(value), do: is_integer(value) and value > 0
end
