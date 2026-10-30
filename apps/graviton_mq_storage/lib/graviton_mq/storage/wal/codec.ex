defmodule GravitonMQ.Storage.WAL.Codec do
  @moduledoc false

  alias GravitonMQ.Core.NodeId
  alias GravitonMQ.Queue.{Event, EventId}

  @uint64 18_446_744_073_709_551_615
  @int64 -9_223_372_036_854_775_808
  @event_type "queue-event/v1"
  @max_identity 1024

  def header(stream, mode) do
    body = <<"GMWL", 1, mode_id(mode), byte_size(stream)::16, stream::binary>>
    <<body::binary, :erlang.crc32(body)::32>>
  end

  def header_size(<<"GMWL", 1, mode, size::16>>) when mode in [0, 1] and size in 1..1024,
    do: {:ok, 12 + size}

  def header_size(_), do: {:error, :wal_header}

  def decode_header(bytes, stream, mode) do
    with {:ok, body} <- checksum(bytes),
         <<"GMWL", 1, actual_mode, size::16, actual::binary-size(size)>> <- body do
      cond do
        actual != stream -> {:error, :stream_id}
        actual_mode != mode_id(mode) -> {:error, :stream_mode}
        true -> :ok
      end
    else
      _ -> {:error, :wal_header}
    end
  end

  def checkpoint(stream, mode, position, offset) do
    body =
      <<"GMCP", 1, mode_id(mode), byte_size(stream)::16, stream::binary, position::64,
        offset::64>>

    <<body::binary, :erlang.crc32(body)::32>>
  end

  def decode_checkpoint(bytes, stream, mode) do
    with {:ok, body} <- checksum(bytes),
         <<"GMCP", 1, actual_mode, size::16, actual::binary-size(size), pos::64, offset::64>> <-
           body,
         true <- actual == stream and actual_mode == mode_id(mode) do
      {:ok, pos, offset}
    else
      _ -> {:error, :checkpoint_corrupt}
    end
  end

  # First measure the entire logical value. Encoding does not begin until the
  # exact finite byte/depth budget succeeds; binaries are never copied to size them.
  def measure(%Event{} = event, :events, limit) do
    with true <- map_size(event) == 6,
         %EventId{value: id} when map_size(event.id) == 2 <- event.id,
         %NodeId{value: node} when map_size(event.node_id) == 2 <- event.node_id,
         true <- identity?(id) and identity?(node) and identity?(event.type),
         true <- is_integer(event.sequence) and event.sequence in 0..@uint64,
         true <- is_map(event.data) and not is_struct(event.data),
         fixed =
           24 + 2 + byte_size(@event_type) + 2 + byte_size(id) + 2 + byte_size(node) + 8 + 2 +
             byte_size(event.type),
         {:ok, remaining} <- take(event.data, limit - fixed, 0) do
      {:ok, limit - remaining}
    else
      false -> {:error, :invalid_event}
      {:error, :invalid_value} -> {:error, :invalid_event}
      {:error, _} = error -> error
      _ -> {:error, :invalid_event}
    end
  end

  def measure({type, body}, :opaque, limit) when is_binary(body) do
    if identity?(type) do
      size = 24 + 2 + byte_size(type) + byte_size(body)
      if size <= limit, do: {:ok, size}, else: {:error, :record_limit}
    else
      {:error, :invalid_record}
    end
  end

  def measure(_, :events, _), do: {:error, :invalid_event}
  def measure(_, :opaque, _), do: {:error, :invalid_record}

  def frame(item, mode, position) do
    payload = payload(item, mode) |> IO.iodata_to_binary()
    head = <<"GMR1", byte_size(payload)::32, position::64>>
    [head, <<:erlang.crc32(head)::32>>, payload, <<:erlang.crc32(payload)::32>>]
  end

  def frame_header(<<"GMR1", size::32, pos::64, crc::32>> = prefix, expected, limit) do
    head = binary_part(prefix, 0, 16)

    cond do
      :erlang.crc32(head) != crc -> {:error, :record_header_checksum}
      pos != expected -> {:error, :record_position}
      size + 24 > limit or size < 3 -> {:error, :record_length}
      true -> {:ok, size}
    end
  end

  def frame_header(_, _, _), do: {:error, :record_header}

  def decode_frame(payload_and_crc, mode) do
    with {:ok, payload} <- checksum(payload_and_crc),
         <<size::16, type::binary-size(size), body::binary>> <- payload,
         true <- identity?(type) do
      decode_payload(type, body, mode)
    else
      _ -> {:error, :record_checksum_or_payload}
    end
  end

  def incomplete_prefix?(bytes) do
    size = min(byte_size(bytes), 4)
    binary_part(bytes, 0, size) == binary_part("GMR1", 0, size)
  end

  defp decode_payload(type, body, :opaque), do: {:ok, {type, body}}

  defp decode_payload(@event_type, body, :events) do
    with <<id_size::16, id::binary-size(id_size), node_size::16, node::binary-size(node_size),
           seq::64, type_size::16, type::binary-size(type_size), data::binary>> <- body,
         true <- identity?(id) and identity?(node) and identity?(type),
         {:ok, decoded, <<>>} <- value(data, 0),
         true <- is_map(decoded) do
      {:ok,
       %Event{
         id: EventId.new(id),
         node_id: NodeId.new(node),
         sequence: seq,
         type: type,
         data: decoded
       }}
    else
      _ -> {:error, :event_payload}
    end
  end

  defp decode_payload(_, _, :events), do: {:error, :event_record_type}

  defp payload({type, body}, :opaque), do: [<<byte_size(type)::16>>, type, body]

  defp payload(event, :events) do
    id = event.id.value
    node = event.node_id.value
    type = event.type

    [
      <<byte_size(@event_type)::16>>,
      @event_type,
      <<byte_size(id)::16>>,
      id,
      <<byte_size(node)::16>>,
      node,
      <<event.sequence::64, byte_size(type)::16>>,
      type,
      encode(event.data)
    ]
  end

  defp take(_, left, _) when left < 0, do: {:error, :record_limit}
  defp take(_, _, depth) when depth > 32, do: {:error, :invalid_value}
  defp take(v, left, _) when is_nil(v) or is_boolean(v), do: charge(left, 1)
  defp take(v, left, _) when is_integer(v) and v >= @int64 and v <= @uint64, do: charge(left, 9)
  defp take(v, left, _) when is_float(v), do: charge(left, 9)
  defp take(v, left, _) when is_binary(v), do: charge(left, 5 + byte_size(v))
  defp take(v, left, depth) when is_list(v), do: take_list(v, left - 5, depth + 1)

  defp take(v, left, depth) when is_map(v) and not is_struct(v) do
    Enum.reduce_while(v, charge(left, 5), fn
      {key, item}, {:ok, budget} when is_binary(key) ->
        with {:ok, budget} <- take(key, budget, depth + 1),
             {:ok, budget} <- take(item, budget, depth + 1) do
          {:cont, {:ok, budget}}
        else
          error -> {:halt, error}
        end

      _, {:error, _} = error ->
        {:halt, error}

      _, _ ->
        {:halt, {:error, :invalid_value}}
    end)
  end

  defp take(_, _, _), do: {:error, :invalid_value}
  defp take_list([], left, _), do: charge(left, 0)

  defp take_list([head | tail], left, depth) do
    with {:ok, left} <- take(head, left, depth), do: take_list(tail, left, depth)
  end

  defp take_list(_, _, _), do: {:error, :invalid_value}
  defp charge(left, cost) when left >= cost, do: {:ok, left - cost}
  defp charge(_, _), do: {:error, :record_limit}

  defp encode(nil), do: <<0>>
  defp encode(false), do: <<1>>
  defp encode(true), do: <<2>>
  defp encode(v) when is_integer(v) and v < 0, do: <<3, v::signed-64>>
  defp encode(v) when is_integer(v), do: <<4, v::64>>
  defp encode(v) when is_float(v), do: <<5, v::float-64>>
  defp encode(v) when is_binary(v), do: [<<6, byte_size(v)::32>>, v]
  defp encode(v) when is_list(v), do: [<<7, length(v)::32>>, Enum.map(v, &encode/1)]

  defp encode(v) when is_map(v) do
    [
      <<8, map_size(v)::32>>,
      v |> Enum.sort() |> Enum.map(fn {k, item} -> [encode(k), encode(item)] end)
    ]
  end

  defp value(_, depth) when depth > 32, do: {:error, :depth}
  defp value(<<0, rest::binary>>, _), do: {:ok, nil, rest}
  defp value(<<1, rest::binary>>, _), do: {:ok, false, rest}
  defp value(<<2, rest::binary>>, _), do: {:ok, true, rest}
  defp value(<<3, v::signed-64, rest::binary>>, _), do: {:ok, v, rest}
  defp value(<<4, v::64, rest::binary>>, _), do: {:ok, v, rest}
  defp value(<<5, v::float-64, rest::binary>>, _), do: {:ok, v, rest}
  defp value(<<6, size::32, v::binary-size(size), rest::binary>>, _), do: {:ok, v, rest}

  defp value(<<7, count::32, rest::binary>>, depth) when count <= byte_size(rest),
    do: list(rest, count, depth + 1, [])

  defp value(<<8, count::32, rest::binary>>, depth) when count <= div(byte_size(rest), 6),
    do: map(rest, count, depth + 1, %{})

  defp value(_, _), do: {:error, :value}
  defp list(rest, 0, _, acc), do: {:ok, Enum.reverse(acc), rest}

  defp list(bytes, count, depth, acc) do
    with {:ok, v, rest} <- value(bytes, depth), do: list(rest, count - 1, depth, [v | acc])
  end

  defp map(rest, 0, _, acc), do: {:ok, acc, rest}

  defp map(bytes, count, depth, acc) do
    with {:ok, key, rest} when is_binary(key) <- value(bytes, depth),
         false <- Map.has_key?(acc, key),
         {:ok, v, rest} <- value(rest, depth) do
      map(rest, count - 1, depth, Map.put(acc, key, v))
    else
      _ -> {:error, :map}
    end
  end

  defp checksum(bytes) when byte_size(bytes) >= 4 do
    size = byte_size(bytes) - 4
    <<body::binary-size(size), crc::32>> = bytes
    if :erlang.crc32(body) == crc, do: {:ok, body}, else: {:error, :checksum}
  end

  defp checksum(_), do: {:error, :checksum}
  defp identity?(value), do: is_binary(value) and byte_size(value) in 1..@max_identity
  defp mode_id(:events), do: 0
  defp mode_id(:opaque), do: 1
end
