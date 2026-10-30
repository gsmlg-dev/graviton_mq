defmodule GravitonMQ.AMQP10.Forwarding do
  @moduledoc """
  Builds a bounded outbound view without replacing authoritative message bytes.

  New hops remove delivery annotations. Header delivery-count adds the queue's
  cumulative `failure_count` to the original uint, defaulting absent/null to
  zero. A true `first-acquirer` is cleared on the onward view: this intermediary
  cannot promise that no other Link has acquired the message. False or omitted
  values retain their original representation. Ordered `mutations` carry complete tagged-map encodings in the
  `amqp-1.0/message-annotations` format. Exact tagged keys are replaced in place;
  new keys append in encounter order. Null annotation values remain values.

  Untouched Header and message-annotations sections retain their original
  bytes. The complete bare-message tail, starting with properties, application
  properties, body or footer, is always retained verbatim. All new sections
  are measured before any section encoding or final binary allocation.

  `max_bytes` bounds the resulting view, so removing delivery annotations can
  allow a larger original to fit. Input and aggregate mutation encodings are
  independently limited to 16 MiB, with the codec's default item/nesting limits.
  Failures return atoms and never include message or mutation content.
  """

  alias GravitonMQ.AMQP10.Codec.{Error, Limits}
  alias GravitonMQ.AMQP10.Codec.Message, as: MessageCodec
  alias GravitonMQ.AMQP10.Codec.Value, as: WireValue
  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.Core.Message.Mutation

  @max_input_bytes 16_777_216
  @max_mutations 65_536
  @max_entries 32_768
  @max_failure_count 18_446_744_073_709_551_615
  @max_delivery_count 4_294_967_295
  @mutable [:header, :delivery_annotations, :message_annotations]

  @spec prepare(binary(), map(), pos_integer()) :: {:ok, binary()} | {:error, atom()}
  def prepare(raw, metadata, max_bytes)
      when is_binary(raw) and is_integer(max_bytes) and max_bytes in 1..0xFFFF_FFFF do
    with :ok <- input_size(raw),
         {:ok, failed, mutations} <- metadata(metadata),
         {:ok, sections} <- decode_message(raw),
         {:ok, prefix, bare_tail} <- prefix(sections, raw, %{}),
         {:ok, header} <- header(Map.get(prefix, :header), failed),
         {:ok, annotations} <- annotations(Map.get(prefix, :message_annotations), mutations),
         plans = [header, annotations, {:raw, bare_tail}],
         limits = %{Limits.default() | max_value_bytes: max_bytes},
         :ok <- measure(plans, limits, max_bytes) do
      if not Map.has_key?(prefix, :delivery_annotations) and
           Enum.all?(plans, &match?({:raw, _}, &1)) do
        {:ok, raw}
      else
        encode(plans, limits)
      end
    end
  end

  def prepare(_raw, _metadata, _max_bytes), do: {:error, :invalid_arguments}

  defp input_size(raw) when byte_size(raw) <= @max_input_bytes, do: :ok
  defp input_size(_raw), do: {:error, :resource_limit}

  defp metadata(%{failure_count: failed, mutations: mutations})
       when is_integer(failed) and failed >= 0 and failed <= @max_failure_count do
    with :ok <- mutation_list(mutations, 0),
         :ok <- mutation_bytes(mutations, 0) do
      {:ok, failed, mutations}
    end
  end

  defp metadata(_metadata), do: {:error, :invalid_metadata}
  defp mutation_list([], _count), do: :ok
  defp mutation_list([_ | _], @max_mutations), do: {:error, :resource_limit}
  defp mutation_list([_ | rest], count), do: mutation_list(rest, count + 1)
  defp mutation_list(_other, _count), do: {:error, :invalid_metadata}

  defp mutation_bytes([], _bytes), do: :ok

  defp mutation_bytes(
         [
           %Mutation{format: "amqp-1.0/message-annotations", encoded_data: bytes} = mutation
           | rest
         ],
         total
       )
       when is_binary(bytes) and map_size(mutation) == 3 do
    total = total + byte_size(bytes)

    if total <= @max_input_bytes,
      do: mutation_bytes(rest, total),
      else: {:error, :resource_limit}
  end

  defp mutation_bytes(_mutations, _total), do: {:error, :invalid_mutation}

  defp decode_message(raw) do
    case MessageCodec.decode(raw) do
      {:ok, message, <<>>} -> {:ok, message.sections}
      {:more, _needed} -> {:error, :incomplete_message}
      {:error, %Error{class: :unsupported}} -> {:error, :unsupported_message}
      {:error, %Error{class: :limit_exceeded}} -> {:error, :resource_limit}
      {:error, %Error{}} -> {:error, :malformed_message}
    end
  end

  defp prefix([{name, body} | sections], raw, parts) when name in @mutable do
    case MessageCodec.decode_section(raw) do
      {:ok, {^name, ^body}, rest} ->
        bytes = binary_part(raw, 0, byte_size(raw) - byte_size(rest))
        prefix(sections, rest, Map.put(parts, name, {body, bytes}))

      _ ->
        {:error, :malformed_message}
    end
  end

  defp prefix(_sections, raw, parts), do: {:ok, parts, raw}

  defp header(original, failed) do
    fields = if original, do: elem(original, 0).value, else: []
    first_acquirer? = Enum.at(fields, 3) == Value.boolean(true)

    original_count =
      case Enum.at(fields, 4) do
        %Value{type: :uint, value: count} -> count
        _absent_or_null -> 0
      end

    cond do
      failed == 0 and not first_acquirer? ->
        {:ok, unchanged(original)}

      failed > @max_delivery_count - original_count ->
        {:error, :delivery_count_overflow}

      true ->
        fields =
          if failed > 0 do
            fields = Enum.take(fields ++ List.duplicate(Value.null(), 5), 5)
            List.replace_at(fields, 4, Value.uint(original_count + failed))
          else
            fields
          end

        fields =
          if first_acquirer?, do: List.replace_at(fields, 3, Value.boolean(false)), else: fields

        {:ok, {:value, Value.described(Value.ulong(0x70), Value.list(fields))}}
    end
  end

  defp annotations(original, []), do: {:ok, unchanged(original)}

  defp annotations(original, mutations) do
    original_entries = if original, do: elem(original, 0).value, else: []
    initial = {Map.new(original_entries), []}

    mutations
    |> Enum.reduce_while({:ok, initial}, fn mutation, {:ok, merged} ->
      with {:ok, entries} <- decode_mutation(mutation.encoded_data),
           {:ok, merged} <- merge_entries(entries, merged) do
        {:cont, {:ok, merged}}
      else
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, {values, added}} ->
        keys = Enum.map(original_entries, &elem(&1, 0)) ++ Enum.reverse(added)
        entries = Enum.map(keys, &{&1, Map.fetch!(values, &1)})

        if entries == original_entries,
          do: {:ok, unchanged(original)},
          else: {:ok, {:value, Value.described(Value.ulong(0x72), Value.map(entries))}}

      error ->
        error
    end
  end

  defp decode_mutation(bytes) do
    case WireValue.decode(bytes) do
      {:ok, %Value{type: :map, value: entries}, <<>>} ->
        if Enum.all?(entries, fn {%Value{type: type}, _} -> type in [:symbol, :ulong] end),
          do: {:ok, entries},
          else: {:error, :invalid_mutation}

      _ ->
        {:error, :invalid_mutation}
    end
  end

  defp merge_entries(entries, merged) do
    Enum.reduce_while(entries, {:ok, merged}, fn {key, value}, {:ok, {values, added}} ->
      exists? = Map.has_key?(values, key)

      if not exists? and map_size(values) >= @max_entries do
        {:halt, {:error, :resource_limit}}
      else
        added = if exists?, do: added, else: [key | added]
        {:cont, {:ok, {Map.put(values, key, value), added}}}
      end
    end)
  end

  defp unchanged(nil), do: {:raw, <<>>}
  defp unchanged({_body, bytes}), do: {:raw, bytes}

  defp measure(plans, limits, maximum) do
    Enum.reduce_while(plans, {:ok, 0}, fn plan, {:ok, total} ->
      with {:ok, size} <- plan_size(plan, limits),
           true <- size <= maximum - total do
        {:cont, {:ok, total + size}}
      else
        _ -> {:halt, {:error, :message_too_large}}
      end
    end)
    |> case do
      {:ok, _total} -> :ok
      error -> error
    end
  end

  defp plan_size({:raw, bytes}, _limits), do: {:ok, byte_size(bytes)}
  defp plan_size({:value, value}, limits), do: WireValue.encoded_size(value, limits)

  defp encode(plans, limits) do
    Enum.reduce_while(plans, {:ok, []}, fn plan, {:ok, bytes} ->
      case encode_plan(plan, limits) do
        {:ok, part} -> {:cont, {:ok, [part | bytes]}}
        _error -> {:halt, {:error, :message_too_large}}
      end
    end)
    |> case do
      {:ok, bytes} -> {:ok, bytes |> Enum.reverse() |> IO.iodata_to_binary()}
      error -> error
    end
  end

  defp encode_plan({:raw, bytes}, _limits), do: {:ok, bytes}
  defp encode_plan({:value, value}, limits), do: WireValue.encode(value, limits)
end
