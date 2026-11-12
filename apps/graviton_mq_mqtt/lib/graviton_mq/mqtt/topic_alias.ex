defmodule GravitonMQ.MQTT.TopicAlias do
  @moduledoc """
  Pure MQTT 5.0 Topic Alias tables for one Network Connection.

  A table lives exactly as long as its Network Connection and is never
  persisted or carried to another connection (MQTT-3.3.2-7). `resolve/2`
  applies §3.3.4 inbound rules and removes the hop-local `topic_alias`
  property. `assign/2` applies the "stable first-N" outbound policy: the first
  `outbound_max` distinct topics receive aliases 1..N, which are never
  replaced. An empty topic is never aliased. `assign/2` returns the updated
  table immediately; the caller must keep it only when it actually emits the
  returned packet and otherwise retain the previous table.

  Stored topics are copied with `:binary.copy/1`: a decoded topic is a
  sub-binary of the whole received packet, and a table that kept it would keep
  that packet (and its payload) alive for the life of the connection.
  """

  alias GravitonMQ.MQTT.Packet.Publish

  defstruct inbound: %{}, inbound_max: 0, outbound: %{}, outbound_max: 0

  @type t :: %__MODULE__{
          inbound: %{pos_integer() => String.t()},
          inbound_max: 0..65_535,
          outbound: %{String.t() => pos_integer()},
          outbound_max: 0..65_535
        }

  @spec new(0..65_535, 0..65_535) :: t()
  def new(inbound_max, outbound_max)
      when inbound_max in 0..65_535 and outbound_max in 0..65_535,
      do: %__MODULE__{inbound_max: inbound_max, outbound_max: outbound_max}

  @spec resolve(t(), Publish.t()) ::
          {:ok, t(), Publish.t()} | {:error, :topic_alias_invalid | :topic_alias_unknown}
  def resolve(%__MODULE__{} = table, %Publish{properties: properties} = packet) do
    case Keyword.fetch(properties, :topic_alias) do
      :error ->
        {:ok, table, packet}

      {:ok, value} when not is_integer(value) or value < 1 or value > table.inbound_max ->
        {:error, :topic_alias_invalid}

      {:ok, value} ->
        properties = Keyword.delete(properties, :topic_alias)

        case packet.topic do
          "" ->
            case Map.fetch(table.inbound, value) do
              {:ok, topic} -> {:ok, table, %{packet | topic: topic, properties: properties}}
              :error -> {:error, :topic_alias_unknown}
            end

          topic ->
            {:ok, %{table | inbound: Map.put(table.inbound, value, :binary.copy(topic))},
             %{packet | properties: properties}}
        end
    end
  end

  @spec assign(t(), Publish.t()) :: {t(), Publish.t()}
  def assign(%__MODULE__{} = table, %Publish{topic: topic, properties: properties} = packet) do
    cond do
      topic == "" or Keyword.has_key?(properties, :topic_alias) ->
        {table, packet}

      Map.has_key?(table.outbound, topic) ->
        {table,
         %{packet | topic: "", properties: properties ++ [topic_alias: table.outbound[topic]]}}

      map_size(table.outbound) < table.outbound_max ->
        value = map_size(table.outbound) + 1

        {%{table | outbound: Map.put(table.outbound, :binary.copy(topic), value)},
         %{packet | properties: properties ++ [topic_alias: value]}}

      true ->
        {table, packet}
    end
  end
end
