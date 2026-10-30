defmodule GravitonMQ.MQTT.Packet.Unsubscribe do
  @moduledoc "Immutable MQTT 5.0 UNSUBSCRIBE protocol data; no role behavior."

  @enforce_keys [:packet_id]
  defstruct [:packet_id, version: :v5, filters: [], properties: []]

  @type t :: %__MODULE__{
          version: :v5,
          packet_id: 1..65_535,
          filters: [binary()],
          properties: GravitonMQ.MQTT.Codec.Properties.t()
        }
end
