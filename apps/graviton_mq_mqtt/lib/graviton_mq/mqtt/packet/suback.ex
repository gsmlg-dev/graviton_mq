defmodule GravitonMQ.MQTT.Packet.Suback do
  @moduledoc "Immutable MQTT 5.0 SUBACK protocol data; no role behavior."

  @enforce_keys [:packet_id]
  defstruct [:packet_id, version: :v5, reason_codes: [], properties: []]

  @type t :: %__MODULE__{
          version: :v5,
          packet_id: 1..65_535,
          reason_codes: [0..255],
          properties: GravitonMQ.MQTT.Codec.Properties.t()
        }
end
