defmodule GravitonMQ.MQTT.Packet.Publish do
  @moduledoc "Immutable MQTT 5.0 PUBLISH protocol data; no role behavior."

  @enforce_keys [:topic, :payload]
  defstruct [
    :topic,
    :payload,
    :packet_id,
    version: :v5,
    qos: 0,
    dup: false,
    retain: false,
    properties: []
  ]

  @type t :: %__MODULE__{
          version: :v5,
          topic: binary(),
          payload: binary(),
          packet_id: 1..65_535 | nil,
          qos: 0..2,
          dup: boolean(),
          retain: boolean(),
          properties: GravitonMQ.MQTT.Codec.Properties.t()
        }
end
