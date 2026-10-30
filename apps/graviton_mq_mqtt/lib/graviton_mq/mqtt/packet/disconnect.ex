defmodule GravitonMQ.MQTT.Packet.Disconnect do
  @moduledoc "Immutable MQTT 5.0 DISCONNECT protocol data; no role behavior."

  defstruct version: :v5, reason_code: 0, properties: []

  @type t :: %__MODULE__{
          version: :v5,
          reason_code: 0..255,
          properties: GravitonMQ.MQTT.Codec.Properties.t()
        }
end
