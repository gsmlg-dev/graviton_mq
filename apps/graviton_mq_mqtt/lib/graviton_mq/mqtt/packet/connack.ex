defmodule GravitonMQ.MQTT.Packet.Connack do
  @moduledoc "CONNACK protocol data; decoding does not establish or resume a session."

  @enforce_keys [:version, :session_present, :reason_code]
  defstruct [:version, :session_present, :reason_code, properties: []]

  @type t :: %__MODULE__{
          version: GravitonMQ.MQTT.version(),
          session_present: boolean(),
          reason_code: 0..255,
          properties: GravitonMQ.MQTT.Codec.Properties.t()
        }
end
