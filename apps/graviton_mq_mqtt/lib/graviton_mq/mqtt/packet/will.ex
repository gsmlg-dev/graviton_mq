defmodule GravitonMQ.MQTT.Packet.Will do
  @moduledoc """
  CONNECT Will data. Payload remains opaque; this value schedules nothing.

  Payload Format Indicator is retained for later broker acceptance policy,
  separately from MQTT UTF-8 Encoded String validation.
  """

  @enforce_keys [:topic, :payload]
  defstruct [:topic, :payload, qos: 0, retain: false, properties: []]

  @type t :: %__MODULE__{
          topic: binary(),
          payload: binary(),
          qos: 0..2,
          retain: boolean(),
          properties: GravitonMQ.MQTT.Codec.Properties.t()
        }
end
