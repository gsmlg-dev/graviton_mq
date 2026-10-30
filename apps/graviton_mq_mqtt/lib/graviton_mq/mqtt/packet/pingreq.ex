defmodule GravitonMQ.MQTT.Packet.Pingreq do
  @moduledoc "Immutable MQTT 5.0 PINGREQ protocol data; no role behavior."

  defstruct version: :v5

  @type t :: %__MODULE__{
          version: :v5
        }
end
