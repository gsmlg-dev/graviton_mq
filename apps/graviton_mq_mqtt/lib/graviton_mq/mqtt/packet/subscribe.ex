defmodule GravitonMQ.MQTT.Packet.Subscribe do
  @moduledoc """
  Immutable MQTT 5.0 SUBSCRIBE protocol data.

  Encoding accepts `{filter, qos}` or `{filter, options}` entries. Decoding
  materializes all four subscription options in each entry's options map.
  """

  @enforce_keys [:packet_id]
  defstruct [:packet_id, version: :v5, subscriptions: [], properties: []]

  @type options :: %{
          optional(:qos) => 0..2,
          optional(:no_local) => boolean(),
          optional(:retain_as_published) => boolean(),
          optional(:retain_handling) => 0..2
        }

  @type t :: %__MODULE__{
          version: :v5,
          packet_id: 1..65_535,
          subscriptions: [{binary(), 0..2 | options()}],
          properties: GravitonMQ.MQTT.Codec.Properties.t()
        }
end
