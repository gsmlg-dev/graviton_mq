defmodule GravitonMQ.MQTT.Packet.Connect do
  @moduledoc """
  Immutable CONNECT fields, without connection or authentication behavior.

  `clean_start` holds the wire clean flag. For MQTT 3.1.1 it means Clean
  Session; version-specific session lifetime rules are not implemented here.
  Omitted optional fields are `nil`, distinct from a present empty binary.
  """

  @enforce_keys [:version, :client_id, :clean_start]
  defstruct [
    :version,
    :client_id,
    :clean_start,
    :will,
    :username,
    :password,
    keep_alive: 0,
    properties: []
  ]

  @type t :: %__MODULE__{
          version: GravitonMQ.MQTT.version(),
          client_id: binary(),
          clean_start: boolean(),
          keep_alive: 0..65_535,
          properties: GravitonMQ.MQTT.Codec.Properties.t(),
          will: GravitonMQ.MQTT.Packet.Will.t() | nil,
          username: binary() | nil,
          password: binary() | nil
        }
end
