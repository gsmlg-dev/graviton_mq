defmodule GravitonMQ.MQTT do
  @moduledoc """
  Pure MQTT protocol data and bounded codecs for GravitonMQ.

  This library provides packet envelopes and CONNECT/CONNACK schemas for
  MQTT 5.0, with the historical MQTT 3.1.1 CONNECT/CONNACK codec retained.
  MQTT 3.1 is unsupported. It starts no processes
  and implements no broker, connection negotiation, authentication, or
  session behavior.
  """

  @type version :: :v3_1_1 | :v5
end
