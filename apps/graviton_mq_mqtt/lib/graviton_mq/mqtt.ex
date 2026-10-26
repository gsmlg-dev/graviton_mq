defmodule GravitonMQ.MQTT do
  @moduledoc """
  Pure MQTT protocol data and bounded codecs for GravitonMQ.

  This library provides packet envelopes and CONNECT/CONNACK schemas for
  MQTT 3.1.1 and 5.0. It starts no processes and implements no broker,
  connection negotiation, authentication, or session behavior.
  """

  @type version :: :v3_1_1 | :v5
end
