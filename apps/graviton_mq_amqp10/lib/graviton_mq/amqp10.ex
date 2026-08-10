defmodule GravitonMQ.AMQP10 do
  @moduledoc """
  Owns AMQP 1.0 protocol concepts and their mapping boundary to the broker core.

  Milestone 1 provides a bounded, process-free codec foundation for recognizing
  the raw AMQP 1.0 protocol header, validating frame envelopes, encoding and
  decoding a small semantic value subset, and validating the Open and Begin
  performative schemas. It does not execute either performative, negotiate
  protocols or SASL, own a transport or process, parse messages, or claim
  complete AMQP compatibility.
  """
end
