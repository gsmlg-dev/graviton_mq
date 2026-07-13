defmodule GravitonMQ.Core do
  @moduledoc """
  Defines protocol-independent broker domain boundaries.

  The core does not know about AMQP frames, runtime processes, or concrete
  storage implementations. Later milestones will place queue lifecycle rules
  behind this boundary.
  """
end
