defmodule GravitonMQ.Queue.Machine do
  @moduledoc """
  Contract boundary for a future pure deterministic queue state machine.

  A later milestone will define `apply(state, command)` to return a new state
  and a list of effects. Milestone 0 intentionally exports no transition
  function and performs no queue behaviour.
  """

  @type transition :: {GravitonMQ.Queue.State.t(), [GravitonMQ.Queue.Effect.t()]}
end
