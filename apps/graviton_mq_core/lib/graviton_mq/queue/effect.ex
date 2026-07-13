defmodule GravitonMQ.Queue.Effect do
  @moduledoc """
  Side-effect descriptions returned by future queue state transitions.

  Effects will be data interpreted outside the pure machine. Milestone 0
  defines no effect variants and executes no effects.
  """

  @type t :: none()
end
