defmodule GravitonMQ.Core.Outcome do
  @moduledoc """
  Protocol-independent results of broker delivery processing.

  A protocol frontend may map these outcomes to protocol-specific settlement
  concepts, but those concepts do not enter the core contract.
  """

  @type t :: :acknowledged | :retry | {:rejected, term()}
end
