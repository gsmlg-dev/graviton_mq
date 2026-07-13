defmodule GravitonMQ.Core.NodeType do
  @moduledoc """
  Names protocol-independent broker node categories.

  Milestone 0 reserves the queue node category while leaving its operational
  contract for a later milestone.
  """

  @type t :: :queue
end
