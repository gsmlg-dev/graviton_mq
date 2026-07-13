defmodule GravitonMQ.Runtime do
  @moduledoc """
  OTP composition boundary between protocol, broker core, and storage.

  Later milestones will place infrastructure, broker-node, listener,
  connection, writer, and session fault domains beneath this namespace.
  Milestone 0 starts none of those workers.
  """
end
