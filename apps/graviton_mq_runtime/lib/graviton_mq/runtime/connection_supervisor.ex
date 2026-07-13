defmodule GravitonMQ.Runtime.ConnectionSupervisor do
  @moduledoc """
  Reserved owner of future per-connection supervision trees.

  Each future connection tree will own a connection, one writer, and a session
  supervisor. Milestone 0 starts no connection processes.
  """
end
