defmodule GravitonMQ.Queue.State do
  @moduledoc """
  Opaque state boundary for the future queue machine.

  The empty Milestone 0 structure deliberately makes no queue lifecycle or
  persistence claims.
  """

  defstruct []

  @opaque t :: %__MODULE__{}
end
