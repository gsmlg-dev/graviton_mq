defmodule GravitonMQ.Storage.WAL do
  @moduledoc """
  Boundary for a future write-ahead-log implementation of
  `GravitonMQ.Core.Storage`.

  It does not claim the behaviour. No files, record encoding, synchronization,
  segments, or recovery operations are implemented in Milestone 0.
  """
end
