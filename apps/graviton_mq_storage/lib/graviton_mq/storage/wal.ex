defmodule GravitonMQ.Storage.WAL do
  @moduledoc """
  Future write-ahead-log implementation of `GravitonMQ.Core.Storage`.

  No files, directories, record formats, syncing, or recovery behaviour are
  implemented in Milestone 0.
  """

  @behaviour GravitonMQ.Core.Storage
end
