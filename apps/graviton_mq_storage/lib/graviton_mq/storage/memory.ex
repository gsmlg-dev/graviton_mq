defmodule GravitonMQ.Storage.Memory do
  @moduledoc """
  Future in-memory implementation of `GravitonMQ.Core.Storage`.

  Milestone 0 declares the implementation boundary without exposing fake
  reads, writes, or acknowledgements.
  """

  @behaviour GravitonMQ.Core.Storage
end
