defmodule GravitonMQ.Storage.Segment do
  @moduledoc """
  Boundary for a future grouping of durable broker records.

  Milestone 0 defines no segment format, file layout, or filesystem operations.
  """

  @type id :: non_neg_integer()
end
