defmodule GravitonMQ.Core.CommitRef do
  @moduledoc """
  Identifies an ordered inclusive position in one logical durability stream.

  Commit references correlate an append with a later synchronization result.
  References from different `stream_id` values are not comparable.
  """

  @enforce_keys [:stream_id, :position]
  defstruct [:stream_id, :position]

  @opaque t :: %__MODULE__{stream_id: binary(), position: non_neg_integer()}

  @spec new(binary(), non_neg_integer()) :: t()
  def new(stream_id, position)
      when is_binary(stream_id) and is_integer(position) and position >= 0 do
    %__MODULE__{stream_id: stream_id, position: position}
  end
end
