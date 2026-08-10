defmodule GravitonMQ.Storage.Record do
  @moduledoc """
  Physical storage record metadata for a future concrete implementation.

  Storage owns the format version, position, encoded logical event, checksum,
  and segment metadata. Core owns the decoded `GravitonMQ.Queue.Event` and does
  not construct this record.
  """

  @enforce_keys [:format_version, :position, :record_type, :encoded_event]
  defstruct [
    :format_version,
    :position,
    :record_type,
    :encoded_event,
    :checksum,
    :segment_id
  ]

  @type t :: %__MODULE__{
          format_version: pos_integer(),
          position: GravitonMQ.Core.CommitRef.t(),
          record_type: binary(),
          encoded_event: binary(),
          checksum: binary() | nil,
          segment_id: non_neg_integer() | nil
        }
end
