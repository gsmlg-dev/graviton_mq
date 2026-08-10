defmodule GravitonMQ.Queue.Event do
  @moduledoc """
  Protocol-independent logical persistence event emitted by a queue machine.

  The envelope contains stable broker identity and serializable data only. It
  deliberately excludes protocol sessions, handles, channels, delivery IDs,
  runtime process identity, and physical storage metadata. Event variants and
  transitions remain for a later queue-machine milestone.
  """

  @enforce_keys [:id, :node_id, :sequence, :type, :data]
  defstruct [:id, :node_id, :sequence, :type, :data]

  @type data_value ::
          nil
          | boolean()
          | integer()
          | float()
          | binary()
          | [data_value()]
          | %{optional(binary()) => data_value()}

  @type t :: %__MODULE__{
          id: GravitonMQ.Queue.EventId.t(),
          node_id: GravitonMQ.Core.NodeId.t(),
          sequence: non_neg_integer(),
          type: binary(),
          data: %{optional(binary()) => data_value()}
        }
end
