defmodule GravitonMQ.Storage.Record do
  @moduledoc """
  Protocol-independent broker record data for future storage implementations.

  Records describe internal broker state. They do not contain AMQP session
  identifiers, delivery IDs, or link handles.
  """

  @enforce_keys [:id, :kind, :payload]
  defstruct [:id, :kind, :payload]

  @type t :: %__MODULE__{id: term(), kind: atom(), payload: term()}
end
