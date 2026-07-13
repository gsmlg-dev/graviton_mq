defmodule GravitonMQ.AMQP10.Link do
  @moduledoc """
  Session-owned AMQP 1.0 link data.

  A link handle is scoped to protocol state and is never a durable queue
  message identifier.
  """

  @enforce_keys [:name, :role]
  defstruct [:name, :role, :handle, :source, :target]

  @type role :: :sender | :receiver
  @type t :: %__MODULE__{
          name: String.t(),
          role: role(),
          handle: GravitonMQ.AMQP10.Types.handle() | nil,
          source: term(),
          target: term()
        }
end
