defmodule GravitonMQ.Core.Message do
  @moduledoc """
  Protocol-independent message data owned by the broker core.

  The `id` is an internal broker identifier. It is not an AMQP delivery ID or
  link handle.
  """

  @enforce_keys [:id, :body]
  defstruct [:id, :body, annotations: %{}, durable?: false]

  @type id :: term()
  @type t :: %__MODULE__{
          id: id(),
          body: term(),
          annotations: map(),
          durable?: boolean()
        }
end
