defmodule GravitonMQ.AMQP10.Link do
  @moduledoc """
  Session-owned AMQP 1.0 link data.

  The local endpoint and peer allocate handles independently. `role` is the
  local endpoint's role and combines with `name` to form the Session-local
  link identity. Handles remain protocol state and are never durable queue
  identifiers.
  """

  @enforce_keys [:name, :role]
  defstruct [
    :name,
    :role,
    :local_handle,
    :remote_handle,
    :source,
    :target,
    :sender_settle_mode,
    :receiver_settle_mode
  ]

  @type role :: :sender | :receiver
  @type identity :: {GravitonMQ.AMQP10.Value.string_value(), role()}
  @type t :: %__MODULE__{
          name: GravitonMQ.AMQP10.Value.string_value(),
          role: role(),
          local_handle: GravitonMQ.AMQP10.Types.handle() | nil,
          remote_handle: GravitonMQ.AMQP10.Types.handle() | nil,
          source: GravitonMQ.AMQP10.Value.described_value() | nil,
          target: GravitonMQ.AMQP10.Value.described_value() | nil,
          sender_settle_mode: GravitonMQ.AMQP10.Settlement.sender_mode() | nil,
          receiver_settle_mode: GravitonMQ.AMQP10.Settlement.receiver_mode() | nil
        }

  @spec identity(t()) :: identity()
  def identity(%__MODULE__{name: name, role: role}), do: {name, role}
end
