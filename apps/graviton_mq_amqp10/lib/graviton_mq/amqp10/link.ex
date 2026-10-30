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
    :receiver_settle_mode,
    :attach_operation,
    :detach_operation,
    :admission_token,
    :partial,
    error_detach?: false,
    error_incoming_ids: MapSet.new(),
    error_outgoing_ids: MapSet.new(),
    peer_initiated?: false,
    peer_attach_replied?: false,
    pending_echo?: false,
    phase: :detached,
    credit: 0,
    delivery_count: 0,
    peer_delivery_count: nil,
    max_message_bytes: 1_048_576,
    peer_max_message_bytes: 0,
    drain: false,
    drain_token: nil,
    tag_to_delivery: %{}
  ]

  @type role :: :sender | :receiver
  @type identity :: {GravitonMQ.AMQP10.Value.string_value(), role()}
  @type t :: %__MODULE__{
          name: GravitonMQ.AMQP10.Value.string_value(),
          role: role(),
          local_handle: GravitonMQ.AMQP10.Types.handle() | nil,
          remote_handle: GravitonMQ.AMQP10.Types.handle() | nil,
          source:
            GravitonMQ.AMQP10.Terminus.Source.t()
            | GravitonMQ.AMQP10.Value.described_value()
            | nil,
          target:
            GravitonMQ.AMQP10.Terminus.Target.t()
            | GravitonMQ.AMQP10.Value.described_value()
            | nil,
          sender_settle_mode: GravitonMQ.AMQP10.Settlement.sender_mode() | nil,
          receiver_settle_mode: GravitonMQ.AMQP10.Settlement.receiver_mode() | nil
        }

  @spec identity(t()) :: identity()
  def identity(%__MODULE__{name: name, role: role}), do: {name, role}
end
