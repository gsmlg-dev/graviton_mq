defmodule GravitonMQ.Queue.State do
  @moduledoc """
  Bounded, deterministic state of one in-memory queue incarnation.

  `messages` owns content exactly once, whether ready or claimed. `ready`
  contains only message identities, and `claims` contains only ownership data.
  Consumer generations are node-wide admission leases, independent of any
  protocol channel, handle, or connection generation.
  """

  @enforce_keys [:node_id, :incarnation, :limits]
  defstruct [
    :node_id,
    :incarnation,
    :limits,
    profile: :memory,
    messages: %{},
    ready: {[], []},
    ready_count: 0,
    claims: %{},
    consumers: %{},
    message_count: 0,
    bytes: 0,
    event_sequence: 0,
    delivery_sequence: 0,
    last_registration_generation: -1
  ]

  @opaque t :: %__MODULE__{}
end
