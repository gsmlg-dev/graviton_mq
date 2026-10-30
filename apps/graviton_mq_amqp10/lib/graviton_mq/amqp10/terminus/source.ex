defmodule GravitonMQ.AMQP10.Terminus.Source do
  @moduledoc """
  Immutable AMQP 1.0 Source composite, retaining tagged fields and extensions.

  Decoding materializes the protocol defaults. This data neither creates a
  node nor implements filtering, expiry, delivery, or durable storage.
  """

  alias GravitonMQ.AMQP10.Value

  defstruct [
    :address,
    :durable,
    :expiry_policy,
    :timeout,
    :dynamic,
    :dynamic_node_properties,
    :distribution_mode,
    :filter,
    :default_outcome,
    :outcomes,
    :capabilities
  ]

  @type t :: %__MODULE__{
          address: Value.t() | nil,
          durable: Value.t() | nil,
          expiry_policy: Value.symbol_value() | nil,
          timeout: Value.t() | nil,
          dynamic: Value.t() | nil,
          dynamic_node_properties: Value.map_value() | nil,
          distribution_mode: Value.symbol_value() | nil,
          filter: Value.map_value() | nil,
          default_outcome: Value.described_value() | nil,
          outcomes: Value.t() | nil,
          capabilities: Value.t() | nil
        }
end
