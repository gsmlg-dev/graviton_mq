defmodule GravitonMQ.AMQP10.Terminus.Target do
  @moduledoc """
  Immutable AMQP 1.0 Target composite with exact tagged values.

  Decoding materializes protocol defaults without creating a node or
  implementing runtime or storage behavior.
  """

  alias GravitonMQ.AMQP10.Value

  defstruct [
    :address,
    :durable,
    :expiry_policy,
    :timeout,
    :dynamic,
    :dynamic_node_properties,
    :capabilities
  ]

  @type t :: %__MODULE__{
          address: Value.t() | nil,
          durable: Value.t() | nil,
          expiry_policy: Value.symbol_value() | nil,
          timeout: Value.t() | nil,
          dynamic: Value.t() | nil,
          dynamic_node_properties: Value.map_value() | nil,
          capabilities: Value.t() | nil
        }
end
