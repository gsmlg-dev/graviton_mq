defmodule GravitonMQ.Storage do
  @moduledoc """
  Namespace for concrete broker persistence implementations.

  Implementations conform to behaviours owned by `graviton_mq_core` and do
  not depend on AMQP 1.0 protocol state. Milestone 0 performs no persistence.
  """
end
