defmodule GravitonMQ.Storage do
  @moduledoc """
  Namespace for concrete broker persistence implementations.

  Implementations conform to behaviours owned by `graviton_mq_core` and do
  not depend on protocol state. `GravitonMQ.Storage.WAL` implements the local
  Linux filesystem profile with logical-event and separate opaque-record
  streams. Historical Milestone 0 performed no persistence.
  """
end
