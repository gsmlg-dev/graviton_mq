defmodule GravitonMQ.AMQP10.ConnectionState do
  @moduledoc """
  Data owned by a future AMQP 1.0 connection process.

  Milestone 0 defines no connection state-machine transitions or negotiation.
  """

  defstruct [:local_container_id, :remote_container_id, sessions: %{}]

  @type t :: %__MODULE__{
          local_container_id: String.t() | nil,
          remote_container_id: String.t() | nil,
          sessions: map()
        }
end
