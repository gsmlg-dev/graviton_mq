defmodule GravitonMQ.AMQP10.SessionState do
  @moduledoc """
  Data owned by one future AMQP 1.0 session process.

  Links are held in the session's `links` map. They are not independent OTP
  processes in the initial design.
  """

  defstruct [:local_channel, :remote_channel, links: %{}]

  @type t :: %__MODULE__{
          local_channel: GravitonMQ.AMQP10.Types.ushort() | nil,
          remote_channel: GravitonMQ.AMQP10.Types.ushort() | nil,
          links: %{optional(String.t()) => GravitonMQ.AMQP10.Link.t()}
        }
end
