defmodule GravitonMQ.AMQP10.Frame do
  @moduledoc """
  Data boundary for an AMQP 1.0 or SASL frame.

  Frame encoding and decoding remain independent of TCP and OTP processes and
  are intentionally absent in Milestone 0.
  """

  @enforce_keys [:type, :channel, :body]
  defstruct [:type, :channel, :body]

  @type frame_type :: :amqp | :sasl
  @type t :: %__MODULE__{
          type: frame_type(),
          channel: GravitonMQ.AMQP10.Types.ushort(),
          body: term()
        }
end
