defmodule GravitonMQ.AMQP10.Codec do
  @moduledoc """
  Defines the bounded, process-free AMQP 1.0 codec foundation.

  Codec modules transform protocol bytes and semantic values without owning
  sockets, processes, or protocol state. The Open and Begin schema codec is a
  pure facade over the bounded value codec; recognizing a performative does not
  execute a Connection or Session transition. Resource limits remain explicit
  so decoding cannot consume unbounded input.
  """

  alias GravitonMQ.AMQP10.Codec.Error

  @type decode_result(value) ::
          {:ok, value, binary()} | {:more, pos_integer()} | {:error, Error.t()}
  @type encode_result :: {:ok, binary()} | {:error, Error.t()}
end
