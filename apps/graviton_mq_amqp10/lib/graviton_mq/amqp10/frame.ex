defmodule GravitonMQ.AMQP10.Frame do
  @moduledoc """
  Data boundary for an AMQP 1.0 or SASL frame.

  The envelope preserves its declared wire boundaries while frame decoding
  remains independent of TCP, OTP processes, and performative parsing.

  Encoding requires `declared_size` to equal eight fixed-header octets plus the
  extended header and body lengths. `data_offset_words` must equal two plus
  the extended header length divided by four; padding is never added implicitly.
  The encoder supports only the `:amqp` frame type and retains both binaries
  exactly without interpreting their content.
  """

  @enforce_keys [:declared_size, :data_offset_words, :type, :channel, :extended_header, :body]
  defstruct [:declared_size, :data_offset_words, :type, :channel, :extended_header, :body]

  @type frame_type :: :amqp | :sasl
  @type t :: %__MODULE__{
          declared_size: 0..4_294_967_295,
          data_offset_words: 0..255,
          type: frame_type(),
          channel: GravitonMQ.AMQP10.Types.ushort(),
          extended_header: binary(),
          body: binary()
        }
end
