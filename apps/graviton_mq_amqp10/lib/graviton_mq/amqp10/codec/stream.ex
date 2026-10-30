defmodule GravitonMQ.AMQP10.Codec.Stream do
  @moduledoc """
  Pure incremental AMQP header/frame envelope reader shared by running roles.

  `next/2` yields one opaque envelope and the remaining input. Callers control
  work per turn by deciding when to consume that remainder. Only an incomplete
  envelope is retained, bounded by the configured frame limit. Coalesced valid
  frames may collectively exceed that limit without enlarging the buffer.
  This reader never decodes a performative or message, opens a connection or
  accepts SASL negotiation.
  """

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Frame, as: FrameCodec
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.ProtocolHeader
  alias GravitonMQ.AMQP10.Frame

  defstruct phase: :protocol_header, buffer: <<>>, limits: %Limits{}

  @type t :: %__MODULE__{
          phase: :protocol_header | :frames,
          buffer: binary(),
          limits: Limits.t()
        }
  @type item :: {:protocol_header, ProtocolHeader.t()} | {:frame, Frame.t()}

  @spec new(Limits.t()) :: {:ok, t()} | {:error, Error.t()}
  def new(limits \\ Limits.default()) do
    with {:ok, limits} <- Limits.validate(limits, :frame_decode) do
      {:ok, %__MODULE__{limits: limits}}
    end
  end

  @spec next(t(), binary()) ::
          {:ok, item(), t(), binary()} | {:more, t()} | {:error, Error.t()}
  def next(stream, input)

  def next(%__MODULE__{phase: phase, buffer: buffer} = stream, input)
      when phase in [:protocol_header, :frames] and is_binary(buffer) and is_binary(input) do
    with {:ok, _limits} <- Limits.validate(stream.limits, :frame_decode),
         :ok <- buffer_size(stream, byte_size(buffer)) do
      read_next(stream, input)
    end
  end

  def next(_stream, _input),
    do: {:error, Error.new(:frame_decode, :invalid_value, :invalid_stream)}

  defp read_next(stream, input) do
    case decode_buffer(stream) do
      {:ok, value, rest} ->
        kind = if stream.phase == :protocol_header, do: :protocol_header, else: :frame
        {:ok, {kind, value}, %{stream | phase: :frames, buffer: rest}, input}

      {:more, _needed} when input == <<>> ->
        {:more, stream}

      {:more, needed} ->
        take = min(needed, byte_size(input))

        with :ok <- buffer_size(stream, byte_size(stream.buffer) + take) do
          <<part::binary-size(take), rest::binary>> = input
          read_next(%{stream | buffer: stream.buffer <> part}, rest)
        end

      {:error, %Error{}} = error ->
        error
    end
  end

  defp decode_buffer(%__MODULE__{phase: :protocol_header, buffer: buffer}),
    do: ProtocolHeader.recognize(buffer)

  defp decode_buffer(%__MODULE__{phase: :frames, buffer: buffer, limits: limits}),
    do: FrameCodec.decode(buffer, limits)

  defp buffer_size(stream, size) do
    maximum = if stream.phase == :protocol_header, do: 8, else: stream.limits.max_frame_size

    if size <= maximum,
      do: :ok,
      else: {:error, Error.new(:frame_decode, :limit_exceeded, :stream_buffer_limit)}
  end
end
