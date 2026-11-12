defmodule GravitonMQ.AMQP10.Codec.Stream do
  @moduledoc """
  Pure incremental AMQP header/frame envelope reader shared by running roles.

  `next/2` yields one opaque envelope and the remaining input. Callers control
  work per turn by deciding when to consume that remainder. Only an incomplete
  envelope is retained, bounded by the configured frame limit (512 octets for
  SASL frames). Coalesced valid frames may collectively exceed that limit
  without enlarging the buffer.

  `new/2` accepts `accept: [0] | [3] | [0, 3]` protocol IDs. A recognized
  SASL header (ID 3) switches the frame phase to SASL frames; after a
  successful sasl-outcome the caller re-enters the header phase with
  `expect_header/2`. Bytes already returned as remaining input are untouched.
  This reader never decodes a performative, SASL body or message and never
  negotiates a layer.
  """

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Frame, as: FrameCodec
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.ProtocolHeader
  alias GravitonMQ.AMQP10.Frame

  @sasl_buffer 512

  defstruct phase: :protocol_header,
            buffer: <<>>,
            limits: %Limits{},
            accept: [0],
            frame_type: :amqp

  @type t :: %__MODULE__{
          phase: :protocol_header | :frames,
          buffer: binary(),
          limits: Limits.t(),
          accept: ProtocolHeader.accept(),
          frame_type: :amqp | :sasl
        }
  @type item :: {:protocol_header, ProtocolHeader.t()} | {:frame, Frame.t()}

  @spec new(Limits.t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(limits \\ Limits.default(), options \\ []) do
    accept = Keyword.keyword?(options) && Keyword.get(options, :accept, [0])

    with {:ok, limits} <- Limits.validate(limits, :frame_decode) do
      if ProtocolHeader.accept?(accept),
        do: {:ok, %__MODULE__{limits: limits, accept: accept}},
        else: invalid_stream()
    end
  end

  @doc """
  Re-enter the protocol-header phase after a SASL outcome.

  Accepted only in the SASL frame phase with no partial frame buffered.
  """
  @spec expect_header(t(), ProtocolHeader.accept()) :: {:ok, t()} | {:error, Error.t()}
  def expect_header(%__MODULE__{phase: :frames, frame_type: :sasl, buffer: <<>>} = stream, accept) do
    if ProtocolHeader.accept?(accept),
      do: {:ok, %{stream | phase: :protocol_header, accept: accept, frame_type: :amqp}},
      else: invalid_stream()
  end

  def expect_header(_stream, _accept), do: invalid_stream()

  @spec next(t(), binary()) ::
          {:ok, item(), t(), binary()} | {:more, t()} | {:error, Error.t()}
  def next(stream, input)

  def next(%__MODULE__{phase: phase, buffer: buffer, frame_type: type} = stream, input)
      when phase in [:protocol_header, :frames] and type in [:amqp, :sasl] and
             is_binary(buffer) and is_binary(input) do
    with true <- ProtocolHeader.accept?(stream.accept) || invalid_stream(),
         {:ok, _limits} <- Limits.validate(stream.limits, :frame_decode),
         :ok <- buffer_size(stream, byte_size(buffer)) do
      read_next(stream, input)
    end
  end

  def next(_stream, _input), do: invalid_stream()

  defp read_next(stream, input) do
    case decode_buffer(stream) do
      {:ok, %ProtocolHeader{} = header, rest} ->
        type = if header.protocol_id == 3, do: :sasl, else: :amqp

        {:ok, {:protocol_header, header},
         %{stream | phase: :frames, frame_type: type, buffer: rest}, input}

      {:ok, %Frame{} = frame, rest} ->
        {:ok, {:frame, frame}, %{stream | buffer: rest}, input}

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

  defp decode_buffer(%__MODULE__{phase: :protocol_header, buffer: buffer, accept: accept}),
    do: ProtocolHeader.recognize(buffer, accept)

  defp decode_buffer(%__MODULE__{
         phase: :frames,
         buffer: buffer,
         limits: limits,
         frame_type: type
       }),
       do: FrameCodec.decode(buffer, limits, types: [type])

  defp buffer_size(stream, size) do
    maximum =
      case stream do
        %{phase: :protocol_header} -> 8
        %{frame_type: :sasl} -> @sasl_buffer
        %{limits: limits} -> limits.max_frame_size
      end

    if size <= maximum,
      do: :ok,
      else: {:error, Error.new(:frame_decode, :limit_exceeded, :stream_buffer_limit)}
  end

  defp invalid_stream, do: {:error, Error.new(:frame_decode, :invalid_value, :invalid_stream)}
end
