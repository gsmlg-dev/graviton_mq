defmodule GravitonMQ.AMQP10.Codec.SASL do
  @moduledoc """
  Pure codec for the five AMQP 1.0 SASL frame bodies (Part 5 §5.3.3).

  A SASL frame body holds exactly one described list with no trailing bytes.
  Decoding accepts numeric (`ulong 0x40`–`0x44`) and symbolic descriptors and
  retains exact tagged field values; encoding emits the numeric descriptor and
  omits trailing absent fields (`nil` or an explicit null) for a canonical
  encoding, preserving interior nulls. Values are bounded by the 512-octet SASL
  frame. Errors carry only the body name and field index — never field bytes,
  lengths or values. `sasl-code` is not whitelisted.
  """

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Frame, as: FrameCodec
  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.Codec.Value, as: ValueCodec
  alias GravitonMQ.AMQP10.Frame
  alias GravitonMQ.AMQP10.SASL.{Challenge, Init, Mechanisms, Outcome, Response}
  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Value.{Array, Described}

  @limits %Limits{
    max_frame_size: 512,
    max_value_bytes: 504,
    max_compound_items: 64,
    max_nesting_depth: 4,
    max_zero_width_elements: 64
  }
  @max_mechanisms 16
  @names %{
    0x40 => :mechanisms,
    0x41 => :init,
    0x42 => :challenge,
    0x43 => :response,
    0x44 => :outcome
  }
  @symbols %{
    "amqp:sasl-mechanisms:list" => 0x40,
    "amqp:sasl-init:list" => 0x41,
    "amqp:sasl-challenge:list" => 0x42,
    "amqp:sasl-response:list" => 0x43,
    "amqp:sasl-outcome:list" => 0x44
  }
  @counts %{0x40 => 1, 0x41 => 3, 0x42 => 1, 0x43 => 1, 0x44 => 2}

  @type body :: Mechanisms.t() | Init.t() | Challenge.t() | Response.t() | Outcome.t()

  @spec limits() :: Limits.t()
  def limits, do: @limits

  @spec decode(binary()) :: {:ok, body()} | {:error, Error.t()}
  def decode(<<>>), do: error(:sasl_decode, :malformed, :empty_sasl_frame)

  def decode(body) when is_binary(body) do
    case ValueCodec.decode(body, @limits) do
      {:ok, value, <<>>} ->
        decode_value(value)

      {:ok, _value, _rest} ->
        error(:sasl_decode, :malformed, :trailing_bytes)

      {:more, _needed} ->
        error(:sasl_decode, :malformed, :truncated_sasl_body)

      {:error, %Error{class: class, reason: reason, offset: offset}} ->
        {:error, Error.new(:sasl_decode, class, reason, offset: offset)}
    end
  end

  def decode(_body), do: error(:sasl_decode, :invalid_value, :invalid_sasl_body)

  @spec encode(body()) :: {:ok, binary()} | {:error, Error.t()}
  def encode(body) do
    with {:ok, code, fields} <- body_fields(body),
         {:ok, _body} <- validate(code, fields, :sasl_encode, :invalid_value) do
      fields =
        fields
        |> Enum.reverse()
        |> Enum.drop_while(&absent?/1)
        |> Enum.reverse()
        |> Enum.map(&(&1 || Value.null()))

      case ValueCodec.encode(Value.described(Value.ulong(code), Value.list(fields)), @limits) do
        {:ok, bytes} -> {:ok, bytes}
        {:error, %Error{class: class, reason: reason}} -> error(:sasl_encode, class, reason)
      end
    end
  end

  @doc "Encode a body as a complete SASL frame (channel 0, data offset 2, at most 512 octets)."
  @spec encode_frame(body()) :: {:ok, binary()} | {:error, Error.t()}
  def encode_frame(body) do
    with {:ok, bytes} <- encode(body) do
      FrameCodec.encode(
        %Frame{
          declared_size: 8 + byte_size(bytes),
          data_offset_words: 2,
          type: :sasl,
          channel: 0,
          extended_header: <<>>,
          body: bytes
        },
        Limits.default(),
        types: [:sasl]
      )
    end
  end

  defp body_fields(%Mechanisms{server_mechanisms: mechanisms}), do: {:ok, 0x40, [mechanisms]}

  defp body_fields(%Init{mechanism: mechanism, initial_response: response, hostname: hostname}),
    do: {:ok, 0x41, [mechanism, response, hostname]}

  defp body_fields(%Challenge{challenge: challenge}), do: {:ok, 0x42, [challenge]}
  defp body_fields(%Response{response: response}), do: {:ok, 0x43, [response]}

  defp body_fields(%Outcome{code: code, additional_data: data}),
    do: {:ok, 0x44, [code, data]}

  defp body_fields(_body), do: error(:sasl_encode, :invalid_value, :invalid_sasl_body)

  defp decode_value(%Value{
         type: :described,
         value: %Described{descriptor: descriptor, value: body}
       }) do
    case {descriptor_code(descriptor), body} do
      {nil, _body} ->
        error(:sasl_decode, :malformed, :not_a_sasl_body)

      {code, %Value{type: :list, value: fields}} when is_list(fields) ->
        validate(code, fields, :sasl_decode, :malformed)

      {code, _body} ->
        error(:sasl_decode, :malformed, :invalid_sasl_body, %{body: Map.fetch!(@names, code)})
    end
  end

  defp decode_value(_value), do: error(:sasl_decode, :malformed, :not_a_sasl_body)

  defp descriptor_code(%Value{type: :ulong, value: code}) when is_map_key(@names, code), do: code

  defp descriptor_code(%Value{type: :symbol, value: symbol}) when is_map_key(@symbols, symbol),
    do: Map.fetch!(@symbols, symbol)

  defp descriptor_code(_descriptor), do: nil

  defp validate(code, fields, operation, class) do
    name = Map.fetch!(@names, code)

    if length(fields) > Map.fetch!(@counts, code),
      do: error(operation, class, :too_many_fields, %{body: name}),
      else: fields(name, fields, operation, class)
  end

  defp fields(:mechanisms, fields, operation, class) do
    case Enum.at(fields, 0) do
      %Value{type: :symbol, value: name} = value when is_binary(name) ->
        {:ok, %Mechanisms{server_mechanisms: value}}

      %Value{type: :array, value: %Array{element_type: :symbol, values: []}} ->
        field_error(operation, class, :empty_mechanism_list, :mechanisms, 0)

      %Value{type: :array, value: %Array{element_type: :symbol, values: values}} = value
      when is_list(values) ->
        cond do
          length(values) > @max_mechanisms ->
            field_error(operation, :limit_exceeded, :too_many_mechanisms, :mechanisms, 0)

          Enum.all?(values, &match?(%Value{type: :symbol, value: name} when is_binary(name), &1)) ->
            {:ok, %Mechanisms{server_mechanisms: value}}

          true ->
            field_error(operation, class, :field_type_mismatch, :mechanisms, 0)
        end

      value ->
        reason = if absent?(value), do: :mandatory_field_missing, else: :field_type_mismatch
        field_error(operation, class, reason, :mechanisms, 0)
    end
  end

  defp fields(:init, fields, operation, class) do
    with {:ok, mechanism} <- required(fields, 0, :symbol, :init, operation, class),
         {:ok, response} <- optional(fields, 1, :binary, :init, operation, class),
         {:ok, hostname} <- optional(fields, 2, :string, :init, operation, class) do
      {:ok, %Init{mechanism: mechanism, initial_response: response, hostname: hostname}}
    end
  end

  defp fields(:challenge, fields, operation, class) do
    with {:ok, challenge} <- required(fields, 0, :binary, :challenge, operation, class),
         do: {:ok, %Challenge{challenge: challenge}}
  end

  defp fields(:response, fields, operation, class) do
    with {:ok, response} <- required(fields, 0, :binary, :response, operation, class),
         do: {:ok, %Response{response: response}}
  end

  defp fields(:outcome, fields, operation, class) do
    with {:ok, code} <- required(fields, 0, :ubyte, :outcome, operation, class),
         {:ok, data} <- optional(fields, 1, :binary, :outcome, operation, class) do
      {:ok, %Outcome{code: code, additional_data: data}}
    end
  end

  defp required(fields, index, type, body, operation, class) do
    case Enum.at(fields, index) do
      %Value{type: ^type} = value ->
        {:ok, value}

      value ->
        reason = if absent?(value), do: :mandatory_field_missing, else: :field_type_mismatch
        field_error(operation, class, reason, body, index)
    end
  end

  defp optional(fields, index, type, body, operation, class) do
    case Enum.at(fields, index) do
      %Value{type: ^type} = value ->
        {:ok, value}

      value ->
        if absent?(value),
          do: {:ok, nil},
          else: field_error(operation, class, :field_type_mismatch, body, index)
    end
  end

  defp absent?(nil), do: true
  defp absent?(%Value{type: :null}), do: true
  defp absent?(_value), do: false

  defp field_error(operation, class, reason, body, index),
    do: error(operation, class, reason, %{body: body, index: index})

  defp error(operation, class, reason, details \\ %{}),
    do: {:error, Error.new(operation, class, reason, details: details)}
end
