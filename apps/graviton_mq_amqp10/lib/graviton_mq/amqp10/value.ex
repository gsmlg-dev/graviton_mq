defmodule GravitonMQ.AMQP10.Value do
  @moduledoc """
  Represents AMQP 1.0 semantic values independently of their wire encoding.

  Semantic types are retained explicitly so equal Elixir payloads do not make
  AMQP `binary`, `string`, and `symbol` values interchangeable. Compact wire
  constructors such as `uint0` and `smalluint` are deliberately absent; a
  future encoder will choose those representations for a semantic `uint`.
  Float and double payloads retain their exact IEEE-754 bits so special values
  and signed zero are representable without relying on BEAM float semantics.
  """

  defmodule Array do
    @moduledoc """
    An AMQP array together with the single semantic element type shared by its
    values.
    """

    @enforce_keys [:element_type, :values]
    defstruct [:element_type, :values]

    @type t :: %__MODULE__{
            element_type: GravitonMQ.AMQP10.Value.array_element_type(),
            values: [GravitonMQ.AMQP10.Value.t()]
          }
  end

  defmodule Described do
    @moduledoc """
    An AMQP described value, including unknown or extension descriptors.
    """

    @enforce_keys [:descriptor, :value]
    defstruct [:descriptor, :value]

    @type t :: %__MODULE__{
            descriptor: GravitonMQ.AMQP10.Value.descriptor_value(),
            value: GravitonMQ.AMQP10.Value.t()
          }
  end

  @semantic_types [
    :null,
    :boolean,
    :ubyte,
    :ushort,
    :uint,
    :ulong,
    :byte,
    :short,
    :int,
    :long,
    :float,
    :double,
    :decimal32,
    :decimal64,
    :decimal128,
    :char,
    :timestamp,
    :uuid,
    :binary,
    :string,
    :symbol,
    :list,
    :map,
    :array,
    :described
  ]

  @scalar_types [
    :boolean,
    :ubyte,
    :ushort,
    :uint,
    :ulong,
    :byte,
    :short,
    :int,
    :long,
    :float,
    :double,
    :decimal32,
    :decimal64,
    :decimal128,
    :char,
    :timestamp,
    :uuid,
    :binary,
    :string,
    :symbol
  ]

  @enforce_keys [:type, :value]
  defstruct [:type, :value]

  @type integer_type :: :ubyte | :ushort | :uint | :ulong | :byte | :short | :int | :long

  @type scalar_type ::
          :boolean
          | integer_type()
          | :float
          | :double
          | :decimal32
          | :decimal64
          | :decimal128
          | :char
          | :timestamp
          | :uuid
          | :binary
          | :string
          | :symbol

  @type semantic_type :: scalar_type() | :null | :list | :map | :array | :described
  @type array_element_type ::
          scalar_type()
          | :list
          | :map
          | :array
          | {:described, descriptor_value(), array_element_type()}

  @type t :: %__MODULE__{
          type: semantic_type(),
          value:
            nil
            | boolean()
            | integer()
            | binary()
            | [t()]
            | [{t(), t()}]
            | Array.t()
            | Described.t()
        }

  @type binary_value :: %__MODULE__{type: :binary, value: binary()}
  @type string_value :: %__MODULE__{type: :string, value: binary()}
  @type symbol_value :: %__MODULE__{type: :symbol, value: binary()}
  @type ulong_value :: %__MODULE__{type: :ulong, value: non_neg_integer()}
  @type descriptor_value :: ulong_value() | symbol_value()
  @type map_value :: %__MODULE__{type: :map, value: [{t(), t()}]}
  @type described_value :: %__MODULE__{type: :described, value: Described.t()}

  @spec semantic_types() :: [semantic_type()]
  def semantic_types, do: @semantic_types

  @spec null() :: t()
  def null, do: value(:null, nil)

  @spec boolean(boolean()) :: t()
  def boolean(value) when is_boolean(value), do: value(:boolean, value)

  @spec ubyte(0..255) :: t()
  def ubyte(value) when value in 0..255, do: value(:ubyte, value)

  @spec ushort(0..65_535) :: t()
  def ushort(value) when value in 0..65_535, do: value(:ushort, value)

  @spec uint(0..4_294_967_295) :: t()
  def uint(value) when value in 0..4_294_967_295, do: value(:uint, value)

  @spec ulong(0..18_446_744_073_709_551_615) :: t()
  def ulong(value) when value in 0..18_446_744_073_709_551_615, do: value(:ulong, value)

  @spec byte(-128..127) :: t()
  def byte(value) when value in -128..127, do: value(:byte, value)

  @spec short(-32_768..32_767) :: t()
  def short(value) when value in -32_768..32_767, do: value(:short, value)

  @spec int(-2_147_483_648..2_147_483_647) :: t()
  def int(value) when value in -2_147_483_648..2_147_483_647, do: value(:int, value)

  @spec long(-9_223_372_036_854_775_808..9_223_372_036_854_775_807) :: t()
  def long(value) when value in -9_223_372_036_854_775_808..9_223_372_036_854_775_807,
    do: value(:long, value)

  @spec float(<<_::32>>) :: t()
  def float(<<_::32>> = value), do: value(:float, value)

  @spec double(<<_::64>>) :: t()
  def double(<<_::64>> = value), do: value(:double, value)

  @spec decimal32(<<_::32>>) :: t()
  def decimal32(<<_::32>> = value), do: value(:decimal32, value)

  @spec decimal64(<<_::64>>) :: t()
  def decimal64(<<_::64>> = value), do: value(:decimal64, value)

  @spec decimal128(<<_::128>>) :: t()
  def decimal128(<<_::128>> = value), do: value(:decimal128, value)

  @spec char(0..1_114_111) :: t()
  def char(value) when value in 0..0x10FFFF and value not in 0xD800..0xDFFF,
    do: value(:char, value)

  @spec timestamp(-9_223_372_036_854_775_808..9_223_372_036_854_775_807) :: t()
  def timestamp(value) when value in -9_223_372_036_854_775_808..9_223_372_036_854_775_807,
    do: value(:timestamp, value)

  @spec uuid(<<_::128>>) :: t()
  def uuid(<<_::128>> = value), do: value(:uuid, value)

  @spec binary(binary()) :: binary_value()
  def binary(value) when is_binary(value), do: value(:binary, value)

  @spec string(binary()) :: string_value()
  def string(value) when is_binary(value), do: value(:string, value)

  @spec symbol(binary()) :: symbol_value()
  def symbol(value) when is_binary(value), do: value(:symbol, value)

  @spec list([t()]) :: t()
  def list(values) when is_list(values) do
    if Enum.all?(values, &match?(%__MODULE__{}, &1)) do
      value(:list, values)
    else
      raise ArgumentError, "AMQP list members must be tagged values"
    end
  end

  @spec map([{t(), t()}]) :: map_value()
  def map(entries) when is_list(entries) do
    if Enum.all?(entries, &tagged_map_entry?/1) do
      value(:map, entries)
    else
      raise ArgumentError, "AMQP map entries must contain tagged keys and values"
    end
  end

  @spec array(array_element_type(), [t()]) :: t()
  def array(element_type, values) when is_list(values) do
    cond do
      not valid_array_element_type?(element_type) ->
        raise ArgumentError, "invalid AMQP array element type: #{inspect(element_type)}"

      Enum.all?(values, &array_element?(&1, element_type)) ->
        value(:array, %Array{element_type: element_type, values: values})

      true ->
        raise ArgumentError,
              "AMQP array values do not match element type #{inspect(element_type)}"
    end
  end

  @spec described(descriptor_value(), t()) :: described_value()
  def described(
        %__MODULE__{type: descriptor_type} = descriptor,
        %__MODULE__{} = described_value
      )
      when descriptor_type in [:ulong, :symbol] do
    value(:described, %Described{descriptor: descriptor, value: described_value})
  end

  def described(%__MODULE__{} = descriptor, %__MODULE__{}) do
    raise ArgumentError,
          "AMQP described-value descriptor must be ulong or symbol, got: #{inspect(descriptor)}"
  end

  defp valid_array_element_type?({:described, descriptor, underlying_type}),
    do: valid_descriptor?(descriptor) and valid_array_element_type?(underlying_type)

  defp valid_array_element_type?(type),
    do: type in @scalar_types or type in [:list, :map, :array]

  defp array_element?(
         %__MODULE__{
           type: :described,
           value: %Described{descriptor: actual_descriptor, value: described_value}
         },
         {:described, expected_descriptor, underlying_type}
       ),
       do:
         actual_descriptor == expected_descriptor and
           array_element?(described_value, underlying_type)

  defp array_element?(%__MODULE__{type: type}, type), do: true
  defp array_element?(_value, _element_type), do: false

  defp tagged_map_entry?({%__MODULE__{}, %__MODULE__{}}), do: true
  defp tagged_map_entry?(_entry), do: false

  defp valid_descriptor?(%__MODULE__{type: type}) when type in [:ulong, :symbol], do: true
  defp valid_descriptor?(_value), do: false

  defp value(type, value), do: %__MODULE__{type: type, value: value}
end
