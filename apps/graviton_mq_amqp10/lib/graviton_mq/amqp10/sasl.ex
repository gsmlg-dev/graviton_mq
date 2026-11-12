defmodule GravitonMQ.AMQP10.SASL do
  @moduledoc """
  AMQP 1.0 SASL (Part 5 §5.3) constants shared by the codec and negotiation machine.

  Only ANONYMOUS (RFC 4505) and PLAIN (RFC 4616) are implemented, without a
  security layer. Mechanism names follow RFC 4422 §3.1; other peer-supplied
  names are retained as data but are never selectable.
  """

  alias GravitonMQ.AMQP10.Value

  @type mechanism :: Value.symbol_value()
  @type code_name :: :ok | :auth | :sys | :sys_perm | :sys_temp | {:unknown, 5..255}

  @anonymous "ANONYMOUS"
  @plain "PLAIN"
  @codes [ok: 0, auth: 1, sys: 2, sys_perm: 3, sys_temp: 4]

  @spec anonymous() :: binary()
  def anonymous, do: @anonymous

  @spec plain() :: binary()
  def plain, do: @plain

  @doc "Mechanisms this implementation can select, in decreasing preference."
  @spec supported_mechanisms() :: [binary()]
  def supported_mechanisms, do: [@plain, @anonymous]

  @spec code_name(0..255) :: code_name()
  for {name, code} <- @codes do
    def code_name(unquote(code)), do: unquote(name)
  end

  def code_name(code) when is_integer(code) and code in 5..255, do: {:unknown, code}

  @spec code(:ok | :auth | :sys | :sys_perm | :sys_temp) :: 0..4
  for {name, code} <- @codes do
    def code(unquote(name)), do: unquote(code)
  end

  @spec mechanism_name?(term()) :: boolean()
  def mechanism_name?(name) when is_binary(name) and byte_size(name) in 1..20 do
    name
    |> :binary.bin_to_list()
    |> Enum.all?(&(&1 in ?A..?Z or &1 in ?0..?9 or &1 in [?-, ?_]))
  end

  def mechanism_name?(_name), do: false

  @doc false
  def inspect_name(%Value{type: :symbol, value: name}) do
    if mechanism_name?(name), do: inspect(name), else: ":invalid"
  end

  def inspect_name(_value), do: ":invalid"
end
