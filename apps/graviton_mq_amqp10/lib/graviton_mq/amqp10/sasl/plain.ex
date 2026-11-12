defmodule GravitonMQ.AMQP10.SASL.Plain do
  @moduledoc """
  RFC 4616 PLAIN message: `[authzid] NUL authcid NUL passwd`.

  `username` (authcid) and `password` are 1..255 octets, `authzid` 0..255
  octets (empty means absent); all are valid UTF-8 without NUL. Failures carry
  no detail. The authorization identity is passed through; the host decides
  whether it is acceptable. `encode/1` re-validates every field, so a
  hand-built struct can never produce ambiguous bytes.

  Redaction covers `inspect/1` only: `Inspect` prints no field, but
  `inspect(plain, structs: false)`, Erlang `~p` formatting, crash reports and
  `:sys.get_status/1` print the raw struct. Callers must not keep a `Plain` (or
  its fields) in process state that can be printed.
  """

  @enforce_keys [:username, :password]
  defstruct [:username, :password, authzid: nil]

  @type t :: %__MODULE__{username: binary(), password: binary(), authzid: binary() | nil}

  @spec new(term(), term(), term()) :: {:ok, t()} | {:error, :invalid_plain_message}
  def new(username, password, authzid \\ nil) do
    if field?(username, 1) and field?(password, 1) and (is_nil(authzid) or field?(authzid, 0)),
      do:
        {:ok,
         %__MODULE__{
           username: username,
           password: password,
           authzid: if(authzid == "", do: nil, else: authzid)
         }},
      else: {:error, :invalid_plain_message}
  end

  @spec parse(term()) :: {:ok, t()} | {:error, :invalid_plain_message}
  def parse(message) when is_binary(message) and byte_size(message) <= 767 do
    case :binary.split(message, <<0>>, [:global]) do
      [authzid, username, password] -> new(username, password, authzid)
      _other -> {:error, :invalid_plain_message}
    end
  end

  def parse(_message), do: {:error, :invalid_plain_message}

  @spec encode(t()) :: {:ok, binary()} | {:error, :invalid_plain_message}
  def encode(%__MODULE__{username: username, password: password, authzid: authzid}) do
    if field?(username, 1) and field?(password, 1) and (is_nil(authzid) or field?(authzid, 0)),
      do: {:ok, (authzid || "") <> <<0>> <> username <> <<0>> <> password},
      else: {:error, :invalid_plain_message}
  end

  def encode(_plain), do: {:error, :invalid_plain_message}

  defp field?(value, minimum),
    do:
      is_binary(value) and byte_size(value) >= minimum and byte_size(value) <= 255 and
        String.valid?(value) and not String.contains?(value, <<0>>)
end

defimpl Inspect, for: GravitonMQ.AMQP10.SASL.Plain do
  def inspect(_plain, _options), do: "#GravitonMQ.AMQP10.SASL.Plain<redacted>"
end
