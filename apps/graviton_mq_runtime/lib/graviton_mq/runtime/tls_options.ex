defmodule GravitonMQ.Runtime.TLSOptions do
  @moduledoc false
  # Broker TLS listener options (S1b, ex_ssl server). An allowlist, because
  # ex_ssl passes every unknown key to :gen_tcp, where a TLS option could change
  # carrier framing. Values are checked structurally here; ex_ssl loads and
  # checks content at listen. Errors carry the key only, never a value.
  # active_chunk_size is owned by the runtime (set by HellHandoff), so it is
  # not a user option.

  @identity_keys ~w(cert certfile key keyfile password)a
  @keys @identity_keys ++
          ~w(certs_keys cacertfile cacerts verify fail_if_no_peer_cert depth ciphers
             versions alpn_preferred_protocols sni_hosts sni_fun)a
  @max_identities 16
  # The in-memory private key forms ex_ssl loads: {type, der}.
  @key_types [:RSAPrivateKey, :ECPrivateKey, :PrivateKeyInfo]
  # The TLS 1.3 suites ex_ssl implements: {id, name, OTP cipher, OTP prf}.
  @suites [
    {0x1301, :tls_aes_128_gcm_sha256, :aes_128_gcm, :sha256},
    {0x1302, :tls_aes_256_gcm_sha384, :aes_256_gcm, :sha384},
    {0x1303, :tls_chacha20_poly1305_sha256, :chacha20_poly1305, :sha256}
  ]

  @type error :: {:unsupported_tls_option, atom()} | {:invalid_tls_option, atom()}

  @spec validate(term()) :: :ok | {:error, error()}
  def validate(options) do
    with :ok <- shape(options),
         :ok <- known(options),
         :ok <- values(options),
         :ok <- prerequisites(options),
         do: identity(options)
  end

  @spec verify_peer?(keyword()) :: boolean()
  def verify_peer?(options), do: Keyword.get(options, :verify, :verify_none) == :verify_peer

  # Defence in depth: hell redacts secret transport options in listener
  # telemetry, and ex_ssl accepts a 0-arity password fun, which prints as
  # #Function<...> wherever Hell keeps or reports its options.
  @spec hell_options(keyword()) :: keyword()
  def hell_options(options), do: Enum.map(options, &protect/1)

  defp protect({:password, password}) when not is_function(password, 0),
    do: {:password, fn -> password end}

  defp protect({:certs_keys, entries}), do: {:certs_keys, Enum.map(entries, &protect_entry/1)}

  defp protect({:sni_hosts, hosts}),
    do:
      {:sni_hosts,
       Enum.map(hosts, fn {host, identity} -> {host, Enum.map(identity, &protect/1)} end)}

  defp protect(option), do: option

  defp protect_entry(%{password: password} = entry) when not is_function(password, 0),
    do: %{entry | password: fn -> password end}

  defp protect_entry(entry), do: entry

  defp shape(options) when is_list(options) do
    if Keyword.keyword?(options) do
      keys = Keyword.keys(options)

      case keys -- Enum.uniq(keys) do
        [] -> :ok
        [key | _] -> invalid(key)
      end
    else
      invalid(:tls_options)
    end
  end

  defp shape(_options), do: invalid(:tls_options)

  defp known(options) do
    case Enum.find(options, fn {key, _value} -> key not in @keys end) do
      nil -> :ok
      {key, _value} -> unsupported(key)
    end
  end

  defp values(options) do
    Enum.reduce_while(options, :ok, fn {key, value}, :ok ->
      case value(key, value) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp value(:certfile, value), do: check(path?(value), :certfile)
  defp value(:keyfile, value), do: check(path?(value), :keyfile)
  defp value(:cacertfile, value), do: check(path?(value), :cacertfile)
  defp value(:cert, value), do: check(der?(value), :cert)
  defp value(:key, value), do: check(key?(value), :key)
  defp value(:password, value), do: check(password?(value), :password)
  defp value(:certs_keys, value), do: certs_keys(value)

  defp value(:cacerts, value),
    do: check(match?([_ | _], value) and Enum.all?(value, &der?/1), :cacerts)

  defp value(:verify, value), do: check(value in [:verify_none, :verify_peer], :verify)
  defp value(:fail_if_no_peer_cert, value), do: check(is_boolean(value), :fail_if_no_peer_cert)
  defp value(:depth, value), do: check(is_integer(value) and value in 1..100, :depth)
  defp value(:ciphers, value), do: check(ciphers?(value), :ciphers)
  defp value(:versions, [:"tlsv1.3"]), do: :ok
  defp value(:versions, _value), do: unsupported(:versions)

  defp value(:alpn_preferred_protocols, value),
    do: check(alpn?(value), :alpn_preferred_protocols)

  defp value(:sni_hosts, value), do: sni_hosts(value)
  defp value(:sni_fun, value), do: check(is_function(value, 1), :sni_fun)

  defp prerequisites(options) do
    verify_peer = verify_peer?(options)
    trust = Keyword.has_key?(options, :cacerts) or Keyword.has_key?(options, :cacertfile)

    cond do
      verify_peer and not trust ->
        invalid(:verify)

      Keyword.has_key?(options, :fail_if_no_peer_cert) and not verify_peer ->
        invalid(:fail_if_no_peer_cert)

      Keyword.has_key?(options, :sni_hosts) and Keyword.has_key?(options, :sni_fun) ->
        invalid(:sni_fun)

      true ->
        :ok
    end
  end

  # A default identity is required even with sni_hosts or sni_fun.
  defp identity(options) do
    keys = Keyword.keys(options)

    cond do
      :certs_keys in keys -> :ok
      certificate?(keys) and private_key?(keys) -> :ok
      certificate?(keys) -> invalid(:keyfile)
      true -> invalid(:certfile)
    end
  end

  defp certs_keys(entries)
       when is_list(entries) and entries != [] and length(entries) <= @max_identities do
    if Enum.all?(entries, &(is_map(&1) and identity_entry?(Map.to_list(&1)))),
      do: :ok,
      else: invalid(:certs_keys)
  end

  defp certs_keys(_entries), do: invalid(:certs_keys)

  # A certs_keys or sni_hosts identity names both its certificate and its
  # private key; it does not inherit them from the top level.
  defp identity_entry?(pairs) do
    keys = Enum.map(pairs, &elem(&1, 0))

    Enum.all?(keys, &(&1 in @identity_keys)) and
      Enum.all?(pairs, fn {key, value} -> value(key, value) == :ok end) and
      certificate?(keys) and private_key?(keys)
  end

  defp sni_hosts([_ | _] = hosts) do
    names =
      Enum.map(hosts, fn
        {host, _identity} when is_list(host) -> Enum.map(host, &downcase/1)
        _other -> nil
      end)

    valid =
      nil not in names and names == Enum.uniq(names) and
        Enum.all?(hosts, fn {host, identity} ->
          charlist?(host) and is_list(identity) and Keyword.keyword?(identity) and
            sni_identity?(identity)
        end)

    check(valid, :sni_hosts)
  end

  defp sni_hosts(_hosts), do: invalid(:sni_hosts)

  defp sni_identity?(identity) do
    case Keyword.pop(identity, :certs_keys) do
      {nil, pairs} -> identity_entry?(pairs)
      {entries, []} -> certs_keys(entries) == :ok
      {_entries, _rest} -> false
    end
  end

  defp ciphers?([_ | _] = entries) do
    ids = Enum.map(entries, &suite_id/1)
    nil not in ids and ids == Enum.uniq(ids)
  end

  defp ciphers?(_entries), do: false

  defp suite_id(entry) do
    Enum.find_value(@suites, fn {id, name, cipher, prf} ->
      forms = [
        id,
        name,
        name |> Atom.to_string() |> String.upcase(),
        %{key_exchange: :any, cipher: cipher, mac: :aead, prf: prf}
      ]

      if entry in forms, do: id
    end)
  end

  defp alpn?([_ | _] = protocols),
    do:
      Enum.all?(protocols, &(is_binary(&1) and byte_size(&1) in 1..255)) and
        protocols == Enum.uniq(protocols)

  defp alpn?(_protocols), do: false

  defp certificate?(keys), do: :cert in keys or :certfile in keys
  defp private_key?(keys), do: :key in keys or :keyfile in keys
  defp key?({type, der}) when type in @key_types, do: der?(der)
  defp key?(_value), do: false
  defp password?(value), do: is_binary(value) or charlist?(value) or is_function(value, 0)
  defp path?(value), do: (is_binary(value) and value != "") or (charlist?(value) and value != [])
  defp der?(value), do: is_binary(value) and value != ""
  defp charlist?(value), do: is_list(value) and Enum.all?(value, &(is_integer(&1) and &1 >= 0))
  defp downcase(char) when char in ?A..?Z, do: char + 32
  defp downcase(char), do: char
  defp check(true, _key), do: :ok
  defp check(false, key), do: invalid(key)
  defp invalid(key), do: {:error, {:invalid_tls_option, key}}
  defp unsupported(key), do: {:error, {:unsupported_tls_option, key}}
end
