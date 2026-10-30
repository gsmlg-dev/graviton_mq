defmodule GravitonMQ.Client.Transport do
  @moduledoc false

  @owned_options ~w(active mode packet header packet_size deliver send_timeout send_timeout_close)a

  # Framing, passive reads and bounded writes belong to the running owner.
  # TLS callers may configure certificates and protocol settings, never replace
  # those socket invariants through OTP's shared TLS/inet option list.
  def valid_tls_options?(options),
    do:
      is_list(options) and Keyword.keyword?(options) and
        Enum.all?(options, fn {key, _value} -> key not in @owned_options end)

  # Both independently supervised clients use the same outgoing OTP transport
  # and TLS identity rules. Protocol state and reconnect policy stay in each
  # role's pure machine.
  def connect(config) do
    options = [
      :binary,
      packet: 0,
      active: false,
      nodelay: true,
      send_timeout: config.send_timeout,
      send_timeout_close: true
    ]

    host = if is_binary(config.host), do: String.to_charlist(config.host), else: config.host

    case config.transport do
      :tcp ->
        :gen_tcp.connect(host, config.port, options, config.connect_timeout)

      :tls ->
        :ssl.connect(
          host,
          config.port,
          options ++ tls_options(config, host),
          config.connect_timeout
        )
    end
  end

  def send(_transport, nil, _bytes), do: {:error, :closed}
  def send(:tcp, socket, bytes), do: :gen_tcp.send(socket, bytes)
  def send(:tls, socket, bytes), do: :ssl.send(socket, bytes)

  def active_once(:tcp, socket), do: :inet.setopts(socket, active: :once)
  def active_once(:tls, socket), do: :ssl.setopts(socket, active: :once)

  def close(_transport, nil), do: :ok
  def close(:tcp, socket), do: :gen_tcp.close(socket)
  def close(:tls, socket), do: :ssl.close(socket)

  defp tls_options(config, host) do
    tls = Keyword.put_new(config.tls_options, :verify, :verify_peer)
    # OTP uses the Host as certificate reference without SNI, including IP SAN
    # matching for tuple addresses. Callers reject :disable and verify_none.
    tls = if is_list(host), do: Keyword.put_new(tls, :server_name_indication, host), else: tls

    tls =
      Keyword.put_new(tls, :customize_hostname_check,
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      )

    if Keyword.has_key?(tls, :cacertfile) or Keyword.has_key?(tls, :cacerts),
      do: tls,
      else: Keyword.put(tls, :cacerts, :public_key.cacerts_get())
  end
end
