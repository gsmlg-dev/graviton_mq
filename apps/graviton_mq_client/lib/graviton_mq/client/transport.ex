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

  # Both independently supervised clients use the same outgoing transport
  # (TCP via :gen_tcp, TLS via the owned ex_ssl package, never OTP :ssl) and
  # TLS identity rules. Protocol state and reconnect policy stay in each
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
        SSL.connect(
          host,
          config.port,
          options ++ tls_options(config, host),
          config.connect_timeout
        )
    end
  end

  def send(_transport, nil, _bytes), do: {:error, :closed}
  def send(:tcp, socket, bytes), do: :gen_tcp.send(socket, bytes)
  def send(:tls, socket, bytes), do: SSL.send(socket, bytes)

  def active_once(:tcp, socket), do: :inet.setopts(socket, active: :once)
  def active_once(:tls, socket), do: SSL.setopts(socket, active: :once)

  def close(_transport, nil), do: :ok
  def close(:tcp, socket), do: :gen_tcp.close(socket)
  def close(:tls, socket), do: SSL.close(socket)

  defp tls_options(config, host) do
    tls = Keyword.put_new(config.tls_options, :verify, :verify_peer)
    # Preserves the pre-ex_ssl client compatibility (OTP offered TLS 1.3 and
    # 1.2); owner decision 2026-11-12. The broker side becomes TLS 1.3-only
    # once its TLS moves to ex_ssl (separate decision, ADR 0009). A
    # caller-supplied `versions` still wins.
    tls = Keyword.put_new(tls, :versions, [:"tlsv1.3", :"tlsv1.2"])
    # ex_ssl verifies the peer against the SNI value, or the dialed host when
    # SNI is absent (IP literals send no SNI and match IP SANs). Callers
    # reject :disable and verify_none.
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
