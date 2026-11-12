defmodule GravitonMQ.Runtime.Test.TLS do
  @moduledoc false
  # Test-only TLS material and peers. OTP :ssl is an independent TLS
  # implementation here; product code uses ex_ssl only. Mix prunes :ssl from
  # the code path because no GravitonMQ application lists it.
  import ExUnit.Assertions

  @compile {:no_warn_undefined, :ssl}
  @password "graviton-s1b-key-password"

  def password, do: @password

  def directory! do
    directory =
      Path.join(
        System.tmp_dir!(),
        "graviton-tls-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(directory)
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(directory) end)
    directory
  end

  # One trusted CA with a localhost server leaf and a client leaf, an
  # untrusted CA with its own client leaf, and an AES-256 PKCS#8 encrypted copy
  # of the server key protected by password/0.
  def certificates!(directory \\ directory!()) do
    path = &Path.join(directory, &1)
    ca = authority!(path, "ca", "Graviton S1b Test CA")
    untrusted = authority!(path, "untrusted", "Graviton S1b Untrusted CA")

    server =
      leaf!(path, "server", ca, "localhost", 1, """
      subjectAltName=DNS:localhost,IP:127.0.0.1
      extendedKeyUsage=serverAuth
      """)

    client = leaf!(path, "client", ca, "graviton-client", 2, "extendedKeyUsage=clientAuth\n")

    stranger =
      leaf!(path, "stranger", untrusted, "graviton-stranger", 3, "extendedKeyUsage=clientAuth\n")

    encrypted_key = path.("server-encrypted.key")

    openssl!([
      "pkcs8",
      "-topk8",
      "-v2",
      "aes-256-cbc",
      "-in",
      server.key,
      "-out",
      encrypted_key,
      "-passout",
      "pass:" <> @password
    ])

    %{
      ca: ca.certificate,
      ca_key: ca.key,
      ca_der: certificate_der!(ca.certificate),
      untrusted: untrusted.certificate,
      certificate: server.certificate,
      key: server.key,
      key_der: key_der!(server.key),
      encrypted_key: encrypted_key,
      client_certificate: client.certificate,
      client_key: client.key,
      client_der: certificate_der!(client.certificate),
      stranger_certificate: stranger.certificate,
      stranger_key: stranger.key
    }
  end

  # Broker listener tls_options for the server identity (paths, not secrets).
  def listener_options(certificates, extra \\ []),
    do: Keyword.merge([certfile: certificates.certificate, keyfile: certificates.key], extra)

  def client_identity(certificates),
    do: [certfile: certificates.client_certificate, keyfile: certificates.client_key]

  def stranger_identity(certificates),
    do: [certfile: certificates.stranger_certificate, keyfile: certificates.stranger_key]

  def ensure_ssl! do
    Mix.ensure_application!(:ssl)
    {:ok, _} = Application.ensure_all_started(:ssl)
    :ok
  end

  # A verified OTP :ssl client: trusts the test CA, checks the name
  # "localhost" and offers TLS 1.3 only unless `options` overrides it.
  def connect(port, certificates, options \\ [], timeout \\ 2_000) do
    ensure_ssl!()

    defaults = [
      active: false,
      verify: :verify_peer,
      cacertfile: String.to_charlist(certificates.ca),
      server_name_indication: ~c"localhost",
      versions: [:"tlsv1.3"]
    ]

    :ssl.connect({127, 0, 0, 1}, port, [:binary | Keyword.merge(defaults, options)], timeout)
  end

  # An ex_ssl server socket handshaken in, and owned by, the calling process,
  # plus a connected OTP :ssl client also owned by the caller.
  def server_pair!(certificates, client_options \\ []) do
    ensure_ssl!()

    {:ok, listen} =
      SSL.listen(0,
        ip: {127, 0, 0, 1},
        certfile: certificates.certificate,
        keyfile: certificates.key
      )

    ExUnit.Callbacks.on_exit(fn -> SSL.close(listen) end)
    {:ok, {_ip, port}} = SSL.sockname(listen)
    caller = self()

    client =
      Task.async(fn ->
        {:ok, peer} = connect(port, certificates, client_options)
        :ok = :ssl.controlling_process(peer, caller)
        peer
      end)

    {:ok, socket} = SSL.transport_accept(listen, 2_000)
    {:ok, socket} = SSL.handshake(socket, 2_000)
    {socket, Task.await(client)}
  end

  def openssl!(arguments) do
    executable = System.find_executable("openssl") || flunk("openssl is required for TLS tests")
    {output, status} = System.cmd(executable, arguments, stderr_to_stdout: true)
    assert status == 0, "openssl #{Enum.join(arguments, " ")} failed: #{output}"
    output
  end

  defp authority!(path, name, common_name) do
    certificate = path.(name <> ".pem")
    key = path.(name <> ".key")

    openssl!([
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-days",
      "1",
      "-subj",
      "/CN=" <> common_name,
      "-addext",
      "basicConstraints=critical,CA:TRUE",
      "-addext",
      "keyUsage=critical,keyCertSign,cRLSign",
      "-keyout",
      key,
      "-out",
      certificate
    ])

    %{certificate: certificate, key: key}
  end

  defp leaf!(path, name, authority, common_name, serial, extensions) do
    certificate = path.(name <> ".pem")
    key = path.(name <> ".key")
    request = path.(name <> ".csr")
    extension_file = path.(name <> ".ext")

    File.write!(
      extension_file,
      "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n" <>
        extensions
    )

    openssl!([
      "req",
      "-new",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-subj",
      "/CN=" <> common_name,
      "-keyout",
      key,
      "-out",
      request
    ])

    openssl!([
      "x509",
      "-req",
      "-in",
      request,
      "-CA",
      authority.certificate,
      "-CAkey",
      authority.key,
      "-set_serial",
      Integer.to_string(serial),
      "-days",
      "1",
      "-extfile",
      extension_file,
      "-out",
      certificate
    ])

    %{certificate: certificate, key: key}
  end

  defp certificate_der!(pem) do
    [{:Certificate, der, :not_encrypted}] = :public_key.pem_decode(File.read!(pem))
    der
  end

  defp key_der!(pem) do
    [{_type, der, :not_encrypted}] = :public_key.pem_decode(File.read!(pem))
    der
  end
end
