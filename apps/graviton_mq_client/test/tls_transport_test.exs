Code.require_file("support/mosquitto.exs", __DIR__)

defmodule GravitonMQ.Client.TLSTransportTest do
  use ExUnit.Case, async: false

  # The client app does not depend on OTP :ssl; only this independent test
  # peer does.
  @compile {:no_warn_undefined, :ssl}

  alias GravitonMQ.Client.Test.Mosquitto
  alias GravitonMQ.Client.Transport
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Codec.{Frame, Performative}
  alias GravitonMQ.AMQP10.Performative.Open

  setup do
    openssl = System.find_executable("openssl")
    assert openssl, "openssl is required to generate the TLS test certificates"

    directory =
      Path.join(System.tmp_dir!(), "graviton-tls-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)

    certs = Mosquitto.certificates!(%{openssl: openssl}, directory)
    # Required: Mix omits :ssl from the code path because the app does not list it.
    Mix.ensure_application!(:ssl)
    {:ok, _} = Application.ensure_all_started(:ssl)

    {listener, port} = listen(certs, [:"tlsv1.3"])
    {:ok, certs: certs, listener: listener, port: port}
  end

  defp listen(certs, versions) do
    {:ok, listener} =
      :ssl.listen(0, [
        :binary,
        active: false,
        ip: {127, 0, 0, 1},
        reuseaddr: true,
        versions: versions,
        certfile: String.to_charlist(certs.certificate),
        keyfile: String.to_charlist(certs.private_key)
      ])

    {:ok, {_, port}} = :ssl.sockname(listener)
    on_exit(fn -> :ssl.close(listener) end)
    {listener, port}
  end

  # Independent OTP :ssl peer: accepts one connection, echoes one read, then
  # optionally closes. Returns the handshake result.
  defp serve(listener, close? \\ false) do
    Task.async(fn ->
      {:ok, transport} = :ssl.transport_accept(listener, 5_000)

      case :ssl.handshake(transport, 5_000) do
        {:ok, socket} ->
          {:ok, data} = :ssl.recv(socket, 0, 5_000)
          :ok = :ssl.send(socket, data)
          {:ok, [protocol: protocol]} = :ssl.connection_information(socket, [:protocol])
          if not close?, do: Process.sleep(200)
          :ssl.close(socket)
          {:ok, protocol}

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  defp config(context, overrides \\ %{}) do
    Map.merge(
      %{
        host: "localhost",
        port: context.port,
        transport: :tls,
        tls_options: [cacertfile: context.certs.ca_certificate],
        connect_timeout: 2_000,
        send_timeout: 1_000
      },
      overrides
    )
  end

  test "echoes through an owned SSL socket with verified peer", context do
    server = serve(context.listener)
    assert {:ok, %SSL.Socket{} = socket} = Transport.connect(config(context))
    assert :ok = Transport.send(:tls, socket, "ping")
    assert :ok = Transport.active_once(:tls, socket)
    assert_receive {:ssl, ^socket, "ping"}, 2_000
    assert :ok = Transport.close(:tls, socket)
    assert Task.await(server) == {:ok, :"tlsv1.3"}
  end

  test "rejects a server certificate from an untrusted CA", context do
    server = serve(context.listener)

    config = config(context, %{tls_options: [cacertfile: context.certs.untrusted]})
    assert {:error, {:tls_alert, {:unknown_ca, _}}} = Transport.connect(config)
    assert {:error, {:tls_alert, {:unknown_ca, _}}} = Task.await(server, 7_000)
  end

  test "rejects an IP host that the certificate does not name", context do
    server = serve(context.listener)

    # Same trusted CA and DNS name succeed in the first test, so only the
    # IP reference against the DNS-only SAN differs. ex_ssl reports the failed
    # reference check as the certificate_unknown alert, not a bad_cert detail.
    assert {:error, {:tls_alert, {:certificate_unknown, _}}} =
             Transport.connect(config(context, %{host: "127.0.0.1"}))

    assert {:error, {:tls_alert, {:certificate_unknown, _}}} = Task.await(server, 7_000)
  end

  test "reports a peer close_notify as ssl_closed", context do
    server = serve(context.listener, true)
    assert {:ok, socket} = Transport.connect(config(context))
    assert :ok = Transport.send(:tls, socket, "bye")
    assert :ok = Transport.active_once(:tls, socket)
    assert_receive {:ssl, ^socket, "bye"}, 2_000
    assert :ok = Transport.active_once(:tls, socket)
    assert_receive {:ssl_closed, ^socket}, 2_000
    assert :ok = Transport.close(:tls, socket)
    Task.await(server)
  end

  test "an option the owned TLS client does not support is an explicit error", context do
    config =
      config(context, %{
        tls_options: [cacertfile: context.certs.ca_certificate, honor_cipher_order: true]
      })

    assert Transport.valid_tls_options?(config.tls_options)
    assert {:error, {:option, :server_only, :honor_cipher_order}} = Transport.connect(config)
  end

  test "default offer still negotiates TLS 1.3 with a TLS 1.3 server", context do
    server = serve(context.listener)
    assert {:ok, socket} = Transport.connect(config(context))
    assert {:ok, [protocol: :"tlsv1.3"]} = SSL.connection_information(socket, [:protocol])
    assert :ok = Transport.send(:tls, socket, "v13")
    assert :ok = Transport.active_once(:tls, socket)
    assert_receive {:ssl, ^socket, "v13"}, 2_000
    assert :ok = Transport.close(:tls, socket)
    assert {:ok, :"tlsv1.3"} = Task.await(server)
  end

  test "default offer falls back to TLS 1.2 against a TLS 1.2-only server", context do
    {listener, port} = listen(context.certs, [:"tlsv1.2"])
    server = serve(listener)
    assert {:ok, socket} = Transport.connect(config(context, %{port: port}))
    assert {:ok, [protocol: :"tlsv1.2"]} = SSL.connection_information(socket, [:protocol])
    assert :ok = Transport.send(:tls, socket, "v12")
    assert :ok = Transport.active_once(:tls, socket)
    assert_receive {:ssl, ^socket, "v12"}, 2_000
    assert :ok = Transport.close(:tls, socket)
    assert {:ok, :"tlsv1.2"} = Task.await(server)
  end

  test "a caller versions override wins over the default", context do
    {listener, port} = listen(context.certs, [:"tlsv1.2"])
    server = serve(listener)

    config =
      config(context, %{
        port: port,
        tls_options: [cacertfile: context.certs.ca_certificate, versions: [:"tlsv1.3"]]
      })

    assert {:error, {:tls_alert, {:protocol_version, _}}} = Transport.connect(config)
    assert {:error, {:tls_alert, {:protocol_version, _}}} = Task.await(server, 7_000)
  end

  test "every reconnect performs a verified handshake; an untrusted certificate is terminal",
       context do
    header = <<"AMQP", 0, 1, 0, 0>>

    client =
      start_supervised!(
        {Client,
         container_id: "tls-reconnect",
         host: "localhost",
         port: context.port,
         transport: :tls,
         tls_options: [cacertfile: context.certs.ca_certificate],
         reconnect: [max_attempts: 5, delay: 20, max_delay: 20]}
      )

    for _attempt <- 1..2 do
      {:ok, transport} = :ssl.transport_accept(context.listener, 5_000)
      {:ok, socket} = :ssl.handshake(transport, 5_000)
      assert {:ok, ^header} = :ssl.recv(socket, 8, 2_000)
      assert %Open{} = ssl_pdu!(socket)

      {:ok, open} =
        Performative.encode(%Open{
          container_id: Value.string("peer"),
          max_frame_size: Value.uint(512)
        })

      :ok = :ssl.send(socket, header <> <<8 + byte_size(open)::32, 2, 0, 0::16, open::binary>>)
      assert_receive {:graviton_mq, ^client, {:connected, "peer"}}, 2_000
      :ok = :ssl.close(socket)
      assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 2_000
      assert_receive {:graviton_mq, ^client, {:reconnecting, _}}, 2_000
    end

    # The server certificate changes to one the client does not trust.
    {:ok, transport} = :ssl.transport_accept(context.listener, 5_000)
    untrusted_key = Path.join(Path.dirname(context.certs.untrusted), "untrusted.key")

    _ =
      :ssl.handshake(
        transport,
        [
          certfile: String.to_charlist(context.certs.untrusted),
          keyfile: String.to_charlist(untrusted_key)
        ],
        5_000
      )

    assert_receive {:graviton_mq, ^client, {:disconnected, {:connect, {:tls_alert, _alert}}}},
                   5_000

    refute_receive {:graviton_mq, ^client, {:reconnecting, _}}, 200
    assert %{owner: :stopped} = Client.status(client)
  end

  defp ssl_pdu!(socket) do
    {:ok, <<size::32>>} = :ssl.recv(socket, 4, 2_000)
    {:ok, rest} = :ssl.recv(socket, size - 4, 2_000)
    {:ok, frame, <<>>} = Frame.decode(<<size::32, rest::binary>>)
    {:ok, pdu, <<>>} = Performative.decode(frame.body)
    pdu
  end
end
