Code.require_file("../../../graviton_mq_client/test/support/peer_process.exs", __DIR__)

defmodule GravitonMQ.AMQP10SASLProtonTest.Auth do
  @moduledoc false
  @behaviour GravitonMQ.Runtime.AMQP10.Auth

  @impl true
  def authenticate(
        %{mechanism: :plain, username: "graviton-user", password: "graviton-password"},
        _
      ),
      do: {:ok, {:user, "graviton-user"}}

  def authenticate(_credentials, _options), do: {:error, :denied}
end

defmodule GravitonMQ.AMQP10SASLProtonTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.AMQP10SASLProtonTest.Auth
  alias GravitonMQ.Client.Test.PeerProcess

  @moduletag :proton_interop
  @moduletag :tmp_dir
  @moduletag timeout: 60_000
  @peer Path.expand("../support/amqp_sasl_peer.py", __DIR__)
  @plain ["--mechs", "PLAIN", "--user", "graviton-user", "--password", "graviton-password"]

  setup_all do
    %{python: System.fetch_env!("GRAVITON_PROTON_PYTHON")}
  end

  test "P1: default Proton SASL ANONYMOUS round trip under the anonymous default", context do
    broker = broker!([])
    round_trip!(context, broker, url(broker), [])
  end

  test "P2: Proton without SASL is unchanged", context do
    broker = broker!([])
    round_trip!(context, broker, url(broker), ["--no-sasl"])
  end

  test "P3/P4: PLAIN over verified TLS admits good credentials and refuses a wrong password",
       context do
    {certificate, key} = certificate!(context)
    test = self()

    authorize = fn principal, action, address ->
      send(test, {:authorized, principal, action, address})
      :allow
    end

    broker =
      broker!(
        [auth: {Auth, []}, authorize: authorize],
        [
          [
            transport: :tls,
            ip: {127, 0, 0, 1},
            port: 0,
            tls_options: [
              certfile: String.to_charlist(certificate),
              keyfile: String.to_charlist(key)
            ]
          ]
        ]
      )

    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    url = "amqps://localhost:#{port}"
    tls = ["--cafile", certificate]
    round_trip!(context, broker, url, @plain ++ tls)
    assert_receive {:authorized, {:user, "graviton-user"}, :send, "orders"}, 1_000
    assert_receive {:authorized, {:user, "graviton-user"}, :receive, "orders"}, 1_000

    path = body_file(context, "never-admitted")
    wrong = ["--mechs", "PLAIN", "--user", "graviton-user", "--password", "wrong"]
    {output, status} = proton(context, "send", url, path, wrong ++ tls)
    assert status != 0, output

    eventually!(fn ->
      match?({:ok, %{sasl_failures: %{auth: 1}, links: 0}}, GravitonMQ.amqp10_status(broker))
    end)

    assert queue(broker).messages == 0
  end

  test "P5: PLAIN over TCP requires opt-in on both sides", context do
    assert {:error, :plain_requires_tls} =
             GravitonMQ.Runtime.Supervisor.start_link(
               name: nil,
               amqp10: amqp_options(auth: {Auth, []})
             )

    broker = broker!(auth: {Auth, []}, allow_insecure_plain: true)
    round_trip!(context, broker, url(broker), @plain ++ ["--insecure"])
  end

  test "P6: Proton without SASL is refused by a required policy", context do
    broker = broker!(auth: {Auth, []}, allow_insecure_plain: true)
    path = body_file(context, "no-sasl-refused")
    {output, status} = proton(context, "send", url(broker), path, ["--no-sasl"])
    assert status != 0, output

    eventually!(fn ->
      match?({:ok, %{connections: 0, links: 0}}, GravitonMQ.amqp10_status(broker))
    end)

    assert queue(broker).messages == 0
  end

  test "P7: ANONYMOUS is refused by a PLAIN-only broker", context do
    broker = broker!(auth: {Auth, []}, allow_insecure_plain: true)
    path = body_file(context, "anonymous-refused")
    {output, status} = proton(context, "send", url(broker), path, ["--mechs", "ANONYMOUS"])
    assert status != 0, output
    assert queue(broker).messages == 0
  end

  defp amqp_options(options, listeners \\ [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]) do
    Keyword.merge(
      [
        container_id: "graviton-#{System.unique_integer([:positive])}",
        auth: :anonymous,
        queues: ["orders"],
        endpoint_options: [max_frame_size: 512],
        listeners: listeners
      ],
      options
    )
  end

  defp broker!(options, listeners \\ [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]) do
    specification =
      Supervisor.child_spec(
        {GravitonMQ,
         name: nil, runtime_supervisor_name: nil, amqp10: amqp_options(options, listeners)},
        id: make_ref()
      )

    broker = start_supervised!(specification)
    assert {:ok, %{listeners: [_]}} = GravitonMQ.amqp10_status(broker)
    broker
  end

  defp url(broker) do
    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    "amqp://127.0.0.1:#{port}"
  end

  defp queue(broker) do
    assert {:ok, %{queues: %{"orders" => status}}} = GravitonMQ.amqp10_status(broker)
    status
  end

  defp round_trip!(context, broker, url, options) do
    body = "sasl-#{System.unique_integer([:positive])}"
    path = body_file(context, body)

    receiver =
      start_supervised!(
        {PeerProcess,
         executable: context.python, args: [@peer, "receive", url, "orders", path] ++ options},
        id: make_ref()
      )

    eventually!(fn ->
      snapshot = PeerProcess.snapshot(receiver)
      if snapshot.exit_status != nil, do: flunk(snapshot.output)
      snapshot.output =~ "\"ready\": true"
    end)

    {output, status} = proton(context, "send", url, path, options)
    assert status == 0, output
    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(receiver)
    assert output =~ digest(body)
    eventually!(fn -> queue(broker).messages == 0 end)
  end

  defp proton(context, operation, url, path, options),
    do:
      System.cmd(context.python, [@peer, operation, url, "orders", path] ++ options,
        stderr_to_stdout: true
      )

  defp certificate!(context) do
    certificate = Path.join(context.tmp_dir, "server.pem")
    key = Path.join(context.tmp_dir, "server.key")

    {output, status} =
      System.cmd(
        "openssl",
        [
          "req",
          "-x509",
          "-newkey",
          "rsa:2048",
          "-nodes",
          "-keyout",
          key,
          "-out",
          certificate,
          "-days",
          "1",
          "-subj",
          "/CN=localhost",
          "-addext",
          "subjectAltName=DNS:localhost"
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
    {certificate, key}
  end

  defp body_file(context, body) do
    path = Path.join(context.tmp_dir, "body-#{System.unique_integer([:positive])}")
    File.write!(path, body)
    path
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp eventually!(predicate), do: until!(predicate, System.monotonic_time(:millisecond) + 12_000)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline,
             "independent peer condition timed out"

      Process.sleep(10)
      until!(predicate, deadline)
    end
  end
end
