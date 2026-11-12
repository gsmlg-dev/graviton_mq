Code.require_file("support/peer_process.exs", __DIR__)

defmodule GravitonMQ.AMQP10Client.ArtemisSASLTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Client.Test.PeerProcess

  @moduletag :artemis_interop
  @moduletag timeout: 30_000
  @moduletag :tmp_dir
  @peer Path.join(__DIR__, "support/amqp_peer.py")
  # Created by scripts/interop/run.py: `artemis create --user ... --password ...`.
  @user "graviton-test"
  @password "ephemeral-test-only"

  setup_all do
    host = System.fetch_env!("ARTEMIS_HOST")
    port = System.fetch_env!("ARTEMIS_PORT") |> String.to_integer()
    python = System.fetch_env!("GRAVITON_PROTON_PYTHON")
    %{host: host, port: port, python: python, url: "amqp://#{host}:#{port}"}
  end

  test "A1: SASL ANONYMOUS reaches Artemis and an independent Proton receiver", context do
    client = connected!(context, sasl: :anonymous)
    round_trip!(context, client, context.url, [])
  end

  @tag :artemis_tls_interop
  test "A2: SASL PLAIN over verified TLS reaches Artemis", context do
    {port, ca} = tls()

    client =
      connected!(context,
        host: "localhost",
        port: port,
        transport: :tls,
        tls_options: [cacertfile: ca],
        sasl: {:plain, @user, @password}
      )

    round_trip!(context, client, "amqps://localhost:#{port}", ["--cafile", ca])
  end

  @tag :artemis_tls_interop
  test "A3: a wrong PLAIN password is refused with an auth outcome" do
    {port, ca} = tls()

    client =
      start_supervised!(
        {Client,
         container_id: address("refused"),
         host: "localhost",
         port: port,
         transport: :tls,
         tls_options: [cacertfile: ca],
         sasl: {:plain, @user, "wrong-password"}}
      )

    assert_receive {:graviton_mq, ^client, {:disconnected, {:sasl, :auth}}}, 5_000
    refute_receive {:graviton_mq, ^client, {:connected, _}}, 50
    assert %{phase: :closed} = Client.status(client)
  end

  defp tls do
    {System.fetch_env!("ARTEMIS_TLS_PORT") |> String.to_integer(),
     System.fetch_env!("ARTEMIS_TLS_CA")}
  end

  defp round_trip!(context, client, url, peer_options) do
    assert {:ok, operation} = Client.open_session(client, 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, 1}}}, 5_000
    address = address("sasl")

    assert {:ok, operation} =
             Client.attach(client, 1, "sasl-sender", :sender, address: Value.string(address))

    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, sender}}}, 5_000
    body = "artemis-sasl-#{System.unique_integer([:positive])}"
    path = Path.join(context.tmp_dir, "body")
    File.write!(path, body)

    receiver =
      start_supervised!(
        {PeerProcess,
         executable: context.python, args: [@peer, "receive", url, address, path] ++ peer_options}
      )

    eventually!(fn -> PeerProcess.snapshot(receiver).output =~ "\"ready\": true" end)
    {:ok, raw} = Message.encode_sections([{:data, Value.binary(body)}])
    assert {:ok, operation} = Client.send_message(client, 1, sender, raw)

    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, %Outcome.Accepted{}}}},
                   10_000

    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(receiver)
    assert output =~ :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)
  end

  defp connected!(context, options) do
    options =
      Keyword.merge(
        [container_id: address("client"), host: context.host, port: context.port],
        options
      )

    client = start_supervised!({Client, options})
    assert_receive {:graviton_mq, ^client, {:connected, peer}}, 5_000
    assert is_binary(peer) and peer != ""
    client
  end

  defp address(prefix), do: "graviton-#{prefix}-#{System.unique_integer([:positive])}"
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
