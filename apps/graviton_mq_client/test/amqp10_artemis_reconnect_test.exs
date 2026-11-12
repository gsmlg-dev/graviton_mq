Code.require_file("support/peer_process.exs", __DIR__)
Code.require_file("support/amqp_cut_proxy.exs", __DIR__)

defmodule GravitonMQ.AMQP10Client.ArtemisReconnectTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Client.Test.{AMQPCutProxy, PeerProcess}

  @moduletag :artemis_interop
  @moduletag timeout: 60_000
  @moduletag :tmp_dir
  @peer Path.join(__DIR__, "support/amqp_peer.py")
  # Created by scripts/interop/run.py (`artemis create --user ... --password ...`).
  @user "graviton-test"
  @password "ephemeral-test-only"

  setup_all do
    host = System.fetch_env!("ARTEMIS_HOST")
    port = System.fetch_env!("ARTEMIS_PORT") |> String.to_integer()
    python = System.fetch_env!("GRAVITON_PROTON_PYTHON")
    %{host: host, port: port, python: python, url: "amqp://#{host}:#{port}"}
  end

  test "R1: a cut while active reconnects, restores and publishes again", context do
    {proxy, client} =
      connected!(context, reconnect: [max_attempts: 3, delay: 100, max_delay: 400])

    session!(client)
    address = address("r1")
    sender = attach!(client, "r1-sender", :sender, address)
    :ok = AMQPCutProxy.cut(proxy)
    assert_receive {:graviton_mq, ^client, {:disconnected, :closed}}, 5_000
    assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
    restored!(client, [{:session, 1}, {:link, 1, sender}])
    round_trip!(context, client, sender, address)
  end

  test "R2: an unsettled delivery is voided and redelivered after new credit", context do
    {proxy, client} =
      connected!(context,
        session_options: [max_message_bytes: 1024],
        reconnect: [max_attempts: 3, delay: 100, max_delay: 400]
      )

    session!(client)
    address = address("r2")
    receiver = attach!(client, "r2-receiver", :receiver, address, max_message_bytes: 1024)
    path = body!(context, "r2-redelivered")
    assert {_, 0} = proton(context, "send", address, path)
    request!(client, receiver, 1, 1024)
    assert_receive {:graviton_mq, ^client, {:delivery, old, 1, ^receiver, first, _}}, 10_000
    :ok = AMQPCutProxy.cut(proxy)
    assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 5_000
    restored!(client, [{:session, 1}, {:link, 1, receiver}])
    assert {:error, :stale_delivery} = Client.settle(client, old, %Outcome.Accepted{})
    # Credit is not restored automatically (owner Q2).
    refute_receive {:graviton_mq, ^client, {:delivery, _, _, _, _, _}}, 300
    request!(client, receiver, 1, 1024)
    assert_receive {:graviton_mq, ^client, {:delivery, fresh, 1, ^receiver, second, _}}, 10_000
    assert body(first) == body(second)
    assert {:ok, settling} = Client.settle(client, fresh, %Outcome.Accepted{})

    assert_receive {:graviton_mq, ^client, {:operation, ^settling, {:ok, %Outcome.Accepted{}}}},
                   5_000

    assert drained(context, address) == 0
  end

  test "R3: an outcome lost with the connection is unknown and never resent", context do
    {proxy, client} =
      connected!(context, reconnect: [max_attempts: 3, delay: 100, max_delay: 400])

    session!(client)
    address = address("r3")
    sender = attach!(client, "r3-sender", :sender, address)
    :ok = AMQPCutProxy.cut_on_disposition(proxy)
    {:ok, raw} = Message.encode_sections([{:data, Value.binary("r3-lost-outcome")}])
    assert {:ok, sending} = Client.send_message(client, 1, sender, raw)
    assert_receive {:proxy_cut, 1, :disposition}, 10_000
    assert_receive {:graviton_mq, ^client, {:operation, ^sending, {:unknown, _}}}, 5_000
    restored!(client, [{:session, 1}, {:link, 1, sender}])
    refute_receive {:proxy_transfer, 2}, 500
    refute_received {:graviton_mq, ^client, {:operation, ^sending, {:ok, _}}}
    assert drained(context, address) <= 1
  end

  test "R4: refused attempts back off by the formula until the peer is reachable", context do
    {proxy, client} =
      connected!(context, reconnect: [max_attempts: 5, delay: 100, max_delay: 400])

    :ok = AMQPCutProxy.refuse(proxy, true)
    :ok = AMQPCutProxy.cut(proxy)

    for attempt <- 1..3,
        do: assert_receive({:graviton_mq, ^client, {:reconnecting, ^attempt}}, 5_000)

    :ok = AMQPCutProxy.refuse(proxy, false)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    accepts = for n <- 2..4, do: receive_accept!(n)
    gaps = accepts |> Enum.chunk_every(2, 1, :discard) |> Enum.map(fn [a, b] -> b - a end)

    # Accept n+1 follows the retry scheduled after accept n failed:
    # delay(2) = 200 ms, delay(3) = 400 ms. Tolerance: -5 ms (timer and clock
    # granularity), +1,500 ms (CI scheduling).
    for {gap, delay} <- Enum.zip(gaps, [200, 400]) do
      assert gap >= delay - 5, "gap #{gap} ms below #{delay} ms"
      assert gap <= delay + 1_500, "gap #{gap} ms far above #{delay} ms"
    end
  end

  @tag :artemis_tls_interop
  test "R5: SASL PLAIN over verified TLS re-authenticates once per attempt", context do
    calls = :counters.new(1, [])
    provider = fn -> :counters.add(calls, 1, 1) && @password end

    {proxy, client} =
      connected!(context,
        upstream: tls_port(),
        parse: false,
        host: "localhost",
        transport: :tls,
        tls_options: [cacertfile: System.fetch_env!("ARTEMIS_TLS_CA")],
        sasl: {:plain, @user, provider},
        reconnect: [max_attempts: 3, delay: 100, max_delay: 400]
      )

    session!(client)
    :ok = AMQPCutProxy.cut(proxy)
    restored!(client, [{:session, 1}])
    assert :counters.get(calls, 1) == 2
  end

  test "R6: a blackholed connection is detected by idle timeout and replaced", context do
    {proxy, client} =
      connected!(context,
        idle_timeout_ms: 2_000,
        close_timeout_ms: 500,
        reconnect: [max_attempts: 3, delay: 100, max_delay: 400]
      )

    session!(client)
    :ok = AMQPCutProxy.blackhole(proxy)
    assert_receive {:graviton_mq, ^client, {:disconnected, :close_timeout}}, 6_000
    assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    # Restoration may be refused while Artemis keeps the half-open
    # connection (spec §6); only completion is asserted.
    assert_receive {:graviton_mq, ^client, {:restore_complete, _counts}}, 10_000
    assert %{owner: owner, attempt: 0} = Client.status(client)
    assert owner in [:active, :restoring]
  end

  test "F1: a permanently refusing peer exhausts the budget and stops cleanly", context do
    {proxy, client} = connected!(context, reconnect: [max_attempts: 3, delay: 50, max_delay: 200])
    :ok = AMQPCutProxy.refuse(proxy, true)
    :ok = AMQPCutProxy.cut(proxy)

    for attempt <- 1..3,
        do: assert_receive({:graviton_mq, ^client, {:reconnecting, ^attempt}}, 5_000)

    assert_receive {:graviton_mq, ^client, {:reconnect_exhausted, 3}}, 5_000
    assert %{owner: :stopped, next_retry_ms: nil} = Client.status(client)
    assert %{socket: nil, retry: nil, timers: timers} = :sys.get_state(client)
    assert timers == %{}
    refute_receive {:proxy_connection, 5, _}, 600
  end

  @tag :artemis_tls_interop
  test "F2: a wrong password on the second attempt is terminal", context do
    calls = :counters.new(1, [])

    provider = fn ->
      :counters.add(calls, 1, 1)
      if :counters.get(calls, 1) == 1, do: @password, else: "wrong-password"
    end

    {proxy, client} =
      connected!(context,
        upstream: tls_port(),
        parse: false,
        host: "localhost",
        transport: :tls,
        tls_options: [cacertfile: System.fetch_env!("ARTEMIS_TLS_CA")],
        sasl: {:plain, @user, provider},
        reconnect: [max_attempts: 3, delay: 100, max_delay: 400]
      )

    :ok = AMQPCutProxy.cut(proxy)
    assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 5_000
    assert_receive {:graviton_mq, ^client, {:disconnected, {:sasl, :auth}}}, 10_000
    refute_receive {:graviton_mq, ^client, {:reconnecting, _}}, 500
    assert %{owner: :stopped} = Client.status(client)
  end

  test "F3: close/1 during the backoff wait prevents any further connection", context do
    {proxy, client} =
      connected!(context, reconnect: [max_attempts: 3, delay: 2_000, max_delay: 2_000])

    :ok = AMQPCutProxy.cut(proxy)
    assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 5_000
    assert {:ok, closing} = Client.close(client)
    assert_receive {:graviton_mq, ^client, {:operation, ^closing, {:ok, :closed}}}, 100
    refute_receive {:proxy_connection, 2, _}, 2_500
    assert %{owner: :stopped} = Client.status(client)
  end

  defp connected!(context, options) do
    {upstream, options} = Keyword.pop(options, :upstream, context.port)
    {parse, options} = Keyword.pop(options, :parse, true)

    proxy =
      start_supervised!(
        {AMQPCutProxy, upstream: {context.host, upstream}, observer: self(), parse: parse}
      )

    options =
      Keyword.merge(
        [container_id: address("client"), host: "127.0.0.1", port: AMQPCutProxy.port(proxy)],
        options
      )

    client = start_supervised!({Client, options})
    assert_receive {:proxy_connection, 1, _}, 5_000
    assert_receive {:graviton_mq, ^client, {:connected, peer}}, 10_000
    assert is_binary(peer) and peer != ""
    {proxy, client}
  end

  defp restored!(client, items) do
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 10_000

    for item <- items do
      assert_receive {:graviton_mq, ^client, {:restored, ^item, {:ok, _}}}, 10_000
    end

    count = length(items)

    assert_receive {:graviton_mq, ^client, {:restore_complete, %{restored: ^count, failed: 0}}},
                   10_000
  end

  defp round_trip!(context, client, sender, address) do
    body = "artemis-reconnect-#{System.unique_integer([:positive])}"
    path = body!(context, body)

    receiver =
      start_supervised!(
        {PeerProcess,
         executable: context.python, args: [@peer, "receive", context.url, address, path]}
      )

    eventually!(fn -> PeerProcess.snapshot(receiver).output =~ "\"ready\": true" end)
    {:ok, raw} = Message.encode_sections([{:data, Value.binary(body)}])
    assert {:ok, sending} = Client.send_message(client, 1, sender, raw)

    assert_receive {:graviton_mq, ^client, {:operation, ^sending, {:ok, %Outcome.Accepted{}}}},
                   10_000

    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(receiver)
    assert output =~ :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)
  end

  defp session!(client) do
    assert {:ok, operation} = Client.open_session(client, 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, 1}}}, 5_000
  end

  defp attach!(client, name, role, address, options \\ []) do
    assert {:ok, operation} =
             Client.attach(
               client,
               1,
               name,
               role,
               Keyword.put(options, :address, Value.string(address))
             )

    identity = {Value.string(name), role}
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^identity}}}, 5_000
    identity
  end

  defp request!(client, identity, slots, bytes) do
    assert {:ok, operation} = Client.request(client, 1, identity, slots, bytes)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^slots}}}, 5_000
  end

  defp body(raw) do
    {:ok, decoded, <<>>} = Message.decode(raw)
    Enum.find(decoded.sections, &match?({kind, _} when kind in [:amqp_value, :data], &1))
  end

  defp drained(context, address) do
    {output, 0} = proton(context, "drain", address, body!(context, "unused"))
    [_, count] = Regex.run(~r/"drained": (\d+)/, output)
    String.to_integer(count)
  end

  defp receive_accept!(n) do
    receive do
      {:proxy_connection, ^n, at} -> at
    after
      10_000 -> flunk("proxy connection #{n} not observed")
    end
  end

  defp body!(context, body) do
    path = Path.join(context.tmp_dir, "body-#{System.unique_integer([:positive])}")
    File.write!(path, body)
    path
  end

  defp tls_port, do: System.fetch_env!("ARTEMIS_TLS_PORT") |> String.to_integer()

  defp proton(context, operation, address, path),
    do:
      System.cmd(context.python, [@peer, operation, context.url, address, path],
        stderr_to_stdout: true
      )

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
