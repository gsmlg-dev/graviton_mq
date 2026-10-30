Code.require_file("support/mosquitto.exs", __DIR__)
Code.require_file("support/puback_loss_proxy.exs", __DIR__)

defmodule GravitonMQ.MQTTClient.SessionMosquittoTest do
  use ExUnit.Case, async: false
  import Bitwise

  alias GravitonMQ.Client.Test.Mosquitto, as: Peer
  alias GravitonMQ.Client.Test.PubackLossProxy, as: Proxy
  alias GravitonMQ.MQTTClient, as: Client

  @moduletag :interop
  @moduletag timeout: 30_000

  setup_all do
    %{executables: Peer.executables!()}
  end

  setup do
    directory = Peer.directory!()
    on_exit(fn -> File.rm_rf!(directory) end)
    %{directory: directory}
  end

  test "lost real PUBACK resumes a live Mosquitto session with the original QoS1 operation",
       context do
    broker = Peer.broker!(context.executables, context.directory)
    topic = "graviton/session/resume"
    payload = "before-ack-loss-" <> String.duplicate("x", 256)
    subscriber = Peer.subscriber!(context.executables, broker, topic)

    proxy =
      start_supervised!({Proxy, upstream_port: broker.port, observer: self()}, id: make_ref())

    client_id = "graviton-resume-#{System.unique_integer([:positive])}"

    client =
      start_supervised!(
        {Client,
         host: "127.0.0.1",
         port: Proxy.port(proxy),
         client_id: client_id,
         observer: self(),
         session_expiry_interval: 60,
         keep_alive: 5,
         operation_timeout: 5_000,
         reconnect: [max_attempts: 2, delay: 300, max_delay: 300]},
        id: make_ref()
      )

    assert_receive {:puback_loss_proxy, ^proxy,
                    {:connect, 1, %{clean_start: true, client_id: ^client_id}}},
                   5_000

    assert_receive {:puback_loss_proxy, ^proxy,
                    {:connack, 1, %{session_present: false, reason_code: 0}}},
                   5_000

    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    original_generation = Client.status(client).generation
    assert {:ok, operation} = Client.publish(client, topic, payload, qos: 1)

    assert_receive {:puback_loss_proxy, ^proxy,
                    {:publish, 1, %{dup: false, qos: 1, topic: ^topic} = first}},
                   5_000

    packet_id = first.packet_id
    assert packet_id in 1..65_535
    assert_receive {:puback_loss_proxy, ^proxy, {:puback_dropped, 1, ^packet_id}}, 5_000
    assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 5_000
    assert Client.status(client).pending == 1
    refute_receive {:graviton_mq, ^client, {:operation, ^operation, _}}, 50

    # The subscriber exits after its first real delivery. QoS1 permits duplicates;
    # this assertion proves the initial publication reached an independent peer.
    Peer.eventually!(
      fn -> Peer.snapshot(subscriber).exit_status != nil end,
      fn -> Peer.snapshot(subscriber) end
    )

    assert %{exit_status: 0, output: output} = Peer.snapshot(subscriber)
    assert output =~ "#{topic}|1|#{payload}"

    assert_receive {:puback_loss_proxy, ^proxy,
                    {:connect, 2, %{clean_start: false, client_id: ^client_id}}},
                   5_000

    assert_receive {:puback_loss_proxy, ^proxy,
                    {:connack, 2, %{session_present: true, reason_code: 0}}},
                   5_000

    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    assert Client.status(client).generation > original_generation

    assert_receive {:puback_loss_proxy, ^proxy,
                    {:publish, 2,
                     %{dup: true, qos: 1, topic: ^topic, packet_id: ^packet_id} = retry}},
                   5_000

    assert retry.header == bor(first.header, 8)
    assert retry.body == first.body
    assert_receive {:puback_loss_proxy, ^proxy, {:puback_forwarded, 2, ^packet_id}}, 5_000
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, :puback}}}, 5_000
    refute_receive {:graviton_mq, ^client, {:operation, ^operation, _}}, 50
    assert Client.status(client).pending == 0
    assert %{connections: 2, dropped: true, open: true} = Proxy.snapshot(proxy)
    assert %{exit_status: nil} = Peer.snapshot(broker.owner)
    assert :ok = Client.disconnect(client)
    Peer.eventually!(fn -> not Proxy.snapshot(proxy).open end)
    assert Client.status(client).phase == :stopped
  end
end
