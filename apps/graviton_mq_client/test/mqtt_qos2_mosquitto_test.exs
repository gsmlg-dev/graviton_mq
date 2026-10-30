Code.require_file("support/mosquitto.exs", __DIR__)
Code.require_file("support/puback_loss_proxy.exs", __DIR__)

defmodule GravitonMQ.MQTTClient.QoS2MosquittoTest do
  use ExUnit.Case, async: false
  alias GravitonMQ.Client.Test.Mosquitto, as: Peer
  alias GravitonMQ.Client.Test.PubackLossProxy, as: Proxy
  alias GravitonMQ.MQTT.Packet.Publish
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

  defp client!(port, options \\ []) do
    options =
      Keyword.merge(
        [
          host: "127.0.0.1",
          port: port,
          observer: self(),
          client_id: "graviton-qos2-#{System.unique_integer([:positive])}",
          session_expiry_interval: 60,
          operation_timeout: 10_000,
          reconnect: [max_attempts: 2, delay: 300, max_delay: 300]
        ],
        options
      )

    client = start_supervised!({Client, options}, id: make_ref())
    assert_receive {:graviton_mq, ^client, {:connected, peer}}, 5_000
    assert peer.maximum_qos == 2
    client
  end

  for transport <- [:tcp, :tls] do
    @transport transport
    test "independent CLI sends and receives QoS2 over #{@transport}", context do
      {broker, client_options, cli_options} =
        if @transport == :tls do
          certs = Peer.certificates!(context.executables, context.directory)

          broker =
            Peer.broker!(context.executables, context.directory,
              max_qos: 2,
              configuration: "certfile #{certs.certificate}\nkeyfile #{certs.private_key}"
            )

          {broker,
           [
             transport: :tls,
             tls_options: [
               cacertfile: certs.ca_certificate,
               server_name_indication: ~c"localhost"
             ]
           ], [host: "localhost", extra: ["--cafile", certs.ca_certificate]]}
        else
          {Peer.broker!(context.executables, context.directory, max_qos: 2), [], []}
        end

      client = client!(broker.port, client_options)

      subscriber =
        Peer.subscriber!(
          context.executables,
          broker,
          "qos2/out",
          Keyword.put(cli_options, :qos, 2)
        )

      assert {:ok, operation} = Client.publish(client, "qos2/out", "to-cli", qos: 2)
      assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, :pubcomp}}}, 5_000

      Peer.eventually!(fn -> Peer.snapshot(subscriber).exit_status != nil end, fn ->
        Peer.snapshot(subscriber)
      end)

      assert %{exit_status: 0, output: output} = Peer.snapshot(subscriber)
      assert output =~ "qos2/out|2|to-cli"

      assert {:ok, subscription} = Client.subscribe(client, "qos2/in", qos: 2)
      assert_receive {:graviton_mq, ^client, {:operation, ^subscription, {:ok, [2]}}}, 5_000
      Peer.publish!(context.executables, broker, "qos2/in", "from-cli", 2, cli_options)
      Peer.eventually!(fn -> Client.status(client).deliveries == 1 end)
      refute_receive {:graviton_mq, ^client, {:delivery, _, _}}, 50
      assert :ok = Client.request(client, 1)

      assert_receive {:graviton_mq, ^client,
                      {:delivery, delivery,
                       %Publish{qos: 2, topic: "qos2/in", payload: "from-cli"}}},
                     5_000

      assert :ok = Client.ack(client, delivery)
      Peer.eventually!(fn -> Client.status(client).deliveries == 0 end)
      assert :ok = Client.disconnect(client)
    end
  end

  test "a real ACL denial returns negative PUBREC without PUBREL success", context do
    acl = Path.join(context.directory, "qos2.acl")
    File.write!(acl, "topic read qos2/#\n")

    broker =
      Peer.broker!(context.executables, context.directory,
        max_qos: 2,
        configuration: "acl_file #{acl}"
      )

    client = client!(broker.port)
    assert {:ok, operation} = Client.publish(client, "qos2/denied", "denied", qos: 2)

    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:error, {:pubrec, 0x87}}}},
                   5_000

    assert Client.status(client).pending == 0
    assert Client.status(client).phase == :connected
    assert :ok = Client.disconnect(client)
  end

  for {type, name} <- [{5, "PUBREC"}, {7, "PUBCOMP"}] do
    @loss_type type
    test "lost real #{name} resumes the correct outgoing QoS2 phase", context do
      broker = Peer.broker!(context.executables, context.directory, max_qos: 2)
      subscriber = Peer.subscriber!(context.executables, broker, "qos2/resume", qos: 2)

      proxy =
        start_supervised!(
          {Proxy, upstream_port: broker.port, observer: self(), drop_packet_type: @loss_type},
          id: make_ref()
        )

      client = client!(Proxy.port(proxy))
      assert {:ok, operation} = Client.publish(client, "qos2/resume", "once", qos: 2)

      assert_receive {:puback_loss_proxy, ^proxy,
                      {:publish, 1, %{packet_id: packet_id, dup: false, qos: 2}}},
                     5_000

      assert_receive {:puback_loss_proxy, ^proxy,
                      {:ack_dropped, :broker, @loss_type, 1, ^packet_id}},
                     5_000

      assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 5_000
      refute_receive {:graviton_mq, ^client, {:operation, ^operation, _}}, 50
      assert_receive {:puback_loss_proxy, ^proxy, {:connect, 2, %{clean_start: false}}}, 5_000
      assert_receive {:puback_loss_proxy, ^proxy, {:connack, 2, %{session_present: true}}}, 5_000
      assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000

      if @loss_type == 5 do
        assert_receive {:puback_loss_proxy, ^proxy,
                        {:publish, 2, %{packet_id: ^packet_id, dup: true, qos: 2}}},
                       5_000
      else
        refute_receive {:puback_loss_proxy, ^proxy, {:publish, 2, _}}, 100
      end

      assert_receive {:puback_loss_proxy, ^proxy, {:ack_forwarded, :client, 6, 2, ^packet_id}},
                     5_000

      assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, :pubcomp}}}, 5_000
      refute_receive {:graviton_mq, ^client, {:operation, ^operation, _}}, 50

      Peer.eventually!(fn -> Peer.snapshot(subscriber).exit_status != nil end, fn ->
        Peer.snapshot(subscriber)
      end)

      assert %{exit_status: 0, output: output} = Peer.snapshot(subscriber)
      assert output =~ "qos2/resume|2|once"
      assert :ok = Client.disconnect(client)
    end
  end

  test "incoming PUBREC waits for host ACK and lost PUBCOMP resumes without redelivery",
       context do
    broker = Peer.broker!(context.executables, context.directory, max_qos: 2)

    proxy =
      start_supervised!(
        {Proxy,
         upstream_port: broker.port,
         observer: self(),
         drop_packet_type: 7,
         drop_direction: :client},
        id: make_ref()
      )

    client = client!(Proxy.port(proxy))
    assert {:ok, subscription} = Client.subscribe(client, "qos2/manual", qos: 2)
    assert_receive {:graviton_mq, ^client, {:operation, ^subscription, {:ok, [2]}}}, 5_000
    assert :ok = Client.request(client, 2)
    Peer.publish!(context.executables, broker, "qos2/manual", "owned-once", 2)

    assert_receive {:graviton_mq, ^client,
                    {:delivery, delivery,
                     %Publish{qos: 2, packet_id: packet_id, payload: "owned-once"}}},
                   5_000

    refute_receive {:puback_loss_proxy, ^proxy, {:ack_forwarded, :client, 5, 1, ^packet_id}}, 100
    assert :ok = Client.ack(client, delivery)

    assert_receive {:puback_loss_proxy, ^proxy, {:ack_forwarded, :client, 5, 1, ^packet_id}},
                   5_000

    assert_receive {:puback_loss_proxy, ^proxy, {:ack_dropped, :client, 7, 1, ^packet_id}}, 5_000
    assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 5_000
    assert_receive {:puback_loss_proxy, ^proxy, {:connack, 2, %{session_present: true}}}, 5_000
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000

    assert_receive {:puback_loss_proxy, ^proxy, {:ack_forwarded, :broker, 6, 2, ^packet_id}},
                   5_000

    assert_receive {:puback_loss_proxy, ^proxy, {:ack_forwarded, :client, 7, 2, ^packet_id}},
                   5_000

    refute_receive {:graviton_mq, ^client, {:delivery, _, _}}, 200
    assert Client.status(client).deliveries == 0
    assert :ok = Client.disconnect(client)
  end
end
