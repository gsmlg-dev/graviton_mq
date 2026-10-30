defmodule GravitonMQ.Runtime.MQTT.ConfigTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Runtime.MQTT.Config

  test "an enabled broker requires an explicit policy and accepts no implicit listener" do
    assert {:error, :invalid_mqtt_options} = Config.build([])

    assert {:ok, %{auth: :anonymous, listeners: [], host_principal: nil}} =
             Config.build(auth: :anonymous)
  end

  test "normalizes explicit loopback ephemeral listeners and bounds" do
    assert {:ok, config} =
             Config.build(
               auth: :anonymous,
               host_principal: :trusted_host,
               max_connections: 4,
               max_packet_size: 512,
               listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
             )

    assert [%{id: 0, transport: :tcp, ip: {127, 0, 0, 1}, port: 0}] =
             Enum.map(config.listeners, &Map.take(&1, [:id, :transport, :ip, :port]))

    assert config.machine_options[:max_connections] == 4
    assert config.limits.max_packet_size == 512
  end

  test "rejects invalid resources, callbacks, version and listener configuration" do
    for options <- [
          [versions: [:v3_1_1]],
          [versions: [:v5, :v3_1_1]],
          [typo: true],
          [auth: nil],
          [auth: {__MODULE__, []}],
          [max_connections: 0],
          [max_pending_bytes: -1],
          [auth_concurrency: 0],
          [auth_timeout: 0],
          [send_timeout: 60_001],
          [max_inflight: 65_536],
          [receive_maximum: 65_536],
          [max_packet_size: 0],
          [persistence: []],
          [persistence: [directory: "relative", stream_id: "mqtt"]],
          [persistence: [directory: "/tmp/durable"]],
          [persistence: [directory: "/tmp/durable", stream_id: "mqtt", max_snapshot_bytes: 0]],
          [persistence: [directory: "/tmp/durable", stream_id: "mqtt", max_log_bytes: 1]],
          [listeners: [[port: 0]]],
          [listeners: [[transport: :websocket, ip: {127, 0, 0, 1}, port: 0]]],
          [listeners: [[transport: :tcp, ip: {300, 0, 0, 1}, port: 0]]],
          [listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: -1]]],
          [listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0, active: true]]],
          [listeners: [[transport: :tls, ip: {127, 0, 0, 1}, port: 0]]],
          [
            listeners: [
              [
                transport: :tls,
                ip: {127, 0, 0, 1},
                port: 0,
                tls_options: [certfile: "cert", keyfile: "key", active: true]
              ]
            ]
          ],
          [
            listeners: [
              [id: :same, transport: :tcp, ip: {127, 0, 0, 1}, port: 0],
              [id: :same, transport: :tcp, ip: {127, 0, 0, 1}, port: 0]
            ]
          ]
        ] do
      assert {:error, :invalid_mqtt_options} =
               Config.build(Keyword.merge([auth: :anonymous], options))
    end
  end
end
