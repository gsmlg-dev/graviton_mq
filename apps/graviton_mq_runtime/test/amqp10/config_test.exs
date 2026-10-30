defmodule GravitonMQ.Runtime.AMQP10.ConfigTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Runtime.AMQP10.Config

  test "memory AMQP listener and queues require an explicit anonymous policy" do
    assert {:error, :explicit_auth_required} = Config.build(queues: ["orders"])

    assert {:ok, config} =
             Config.build(
               auth: :anonymous,
               container_id: "broker",
               queues: ["orders"],
               listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
             )

    assert config.queues == ["orders"]
    assert [%{transport: :tcp, port: 0, ip: {127, 0, 0, 1}}] = config.listeners
    assert Keyword.fetch!(config.endpoint_options, :role) == :broker
    assert config.profile == :memory
  end

  test "queue names and all shell capacities are validated before starting owners" do
    for options <- [
          [queues: []],
          [queues: ["q", "q"]],
          [queues: [""]],
          [queues: [<<255>>]],
          [queues: [:external_atom]],
          [profile: :durable],
          [max_connections: 0],
          [max_auth_jobs: 0],
          [max_input_bytes: 0],
          [max_input_frames_per_turn: 0],
          [max_pump_actions: 0],
          [auth_timeout: :infinity],
          [send_timeout: :infinity],
          [connect_timeout: 60_000],
          [authorize: fn -> :allow end],
          [queue_options: [unknown: 1]],
          [queue_options: [max_bytes: :infinity]],
          [endpoint_options: [session_options: [max_message_bytes: :infinity]]],
          [endpoint_options: [role: :client]],
          [endpoint_options: [container_id: "other"]],
          [endpoint_options: [session_options: [peer_attach_policy: :auto]]],
          [listeners: [[transport: :websocket]]],
          [listeners: [[transport: :tcp, ip: {256, 0, 0, 1}, port: 0]]],
          [listeners: [[transport: :tcp, port: -1]]],
          [amqp_typo: true]
        ] do
      assert {:error, _} =
               Config.build(
                 Keyword.merge([auth: :anonymous, container_id: "broker", queues: ["q"]], options)
               )
    end
  end

  test "TLS listener options cannot replace passive binary framing or write limits" do
    for option <- [
          [active: true],
          [mode: :list],
          [packet: 4],
          [header: 1],
          [deliver: :port],
          [packet_size: 0],
          [send_timeout: :infinity],
          [send_timeout_close: false]
        ] do
      tls = [certfile: "server.pem", keyfile: "server.key"] ++ option

      assert {:error, _} =
               Config.build(
                 auth: :anonymous,
                 container_id: "broker",
                 queues: ["q"],
                 listeners: [[transport: :tls, tls_options: tls]]
               )
    end

    assert {:error, _} =
             Config.build(
               auth: :anonymous,
               container_id: "broker",
               queues: ["q"],
               listeners: [[transport: :tls, tls_options: []]]
             )
  end

  test "bounded preconfigured queues and authorizer remain data for the runtime shell" do
    authorizer = fn
      :anonymous, :send, "orders" -> :allow
      _, _, _ -> :deny
    end

    assert {:ok, config} =
             Config.build(
               auth: :anonymous,
               container_id: "embedded-amqp",
               queues: ["orders", "audit"],
               authorize: authorizer,
               max_connections: 2,
               endpoint_options: [max_sessions: 2, session_options: [max_links: 2]]
             )

    assert config.authorize == authorizer
    assert config.max_connections == 2
    assert Keyword.fetch!(config.endpoint_options, :container_id) == "embedded-amqp"
  end
end
