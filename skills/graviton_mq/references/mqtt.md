# MQTT 5.0 integration

Use `graviton_mq_client` for outgoing connections and `graviton_mq` for a
broker. The examples use the 0.2.0 APIs and bounded, one-shot script processes.
An application should keep its observer alive and handle the same events in
its own service. For TLS, authentication or recovery, read
[operations](operations.md).

## Network client

Run this in a consumer with `{:graviton_mq_client, "~> 0.2.0"}` and a running
MQTT5 broker. `MQTT_HOST`/`MQTT_PORT` select that broker. The subscription and
publication deliberately share a topic so the broker returns a real delivery.

```elixir
alias GravitonMQ.MQTTClient, as: Client

{:ok, supervisor} =
  Supervisor.start_link(
    [{Client,
      observer: self(),
      host: System.get_env("MQTT_HOST", "127.0.0.1"),
      port: String.to_integer(System.get_env("MQTT_PORT", "1883")),
      client_id: "graviton-example-mqtt",
      keep_alive: 30}],
    strategy: :one_for_one
  )

[{_, client, _, _}] = Supervisor.which_children(supervisor)

receive do
  {:graviton_mq, ^client, {:connected, _connack}} -> :ok
after
  10_000 -> raise "MQTT connection was not established"
end

await = fn reference ->
  receive do
    {:graviton_mq, ^client, {:operation, ^reference, {:ok, result}}} -> result
    {:graviton_mq, ^client, {:operation, ^reference, result}} ->
      raise "MQTT operation did not succeed: #{inspect(result)}"
  after
    10_000 -> raise "No MQTT completion observed; outcome is unconfirmed"
  end
end

{:ok, subscription} = Client.subscribe(client, "events/example", qos: 1)
[1] = await.(subscription)
:ok = Client.request(client, 1)
{:ok, publication} = Client.publish(client, "events/example", "hello", qos: 1)
:puback = await.(publication)

receive do
  {:graviton_mq, ^client, {:delivery, delivery, %{payload: "hello"} = packet}} ->
    IO.puts(packet.payload)
    :ok = Client.ack(client, delivery)
after
  10_000 -> raise "Expected MQTT delivery was not observed"
end

:ok = Supervisor.stop(supervisor)
```

For an ongoing consumer, replenish `Client.request(client, count)` after
processing and acknowledging deliveries. Demand and outstanding ownership are
separate bounds. Only the configured observer can acknowledge its deliveries.
Choose QoS deliberately and handle the actual negotiated subscription grant;
the example asserts a QoS1 grant from a peer that supports it.

| Operation | Terminal result inside `{:operation, reference, result}` |
| --- | --- |
| QoS0 publish | `{:ok, :sent}`: socket acceptance, no proof of peer receipt |
| QoS1 publish | `{:ok, :puback}` or `{:error, {:puback, code}}` |
| QoS2 publish | `{:ok, :pubcomp}`; negative PUBREC/PUBCOMP keep packet and reason code |
| Subscribe | `{:ok, reason_codes}`: inspect each grant or rejection |
| Unsubscribe | `{:ok, reason_codes}` after UNSUBACK |
| Possibly sent operation interrupted | `{:error, :unknown_outcome}` |

`Client.unsubscribe(client, filter)` admits an asynchronous unsubscribe.
`Client.disconnect/1` intentionally disconnects and cancels automatic retry;
the owner remains inspectable through `Client.status/1`.

## Broker

For an embedded broker, use `{:graviton_mq, "~> 0.2.0", runtime: false}`.
This local example explicitly enables anonymous access and chooses a free
loopback port. Set a stable listener port such as 1883 for a service, after
choosing its authentication policy and transport.

```elixir
{:ok, broker_supervisor} =
  Supervisor.start_link(
    [{GravitonMQ,
      name: MyApp.MQTTBroker,
      runtime_supervisor_name: MyApp.MQTTBrokerRuntime,
      mqtt: [
        auth: :anonymous,
        host_principal: "local-host",
        max_connections: 128,
        max_pending_bytes: 8_388_608,
        listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
      ]}],
    strategy: :one_for_one
  )

{:ok, %{owner: owner, listeners: [%{port: mqtt_port}]}} =
  GravitonMQ.status(MyApp.MQTTBroker)
IO.puts("MQTT5 listener: 127.0.0.1:#{mqtt_port}")
```

For standalone startup, put the child options under
`config :graviton_mq, :default_instance, ...` and let its Application start the
default instance. Omit `runtime: false` in that mode. For a host-only broker,
`listeners: []` keeps the MQTT aggregate without a network listener.
`host_principal` enables local host operations; omitting it disables them.

## Host API

Continue in the same live process as the broker example. `owner` is the
internal MQTT owner returned by status, not the public broker supervisor PID.
This path is useful for host integration; it does not replace a network client.

```elixir
await_host = fn reference ->
  receive do
    {:graviton_mq, ^owner, {:operation, ^reference, {:ok, result}}} -> result
    {:graviton_mq, ^owner, {:operation, ^reference, result}} ->
      raise "Host operation did not succeed: #{inspect(result)}"
  after
    10_000 -> raise "No host completion observed; outcome is unconfirmed"
  end
end

{:ok, subscribed} = GravitonMQ.subscribe(MyApp.MQTTBroker, "events/#", qos: 1)
subscription = await_host.(subscribed)
:ok = GravitonMQ.request(MyApp.MQTTBroker, subscription, 1)
{:ok, published} = GravitonMQ.publish(MyApp.MQTTBroker, "events/host", "hello", qos: 1)
:accepted_in_memory = await_host.(published)

receive do
  {:graviton_mq, ^owner, {:delivery, ^subscription, delivery, %{payload: "hello"}}} ->
    :ok = GravitonMQ.ack(MyApp.MQTTBroker, delivery)
after
  10_000 -> raise "Expected host delivery was not observed"
end

:ok = GravitonMQ.unsubscribe(MyApp.MQTTBroker, subscription)
```

Request more demand for subsequent deliveries. The calling process owns its
subscriptions; death removes them. Stop `broker_supervisor` when the example
is finished. With persistence enabled, publication completion changes to
`{:accepted_durable, commit_ref}`; the memory assertion above then needs that
selected result instead.

The broker supports sessions, retained messages, Will/delay, expiry and
QoS0/1/2. `maximum_qos: 0` or `1` advertises a lower capability; the default
supports 2. Topic aliases, shared subscriptions and Subscription Identifiers
are negotiated off. MQTT3.1.1 CONNECT/CONNACK codec compatibility does not
enable a legacy runtime. See the [released API guide](https://github.com/gsmlg-dev/graviton_mq/blob/v0.2.0/docs/MQTT_BROKER.md)
for additional options; these examples use published version 0.2.0.
