# AMQP 1.0 integration

Use `graviton_mq_client` for outgoing connections and `graviton_mq` for a
broker. This is AMQP 1.0, with sessions, links, credit and outcomes. An AMQP
0-9-1 library or exchange/binding API cannot drive it. The 0.2.0 profile uses
raw AMQP without SASL; check [operations](operations.md#supported-profile)
before choosing it for an existing broker's authentication requirements.

## Network client

Run this bounded script in a consumer with
`{:graviton_mq_client, "~> 0.2.0"}`. `AMQP_HOST`/`AMQP_PORT` select a broker
that accepts no-SASL AMQP and has an `orders` address. Graviton's broker needs
that queue preconfigured; an Artemis test peer can create it under its own
policy. Keep the observer alive in an application service and handle these
events in that process.

```elixir
alias GravitonMQ.AMQP10.{Outcome, Value}
alias GravitonMQ.AMQP10.Codec.Message, as: MessageCodec
alias GravitonMQ.AMQP10Client, as: Client

{:ok, supervisor} =
  Supervisor.start_link(
    [{Client,
      observer: self(),
      host: System.get_env("AMQP_HOST", "127.0.0.1"),
      port: String.to_integer(System.get_env("AMQP_PORT", "5672")),
      container_id: "graviton-example-amqp"}],
    strategy: :one_for_one
  )

[{_, client, _, _}] = Supervisor.which_children(supervisor)

receive do
  {:graviton_mq, ^client, {:connected, _remote_container_id}} -> :ok
after
  10_000 -> raise "AMQP connection was not established"
end

await = fn reference ->
  receive do
    {:graviton_mq, ^client, {:operation, ^reference, {:ok, result}}} -> result
    {:graviton_mq, ^client, {:operation, ^reference, result}} ->
      raise "AMQP operation did not succeed: #{inspect(result)}"
  after
    10_000 -> raise "No AMQP completion observed; outcome is unconfirmed"
  end
end

{:ok, opened} = Client.open_session(client, 1)
1 = await.(opened)
receiver = {Value.string("orders-in"), :receiver}
sender = {Value.string("orders-out"), :sender}

{:ok, attached} = Client.attach(client, 1, "orders-in", :receiver,
  address: Value.string("orders"), max_message_bytes: 65_536)
^receiver = await.(attached)
{:ok, attached} = Client.attach(client, 1, "orders-out", :sender,
  address: Value.string("orders"), max_message_bytes: 65_536)
^sender = await.(attached)

{:ok, requested} = Client.request(client, 1, receiver, 1, 65_536)
1 = await.(requested)
{:ok, encoded} = MessageCodec.encode_sections([{:data, Value.binary("hello")}])
{:ok, published} = Client.send_message(client, 1, sender, encoded)
%Outcome.Accepted{} = await.(published)

delivery =
  receive do
    {:graviton_mq, ^client, {:delivery, reference, 1, ^receiver, raw, %{message_format: 0}}} ->
      {:ok, message, <<>>} = MessageCodec.decode(raw)
      true = {:data, Value.binary("hello")} in message.sections
      IO.inspect(message.sections)
      reference
  after
    10_000 -> raise "Expected AMQP delivery was not observed"
  end

{:ok, settled} = Client.settle(client, delivery, %Outcome.Accepted{})
%Outcome.Accepted{} = await.(settled)
{:ok, closed} = Client.close(client)
:closed = await.(closed)
:ok = Supervisor.stop(supervisor)
```

The link identity is `{Value.string(link_name), local_role}`, scoped to the
session. `attach/5` takes an ordinary binary link name and a tagged AMQP
address. Do not substitute remote handles, channels or delivery IDs for host
identities; the client allocates local and remote protocol indexes separately.

`request/5` reserves a finite number of slots and the worst-case message-byte
budget before granting credit. Request again for subsequent messages after
settlement. A zero-credit receiver should not be expected to receive data.
Keep the opaque delivery reference from the event for `settle/3`.

| Publication completion | Meaning |
| --- | --- |
| `{:ok, %Outcome.Accepted{}}` | Peer accepted responsibility under its selected policy |
| `{:ok, %Outcome.Released{}}` | Peer released it; not successful acceptance |
| `{:ok, %Outcome.Rejected{}}` | Rejection with protocol error data |
| `{:ok, %Outcome.Modified{}}` | Modified release; retain delivery-failed/undeliverable-here/annotation data |
| `{:error, reason}` | Definite local/terminal failure, subject to the known-outcome case below |
| `{:unknown, reason}` | Possibly emitted publication interrupted; acceptance is unknown |
| `{:error, {:outcome_known, outcome, reason}}` | Outcome is known, but completing settlement failed |

`Received` progress and successful socket writes do not complete publication.
Keep the original encoded message when forwarding; decode only the sections
needed by the application. Preserve semantic tags such as binary/string/symbol
and signed/unsigned integers. Body terms alone cannot replace encoded sections.

## Broker

Use `{:graviton_mq, "~> 0.2.0", runtime: false}` for this embedded example.
It opens an ephemeral loopback listener. Choose a service port such as 5672
or a TLS listener deliberately when adapting it.

```elixir
{:ok, broker_supervisor} =
  Supervisor.start_link(
    [{GravitonMQ,
      name: MyApp.AMQPBroker,
      runtime_supervisor_name: MyApp.AMQPBrokerRuntime,
      amqp10: [
        container_id: "orders-broker",
        auth: :anonymous,
        queues: ["orders"],
        authorize: fn
          :anonymous, action, "orders" when action in [:send, :receive] -> :allow
          _, _, _ -> :deny
        end,
        queue_options: [max_messages: 10_000, max_bytes: 67_108_864],
        listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
      ]}],
    strategy: :one_for_one
  )

{:ok, %{listeners: [%{port: amqp_port}]}} =
  GravitonMQ.amqp10_status(MyApp.AMQPBroker)
IO.puts("AMQP1 listener: 127.0.0.1:#{amqp_port}")
```

Stop `broker_supervisor` when the example is finished. Standalone operation
uses the same options under `config :graviton_mq, :default_instance, ...`,
with the normal broker dependency instead of `runtime: false`.

Queues are named, preconfigured move queues. Unknown addresses fail; there
are no implicit queues, exchanges, bindings or topic bridge. Use an actual
AMQP network client to publish/receive; `GravitonMQ.publish/4` is the MQTT host
API, not an AMQP publication API. `amqp10_status/1` is observation only.

`auth: :anonymous` is the only implemented authentication mode. The optional
`authorize/3` function admits or denies `:send`/`:receive` Links for a configured
address; it does not authenticate a user. Only `:allow` grants access.
TLS does not add SASL or automatically map a client certificate to a principal.

In memory mode, durable-header messages are refused. Non-durable Accepted
means memory responsibility. A consumer's Accepted retires a claim, Released
requeues it, Rejected discards it under this profile, and Modified preserves
its release data. Unresolved claims are released on disconnect. Local durable
queues require the explicit [persistence options](operations.md#local-durability);
durable termini and client resumption remain separate unsupported capabilities.

See the [released client guide](https://github.com/gsmlg-dev/graviton_mq/blob/v0.2.0/docs/AMQP10_CLIENT.md)
and [broker guide](https://github.com/gsmlg-dev/graviton_mq/blob/v0.2.0/docs/AMQP10_BROKER.md)
for additional bounds and policy details.
