# MQTT5 single-node broker

The G2 broker runs in this project, with pure topic/session/routing transitions
and OTP TCP/TLS shells. It does not depend on `graviton_mq_client` or an external
broker. The [capability matrix](CAPABILITIES.md) and
[G2 qualification](qualification/2026-10-30-g2.md) record exact evidence.
G5 extends this intermediate profile to QoS0/1/2, with Mosquitto and Paho
handshake/reconnect evidence. G6 adds opt-in local persistence below. Memory
acceptance is not disk durability, and QoS2 is no business exactly-once claim.

## Local durability

Omit `:persistence` for the memory profile. A local Linux durable instance uses
the same standalone/embedded API and protocol state machine:

```elixir
mqtt: [
  auth: {MyApp.MQTTPolicy, []},
  host_principal: :host,
  persistence: [
    directory: "/var/lib/my-app/mqtt",
    stream_id: "my-app/mqtt-primary",
    max_snapshot_bytes: 33_554_432,
    max_log_bytes: 1_073_741_824
  ],
  listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 1883]]
]
```

The absolute directory's parent must exist. The owner creates only the leaf,
synchronizes its parent, and holds a kernel-owned inode/device lock across
path aliases, roles and BEAMs. Keep the configured stream identity stable and
give each instance a distinct directory. This profile supports local Linux
filesystems with working file/directory synchronization; distributed filesystems
and hardware power-loss qualification are outside its evidence.

Every changed durable projection is appended as a bounded, versioned opaque
WAL snapshot. WAL synchronization, checkpoint-file synchronization, atomic
checkpoint replacement and directory synchronization finish before the
corresponding wire ACK, forwarding effect or host success runs. MQTT state is
never converted into an AMQP Queue.Event. Host publication completion becomes
`{:ok, {:accepted_durable, commit_ref}}`; the reference correlates with the
stream's durable-through boundary. The memory profile keeps
`{:ok, :accepted_in_memory}`. A durable receipt records broker responsibility
under the selected session/retention policy; it does not make QoS0 delivery or
a transient host subscription survive its owner's lifetime.

Sessions, subscriptions, retained messages, Will jobs, incoming QoS2 markers,
and outgoing QoS1/2 packet IDs/phases survive recovery. Connection handles,
authentication requests/credentials, callbacks, write receipts, host PIDs and
host subscription ownership do not. Authentication principals must be bounded
portable data (prefer stable binaries); PID/ref/function identities reject the
durable transition. Reconnecting clients authenticate again and retain principal
matching. Old host-origin identities cannot collide with newly registered
callers after restart.

Offline Session Expiry, retained/message expiry and scheduled Will deadlines
subtract wall-clock downtime once. A connection that was still online at the
last checkpoint is detected lost on recovery; its offline Session Expiry and
Will delay start at that explicit detection. An already scheduled deadline
never restarts with its original duration. A backward wall-clock reading fails
startup. Recovered Will authorization is retried with its original publication
expiry, and publication dispatch authorizes the recipient again.

On write/sync failure the instance closes network connections, cancels auth
work and stops admitting operations. Status remains available with
`storage: :failed`. A possible write reports `:unknown_outcome`; a pre-append
admission failure reports `:persistence_failed`. Neither produces a successful
ACK. Reopening validates the durable prefix, repairs only an incomplete tail
beyond it, and resynchronizes possible completed unacknowledged records; those
records may survive without proving a previous successful result.

Status reports `profile: :local_durable`, `storage` and `durable_through`.
Snapshot bytes, 500,000 terms, depth 64 and the total log size bound admission.
The first WAL is one bounded append-only file with no rotation/compaction;
exhaustion fails closed and requires operator action. The bounded
[G6 local durability exit](qualification/2026-10-30-g6-local-durability.md)
passed its integrated gates and installed recovery consumers. Subsequent
selected-profile G5/G7 acceptance is recorded in the matrix and G7 record.

## Start and embed

Both lifecycles use `GravitonMQ.start_link/1` and `child_spec/1`. A standalone
application selects its default instance in configuration:

```elixir
config :graviton_mq, :default_instance,
  mqtt: [
    auth: :anonymous,
    host_principal: :local_host,
    listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 1883]]
  ]
```

`:anonymous` is an explicit policy allowing every operation in that instance.
For embedding, set the host dependency's `runtime: false` and supervise the
broker explicitly. The host must start OTP `:ssl` when using TLS, for example
through its own `extra_applications`.

```elixir
children = [
  {GravitonMQ,
   name: :messages,
   runtime_supervisor_name: :messages_runtime,
   mqtt: [
     auth: {MyApp.MQTTPolicy, []},
     host_principal: :host,
     max_connections: 128,
     max_pending_bytes: 8_388_608,
     listeners: [[
       transport: :tls, ip: {127, 0, 0, 1}, port: 8883,
       tls_options: [certfile: ~c"/run/certs/server.pem", keyfile: ~c"/run/certs/server.key"]
     ]]
   ]}
]
Supervisor.start_link(children, strategy: :one_for_one)
```

`MyApp.MQTTPolicy` must implement the callbacks below. Both registered names
must differ between instances. Names are exact OTP names; external strings
are never converted to atoms. Unregistered instances can use `nil` names and
distinct child IDs. `listeners: []` enables host-only operation; omitting
`:mqtt` preserves the historical empty runtime. Port `0` selects an ephemeral
listener; `GravitonMQ.status(instance)` reports its actual address and port.

## Host operations and responsibility

Host APIs use the configured `host_principal`; omitting it disables those
operations. The calling process owns subscriptions and delivery references.

```elixir
{:ok, operation} = GravitonMQ.subscribe(:messages, "sensors/+", qos: 1)

subscription = receive do
  {:graviton_mq, owner, {:operation, ^operation, {:ok, subscription}}} ->
    {owner, subscription}
end

{owner, subscription} = subscription
:ok = GravitonMQ.request(:messages, subscription, 1)
{:ok, publication} = GravitonMQ.publish(:messages, "sensors/temperature", "21", qos: 1)

receive do
  {:graviton_mq, ^owner, {:operation, ^publication, {:ok, :accepted_in_memory}}} -> :ok
end

receive do
  {:graviton_mq, ^owner, {:delivery, ^subscription, delivery, message}} ->
    MyApp.process_message(message)
    :ok = GravitonMQ.ack(:messages, delivery)
end

:ok = GravitonMQ.unsubscribe(:messages, subscription)
```

The initial `{:ok, reference}` admits local work. Its asynchronous operation
message reports completion or a sanitized error. Successful publication
completion commits memory obligations and retained mutation atomically after
authorization; it does not prove subscriber receipt or processing. A host ACK
retires that delivery and adds no demand. Call `request/3` again to receive more.
Subscription options include `:qos`, `:no_local`, `:retain_as_published` and
`:retain_handling`; publication options include `:qos`, `:retain`, `:properties`.
Properties use the codec's ordered keyword representation.

Admission and completion can fail under finite capacity or callback failure.
Call timeout/owner loss may return `{:error, :unknown_outcome}`; it is not a
definite rejection and must not be blindly retried as a new publication.
References from another instance/caller or a replaced owner are rejected.
Caller death removes its subscriptions, queued host deliveries and prepared
operations. The outgoing `GravitonMQ.MQTTClient` has a separate API and lifecycle.

## Authentication and authorization

Implement `GravitonMQ.Runtime.MQTT.Auth`:

- `authenticate(credentials, options)` returns `{:ok, principal}` or `{:error, reason}`.
- `authorize(principal, action, resource, options)` returns `:ok` or `{:error, reason}`.

Actions are `:publish` for an exact topic, `:subscribe` for the entire filter
scope, and `:deliver` for the concrete topic of each recipient. Delivery is
checked again when pending/retained data is dispatched, including after a
session resumes. Explicit denial and callback infrastructure failure are
distinct; the latter cannot silently discard accepted responsibility.

Credentials include Client ID, username/password and peer context. Peer
context contains actual IP/port, transport and optional DER certificate.
`certificate_verified` is true only when a certificate is present and the TLS
handshake verified it under `verify: :verify_peer`. Optional client certificate
mode (`fail_if_no_peer_cert: false`) permits a missing certificate and reports
it as unverified; the policy decides whether to accept that identity.

Callbacks run outside pure transitions and the serial owner, under bounded
concurrency and deadlines. Denial, invalid returns, exceptions, timeout and
exhaustion fail closed without returning credential/callback data. Cancellation
also terminates a callback that traps ordinary exit signals. Authentication
precedes Client-ID takeover; a different principal cannot resume or erase a
retained session by guessing its Client ID.

## Negotiation, limits and failure

CONNACK supports QoS2 by omitting Maximum QoS, whose MQTT5 default is 2.
`:maximum_qos` can select 0 or 1 explicitly; the historical G2 QoS1 profile is
still tested. Retain/wildcards are available, shared subscriptions and
Subscription Identifiers unavailable, and inbound Topic Alias Maximum=0.
Publications/Wills above the configured QoS, enhanced authentication, shared
subscriptions and unsupported property use follow explicit rejection paths.
Subscriptions receive the supported grant. MQTT3.1.1 remains CONNECT/CONNACK codec-only;
this runtime requires MQTT5.

Sessions support Clean Start, Session Present and expiry, offline QoS1/2,
reconnection responsibility, delayed Will cancellation/publication, retained
handling and message expiry. Live routing preserves No Local and Retain As
Published. Received content and properties remain separate from protocol ACKs.
An expired unsent message is removed; already-sent QoS1/2 responsibility remains
until settlement. QoS2 uses receiver method B: atomic memory admission and
forwarding precede PUBREC, with a session-owned identifier marker suppressing
duplicate forwarding until PUBREL. PUBCOMP retires that marker. On reconnect,
outgoing messages retry PUBLISH with DUP before PUBREC, or PUBREL afterwards.
Receive Maximum remains occupied until PUBCOMP; connection quotas reset when
the transport changes. Oversized outbound application messages are discarded
as completed, as required by MQTT-3.1.2-25, without disconnecting the recipient.
A due Will remains owned under route pressure.

Count/byte limits independently bound connections, sessions, subscriptions,
retained data, Wills, prepared operations, incoming/pending/offline deliveries,
inflight IDs, parser buffers and writes. Host demand and outstanding deliveries
have separate limits. Default packet maximum is 1 MiB, receive maximum and
outgoing inflight are 16, auth concurrency is 8, callback deadline is 1 second,
CONNECT timeout is 3 seconds, and socket send timeout is 1 second.
`GravitonMQ.status/1` reports current finite machine limits and counts.

Each instance has a supervised owner for the protocol aggregate, independent
logical session records, temporary connection readers and bounded writers.
Connection/listener failure preserves the owner. In the default memory profile,
owner/VM failure loses memory
sessions, retained data and inflight responsibility; supervised restart closes
old readers/writers before the replacement serves traffic. This is tested
cleanup, not recovered delivery or durability. The opt-in local persistence
profile above restores session/retained/Will/inflight responsibility through
actual WAL recovery. The [requirement inventory](qualification/2026-10-30-mqtt5-requirement-inventory.md)
and [G7 local record](qualification/2026-10-30-g7-local-qualification.md)
record applicable clauses, resource behavior, sustained operation and their
exact qualification limits.
