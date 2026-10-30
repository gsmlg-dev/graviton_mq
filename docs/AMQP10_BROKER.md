# AMQP 1.0 single-node broker

The initial G4 broker uses the same standalone and embedded lifecycle as MQTT.
It is explicitly enabled; the default instance opens no listener. The
[capability matrix](CAPABILITIES.md) records qualification and remaining stages.

```elixir
children = [
  {GravitonMQ,
   name: :messages,
   runtime_supervisor_name: :messages_runtime,
   amqp10: [
     container_id: "orders-broker",
     auth: :anonymous,
     queues: ["orders", "audit"],
     authorize: fn
       :anonymous, :send, "orders" -> :allow
       :anonymous, :receive, "orders" -> :allow
       _, _, _ -> :deny
     end,
     queue_options: [max_messages: 10_000, max_bytes: 67_108_864],
     listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 5672]]
   ]}
]
Supervisor.start_link(children, strategy: :one_for_one)
```

Hosts use `runtime: false` for the broker dependency and supervise this child.
Standalone `GravitonMQ.Application` passes its `:default_instance` configuration
to the same API. `:name` and `:runtime_supervisor_name` are exact OTP names;
two named instances need distinct pairs. No atoms are derived from addresses.
MQTT and AMQP can be explicitly configured on the same instance and retain
separate queues, topic/session state, listeners and running owners. There is
no cross-protocol bridge.

`GravitonMQ.amqp10_status(instance)` returns `{:ok, status}`, with `:profile`,
`:listeners`, connection/link/auth/operation counts and queue capacity counts.
It returns `{:error, :amqp10_not_enabled}` for an instance without AMQP.
This is a local observation API; use an actual AMQP network client to publish
or receive. The independent Elixir client is described in
[AMQP10_CLIENT.md](AMQP10_CLIENT.md). Installing a client never starts a broker.

## Profile and responsibility

Only named, preconfigured move queues are supported. The default memory profile
uses protocol ID 0 (no SASL), non-dynamic termini, format-zero messages,
unsettled senders and receiver-first settlement. Unsupported termini, copy
distribution, required capabilities and settlement modes are rejected. Unknown
addresses receive an error Detach after a null Attach. There are no implicit
queues or successful placeholder operations.

`auth: :anonymous` is mandatory and explicit. The optional `authorize/3`
callback receives `:anonymous`, the peer action `:send` or `:receive`, and the
configured address. Only `:allow` admits the Link. Tasks, concurrency and
deadlines are bounded; denial, timeout and callback exceptions grant no Link
or credit. Authorization applies to the Link lifetime. SASL, enhanced identity
providers and ACL revalidation on an already admitted Link are unavailable.

The default broker sends Accepted after the queue takes memory responsibility.
It rejects durable messages in memory mode. Both profiles reject Header TTL
and absolute-expiry-time with an explicit precondition error while expiry is
unimplemented. Memory Accepted is neither disk durability nor business processing.
Accepted by a receiving peer retires responsibility; Released requeues;
Rejected discards under this profile. Dead-letter action is unsupported.
Modified retains ordered annotation deltas, delivery-failed and per-consumer
undeliverable-here exclusions. Exclusions end with that consumer lease.

Authoritative encoded message content remains unchanged in the queue.
Outbound views remove delivery annotations, update Header delivery-count for
delivery-failed attempts, clear an explicit first-acquirer claim and merge
Modified message annotations. The conservative false first-acquirer value
allows acquisition by another Link without promising otherwise. Original bare
message bytes, including noncanonical encodings, are retained verbatim.

Connection/Session/Link closure returns unresolved claims. The instance Manager
serializes final known outcomes before processing Connection DOWN; queues
monitor that settlement coordinator rather than racing their own independent
Connection DOWN against a final outcome. Manager failure closes old sockets
and returns its leases in surviving queues. Queue failure loses that queue's
memory state and restarts dependent owners. VM/process memory loss has no
WAL or restart recovery in G4. Interrupted possible publications can have an
unknown outcome; the client must not infer successful disk storage or retry
with an exactly-once claim.

## Local durable queues

Add `persistence: [directory: "/var/lib/my-app/amqp", stream_id: "my-app/amqp-primary"]`
inside the `:amqp10` configuration to select `profile: :local_durable`.
The parent must exist. A real opaque WAL at the base directory establishes
exclusive broker ownership and its stable stream identity. Each preconfigured
queue has a hashed child directory and its own logical-event WAL. Directory
aliases and a second broker with a different queue list cannot share ownership.
The profile is local Linux with actual file and directory synchronization.

Durable-header messages are admitted in this mode. Queue transitions append
real protocol-independent Queue.Event values and synchronize their selected
durable-through boundary before delivering claims, notifying watchers or
sending publisher Accepted. The internal queue receipt is
`{:ok, {:accepted_durable, commit_ref}}`. Original encoded messages, stable queue
incarnation/identities and exact retirement/rejection/release/Modified decisions
are retained. Append ordering alone never establishes successful acceptance.

Startup streams and validates history, reconstructs bounded queue state and
releases lost consumer leases through new synchronized events before serving
traffic. A broker owner or whole BEAM failure can therefore redeliver an
unsettled claim without losing authoritative bytes or Modified failure counts
and annotation deltas. Unknown writes/synchronization poison the queue and
suppress successful effects; a timed-out possible operation remains unknown.
Only incomplete tails beyond the confirmed prefix are repaired. Durable-prefix
corruption or an incompatible queue identity/configuration rejects startup.

`:max_record_bytes` defaults to 1 MiB and `:max_log_bytes` to 1 GiB per queue.
They are finite admission bounds; size a record for the logical event framing
as well as the message bytes. The initial WAL has no rotation/compaction.
Exhaustion fails closed. TTL/absolute expiry, dead-letter actions, distributed
durability and hardware power-loss qualification remain outside this increment.
G6 integrated gates and installed consumers are recorded separately from
focused Proton/process/VM recovery evidence.

## Bounds and transport

Defaults are 64 connections, 16 concurrent authorization tasks and a 2-second
authorization/handshake bound. Each queue allows 10,000 messages, 64 MiB of
logical charged data, a 1 MiB charged message limit, 1,024 consumer leases and
1,024 inflight claims. Charges include content, identities, index and mutation
data. They differ from AMQP wire message-size limits. Small queue limits also
reduce advertised inbound message capacity. Over-capacity admission fails
explicitly; an already queued message that cannot fit a receiving Link remains
queued and that Link receives an error.

Pure Endpoint/SessionEngine limits separately bound sessions, links, windows,
fragments, incoming/outgoing bytes, ledgers, pending operations and reserved
control writes. Running shells bound input bytes, per-turn frames, command/
event handoffs, writer frames/bytes and pump route inspections. Slow or failed
socket writes do not block queue transitions. Local shutdown deadlines and
lease cleanup are finite; an uncertain local queue cleanup stops the Manager
so its monitored leases cannot remain orphaned.

TCP listeners default to loopback and port zero. TLS is opt-in:

```elixir
listeners: [[
  transport: :tls,
  ip: {127, 0, 0, 1},
  port: 5671,
  tls_options: [certfile: ~c"server.pem", keyfile: ~c"server.key"]
]]
```

TLS options cannot replace transport-owned binary/passive framing or write
timeouts. This anonymous profile does not convert a client certificate into
an application principal. Peer trust and hostname verification belong to the
connecting client; the independent Proton tests cover trusted and wrong CA
behavior. Callbacks and message content are excluded from ordinary OTP status,
debug-ring state/events and sanitized failure details.

The [clause inventory](qualification/2026-10-30-amqp10-requirement-inventory.md)
records selected-profile obligations and their source/unit/peer/recovery
evidence. Shared scalar/array wire support and opt-in local persistence have
dedicated tests; see [G6 qualification](qualification/2026-10-30-g6-local-durability.md)
and the [G7 local record](qualification/2026-10-30-g7-local-qualification.md)
for exact source, resource/model/fuzz/soak and installation evidence. Hosted
CI for the uncommitted tree is NOT RUN.
Transactions, clustering, Raft, management UI and bridges are outside the Goal.
