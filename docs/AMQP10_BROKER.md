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
uses protocol ID 0 with optional SASL (below), non-dynamic termini, format-zero
messages, unsettled senders and receiver-first settlement. Unsupported termini, copy
distribution, required capabilities and settlement modes are rejected. Unknown
addresses receive an error Detach after a null Attach. There are no implicit
queues or successful placeholder operations.

`auth` is mandatory and explicit: `:anonymous` or `{module, options}` with the
`GravitonMQ.Runtime.AMQP10.Auth` behaviour (`authenticate(credentials,
options)`). The optional `authorize/3` callback receives the connection's
principal, the peer action `:send` or `:receive`, and the configured address.
Only `:allow` admits the Link. Tasks, concurrency and deadlines are bounded;
denial, timeout and callback exceptions grant no Link or credit. Authorization
applies to the Link lifetime. ACL revalidation on an already admitted Link is
unavailable.

### SASL (S2, 2026-11-12, unreleased)

SASL (AMQP 1.0 Part 5 §5.3) supports ANONYMOUS and PLAIN only, without a
security layer, over TCP and implicit-TLS listeners.

| Option | Default | Meaning |
| --- | --- | --- |
| `sasl` | `:optional` under `auth: :anonymous`; `:required` under `{module, options}` | `:optional` accepts peers with or without SASL; `:required` answers a no-SASL header with `AMQP 3 1 0 0` and closes; `:disabled` answers a SASL header with `AMQP 0 1 0 0` and closes |
| `sasl_mechanisms` | `["ANONYMOUS"]` / `["PLAIN"]` | Ordered, duplicate-free subset of `"PLAIN"` and `"ANONYMOUS"` |
| `allow_insecure_plain` | `false` | Must be `true` to offer PLAIN while any listener uses `transport: :tcp` |
| `sasl_timeout` | 5,000 ms (1..10,000) | Bounds the header wait, the exchange and the post-outcome AMQP header |
| `max_sasl_jobs` | 16 (1..128) | Concurrent authentication callbacks, separate from `max_auth_jobs` |
| `auth_timeout` | 2,000 ms (1..3,000) | Existing Link-authorization bound, also applied to each authentication callback |

Start fails with `:sasl_required_for_authentication` for a module policy with
`sasl: :optional | :disabled` (no unauthenticated bypass),
`:plain_requires_authentication` for `"PLAIN"` under `auth: :anonymous`,
`:plain_requires_tls` for PLAIN on a TCP listener without the opt-in, and
`:invalid_sasl_mechanisms` for an empty, duplicate or unknown mechanism list.

Under `auth: :anonymous`, no-SASL and SASL ANONYMOUS peers get principal
`:anonymous`; no callback runs, and PLAIN is not offered. A module policy
invokes `authenticate/2` for PLAIN and for a listed `"ANONYMOUS"`
(`mechanism: :anonymous`, with the optional trace). Credentials carry
`mechanism`, `username`, `password`, `authzid`, `trace`, `hostname` and
`peer: %{ip, port, transport, certificate, certificate_verified}`; the callback
owns verification (constant-time comparison) and the `authzid` policy. Its
`{:ok, principal}` becomes the first argument of `authorize/3` and is retained
for the connection lifetime, so it must not contain secrets. Until a principal
is reported, Link admission is denied without calling `authorize/3`;
`:unauthenticated` is reserved for that state, always denied, and refused at
SASL (`sys`) if a callback returns it.

| Callback result or peer input | sasl-outcome |
| --- | --- |
| `{:ok, principal}` | `ok`, then the AMQP header exchange and Open |
| `{:error, _}`, unoffered mechanism, invalid PLAIN/ANONYMOUS message | `auth` |
| Invalid return, exception or exit | `sys` |
| `auth_timeout` deadline, exhausted `max_sasl_jobs`, unavailable Authenticator | `sys-temp` |

A failure outcome is flushed and the socket is closed. Protocol violations
(out-of-order or malformed body, frame above 512 octets, empty frame, second
sasl-init, a sasl-response without a challenge) and the `sasl_timeout`
deadline close without an outcome. Callbacks run in sensitive tasks under a
dedicated `:sasl_tasks` supervisor started by the policy-holding
`Authenticator`; transport loss kills a running callback and late results are
ignored. Passwords, usernames, authzid, initial responses and policy options
are never logged, returned, put into status, published in the Listener's
connection specification or stored in the Broker (Manager) state. Privacy
tests assert this for the password and the policy options; usernames and
authzid rely on the redacting `Inspect` of the SASL structs. `amqp10_status/1` adds `authenticating` (count) and `sasl_failures` by code
(`auth`, `sys`, `sys_perm`, `sys_temp`). EXTERNAL, SCRAM, other mechanisms,
SASL security layers and the TLS-upgrade header (protocol ID 2) are not
implemented; credential storage, lockout and rate limiting belong to the host.

Upgrade note (S2): the broker now always waits for the client's protocol
header before sending its own, in every `sasl` mode (previously it sent
`AMQP 0 1 0 0` and Open immediately after accept). A silent peer is closed by
`sasl_timeout`, also under `sasl: :disabled`, instead of the Open deadline. A
malformed or unsupported header receives the supported header and a socket
close, but no AMQP Close frame, because the Endpoint is not started. Existing
`auth: :anonymous` configurations need no change: no-SASL peers still connect
and `authorize/3` still receives `:anonymous`; peers that default to SASL
ANONYMOUS, such as Proton with `sasl_enabled=True`, now connect too.

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
serializes final known outcomes before processing Connection DOWN; each queue
monitors a per-queue lease anchor that lives exactly as long as that settlement
coordinator, rather than racing its own independent Connection DOWN against a
final outcome. Manager failure closes old sockets and returns its leases in
surviving queues. Queue failure loses that queue's memory state and restarts
dependent owners. VM/process memory loss has no WAL or restart recovery in G4.
Interrupted possible publications can have an unknown outcome; the client must
not infer successful disk storage or retry with an exactly-once claim.

Since the 2026-11-08 maintenance changes, an unanswered or uncertain lease
command (register, claim, settle or disconnect) or status/watch call fences
only that queue instead of stopping the Manager. The Manager kills that queue's lease
anchor, so the queue returns every claim held there when it next runs; it
detaches the queue's attached Links with `amqp:internal-error` "queue is
unavailable", refuses new attaches with the same error, and reports the queue as
`:unavailable` in `amqp10_status/1`. An enqueue whose outcome is unknown (call
timeout or storage uncertainty) only closes the publishing Connection without a
disposition and does not fence: enqueue holds no lease, so no claim state
diverges; a truly unresponsive queue is fenced by its next status or lease
command, and a queue whose storage failed stays observable as
`storage: :failed`. The fenced queue is probed every second outside the Manager and restored when it answers. Messages stay in the queue
process. Remaining limit: the first unanswered call still blocks the Manager
for the queue's internal 5-second call timeout. An actual queue crash still
restarts later owners as above. Evidence: `amqp10_availability_test.exs` and
`amqp10_boundaries_test.exs`; independent-peer requalification of the
unknown-enqueue change passed its CI interoperability run (42501601934).

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
Only an incomplete or invalid tail beyond the confirmed prefix is truncated; a
log with no checkpoint that holds at most a (partial) header is reinitialized.
Durable-prefix corruption, any other log without a checkpoint, or an
incompatible queue identity/configuration rejects startup.

`:max_record_bytes` defaults to 1 MiB and `:max_log_bytes` to 1 GiB per queue.
They are finite admission bounds; size a record for the logical event framing
as well as the message bytes. Since 2026-11-08 the queue WAL is compacted, so
`:max_log_bytes` is no longer a lifetime cap. Before an append, when the log is
at least half its limit and at least twice its compacted base, on open, and
once after an append rejected by the log limit, the durable machine state is
rewritten as the identity event followed by `queue_snapshot/v1` chunk events
(deterministic ETF, decoded with `:safe`, compressed ETF refused). Replay
restores the exact state and counters. The compacted live state must still fit
the limit; otherwise admission fails closed, and a failed compaction poisons
the queue. See [ADR 0008](adr/0008-local-durability-profiles.md) for the commit
design. Evidence: `queue_compaction_test.exs` (platform-independent) and
`durable_queue_test.exs` plus the storage WAL tests, which are Linux-only and
passed for `696dca0` on a local NixOS Linux host on 2026-11-08. TTL/absolute expiry, dead-letter actions, distributed
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
lease cleanup are finite; an uncertain local queue cleanup fences that queue
(above), whose killed lease anchor releases the leases, so none remain orphaned.
A Connection that misses the 25 ms capacity query is skipped for that pump turn
and retried after 50 ms; it is not closed. Socket input is event-driven with
`active: :once`, re-armed only after bounded input drains, with no periodic
polling (`connection_input_test.exs`).

The shared decoder bounds zero-width array elements (constructors `0x41`–`0x45`)
by `Codec.Limits.max_zero_width_elements` (default 65,536), cumulatively per
top-level decode (one performative) and shared across all sections of one
message, one annotation check and one raw composite-field walk;
excess fails with `:zero_width_element_limit` before allocation. Begin accepts
any uint incoming/outgoing window. After a clean local Detach, in-flight peer
Flow/Transfer for that handle is tolerated until the peer's Detach; such a
Transfer still counts against the Session window and delivery-id sequence.
After an error Detach, errant-link handling is unchanged.

TCP listeners default to loopback and port zero. TLS is opt-in:

```elixir
listeners: [[
  transport: :tls,
  ip: {127, 0, 0, 1},
  port: 5671,
  tls_options: [certfile: ~c"server.pem", keyfile: ~c"server.key"]
]]
```

TLS options cannot replace transport-owned binary/active-once framing or write
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
