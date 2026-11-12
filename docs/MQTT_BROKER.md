# MQTT5 single-node broker

The G2 broker runs in this project, with pure topic/session/routing transitions
and OTP shells over `hell`/`ex_ssl` TCP/TLS listeners. It does not depend on `graviton_mq_client` or an external
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
ACK. Reopening validates the durable prefix, truncates only an incomplete or
invalid tail beyond it, and resynchronizes possible completed unacknowledged
records; those records may survive without proving a previous successful result.
A log with no checkpoint that holds at most a (partial) header is reinitialized;
any other log without a checkpoint is an error.

Status reports `profile: :local_durable`, `storage` and `durable_through`.
Snapshot bytes, 500,000 terms, depth 64 and the total log size bound admission.
Since 2026-11-08 the WAL is compacted to the latest committed snapshot after a
commit or on open when the log is at least half of `max_log_bytes` and at least
twice its compacted base, and once (then retried) when an append is rejected by
the log limit. `max_log_bytes` is therefore no longer a lifetime cap, but one
compacted snapshot plus the next one must still fit; otherwise the commit fails
explicitly. A failed or uncertain compaction poisons storage like any other
write failure. See [ADR 0008](adr/0008-local-durability-profiles.md) for the
commit design. Evidence: `apps/graviton_mq_runtime/test/mqtt/persistence_test.exs`
and the storage WAL compaction/crash/fault tests, which are Linux-only and
passed for `696dca0` on a local NixOS Linux host on 2026-11-08. The bounded
[G6 local durability exit](qualification/2026-10-30-g6-local-durability.md)
passed its integrated gates and installed recovery consumers. Subsequent
selected-profile G5/G7 acceptance is recorded in the matrix and G7 record.

Upgrade note (S4): the broker writes snapshot version 2 (Subscription
Identifiers, shared memberships and shared delivery entries) under the same WAL
record type. Version 1 data is upgraded in memory on recovery and the next
commit writes version 2; no offline migration is needed and there is no
downgrade. A release before S4 cannot read version 2 and fails closed: its
version check returns `:invalid_broker_checkpoint`, or its bounded term reader
returns `:invalid_checkpoint_term` when the snapshot carries atoms that release
does not know (for example queued deliveries with Subscription Identifiers);
keep a copy
of the data directory if a rollback may be needed. Restarting with
`shared_subscriptions: false` or `subscription_identifiers: false` while the
snapshot holds that state stops startup with `:checkpoint_capability_mismatch`
instead of dropping it. `GravitonMQ.start_link/1` returns the owner's reason
nested in the supervisors' standard start chain (ExUnit's `start_supervised/2`
wraps it once more as `{reason, child_info}`):

```elixir
{:error,
 {:shutdown,
  {:failed_to_start_child, GravitonMQ.Runtime.Supervisor,
   {:shutdown,
    {:failed_to_start_child, GravitonMQ.Runtime.MQTT.Supervisor,
     {:shutdown,
      {:failed_to_start_child, GravitonMQ.Runtime.MQTT.Broker,
       :checkpoint_capability_mismatch}}}}}}}
```

The refused start leaves durable state intact; re-enable the feature to start.

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
broker explicitly.

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

TLS listeners (S1b, 2026-11-13, unreleased) accept through `hell` with
`ex_ssl`; the runtime neither references nor starts OTP `:ssl`, and the host
does not need to start it. They negotiate **TLS 1.3 only**: TLS 1.2 and older
clients are refused during the handshake. `tls_options` are validated against
an allowlist before any socket opens:

| Key | Accepted value |
| --- | --- |
| `certfile`, `keyfile`, `cacertfile` | non-empty binary or charlist path |
| `cert` | DER certificate |
| `key` | in-memory private key `{:RSAPrivateKey \| :ECPrivateKey \| :PrivateKeyInfo, der}` |
| `certs_keys` | 1..16 identity maps, each with its own certificate and private key (`cert`, `certfile`, `key`, `keyfile`, `password`) |
| `password` | binary, charlist or 0-arity fun; always passed to Hell as a closure |
| `cacerts` | non-empty list of DER certificates |
| `verify` | `:verify_none` or `:verify_peer`; `:verify_peer` requires `cacerts` or `cacertfile` |
| `fail_if_no_peer_cert` | boolean, only with `verify: :verify_peer` |
| `depth` | 1..100 |
| `ciphers` | non-empty, duplicate-free TLS 1.3 suites (`TLS_AES_128_GCM_SHA256`, `TLS_AES_256_GCM_SHA384`, `TLS_CHACHA20_POLY1305_SHA256`) |
| `versions` | exactly `[:"tlsv1.3"]` |
| `alpn_preferred_protocols` | non-empty, duplicate-free list of 1..255-byte binaries |
| `sni_hosts`, `sni_fun` | per-name identities or a 1-arity fun (not both); the broker does not route on them |

A default identity is required: a certificate (`certfile` or `cert`) with a
private key (`keyfile` or `key`), or `certs_keys`. Any other key fails start
with `{:unsupported_tls_option, key}`, including OTP-only options
(`verify_fun`, `partial_chain`, `crl_check`, `reuse_sessions`,
`session_tickets`, `secure_renegotiate`, `honor_cipher_order`) and socket
options. `active_chunk_size` is owned by the runtime and rejected from users:
the runtime sets 16,384 bytes per active-once delivery, so the
`max_packet_size` input bound behaves as on TCP. A malformed value, a missing
identity or a dependent key without its prerequisite fails with
`{:invalid_tls_option, key}`. Errors carry the key only, never a value. An
unreadable file, a bad key or a wrong password fails start with
`{:listener_setup_failed, :tls}`.

There is no TLS session resumption (every connection performs a full
handshake) and no client-certificate revocation checking. `connect_timeout`
bounds the TLS handshake, including the server flight and `sni_fun`; the
CONNECT deadline starts after it, so a client gets at most
2 × `connect_timeout` for TLS plus CONNECT. Concurrent TLS connections per
listener, handshakes included, are bounded at `max_connections + 3`. A failed
handshake closes only that socket and is not reported to the Broker. A TLS
listener failure closes that listener's live connections, as for TCP; the
Broker, its sessions and retained messages survive.

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

## Topic Alias, Subscription Identifiers and shared subscriptions

| Option | Default | Meaning |
| --- | --- | --- |
| `topic_alias_maximum` | `10` | CONNACK Topic Alias Maximum (0..65,535); every alias 1..N is accepted |
| `max_outbound_topic_aliases` | `0` (off) | Broker cap for aliases it sends, bounded by the client's CONNECT Topic Alias Maximum |
| `max_alias_bytes` | derived | Static bound `max_connections x (topic_alias_maximum + max_outbound_topic_aliases) x min(65_535, max_packet_size)`; an explicit smaller value fails startup |
| `subscription_identifiers` | `true` | Subscription Identifier Available; `false` restores DISCONNECT 0xA1 |
| `shared_subscriptions` | `true` | Shared Subscription Available; `false` restores DISCONNECT 0x9E |

Alias tables belong to one Network Connection and are never persisted. Aliases
resolve after the packet passes the malformed-packet and size checks and before
every PUBLISH-semantic check. An alias above the advertised maximum gets
DISCONNECT 0x94 and an empty topic with an unknown alias 0x82; an alias of 0 is
already rejected by the codec as a malformed packet (DISCONNECT 0x81). Subscribers
receive full topics. Outbound aliases are off by default; when enabled they use
the first N distinct topics per connection and never replace them. A
publication is delivered only if its full-topic form fits the client's Maximum
Packet Size; the aliased form is sent only if it also fits, otherwise the full
topic is sent and no alias is recorded.

Maximum Packet Size limits the wire packet only (MQTT 5.0 §3.1.2.11.4). An
aliased PUBLISH is resolved and stored under the protocol maximum
(268,435,460 bytes), not the broker's receive limit, and each subscriber
receives it only if it fits that subscriber's own limit (otherwise MQTT-3.1.2-25
applies). Only a resolved packet beyond the protocol maximum gets DISCONNECT
0x95. The runtime owner likewise encodes outbound packets under the protocol
maximum, because the pure machine already bounds every outbound packet by the
recipient's Maximum Packet Size.

`max_alias_bytes` is a static bound checked at start, not an allocation, and an
alias is never refused because of it (MQTT-3.3.2-12). When omitted it is
derived from the other limits (128 x 10 x 65,535 = 83,884,800 bytes with the
defaults), so configurations written before Topic Alias support still start.
An explicit value below that worst case fails startup with
`:alias_bytes_exceeded`. Upgrade note: the derived value always equals the
worst case, so the bound only guards memory when you set it explicitly.

One SUBSCRIBE Subscription Identifier applies to every filter in it; a
re-SUBSCRIBE replaces it. A forwarded copy carries the identifiers of all
matching subscriptions (repeated identifiers stay repeated); a shared copy
carries only the receiving member's identifier. Retained messages sent on
subscribe carry that subscription's identifier. Retransmissions keep the
identifiers captured when the message was queued.

`$share/{group}/{filter}` groups are broker-wide and derived from session
memberships; persistent sessions keep their memberships across restarts.
Each matching message goes to one member per group, bound at publication:
deterministic, quota-aware round-robin over Client Identifiers after a
transient cursor, preferring connected members that fit and have send quota,
then connected members that fit, then (QoS>0) offline persistent members
within the offline limits. A QoS0 copy that no connected member can take is
discarded. When the only candidates are offline members whose offline limits
are full, the publication is refused with 0x97, as for an ordinary
subscription. Authorization runs asynchronously, so at commit time a binding
that has gone stale (session ended, member left or re-subscribed, Clean
Start, connected member gone offline or now too small) is re-selected among
the current members rather than dropped after PUBACK. At dispatch, a
never-sent copy that no longer fits its member's Maximum Packet Size (for
example after a resume with a smaller limit) is offered to the other members
and dropped only if none fits.

Redistribution follows MQTT 5.0 §4.8.2. On connection loss, the member's
never-sent entries move to a connected member; entries no connected member
can take stay with the member, and already-sent entries stay to resume with
it. On session end, QoS1 entries and never-sent QoS2 entries move (to an
offline member too, within the offline limits); already-sent QoS2 entries are
discarded (MQTT-4.8.2-5). On UNSUBSCRIBE of the shared filter, that filter's
never-sent entries move. A moved entry is appended to its new member's queue,
is authorized for that member at dispatch, and is sent after the member's
earlier entries; moved entries keep their relative order. Each member
receives its own entries in publication order; there is no ordering across
members (MQTT-4.6.0-6 covers non-shared subscriptions only). Moves triggered by
UNSUBSCRIBE dispatch immediately; those triggered by connection loss or
session end are dispatched by the next pump, at the latest on the next tick.

Shared subscriptions never receive retained messages. Authorization sees the
full `$share/...` filter on subscribe and the chosen member's concrete topic on
delivery; a `:deliver` denial drops that group copy without fallback. No Local
on a shared filter gets DISCONNECT 0x82. The host API (`GravitonMQ.subscribe/3`)
does not join shared groups and has no Subscription Identifiers.

`GravitonMQ.status/1` adds:

- `shared_groups`: distinct shared filters with at least one member session;
- `shared_discarded`: shared copies dropped because no member could take them
  (at publication, at commit-time re-selection, on redistribution, or when a
  never-sent oversized copy fits no other member). It does not count
  `:deliver` denials, expired messages, an already-sent oversized copy
  completed under MQTT-3.1.2-25, or sent QoS2 entries discarded at session end;
- `shared_qos2_discarded`: already-sent QoS2 shared entries discarded at
  session end (MQTT-4.8.2-5);
- `topic_aliases: %{inbound: n, outbound: n}`: alias entries currently held
  across live connections.

The two discard counters are transient since the owner started; they are not
checkpointed.

## Negotiation, limits and failure

CONNACK supports QoS2 by omitting Maximum QoS, whose MQTT5 default is 2.
`:maximum_qos` can select 0 or 1 explicitly; the historical G2 QoS1 profile is
still tested. Retain, wildcards, shared subscriptions and Subscription
Identifiers are available and inbound Topic Alias Maximum is 10 by default.
Publications/Wills above the configured QoS, enhanced authentication and
unsupported property use follow explicit rejection paths.
Subscriptions receive the supported grant. MQTT3.1.1 remains CONNECT/CONNACK codec-only;
this runtime requires MQTT5.

Packets a client pipelines after CONNECT are held unprocessed while CONNECT
authentication or Will authorization is outstanding. They are replayed in
arrival order after the successful CONNACK and discarded with the connection
if CONNECT is rejected (MQTT-3.1.4-6); a held second CONNECT is still a protocol
error on replay. The hold is bounded per connection by Receive Maximum packets
and Maximum Packet Size total bytes, and across connections by
`max_incoming_bytes`. Overflow refuses CONNECT with 0x97 (Quota exceeded),
never a DISCONNECT before a successful CONNACK.

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

Takeover by the same principal closes the old Network Connection first (0x8E).
Behaviour change on 2026-11-08: when the taken-over session has Session Expiry
0, that loss ends it; the old Will becomes due under the ordinary
min(Will Delay, Session Expiry) rule, and the new connection receives
Session Present 0 with a fresh session. Previously that session was resumed.
With a nonzero expiry, the post-disconnect session state is resumed. A refused
CONNECT (principal mismatch or session limit) leaves the current owner
undisturbed. Evidence: `broker_connect_hold_test.exs` and `broker_test.exs`;
independent-peer requalification is pending.

Count/byte limits independently bound connections, sessions, subscriptions,
retained data, Wills, prepared operations, incoming/pending/offline deliveries,
inflight IDs, parser buffers and writes. Host demand and outstanding deliveries
have separate limits. Default packet maximum is 1 MiB, receive maximum and
outgoing inflight are 16, auth concurrency is 8, callback deadline is 1 second,
CONNECT timeout is 3 seconds, and socket send timeout is 1 second.
`GravitonMQ.status/1` reports current finite machine limits and counts.

Each instance has a supervised owner for the protocol aggregate, independent
logical session records, temporary connection readers and bounded writers.
Connection/listener failure preserves the owner. Since 2026-11-12
([ADR 0009](adr/0009-owned-network-stack.md), unreleased) a plain-TCP
listener failure closes that listener's connections; other listeners and
their connections are unaffected. In the default memory profile,
owner/VM failure loses memory
sessions, retained data and inflight responsibility; supervised restart closes
old readers/writers before the replacement serves traffic. This is tested
cleanup, not recovered delivery or durability. The opt-in local persistence
profile above restores session/retained/Will/inflight responsibility through
actual WAL recovery. The [requirement inventory](qualification/2026-10-30-mqtt5-requirement-inventory.md)
and [G7 local record](qualification/2026-10-30-g7-local-qualification.md)
record applicable clauses, resource behavior, sustained operation and their
exact qualification limits.
