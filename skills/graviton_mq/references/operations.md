# Transport, policies and recovery

These are the 0.2.0 public configuration shapes. Follow the installed version's
API when it differs. The [support matrix](https://github.com/gsmlg-dev/graviton_mq/blob/4b71fc36de57d8c2a7ab0adc43e20cbfee66c242/docs/CAPABILITIES.md)
and [release qualification](https://github.com/gsmlg-dev/graviton_mq/releases/tag/v0.2.0)
separate implementation, independent-peer evidence, recovery and limitations.

## Supported profile

| Area | Supported / boundary |
| --- | --- |
| MQTT5 | QoS0/1/2, sessions, retained, Will/expiry, bounded operations/deliveries, TCP/TLS |
| MQTT optional features | Topic aliases, shared subscriptions and Subscription Identifiers negotiated off; enhanced authentication refused |
| MQTT3.1.1 | CONNECT/CONNACK codec regressions only; no qualified runtime; MQTT3.1 unsupported |
| AMQP1 | Protocol ID 0 without SASL, format-zero sections, unsettled senders, receiver-first settlement, preconfigured named move queues |
| AMQP optional features | No dynamic/durable termini, filters, Link resumption, transactions, Header TTL or absolute expiry |
| AMQP client recovery | No automatic reconnect/session resume or client process/VM recovery; a new owner is a new connection |
| Broker persistence | Opt-in local Linux durable profiles; memory remains the default |
| Broader services | No clustering/Raft, management UI, cross-protocol bridge, WebSocket or QUIC transport |

If the requested AMQP peer requires SASL credentials, or the application needs
an excluded feature, report that mismatch. Do not fabricate accepted options,
silently downgrade the requirement or suggest a working feature merely
because a codec can encode it.

## TCP, TLS and identities

| Role | TCP convention | TLS convention |
| --- | --- | --- |
| MQTT client/broker | 1883 | 8883 |
| AMQP client/broker | 5672 | 5671 |

Broker listeners are opt-in. Port `0` selects a free local port reported by
`GravitonMQ.status/1` (MQTT) or `GravitonMQ.amqp10_status/1` (AMQP).
Anonymous loopback examples are for local integration. Configure the actual
listener address, transport and access policy before exposing the service.

Both network clients accept:

```elixir
[
  transport: :tls,
  host: "broker.example",
  tls_options: [cacertfile: "/etc/my-app/broker-ca.pem"]
]
```

Specify the protocol's TLS port as well. Client TLS verifies trust and DNS/IP
identity; `verify_none` and disabling SNI are rejected. A tuple IP host is
checked against IP SANs unless an explicit server identity is supplied. Never
fall back to TCP after a failed TLS handshake.

Both broker protocol configurations accept a listener such as:

```elixir
[
  listeners: [[
    transport: :tls,
    ip: {127, 0, 0, 1},
    port: 8883,
    tls_options: [certfile: ~c"/run/certs/server.pem", keyfile: ~c"/run/certs/server.key"]
  ]]
]
```

Use port 5671 for an AMQP service. A host embedding the broker with
`runtime: false` must start OTP `:ssl`, for example in its own
`extra_applications: [:ssl]`. TLS transport does not itself establish an
application principal. Supply secrets through the application's runtime
configuration rather than source files or logs.

## Authentication and authorization

MQTT broker `auth` is explicitly `:anonymous` or `{MyApp.MQTTPolicy, options}`.
A module policy implements `GravitonMQ.Runtime.MQTT.Auth`:

- `authenticate(credentials, options)` → `{:ok, principal}` or `{:error, reason}`.
- `authorize(principal, action, resource, options)` → `:ok` or `{:error, reason}`.

Authorize `:subscribe` against the entire filter scope, `:publish` against
the exact topic, and `:deliver` against the concrete recipient topic.
Callbacks are bounded; exceptions, invalid returns and timeouts fail closed.
For persistence, use a stable portable principal such as a binary, rather
than a PID/reference/function. The configured `host_principal` supplies the
identity for local host APIs.

MQTT clients accept `username:` and `password:` CONNECT options; enhanced
authentication is unavailable. The AMQP broker instead requires
`auth: :anonymous` and optionally
`authorize: fn principal, action, address -> :allow | :deny end` for the lifetime
of a Link. Do not reuse the MQTT policy tuple or return contract as an AMQP
authentication implementation.

## Reconnect and bounded ownership

MQTT client retry is finite and defaults to zero attempts. A policy can set:

```elixir
[
  clean_start: false,
  session_expiry_interval: 60,
  reconnect: [max_attempts: 5, delay: 250, max_delay: 5_000]
]
```

Preserve the Client ID when resuming a session. Resume depends on actual
CONNACK Session Present, not a local snapshot alone. Lost subscriptions are
reported as `{:subscriptions_lost, grants}` inside the observer envelope;
resubscribe explicitly and inspect new SUBACKs. Protocol/auth failures and
intentional disconnect cancel retry. No periodic publish retry runs on a
healthy socket. AMQP has no corresponding reconnect option in this release.

MQTT options such as `max_inflight`, `max_operations`, `max_deliveries`,
`max_subscriptions`, `receive_maximum` and `max_packet_size` bound admission.
AMQP has `max_operations`, endpoint/session limits, and per-request slot/byte
credit. Tune capacity to the payload and ownership model. Raising a caller
receive timeout does not determine whether a remote operation succeeded.

## Local durability

Omit persistence for memory behavior. Add the relevant `persistence:` options
inside the MQTT broker, AMQP broker or MQTT client options respectively:

| Owner | Required configuration | Additional bounds |
| --- | --- | --- |
| MQTT broker | `persistence: [directory: absolute_path, stream_id: stable_binary]` | `max_snapshot_bytes`, `max_log_bytes` |
| AMQP broker | `persistence: [directory: absolute_path, stream_id: stable_binary]` | `max_record_bytes`, `max_log_bytes` per queue |
| MQTT client | Nonempty stable `client_id:` plus `persistence: [directory: absolute_path]` | `max_bytes` |

The parent directory must already exist. Each owner creates its leaf and
holds an exclusive inode/device lock, including across aliases and BEAMs.
Give broker protocols, instances and clients distinct absolute directories
and keep broker stream identities stable. These adapters need local Linux
file/directory synchronization; distributed storage and hardware power-loss
qualification are outside the evidence. Logs are bounded without initial
rotation/compaction; plan capacity instead of truncating a live WAL.

Broker host publication returns `{:ok, {:accepted_durable, commit_ref}}`
inside its operation event after the selected durable-through boundary.
MQTT wire ACKs and AMQP publisher Accepted are likewise held until their
selected persistence effects succeed. An append alone is not durability;
failed/uncertain sync closes admission and cannot become a successful ACK.
AMQP durable messages require an explicit durable Header section as well as
the durable broker profile; persistence does not implement durable termini.

MQTT client recovery supplies `{:operation_identity, public_ref, stable_id}`,
`{:recovered_operation, stable_id, kind}`,
`{:recovered_operation_result, stable_id, result}` and
`{:recovered_delivery, new_reference, packet, qos2_phase}` events. A later
`{:recovered_protocol_result, stable_id, result}` describes protocol progress
after an already terminal host result. Handle these envelopes explicitly and
deduplicate by stable identity; another crash can repeat a recovery event.
Supply observer PIDs, credentials and callbacks anew. Deadlines include
downtime, clock rollback rejects recovery, and local storage cannot fabricate
a remote session.

When durability is required, validate restart/crash and uncertain-I/O behavior
for the chosen profile in addition to a network exchange. Do not infer
subscriber business processing or business exactly-once from QoS2, Accepted,
PUBCOMP or a durable-through receipt.
