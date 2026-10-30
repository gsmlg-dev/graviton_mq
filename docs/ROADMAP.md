# Roadmap

## Active G0–G7 Goal — 2026-10-29

The owner now authorizes four independently verifiable network roles:
AMQP1 broker/client and MQTT5 broker/client. The
[active scope](ACTIVE_SCOPE.md) defines the ordered stages and actual exits;
the [capability matrix](CAPABILITIES.md) records their implementation and
evidence. MQTT client is the first operational path, followed by memory MQTT
broker; AMQP client/broker, full selected-profile semantics, real persistence,
and CI/installation qualification follow. Memory-only operation is intermediate.

G0/G1 and the bounded G2 memory broker exit now have recorded qualification:
[G0/G1](qualification/2026-10-29-g0-g1.md) and
[G2](qualification/2026-10-30-g2.md), including independent peers and clean
archive consumers. The initial [G3 AMQP client exit](qualification/2026-10-30-g3-client.md)
now has pure endpoints, native outgoing runtime, five Artemis tests and a
client-only archive exchange. The bounded [G4 memory broker exit](qualification/2026-10-30-g4-broker.md)
passed with the deterministic queue, memory runtime, independent Proton and
rebuilt consumers. The bounded [G6 local durability exit](qualification/2026-10-30-g6-local-durability.md)
passed 1,073 tests, all eight gates and all seven archives/three clean consumers.
[G7 local evidence](qualification/2026-10-30-g7-local-qualification.md) includes
pinned peer preparation, model/fuzz and sustained four-role traffic. G5 clause
audits and their repairs now have final integrated evidence: 1,117 tests,
all eight gates, seven archives and three clean installed consumers. G7's
final-source 15-minute run passed 35,976 exact four-role round trips, final
drain and cleanup. Local selected-profile G0–G7 exits are complete; hosted
CI was NOT RUN at that local checkpoint. Excluded capabilities and execution
limits remain explicit in the matrix; this is not universal protocol or
production qualification.

[ADR 0007](adr/0007-independent-network-client-application.md) introduces a
separate client application with no broker dependencies. Preserve MQTT3.1.1
CONNECT/CONNACK codec regressions; qualify any legacy runtime separately.
Historical milestone exclusions and evidence below retain their original
meaning and do not block these newly authorized stages. The original Goal
implied no branch/worktree, commit, publication, release, or deployment.
The owner's subsequent [release authorization](ACTIVE_SCOPE.md#release-authorization--2026-10-30)
adds commit, push and `0.2.0` package/GitHub publication on the existing branch.
See the [release notes](releases/0.2.0.md) for shipped profiles and evidence.
The [v0.2.0 release qualification](qualification/2026-10-30-v0.2.0-release.md)
now records successful hosted CI, seven published Hex packages, matching
GitHub assets and three public installed consumers. Historical local and
milestone records below retain their original execution boundaries.

The roadmap preserves the separation between AMQP 1.0 and MQTT protocol work,
broker semantics, storage, and runtime processes. A milestone must not advertise
capability until its tests exercise that capability.

## Milestone 0: repository baseline and architecture hardening

- Create the original five-application Mix umbrella and exact dependency graph.
- Establish protocol, core, storage, runtime, and public API boundaries.
- Represent AMQP values by semantic type, including distinct binary, string,
  symbol, and signed and unsigned integer values, without implementing their
  wire encodings.
- Model independent local and remote Link handles, Session handle indexes, and
  Connection channel indexes without implementing protocol behavior.
- Define stable serializable broker identities, protocol-independent logical
  queue events, lossless settlement instructions, and an opaque message-content
  envelope with a parsed broker index.
- Define a mandatory core storage contract that separates ordered append from
  durable-through synchronization and provides streaming recovery. Keep
  storage-owned physical records separate from core logical events.
- Start only per-instance top-level and empty runtime supervision boundaries,
  using the same public lifecycle for standalone and host-supervised embedding.
- Record clean-room rules, delivery intent, fault domains, message-preservation
  and lifecycle decisions, and explicit milestone exclusions.
- Verify formatting, warning-free compilation, tests, application lifecycle,
  declared dependency direction, actual source references, and dependency
  cycles in local checks and CI.

Milestone 0 explicitly provides no functioning AMQP listener, codec, protocol
state machine, message queue, storage backend, recovery path, or effect
executor. Its value, state, event, record, and lifecycle modules establish
contracts and ownership only.

## Milestone 1: pure AMQP 1.0 codec foundation

Milestone 1 implements and tests a deliberately bounded, process-free AMQP 1.0
codec foundation:

- recognize the raw AMQP 1.0 protocol header without negotiating SASL or
  alternate versions;
- validate one AMQP frame envelope while preserving its body and remainder as
  opaque bytes;
- encode and decode `null`, `ushort`, `uint`, `ulong`, `string`, `symbol`,
  `list`, ordered `map`, symbol arrays, and supported described values;
- encode and decode only Open and Begin into dedicated immutable structs with
  exact tagged fields, accepting their standard numeric and symbolic
  descriptors;
- validate their fixed positional schemas, mandatory fields, exact field
  types, multiple symbols, symbol-keyed property maps, and specification
  defaults, while deferring protocol-context rules;
- bound frame size, value size, compound counts, and nesting; and
- return explicit incomplete, malformed, unsupported, limit-exceeded, and
  invalid-value results.

The codec preserves the Milestone 0 value identities while choosing compact
wire constructors independently. Open's default `max_frame_size` and
`channel_max`, and Begin's default `handle_max`, materialize as exact tagged
values; canonical encoding uses numeric descriptors and positional nulls while
omitting trailing nulls. Frame bodies and authoritative encoded messages stay
opaque. The codec operates on binaries without TCP, OTP process ownership,
SASL negotiation, protocol state, queue transitions, storage calls, or broker
commands. Source-level checks enforce the process-free codec namespace. This
milestone does not claim full protocol compatibility or begin queue-machine
behavior.

## Post-Milestone-1: End, Close, and Error codec slice

This completed slice provides:

- Encode and decode End and Close as dedicated immutable structs.
- Validate their optional nested Error composite without whitelisting condition
  symbols or widening the bounded value subset.
- Preserve the process-free codec, opaque-frame, structured-error, and exact
  remainder boundaries.
- Do not add protocol transitions, other performatives, message sections,
  transport, queues, or storage.

## Post-Milestone-1: Boolean and Detach codec slice

This completed slice provides:

- Decode all three Boolean constructors into the existing exact tagged type,
  reject undefined payload octets, and encode with compact constructors.
- Extend the shared bounded subset recursively, including Open/Begin properties
  and End/Close/Detach Error info, while keeping arrays symbol-only.
- Encode and decode Detach's required `uint` handle, defaulted Boolean `closed`,
  and optional nested Error with its correct outer field index.
- Preserve canonical numeric descriptors, positional nulls, explicit errors,
  incomplete prefixes, exact remainders, limits, and opaque frame bodies.
- Keep protocol transitions, other schemas, transport, queues, and storage out
  of this slice.

## MQTT M0: pure CONNECT/CONNACK codec foundation

The completed, owner-approved M0 slice adds a sixth library application
depending only on core, with runtime composition and source-enforced protocol isolation. It
provides the opaque common packet envelope, MQTT 5.0 CONNECT/CONNACK,
protocol-level validation, ordered MQTT 5 properties, configured limits,
and explicit incomplete/malformed/unsupported/limit/invalid-value results.
Package qualification includes six archives and separate standalone and
host-supervised clean consumers. No runtime broker behavior is added.
The historical MQTT5-only amendment rejected legacy codecs. The active Goal
now preserves MQTT3.1.1 CONNECT/CONNACK regressions, without qualifying its runtime.

Initial M0 qualification on 2026-10-26, before the MQTT 5.0-only amendment,
passed the complete root verification sequence:
290 tests, zero failures (76 MQTT tests), warning-free compilation, architecture
analysis across six applications, and no xref cycles. All six archives and
both clean consumers passed; see the
[M0 evidence](superpowers/specs/2026-10-26-mqtt-broker-embedding-design.md#m0-implementation-qualification--2026-10-26).

The MQTT 5.0-only amendment then passed all eight checks with 294 tests,
zero failures (80 MQTT), warning-free compilation, and no architecture or
xref cycle violations. Six rebuilt archives and both clean consumers passed;
an unpacked consumer explicitly rejected MQTT 3.1/3.1.1. See the
[amendment evidence](superpowers/specs/2026-10-26-mqtt-broker-embedding-design.md#mqtt-50-only-boundary-amendment--2026-10-26).

The owner subsequently required our own MQTT 5.0 network client and broker.
The [MQTT design](superpowers/specs/2026-10-26-mqtt-broker-embedding-design.md)
retains a shared pure protocol layer and separates each role's state and
runtime. Neither role may require the other to be started. M0 remains
complete at its original codec/lifecycle boundary. The original design's
decomposition below is mapped into active G1–G7; current completion follows
the capability matrix rather than these historical future labels:

| Stage | Future deliverable |
| --- | --- |
| M1 | Shared remaining packet schemas and their property/direction validation |
| M2a | Pure client lifecycle, QoS 0/1 send/receive, subscriptions/grants, keep alive, session resumption, and reconnect effects |
| M2b | Pure broker topic/session/routing/retained/Will/expiry semantics and effects |
| M3a | Independent host-supervised network client, outgoing TCP/TLS, credentials, bounded API, QoS 0/1, and in-memory reconnects |
| M3b | Embedded MQTT 5 broker, opt-in TCP/TLS, authentication, QoS 0/1, and in-memory reconnects |
| M4 | MQTT 5.0 QoS 2 on both roles, including duplicate and reconnect qualification |
| M5 | Separately designed broker persistence and client inflight persistence, recovery, and durability qualification |

M3a follows M1/M2a and M3b follows M1/M2b; deliver the independent client
before the embedded broker, each with a bounded reviewed plan. Qualify our
client against an independent broker and independent clients against our
broker, then exercise our client/broker together. Self-to-self exchanges
alone can hide shared defects. Test peers are not production dependencies.

These stages require independent acceptance evidence; codec tests do not
establish a functioning network client, broker, or interoperability. See
[MQTT scope](MQTT_SCOPE.md) for the exact M0 API and exclusions.

### Unscheduled MQTT QUIC TODO

- [ ] Reliable MQTT 5.0 transport over QUIC streams for the client and broker.
- [ ] Realtime state over QUIC DATAGRAM after explicit peer negotiation,
  using a separately versioned extension with no MQTT standard capability claim.

Record only; implementation is deferred. See the
[QUIC TODO boundary](MQTT_SCOPE.md#todo-quic-support-not-implemented).
The existing milestone implementation and acceptance scopes remain unchanged.

## Historical AMQP and shared broker sequence

The sequence recorded below predates the active G0–G7 implementation. Retain
it as design history; current behavior and evidence follow the active scope
and capability matrix. The original planned increments were:

1. Complete the remaining required AMQP performative and message-section codec
   surface in separately bounded slices. Flow is the recommended next schema,
   followed by Attach with its Source/Target composites.
2. Add pure Connection and Session transition models, keeping Link state owned
   by Session state.
3. Define a minimal protocol-independent queue machine using commands, logical
   events, and effects, with no persistence or networking inside transitions.
4. Add an in-memory implementation of the core storage behaviour and a runtime
   effect executor with explicit success, failure, and uncertainty results.
5. Introduce a supervised TCP connection tree and Writer, then integrate the
   tested codec and protocol transitions.
6. Implement transfer assembly, flow control, settlement, and an at-least-once
   delivery path.
7. Implement physical record encoding, a filesystem WAL, segment storage, and
   streaming recovery, preserving the rule that publisher acceptance follows
   the selected durability boundary.
8. Explore queue-type-specific replication only after local recovery semantics
   are stable.
9. Add management capabilities outside the message hot path.

At that baseline, TLS, WebSocket transport, clustering, Raft, other protocol
extensions and management UI were deferred. The active Goal now implements
TCP/TLS roles; WebSocket, clustering/Raft, bridges, transactions, management UI
and QUIC retain their explicit current exclusions.
