# MQTT broker and host-supervised embedding

- Date: 2026-10-26
- Status: Design approved by the owner with `LGTM` on 2026-10-26; M0 implemented and qualified on the same date. M1–M5 remain future work.
- Target: Add a native MQTT broker to GravitonMQ, usable standalone or inside an Elixir/OTP host application.
- Proposed protocol target: MQTT 5.0 and MQTT 3.1.1, delivered in separate, tested stages.

## 1. Current evidence and scope change

At design review, the `main` checkout was at `933ac94`, following the AMQP
codec work in `9158fa6`. That baseline contained five umbrella
applications and a bounded, process-free AMQP 1.0 codec. There is no working
listener, broker routing path, queue transition function, concrete storage
backend, or effect executor. `GravitonMQ.Queue.Machine` and
`GravitonMQ.Storage.Memory` are contract boundaries, not implementations.

Embedding already has a real lifecycle: `GravitonMQ.start_link/1`,
`GravitonMQ.child_spec/1`, exact caller-supplied supervisor names, and one
Application callback. Existing lifecycle tests cover two named empty
instances. They do not prove that two operational brokers are isolated.

The owner's new MQTT request expands the previous milestone exclusion of
MQTT. It does not expand the completed AMQP codec slice, implement an AMQP
broker, or authorize an implicit MQTT/AMQP bridge. Preserve the completed
AMQP behavior, semantic identities, opaque message content, and clean-room
rules. Before implementation, update the active scope in `AGENTS.md`,
`docs/ARCHITECTURE.md`, `docs/ROADMAP.md`, and `README.md` to reflect the
approved MQTT slice while retaining historical milestone boundaries.

Packaging, version, license, and workflow work was already present when
design review started and was committed independently as `933ac94` during
the review. This proposal does not change or approve that work. MQTT integration
must accommodate those interfaces and must not overwrite concurrent edits. Commits,
publication, releases, and deployment require their own authorization.

## 2. Alternatives and recommendation

| Approach | Benefits | Costs and limits |
| --- | --- | --- |
| Native MQTT frontend alongside AMQP, composed by the existing runtime — recommended | Same VM embedding, instance ownership, independent protocol testing, preserves current layering | Requires real topic, session, routing, and delivery implementations |
| Operate an external MQTT broker and provide a client integration | Shorter path to mature external protocol support | Requires an additional service; does not provide the requested embedded GravitonMQ broker |
| Build a separate MQTT broker product | MQTT can evolve independently | Duplicates startup, configuration, supervision, and distribution work; weakens the existing product composition |

Use the first approach. The shared core supplies only capabilities that are
actually protocol independent. MQTT is not translated into unfinished AMQP
Connection, Session, Link, or settlement behavior.

## 3. Product contract

### Embedding

For this proposal, embedding means running in the same BEAM VM as an
Elixir/OTP host. A host owns broker startup, shutdown, configuration, and
restart policy through its supervision tree. A non-Elixir application can
connect as an MQTT network client; an in-process C ABI is outside this design.

Support both of these uses:

1. Host-supervised broker with explicitly configured MQTT listeners.
2. Host-supervised broker with no listeners, using a native publish/subscribe
   API. Network clients and local subscribers use the same routing and
   admission policy when listeners are enabled.

Keep the existing `runtime: false` dependency guidance so embedding does not
also start the default Application instance. Per-instance options are passed
explicitly, rather than copied into global application environment.

### Compatibility and delivery

The first operational milestone is a single-node, in-memory MQTT 5.0 broker
with QoS 0 and 1. It must advertise `Maximum QoS = 1`. A SUBSCRIBE request at
QoS 2 must still be processed and can be granted QoS 1; an incoming PUBLISH or
Will above the advertised capability follows the specification's error path.
Do not silently reduce an inbound publication's QoS.

MQTT 3.1.1 has no equivalent Maximum QoS negotiation. Complete and test the
QoS 2 handshake before describing the dual-version milestone as compatible
with MQTT 3.1.1. Codec support alone is never a broker compatibility claim.
QoS 2 controls MQTT protocol delivery; it does not promise exactly-once host
business processing or a cross-protocol guarantee.

Retain session state across network reconnects while the broker instance
lives. An in-memory milestone does not promise survival of broker-process,
VM, host, or power failure. A later durability milestone must establish its
own acknowledgement and recovery boundaries before making stronger claims.

## 4. Application and source boundaries

Add `graviton_mq_mqtt`, with the namespace `GravitonMQ.MQTT`:

```text
graviton_mq_core                       (no child dependencies)
  ^             ^              ^
  |             |              |
storage       amqp10          mqtt     (each depends only on core)
  ^             ^              ^
  +-------------+--------------+
                |
       graviton_mq_runtime
                ^
                |
           graviton_mq
```

Arrows point from a consumer toward its dependency. The public application
still depends only on runtime and remains the sole owner of an Application
callback. The MQTT child is a library application.

| Owner | Responsibilities |
| --- | --- |
| `graviton_mq_mqtt` | Packet values and codec, UTF-8 and topic validation, topic matching, MQTT session and delivery transitions, version-specific capabilities and errors |
| `graviton_mq_core` | Existing stable broker identities and opaque message envelopes; genuinely shared admission or delivery contracts added only when exercised |
| `graviton_mq_storage` | Concrete storage implementations and physical record handling; no references to MQTT or AMQP modules |
| `graviton_mq_runtime` | Socket ownership, timers, session processes, route indexes, bounded delivery queues, local subscriber lifecycle, effect execution, authentication integration |
| `graviton_mq` | Validated public configuration, common startup API, and native host API delegating only to runtime |

Extend `mix graviton_mq.check_architecture` and its regression tests for the
sixth child. Retain compiler xref plus parsed syntax analysis, including
typespec references. MQTT codec, topic, and transition modules must be pure:
no runtime/storage references, socket calls, processes, clocks, or timers.
The AMQP and MQTT children must not depend on each other.

The public API must not construct MQTT structs by importing the protocol
child. Runtime converts validated public binary/map inputs to protocol data.

## 5. Protocol data and message preservation

Keep version-specific packet fields explicit. MQTT 5.0 properties retain
their specified types, permitted packet locations, singleton/repeatable
rules, and ordering for repeated User Properties. Do not turn property pairs
into a map that loses duplicates or silently accept unknown property IDs.

Decode incrementally from binaries, preserving the exact suffix after one
packet. Distinguish incomplete, malformed, unsupported, configured-limit,
and invalid-value results. Bound Remaining Length before buffering the body;
reject overlong Variable Byte Integers and invalid fixed-header flags.
Implement version-specific UTF-8 constraints without Unicode normalization.

Topic Names and Topic Filters are different values. Validate empty levels,
whole-level `+`, final whole-level `#`, and the special matching rules for
names beginning with `$`. Do not derive atoms from topic or client strings.
MQTT 5.0 Topic Aliases belong to one network connection, never the retained
session or durable broker identity.

Preserve inbound publication bytes and binary payload without interpreting
JSON or converting to an AMQP message. Use the existing protocol-neutral
message format discriminator to identify MQTT content. Routing metadata
includes the resolved Topic Name and derived delivery policy. Outbound MQTT
packets must be encoded afresh with the recipient's protocol version,
Packet Identifier, QoS, flags, properties, and connection-local alias state.
Forwarding an entire inbound PUBLISH packet byte-for-byte would be incorrect.
For host-originated messages, runtime creates the MQTT content envelope from
the explicit topic, binary payload, and supported metadata.

## 6. Session, routing, and process ownership

Separate network connection state from client session state:

- A network connection owns its parser buffer, negotiated version,
  authentication exchange, keep-alive observation, aliases, and Writer.
- A logical MQTT session is keyed by `{instance, client_id}` and owns
  subscriptions, pending deliveries, inbound/outbound Packet Identifier
  state, and the relevant Will/session expiry data.
- Retained publications are instance-owned by exact Topic Name and outlive
  individual sessions. A zero-length retained payload removes the retained
  publication while still participating in normal delivery.

Use a supervised session owner when mutable state and failure isolation are
needed. Session owners are siblings of connection trees and can survive
network disconnects. Do not create a process per topic, subscription,
publication, or protocol packet. Topic matching and transitions stay ordinary
functions. Inactive-session resource limits must be explicit.

The runtime topology is introduced only with functioning children:

```text
Host supervisor or GravitonMQ.Application
  `-- GravitonMQ.Supervisor
        `-- Runtime.Supervisor
              |-- instance infrastructure and routing state
              |-- MQTT session supervisor
              |     `-- session owners, including retained offline sessions
              |-- listener supervisor             (only if configured)
              `-- connection supervisor
                    `-- connection tree
                          |-- connection reader/state
                          `-- Writer
```

Keep the existing exact `:name` and `:runtime_supervisor_name` options. Any
additional registered infrastructure name must be explicit and instance
scoped; callers provide literal names or supported OTP naming mechanisms.
No external string is converted into an atom. Coexisting instances must not
share subscriptions, sessions, retained messages, identifiers, or listeners.

Client-ID takeover is serialized at session ownership. Associate each
attachment with a runtime generation token, fence the old connection, and
ignore its stale acknowledgements, disconnects, and timer messages. Apply
the version's Will and session rules explicitly; do not decide Will behavior
from an undifferentiated process `DOWN` message.

Pure machines receive explicit events, including elapsed-time facts, and
return state plus effects. Runtime interprets authentication, routing,
delivery, socket, and timer effects and feeds their actual results back.
Test Clean Session versus Clean Start/Session Expiry, Session Present,
duplicate CONNECT, keep-alive timeout, graceful versus abnormal disconnect,
Will Delay, expiry, and reconnect retransmission as separate transitions.

## 7. Routing, acknowledgement, and pressure

Resolve a publication into eligible subscriptions, apply authorization and
per-subscription options, and create bounded pending deliveries before
acknowledging ownership to the publisher. Overlapping subscriptions must
follow MQTT's granted-QoS and delivery rules; retain the information needed
for No Local, Retain As Published, Retain Handling, and later Subscription
Identifiers. Preserve required per-publisher/topic ordering.

For the in-memory QoS 1 milestone, PUBACK means the instance has accepted
responsibility in memory for the publication and the required pending
deliveries/retained update. It does not mean all subscribers acknowledged,
the host application processed the payload, or data reached disk. Failures
or uncertain admission must not produce a success acknowledgement. QoS 0
does not gain an acknowledgement or stronger guarantee through this API.

Bound packet size, parser buffers, connections, session count, subscriptions,
offline deliveries, retained bytes, inflight delivery counts, and Writer
queues. Observe MQTT 5.0 Receive Maximum and peer Maximum Packet Size.
Pause socket admission before buffers are exhausted. If admission cannot
succeed, use the defined version-specific failure/connection policy instead
of silently acknowledging and dropping a QoS 1/2 publication.

The native host API uses monitored subscribers with explicit demand and a
finite outstanding-delivery limit. A host mailbox is not an unbounded broker
queue. Acknowledgements release broker delivery responsibility; business
transactions remain the host's concern. A subscriber crash or unsubscribe
must release resources deterministically. Demand and cancellation carry a
runtime subscription reference; acknowledgements carry a delivery reference.
Both are scoped to one instance and subscriber lifetime.

## 8. Public embedding and host API

Preserve the existing public startup functions. This is an illustrative
target configuration, not currently supported listener options:

```elixir
# Host mix.exs: select the package/source version independently.
{:graviton_mq, "~> 0.1", runtime: false}

# Host supervision tree: exact names, owned by the host.
{GravitonMQ,
 name: MyApp.Broker,
 runtime_supervisor_name: MyApp.BrokerRuntime,
 mqtt: [versions: [:v5], listeners: []]}
```

Listener configuration must explicitly select bind address, port, transport,
and authentication policy. An empty list opens no sockets. The first network
qualification uses loopback and ephemeral ports; TCP and OTP `:ssl` listener
support are verified independently. Do not add Ranch, Phoenix, or a transport
dependency merely to populate the tree.

Proposed host API, introduced only with real routing and lifecycle tests:

```elixir
GravitonMQ.publish(instance, topic, payload, options)
GravitonMQ.subscribe(instance, topic_filter, options)
GravitonMQ.request(instance, subscription_ref, demand)
GravitonMQ.ack(instance, delivery_ref)
GravitonMQ.unsubscribe(instance, subscription_ref)
```

These functions are specifically a topic-based MQTT-facing API. They do not
define an AMQP bridge. `topic`, `topic_filter`, and `payload` are binaries;
local delivery metadata contains no socket or protocol Packet Identifier.
The implementation slice must fix return tuples and delivery message shapes
before coding, including known failure versus uncertain completion. A
timeout must not be reported as proof that publication was rejected; blind
retry may duplicate an accepted publication.

Authentication and authorization are host-supplied behaviours with explicit
connect, publish, and subscribe decisions. Network authentication does not
leak credentials into logs. Callback error, crash, or timeout fails closed;
callbacks run with bounded concurrency and deadlines outside the pure
machines. Anonymous access requires explicit configuration. Native callers
use an explicit host principal/policy rather than impersonating a network
client. Subscription authorization must account for wildcard scope and actual
delivery permissions.

## 9. Storage boundary

The first broker milestone uses real in-memory session, retained, and delivery
state. It must not claim to implement `GravitonMQ.Core.Storage` by returning
synthetic durable-through references. The existing append/sync/
durable-through/fold contract remains mandatory for implementations that
claim it.

MQTT session and retained-state persistence is not already solved by the
queue-event storage behaviour. Do not disguise MQTT protocol state as
`Queue.Event` data or import MQTT structs into storage. Before a durable
milestone, design a versioned opaque state/event boundary, atomic publication
and delivery responsibility, recovery ordering, expiry/Will recovery, and the
correlation between accepted protocol handshakes and durable-through state.
That is a separate implementation design, not a promise of this memory
milestone. Filesystem WAL, fsync, recovery, and storage-failure qualification
remain deferred until that design is approved and implemented.

## 10. Development stages and acceptance

Each stage has a bounded implementation plan after review. Completion of a
stage does not imply completion of later rows.

| Stage | Deliverable | Required observable result |
| --- | --- | --- |
| M0: boundary and first codec slice | Sixth library child, architecture rules, common packet envelope, CONNECT/CONNACK for levels 4 and 5, packet limits | Independent wire vectors, every strict prefix, exact suffix, malformed flags/lengths/properties, zero process/socket activity; existing AMQP tests unchanged |
| M1: packet surface | PUBLISH and QoS acknowledgements, SUBSCRIBE/SUBACK, UNSUBSCRIBE/UNSUBACK, PING, DISCONNECT; MQTT 5 properties and explicit enhanced-auth rejection | Valid packets round-trip semantically; packet-specific field, direction, and property rules reject invalid input; no broker claim |
| M2: pure broker semantics | Topic matching, session transitions, routing plans, retained state, Will, expiry, QoS 0/1 admission and delivery effects | Deterministic event sequences prove policy and ordering without clocks, sockets, or processes |
| M3: embedded MQTT 5 broker | Real session/routing owners, bounded host API, opt-in TCP/TLS listeners, auth integration, QoS 0/1 and in-memory reconnects | Independent network clients exchange messages; host-to-network and network-to-host paths work; two live instances isolate state; advertised capability matches tests |
| M4: dual-version broker | QoS 2 state and ownership, MQTT 3.1.1 runtime semantics, client takeover/reconnect qualification | PUBREC/PUBREL/PUBCOMP under duplicate and disconnect boundaries; 3.1.1 Clean Session and 5.0 session semantics each pass independent clients |
| M5: separately designed durability | Persistent session/retained/delivery state and actual recovery | Crash/restart and failed/uncertain synchronization prove acknowledgements follow the selected durable boundary |

MQTT 5.0 optional capabilities must be explicit during M3: advertise shared
subscriptions and Subscription Identifiers as unavailable until implemented;
advertise inbound Topic Alias Maximum as zero until supported. Features that
cannot be disabled by capability negotiation, including supported packet
properties, message expiry, subscription options, and Will/session behavior,
must work or cause a specification-defined rejection. Never silently ignore
their semantics. Enhanced authentication, shared subscriptions, WebSocket,
clustering, bridges, management APIs/UI, MQTT-SN, and MQTT 3.1 are outside the
initial operational milestone.

### First implementation slice only

After owner review, write the detailed M0 implementation plan first. Its
allowed writes are the new MQTT child, runtime's dependency declaration,
architecture rules and associated tests, scope documentation, and the
existing package-list integration needed for a sixth child. Preserve ongoing
packaging edits and adapt to their final interface. Do not add listeners,
native publish/subscribe functions, Session processes, queue/storage
implementations, or empty-success broker stubs in M0.

CONNECT/CONNACK coverage includes protocol name/level, reserved and Will
flags, clean-session/start representation, Client ID, keep alive, optional
credentials and Will data, packet length boundaries, MQTT 5.0 property
cardinality/location, and version-specific reason codes. Decoding CONNECT
does not authenticate, register a Client ID, start a session, or run a timer.
After CONNECT establishes a version, other packet decoding takes that version
explicitly; do not infer it from packet contents.

The first codec follows the existing explicit result style: decoding returns
`{:ok, packet, rest}`, `{:more, positive_byte_count}`, or a structured MQTT
codec error; encoding returns `{:ok, bytes}` or a structured error. Error
categories distinguish malformed wire data, unsupported packet/version,
configured limits, and invalid semantic values. Limit validation is pure,
and packet-size limits cover the fixed header as well as Remaining Length.
Every strict prefix of each valid independent packet fixture must remain
incomplete without losing bytes or invoking runtime behavior.

## 11. Verification and evidence rules

Every implementation slice runs the repository's complete root sequence
under `.tool-versions` (Elixir 1.18.4 / OTP 28.5.0.1):

```bash
mix deps.get
mix format
mix format --check-formatted
mix compile --warnings-as-errors
mix test
mix graviton_mq.check_architecture
mix help xref
mix xref graph --format cycles --fail-above 0
```

Add wire fixtures derived from the OASIS specifications and independent
clients rather than encoder/decoder self-consistency alone. Codec slices
exercise incomplete prefixes, concatenated packets, invalid UTF-8, integer
limits, property duplicates, and size limits. Runtime slices use real
loopback sockets, deterministic timer inputs where possible, and assertions
on process cleanup. Independently verify TCP and TLS; do not infer either
from pure codec tests.

Before M3 acceptance, test slow subscribers, full delivery/Writer queues,
subscriber death, auth callback failure, malformed peers, duplicate Client
IDs, stale connection generations, listener startup failure, reconnects,
retained deletion, Will delivery, expiry, two-instance isolation, and host
shutdown. Before M4 acceptance, add QoS 2 duplicate and reconnect boundaries
in both protocol versions. Before any distribution claim, build all six
archives and start a clean external consumer using the documented embedding
path; no package publication is implied.

Report PASS, FAIL, BLOCKED, and NOT RUN at their actual scope. Existing AMQP
compatibility, production security, throughput, crash recovery, and public
package availability cannot be inferred from MQTT unit tests. This design
introduces no throughput target without a workload and benchmark.

Design-session baseline verification on 2026-10-26 used the exact declared
Elixir 1.18.4 / OTP 28.5.0.1 pair and an isolated temporary build directory.
All eight commands above exited zero: 210 existing tests, zero failures,
warning-free compilation, 1,359 source references across five applications,
and no xref cycles. Formatting changed no files. These were pre-MQTT baseline
results and did not qualify MQTT behavior.

### M0 implementation qualification — 2026-10-26

The integrated M0 implementation passed all eight required commands above
under Elixir 1.18.4 / OTP 28.5.0.1, with warning-free compilation and 290
tests, zero failures. The total includes 76 MQTT tests and the unchanged 172
AMQP tests. Architecture analysis passed for 1,698 source references across
six applications; the xref graph had no cycles.

Wire coverage includes independent CONNECT/CONNACK and property fixtures,
every strict prefix, exact suffixes, version-specific flags and reason codes,
UTF-8 and binary fields, malformed internal lengths, canonical MQTT 5 VBIs,
configured limits, full 16-bit field boundaries, and deterministic adversarial
inputs. Independent read-only review identified a property-length overflow
guard, which was corrected. Full integration testing also exposed and resolved
the old five-application release-test fixture.

All six version `0.1.0` Hex archives were built in
`/tmp/graviton-mqtt-m0-hex-tRN3zr/packages/artifacts`. Their actual archive
metadata was checked for package identity, version, and exact internal
dependencies. Separate clean consumers compiled with warnings as errors:
standalone startup decoded an independent CONNECT fixture, while a
`runtime: false` host proved no default product instance autostart, supervised
a named instance, decoded the same fixture, and stopped its children cleanly.
Only the public package retains an Application callback.

M0 is complete at the codec and empty embedding-lifecycle boundary. Broker
routing, publish/subscribe, sessions, TCP/TLS, live-client interoperability,
and durability remain unimplemented future stages. No commit, package
publication, release, or deployment was performed.

## 12. References

- [OASIS MQTT 5.0 standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html): §§1.5, 2–4, 7; in particular §3.2.2.3.4 Maximum QoS, §4.1 Session State, §4.4 retry, and §4.7 topics.
- [OASIS MQTT 3.1.1 standard](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html): §§3.1.2.4, 4.1, 4.3–4.7, 7.
- Existing [architecture](../../ARCHITECTURE.md), [roadmap](../../ROADMAP.md), and [embedding lifecycle ADR](../../adr/0006-support-standalone-and-embedded-lifecycles.md).
- Existing [opaque message ADR](../../adr/0005-preserve-amqp-message-as-opaque-content.md) and [failure model](../../FAILURE_MODEL.md).

The official versioned HTML references were retrieved during design review;
Firecrawl returned cached copies from 2026-10-24 (5.0) and 2026-10-25 (3.1.1).
They are protocol references, not evidence that the proposed broker works.
