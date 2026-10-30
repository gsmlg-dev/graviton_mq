# MQTT 5.0 client, broker, and host-supervised embedding

Active amendment — 2026-10-29: the owner authorized the
[G0–G7 Goal](../../ACTIVE_SCOPE.md). [ADR 0007](../../adr/0007-independent-network-client-application.md)
supersedes this design's proposed client runtime/facade ownership: outgoing
roles now belong to `graviton_mq_client`, independent of broker runtime/storage.
Preserve MQTT3.1.1 CONNECT/CONNACK codec regression coverage; its later runtime
requires separate acceptance. Operational roles still target MQTT5. The
historical M0 qualification and original design below are retained; later
stage status comes from [current evidence](../../CAPABILITIES.md).

- Date: 2026-10-26
- Status: Initial broker design approved by the owner with `LGTM` on 2026-10-26; M0 implemented and qualified on the same date. The owner subsequently required our own client as well as broker. M1–M5 remain future work, with M2 and M3 split by role below.
- Target: Implement both a native MQTT client and broker in GravitonMQ. The client connects to remote brokers independently; the broker runs standalone or inside an Elixir/OTP host application.
- Protocol target: MQTT 5.0 only. The owner narrowed this boundary after initial M0 qualification; MQTT 3.1 and 3.1.1 are unsupported and have no planned implementation stage.

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

The owner subsequently confirmed two product roles: our own MQTT 5.0 client
and our own broker. This amendment defines their shared protocol layer,
separate state ownership, and delivery stages. The completed M0 codec is
usable by both roles; it does not implement either runtime. Later stages
still need bounded designs, implementation plans, and acceptance evidence.

Packaging, version, license, and workflow work was already present when
design review started and was committed independently as `933ac94` during
the review. This proposal does not change or approve that work. MQTT integration
must accommodate those interfaces and must not overwrite concurrent edits. Commits,
publication, releases, and deployment require their own authorization.

## 2. Alternatives and recommendation

| Approach | Benefits | Costs and limits |
| --- | --- | --- |
| Shared native MQTT codec with separate client and broker machines — selected | Both roles are independently usable, share wire validation, and preserve current layering | Requires independent role semantics and interoperability qualification |
| Implement client and broker with separate codecs | Each role can change its wire layer independently | Duplicates validation and risks conflicting protocol behavior |
| Wrap an existing MQTT client or run an external broker | Earlier access to mature implementations | Does not meet the owner's requirement for our own client and broker |

Use the first approach within the existing six-application graph. The shared
core supplies only capabilities that are actually protocol independent.
MQTT is not translated into unfinished AMQP Connection, Session, Link, or
settlement behavior.

## 3. Product contract

### Native implementation and supported roles

Implement MQTT packet encoding/decoding, validation, client and broker state
machines, QoS handshakes, reconnect behavior, broker sessions, topic routing,
retained publications, and Will/expiry handling ourselves from the OASIS
MQTT 5.0 specification. Do not substitute a third-party MQTT library or an
external broker for either production role, or port another implementation's
source. Independent implementations remain useful test peers.

Use standard Elixir/OTP supervision, `:gen_tcp`, and `:ssl` as infrastructure.
This requirement does not call for a new TCP stack or cryptographic/TLS
implementation. MQTT 3.1 and 3.1.1 are excluded on both sides, with no fallback.

The remote client is a first-class host-supervised component. It must connect,
publish, subscribe, receive, acknowledge, unsubscribe, reconnect, and
disconnect without starting a local GravitonMQ broker. Broker-local host
publish/subscribe operations remain a separate API; they do not fulfill the
network-client requirement. A broker must likewise work with independent
clients without requiring our client.

### Embedding

For this proposal, embedding means running in the same BEAM VM as an
Elixir/OTP host. A host owns client and broker startup, shutdown, configuration,
and restart policy through its supervision tree. A non-Elixir application can
connect as an MQTT network client; an in-process C ABI is outside this design.

Support these uses independently:

1. Host-supervised broker with explicitly configured MQTT listeners.
2. Host-supervised broker with no listeners, using a native publish/subscribe
   API. Network clients and local subscribers use the same routing and
   admission policy when listeners are enabled.
3. Host-supervised MQTT network clients connecting to remote brokers, with
   no local broker, routing state, retained store, or listeners started.
4. A host running both roles, with client and broker lifecycles and names
   isolated. Communication between them uses the same MQTT wire path as
   communication with independent peers.

Keep the existing `runtime: false` dependency guidance so embedding does not
also start the default Application instance. Per-instance options are passed
explicitly, rather than copied into global application environment.

### Compatibility and delivery

The first operational stages provide an in-memory MQTT 5.0 client and a
single-node, in-memory broker with QoS 0 and 1. The client respects the
remote broker's granted QoS and negotiated limits, rejects unsupported local
QoS 2 requests explicitly, and never reports a grant above its implementation.
For inbound QoS 2, MQTT 5.0 has no client-side Maximum QoS capability: until
M4, such delivery is an explicit unsupported boundary, not a claim of full
client compatibility with arbitrary broker/session state.

The broker must advertise `Maximum QoS = 1`. A SUBSCRIBE request at
QoS 2 must still be processed and can be granted QoS 1; an incoming PUBLISH or
Will above the advertised capability follows the specification's error path.
Do not silently reduce an inbound publication's QoS.

The later QoS 2 milestone covers both roles and remains MQTT 5.0 only.
MQTT 3.1 and 3.1.1 are excluded from both the codec and operational milestones.
Codec support alone is never a working client or broker compatibility claim.
QoS 2 controls MQTT protocol delivery; it does not promise exactly-once host
business processing or a cross-protocol guarantee.

Retain each role's session state across network reconnects while its state
owner lives. An in-memory milestone does not promise survival of that owner's
failure, VM/host restart, or power failure. A later durability milestone must
establish separate client and broker recovery boundaries before making
stronger claims.

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
| `graviton_mq_mqtt` | Shared packet values/codecs and validation; separate pure client and broker session/delivery machines; broker topic matching and routing plans; capabilities and errors |
| `graviton_mq_core` | Existing stable broker identities and opaque message envelopes; genuinely shared admission or delivery contracts added only when exercised |
| `graviton_mq_storage` | Concrete storage implementations and physical record handling; no references to MQTT or AMQP modules |
| `graviton_mq_runtime` | Separate client and broker owners; outgoing client connections/reconnects; broker listeners, sessions, route indexes; bounded queues, timers, effect execution, and authentication integration |
| `graviton_mq` | Validated public configuration, existing broker lifecycle and broker-local host API, plus an independent client facade; delegates only to runtime |

Extend `mix graviton_mq.check_architecture` and its regression tests for the
sixth child. Retain compiler xref plus parsed syntax analysis, including
typespec references. MQTT codec, topic, and transition modules must be pure:
no runtime/storage references, socket calls, processes, clocks, or timers.
The AMQP and MQTT children must not depend on each other.

The public API must not construct MQTT structs by importing the protocol
child. Runtime converts validated public binary/map inputs to protocol data.

Reserve `GravitonMQ.MQTT.Client` and `GravitonMQ.MQTT.Broker` for pure role
state beneath the MQTT library. Future runtime owners live under
`GravitonMQ.Runtime.MQTT.Client` and `GravitonMQ.Runtime.MQTT.Broker`.
The proposed public network-client facade is `GravitonMQ.MQTTClient` in the
public application, outside the pure `GravitonMQ.MQTT` namespace. No extra
umbrella application, Application callback, or dependency is needed to record
this design; the current six-package distribution remains unchanged.

## 5. Protocol data and message preservation

Keep MQTT 5.0 packet fields explicit. MQTT 5.0 properties retain
their specified types, permitted packet locations, singleton/repeatable
rules, and ordering for repeated User Properties. Do not turn property pairs
into a map that loses duplicates or silently accept unknown property IDs.

Decode incrementally from binaries, preserving the exact suffix after one
packet. Distinguish incomplete, malformed, unsupported, configured-limit,
and invalid-value results. Bound Remaining Length before buffering the body;
reject overlong Variable Byte Integers and invalid fixed-header flags.
Implement MQTT 5.0 UTF-8 constraints without Unicode normalization.

Topic Names and Topic Filters are different values. Validate empty levels,
whole-level `+`, final whole-level `#`, and the special matching rules for
names beginning with `$`. Do not derive atoms from topic or client strings.
MQTT 5.0 Topic Aliases belong to one network connection, never the retained
session or durable broker identity.

Preserve inbound publication bytes and binary payload without interpreting
JSON or converting to an AMQP message. Use the existing protocol-neutral
message format discriminator to identify MQTT content. Routing metadata
includes the resolved Topic Name and derived delivery policy. Outbound MQTT
packets must be encoded afresh with the recipient's Packet Identifier, QoS,
flags, properties, and connection-local alias state using MQTT 5.0.
Forwarding an entire inbound PUBLISH packet byte-for-byte would be incorrect.
For host-originated messages, runtime creates the MQTT content envelope from
the explicit topic, binary payload, and supported metadata.

## 6. Client, broker session, and process ownership

### Broker ownership

Separate broker-side network connection state from its retained client session:

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

The broker runtime topology is introduced only with functioning children:

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
Test Clean Start/Session Expiry, Session Present,
duplicate CONNECT, keep-alive timeout, graceful versus abnormal disconnect,
Will Delay, expiry, and reconnect retransmission as separate transitions.

### Remote-client ownership

Each outgoing client has its own logical owner, endpoint, Client ID, local
operation references, subscriptions requested by the host and their actual
grants, inflight QoS state, and finite pending-operation/delivery buffers.
Inbound and outbound Packet Identifier spaces are distinct. A Client ID is
a remote protocol identity, not a global OTP registration or durable broker
identity. Clients targeting different brokers must not share state by ID.

The client owner survives an ordinary transport disconnect. Its connection
generation owns the socket, parser, Writer, negotiated properties, aliases,
and keep-alive timers. Reconnect replaces that connection, resets aliases,
and fences stale socket and timer events. The client supplies Will settings;
only the remote broker decides when to publish the Will.

The independent runtime target is:

```text
Host supervisor
  `-- MQTT client subtree                  (one per configured client)
        |-- client owner and bounded operation/delivery state
        `-- outgoing connection subtree   (only while connecting/connected)
              |-- connection reader/state
              `-- Writer
```

Client machines receive explicit connection, packet, host-operation, and
elapsed-time events and emit effects. They do not dial sockets, schedule
timers, or invoke broker routing. Keep client and broker transitions separate
even when their packet types and QoS invariants overlap.

Session resumption uses Clean Start, Session Expiry, and CONNACK's Session
Present together with available local state. Resume only valid inflight
state; report local-state loss explicitly rather than inventing a successful
resume. A missing remote session clears stale protocol state and reports
affected operations; restoring desired subscriptions is an explicit client
policy whose new SUBACK results must be observed. Retransmit unacknowledged
QoS publications at the MQTT-defined reconnect boundary, not on a periodic
retry timer while a healthy connection remains open.

Reconnect is configurable, bounded, and can be disabled. Runtime supplies
backoff/jitter facts to the pure machine. Intentional disconnect cancels
reconnect; authentication/protocol failures are reported rather than retried
indefinitely. When disconnected, reject operations or use an explicitly
configured finite buffer. Stopping one client cleans up its sockets/timers
without affecting any broker instance or another client.

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
succeed, use the defined MQTT 5.0 failure/connection policy instead
of silently acknowledging and dropping a QoS 1/2 publication.

The native host API uses monitored subscribers with explicit demand and a
finite outstanding-delivery limit. A host mailbox is not an unbounded broker
queue. Acknowledgements release broker delivery responsibility; business
transactions remain the host's concern. A subscriber crash or unsubscribe
must release resources deterministically. Demand and cancellation carry a
runtime subscription reference; acknowledgements carry a delivery reference.
Both are scoped to one instance and subscriber lifetime.

Client send results distinguish local admission, completed remote handshake,
explicit rejection, and uncertain completion after timeout/disconnect.
Socket-write success for QoS 0 does not prove remote receipt. A successful
QoS 1 PUBACK proves remote MQTT acceptance under that broker's policy, not
subscriber processing or durability. Never report a pending operation as
completed, or silently retry an uncertain host publication as a new operation.

Client receive delivery uses explicit host demand and a finite outstanding
limit, with protocol acknowledgements tied to the documented ownership
boundary. Apply Receive Maximum, peer packet size, and bounded Writer/parser
queues on both roles. A slow or dead host consumer cannot create an unbounded
mailbox or cause premature success acknowledgements. Host delivery references
are local and distinct from reusable wire Packet Identifiers.

## 8. Public embedding and APIs

### Broker-local host API

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

### Independent MQTT network-client API

Use the same `graviton_mq` dependency with `runtime: false`, then place only
the client in the host tree. This is a proposed M3a API and configuration;
`GravitonMQ.MQTTClient` does not exist in M0:

```elixir
{GravitonMQ.MQTTClient,
 name: MyApp.SensorClient,
 client_id: "sensor-client",
 endpoint: [host: "broker.example", port: 8883, transport: :tls]}

GravitonMQ.MQTTClient.start_link(options)
GravitonMQ.MQTTClient.child_spec(options)
GravitonMQ.MQTTClient.publish(client, topic, payload, options)
GravitonMQ.MQTTClient.subscribe(client, topic_filter, options)
GravitonMQ.MQTTClient.request(client, subscription_ref, demand)
GravitonMQ.MQTTClient.ack(client, delivery_ref)
GravitonMQ.MQTTClient.unsubscribe(client, subscription_ref)
GravitonMQ.MQTTClient.disconnect(client, options)
```

These operations travel to the configured remote broker. They do not require
`GravitonMQ.start_link/1` or use the broker-local host API as a shortcut.
Starting a client subtree is distinct from receiving a successful CONNACK;
connection readiness, SUBACK grants, and publish completion must be observable
separately. The M3a plan must specify result tuples, operation references,
connection/delivery events, deadlines, and graceful-stop/reconnect semantics
before implementing this facade.

Configuration selects TCP or TLS explicitly, supplies trust and credential
settings per client, and never falls back to a legacy MQTT version or to
plaintext. TLS verifies the peer certificate and hostname by default.
Authentication Method/Data remain unsupported until an enhanced-auth slice
is designed; fail explicitly rather than treating them as username/password.
Do not log passwords or authentication data. Names are exact optional OTP
registrations, with distinct child IDs for multiple clients and no generated
atoms. Client-only hosts must start without any default broker or listener.

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

Client inflight/retransmission state has its own future persistence contract;
a durable broker does not make a memory-only client restart-safe. M5 must
qualify each role separately, including incomplete handshakes and loss of
local versus remote session state. No MQTT struct becomes a storage-layer
dependency.

## 10. Development stages and acceptance

Each stage has a bounded implementation plan after review. Completion of a
stage does not imply completion of later rows.

| Stage | Deliverable | Required observable result |
| --- | --- | --- |
| M0: boundary and first codec slice | Sixth library child, architecture rules, common packet envelope, CONNECT/CONNACK for level 5 only, packet limits | Independent wire vectors, every strict prefix, exact suffix, malformed flags/lengths/properties, zero process/socket activity; existing AMQP tests unchanged |
| M1: shared packet surface | PUBLISH and QoS acknowledgements, SUBSCRIBE/SUBACK, UNSUBSCRIBE/UNSUBACK, PING, DISCONNECT; MQTT 5 properties and explicit enhanced-auth rejection | Independent vectors cover both wire directions and packet/property rules; no working client or broker claim |
| M2a: pure client semantics | CONNECT/CONNACK lifecycle, subscriptions/grants, QoS 0/1 send/receive, keep alive, disconnect, session resumption, and reconnect effects | Deterministic events cover success, rejection, uncertainty, inflight resumption, lost sessions, stale generations, and finite buffers without processes or clocks |
| M2b: pure broker semantics | Topic matching, session transitions, routing plans, retained state, Will, expiry, QoS 0/1 admission and delivery effects | Deterministic event sequences prove policy and ordering without clocks, sockets, or processes |
| M3a: independent MQTT 5 client | Host-supervised client owner, outgoing TCP/TLS, bounded API, credentials, QoS 0/1, and in-memory reconnects | Our client exchanges messages with an independent broker; real SUBACK/PUBACK, reconnect, TLS, pressure, and host shutdown pass without a local GravitonMQ broker |
| M3b: embedded MQTT 5 broker | Real session/routing owners, bounded host API, opt-in TCP/TLS listeners, auth integration, QoS 0/1 and in-memory reconnects | Independent clients exchange messages with our broker; native client/broker and host/network paths work; two live broker instances isolate state |
| M4: MQTT 5.0 QoS 2 on both roles | Client and broker QoS 2 state/ownership, takeover and reconnect qualification | PUBREC/PUBREL/PUBCOMP duplicate/disconnect boundaries pass against independent peers in each direction |
| M5: separately designed durability | Persistent broker session/retained/delivery state and client inflight state with actual recovery | Each role's crash/restart and failed/uncertain synchronization prove acknowledgements follow its selected durable boundary |

M1 is shared; M3a depends on M2a and M3b on M2b. Deliver the independent
client first, then the embedded broker, each with its own reviewed plan and
qualification. Splitting M2/M3 preserves the original milestone purposes;
it does not mark any future work complete. Neither role may depend on the
other being started.

MQTT 5.0 optional capabilities must be explicit during M3b: advertise shared
subscriptions and Subscription Identifiers as unavailable until implemented;
advertise inbound Topic Alias Maximum as zero until supported. Features that
cannot be disabled by capability negotiation, including supported packet
properties, message expiry, subscription options, and Will/session behavior,
must work or cause a specification-defined rejection. Never silently ignore
their semantics. Enhanced authentication, shared subscriptions, WebSocket,
clustering, bridges, management APIs/UI, and MQTT-SN are outside the initial
operational milestone. MQTT 3.1 and 3.1.1 remain unsupported in every stage.
For M3a, bound subscription requests to supported QoS and reject unsupported
capability use explicitly. Any unimplemented received property that has
mandatory semantics must follow the specification-defined failure path.

### Deferred QUIC TODO — 2026-10-26

The owner requested recording future client and broker support for reliable
MQTT 5.0 over QUIC streams, plus a realtime state extension over QUIC DATAGRAM
after explicit peer negotiation. Version the DATAGRAM extension separately
and do not present it as standard MQTT capability. See the
[QUIC TODO boundary](../../MQTT_SCOPE.md#todo-quic-support-not-implemented).
This is an unscheduled TODO, not an implementation request or an expansion of
M0 or M3a/M3b TCP/TLS acceptance. Transport mapping and the extension contract
need a later design before code or dependencies are introduced.

### First implementation slice only

After owner review, write the detailed M0 implementation plan first. Its
allowed writes are the new MQTT child, runtime's dependency declaration,
architecture rules and associated tests, scope documentation, and the
existing package-list integration needed for a sixth child. Preserve ongoing
packaging edits and adapt to their final interface. Do not add listeners,
native publish/subscribe functions, Session processes, queue/storage
implementations, a remote-client facade/runtime, or empty-success stubs in M0.

CONNECT/CONNACK coverage includes protocol name/level, reserved and Will
flags, Clean Start representation, Client ID, keep alive, optional
credentials and Will data, packet length boundaries, MQTT 5.0 property
cardinality/location, and MQTT 5.0 reason codes. Decoding CONNECT
does not authenticate, register a Client ID, start a session, or run a timer.
The `:auto` selector only verifies CONNECT level 5; other packet decoding
takes `:v5` explicitly. Do not infer the version from packet contents.

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

Interoperability requires three separately reported paths:

| Path | Evidence required |
| --- | --- |
| Our client → independent MQTT 5.0 broker | Connection and rejection, SUBACK grants, publish/receive/unsubscribe, keep alive, session resumption, and TLS |
| Independent MQTT 5.0 client → our broker | Equivalent broker behavior, routing/retained/Will/expiry, admission/authentication failures, and TLS |
| Our client ↔ our broker | End-to-end native exchange, host embedding, isolation, and cleanup |

The third path alone can hide shared codec/state-machine defects and cannot
qualify either role's interoperability. Test peers are test infrastructure,
not production dependencies. Record their implementation/version, exact
configuration, and observed negotiated capabilities with each result.

Before M3a acceptance, test refused/lost connections, unsuccessful CONNACK,
lower SUBACK grants, negative PUBACK, peer limits, slow/dead host consumers,
finite offline/inflight buffers, unknown completion, reconnect without a
remote session, stale socket/timer events, intentional disconnect, TLS trust
and hostname failure, multiple clients, and client-only host shutdown.

Before M3b acceptance, test slow subscribers, full delivery/Writer queues,
subscriber death, auth callback failure, malformed peers, duplicate Client
IDs, stale connection generations, listener startup failure, reconnects,
retained deletion, Will delivery, expiry, two-instance isolation, and host
shutdown. Before M4 acceptance, add QoS 2 duplicate and reconnect boundaries
on both roles in MQTT 5.0. Before any distribution claim, build all six
archives and start clean external broker-only, client-only, and combined
consumers using the documented embedding paths. Add these operational
consumers when their respective stages exist; M0 package smoke remains
codec/lifecycle evidence only. No package publication is implied.

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

This records the original M0 implementation before the owner narrowed the
protocol boundary to MQTT 5.0 only. It is historical qualification evidence.

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

### MQTT 5.0-only boundary amendment — 2026-10-26

The owner narrowed the protocol boundary to MQTT 5.0 only after the initial
M0 implementation. MQTT 3.1 and 3.1.1 are unsupported in both current code
and future stages. Legacy CONNECT levels and caller version selectors fail
explicitly; their codec branches are removed. The opaque envelope now
enforces minimum-length MQTT 5.0 Remaining Length encoding directly.

All eight required root checks passed under Elixir 1.18.4 / OTP 28.5.0.1:
294 tests, zero failures (80 MQTT tests), zero compilation warnings, 1,690
source references across six applications, and no xref cycles. The four
new boundary tests failed before the change and passed afterward.

Six `0.1.0` archives were rebuilt in
`/tmp/graviton-mqtt5-only-hex-Lxmk2J/packages/artifacts`; actual archive
metadata retained exact internal versions and the MQTT 5.0-only package
description. Separate clean standalone and host-supervised consumers passed
with MQTT 5.0 CONNECT fixtures. An unpacked archive consumer also explicitly
rejected MQTT 3.1/3.1.1 packets and the legacy version selector. Broker
operations, listeners, and durability remain outside M0.

## 12. References

- [OASIS MQTT 5.0 standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html): §§1.5, 2–4, 7; in particular §3.2.2.3.4 Maximum QoS, §4.1 Session State, §4.4 retry, and §4.7 topics.
- Existing [architecture](../../ARCHITECTURE.md), [roadmap](../../ROADMAP.md), and [embedding lifecycle ADR](../../adr/0006-support-standalone-and-embedded-lifecycles.md).
- Existing [opaque message ADR](../../adr/0005-preserve-amqp-message-as-opaque-content.md) and [failure model](../../FAILURE_MODEL.md).

The official versioned MQTT 5.0 HTML reference was retrieved during design
review; Firecrawl returned a cached copy from 2026-10-24. It is a protocol
reference, not evidence that the proposed broker works.
