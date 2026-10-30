# Architecture

The 2026-10-29 [active scope](ACTIVE_SCOPE.md) advances beyond the completed
codec milestones below. [ADR 0007](adr/0007-independent-network-client-application.md)
places both outgoing network runtimes in a separate `graviton_mq_client`
library depending only on core and pure protocol libraries. It supersedes
the future client placement proposed by the MQTT embedding design. Broker
ownership, pure transition/effect boundaries, and historical evidence remain.
See the [capability matrix](CAPABILITIES.md) for actual qualification.

## Status

GravitonMQ has completed Milestone 1 and its bounded End/Close/Error
and Boolean/Detach codec slices. The repository retains the hardened data,
dependency, durability, and OTP composition boundaries from Milestone 0 and
provides a bounded, process-free AMQP 1.0 codec foundation. MQTT M0 established
the pure packet envelope and CONNECT/CONNACK foundation. G1 adds shared MQTT5
packet schemas, pure client transitions and a separate outgoing TCP/TLS runtime.
G2 adds a pure memory broker with topics/session/retained/Will/expiry transitions,
an instance-owned running shell, bounded host operations, auth callbacks and
TCP/TLS. G3 adds pure AMQP Endpoint/SessionEngine transitions, bounded opaque
stream input and an independent outgoing client runtime. Artemis qualification
is recorded in the capability matrix. G4 adds Queue.Machine and the AMQP
memory broker. G5 extends MQTT QoS2 and AMQP scalar/array wire coverage and
protocol boundaries. G6 adds local Linux WAL synchronization and streaming
recovery, broker-owned durable profiles, and an independent MQTT client store.
Qualification, clause inventories and G7 evidence live in the capability matrix.

GravitonMQ targets AMQP 1.0 and MQTT 5.0 only. AMQP 0-9-1 operations and
concepts such as exchanges, bindings, `basic.publish`, `basic.consume`, and
AMQP 0-9-1 channels are not the foundation of this design. RabbitMQ is an architectural reference
for ownership, supervision, state-machine/effect separation, storage
boundaries, and fault isolation only. GravitonMQ source is independently
designed and is not copied, translated, transliterated, or mechanically ported
from RabbitMQ.

## Umbrella applications

The umbrella has seven child applications, ordered from stable domain concepts
to the public composition root:

| Application | Responsibility |
| --- | --- |
| `graviton_mq_core` | Protocol-independent broker types, logical queue events, outcomes, durable identities, and the storage behaviour |
| `graviton_mq_storage` | Physical records, bounded local WAL append/sync/durable-through and streaming recovery |
| `graviton_mq_amqp10` | Exact AMQP 1.0 values, performative/message/frame codecs, and pure endpoint/session/link transitions |
| `graviton_mq_mqtt` | Pure MQTT packet codecs, topic matching and separate role machines; historical 3.1.1 CONNECT/CONNACK codec |
| `graviton_mq_client` | Outgoing host-supervised AMQP1/MQTT5 runtimes and facades, independent of broker runtime/storage/composition |
| `graviton_mq_runtime` | OTP composition, MQTT/AMQP broker effects, local durable adapters, bounded TCP/TLS and auth integration |
| `graviton_mq` | Public lifecycle, configuration, architecture tooling, and the product Application callback |

The internal dependency graph is deliberately acyclic. Each arrow means
"depends on":

```text
graviton_mq         -> graviton_mq_runtime
graviton_mq_runtime -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_storage -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_amqp10  -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_mqtt    -> graviton_mq_core
graviton_mq_client  -> graviton_mq_core / graviton_mq_amqp10 / graviton_mq_mqtt
```

Core knows neither protocol library nor runtime or physical-storage modules.
Storage, AMQP 1.0, and MQTT each depend only on core and do not depend on one
another. Runtime is the first layer allowed to compose all four. The public
application depends only on runtime. The client library depends only on core
and the pure protocol libraries; no broker application depends on the client.

`mix graviton_mq.check_architecture` enforces these rules against actual source
references. It combines compiler xref manifests with parsed Elixir syntax so
typespec-only references are checked as well. The checker also rejects child
application cycles and transport/socket references from the pure AMQP
application. Within the codec namespace it additionally rejects OTP and BEAM
process modules plus direct `spawn`, `spawn_link`, `spawn_monitor`, `send`, and
`receive` forms. This supplements, rather than replaces, the declared
dependency tests and Mix's xref cycle check. All MQTT modules reject process,
transport/socket, and clock dependencies; parsed syntax detects direct Erlang
process/timer and clock primitives as well as typespec-only protocol edges.
The same process/clock restrictions now cover the entire AMQP protocol
application, including its pure Endpoint and SessionEngine. Running client
and broker owners receive explicit effects and supply monotonic time as data.
Parsed Erlang/Kernel calls and compiler xref references enforce that boundary;
the earlier codec-only checks remain in place as additional regressions.

## Historical MQTT M0 boundary

`graviton_mq_mqtt` owns `GravitonMQ.MQTT` and has no Application callback.
`Codec.Frame` validates the common packet envelope and preserves the body and
suffix as opaque bytes. M0's `Codec.Packet` surface was CONNECT/CONNACK.
The active Goal retains MQTT3.1.1 codec regressions and adds MQTT5 operational
schemas; only the runtime is MQTT5-only. MQTT3.1 is unsupported.
Ordered properties, exact binary fields, configured limits, and explicit
incomplete/error results are protocol data. No decode starts processes or
runs clocks, sockets, authentication, client registration, or broker actions.
See [MQTT scope](MQTT_SCOPE.md) and the
[MQTT design](superpowers/specs/2026-10-26-mqtt-broker-embedding-design.md).

## MQTT client and broker target

The owner requires our own MQTT 5.0 network client and broker. Implement their
protocol behavior independently from the standard, without substituting an
existing MQTT library or an external production broker. Standard OTP TCP/TLS
primitives remain infrastructure. Independent MQTT implementations are test
peers for qualification in both directions.

The seven-application graph separates these roles under ADR 0007:

| Layer | Target ownership |
| --- | --- |
| `graviton_mq_mqtt` | Shared pure codecs/validation and separate `GravitonMQ.MQTT.Client` and `GravitonMQ.MQTT.Broker` state machines |
| `graviton_mq_client` | `GravitonMQ.MQTTClient` facade and outgoing `GravitonMQ.Client.MQTT` owner: connections, reconnects and bounded host deliveries |
| `graviton_mq_runtime` | Broker listener/connection/writer shells and the instance owner `GravitonMQ.Runtime.MQTT.Broker` |
| `graviton_mq` | Broker lifecycle and broker-local host API; delegates only to broker runtime |

A host must be able to start the client without a local broker, retained store,
or listener. A broker must accept independent clients without our client being
started. Client network operations and broker-local host operations have
separate ownership and completion contracts. Reconnect preserves each role's
available session state without conflating remote Session Present with local
state or claiming process/VM restart recovery.

G1 covers the original M1/M2a/M3a client path with a bounded packet subset;
M2b/M3b broker behavior is implemented in G2. QoS2 and durability follow separately.
See the [roadmap](ROADMAP.md) and the
[design](superpowers/specs/2026-10-26-mqtt-broker-embedding-design.md#10-development-stages-and-acceptance).

## AMQP protocol state

The protocol layer owns AMQP Connection, Session, Link, Flow, Transfer, and
Disposition concepts. Its codec remains independent of TCP and OTP processes
so binary transformations can be tested as pure code. Connection, Session, and
Link are declarative data structures at this milestone. A Session owns its Link
state; a Link is not initially a process.

AMQP semantic values are represented by the tagged
`GravitonMQ.AMQP10.Value` struct. Compound array and described values use its
`Array` and `Described` structs. Thus `binary`, `string`, and `symbol`, the
signed and unsigned integer types, and all other represented AMQP semantic
types retain their identities through nesting. Compact wire constructors such
as `smalluint` and `uint0` are encoder choices for a supported semantic `uint`,
not separate semantic value types. Arrays enforce one explicit element type;
described arrays retain both the common descriptor and common underlying
semantic type. Float and double values retain exact IEEE-754 bit patterns so
special values and signed zero remain representable.

The current pure codec surface builds on the intentionally narrow Milestone 1
foundation:

- `Codec.ProtocolHeader.recognize/1` recognizes only raw AMQP protocol ID 0,
  version 1.0.0, and does not negotiate;
- `Codec.Frame.decode/2` and `encode/2` validate an AMQP frame envelope while
  preserving its extended header and body; decode retains trailing input,
  and encode validates the supplied size/offset before allocating;
- `Codec.Value.decode/2` and `encode/2` support `null`, `boolean`, all signed
  and unsigned integer types, `timestamp`, `uuid`, `binary`, `string`, `symbol`,
  `list`, ordered `map`, symbol arrays, and
  described values recursively composed from that subset;
- `Codec.Performative.decode/2` and `encode/2` validate all nine transport
  composite schemas and return dedicated immutable structs whose
  present fields remain exact tagged AMQP values, with an optional validated
  nested Error in Detach, End, Close or Rejected;
- pure Attach/Terminus and DeliveryState helpers validate Source/Target,
  Received and the four nontransactional outcomes;
- `Codec.Message` validates the nine format-0 section schemas and ordering,
  retaining the original encoded message as authoritative content; and
- immutable limits bound frames, values, compound counts, and nesting.

The performative codec accepts Open's numeric descriptor `0x10` or symbolic
descriptor `amqp:open:list` and Begin's numeric descriptor `0x11` or symbolic
descriptor `amqp:begin:list`. Open's ordered fields are `container_id`,
`hostname`, `max_frame_size`, `channel_max`, `idle_time_out`,
`outgoing_locales`, `incoming_locales`, `offered_capabilities`,
`desired_capabilities`, and `properties`. Begin's ordered fields are
`remote_channel`, `next_outgoing_id`, `incoming_window`, `outgoing_window`,
`handle_max`, `offered_capabilities`, `desired_capabilities`, and `properties`.
Mandatory fields, exact field types, positional nulls, multiple symbol values,
symbol-keyed property maps, field counts, and Open's minimum frame size are
validated without applying Connection or Session transition rules.

End accepts numeric descriptor `0x17` or symbolic descriptor `amqp:end:list`,
and Close accepts numeric descriptor `0x18` or symbolic descriptor
`amqp:close:list`. Each has one optional Error field. The nested Error accepts
numeric descriptor `0x1D` or symbolic descriptor `amqp:error:list`; its three
positions are mandatory tagged symbol `condition`, optional tagged string
`description`, and optional ordered symbol-keyed tagged map `info`. Extension
condition symbols are accepted. Canonical encoding emits numeric descriptors
and preserves the map entry order and any required interior null.

End and Close remain immutable protocol data. Decoding them does not end a
Session, close a Connection, dispatch a frame, or affect runtime ownership.

Detach accepts numeric descriptor `0x16` or symbolic descriptor
`amqp:detach:list`. Its positions are required tagged `uint` `handle`, tagged
Boolean `closed` defaulting to false, and optional Error. Canonical encoding
omits the false default while preserving a null hole when Error is present.
Shared Error validation reports Detach's outer Error field at index `2`,
preserving End/Close's index `0` and the nested Error's own indexes. Decoding
Detach does not interpret a handle in Session state or detach a live Link.

Boolean scalar constructors normalize to the existing tagged Boolean type.
Encoding uses compact `0x41` and `0x42`; `0x56` accepts only payload `0` or `1`.
The shared Boolean subset includes recursive lists, maps, described values,
Open/Begin properties, and nested Error info. Schema map keys remain symbols
and descriptor types remain `ulong` or `symbol`. G5 extends the original
symbol-only arrays to homogeneous scalar, compound, nested-array and described
constructors. It also adds exact-bit float/double/decimal values and Unicode
scalar chars. These remain pure data transformations with pre-allocation
budgets, independent of runtime interpretation.

Error is unsupported as a top-level performative. Attach/Source/Target, Flow,
Transfer/Disposition and delivery states/outcomes are pure data. Transfer keeps
an absent settled flag distinct from explicit false; continuation inheritance
belongs to the endpoint. Message parsing is opt-in, never implicit in frame
decoding. The original bare-message encoding is retained; only constructing a
new message uses canonical section encoding. Aggregate message byte budgets
are preflighted before allocating child encodings. No credit, window,
fragmentation or settlement transition is executed by these schemas.

Decoding materializes the specification defaults as tagged values: Open's
`max_frame_size` is `uint(4_294_967_295)`, Open's `channel_max` is
`ushort(65_535)`, Begin's `handle_max` is `uint(4_294_967_295)`, and Detach's
`closed` is tagged Boolean false. Canonical encoding uses the numeric
descriptors, inserts nulls only for required
positional holes, and trims trailing nulls. Incomplete prefixes return a
positive byte requirement, while malformed, unsupported, limit-exceeded, and
invalid-value cases return structured error data. Unknown described values
remain lossless in the generic value layer but are unsupported by this bounded
schema facade. Other Milestone 0 semantic types stay representable in memory
but are explicitly unsupported by this bounded codec.

The frame decoder never interprets its body and does not invoke the
performative codec implicitly. Frame bodies and the original encoded AMQP
message content therefore remain opaque and byte-exact.

An AMQP Link has independently allocated `local_handle` and `remote_handle`
values. Its Session-local stable identity is `{name, role}`, where `name` is a
typed AMQP string value and `role` is the local endpoint's role. A Session
stores Links canonically in `links_by_identity` and maintains
`local_handle_to_link` and `remote_handle_to_link` indexes. Incoming frames use
the remote-handle index; outgoing frames use the local handle. Duplicate
handles in either direction are invalid.

Likewise, a Connection stores Sessions canonically in
`sessions_by_identity` and maintains `local_channel_to_session` and
`remote_channel_to_session`. One Session may therefore have different local
and remote channel numbers. AMQP delivery IDs, handles, and channels are
protocol-scoped identifiers and are never durable queue identities.

AMQP Source and Target addresses will eventually be resolved by the protocol
frontend to protocol-independent broker node addresses and node types. The core
does not interpret AMQP performatives or transport state.

## Message preservation and durable identity

`GravitonMQ.Core.Message` keeps the original encoded protocol message content
as authoritative opaque bytes. Its fields are:

- `id`, a stable `GravitonMQ.Core.MessageId`;
- `encoded_content`, the unchanged message bytes;
- `wire_format`, a protocol-neutral `Message.Format` containing a binary
  protocol-family name and a non-negative format identifier;
- `index`, a `Message.Index` containing only parsed routing, policy,
  expiration, durability, and management data needed by the broker; and
- `durable?`, the broker durability attribute.

The protocol adapter retains the AMQP Transfer `message-format` in the opaque
format identifier. Preserving the original bytes allows later forwarding
without losing or rewriting unknown described values, delivery and message
annotations where applicable, properties, application properties, footer,
body-section kind, multiple body sections, or exact nested AMQP value types.
The parsed index is derived and is not a replacement for those bytes. Core does
not import AMQP message-section structs, and a decoded `body: term()` is not the
message of record.

Durable broker identities use stable serializable values:

- `GravitonMQ.Core.MessageId`, `GravitonMQ.Core.NodeId`,
  `GravitonMQ.Core.DeliveryRef`, and `GravitonMQ.Queue.EventId` wrap binary
  values;
- a queue event has a non-negative per-node logical `sequence`; and
- `GravitonMQ.Core.CommitRef` contains a binary `stream_id` and non-negative
  ordered `position`.

PIDs, Erlang references, ports, arbitrary terms, AMQP handles, AMQP delivery
IDs, and AMQP channel numbers are not durable identities. Runtime-only
identities may remain opaque within `graviton_mq_runtime`, but they do not cross
the persistence boundary.

## State, effects, logical events, and physical storage

The queue machine is a pure deterministic transition system with the interface:

```elixir
{new_state, effects} = GravitonMQ.Queue.Machine.apply(state, command)
```

Commands and effects are protocol-independent. A transition describes work
through effects instead of writing storage, sending network data, or managing
processes itself. Runtime queue owners interpret those effects after the
selected memory or durable responsibility boundary.

Logical persistence data is owned by core. `GravitonMQ.Queue.Event` is a
serializable envelope containing a stable event ID, node ID, sequence, event
type, and protocol-independent data. It contains no protocol session, handle,
channel, or delivery ID; no PID, reference, port, or socket state; and no
checksum, segment offset, or other physical storage metadata.

Physical persistence data is owned by storage. `GravitonMQ.Storage.Record` is
the boundary for a format version, commit position, record type, encoded
logical event, checksum, and segment identity. Core neither depends on storage
nor constructs physical records. The intended flow is:

```text
Queue.Machine
  -> emits a protocol-independent Queue.Event or persistence effect
  -> Runtime executes the effect
  -> Storage converts the logical event into a physical Storage.Record
```

`GravitonMQ.Core.Storage` defines a mandatory, protocol-independent durability
contract:

- `append/2` appends one non-empty ordered event batch and returns the inclusive
  `CommitRef` assigned to the batch's last event;
- `sync/2` requests synchronization through a previously returned reference
  and returns the highest contiguous durable-through reference;
- `durable_through/1` reports that boundary, or `:none` before any position is
  known durable; and
- `fold/4` incrementally folds recovery events in ascending reference order;
  `:origin` starts at the first record, while a supplied commit reference is an
  exclusive cursor and resumes strictly after that record.

Append success establishes order, not durability. References are ordered only
within the same `stream_id`; references from different streams are not
comparable. An appended batch preserves event order and is correlated to later
durability through its returned last reference. Expected failures return
`{:error, reason}` and cannot be converted into successful publisher
acceptance. `Storage.WAL` implements the contract for a bounded local Linux
stream with checksummed records, a separately published durable checkpoint,
exclusive inode ownership and streaming replay. Its separate opaque-record API
stores protocol snapshots without importing protocol types into storage.
`Storage.Memory` remains a historical namespace; the running memory AMQP queue
uses the pure Queue.Machine directly and makes no disk claim. See
[ADR 0008](adr/0008-local-durability-profiles.md) for filesystem certainty,
failure handling and the independently owned client store.

## OTP ownership and embedding lifecycle

Only `graviton_mq` owns the public product Application callback. Standalone
startup and host-supervised embedding use the same public lifecycle:

```text
GravitonMQ.start_link(options)
GravitonMQ.child_spec(options)

GravitonMQ.Application
`-- GravitonMQ.Supervisor
    `-- GravitonMQ.Runtime.Supervisor
```

`GravitonMQ.Application` reads the default instance options from
`Application.get_env(:graviton_mq, :default_instance, [])` and delegates to
`GravitonMQ.start_link/1`. For embedding, a host configures the dependency with
`runtime: false` and places `{GravitonMQ, options}` in its own supervision tree.

```elixir
# host mix.exs
{:graviton_mq, "~> 0.1", runtime: false}

# host Application supervision tree
children = [
  {GravitonMQ,
   name: :primary_graviton_mq,
   runtime_supervisor_name: :primary_graviton_mq_runtime}
]
```

The version requirement is illustrative until GravitonMQ is published; the
same `runtime: false` setting applies to the package or source selected by the
host.

The naming strategy is explicit: `:name` is the exact registration name of the
top-level supervisor and `:runtime_supervisor_name` is the exact registration
name of that instance's runtime supervisor. Defaults are
`GravitonMQ.Supervisor` and `GravitonMQ.Runtime.Supervisor`. The implementation
does not create atoms from strings or derive new atom names from an instance
identifier. Differently named pairs can coexist. Reusing the top-level `:name`
fails with the normal predictable `{:already_started, pid}` result; a duplicate
runtime-supervisor name fails while starting that child. The top-level child
specification is scoped by the supplied top-level name.

The lower-level applications are libraries. Runtime exposes its supervisor but
does not own a second product Application callback. Without `:mqtt` options,
its supervisor preserves the historical empty boundary. Explicit configuration
adds the functioning MQTT memory broker; listeners are individually opt-in.
See the [broker guide](MQTT_BROKER.md) and
[G2 ownership specialization](superpowers/plans/2026-10-30-mqtt-memory-broker.md).

G2 uses one supervised broker owner around a deterministic aggregate. Logical
sessions remain separate Client-ID-indexed records independent of socket
lifetimes. One transition commits retained mutation and all recipient memory
obligations after authorization. This serial owner bounds throughput and loses
all state on owner/VM failure. `:rest_for_one` teardown fences old connections,
writers and authorization operations before a replacement owner serves traffic.
Listener and individual connection failures preserve the owner. This MQTT
topology specializes the original design; AMQP Session-owned Link and separate
channel/handle allocation rules remain intact.

The runtime executes explicit authentication/authorization requests in finite
supervised tasks. Parser, writers, pending responsibilities, host demand and
outstanding deliveries have separate bounds. Slow socket I/O runs outside the
owner; finite writer credits are reserved before enqueue. Host references bind
to the instance owner and monitored caller. A host ACK retires responsibility
without adding demand. No MQTT memory ACK represents disk durability.

The intended later AMQP runtime organization is:

```text
GravitonMQ.Runtime.Supervisor
|-- InfrastructureSupervisor
|-- StorageSupervisor
|-- NodeSupervisor
|-- ListenerSupervisor
`-- ConnectionSupervisor
    `-- ConnectionTree
        |-- Connection
        |-- Writer
        `-- SessionSupervisor
            `-- Session
```

Each accepted connection will eventually receive an isolated supervision
tree. A connection-level Writer will own outbound socket writes, and each AMQP
Session will have one process. Queue processes will live under a node-oriented
boundary, not beneath a connection. These are documented future ownership
rules, not functioning processes at this milestone.

## G4 AMQP memory ownership specialization — 2026-10-30

The initial AMQP broker now has functioning queue, connection and effect owners:

```text
Runtime.Supervisor
`-- AMQP10.Supervisor (rest_for_one)
    |-- Queue owners (one per configured address)
    |-- finite authorization Task.Supervisor
    |-- Broker Manager (routes, authorization, delivery leases)
    |-- connections DynamicSupervisor
    |   `-- Connection + bounded Writer
    `-- opt-in TCP/TLS listeners
```

The preceding Session-process diagram describes later ownership, not the G4
topology. G4 Sessions and their Links remain pure state within each Connection;
there is no Link worker and no empty Session supervisor. Protocol transitions
and credit/sequence arithmetic stay in Endpoint/SessionEngine. The Manager
asks the pure endpoint for capacity and Queue.Machine for pull claims.
Queue owners perform no socket work; their memory transitions consume logical
events without implying append, synchronization or durable-through.

The Manager owns queue leases and monitors Connections. It processes each
Connection's ordered final outcome events before DOWN releases remaining
leases. Monitoring a Connection independently in Queue would race DOWN with
the Manager's final settlement; queues instead monitor their settlement
coordinator and release all its leases on Manager failure. Old asynchronous
authorization/operation results retain the opaque admission token, including
Session incarnation, so Session/Link identity reuse cannot affect a new lease.

Queue child failure restarts later owners. Manager failure closes old sockets
and releases claims in surviving queues; surviving memory is not crash recovery
or disk durability. Event handoffs survive normal Connection exit until the
Manager consumes them. Commands/events and retained state use bounded opaque
ETS handoffs, keeping encoded content out of OTP debug rings. Passive reads
poll with finite per-turn byte/frame work; writers have independent admission
credits and finite send timeouts. Pump waves count inspected routes, and a
nonresponsive Connection has a bounded close watchdog.

The [broker guide](AMQP10_BROKER.md) defines the initial profile. G6 adds a
broker-wide WAL directory owner and per-queue logical event streams. Queue
effects append events, synchronize their boundary, then release Accepted and
delivery effects; append alone cannot produce durable acceptance. Streaming
replay rebuilds the pure machine and synchronizes lost-lease releases before
serving. MQTT uses a separate opaque snapshot stream, preserving the core
Queue.Event/storage-record distinction. Failed or uncertain I/O freezes the
affected owner. See [ADR 0008](adr/0008-local-durability-profiles.md).
The G5–G7 clause inventories and fault/resource qualification are recorded in
the [capability matrix](CAPABILITIES.md) at their exact selected-profile scope.

## Management boundary

Management functionality will use broker APIs and observations outside the
message data path. A future UI or management API must not become a required hop
for transfer, settlement, queue transitions, persistence, or delivery.

## Milestone 1 limits

The historical Milestone 1 codec foundation did not implement TCP, TLS, or WebSocket listeners;
performative schemas beyond Open and Begin; message-section parsing; SASL
negotiation or protocol negotiation; Connection or Session transition
behavior; Link attach, Transfer assembly, Flow, or settlement behavior; a
working queue machine, publisher acceptance, consumer delivery, or Accepted
disposition; filesystem storage, WAL, segments, persistence, fsync, or
recovery; Raft, the `ra` dependency, Khepri, or clustering; Phoenix, a
management UI or REST API; MQTT broker behavior; or AMQP 0-9-1. The semantic
codec subset is not complete AMQP 1.0 support.

At that historical boundary the runtime started no listeners, connection workers, queue workers,
storage workers, effect executors, data directories, or speculative Registries.
The active Goal and current MQTT runtime above supersede those historical
feature exclusions without changing their original acceptance evidence.

## Design constraints

- Standard Elixir/OTP is sufficient for Milestone 1; there are no speculative
  third-party runtime dependencies.
- Link state remains Session-owned rather than one process per Link.
- Pure state machines perform no network or storage side effects.
- The initial delivery target is at-least-once, not exactly-once.
- A later publisher acceptance may be emitted only after its configured
  durability boundary has been reached.
