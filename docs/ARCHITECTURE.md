# Architecture

## Status

GravitonMQ is at Milestone 1. The repository retains the hardened data,
dependency, durability, and OTP composition boundaries from Milestone 0 and
adds a bounded, process-free AMQP 1.0 codec foundation. It still does not
contain a functioning broker, AMQP listener, protocol state machine, queue
machine, or storage engine.

GravitonMQ targets AMQP 1.0. AMQP 0-9-1 operations and concepts such as
exchanges, bindings, `basic.publish`, `basic.consume`, and AMQP 0-9-1 channels
are not the foundation of this design. RabbitMQ is an architectural reference
for ownership, supervision, state-machine/effect separation, storage
boundaries, and fault isolation only. GravitonMQ source is independently
designed and is not copied, translated, transliterated, or mechanically ported
from RabbitMQ.

## Umbrella applications

The umbrella has five child applications, ordered from stable domain concepts
to the public composition root:

| Application | Responsibility |
| --- | --- |
| `graviton_mq_core` | Protocol-independent broker types, logical queue events, outcomes, durable identities, and the storage behaviour |
| `graviton_mq_storage` | Physical storage records and future concrete implementations of the core storage contract |
| `graviton_mq_amqp10` | Exact AMQP 1.0 values, bounded pure value and Open/Begin schema codecs, protocol data, and declarative protocol-facing state |
| `graviton_mq_runtime` | OTP composition and future effect execution across the lower layers |
| `graviton_mq` | Public lifecycle, configuration, architecture tooling, and the product Application callback |

The internal dependency graph is deliberately acyclic. Each arrow means
"depends on":

```text
graviton_mq         -> graviton_mq_runtime
graviton_mq_runtime -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_storage -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_amqp10  -> graviton_mq_core
```

Core knows neither AMQP 1.0 nor runtime or physical-storage modules. Storage
and AMQP 1.0 each depend only on core and do not depend on one another. Runtime
is the first layer allowed to compose all three. The public application depends
only on runtime.

`mix graviton_mq.check_architecture` enforces these rules against actual source
references. It combines compiler xref manifests with parsed Elixir syntax so
typespec-only references are checked as well. The checker also rejects child
application cycles and transport/socket references from the pure AMQP
application. Within the codec namespace it additionally rejects OTP and BEAM
process modules plus direct `spawn`, `spawn_link`, `spawn_monitor`, `send`, and
`receive` forms. This supplements, rather than replaces, the declared
dependency tests and Mix's xref cycle check.

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

The Milestone 1 codec surface is intentionally narrow:

- `Codec.ProtocolHeader.recognize/1` recognizes only raw AMQP protocol ID 0,
  version 1.0.0, and does not negotiate;
- `Codec.Frame.decode/2` validates one AMQP frame envelope while preserving
  its extended header, body, and trailing input exactly;
- `Codec.Value.decode/2` and `encode/2` support `null`, `ushort`, `uint`,
  `ulong`, `string`, `symbol`, `list`, ordered `map`, symbol arrays, and
  described values recursively composed from that subset;
- `Codec.Performative.decode/2` and `encode/2` validate only the Open and Begin
  composite schemas and return dedicated immutable structs whose present
  fields remain exact tagged AMQP values; and
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

Decoding materializes the specification defaults as tagged values: Open's
`max_frame_size` is `uint(4_294_967_295)`, Open's `channel_max` is
`ushort(65_535)`, and Begin's `handle_max` is `uint(4_294_967_295)`. Canonical
encoding uses the numeric descriptors, inserts nulls only for required
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

The future queue machine will be a pure deterministic transition system with
the conceptual interface:

```elixir
{new_state, effects} = GravitonMQ.Queue.Machine.apply(state, command)
```

Commands and effects are protocol-independent. A transition describes work
through effects instead of writing storage, sending network data, or managing
processes itself. Runtime effect executors will eventually interpret those
effects.

Logical persistence data is owned by core. `GravitonMQ.Queue.Event` is a
serializable envelope containing a stable event ID, node ID, sequence, event
type, and protocol-independent data. It contains no protocol session, handle,
channel, or delivery ID; no PID, reference, port, or socket state; and no
checksum, segment offset, or other physical storage metadata.

Physical persistence data is owned by storage. `GravitonMQ.Storage.Record` is
the future boundary for a format version, commit position, record type, encoded
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
acceptance. The Memory and WAL namespaces are future implementation boundaries;
they do not claim to implement this behaviour, and no filesystem I/O, fsync,
segment, encoding, or recovery implementation exists.

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
does not own a second product Application callback. Its supervisor remains
intentionally empty: starting either lifecycle creates only the top-level and
runtime supervision boundaries, with no listener or broker functionality.

The intended later runtime organization is:

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

## Management boundary

Management functionality will use broker APIs and observations outside the
message data path. A future UI or management API must not become a required hop
for transfer, settlement, queue transitions, persistence, or delivery.

## Milestone 1 limits

This codec foundation does not implement TCP, TLS, or WebSocket listeners;
performative schemas beyond Open and Begin; message-section parsing; SASL
negotiation or protocol negotiation; Connection or Session transition
behavior; Link attach, Transfer assembly, Flow, or settlement behavior; a
working queue machine, publisher acceptance, consumer delivery, or Accepted
disposition; filesystem storage, WAL, segments, persistence, fsync, or
recovery; Raft, the `ra` dependency, Khepri, or clustering; Phoenix, a
management UI or REST API; MQTT; or AMQP 0-9-1. The semantic codec subset is
not complete AMQP 1.0 support.

The runtime starts no fake listeners, connection workers, queue workers,
storage workers, effect executors, data directories, or speculative Registries.
The next feature milestone remains separate from this bounded codec work.

## Design constraints

- Standard Elixir/OTP is sufficient for Milestone 1; there are no speculative
  third-party runtime dependencies.
- Link state remains Session-owned rather than one process per Link.
- Pure state machines perform no network or storage side effects.
- The initial delivery target is at-least-once, not exactly-once.
- A later publisher acceptance may be emitted only after its configured
  durability boundary has been reached.
