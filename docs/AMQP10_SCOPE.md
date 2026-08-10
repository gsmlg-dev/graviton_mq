# AMQP 1.0 Scope

## Protocol target and clean-room constraint

GravitonMQ targets AMQP 1.0. Milestone 1 provides only a bounded binary codec
foundation: raw protocol-header recognition, frame-envelope validation, and a
small semantic-value subset plus schema validation for only the Open and Begin
performatives. It does not claim complete protocol compatibility, parse AMQP
messages, negotiate a protocol, or own a listener.

AMQP 0-9-1 is outside the product foundation. In particular,
`exchange.declare`, `queue.bind`, `basic.publish`, `basic.consume`, and AMQP
0-9-1 channels do not define the GravitonMQ protocol model. RabbitMQ is used
only as an architectural reference for ownership, supervision organization,
protocol boundaries, queue abstraction, state/effect separation, storage, and
fault isolation. Its source is not copied, translated, transliterated, or
mechanically ported.

## Exact AMQP value identity

`GravitonMQ.AMQP10.Value` represents a decoded AMQP semantic value as a tagged
struct with `type` and `value` fields. The model currently names these semantic
types:

```text
null
boolean
ubyte ushort uint ulong
byte short int long
float double
decimal32 decimal64 decimal128
char timestamp uuid
binary string symbol
list map array described
```

This makes values such as `%Value{type: :binary, value: bytes}`,
`%Value{type: :string, value: bytes}`, and
`%Value{type: :symbol, value: bytes}` structurally distinct even when their
Elixir binary payloads are equal. The signed and unsigned integer families are
likewise distinct semantic types.

AMQP float and double values retain exact 32- and 64-bit IEEE-754 bit patterns
rather than using an unconstrained BEAM float. This represents NaN, infinities,
signed zero, and binary32 range exactly without treating their fixed-width
payloads as compact-constructor alternatives.

Lists contain tagged AMQP values. Maps retain an ordered list of tagged
key/value pairs, so an AMQP string key and symbol key with equal bytes remain
different keys. `Value.Array` retains both the array's explicit common
`element_type` and its tagged values. Construction enforces that every member
has that exact semantic type. A described array's element type records both
the descriptor and underlying semantic type, and construction requires both to
match every member. `Value.Described` retains an exact tagged descriptor and
tagged value; unknown and extension descriptors therefore remain representable
without assigning them a project-specific type.

Semantic identity is separate from binary encoding width. Compact constructors
such as `uint0` and `smalluint` are alternative encodings of the AMQP `uint`
semantic type and intentionally do not appear in `Value.semantic_types/0`.
The bounded encoder selects compact constructors for its supported subset
without changing the decoded semantic identity.

The Open and Begin performative structs retain every present field as an exact
tagged AMQP value; `nil` represents an absent or explicitly null optional
field. General values, symbols, error conditions, SASL mechanisms, maps,
message sections, described values, and other fields whose exact type must
survive round trips likewise use the tagged value algebra. The codec below
implements only an explicit subset of that larger in-memory model.

## Ownership of AMQP concepts

`graviton_mq_amqp10` owns the representation and future handling of:

- Connection state and protocol negotiation;
- Sessions and their channel and delivery-number spaces;
- Links as state owned by a Session;
- Source and Target termini;
- Flow and credit;
- Transfer framing and delivery association;
- Disposition and settlement;
- AMQP message sections, performatives, errors, outcomes, and SASL concepts.

Connection, Session, Link, Flow, Transfer, and Disposition belong to the
protocol layer even when they cause broker commands or report broker outcomes.
They do not become queue-domain entities.

## Directional Link and Session ownership

The local endpoint and its peer allocate Link handles independently. A Link
therefore has both `local_handle` and `remote_handle` fields, as well as its
typed AMQP string `name`, local `role`, Source, Target, sender settle mode, and
receiver settle mode. Local and remote handle values may differ.

The stable Session-local Link identity is `{name, role}`, where `role` is the
local endpoint's sender or receiver role. Link name alone is not a dispatch
key. Session state uses three explicit structures:

```text
links_by_identity
local_handle_to_link
remote_handle_to_link
```

Both handle indexes point to the same canonical Link identity. Incoming frames
carry the peer's handle and resolve through `remote_handle_to_link`. Outgoing
frames use the Link's locally allocated handle and resolve through
`local_handle_to_link`. Index construction rejects duplicate identities,
duplicate local handles, and duplicate remote handles. A Link with an
unassigned handle may omit that direction from its index. Links remain
Session-owned immutable state; Milestone 0 does not add one process per Link.

Connection state applies the same directional rule to Sessions:

```text
sessions_by_identity
local_channel_to_session
remote_channel_to_session
```

One Session may have different local and remote channels, and incoming and
outgoing lookup use their respective maps. No Connection or Session transition
behavior is implemented.

## Boundary with the broker core

The protocol frontend will eventually resolve Source and Target addresses into
protocol-independent broker addresses and node types. It will translate an
eligible Transfer into a core command and translate AMQP settlement outcomes
into protocol-independent core instructions. The core remains unaware of
performatives, sessions, link handles, delivery tags, delivery IDs, and channel
numbers.

AMQP delivery IDs, link handles, and channel numbers have scope and lifetime
within protocol state. They are not durable queue message IDs, internal durable
delivery references, or storage commit references.

Queue message lifecycle, ordering, durable identity, logical events,
availability, and removal belong to `graviton_mq_core`. Concrete persistence
and physical records belong to `graviton_mq_storage`. OTP processes, effect
execution, and transport integration belong to `graviton_mq_runtime`.

## Message preservation

At the AMQP boundary, `GravitonMQ.AMQP10.Message` retains both the original
encoded message content and the Transfer `message_format`. Its optional parsed
section view uses exact tagged AMQP values and can retain multiple body
sections. Parsing is not implemented in this milestone.

When admitted to the protocol-independent broker model, the encoded bytes stay
authoritative in `GravitonMQ.Core.Message.encoded_content`. The Transfer
message-format is retained through the protocol-neutral `Message.Format`, and
`Message.Index` holds only parsed routing, policy, expiration, durability, and
management fields the broker needs. This permits future forwarding without
losing or rewriting unknown described values, delivery annotations where
applicable, message annotations, properties, application properties, footer,
body-section kind, multiple body sections, or nested semantic type identity.
Core imports no AMQP structs, and `body: term()` is not the AMQP-to-Core message
representation.

## Codec, architecture, and transport

The codec accepts and returns data without owning sockets or OTP processes:

```text
Codec.ProtocolHeader.recognize/1
Codec.Frame.decode/2
Codec.Value.decode/2
Codec.Value.encode/2
Codec.Performative.decode/2
Codec.Performative.encode/2
```

Decode operations distinguish an incomplete prefix from malformed,
unsupported, and limit-exceeded input. Encoding distinguishes unsupported
semantic types from invalid values. Frame size, value size, compound count,
and nesting limits are immutable caller data rather than negotiated or
process-owned state.

The semantic subset is exactly `null`, `ushort`, `uint`, `ulong`, `string`,
`symbol`, `list`, ordered `map`, symbol arrays, and recursively composed
described values with `ulong` or `symbol` descriptors. Signed integers,
booleans, binary, floating and decimal values, timestamps, UUIDs, general
arrays, and the rest of the full AMQP type system return explicit unsupported
errors at this boundary even though the Milestone 0 algebra can represent
them.

The performative facade recognizes the numeric and symbolic descriptors for
Open (`0x10` and `amqp:open:list`) and Begin (`0x11` and
`amqp:begin:list`). Open's ten positions are `container_id`, `hostname`,
`max_frame_size`, `channel_max`, `idle_time_out`, `outgoing_locales`,
`incoming_locales`, `offered_capabilities`, `desired_capabilities`, and
`properties`. Begin's eight positions are `remote_channel`,
`next_outgoing_id`, `incoming_window`, `outgoing_window`, `handle_max`,
`offered_capabilities`, `desired_capabilities`, and `properties`. Decoding
validates mandatory values, exact tagged types, list length, positional nulls,
multiple symbol forms, symbol property keys, and Open's minimum frame size.
Session-context rules, including when Begin must carry `remote_channel`, are
not part of this pure schema boundary.

Absent defaults normalize to tagged values: Open's `max_frame_size` is
`uint(4_294_967_295)`, Open's `channel_max` is `ushort(65_535)`, and Begin's
`handle_max` is `uint(4_294_967_295)`. Encoding uses numeric descriptors,
retains interior null placeholders required by later positions, and removes
trailing nulls. Unknown well-formed descriptors are unsupported by this
facade; known descriptors with malformed schemas and invalid outbound structs
return explicit structured errors. The generic value codec still preserves
unknown described values without assigning them a schema.

The raw AMQP protocol header recognizer accepts only protocol ID 0 and version
1.0.0. SASL and other identifiers or versions are unsupported rather than
negotiated. Frame validation accepts only AMQP frame type 0 and preserves the
extended header, complete body, and trailing bytes exactly. It never invokes
the performative codec automatically and does not decode a message payload.
The authoritative encoded message content likewise remains opaque and
byte-exact.

Transport code will eventually supply bytes to the codec and decide how
results affect a connection process. The source architecture checker rejects
AMQP references to runtime, storage, and transport/socket modules. It also
rejects process facilities in the codec namespace, including direct process
primitives found through parsed syntax. This separation keeps codec tests
deterministic.

## Milestone 1 exclusions

Milestone 1 intentionally excludes:

- performative schemas other than Open and Begin;
- message-section parsing or reconstruction;
- SASL and AMQP protocol negotiation;
- Connection or Session state-machine behavior;
- Link attach negotiation, Flow processing, and credit management;
- Transfer assembly, Disposition processing, and settlement behavior;
- publisher acceptance and consumer delivery;
- TCP, TLS, and WebSocket transports; and
- queue transitions, persistence, recovery, and clustering.

No module or test should imply that these capabilities work or that the
bounded value and Open/Begin schema subset is complete AMQP compatibility. Any
next performative codec slice requires separate design and review and remains
pure, bounded, and independent of protocol processes and transport.
