# ADR 0002: Separate AMQP 1.0 from the broker core

- Status: Accepted
- Date: 2026-07-13
- Updated: 2026-07-14

## Context

AMQP Connection, Session, Link, Flow, Transfer, Disposition, values, and
message sections have scopes and identity defined by the wire protocol. Queue
admission, stable broker identity, availability, policy, and removal remain
meaningful without a particular connection or link. Sharing one data model
would make durable broker state depend on transient protocol identifiers and
would obscure which layer owns lossless AMQP representation.

AMQP semantic identity also cannot be inferred from ordinary Elixir values.
For example, AMQP `binary`, `string`, and `symbol` may carry equal Elixir
binaries but remain distinct wire-level values. Compact constructors such as
`smalluint` are encoding choices, not additional semantic types.

The product targets AMQP 1.0. AMQP 0-9-1 exchanges, bindings, publishing
methods, and channels are not an appropriate foundation.

## Decision

Place exact AMQP values, frames, performatives, errors, outcomes, messages,
and protocol-facing state in `graviton_mq_amqp10`. Place
protocol-independent messages, durable identities, delivery instructions,
addresses, node types, logical queue events, and the storage contract in
`graviton_mq_core`.

Represent AMQP semantic values with the tagged `GravitonMQ.AMQP10.Value`
algebra. It distinguishes signed and unsigned integer types, binary, string,
symbol, recursive lists and maps, explicitly typed arrays, and described
values. Described-array element identity includes both its common descriptor
and its common underlying semantic type. Unknown descriptors remain data.
Float and double retain exact fixed-width IEEE-754 bits, including special
values and signed zero. Future encoding-width selection for types with compact
constructors is outside this algebra.

Peers allocate Link handles independently. A Link therefore records
`local_handle` and `remote_handle`; its stable Session-local identity is
`{AMQP Value.string link name, local endpoint role}`. A Session owns Link state
in `links_by_identity` and keeps separate local- and remote-handle indexes.
Incoming peer handles resolve through the remote index; outgoing frames use
the local handle. A Connection likewise keeps canonical Session identity plus
separate local- and remote-channel indexes.

The protocol frontend will resolve Source and Target addresses to broker
addresses and node types, map eligible Transfers to core commands, and map
AMQP Accepted, Released, Rejected, and Modified outcomes into distinct core
instructions. Opaque encoded mutation data retains protocol detail without
introducing an AMQP dependency into core.

AMQP delivery IDs, handles, and channels will not serve as durable broker
identities. Core stable message, node, event, and delivery identities are
serializable values of its own. The codec will remain independent of TCP and
OTP processes.

The message-of-record decision is specified separately in
[ADR 0005](0005-preserve-amqp-message-as-opaque-content.md).

## Consequences

Protocol representation and broker semantics can be tested independently.
Translation at the frontend is explicit work rather than an implicit shared
model. Local and peer numbering may differ without ambiguity, and Link state
remains Session-owned instead of becoming one process per Link.

Some concepts have separate AMQP and broker forms. This duplication is
intentional when identity, lifetime, serialization, or error semantics differ.
The data structures do not implement codec, Connection, Session, Link,
Transfer, or settlement behavior in Milestone 0.
