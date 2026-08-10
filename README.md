# GravitonMQ

GravitonMQ is an embeddable AMQP 1.0 message broker being designed in
Elixir/OTP. Milestone 0 established exact data identity, process ownership,
durability contracts, lifecycle composition, and enforceable application
boundaries. Milestone 1 now provides a deliberately bounded, process-free codec
foundation.

RabbitMQ informs architectural study only. GravitonMQ is independently
implemented and does not copy, translate, transliterate, or mechanically port
RabbitMQ source. This project implements AMQP 1.0, not AMQP 0-9-1; exchanges,
bindings, AMQP 0-9-1 channels, `basic.publish`, and `basic.consume` are not its
protocol foundation.

## Current status: bounded Milestone 1 codec foundation

The repository recognizes the raw AMQP 1.0 protocol header, validates AMQP
frame envelopes without interpreting their bodies, and encodes or decodes a
small semantic-value subset. A bounded schema codec also encodes and decodes
only the Open and Begin performatives as dedicated immutable structs with exact
tagged AMQP field values. It does not negotiate protocols or SASL, parse
message sections, open a listener, execute
Connection/Session/Link behavior, transition a queue, accept a publisher
delivery, deliver to a consumer, persist data, or recover a log.

No module should be read as claiming those features are operational.

## Supported toolchain

The supported and CI-tested development pair is:

- Elixir 1.18.4;
- Erlang/OTP 28.5.0.1.

`.tool-versions` declares the pair. `build/project.exs` centralizes the Elixir
requirement as `~> 1.18.4`, and every child application uses that shared
requirement so the umbrella cannot silently diverge. Broader compatibility is
not claimed until it is tested.

## Umbrella boundaries

| Application | Responsibility |
| --- | --- |
| `graviton_mq_core` | Protocol-independent messages, durable identities, outcomes, logical queue events, and the storage contract |
| `graviton_mq_storage` | Physical record boundary and namespaces for future concrete storage implementations |
| `graviton_mq_amqp10` | Exact AMQP 1.0 values, bounded pure codec operations, protocol data, and declarative directional state |
| `graviton_mq_runtime` | OTP integration boundary and intentionally empty per-instance runtime supervisor |
| `graviton_mq` | Public startup API, configuration, composition supervisor, and sole Application callback |

An arrow points from a consumer toward the child application it depends on:

```text
                         graviton_mq_core
                         ▲             ▲
                         │             │
              graviton_mq_storage   graviton_mq_amqp10
                         ▲             ▲
                         └──────┬──────┘
                                │
                      graviton_mq_runtime
                                ▲
                                │
                         graviton_mq
```

Core has no GravitonMQ child dependency. Storage and AMQP 1.0 depend only on
core. Runtime composes core, storage, and AMQP 1.0. The public application
depends only on runtime. Only `GravitonMQ.Application` owns a product
Application callback.

`mix graviton_mq.check_architecture` enforces actual source references, not
only `mix.exs` declarations. It combines references from compiler xref
manifests with parsed-AST references, catches typespec-only edges and transport
calls, rejects forbidden child-application edges, detects child cycles, and
prevents codec modules from depending on OTP or BEAM process facilities.
The separate Mix xref cycle check remains part of CI.

## Hardened data model

### AMQP value identity

`GravitonMQ.AMQP10.Value` is a tagged struct algebra. It represents `null`,
`boolean`, every AMQP signed and unsigned integer type, `float`, `double`,
`decimal32`, `decimal64`, `decimal128`, `char`, `timestamp`, `uuid`, `binary`,
`string`, `symbol`, `list`, `map`, `array`, and described values.

Float and double payloads retain their exact 32- and 64-bit IEEE-754 patterns,
so NaN, infinities, signed zero, and finite precision survive without relying
on the subset of values representable as BEAM floats.

Consequently, equal Elixir binaries in AMQP `binary`, `string`, and `symbol`
values remain structurally different. Lists and map entries recursively retain
exact types, map entries are stored as typed key/value pairs, described values
retain both descriptor and value, and `Value.Array` records and enforces an
explicit shared element type. For described arrays that type includes the
shared descriptor and underlying semantic type. Unknown or extension
descriptors remain representable.

Compact wire constructors such as `uint0` and `smalluint` are deliberately not
semantic value variants. The bounded encoder chooses the appropriate width for
supported semantic `uint` and `ulong` values without changing their identity.

### Bounded codec surface

The pure codec API is:

```elixir
GravitonMQ.AMQP10.Codec.ProtocolHeader.recognize(bytes)
GravitonMQ.AMQP10.Codec.Frame.decode(bytes, limits)
GravitonMQ.AMQP10.Codec.Value.decode(bytes, limits)
GravitonMQ.AMQP10.Codec.Value.encode(value, limits)
GravitonMQ.AMQP10.Codec.Performative.decode(bytes, limits)
GravitonMQ.AMQP10.Codec.Performative.encode(open_or_begin, limits)
```

Decode operations return `{:ok, value, rest}`, `{:more, n}`, or a structured
`Codec.Error`; encode returns `{:ok, bytes}` or a structured error. The
supported semantic subset is `null`, `ushort`, `uint`, `ulong`, `string`,
`symbol`, `list`, ordered `map`, symbol arrays, and described values composed
from that subset with `ulong` or `symbol` descriptors.

`Codec.Performative` recognizes the standard numeric and symbolic descriptors
for Open (`0x10` and `amqp:open:list`) and Begin (`0x11` and
`amqp:begin:list`). It validates the specification's positional fields and
exact tagged types. Open names `container_id`, `hostname`, `max_frame_size`,
`channel_max`, `idle_time_out`, `outgoing_locales`, `incoming_locales`,
`offered_capabilities`, `desired_capabilities`, and `properties`; Begin names
`remote_channel`, `next_outgoing_id`, `incoming_window`, `outgoing_window`,
`handle_max`, `offered_capabilities`, `desired_capabilities`, and `properties`.
Multiple symbol fields and symbol-keyed property maps retain tagged values.

Absent defaults materialize as tagged values: Open uses
`uint(4_294_967_295)` for `max_frame_size` and `ushort(65_535)` for
`channel_max`, while Begin uses `uint(4_294_967_295)` for `handle_max`.
Encoding emits the numeric descriptor, preserves positional nulls required by
later fields, and omits trailing nulls. Schema failures and unsupported
descriptors return explicit structured errors; incomplete input continues to
return `{:more, n}`. Unknown described values remain lossless in the generic
value codec, but the performative facade accepts only Open and Begin.

Decoding a frame never invokes the performative codec automatically. Frame
bodies, trailing input, and authoritative encoded message content remain
byte-exact and opaque.

Configured limits bound frame size, value size, compound member count, and
nesting depth. Malformed input, unsupported protocol/type surface, configured
limit violations, and invalid semantic values are distinct error classes.
This is not a claim of complete AMQP 1.0 compatibility.

### Directional protocol identity

A `GravitonMQ.AMQP10.Link` records its AMQP string name, local endpoint role,
independently allocated `local_handle` and `remote_handle`, source, target, and
settlement modes. Its stable Session-local identity is:

```text
{AMQP Value.string link name, local role}
```

Session state is declarative and owns all Links. `links_by_identity` is the
canonical map; `local_handle_to_link` selects a Link for an outgoing frame and
`remote_handle_to_link` resolves an incoming peer handle. Duplicate handles in
either direction are invalid. Link state is not a separate process.

Connection state likewise uses `sessions_by_identity`,
`local_channel_to_session`, and `remote_channel_to_session`. One Session may
therefore have different local and remote channel numbers.

Handles, channels, and AMQP delivery IDs are protocol-scoped and are never
durable queue identities.

### Durable identities, events, and records

Core defines stable, serializable structs around binary identity for
`MessageId`, `NodeId`, `DeliveryRef`, and `Queue.EventId`. Queue-event sequence
numbers are non-negative integers. `CommitRef` combines a binary durability
stream ID with a non-negative inclusive position; references from different
streams are not comparable. No PID, port, Erlang reference, arbitrary term,
AMQP handle, channel, or delivery ID is durable identity.

Core owns protocol-independent `GravitonMQ.Queue.Event` values. Storage owns
`GravitonMQ.Storage.Record`, including physical format version, record type,
encoded logical event, commit position, checksum, and segment metadata. The
intended later flow is:

```text
Queue.Machine
  -> emits Queue.Event or another persistence effect
  -> Runtime executes the effect
  -> Storage encodes the logical event as a physical Storage.Record
```

No queue transition or record encoding is implemented yet.

### Storage contract

`GravitonMQ.Core.Storage` has four mandatory operations:

- `append/2` accepts a non-empty ordered batch of logical queue events and
  returns the inclusive `CommitRef` assigned to its final event;
- `sync/2` requests durability through a previously returned reference and
  returns the resulting durable-through reference;
- `durable_through/1` reports the highest contiguous durable position, or
  `:none`; and
- `fold/4` incrementally folds ordered recovery events from the first record
  for `:origin`, or strictly after an exclusive commit-reference cursor.

Append success establishes ordering, not durability. A later sync or
durable-through result provides the correlation boundary. Batches preserve
event order, references from different streams are incomparable, and expected
failures return `{:error, reason}`. Fold-based recovery avoids loading a whole
log into memory. The `Memory` and `WAL` namespaces do not claim to implement
this behavior while they remain future boundaries.

### Settlement instructions

AMQP outcome values map into distinct protocol-independent core instructions.
When parsed rejection info or modified annotations exist, their original
encoded form is required so the translation cannot silently discard fields:

| AMQP 1.0 outcome | Core instruction | Retained information |
| --- | --- | --- |
| Accepted | `Outcome.Retire` | successful terminal removal |
| Released | `Outcome.MakeAvailable` | unchanged availability for another delivery |
| Rejected | `Outcome.Reject` | reason, description, opaque encoded detail, and discard/dead-letter policy |
| Modified | `Outcome.ModifyAndMakeAvailable` | delivery-failed, undeliverable-here, and opaque encoded annotation mutation |

Opaque mutation bytes allow the AMQP adapter to preserve protocol detail
without making core depend on AMQP structs. No settlement behavior is
implemented.

### Message preservation

`GravitonMQ.Core.Message` keeps the original encoded AMQP 1.0 message content
as authoritative opaque bytes. Its protocol-neutral `Format` identifies the
wire family and transfer message-format value, while `Index` contains only
derived routing, policy, expiration, durability, and management fields needed
by the broker.

This permits future forwarding without reconstructing or rewriting unknown
described values, annotations, properties, application properties, footers,
body section kinds, multiple body sections, or exact nested AMQP values. Core
does not import AMQP structs, and a decoded `body: term()` is not the message of
record. Message-section parsing and reconstruction remain later work. See
[ADR 0005](docs/adr/0005-preserve-amqp-message-as-opaque-content.md).

## Standalone and embedded lifecycle

Both modes use the same public API:

```elixir
GravitonMQ.start_link(options)
GravitonMQ.child_spec(options)
```

For standalone use, `GravitonMQ.Application` reads
`:graviton_mq, :default_instance` from application environment and delegates
to `GravitonMQ.start_link/1`. With no overrides, the registered names are
`GravitonMQ.Supervisor` and `GravitonMQ.Runtime.Supervisor`.

```elixir
config :graviton_mq, :default_instance,
  name: GravitonMQ.Supervisor,
  runtime_supervisor_name: GravitonMQ.Runtime.Supervisor
```

For host-supervised embedding, configure the dependency with `runtime: false`
so the dependency does not autonomously start its Application tree:

```elixir
# Host application's mix.exs; choose the appropriate package/source option.
{:graviton_mq, "~> 0.1", runtime: false}
```

The version requirement is illustrative until GravitonMQ is published; use
the same `runtime: false` setting with the source selected by the host.

Then place the instance in the host supervision tree:

```elixir
children = [
  {GravitonMQ,
   name: :primary_graviton_mq,
   runtime_supervisor_name: :primary_graviton_mq_runtime}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

`:name` and `:runtime_supervisor_name` are exact OTP registration names. They
are never generated by converting arbitrary strings to atoms. Two registered
instances can coexist when both names differ; a duplicate top-level name
fails with the ordinary `{:already_started, pid}` result. Each runtime
supervisor is intentionally empty: starting an instance opens no listener and
starts no queue or storage implementation. See
[ADR 0006](docs/adr/0006-support-standalone-and-embedded-lifecycles.md).

## Development and verification

Run from the umbrella root:

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

CI runs the same substantive checks with Elixir 1.18.4 and OTP 28.5.0.1.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [AMQP 1.0 scope](docs/AMQP10_SCOPE.md)
- [Delivery semantics](docs/DELIVERY_SEMANTICS.md)
- [Failure model](docs/FAILURE_MODEL.md)
- [Clean-room research notes](docs/RESEARCH_NOTES.md)
- [Roadmap](docs/ROADMAP.md)
- [Architecture decision records](docs/adr/)

## Next milestone

Milestone 1 stops after the bounded Open-and-Begin performative-schema codec.
The recommended next task is a separately designed and reviewed pure codec
slice for the next required AMQP 1.0 performative schemas. It must not add
protocol state or negotiation, SASL, message parsing, TCP, OTP protocol
processes, queue behavior, storage behavior, or a claim of full AMQP
compatibility.

No project license has been selected. License selection remains an explicit
project-owner decision.
