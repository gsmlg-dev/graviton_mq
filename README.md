# GravitonMQ

GravitonMQ provides independently usable AMQP 1.0 and MQTT 5.0 brokers and
native Elixir/OTP network clients under the profiles documented below. This
project implements their protocol behavior using standard OTP networking/TLS primitives.
The network client runs independently of a local broker; both roles
share pure protocol codecs and have separate runtime owners.

The owner-authorized [G0–G7 Goal](docs/ACTIVE_SCOPE.md) advances beyond the
historical codec milestones. [Current capabilities](docs/CAPABILITIES.md)
distinguish implementation, unit checks, independent-peer interoperability,
and recovery evidence for each role. [ADR 0007](docs/adr/0007-independent-network-client-application.md)
defines an independently installable client library without broker dependencies.

Milestone 0 established exact data identity, process ownership,
durability contracts, lifecycle composition, and enforceable application
boundaries. Milestone 1 now provides a deliberately bounded, process-free codec
foundation. The first post-Milestone-1 codec slice extends that foundation with
End, Close, and nested Error schema handling. The subsequent bounded slice adds
Boolean scalar wire support and the Detach schema. MQTT M0 adds a pure
MQTT 5.0 packet envelope and CONNECT/CONNACK codec foundation.

RabbitMQ informs architectural study only. GravitonMQ is independently
implemented and does not copy, translate, transliterate, or mechanically port
RabbitMQ source. This project implements AMQP 1.0, not AMQP 0-9-1; exchanges,
bindings, AMQP 0-9-1 channels, `basic.publish`, and `basic.consume` are not its
protocol foundation.

## Current status: MQTT5 and AMQP1 brokers and independent clients

The repository recognizes the raw AMQP 1.0 protocol header, validates AMQP
frame envelopes without interpreting their bodies, and encodes or decodes
bounded semantic values, including all scalar families and homogeneous arrays.
A bounded schema codec also encodes and decodes
all nine transport performatives as dedicated immutable structs with exact
tagged fields, Source/Target and delivery-state/outcome data. A separate message
codec validates format-0 sections and retains the authoritative original bytes.
Pure Endpoint/SessionEngine transitions add bounded channels, Session-owned
Links, credit/windows, fragmented transfer, settlement and explicit effects.
`GravitonMQ.AMQP10Client` interprets those effects over outgoing TCP/verified
TLS without broker dependencies. Its initial no-SASL, volatile format-zero
profile has real Artemis/Proton send/receive and failure evidence. An opt-in
AMQP memory broker adds named queues, bounded admission/credit/settlement,
release/redelivery, peer-disconnect responsibility and TCP/TLS. Its independent
Proton qualification and remaining limits are recorded in the matrix.
AMQP client reconnect/resumption remains unavailable. The opt-in broker
`local_durable` profile adds queue event replay through the real WAL and sync
boundary; see [ADR 0008](docs/adr/0008-local-durability-profiles.md).

The separate MQTT library provides CONNECT/CONNACK, MQTT5 QoS0/1/2 packet
schemas, topic matching and separate pure client/broker transitions. Historical MQTT3.1.1 CONNECT/CONNACK
codec compatibility is retained; MQTT3.1 and legacy runtime are unsupported.
`graviton_mq_client` adds a host-supervised MQTT5 TCP/TLS client with bounded
operations/deliveries, keep alive and controlled memory-session reconnect.
An opt-in MQTT5 memory broker provides topics, sessions, retained publications,
Will/expiry, bounded host operations, explicit auth callbacks and TCP/TLS.
See the [client guide](docs/MQTT_CLIENT.md), [broker guide](docs/MQTT_BROKER.md)
and [qualification matrix](docs/CAPABILITIES.md). The [AMQP client guide](docs/AMQP10_CLIENT.md)
documents admission, outcomes, credit and its bounded profile; the
[AMQP broker guide](docs/AMQP10_BROKER.md) covers queue responsibility and embedding.
Opt-in local persistence adds MQTT session/retained/Will/inflight recovery and
client operation identities. Its sync and recovery evidence is tracked
separately from memory behavior. The selected profiles and their actual
qualification limits are recorded in the capability matrix.

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
| `graviton_mq_core` | Protocol-independent messages, durable identities, outcomes, deterministic queue transitions, logical events, and the storage contract |
| `graviton_mq_storage` | Bounded Linux WAL, checksums, explicit sync/durable-through, streaming recovery and physical records |
| `graviton_mq_amqp10` | Exact AMQP 1.0 values, bounded pure codec operations, protocol data, and declarative directional state |
| `graviton_mq_mqtt` | Pure MQTT packet codecs, limits, topics and role transitions; historical 3.1.1 CONNECT/CONNACK codec |
| `graviton_mq_client` | Independent host-supervised AMQP1/MQTT5 network clients; no broker dependencies or Application callback |
| `graviton_mq_runtime` | Instance-owned MQTT/AMQP brokers, memory/local durability profiles, queue owners, auth integration, bounded TCP/TLS connections and writers |
| `graviton_mq` | Public startup and broker host APIs, configuration, composition, and sole Application callback |

An arrow points from a consumer toward the child application it depends on:

```text
graviton_mq         -> graviton_mq_runtime
graviton_mq_runtime -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_storage -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_amqp10  -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_mqtt    -> graviton_mq_core
graviton_mq_client  -> graviton_mq_core / graviton_mq_amqp10 / graviton_mq_mqtt
```

Core has no GravitonMQ child dependency. Storage, AMQP 1.0, and MQTT depend only
on core. Runtime composes core, storage, AMQP 1.0, and MQTT. The public application
depends only on runtime. Only `GravitonMQ.Application` owns a product
Application callback. The independent client library depends only on core and
pure protocol libraries; using it never starts the broker product.

`mix graviton_mq.check_architecture` enforces actual source references, not
only `mix.exs` declarations. It combines references from compiler xref
manifests with parsed-AST references, catches typespec-only edges and transport
calls, rejects forbidden child-application edges, detects child cycles, and
prevents codec modules from depending on OTP or BEAM process facilities.
All AMQP and MQTT modules reject process, transport, and clock dependencies,
including direct Erlang primitives. The protocol libraries cannot reference
one another. The separate Mix xref cycle check remains part of CI.

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
GravitonMQ.AMQP10.Codec.Frame.encode(frame, limits)
GravitonMQ.AMQP10.Codec.Value.decode(bytes, limits)
GravitonMQ.AMQP10.Codec.Value.encode(value, limits)
GravitonMQ.AMQP10.Codec.Performative.decode(bytes, limits)
GravitonMQ.AMQP10.Codec.Performative.encode(performative, limits)
```

Decode operations return `{:ok, value, rest}`, `{:more, n}`, or a structured
`Codec.Error`; encode returns `{:ok, bytes}` or a structured error. The
supported semantic subset is `null`, `boolean`, all signed/unsigned integer
types, exact-bit `float`, `double`, and decimal types, Unicode scalar `char`,
`timestamp`, `uuid`, `binary`, `string`, `symbol`, `list`, ordered `map`,
homogeneous arrays, and described values composed from that subset with
`ulong` or `symbol` descriptors.

`Codec.Performative` recognizes the standard numeric and symbolic descriptors
for Open (`0x10` and `amqp:open:list`), Begin (`0x11` and
`amqp:begin:list`), Attach (`0x12` and `amqp:attach:list`), Flow (`0x13` and
`amqp:flow:list`), Transfer (`0x14` and `amqp:transfer:list`), Disposition
(`0x15` and `amqp:disposition:list`), Detach (`0x16` and `amqp:detach:list`), End (`0x17` and
`amqp:end:list`), and Close (`0x18` and `amqp:close:list`). It validates the
specification's positional fields and exact tagged types. Open names
`container_id`, `hostname`, `max_frame_size`,
`channel_max`, `idle_time_out`, `outgoing_locales`, `incoming_locales`,
`offered_capabilities`, `desired_capabilities`, and `properties`; Begin names
`remote_channel`, `next_outgoing_id`, `incoming_window`, `outgoing_window`,
`handle_max`, `offered_capabilities`, `desired_capabilities`, and `properties`.
Detach fields are mandatory tagged `uint` `handle`, defaulted tagged Boolean
`closed`, and optional Error. End and Close each have one optional Error field.
Multiple symbol fields and symbol-keyed property maps retain tagged values.

Error is nested protocol data rather than a top-level performative. Its
mandatory condition is an exact tagged symbol; its optional description and
info retain tagged string and ordered symbol-keyed map values from the bounded
value subset. The codec accepts numeric and symbolic Error descriptors only
inside Detach, End, Close, Rejected or a reserved rejected delivery annotation
and canonically emits the numeric descriptor for typed data.

Boolean constructors `0x41`, `0x42`, and `0x56` with payload `0` or `1`
normalize to tagged Boolean values; other `0x56` payload octets are malformed.
Canonical encoding uses `0x41` and `0x42`. Boolean is supported recursively in
the shared value subset, including Open/Begin properties and nested Error info.
Homogeneous arrays support scalar, compound, nested-array and described
constructors; null elements are forbidden by AMQP. Exact semantic types remain
distinct. Float/double and decimal values carry bit binaries to preserve NaN
payloads, signed zero and decimal encodings; char carries a Unicode scalar.

Absent defaults materialize as tagged values: Open uses
`uint(4_294_967_295)` for `max_frame_size` and `ushort(65_535)` for
`channel_max`, while Begin uses `uint(4_294_967_295)` for `handle_max` and
Detach uses tagged Boolean false for `closed`.
Encoding emits the numeric descriptor, preserves positional nulls required by
later fields, and omits trailing nulls. Schema failures and unsupported
descriptors return explicit structured errors; incomplete input continues to
return `{:more, n}`. Unknown described values remain lossless in the generic
value codec. The performative facade accepts the nine transport performatives;
Error, Source/Target, delivery states and outcomes remain nested data.

`Codec.Message.decode/2` parses a complete format-0 message into an optional
tagged section view without changing its original content. `encode/2` returns
that authoritative content unchanged; `encode_sections/2` constructs a new
message with validated section order and an aggregate byte budget checked
before allocating child encodings. It must not replace forwarding the original
bare-message bytes. The bounded application-properties profile supports simple
scalar/null values. Other message section values use the shared bounded value
codec, including homogeneous arrays.

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
implemented AMQP queue flow is:

```text
Queue.Machine
  -> emits Queue.Event or another persistence effect
  -> Runtime executes the effect
  -> Storage encodes the logical event as a physical Storage.Record
```

Queue.Machine now implements bounded queue transitions. The AMQP runtime
interprets their effects; the opt-in local durable profile encodes and
synchronizes logical events before successful responsibility effects.

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
log into memory. `Storage.WAL` implements bounded local Linux append, sync,
durable-through and streaming recovery. `Storage.Memory` remains a historical
namespace; memory queue operation has no disk durability claim.

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
without making core depend on AMQP structs. Queue.Machine and the AMQP runtime
implement retirement, release, discard-on-rejection and Modified redelivery.
Dead-letter policy is explicitly unsupported in this profile.

### Message preservation

`GravitonMQ.Core.Message` keeps the original encoded AMQP 1.0 message content
as authoritative opaque bytes. Its protocol-neutral `Format` identifies the
wire family and transfer message-format value, while `Index` contains only
derived routing, policy, expiration, durability, and management fields needed
by the broker.

The AMQP forwarding view preserves bare-message properties, application
properties, body sections and footer bytes exactly, including noncanonical
encodings. Only mutable Header/annotation fields are adjusted according to
delivery policy; the original queue record remains authoritative. Protocol
message-section codecs validate their own schemas. Core does not import AMQP
structs, and a decoded `body: term()` does not replace the message of record. See
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
fails with the ordinary `{:already_started, pid}` result. Without `:mqtt`
configuration, each runtime retains the historical empty boundary. Explicit
`:mqtt` options enable the memory broker and any configured listeners;
no AMQP queue or storage engine starts. See
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

## Releases and Hex packages

`VERSION` is the canonical version for the umbrella and all seven child
applications. The manual **Release** workflow accepts a SemVer `version`
(without a `v` prefix or build metadata) and a `git_ref` branch, defaulting to
`main`. Configure the repository or organization secret `HEX_API_KEY` with
permission to publish all seven packages, and allow the workflow token to push
the release commit and tag to the selected branch.

The workflow updates `VERSION`, checks the source, builds every package, and
compiles separate standalone and host-supervised consumers from the unpacked
archives. Consumers verify standalone and embedded memory brokers, and the
host consumer proves `runtime: false` prevents default-instance startup.
A client-only consumer checks the dependency closure without broker packages.
With the peer environment variables below, installation checks also exchange
real network traffic. The release workflow then
publishes `graviton_mq_core`, `graviton_mq_storage`, `graviton_mq_amqp10`,
`graviton_mq_mqtt`, `graviton_mq_client`, `graviton_mq_runtime`, and `graviton_mq` in dependency order.
Each package has the same version and exact requirements on that version of its
internal dependencies. Only after every Hex publication succeeds does the workflow
commit the version, push the commit and `v<version>` tag together, and create
a GitHub Release containing the seven Hex archives. Prerelease versions create
GitHub prereleases.

Package staging preserves the source umbrella dependency graph while producing
self-contained Mix projects with registry dependencies, a README, and the MIT
license. To build and verify packages locally without publishing, choose a
new staging directory:

```bash
elixir scripts/stage_hex.exs /tmp/graviton_mq_hex
elixir scripts/build_hex.exs /tmp/graviton_mq_hex
```

Set `GRAVITON_MOSQUITTO_BIN` to Mosquitto 2.1.2 for broker archive network
checks, and `GRAVITON_MQTT_TEST_PORT` to an isolated Mosquitto listener for the
client-only archive exchange. Without them the script reports those network
checks as NOT RUN. `ARTEMIS_HOST`/`ARTEMIS_PORT` additionally enable the
independent AMQP archive client's real Artemis exchange. This builds local
archives; it does not publish them.

CI checks formatting, compilation, architecture, and cycles on every branch.
The Tests workflow runs unit tests and the package archive checks on pushes
to `main` and pull requests targeting `main`.

Hex publication is sequential and cannot be atomic across packages. A failed
or cancelled release may have published a subset; inspect Hex before retrying.
Existing Git tags are rejected, and published package versions are never
automatically replaced.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [AMQP 1.0 scope](docs/AMQP10_SCOPE.md)
- [MQTT codec scope and client/broker stages](docs/MQTT_SCOPE.md)
- [MQTT5 client guide](docs/MQTT_CLIENT.md)
- [MQTT5 memory broker guide](docs/MQTT_BROKER.md)
- [Delivery semantics](docs/DELIVERY_SEMANTICS.md)
- [Failure model](docs/FAILURE_MODEL.md)
- [Clean-room research notes](docs/RESEARCH_NOTES.md)
- [Roadmap](docs/ROADMAP.md)
- [Architecture decision records](docs/adr/)

## MQTT codec API

```elixir
GravitonMQ.MQTT.Codec.Frame.decode(bytes, limits)
GravitonMQ.MQTT.Codec.Frame.encode(frame, limits)
GravitonMQ.MQTT.Codec.Packet.decode(bytes, :auto, limits)
GravitonMQ.MQTT.Codec.Packet.encode(packet, limits)
```

Limits default to `GravitonMQ.MQTT.Codec.Limits.default()`. `:auto` verifies
CONNECT's protocol level is 5 or historical 3.1.1; CONNACK decoding requires
an explicit version. Operational roles require MQTT5. CONNECT/CONNACK,
PUBLISH/PUBACK, SUBSCRIBE/SUBACK, UNSUBSCRIBE/UNSUBACK, PINGREQ/PINGRESP and
DISCONNECT have schemas. Envelope decoding
keeps the body opaque and never invokes a schema decoder. Decode returns
`{:ok, value, rest}`, `{:more, positive_count}`, or a structured codec error.
Encode returns `{:ok, bytes}` or a structured codec error. Errors distinguish
malformed, unsupported, configured-limit, and invalid-semantic-value failures.

Properties are ordered `{name, value}` pairs; repeated User Properties retain
order. Password, Will payload, and binary properties remain bytes. The codec
never executes their broker semantics. Configured broker runtime interprets
pure role effects outside the codec.

## Active profiles and qualification

Historical AMQP milestones stop at Boolean/Detach and nested Error. Active G3
now includes required scalar wire types, frame encoding, all transport schemas,
Source/Target, delivery states/outcomes, message sections, pure endpoints and
the independent AMQP client. See [AMQP scope](docs/AMQP10_SCOPE.md), the
[client guide](docs/AMQP10_CLIENT.md) and [capabilities](docs/CAPABILITIES.md).
G4 adds the real Queue.Machine, runtime effects and memory broker, with
independent Qpid Proton evidence. G5 adds MQTT QoS2 plus reviewed AMQP
flow/drain, annotations and serial boundaries. G6 adds real WAL/sync/streaming
recovery and the independent MQTT client store. G7 adds pinned independent
peers, clause inventories, model/fuzz/resource checks, sustained traffic and
clean installed consumers. The [qualification matrix](docs/CAPABILITIES.md)
records the exact tested source and limits. Hosted CI for this uncommitted
working tree is NOT RUN; no publication or deployment is inferred.

GravitonMQ is licensed under the [MIT License](LICENSE).
