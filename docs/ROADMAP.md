# Roadmap

The roadmap preserves the separation between AMQP 1.0 protocol work, broker
semantics, storage, and runtime processes. A milestone must not advertise
capability until its tests exercise that capability.

## Milestone 0: repository baseline and architecture hardening

- Create the five-application Mix umbrella and exact dependency graph.
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

## Later milestones

Later work should proceed in independently verifiable increments:

1. Complete the remaining required AMQP performative and message-section codec
   surface in separately bounded slices.
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

TLS, WebSocket transport, clustering, Raft, alternative protocols, and a
management UI are intentionally deferred until their prerequisite semantics
are demonstrated.
