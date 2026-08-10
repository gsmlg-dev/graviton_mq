# Research Notes

## Clean-room policy

GravitonMQ is independently designed. RabbitMQ may be studied as a public
architectural reference, but its source code is not a template for this
project. Contributors must not copy, translate, transliterate, mechanically
port, or recreate RabbitMQ functions under different names.

Permitted research inputs include the public AMQP 1.0 specifications, public
RabbitMQ architecture documentation, documented process and module
responsibilities, and observable protocol behavior. Research notes must use
original wording and distinguish an observation from a GravitonMQ design
decision.

No RabbitMQ source code was used to produce the Milestone 0 baseline, its
architecture-hardening changes, the Milestone 1 codec foundation, or these
notes.

## Architectural observations

The following general lessons are useful independently of any particular
implementation:

1. Long-lived state is easier to reason about when one process has explicit
   mutation ownership.
2. A connection-specific supervision boundary limits client failures without
   coupling unrelated connections.
3. A single outbound writer per connection provides a clear serialization
   point for frames and socket errors.
4. AMQP Sessions are natural concurrency and failure boundaries. Their Links
   can begin as session-owned state; introducing one process per Link before a
   measured need would add coordination without improving ownership.
5. Protocol state and broker state have different lifetimes. A Session or Link
   can disappear while a durable queued message remains.
6. Queue implementations benefit from a common semantic boundary even when
   their storage or replication strategies differ.
7. A deterministic state machine can describe required effects without
   performing storage and network I/O inside the transition.
8. Storage recovery is a broker concern and should not rebuild durable identity
   from transient protocol handles.
9. Management traffic should observe and control the system without becoming
   part of the message data path.
10. AMQP peers allocate Link handles and Session channels independently, so
    inbound and outbound lookup spaces need separate indexes even when they
    resolve to one logical entity.
11. AMQP semantic value identity is separate from compact wire-constructor
    choice. Preserving a value for later encoding requires retaining types such
    as binary, string, symbol, and integer widths without treating alternate
    encodings as new semantic types.
12. An ordered storage append and a durable storage boundary are different
    facts. A commit reference lets later synchronization and recovery correlate
    them without pretending append success means durable completion.
13. Recovery scales more safely as an ordered fold than as an operation that
    materializes the entire persistence log in memory.
14. Preserving original protocol message bytes alongside a small broker index
    avoids rewriting information the broker does not need to interpret.
15. A library can support standalone and host-supervised use through one
    instance-starting API when process names and ownership are explicit.
16. A codec can distinguish incomplete prefixes from malformed and unsupported
    input while bounding attacker-controlled lengths before waiting for their
    payloads.
17. AMQP frame-envelope validation can preserve the complete body as opaque
    bytes, keeping the separately invoked performative-schema layer independent
    while message parsing remains later work.
18. AMQP performative composites use fixed positional fields: interior absent
    values require null placeholders, trailing null fields may be omitted, and
    the same standard descriptor may arrive in numeric or symbolic form.

## GravitonMQ decisions derived from those observations

- Use five small umbrella applications with an acyclic dependency direction.
- Keep AMQP 1.0 Connection, Session, Link, Flow, Transfer, and Disposition in a
  protocol frontend separate from the queue-domain core.
- Resolve AMQP Source and Target addresses to protocol-independent broker
  nodes at the frontend boundary.
- Give each future connection a supervised tree, one Writer, and one process
  per Session. Keep Link state within its Session initially.
- Define queue commands and effects without AMQP 1.0 types.
- Make future queue transitions pure and move effect interpretation into the
  runtime layer.
- Represent AMQP semantic values exactly, including distinct binary, string,
  and symbol values, while leaving compact encoding choices to the codec
  layer.
- Give each Link separate local and remote handles. Index Session Links by
  stable Link identity and both handle directions, and index Connection
  Sessions by both channel directions.
- Put stable, serializable broker identities and logical queue events in core;
  keep transient protocol and runtime identities out of durable state.
- Place concrete persistence behind a mandatory behaviour owned by core.
  Treat append as ordered positioning, synchronization as the durability
  boundary, and recovery as an incremental fold of logical events.
- Keep storage format versions, encoded event bytes, checksums, positions, and
  segment metadata in storage-owned physical records.
- Preserve original encoded message content opaquely and maintain only the
  protocol-independent broker index needed for policy and management.
- Map Accepted, Released, Rejected, and Modified into distinct
  protocol-independent core instructions without discarding outcome details.
- Use `GravitonMQ.start_link/1` and `GravitonMQ.child_spec/1` for both the
  standalone default instance and host-supervised instances. Require explicit,
  non-conflicting OTP names for coexisting instances.
- Check both declared application dependencies and compiled source references,
  including dependency cycles, before accepting architectural changes.
- Require the selected durability boundary before reporting publisher
  acceptance.
- Target at-least-once delivery first.
- Keep management functionality outside the transfer and delivery hot path.
- Build the first AMQP codec slice as pure binary transformations with explicit
  limits and structured errors. Recognize only raw AMQP 1.0, keep frame bodies
  opaque, and support only the bounded value constructors required by the
  current slice.
- Add a pure schema facade for only Open and Begin. Normalize their numeric and
  symbolic descriptors to dedicated immutable structs, preserve exact tagged
  field values, validate ordered fields and mandatory types, materialize the
  specification defaults, and leave Connection and Session context rules to
  later state-transition work.
- Enforce the process-free codec boundary through compiler references and
  parsed syntax rather than convention alone.

## Protocol model guardrail

The project targets AMQP 1.0, not AMQP 0-9-1. Exchange declarations, bindings,
`basic.publish`, and AMQP 0-9-1 channels must not be imported as foundational
protocol abstractions. Similar operational goals must be expressed through
AMQP 1.0 termini, links, transfers, outcomes, and protocol-independent broker
nodes.

## Research log

| Milestone | Inputs | Outcome |
| --- | --- | --- |
| 0 | Product requirements, public architectural patterns, and AMQP 1.0 terminology | Repository boundaries, supervision ownership, clean-room guardrails, and ADRs |
| 0 hardening | AMQP 1.0 semantic requirements, OTP embedding requirements, and broker durability and recovery constraints | Exact protocol value and directional identity models; stable core identities and events; meaningful storage, message-preservation, lifecycle, and source-dependency boundaries |
| 1 codec foundation | OASIS AMQP 1.0 Part 1 Types and Part 2 Transport | Hand-built header, frame, primitive, and compound fixtures; bounded pure decoding and encoding with explicit incomplete, malformed, unsupported, and limit errors |
| 1 Open/Begin schemas | OASIS AMQP 1.0 Part 1 composite rules and Part 2 Open and Begin definitions | Dedicated tagged Open and Begin values; numeric and symbolic descriptor recognition; positional, mandatory-field, type, default, and canonical-encoding checks without protocol state or transport |

Future entries should name the public specification section or documentation
page consulted, summarize the observation in original words, and record the
independent GravitonMQ decision. Do not paste source fragments into this file.

The Milestone 1 normative inputs are the OASIS
[AMQP 1.0 type system](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html)
and
[AMQP 1.0 transport specification](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html).
