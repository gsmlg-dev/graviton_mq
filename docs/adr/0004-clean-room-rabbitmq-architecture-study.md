# ADR 0004: Study RabbitMQ architecture under clean-room rules

- Status: Accepted
- Date: 2026-07-13
- Updated: 2026-07-14

## Context

RabbitMQ demonstrates mature message-broker process ownership, supervision,
queue abstraction, persistence separation, state-machine organization, and
fault isolation. Those general lessons are useful, but RabbitMQ source is
neither the AMQP 1.0 specification nor an implementation template for
GravitonMQ. RabbitMQ supports different protocol and storage concerns, while
GravitonMQ specifically targets AMQP 1.0 and must remain independently
implemented.

## Decision

Research may use public AMQP 1.0 specifications, public RabbitMQ architecture
documentation, public descriptions of responsibilities and process
organization, and documented observable behavior.

Contributors must not copy, translate, transliterate, mechanically port,
reconstruct, or disguise RabbitMQ functions, data structures, tests, or
control flow under renamed modules or variables. Research observations and
resulting GravitonMQ decisions use original wording in
`docs/RESEARCH_NOTES.md`. Protocol behavior is derived from AMQP 1.0
specifications and independent tests, not from translating RabbitMQ source.

RabbitMQ may inform only general architecture such as:

- explicit process ownership and fault domains;
- per-connection supervision and a serialized outbound writer;
- Connection and Session boundaries, with Link state initially owned by its
  Session;
- queue-type abstraction;
- deterministic state machines returning effects;
- separation of logical broker events from physical storage; and
- keeping management outside the data path.

GravitonMQ independently chooses its AMQP 1.0 value algebra, directional Link
handle and Session channel indexes, storage contract, stable durable identity,
message preservation strategy, settlement mapping, and embedding lifecycle.
AMQP 0-9-1 exchanges, bindings, channels, and basic methods are not imported as
foundational abstractions.

## Consequences

The project can learn from public operational experience while maintaining
independent code, naming, data structures, and control flow. Architectural
similarity must be explainable from public concepts and GravitonMQ
requirements.

Research requires care: notes identify permitted inputs, avoid pasted source
fragments, and separate observations from decisions. No RabbitMQ source was
copied or translated for Milestone 0. A convenient source port is never an
acceptable shortcut.
