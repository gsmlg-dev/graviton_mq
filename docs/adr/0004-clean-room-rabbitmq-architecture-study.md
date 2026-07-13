# ADR 0004: Study RabbitMQ architecture under clean-room rules

- Status: Accepted
- Date: 2026-07-13

## Context

RabbitMQ demonstrates mature message-broker process ownership, supervision,
queue abstraction, persistence separation, and operational boundaries. Those
architectural lessons are useful, but RabbitMQ source is neither the protocol
specification nor an implementation template for GravitonMQ. GravitonMQ targets
AMQP 1.0 and must remain independently implemented.

## Decision

Research may use public AMQP 1.0 specifications, public RabbitMQ architecture
documentation, public descriptions of module responsibilities and process
organization, and documented observable behavior.

Contributors must not copy, translate, transliterate, mechanically port, or
reproduce RabbitMQ functions under renamed modules or variables. Research
observations and resulting GravitonMQ decisions are recorded in original
wording in `docs/RESEARCH_NOTES.md`. Protocol behavior is derived from AMQP 1.0
specifications and independent tests, not from translating RabbitMQ source.

RabbitMQ is used only to inform general architecture such as explicit process
ownership, per-connection supervision, one connection Writer, Session process
boundaries, queue-type abstraction, state machines with effects, and separation
of management from the data path.

## Consequences

The project can learn from public operational experience while maintaining
independent code, naming, data structures, and control flow. Architectural
similarity must be explainable from public concepts and GravitonMQ requirements.

Research takes additional care: notes identify permitted inputs, avoid pasted
source fragments, and separate observations from decisions. A convenient code
port is not an acceptable shortcut.
