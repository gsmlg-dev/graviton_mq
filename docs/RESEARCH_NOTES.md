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

No RabbitMQ source code was used to produce the Milestone 0 implementation or
these notes.

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
- Place concrete persistence behind a behaviour owned by the core.
- Require the selected durability boundary before reporting publisher
  acceptance.
- Target at-least-once delivery first.
- Keep management functionality outside the transfer and delivery hot path.

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

Future entries should name the public specification section or documentation
page consulted, summarize the observation in original words, and record the
independent GravitonMQ decision. Do not paste source fragments into this file.
