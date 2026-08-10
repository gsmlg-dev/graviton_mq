# ADR 0003: Use state machines and effects

- Status: Accepted
- Date: 2026-07-13
- Updated: 2026-07-14

## Context

Queue behavior must eventually coordinate state changes, ordered durable
writes, delivery attempts, timers, and protocol outcomes. Performing those
side effects inside transition logic would couple deterministic queue
semantics to processes, physical storage, and network timing. It would also
make recovery and uncertain completion ambiguous.

Logical broker history and physical storage records have different ownership.
A queue event should not contain AMQP handles or delivery IDs, runtime process
identity, checksums, segment offsets, or another storage format concern.

## Decision

Model the future queue transition as a pure function with the conceptual form:

```elixir
{new_state, effects} = GravitonMQ.Queue.Machine.apply(state, command)
```

Commands, queue state, logical `GravitonMQ.Queue.Event` values, and effects are
protocol-independent. The machine describes persistence, delivery, and
scheduling work rather than executing it. Runtime effect executors will
interpret effects and surface completion, failure, or uncertainty at explicit
boundaries. No pure transition performs storage, network, socket, timer, or
process side effects.

Storage converts logical queue events into its own physical
`GravitonMQ.Storage.Record` form. The core storage behaviour has four mandatory
semantic operations:

1. append a non-empty ordered event batch and return the inclusive commit
   reference of its last event;
2. synchronize through a returned commit reference;
3. report the highest contiguous durable-through reference; and
4. fold ordered recovery events incrementally from the first record at
   `:origin`, or strictly after an exclusive commit-reference cursor.

A `CommitRef` contains a stable stream ID and ordered position. References from
different streams are not comparable. Append success proves ordering, not
durability; the returned reference correlates the append with a later sync or
durable-through boundary. Batches do not reorder events. Fold reducers receive
ascending references, allowing checkpointed recovery to resume without
replaying its cursor. Expected failures are explicit errors, and streaming
fold avoids loading a complete log into memory.

No empty storage namespace may claim this behavior merely through
`@behaviour`. `Memory` and `WAL` remain future implementation boundaries until
they implement the required contract.

No working queue transition, effect execution, physical record encoding, WAL,
sync, or recovery is introduced in Milestone 0. A future publisher acceptance
may occur only after the selected durability boundary has completed.

## Consequences

Transitions can be deterministic and exhaustively tested. Core owns logical
events without depending on storage; storage owns physical layout and
durability without importing protocol state. Runtime is the explicit bridge.

The runtime must eventually manage effect order, batching, retries,
idempotency, and uncertain completion. It must never turn a failed or uncertain
durability result into successful publisher acceptance. Exactly-once delivery
is not implied; the first guarantee target remains at-least-once.
