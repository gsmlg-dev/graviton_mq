# ADR 0003: Use state machines and effects

- Status: Accepted
- Date: 2026-07-13

## Context

Queue behavior must eventually coordinate state changes, durable writes,
delivery attempts, timers, and protocol outcomes. Performing those side
effects inside transition logic would couple deterministic queue semantics to
processes, storage implementations, and network timing. It would also make
recovery and failure testing ambiguous.

## Decision

Model the future queue transition as a pure function with the conceptual form:

```elixir
{new_state, effects} = GravitonMQ.Queue.Machine.apply(state, command)
```

Commands, queue state, and effects are protocol-independent. The machine will
describe storage, delivery, and scheduling work as effects rather than
executing it. Runtime effect executors will interpret effects and surface
completion, failure, or uncertainty at explicit boundaries. Concrete storage
will implement a behaviour owned by core.

No working queue transition or effect execution is introduced in Milestone 0.
A later publisher acceptance may occur only after every effect required by the
selected durability policy has completed.

## Consequences

Transitions can be deterministic and exhaustively tested. Persistence and
network adapters can evolve without changing the queue command language.
Recovery can replay broker records without rebuilding transient AMQP state.

The runtime must manage effect ordering, retries, idempotency, and uncertain
completion. State-machine code cannot hide those concerns behind a successful
return value, and exactly-once delivery is not implied. The first guarantee
target remains at-least-once.
