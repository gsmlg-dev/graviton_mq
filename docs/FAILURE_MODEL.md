# Failure Model

## Status

Milestone 0 establishes fault-domain boundaries without starting operational
listener, connection, session, queue, storage, or effect-executor processes.
The descriptions below are design constraints for later milestones, not claims
of implemented recovery behavior.

## Ownership and isolation

Explicit ownership determines which process may mutate operational state:

- the public `GravitonMQ.Application` owns the product supervision tree;
- `GravitonMQ.Runtime.Supervisor` will own runtime subsystems;
- each accepted connection will receive a connection tree;
- one Writer per connection will serialize outbound writes;
- each AMQP Session will be represented by one process;
- Link state will initially be owned by its Session, not by independent Link
  processes;
- queue workers will be owned by a node-oriented supervisor independently of
  connection lifetimes;
- storage resources will belong to a dedicated storage supervision boundary.

These rules avoid shared socket ownership and prevent a connection failure
from directly owning durable queue state.

## Intended fault domains

The future runtime shape is:

```text
Runtime.Supervisor
|-- InfrastructureSupervisor
|-- StorageSupervisor
|-- NodeSupervisor
|-- ListenerSupervisor
`-- ConnectionSupervisor
    `-- ConnectionTree
        |-- Connection
        |-- Writer
        `-- SessionSupervisor
            `-- Session
```

A malformed frame or session protocol error should be contained at the
narrowest AMQP-defined scope. A failed connection tree must not take down peer
connections or queue ownership. A failed queue must not be restarted as a
child of a client connection. Listener failure and storage failure require
policies distinct from ordinary connection churn.

No restart strategy in Milestone 0 pretends that these future children exist.
Strategies will be chosen when child start order and recovery invariants can be
tested with real processes.

## State machines and failed effects

Pure queue transitions will return effects rather than performing I/O. The
runtime must distinguish three states of an effect: not attempted, known to
have failed, and completion uncertain. Retrying an uncertain effect requires
an idempotency or reconciliation rule; silently treating it as complete would
violate at-least-once and durability promises.

A future publisher acceptance may be sent only after the selected durability
effects have completed. Process termination, storage errors, or uncertain
writes before that boundary must not result in a success disposition.

## Protocol failure versus broker failure

AMQP Connection, Session, Link, Flow, Transfer, and Disposition state remains
in the protocol layer. Queue message lifecycle remains in the broker core.
Transient delivery IDs and link handles are not recovery keys for durable
messages. Recovery reconstructs broker state from broker records; protocol
state is re-established through AMQP interactions.

## Management isolation

Management queries and control operations must remain outside the message hot
path. Slow or failed management clients must not own queue transitions,
storage acknowledgements, settlement decisions, or socket writers.

## Deferred policies

Milestone 0 does not define crash-loop limits, backoff, corruption handling,
WAL recovery, replication quorum behavior, network partition handling, or
operator repair workflows. Those policies must be introduced alongside the
subsystems they govern and verified with failure-focused tests.
