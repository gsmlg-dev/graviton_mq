# Failure Model

## Status

Milestone 0 establishes fault-domain boundaries without starting operational
listener, connection, session, queue, storage, or effect-executor processes.
The descriptions below are design constraints for later milestones, not claims
of implemented recovery behavior.

## Ownership and isolation

Explicit ownership determines which process may mutate operational state:

- in standalone mode, `GravitonMQ.Application` starts the default instance
  through `GravitonMQ.start_link/1`;
- in embedded mode, the host supervisor owns the instance started from
  `{GravitonMQ, options}`; a dependency configured with `runtime: false` does
  not also start an application-owned instance;
- each instance has its own public supervisor and empty
  `GravitonMQ.Runtime.Supervisor`, registered under the exact names supplied in
  its options;
- `GravitonMQ.Runtime.Supervisor` will own that instance's future runtime
  subsystems;
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

The standalone and embedded paths deliberately use the same public lifecycle.
Starting a second instance with the same registered top-level name fails with
the standard `{:error, {:already_started, pid}}` result; it does not attach to
or silently reuse the existing tree. Coexisting instances therefore need
distinct top-level and runtime supervisor names. Names are caller-supplied OTP
names, not atoms derived from arbitrary strings.

Milestone 0 starts only these empty supervision boundaries. It starts no
listener, connection, session, queue, storage, recovery, or effect-executor
process.

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

## Append, durability, and uncertainty

The core storage contract separates ordered append from durable completion. An
append submits a non-empty ordered batch of logical queue events and returns a
commit reference for the final event. That result proves that the events have
positions in the storage stream; it does not prove that the bytes are durable.
Commit references are ordered within one storage stream and are not comparable
across streams.

A later synchronization request names a previously returned commit reference.
Successful synchronization reports the highest contiguous commit reference
known durable through that point. `durable_through/1` exposes that boundary so
append results can be correlated with later durability. A returned error is a
known failure. A timeout, caller crash, or lost reply may leave synchronization
completion uncertain even if storage eventually crossed the requested
boundary. Runtime code must query or reconcile the durable-through position
before retrying or reporting publisher acceptance; it must not manufacture a
successful result.

No Memory or WAL backend implements this contract yet. Milestone 0 defines the
mandatory operations and their failure semantics without claiming append,
synchronization, or filesystem behavior.

## Recovery boundary

Core owns serializable, protocol-independent `GravitonMQ.Queue.Event` values.
Storage owns physical records, including future format versions, encoded event
bytes, positions, checksums, and segment metadata. A future storage backend
will validate and decode physical records, then expose ordered commit
references and logical queue events through the core storage fold contract.

Recovery is fold-based so a caller can rebuild state incrementally or stop
early without loading an entire log into memory. `:origin` yields the first
record, while a supplied commit reference is an exclusive checkpoint cursor;
subsequent events are yielded with ascending references. A future queue machine
will consume the recovered logical events; it will not inspect physical record
metadata. Recovery must not reconstruct durable identity from AMQP channels,
link handles, delivery IDs, process identifiers, references, or socket state.

## Protocol failure versus broker failure

AMQP Connection, Session, Link, Flow, Transfer, and Disposition state remains
in the protocol layer. Queue message lifecycle remains in the broker core.
Transient delivery IDs and link handles are not recovery keys for durable
messages. Recovery reconstructs broker state from core logical events decoded
from storage-owned physical records; protocol state is re-established through
AMQP interactions.

## Management isolation

Management queries and control operations must remain outside the message hot
path. Slow or failed management clients must not own queue transitions,
storage acknowledgements, settlement decisions, or socket writers.

## Deferred policies

Milestone 0 does not define crash-loop limits, backoff, corruption handling,
WAL recovery, replication quorum behavior, network partition handling, or
operator repair workflows. Those policies must be introduced alongside the
subsystems they govern and verified with failure-focused tests.
