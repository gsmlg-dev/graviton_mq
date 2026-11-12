# Failure Model

This document preserves the Milestone 0 fault-domain design and its original
qualification boundary. The [current architecture](ARCHITECTURE.md) specializes
that design with running connection owners and pure Session-owned Link state;
the [capability matrix](CAPABILITIES.md) and role guides record actual failure
and recovery evidence. Future-tense and empty-supervisor statements below
describe Milestone 0.

## Status

Milestone 0 establishes fault-domain boundaries without starting operational
listener, connection, session, queue, storage, or effect-executor processes.
The descriptions below are design constraints for later milestones, not claims
of implemented recovery behavior.

## Implemented amendments — 2026-11-08

These maintenance changes (`696dca0` and its follow-up) refine the running
fault domains:

- AMQP broker: an unanswered or uncertain lease command or status/watch call
  fences only that queue. Its lease anchor is killed so its claims return, its Links are
  detached, new attaches are refused, and it is probed and restored when it
  answers; the Manager and other queues keep running. A real queue crash still
  restarts later owners. An enqueue with an unknown outcome only closes its
  publishing Connection without a disposition and does not fence the queue,
  so failed storage stays observable as `storage: :failed`. See
  [AMQP10_BROKER.md](AMQP10_BROKER.md).
- A busy AMQP Connection that misses a capacity query is retried, not closed.
- WAL: compaction commits at a single checkpoint rename; an invalid or
  incomplete tail beyond the durable checkpoint is truncated, and a
  header-only log without a checkpoint is reinitialized. See the
  [ADR 0008 amendment](adr/0008-local-durability-profiles.md).

Linux-only WAL tests passed for `696dca0` on a local Linux host on 2026-11-08;
independent-peer requalification is pending the follow-up's CI
interoperability run.

## Implemented amendments — 2026-11-13 (S3, unreleased)

- AMQP broker: the instance Manager sends queue requests asynchronously and
  never blocks on a queue (the boot `watch` is the only remaining blocking
  call). Each request has a 5-second deadline. At the deadline an atomic
  take-back classifies it as not executed, known or uncertain; the queue
  fence now happens at that deadline without blocking the Manager, so an
  unresponsive queue delays only its own Links. A not-executed enqueue is
  settled `Released` (the Connection survives); an uncertain enqueue still
  closes only its publishing Connection without a disposition. A fence
  settles the enqueues it abandons before it detaches the queue's Links, so
  `Released` reaches the publisher before its Detach. `amqp10_status/1`
  answers within 1 second (900 ms for the concurrent queue part) and reports
  a queue that misses it as `:unresponsive`; if the Manager itself is
  unresponsive, the call can still wait up to its 5-second timeout. See
  [AMQP10_BROKER.md](AMQP10_BROKER.md).
- AMQP client: an opt-in bounded reconnect opens a new Connection and
  restores the Sessions and Links the host declared. It never replays an
  uncertain publication and does not restore credit; a delivery received on
  the lost Connection is voided (`{:error, :stale_delivery}` on settle) and
  may be redelivered. See [AMQP10_CLIENT.md](AMQP10_CLIENT.md).

Evidence: local fault-injection tests B1–B8 (S3 plan execution log) and the
client reconnect tests passed in Tests run 43223447801 and CI run
43223448120 for `375f677`; Artemis 2.41.0 reconnect cases R1–R6 and F1–F3
passed in that run's `interoperability` job. Delivery stays at-least-once;
nothing here is exactly-once.

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
|-- ListenerSupervisor   (implemented per protocol; see 2026-11-12 note)
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

2026-11-12 (S1a, unreleased): listeners do not sit directly under
`Runtime.Supervisor`. Each protocol supervisor (`MQTT.Supervisor`,
`AMQP10.Supervisor`, both rest_for_one) ends, only when listeners are
configured, with a one_for_one `ListenerSupervisor` (child `:listeners`)
holding one `{Listener, id}` per listener; each plain-TCP listener links its
own Hell server. A TCP listener failure closes that listener's connections;
the owner and sibling listeners survive. See
[ADR 0009](adr/0009-owned-network-stack.md).
2026-11-12 (S1b, unreleased): TLS listeners use the same layout, each
linking its own Hell server over ex_ssl, so a TLS listener failure now also
closes that listener's connections (previously they survived it); siblings
are unaffected.

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
