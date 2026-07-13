# Architecture

## Status

GravitonMQ is at Milestone 0. The repository establishes boundaries and OTP
composition, but it does not yet contain a functioning broker, AMQP listener,
codec, queue, or storage engine.

GravitonMQ targets AMQP 1.0. AMQP 0-9-1 operations and concepts such as
exchanges, bindings, `basic.publish`, and channels are not the foundation of
this design.

## Umbrella applications

The umbrella has five child applications, ordered from stable domain concepts
to the public composition root:

| Application | Responsibility |
| --- | --- |
| `graviton_mq_core` | Protocol-independent broker types, queue contracts, and the storage behaviour |
| `graviton_mq_storage` | Future concrete implementations of core storage contracts |
| `graviton_mq_amqp10` | AMQP 1.0 types and protocol-facing state |
| `graviton_mq_runtime` | Future OTP process composition across the lower layers |
| `graviton_mq` | Public API, configuration, and the product Application callback |

The internal dependency graph is deliberately acyclic. Each arrow means
"depends on":

```text
graviton_mq         -> graviton_mq_runtime
graviton_mq_runtime -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_storage -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_amqp10  -> graviton_mq_core
```

Core knows neither AMQP 1.0 nor runtime modules. Storage and AMQP 1.0 each
depend only on core and do not depend on one another. Runtime is the first
layer allowed to compose all three. The public application depends only on
runtime.

## Protocol and broker boundaries

The protocol layer owns AMQP Connection, Session, Link, Flow, Transfer, and
Disposition concepts. Its codec boundary will remain independent of TCP and
OTP processes so that binary transformations can be tested as pure code.
Connection, Session, and Link are data structures at this milestone. A Session
owns its Link state; a Link is not initially a process.

The broker core owns the queue message lifecycle. Durable queue message
identity is internal to that lifecycle. AMQP delivery IDs and link handles are
scoped protocol identifiers and are never durable queue message IDs.

AMQP Source and Target addresses will be resolved by the protocol frontend to
protocol-independent broker node addresses and node types. The core does not
interpret AMQP performatives or transport state.

## State, effects, and storage

The future queue machine will be a pure deterministic transition system with
the conceptual interface:

```elixir
{new_state, effects} = GravitonMQ.Queue.Machine.apply(state, command)
```

Commands and effects are protocol-independent. A transition describes work
through effects instead of writing storage, sending network data, or managing
processes itself. Runtime effect executors will eventually interpret those
effects. Storage implementations remain separate from both protocol state and
the pure queue state transition logic.

No queue transition or effect execution is implemented in Milestone 0.

## OTP ownership

Only `graviton_mq` owns the public product Application callback:

```text
GravitonMQ.Application
`-- GravitonMQ.Supervisor
    `-- GravitonMQ.Runtime.Supervisor
```

The lower-level applications are libraries. Runtime exposes supervisor modules
but does not own a second product Application callback. Its supervisor is
intentionally empty in Milestone 0 rather than starting placeholder workers.

The intended later runtime organization is:

```text
GravitonMQ.Runtime.Supervisor
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

Each accepted connection will eventually receive an isolated supervision
tree. A connection-level Writer will own outbound socket writes, and each AMQP
Session will have one process. Queue processes will live under a node-oriented
boundary, not beneath a connection. These are documented future ownership
rules, not functioning processes at this milestone.

## Management boundary

Management functionality will use broker APIs and observations outside the
message data path. A future UI or management API must not become a required hop
for transfer, settlement, queue transitions, persistence, or delivery.

## Milestone 0 limits

This milestone does not implement TCP, TLS, or WebSocket listeners; an AMQP
codec, frame or performative parser, message-section parser, SASL negotiation,
or protocol negotiation; Connection or Session transition behavior; Link
attach, Transfer assembly, Flow, or settlement behavior; a working queue
machine, publisher confirmation, or Accepted disposition; filesystem storage,
WAL, segments, persistence, or recovery; Raft, the `ra` dependency, Khepri, or
clustering; Phoenix, a management UI or REST API; MQTT; AMQP 0-9-1; or
RabbitMQ plugins and dependencies.

The runtime starts no fake listeners, connection workers, queue workers,
storage workers, effect executors, data directories, or speculative Registries.

## Design constraints

- Standard Elixir/OTP is sufficient for Milestone 0; there are no speculative
  third-party dependencies.
- RabbitMQ is an architectural reference for ownership and process
  organization only. GravitonMQ source is independently designed and is not
  copied, translated, or mechanically ported from RabbitMQ.
- The initial delivery target is at-least-once, not exactly-once.
- A later publisher acceptance may be emitted only after its configured
  durability boundary has been reached.
