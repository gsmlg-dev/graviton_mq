# GravitonMQ

GravitonMQ is an embeddable message broker being designed in Elixir/OTP around
AMQP 1.0. It uses explicit ownership, narrow supervision fault domains, a
protocol-independent broker core, pure state-machine boundaries, and separate
storage implementations. RabbitMQ informs architectural study only;
GravitonMQ is independently implemented and does not copy or translate
RabbitMQ source.

## Current status: Milestone 0

Milestone 0 establishes repository structure, dependency direction, module
boundaries, supervision composition, documentation, and verification.

There is no functioning AMQP listener or message-queue implementation yet.
The repository does not encode or parse AMQP frames, bind a network port,
execute queue transitions, store messages, recover data, or claim AMQP 1.0
compatibility.

GravitonMQ targets AMQP 1.0, not AMQP 0-9-1. AMQP 0-9-1 concepts such as
exchanges, bindings, `basic.publish`, and channels are not the foundation of
the protocol layer.

## Umbrella application map

| Application | Milestone 0 responsibility |
| --- | --- |
| `graviton_mq_core` | Protocol-independent broker, queue, and storage contracts |
| `graviton_mq_storage` | Namespaces for future concrete core-storage implementations |
| `graviton_mq_amqp10` | AMQP 1.0 data and protocol boundaries, independent of transport |
| `graviton_mq_runtime` | OTP integration boundary and intentionally empty runtime supervisor |
| `graviton_mq` | Small public namespace, configuration boundary, and sole Application callback |

## Dependency graph

An arrow means "depends on":

```text
graviton_mq
    -> graviton_mq_runtime
        -> graviton_mq_core
        -> graviton_mq_storage
            -> graviton_mq_core
        -> graviton_mq_amqp10
            -> graviton_mq_core
```

The graph is acyclic. Core has no internal dependencies; storage and AMQP 1.0
depend only on core; runtime composes core, storage, and AMQP 1.0; the public
application depends only on runtime.

## Supervision baseline

Only `apps/graviton_mq` owns the public OTP Application callback:

```text
GravitonMQ.Application
`-- GravitonMQ.Supervisor
    `-- GravitonMQ.Runtime.Supervisor
```

`GravitonMQ.Runtime.Supervisor` has no children in Milestone 0. Future listener,
connection, session, queue, storage, and effect-executor boundaries are
documented without starting placeholder processes.

## Development

Use the installed Elixir and Erlang/OTP toolchain from the repository root:

```bash
mix deps.get
mix format
mix format --check-formatted
mix compile --warnings-as-errors
mix test
mix cmd mix deps.tree
```

No third-party dependency is required for Milestone 0. Build output and fetched
dependencies are ignored by Git.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [AMQP 1.0 scope](docs/AMQP10_SCOPE.md)
- [Delivery semantics](docs/DELIVERY_SEMANTICS.md)
- [Failure model](docs/FAILURE_MODEL.md)
- [Clean-room research notes](docs/RESEARCH_NOTES.md)
- [Roadmap](docs/ROADMAP.md)
- [Architecture decision records](docs/adr/)

## Next milestone

Milestone 1 should implement a bounded, process-free AMQP 1.0 codec foundation:
protocol-header recognition, frame-envelope validation, and only the
primitive/compound type subset needed by the first supported performatives.
It must operate on binaries without TCP, OTP process ownership, SASL
negotiation, broker commands, or a claim of full protocol compatibility.
