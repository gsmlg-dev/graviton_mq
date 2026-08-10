# ADR 0001: Use a small Mix umbrella

- Status: Accepted
- Date: 2026-07-13
- Updated: 2026-07-14

## Context

GravitonMQ needs strict boundaries among broker semantics, AMQP 1.0 protocol
state, physical persistence, OTP runtime composition, and the public product.
A single application would make dependency drift easy. More applications
would add release and supervision overhead before behavior justifies them.

Dependency declarations alone are insufficient: an application can reference
a forbidden module from source or a typespec even if its `mix.exs` does not
declare the intended dependency.

## Decision

Use one Mix umbrella with exactly five child applications:

- `graviton_mq_core` has no internal umbrella dependency;
- `graviton_mq_storage` depends only on `graviton_mq_core`;
- `graviton_mq_amqp10` depends only on `graviton_mq_core`;
- `graviton_mq_runtime` depends on core, storage, and AMQP 1.0; and
- `graviton_mq` depends only on runtime.

An arrow points from a consumer toward the child application it depends on:

```text
                         graviton_mq_core
                         ▲             ▲
                         │             │
              graviton_mq_storage   graviton_mq_amqp10
                         ▲             ▲
                         └──────┬──────┘
                                │
                      graviton_mq_runtime
                                ▲
                                │
                         graviton_mq
```

Only `graviton_mq` owns the public Application callback and composition root.
The lower-level applications are libraries. Runtime may expose per-instance
supervisors but does not own a competing product Application callback.

Enforce both declared and actual source direction. The architecture task reads
compiler xref manifests and supplements them with parsed Elixir AST references
so it can catch typespec-only edges and direct transport calls. It rejects
forbidden child references and application cycles. Pure AMQP type and codec
source may not reference runtime, storage, TCP, sockets, or transport
libraries. Mix xref cycle analysis remains an independent verification step.

## Consequences

Core cannot reach protocol, concrete storage, runtime, or public composition
code. Storage and AMQP remain independent siblings. Runtime is the first layer
allowed to compose core, storage, and AMQP; the public application has only
the runtime below it.

The umbrella adds project files and explicit translation boundaries. That
cost is intentional. A convenience reference that reverses an arrow, bypasses
runtime composition, or creates a cycle fails verification even when a child
`mix.exs` declaration appears correct.
