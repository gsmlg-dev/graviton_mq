# ADR 0001: Use a small Mix umbrella

- Status: Accepted
- Date: 2026-07-13

## Context

GravitonMQ needs strict boundaries among broker semantics, AMQP 1.0 protocol
state, persistence implementations, OTP runtime composition, and the public
product. A single application would allow dependencies to drift across those
boundaries. Too many applications would add release and supervision overhead
before the system has behavior to justify it.

## Decision

Use one Mix umbrella with exactly five child applications:

- `graviton_mq_core` has no internal umbrella dependency;
- `graviton_mq_storage` depends only on `graviton_mq_core`;
- `graviton_mq_amqp10` depends only on `graviton_mq_core`;
- `graviton_mq_runtime` depends on core, storage, and AMQP 1.0;
- `graviton_mq` depends only on runtime.

Only `graviton_mq` owns the public Application callback and composition root.
The lower-level applications are initially libraries. Runtime may expose
supervisor modules but does not own a competing product Application callback.

## Consequences

Mix dependency declarations make the intended direction reviewable and
testable. Core cannot reach protocol or runtime code, and concrete storage
cannot become a dependency of the domain contracts. Runtime is the explicit
integration layer.

The umbrella adds some project files and application boundaries, but each
boundary has a distinct ownership role. New cross-layer convenience
dependencies are rejected if they reverse an arrow or create a cycle.
