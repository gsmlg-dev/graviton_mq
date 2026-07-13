# ADR 0002: Separate AMQP 1.0 from the broker core

- Status: Accepted
- Date: 2026-07-13

## Context

AMQP Connection, Session, Link, Flow, Transfer, and Disposition have scopes and
lifetimes defined by the wire protocol. Queue admission, durable identity,
availability, and removal remain meaningful without a particular connection or
link. Mixing them would make broker state depend on transient protocol
identifiers and would obstruct other frontends or internal tests.

The product targets AMQP 1.0. AMQP 0-9-1 exchanges, bindings, publishing
methods, and channels are not an appropriate protocol foundation.

## Decision

Place AMQP 1.0 types, frames, performatives, messages, and protocol-facing
state in `graviton_mq_amqp10`. Place protocol-independent messages, deliveries,
outcomes, addresses, node types, queue contracts, and storage contracts in
`graviton_mq_core`.

The protocol frontend will map Source and Target addresses to broker addresses
and node types. It will map Transfers to core commands and map core outcomes to
protocol actions. AMQP delivery IDs and link handles will not serve as durable
queue message IDs. Link state will initially remain part of Session-owned state.

The codec will be independent of TCP and OTP processes. Core will not reference
AMQP 1.0 or runtime modules.

## Consequences

Once implemented, protocol conformance will be testable without starting
sockets, while broker semantics will be testable without constructing AMQP
state. Translation at the frontend boundary is explicit work rather than an
implicit shared data model.

Some concepts need separate protocol and broker representations. That
duplication is intentional when the concepts have different identity,
lifetime, or error semantics.
