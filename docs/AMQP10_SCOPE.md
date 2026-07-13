# AMQP 1.0 Scope

## Protocol target

GravitonMQ targets AMQP 1.0. It does not claim protocol compatibility in
Milestone 0, because no AMQP binary codec, frame parser, negotiation flow, or
network listener exists yet.

AMQP 0-9-1 is outside the product foundation. In particular,
`exchange.declare`, `queue.bind`, `basic.publish`, and AMQP 0-9-1 channels do
not define the GravitonMQ protocol model.

## Ownership of AMQP concepts

`graviton_mq_amqp10` owns the representation and future handling of:

- Connection state and protocol negotiation;
- Sessions and their delivery-number spaces;
- Links as state owned by a Session;
- Source and Target termini;
- Flow and credit;
- Transfer framing and delivery association;
- Disposition and settlement;
- AMQP message sections, performatives, errors, and SASL concepts.

Connection, Session, Link, Flow, Transfer, and Disposition belong to the
protocol layer even when they cause broker commands or report broker outcomes.
They do not become queue-domain entities.

## Boundary with the broker core

The protocol frontend will resolve Source and Target addresses into
protocol-independent broker addresses and node types. It will translate an
eligible Transfer into a core command and translate a core outcome into the
appropriate protocol action. The core remains unaware of performatives,
sessions, link handles, delivery tags, and delivery IDs.

AMQP delivery IDs and link handles have scope and lifetime within protocol
state. They are not durable queue message IDs and must not be stored as though
they were the identity of a queued message.

Queue message lifecycle, ordering, durable identity, availability, and removal
belong to `graviton_mq_core`. Concrete persistence belongs to
`graviton_mq_storage`. OTP processes and transport integration belong to
`graviton_mq_runtime`.

## Codec and transport

The future codec will accept and return data without owning sockets or OTP
processes. Transport code will supply bytes to it and decide how protocol
results affect a connection process. This separation permits deterministic
codec tests and prevents TCP concerns from leaking into AMQP type definitions.

## Milestone 0 exclusions

Milestone 0 defines module and data boundaries only. It intentionally excludes:

- AMQP primitive and compound binary encoding or decoding;
- frame and performative parsing;
- message-section parsing;
- SASL and AMQP protocol negotiation;
- Connection or Session state machine behavior;
- Link attach negotiation, Flow processing, and credit management;
- Transfer assembly, Disposition handling, and settlement behavior;
- TCP, TLS, and WebSocket transports.

No module or test should imply that any of these capabilities works yet.
