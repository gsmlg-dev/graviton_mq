# Roadmap

The roadmap preserves the separation between AMQP 1.0 protocol work, broker
semantics, storage, and runtime processes. A milestone must not advertise
capability until its tests exercise that capability.

## Milestone 0: repository and architecture baseline

- Create the five-application Mix umbrella and exact dependency graph.
- Establish protocol, core, storage, runtime, and public API modules.
- Start only the top-level and empty runtime supervision boundaries.
- Record clean-room rules, delivery intent, fault domains, and ADRs.
- Verify compilation, formatting, application lifecycle, module loading, and
  dependency direction.

Milestone 0 explicitly provides no functioning AMQP listener or message queue.

## Milestone 1: pure AMQP 1.0 codec foundation

The recommended next task is to implement and test a deliberately bounded,
process-free AMQP 1.0 codec foundation: protocol header recognition, frame
envelope validation, and only the primitive/compound type subset required by
the first supported performatives. It must operate on binaries without TCP,
OTP process ownership, SASL negotiation, or broker commands. Unsupported and
malformed input must produce explicit errors, and the supported subset must be
documented without claiming full protocol compatibility.

## Later milestones

Later work should proceed in independently verifiable increments:

1. Complete the required AMQP performative and message-section codec surface.
2. Add pure Connection and Session transition models, keeping Link state owned
   by Session state.
3. Define a minimal protocol-independent queue machine using commands and
   effects, with no persistence or networking inside transitions.
4. Add the in-memory implementation of the core storage behaviour and an
   effect executor with explicit failure results.
5. Introduce a supervised TCP connection tree and Writer, then integrate the
   tested codec and protocol transitions.
6. Implement transfer assembly, flow control, settlement, and an at-least-once
   delivery path.
7. Define a durable record model, WAL, segment storage, recovery, and the rule
   that publisher acceptance follows the selected durability boundary.
8. Explore queue-type-specific replication only after local recovery semantics
   are stable.
9. Add management capabilities outside the message hot path.

TLS, WebSocket transport, clustering, Raft, alternative protocols, and a
management UI are intentionally deferred until their prerequisite semantics
are demonstrated.
