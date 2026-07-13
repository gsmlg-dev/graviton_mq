# Delivery Semantics

## Status and initial target

Milestone 0 defines terminology and future invariants; it implements no
delivery behavior. The first delivery guarantee target is at-least-once.
GravitonMQ does not promise exactly-once delivery.

At-least-once means a message may be delivered again when the system cannot
prove that the previous attempt crossed the required settlement boundary.
Applications that need deduplication must use stable application-level
identity rather than AMQP link handles or delivery IDs.

## Separate lifecycles

Two lifecycles interact but remain distinct:

1. The AMQP 1.0 protocol lifecycle covers Transfer, delivery state,
   Disposition, and settlement within a Connection, Session, and Link.
2. The broker lifecycle covers admission, durable identity, queue state,
   availability, delivery attempts, and eventual removal.

AMQP delivery IDs and link handles identify protocol-scoped activity. They are
not durable queue message IDs. Reconnection or link reattachment must not turn
those transient numbers into broker identity.

## Future publisher acceptance rule

A future inbound delivery may receive publisher acceptance only after the
selected durability boundary has been reached. The boundary may vary by queue
type or durability policy, but the order is invariant:

```text
validate and map transfer
        |
        v
apply protocol-independent broker command
        |
        v
complete required durability effects
        |
        v
emit the corresponding AMQP outcome or disposition
```

An in-memory transition is not evidence of durable storage when durability was
selected. Failed or uncertain durability effects must not be converted into a
successful acceptance.

## Commands, transitions, and effects

The future queue machine will process protocol-independent commands and return
a new state plus declarative effects. It will not write files, send frames, or
call protocol modules. Runtime code will execute effects and report their
results at an explicit boundary. This keeps state transitions deterministic
and makes acceptance ordering testable.

## Consumer settlement

Future consumer Dispositions will be mapped into broker commands without
embedding AMQP state in durable queue records. A positive terminal outcome may
allow a message to leave the queue; release, rejection, modification, timeout,
and connection loss require explicit policies. Those policies are intentionally
not selected or implemented in Milestone 0.

## Non-goals

This document does not claim working flow control, settlement, publisher
confirmation, redelivery, persistence, recovery, or an AMQP listener. Exactly
once, distributed transactions, and replication semantics are outside the
initial delivery target.
