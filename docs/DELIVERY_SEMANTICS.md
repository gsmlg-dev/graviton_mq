# Delivery Semantics

## Status and initial target

Milestone 0 defines data structures, mappings, and future invariants; it
implements no delivery behavior. The first delivery guarantee target is
at-least-once. GravitonMQ does not promise exactly-once delivery.

At-least-once means a message may be delivered again when the system cannot
prove that the previous attempt crossed the required durability and settlement
boundaries. Applications that need deduplication must use stable
application-level identity rather than AMQP link handles, delivery IDs, or
channel numbers.

## Separate lifecycles and identities

Two lifecycles interact but remain distinct:

1. The AMQP 1.0 protocol lifecycle covers Transfer, delivery state,
   Disposition, and settlement within a Connection, Session, and Link.
2. The broker lifecycle covers admission, stable message identity, queue state,
   availability, internal delivery attempts, and eventual removal.

AMQP delivery IDs, link handles, and channels identify protocol-scoped
activity. They are not durable queue identities. Reconnection or Link
reattachment must not turn those transient numbers into a
`GravitonMQ.Core.MessageId` or `GravitonMQ.Core.DeliveryRef`.

## Protocol-independent settlement outcomes

`GravitonMQ.AMQP10.OutcomeMapper` maps AMQP outcome data to distinct
`GravitonMQ.Core.Outcome` instructions. The mapping is pure and performs no
Disposition handling, queue transition, persistence, or network operation.

| AMQP 1.0 outcome | Core instruction | Preserved meaning |
| --- | --- | --- |
| Accepted | `Outcome.Retire` | Retire/delete a successfully processed message |
| Released | `Outcome.MakeAvailable` | Make the unchanged message available for another delivery |
| Rejected | `Outcome.Reject` | Carry a stable reason, optional description and opaque encoded error details, plus an explicit `:discard` or `:dead_letter` policy action |
| Modified | `Outcome.ModifyAndMakeAvailable` | Make the message available while retaining `delivery_failed?`, `undeliverable_here?`, and optional opaque annotation mutation bytes |

Released and Modified are deliberately not collapsed into an information-free
`:retry`. The Modified mapping retains encoded message-annotation mutation data
as `GravitonMQ.Core.Message.Mutation` with the format
`"amqp-1.0/message-annotations"`. If parsed message annotations exist but their
encoded bytes are absent, the mapper rejects the lossy conversion. A Rejected
condition and description map directly; when parsed AMQP `Error.info` is
present, the encoded error bytes are required or the mapper likewise rejects
the lossy conversion. Those bytes use the mutation format
`"amqp-1.0/error"`, while the core rejection action remains a broker-policy
choice.

These structures retain enough information for a later queue machine and AMQP
adapter to preserve Accepted, Released, Rejected, and Modified semantics
without making core depend on AMQP modules. They do not select dead-lettering,
redelivery, annotation application, or any other working queue policy in this
milestone.

## Message preservation during delivery

The message of record is `GravitonMQ.Core.Message.encoded_content`: the
original encoded protocol content retained as opaque bytes. A protocol-neutral
`Message.Format` identifies the wire family and Transfer message-format, while
a parsed `Message.Index` contains only the fields needed for routing, policy,
expiration, durability, and management.

This split lets a future delivery forward the original content without losing
unknown described values, annotations, properties, application properties,
footer, body-section kind, multiple body sections, or exact nested AMQP type
identity. Core does not use decoded `body: term()` as the only representation
and does not import AMQP section or value structs.

## Future publisher acceptance rule

A future inbound delivery may receive publisher acceptance only after the
selected durability boundary has been reached. The boundary may vary by queue
type or durability policy, but the order is invariant:

```text
validate and map Transfer
        |
        v
apply protocol-independent broker command
        |
        v
emit logical Queue.Event or persistence effect
        |
        v
Runtime asks Storage to append the ordered event batch
        |
        v
Storage returns the last event's CommitRef
        |
        v
Runtime synchronizes through that CommitRef and observes a contiguous
durable-through boundary at or beyond it
        |
        v
emit the corresponding AMQP outcome or Disposition
```

`GravitonMQ.Core.Storage.append/2` returns an inclusive commit reference for
the final event in a non-empty ordered batch. Append success establishes
ordering but not durability. `sync/2` requests synchronization through that
reference; `durable_through/1` reports the highest contiguous known-durable
position in the same stream. Commit references from different streams are not
comparable. This correlation prevents an earlier or unrelated durable position
from being mistaken for durability of a later publisher operation.

An in-memory transition is not evidence of durable storage when durability was
selected. Failed, partial, or uncertain append/synchronization results must not
be converted into successful acceptance. No queue type or durability policy is
implemented yet, so no code currently emits publisher acceptance.

## Commands, transitions, effects, and recovery

The future `GravitonMQ.Queue.Machine` will process protocol-independent
commands and return a new immutable state plus declarative effects. It will not
write files, call storage or protocol modules, send frames, or perform network
I/O. Runtime will execute effects and report their results at explicit
boundaries, keeping state transitions deterministic and acceptance ordering
testable.

Core owns logical `GravitonMQ.Queue.Event` values. Runtime will execute their
persistence effects, and storage will encode them into physical
`GravitonMQ.Storage.Record` values. Logical events contain stable broker
identity and serializable data, not AMQP delivery IDs or storage offsets.
Physical records own format version, position, encoded event bytes, checksum,
and segment metadata.

Recovery is expressed as a streaming `fold/4` over logical events in ascending
commit-reference order. `:origin` starts with the first record. A supplied
commit reference is an exclusive cursor, so recovery resumes strictly after
that record. The fold does not require materializing the complete log in one
list. This contract is defined but has no Memory, WAL, filesystem, fsync,
segment, or recovery implementation in Milestone 0.

## Consumer settlement and failures

Future consumer Dispositions will be translated into the four core instruction
families above and then interpreted by an explicit queue policy. Connection
loss, timeout, and uncertainty must likewise become explicit broker decisions;
they must not silently retire a message or claim settlement.

The failure model remains conservative: if GravitonMQ cannot prove that the
required durability and settlement boundary was crossed, at-least-once
semantics permit the message to become available and be delivered again. The
exact timeout, dead-letter, redelivery, and mutation policies remain later
milestone work.

## Non-goals

This document does not claim working flow control, Transfer or Disposition
processing, settlement, publisher confirmation, queue transitions, redelivery,
persistence, recovery, or an AMQP listener. Exactly-once delivery, distributed
transactions, and replication semantics are outside the initial delivery
target. The AMQP codec and queue-machine milestones remain separate from this
architecture-hardening task.
