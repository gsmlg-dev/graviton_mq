# G6 local durability increment — 2026-10-30

Working-tree implementation at rechecked HEAD
`66b4a6c161f521cb6b6379ad4f952dcdc7f3447a`. No commit, publication or
deployment. This extends the qualified memory profiles under
[ADR 0008](../adr/0008-local-durability-profiles.md); it does not close G5/G7
or claim complete AMQP1/MQTT5 conformance.

## Responsibility and recovery evidence

| Requirement | Implementation | Observable verification |
| --- | --- | --- |
| Append is distinct from durable-through | Storage `WAL`, `WAL.Codec`, core `CommitRef` | Real files, checksummed framing/positions, explicit sync; a closed writer does not imply sync |
| Durable publication boundary | WAL fsync, checkpoint file fsync, atomic rename, directory fsync | Faults at every step; barriers prove no positive protocol ACK or host success escapes before final sync |
| Recovery preserves certainty | Streaming fold, prefix/checkpoint validation, fresh sync before reopening | Whole-BEAM SIGKILL, partial writes, incomplete undurable tail repair, durable corruption rejection, poisoned writers |
| Exclusive local ownership | Directory device/inode abstract Unix-socket lock | Independent BEAM/process contention, path aliases, cross-WAL/client-store contention and owner death |
| MQTT5 broker sessions and responsibility, §§3.1–3.3, 4.1–4.4 | Pure `MQTT.Broker.Checkpoint`, runtime `MQTT.Persistence`, separate opaque WAL record | Paho 2.1.0 process/VM loss and five QoS2 cut/restart phases; subscriptions, retained, Will, offline expiry and incoming/outgoing ownership |
| MQTT5 positive ACK follows durable state | Runtime commits pure transitions before effects | Actual socket PUBACK/PUBREC/PUBCOMP withheld at directory-sync barrier; explicit failed/unknown results close peers and stop admission |
| MQTT5 independent client recovery | Client-owned Store/IO/Checkpoint and pure Client | Mosquitto 2.1.2 lost PUBACK/PUBREC/PUBCOMP, host-owned incoming delivery, subscriptions, downtime and fresh-VM restart; no broker storage dependency |
| Terminal host result differs from protocol responsibility | Retained QoS phases and stable operation identity | Terminal/retry-exhausted publication may report unknown once; subsequent restart must finish the saved PUBLISH/PUBREL without contradictory ordinary success |
| AMQP1 durable queue responsibility, Messaging §§3.2.1, 3.4 | Core Queue.Machine logical events, runtime AMQP Queue/Persistence/Store | Proton 0.40.0 durable enqueue, owner/VM restart, unsettled lease release, exact encoded body, Modified annotations/failure count, rejection and retirement |
| AMQP1 Accepted follows queue sync | Queue event append/sync before runtime effects | Independent Proton observes no Accepted at actual WAL barrier; injected failures close without Accepted |

MQTT snapshots exclude credentials, CONNECT packets, callbacks, sockets, timers,
host PIDs/references and active auth operations. Host subscriptions are not
durable broker sessions. Recovered MQTT host-origin messages use a fresh caller
epoch, preventing No Local identity collisions. Known offline deadlines deduct
wall-clock downtime once. Previously online broker connections are discovered
lost at recovery; their offline Will/Session Expiry timers begin then.

AMQP uses stable broker/queue identities and independent per-queue event streams
under one exclusive broker directory. Replay reconstructs the pure machine and
compares generated events with history. Lost leases produce newly synchronized
release events before serving. Large release batches append bounded chunks and
synchronize the final boundary once. The source protocol message remains opaque;
delivery IDs/handles/channels never become durable queue identities.

Client checkpoints contain stable store-scoped operation identities and a result
outbox. Recovery events distinguish unfinished host operations, terminal host
results, incoming ownership and protocol completion after a prior unknown
outcome. Reopening re-establishes file/directory synchronization before exposing
recovered state, including after an uncertain prior directory sync. Backward
wall-clock movement rejects recovery. Message/operation expiry includes downtime.
Online client snapshots preserve unknown disconnect timing; recovery cannot
count healthy uptime since an older snapshot as offline Session Expiry.
The one-second-session Mosquitto regression keeps the connection healthy past
the snapshot's age, kills the owner and verifies actual Session Present resume.

## Additional protocol repairs

The selected-profile review found and reproduced these defects before repair:

| Specification | Repair and regression |
| --- | --- |
| MQTT5 §3.1.2.11.7 | Request Problem Information=0 rejects Reason String/User Property on incoming ACKs with DISCONNECT0x82; PUBLISH remains permitted |
| MQTT5 §3.2.2.3.2 | Omitted CONNACK Session Expiry falls back to the actual current CONNECT, including after a previous override |
| MQTT5 §3.1.2.5, §3.1.3.2.2 | Only DISCONNECT0 suppresses Will; reason4 and other client errors retain min(Will Delay, Session Expiry) |
| MQTT5 §3.2.2.3.13, §4.13 | Advertised-off shared subscription disconnects with0x9E before authorization instead of granting a SUBACK |
| MQTT5 §§4.4, 4.6, 4.9 | Persist first-PUBREC release order; resumed PUBREL advances independently of PUBLISH quota; a real Paho mixed-QoS session resumes after Receive Maximum decreases3→1 |
| MQTT5 §§3.1.2.11.4, 3.2.2.3.6, 4.13 | Oversized announced packets use the specific DISCONNECT0x95 in both roles before receiving the body; other decode errors retain their behavior |
| AMQP1 Transport §§2.6.12, 2.7.6 | Sender settlement retains host ownership; receiver settlement without a terminal outcome completes one operation as unknown, preserving unrelated Links and finishing a fragmented transfer with abort |
| AMQP1 Transport §2.4.3 | Accept peer Close on any valid negotiated channel while continuing to emit local Close on channel0 |

The broker repairs additionally passed four independent Paho tests. Paho2.1.0
does not expose a compact DISCONNECT reason correctly through its callback;
the shared-subscription test observes the exact packet Paho received (`9e`)
as well as its callback and socket shutdown. Production encoding remains the
legal compact encoding. Two real timed cases verify delayed Will after
DISCONNECT4/128, alongside normal-DISCONNECT suppression.

AMQP shared wire completion adds exact-bit float/double/decimal32/64/128,
Unicode scalar char and homogeneous scalar/compound/described/nested arrays.
Twelve new focused tests and 73 independent Proton producer→decoder/encoder→
Proton consumer fixtures cover this increment. Historical symbol-array and
semantic identity regressions remain. This expands wire support, without
implying endpoint/profile completeness.

## Integrated verification

The bounded G6 checkpoint passed all eight root gates under Elixir1.18.4 /
OTP28.5.0.1 with `ERL_FLAGS='+S 4:4'`: **1,073 tests, zero failures**
(core38, AMQP441, MQTT257, storage37, client86, runtime76, composition138).
All independent-peer test groups were enabled. Compilation had zero warnings;
architecture analyzed **6,623 source references across seven applications**
and xref found no cycles. Expected negative TLS and injected process-failure
reports are separate from assertion failures and compilation warnings.

Results are in `/tmp/graviton-goal-20261029-v4ozpgqs/g6-g7-final-gates.json`
and `g6-g7-final-gate-01.log` through `08.log`. The source stayed stable during
verification. Its manifest/digest are in `g6-g7-final-source-manifest.json`:
`d3b6c5c1ac611d7c93365c947455907e0ed8c5d6cb8d41b8f1bfe89a50b9cd51`.
The digest identifies that tested checkpoint, not future working-tree edits.

Seven archives and all three clean consumers passed from
`g6-g7-final-hex-staging`: standalone broker, host-supervised embedding and
independent client-only installation. Installed checks exercised MQTT
durable-through/retained recovery, Proton durable enqueue/replay/retirement,
and client-owned QoS2 recovery against Mosquitto. The client dependency closure
contained no broker runtime, storage or composition package. All145 archived
library files matched the gated source; exact archive hashes are in
`g6-g7-final-package-manifest.json`. The full consumer log is
`g6-g7-final-hex-build-consumers.log`. Both peer wrappers recorded exit0 and
complete peer/temporary-instance cleanup.

Artemis TLS qualification additionally passed8 tests, including actual
bidirectional Proton-assisted exchange, wrong CA and wrong hostname. Those
tests also ran in the root gate. MQTT oversized-header socket checks passed
18 tests across the two runtime files, and the Paho QoS2 recovery file passed
6, including the new legal mixed-QoS/quota-resume case.

Two earlier integration attempts are retained as failures: peer preparation
initially omitted `mosquitto_passwd`; after fixing that environment, the full
1,044-test suite passed but architecture analysis failed on `__MODULE__.IO`.
The parser repair resolves the relative module in its lexical module scope
without dropping its reference or weakening dependency rules; its two new
regressions reproduced the crash before repair. These earlier runs are not
relabeled as successful complete gates.

## Profile limits and next entry

The qualified storage design is local Linux, existing parent directories, one
serialized owner per inode, bounded record/log sizes and one WAL per stream.
There is no rotation/compaction or distributed-filesystem guarantee. Reaching
admission limits is explicit; failed/uncertain I/O freezes the affected owner.
A complete unacknowledged append may survive a crash and is still a possible
outcome. Memory ACK does not imply disk durability; QoS2 does not imply business
exactly-once processing.

AMQP client reconnect/link resumption and client process/VM recovery remain
unimplemented. Its initial volatile/format0/no-SASL/unsettled/receiver-first
profile remains distinct from broker queue persistence. AMQP TTL/absolute expiry
messages and dead-letter policy are explicitly outside the current accepted
broker profile. Full selected-profile mandatory/conditional review, remaining
flow/settlement/disconnect combinations and broader G7 qualification remain open.
The subsequent [AMQP clause audit](2026-10-30-amqp10-clause-audit.md) confirmed
two G5 blockers at this checkpoint: cross-Session/container-pair Link-name
uniqueness and the conditional precondition-failed Detach when a memory receiver
cannot honor a durable message and Rejected is not an allowed outcome. The G6
storage exit does not conceal those protocol defects or close G5.
The subsequent audit records their repairs and the additional A03–A08
boundaries, exact message corpora and requirement inventories. Final combined
acceptance is tracked in [G7 qualification](2026-10-30-g7-local-qualification.md);
these checkpoint blockers are historical rather than unresolved defects in
the later source.
MQTT3.1.1 CONNECT/CONNACK regressions do not qualify its runtime. Clustering/Raft,
management UI, protocol bridges, AMQP transactions and QUIC remain deferred.
