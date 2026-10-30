# Active implementation scope

Owner authorization: 2026-10-29, four independently verifiable network roles.
The 2026-10-28 review baseline was
`66b4a6c161f521cb6b6379ad4f952dcdc7f3447a`; the checkout was rechecked at that
same HEAD on 2026-10-29, with existing MQTT and documentation edits. Evidence
below applies to the working tree, not an assumed unchanged or published HEAD.
The HEAD and current changes were rechecked again on 2026-10-30 at the same
commit; the G0–G7 implementation is uncommitted.
The initial G3 client profile passed its Artemis and clean archive-consumer
exit in [the qualification record](qualification/2026-10-30-g3-client.md).
The bounded G4 Queue.Machine/memory broker exit passed its gates, independent
Proton path and rebuilt clean consumers; see [G4 evidence](qualification/2026-10-30-g4-broker.md).
G5 selected-profile clause review and its reported repairs now have final
integrated gate and installed-consumer evidence. The bounded
[G6 local durability exit](qualification/2026-10-30-g6-local-durability.md)
passed all eight gates (1,073 tests), seven archives and three installed
consumers under [ADR 0008](adr/0008-local-durability-profiles.md).
[G7 local qualification](qualification/2026-10-30-g7-local-qualification.md)
adds pinned peers, model/fuzz tests and a runnable CI path. Its final eight
gates passed 1,117 tests with zero failures/warnings; seven archives and three
installed consumers passed. The final 15-minute four-role memory/TCP run passed
35,976 exact round trips, final drain and complete cleanup. This closes the
local G0–G7 exits for the selected profiles. Hosted CI remains NOT RUN.
The clause audits retain exact profile and execution limits; no confirmed
source defect remains pending in their review.

## Scope and historical records

This authorization advances beyond completed AMQP codec milestones and MQTT
M0. Their exclusions describe those historical slices; they do not prohibit
the G0–G7 implementation below. Preserve their tests and acceptance records.
The [MQTT embedding design](superpowers/specs/2026-10-26-mqtt-broker-embedding-design.md)
continues to define broker/session, host API, backpressure, Will, expiry, and
TCP/TLS behavior. [ADR 0007](adr/0007-independent-network-client-application.md)
supersedes its proposed placement of client runtime in the broker package.

AMQP 1.0 and MQTT 5.0 are the operational targets. Preserve the existing MQTT
3.1.1 CONNECT/CONNACK codec and regression fixtures; that is a codec-only
compatibility surface. Legacy runtime requires its own later qualification.
MQTT 3.1, AMQP 0-9-1, clustering/Raft, management UI, cross-protocol bridges,
AMQP transactions, and the recorded QUIC TODO remain outside this Goal.

## Authorized stages and observable exits

| Stage | Implementation | Exit evidence |
| --- | --- | --- |
| G0 | Generic AMQP null map keys; pre-allocation compound byte budgets; capability matrix; client ADR | Independent vectors, allocation-bound regressions, historical schema regressions, complete repository gates |
| G1 | Independently installable MQTT5 client, pure client transitions, QoS0/1, negotiation, keep alive, errors, controlled reconnect | Fixed-version Mosquitto send/receive and failure tests; TCP/TLS; client-only package consumer with no broker runtime/storage |
| G2 | Single-node memory MQTT broker, topics, sessions, retained, Will/expiry, bounded host API, authn/authz, TCP/TLS | Paho/Mosquitto client, host/network paths, real two-instance isolation and cleanup |
| G3 | Necessary AMQP wire types, frame encoder, Attach/Flow/Transfer/Disposition, message sections/outcomes, pure endpoints, independent client | Graviton AMQP client against fixed-version Artemis |
| G4 | Deterministic Queue.Machine, runtime effects, memory AMQP broker | Qpid Proton credit, transfer, settlement, release/redelivery, disconnect responsibility |
| G5 | MQTT QoS2 and applicable MQTT5 semantics; AMQP flow/settlement/sequence/disconnect boundaries | Independent peers exercise duplicate, reconnect, wraparound and failure boundaries; legacy MQTT runtime separately qualified |
| G6 | Real WAL/sync/streaming recovery; MQTT session/retained/inflight persistence | Crash/restart, partial writes, explicit failed/unknown sync; durable ACK only after selected durable-through boundary |
| G7 | Four external interoperability paths in CI; resources, fuzz/model, soak, installation qualification | Clause-to-code/test/interop/recovery evidence for the selected profile, accurate published support matrix |

MQTT forms the first usable network path. AMQP work can proceed independently
with disjoint file/build ownership. Each change is reviewable; a stage stays
open until its actual exit evidence exists. A codec or empty supervisor does
not qualify a network role. The single-node memory profile is intermediate.

## Invariants and delivery

Protocol codecs and role transitions remain pure and return state/effects.
Running owners interpret sockets, clocks/timers, storage, and auth callbacks.
Keep AMQP semantic types, authoritative encoded messages, independently
allocated local/remote channels and handles, Session-owned Link state, stable
broker identities, and separate append/durable-through boundaries.

Client dependencies exclude broker runtime, storage, and product composition.
Client installation/startup must be tested from unpacked packages separately
from broker and combined consumers. Only the broker product owns the default
Application callback; each client is explicitly supervised by its host.

Initial MQTT broker negotiation advertises Maximum QoS=1. Unsupported optional
capabilities are negotiated off or rejected; mandatory semantics cannot be
silently ignored. QoS2 is no business exactly-once guarantee. Operation results
separate local admission, peer handshake completion, explicit failure, and
unknown outcome. Memory ACK is not disk durability.

Use `.tool-versions` and all eight commands in `AGENTS.md`. Report PASS, FAIL,
BLOCKED, NOT RUN, and UNSUPPORTED separately in the
[capability matrix](CAPABILITIES.md). Peer versions/configuration and exercised
paths accompany network evidence. The original Goal did not authorize a
branch/worktree, commit, push, release, publication, or deployment. Preserve
unrelated working changes.

## Release authorization — 2026-10-30

After the local G0–G7 exit, the owner explicitly requested commit and release,
including the existing Hex.pm publication workflow. This authorizes committing
and pushing the implementation on the current `main` branch and publishing
the seven synchronized packages as `0.2.0`, with a `v0.2.0` GitHub release.
It supersedes the original Goal's commit/publication restriction. New branches,
worktrees and deployment remain outside this authorization.

The local qualification records above retain their pre-publication status.
The [release notes](releases/0.2.0.md) describe the shipped profiles; the
[GitHub release](https://github.com/gsmlg-dev/graviton_mq/releases/tag/v0.2.0)
records hosted workflow and public package verification evidence after delivery.

Publication completed on 2026-10-30: all seven `0.2.0` Hex packages and the
annotated `v0.2.0` tag/GitHub release refer to code commit
`0b23da0fcbae9b91dacfdd2bd4384a4449a17694`. Hosted CI and three fresh public
Hex consumers passed all four network paths and installed recovery checks.
The [release qualification](qualification/2026-10-30-v0.2.0-release.md)
records exact workflow links, archive checksums and profile limits. This
post-publication documentation update changes no shipped library source.

Agent Note has no explicitly configured project label in this checkout. Do
not guess a label or search all notes; that limitation does not block work.
