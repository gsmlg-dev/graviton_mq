# Capability and qualification matrix

Working-tree evidence, initiated 2026-10-29 and rechecked 2026-10-30. Review baseline and checked HEAD:
`66b4a6c161f521cb6b6379ad4f952dcdc7f3447a`. Existing uncommitted MQTT5-only
amendments were present. This matrix is updated with implementation evidence;
authorization in [active scope](ACTIVE_SCOPE.md) is not a completion claim.

Release update, 2026-10-30: **v0.2.0 is published** from
`0b23da0fcbae9b91dacfdd2bd4384a4449a17694`. Hosted CI, all four independent
network paths, recovery, sustained traffic and three public Hex consumers
passed. Seven public package archives match the GitHub assets and all 145
library files match the released source. See the
[release qualification](qualification/2026-10-30-v0.2.0-release.md) and its
[machine-readable record](qualification/2026-10-30-v0.2.0-release.json).

PASS means the named check ran successfully. NOT RUN means evidence is still
required. UNIMPLEMENTED and UNSUPPORTED describe distinct product boundaries.
No published package, throughput, security certification, or disk durability
is inferred from unit or package tests.

## Independently qualified network roles

| Role | Requirements / implementation | Unit evidence | External interoperability | Recovery evidence | Current limitation |
| --- | --- | --- | --- | --- | --- |
| MQTT5 client | OASIS MQTT5 §§3–4, 7; `apps/graviton_mq_client` and pure `GravitonMQ.MQTT.Client` | PASS: shared client/QoS2/terminal/property tests, shell/checkpoint/store/fault tests; [clause ledger](qualification/2026-10-30-mqtt5-clause-audit.md) | PASS: Mosquitto2.1.2 send/receive, QoS0/1/2, negotiation, keep alive, TCP/TLS, negative ACKs and controlled reconnect; additional Paho2.1.0 metadata tests both directions | PASS: lost PUBACK/PUBREC/PUBCOMP, host-owned incoming delivery, session loss, owner/whole-VM recovery and failed/uncertain local synchronization | Memory and opt-in Linux local durable profiles; enhanced auth unavailable, Topic Alias Maximum0; local selected-profile qualification is recorded below |
| MQTT5 broker | MQTT5 §§3–4, 7; pure Topic/Broker, `apps/graviton_mq_runtime/.../mqtt`, public host API | PASS: topic/session/Will/expiry/QoS2, runtime pressure/auth/privacy, checkpoint/WAL barrier tests | PASS: Mosquitto2.1.2/Paho2.1.0 TCP/TLS, host/network, real two-instance isolation, QoS2 cut/resume, mixed quota and ordered PUBREL resume; exact metadata live/retained/recovered | PASS: process/whole-VM recovery of session/subscription/retained/inflight/Will obligations; PUBACK/PUBREC/PUBCOMP withheld until durable-through; failed/uncertain sync closes admission | QoS0/1/2, configurable Maximum QoS0/1; aliases, shared subscriptions and Subscription Identifiers negotiated off; local selected-profile qualification is recorded below |
| AMQP1 client | OASIS AMQP1 Parts1–3; pure Endpoint/SessionEngine, `apps/graviton_mq_client/.../amqp10` | PASS: exact wire/message/array tests, bounded endpoint/session/credit/serial/settlement tests and shell/config/pressure tests; [clause ledger](qualification/2026-10-30-amqp10-clause-audit.md) | PASS: Artemis2.41.0 with Proton0.40.0 send/receive, fragmentation, release/redelivery, uint32 wrap, lost/unknown outcome and verified TLS including wrong-CA/hostname rejection; Proton late-settlement cases | PASS: bounded non-reading-peer failure, explicit failed/unknown completion and transport cleanup; reconnect/resumption and client process/VM recovery UNIMPLEMENTED | Volatile format0/no-SASL/initially-unsettled/receiver-first profile; homogeneous scalar/compound/described arrays supported; Link-name/error-path repairs are covered by the final gates; no protocol resumption |
| AMQP1 broker | AMQP1 Parts1–3; Queue.Machine/Forwarding, `apps/graviton_mq_runtime/.../amqp10`, opt-in public lifecycle | PASS: independent queue model, pure protocol, runtime/auth/pressure, storage replay/fault/barrier tests | PASS: Proton0.40.0 fragmentation, zero credit, drain, annotations, settlement/release/redelivery, TCP/verified TLS, two-instance isolation and durable queue path | PASS: queue-owner/whole-VM recovery, abandoned lease release, Modified data, durable retirement and uncertain I/O; Accepted withheld until selected durable-through | Memory and opt-in local durable named move queues; volatile termini, no Link resumption; TTL/absolute-expiry refused; local selected-profile qualification is recorded below |

Empty lifecycle tests remain foundation evidence. G2 adds actual MQTT routing,
dual-instance state isolation, network/host delivery and standalone/embedded
archive exchanges. G4 adds actual AMQP memory queues and Proton evidence. No client
starts a broker as an installation side effect. Self-to-self exchanges are
additional evidence only.

The final integrated [G7 gates and installation checks](qualification/2026-10-30-g7-local-qualification.md)
passed **1,117 tests, zero failures**, all eight root commands with all peer
groups enabled, zero compilation warnings, 6,738 source references across
seven applications and no cycles. Seven archives and three clean installed
consumers passed; all 145 archived library files match the gated source.
These results include the clause-audit repairs, metadata, QoS2 order/quota,
exact AMQP message corpus and recovery cases. Final source-fixed sustained
traffic passed **900,182 ms / 35,976 exact round trips**, final drain and
complete cleanup. The local selected-profile G0–G7 exits are complete.
Hosted execution was NOT RUN at that local checkpoint. The subsequent
[release CI](https://github.com/gsmlg-dev/graviton_mq/actions/runs/40995883322)
passed all four external paths, recovery, a 300,073 ms / 11,996-round-trip
soak, final drain/cleanup and installed consumers. Public Hex installation
and publication checks also passed in the release qualification above.

## Foundation and active increments

| Capability | Implementation / regression location | Status and evidence |
| --- | --- | --- |
| Exact AMQP semantic identity and opaque content | `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/value.ex`; core message and outcome contracts | PASS: existing AMQP/core baseline tests |
| Generic AMQP null map key | `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/value.ex`; `test/codec/value_compound_test.exs` and `value_null_key_schema_test.exs` | PASS: null key vectors/duplicates and Open/Begin/Error symbol-keyed schema regressions; 182 AMQP tests |
| Compound encoder allocation bound | Same codec; `test/codec/value_budget_test.exs` | PASS: preflight list/map/array/described budgets, exact size boundaries and a 24 MB process-memory regression; no eager map flatten/key index before admission |
| AMQP required scalar wire increment | Same codec; `test/codec/value_scalar_extension_test.exs` | PASS: 15 tests, 44 independent vectors; exact binary/ubyte/byte/short/int/long/timestamp/UUID types and compact signed constructors |
| AMQP frame encoder | `codec/frame.ex`; `test/codec/frame_encode_test.exs` | PASS: 13 tests, strict size/offset/alignment/channel/limit validation before allocation; opaque header/body; SASL explicitly unsupported |
| AMQP pure Flow schema | `performative/flow.ex`, `codec/performative.ex`; `test/codec/flow_performative_test.exs` | PASS: 16 tests, 11 fields, numeric/symbolic descriptors, tagged defaults and positional holes; no credit/session transitions |
| AMQP Attach and termini | `codec/attach.ex`, `codec/terminus.ex`, typed Attach/Source/Target | PASS: 31 focused tests; sender initial count, dynamic address/role rules, fields/filter/extension data, custom distribution modes and nonempty supported modes; no live Links |
| AMQP Transfer/Disposition and delivery states | `codec/delivery_performative.ex`, `codec/delivery_state.ex`, `codec/raw_field.ex` | PASS: 35 focused tests; exact tags, defaults/holes, continuation settled omission, wraparound data, nested Error/annotation provenance, existing core outcome mapping |
| AMQP format-0 message sections | `codec/message.ex`, `message.ex`; `test/codec/message_test.exs` | PASS: 17 tests; nine section schemas/order, original bytes authoritative, scalar/null application properties, forbidden content encoding and aggregate pre-allocation budget |
| Pure AMQP endpoint/session behavior | `endpoint.ex`, `session_engine.ex`, `serial.ex`; dedicated tests and `test/codec/stream_test.exs` | PASS: bounded incremental input, independent directional IDs/indexes, lazy fragments, windows/credit, finite ledgers/timers, four terminal outcomes, unknown/known-outcome loss; all AMQP modules forbid process/clock/transport dependencies |
| AMQP complete scalar/array wire subset | `value.ex`, `codec/value.ex`; `test/codec/value_wire_completion_test.exs` | PASS: exact-bit float/double/decimal, Unicode scalar char, homogeneous scalar/compound/described/nested arrays; twelve focused tests and73 independent Proton producer/consumer fixtures; historical symbol arrays preserved |
| Independent client dependency boundary | ADR 0007; architecture rules and package staging | PASS: seven archives; combined, broker-only embedding and client-only unpacked consumers; real client-only Mosquitto and Artemis exchanges with no broker runtime/storage/composition installed |
| MQTT CONNECT/CONNACK and opaque envelope | `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/codec`; `test/codec` | PASS: initial working-tree baseline, 80 MQTT tests; codec only |
| MQTT3.1.1 historical codec | CONNECT/CONNACK only; `test/codec/legacy_connect_regression_test.exs` and version boundary regressions | PASS: historical codec restored under latest owner instruction; MQTT3.1.1 runtime remains unimplemented and NOT RUN |

Initial audit ran `mix test` under Elixir 1.18.4 / OTP 28.5.0.1 in
`/tmp/graviton-goal-20261029-v4ozpgqs/baseline-build`: 294 tests, 0 failures
(core 9, AMQP 172, MQTT 80, storage 3, runtime 2, composition 28).
This is before G0 changes and before any network role.

G0/G1 integrated qualification on 2026-10-29 passed all eight root gates:
384 tests, zero failures (core 9, AMQP 182, MQTT 135, storage 3, client 22,
runtime 2, composition 31). The client count includes all 11 explicitly
enabled Mosquitto tests. Compilation had zero warnings; architecture analysis
covered 2,376 source references across seven applications; xref found no
cycles. Seven archives and three clean consumers passed, including real
network exchange from the client-only dependency closure. See the
[qualification record](qualification/2026-10-29-g0-g1.md) for reproducible
commands, clause/evidence mapping, peer configuration and remaining limits.

The 2026-10-30 integrated gates passed **529 tests, zero failures** (core 9,
AMQP 226, MQTT 191, storage 3, client 22, runtime 21, composition 57).
All 25 explicitly enabled external-peer tests ran: client→Mosquitto 11,
Mosquitto/Paho→broker 14. All eight root commands passed with zero compilation
warnings. Architecture analysis covered 3,265 source references across seven
applications; xref found no cycles. See [G2 qualification](qualification/2026-10-30-g2.md)
for installation evidence, the runtime privacy/TLS repairs and the distinction
between memory cleanup and durable recovery; see
[G3 codec evidence](qualification/2026-10-30-g3-codec.md) for AMQP increments.

After the transport/message schema increment and independent review repairs,
all eight root gates passed **612 tests, zero failures** (AMQP 309; other
application counts unchanged). All 25 MQTT external-peer tests remained enabled.
Compilation had zero warnings; architecture analysis covered 3,986 source
references across seven applications and xref found no cycles. This updates
pure-codec evidence, not either AMQP network-role exit.

The G3 client exit passed **698 tests, zero failures** with all eight root gates:
core 9, AMQP 368, MQTT 191, storage 3, client 38, runtime 21, composition 68.
All **30 external peer tests** ran, adding five real Artemis tests to the 25
MQTT paths. Compilation had zero warnings, architecture covered 4,735 source
references across seven applications, and xref found no cycles. Seven new
archives and all three clean consumers passed, including an independently
installed AMQP client exchanging with Artemis without broker packages. See
[G3 client qualification](qualification/2026-10-30-g3-client.md) for precise
scope, review repairs, peer versions, logs and recovery limitations.

## Mandatory semantics and profile limits

The clause inventories record accepted mandatory/conditional semantics,
explicit refusals and inactive predicates with source/test/interop/recovery
locators. The rows here summarize those detailed ledgers and their profile
boundaries; they are not a universal conformance declaration.

| Behavior | Required boundary |
| --- | --- |
| MQTT QoS | Initial G2 advertised Maximum QoS=1; G5 adds QoS2 and omits Maximum QoS for its normative default of 2; no business exactly-once claim |
| MQTT optional features | Explicit capability negotiation, supported behavior or specified rejection; never silently ignore accepted mandatory semantics |
| Sessions and reconnect | Clean Start/Session Expiry/Session Present with actual local state; stale connection events fenced; bounded retries |
| Host/network pressure | Finite packets, parser, pending/inflight/deliveries; explicit demand/ACK; host death and saturation tested |
| Operation completion | Local admission, peer handshake, explicit failure and unknown outcome are separate |
| Memory vs durability | Memory ACK is responsibility in memory; disk ACK waits for selected durable-through in G6 |
| AMQP responsibilities | Independent local/remote handles/channels; credit/sequence/settlement; release/redelivery and disconnect owner retained |
| Persistent recovery | Actual WAL/sync/streaming recovery; partial writes, crash/restart and failed/uncertain sync evidence required |
| Deferred | Clustering/Raft, management UI, cross-protocol bridge, AMQP transactions, QUIC, MQTT3.1 runtime |

## Stage exits and remaining execution

G0/G1 and the bounded G2 QoS0/1 memory path retain their original qualification.
G3 independently qualifies the AMQP client against Artemis; G4 adds the
Queue.Machine/memory broker and independent Proton. The G5 increment passed
884 tests and its original seven-archive/three-consumer exit. G6 adds actual
Linux local durability, streaming replay and installed crash/recovery checks.
Their dated counts identify earlier source; the final 1,117-test integrated
exit above includes all subsequent repairs.

The [MQTT inventory](qualification/2026-10-30-mqtt5-requirement-inventory.md)
records 252 literal requirement IDs and 74 unnumbered requirements; the
[AMQP inventory](qualification/2026-10-30-amqp10-requirement-inventory.md)
records 220 modal sentences and 302 grammar entries. Clause audits retain
source, unit, independent-peer and recovery locators for accepted semantics,
explicit refusals and inactive conditions. No confirmed source defect remains
pending in those reviews; individual execution limits remain explicit.

G7 has hash-pinned peers, four paths in CI, deterministic model/fuzz/resource
checks, final root gates and clean installation. The final-source 15-minute
soak passed all four paths with bounded resources, exact bytes/order, drained
responsibility and confirmed cleanup. Local selected-profile G0–G7 acceptance
is complete. At that local qualification checkpoint, hosted execution was
NOT RUN; commit, push, publication and deployment were outside the Goal.
The owner's subsequent [release authorization](ACTIVE_SCOPE.md#release-authorization--2026-10-30)
permits the `0.2.0` commit and publication. Dated local evidence remains
unchanged; hosted and public installation results accompany the
[GitHub release](https://github.com/gsmlg-dev/graviton_mq/releases/tag/v0.2.0).
These are bounded selected-profile results, not a universal AMQP1/MQTT5
conformance certificate or production throughput/security/power-loss claim.
