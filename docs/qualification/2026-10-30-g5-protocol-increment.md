# G5 protocol increment — 2026-10-30

Working-tree evidence at rechecked HEAD
`66b4a6c161f521cb6b6379ad4f952dcdc7f3447a`; no commit or publication.
G5 was active at this checkpoint. This historical increment does not close
the overall G0–G7 Goal or claim full mandatory-clause conformance. Subsequent
clause review and final integrated evidence are recorded in
[G7 qualification](2026-10-30-g7-local-qualification.md).

| Requirement | Implementation | Evidence |
| --- | --- | --- |
| MQTT5 §§3.4–3.7, 4.3.3: QoS2 wire and ownership | Shared PUBREC/PUBREL/PUBCOMP codecs; separate pure Client/Broker transitions | Codec reason/flags/property vectors; duplicate, identifier reuse, host ACK, expiry and late-receipt tests |
| MQTT5 §§4.4, 4.9: retry phases and quotas | Connection-local quotas; session-owned QoS2 state | Six new client→Mosquitto 2.1.2 tests: TCP/TLS independent CLI send/receive, negative PUBREC, lost PUBREC/PUBCOMP and inbound host ownership |
| MQTT5 server recovery, without disk claims | Broker method B, session-owned incoming markers and outgoing phases | Five independent Paho 2.1.0 tests cut transport before PUBREC, PUBREL or PUBCOMP, in both delivery directions; Session Present and no duplicate onward delivery asserted |
| MQTT5 capability and packet limits | Configurable Maximum QoS; omission means QoS2; oversized outbound message completion | Historical QoS1 downgrade regressions retained, actual Mosquitto QoS2 publisher/subscriber through broker, host path and MQTT-3.1.2-25 regression |
| AMQP Transport §§2.5.6, 2.6.7: Flow and drain | Pure synchronization; owner-confirmed exhaustion with stale-token fencing | Pure wrap/window vectors and real Proton drain of preloaded fragmented messages, empty queue and subsequent publication |
| AMQP Messaging §3.2.10: mandatory annotations | Bounded `AnnotationPolicy` scans keys while skipping opaque body/footer values; SessionEngine rejects before delivery | Shared client/broker receiver vectors and actual Proton mandatory-key rejection; optional annotations retain exact bytes |
| AMQP honest terminus negotiation | Source/Target normalization, expiry/capability/outcome admission | Unsupported requested semantics rejected or omitted; negotiated outcomes enforced in both receiver directions |
| AMQP uint32 serial and bounded writes | Existing pure serial arithmetic and outgoing shell | Artemis 2.41.0 receives three messages with Transfer/delivery IDs seeded at `0xFFFFFFFE`, crossing zero; non-reading TCP peer bounds send failure/status/shutdown and produces unknown, never success |

The Paho final-PUBCOMP-loss case deliberately receives `0x92` for a resumed
PUBREL whose marker was already retired. The test asserts that explicit code
and absence of a duplicate publication; it does not relabel it as positive
publication success. QoS2 does not imply business exactly-once processing.
All memory acknowledgements still lack disk durability.

Focused broker tests passed 59 tests; the new Paho tests passed 5; the AMQP
shell suite passed 12 and the fresh Artemis suite passed 6. The independent
worker's shared/client and Proton evidence is integrated by the root gates
below, rather than inferred solely from a handoff.

## Integrated verification

All eight root gates passed under Elixir 1.18.4 / OTP 28.5.0.1 with
`ERL_FLAGS='+S 4:4'`. The integrated run passed **884 tests, zero failures**:
core 31, AMQP 412, MQTT 224, storage 3, client 46, runtime 69 and composition
99. All **54 external peer tests** ran: MQTT client→Mosquitto 17,
Mosquitto/Paho→MQTT broker 20, AMQP client↔Artemis 6 and Proton↔AMQP broker 11.
Compilation had zero warnings. Architecture analyzed **5,910 source
references across seven applications**; xref found no cycles.

Root command results and logs are under
`/tmp/graviton-goal-20261029-v4ozpgqs/g5-protocol-gates.json` and
`g5-protocol-gate-01.log` through `08.log`. Expected negative TLS and injected
process-failure reports are separate from compilation warnings and assertions.
The root Artemis wrapper confirmed its peer stopped and temporary instance
was removed. Seven archives were subsequently rebuilt and all three clean
consumers passed: standalone broker, host-supervised broker, and independently
installed clients. MQTT exchanges used Mosquitto 2.1.2; AMQP exchanges used
Proton 0.40.0 and Artemis 2.41.0. The client dependency closure contained no
broker composition/runtime/storage applications. The full log is
`/tmp/graviton-goal-20261029-v4ozpgqs/g5-final-hex-build-consumers.log`;
staging is in `g5-final-hex-staging`. Both temporary peers confirmed cleanup.

## Remaining acceptance at this checkpoint

G5 still requires the complete selected-profile clause ledger, further AMQP
scalar/body interoperability and the remaining settlement/disconnect matrix.
Arrays in the semantic value codec remain symbol-only; the annotation scanner
does not expand that public codec. MQTT3.1.1 CONNECT/CONNACK regressions remain
separate from its unimplemented runtime. G6 requires real persistence and
crash/restart qualification; G7 requires all four paths in CI and resource,
fuzz/model, soak and installation qualification.

The later [G6 increment](2026-10-30-g6-local-durability.md) expanded homogeneous
arrays and qualified local persistence. The [MQTT clause inventory](2026-10-30-mqtt5-requirement-inventory.md)
and [AMQP clause inventory](2026-10-30-amqp10-requirement-inventory.md) supersede
the outstanding ledger/body/settlement work listed above; the original
884-test result retains its original source and scope. Current acceptance is
recorded in the [capability matrix](../CAPABILITIES.md).
