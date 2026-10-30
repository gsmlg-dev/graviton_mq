# G0–G7 local selected-profile qualification — 2026-10-30

This records uncommitted working-tree behavior at rechecked HEAD
`66b4a6c161f521cb6b6379ad4f952dcdc7f3447a`. It is not a hosted Actions,
publication, production deployment or complete protocol conformance result.
The local G0–G7 exits passed for the selected profiles: final root gates,
four independent network paths, installed consumers and final-source sustained
traffic. Profile/refusal boundaries and execution limits stay visible in the
[capability matrix](../CAPABILITIES.md).

## Independent peers and CI path

`scripts/interop/peers.json` and the hash-pinned Python requirement files fix
Mosquitto 2.1.2, Paho 2.1.0, Qpid Proton 0.40.0 and Artemis 2.41.0. The default
`prepare.py` path actually downloaded, verified and built Mosquitto with TLS
and its pub/sub/passwd tools, and installed Proton with its native CFFI/TLS
extension. The successful build is recorded in
`/tmp/graviton-goal-20261029-v4ozpgqs/g7-prepare-complete-tools.log`; selected
executables and library paths are in `g7-build-peers/environment.json`.

`run.py` starts temporary loopback Mosquitto and Artemis peers, verifies the
AMQP header over TCP and trusted TLS, supplies all interoperability test
variables, and records exit status plus peer/instance cleanup. Artemis TLS
uses a temporary one-day CA and PKCS12 key; only public certificates are
retained in evidence. Private keys disappear with the temporary instance.

The CI interoperability job runs all four real network paths, process/VM
recovery cases, a five-minute four-role memory soak, and seven archives/three
installed consumers. Ordinary CI also runs deterministic model and decoder
tests. Local `actionlint` and Python syntax checks passed. Hosted CI is
**NOT RUN**: this working tree has not been committed or pushed, and the Goal
does not authorize either operation. A workflow definition is not a hosted
success result.

## Deterministic model and decoder tests

Twelve focused tests passed under the declared Elixir 1.18.4 / OTP 28.5.0.1 pair
with seed 20261030; log `g7-model-final.log`:

| Surface | Evidence | Limit |
| --- | --- | --- |
| Queue.Machine independent model | 4,263 transitions; eight fixed seeds, each with 500 generated commands; 64 portable checkpoint continuations and abandoned-lease continuation | Pure model/checkpoint evidence is separate from WAL recovery |
| Oracle sensitivity | Six intentionally corrupted states are detected | Does not establish all possible state corruptions |
| AMQP decoders | 6,642 adversarial calls; 288 semantic vectors and 2,112 strict prefixes | Deterministic bounded corpus, not exhaustive fuzzing |
| MQTT decoders | 7,444 adversarial calls; 96 semantic vectors and 1,364 packet/frame prefixes; all 256 headers and 64 nonminimal VBIs | MQTT3.1.1 remains CONNECT/CONNACK codec-only |
| Frame/stream boundaries | 1,458 boundary cases and 3,465 fragmented/coalesced streams | No network role is inferred from these pure tests |

The complete root test gate includes these files. Resource limits also have
targeted runtime coverage for full writer pools, non-reading sockets, finite
parser work, command/admission ledgers, auth deadlines, host demand and failure
cleanup. This does not establish a production throughput or maximum-capacity
claim.

## Fifteen-minute traffic checkpoint

The actual command was:

```bash
python3 scripts/interop/run.py --directory /tmp/graviton-goal-20261029-v4ozpgqs/g7-build-peers \
  --logs /tmp/graviton-goal-20261029-v4ozpgqs/g7-soak-900-wrapper -- \
  mix run --no-start scripts/interop/soak.exs --duration 900 --rate 10 \
  --payload-bytes 2048 --logs /tmp/graviton-goal-20261029-v4ozpgqs/g7-soak-900
```

This checkpoint used code
compiled before the subsequent reviewed settlement/order/Close repairs.
It must not be presented as their final integration proof.

The memory/TCP workload ran four paths concurrently with one outstanding
round trip per role, a 2 KiB payload, target 10 round trips/second/role, alternating
MQTT QoS1/2, and AMQP Accepted/receiver settlement. Exact payloads, order,
rolling digests, progress and final drain were asserted.

| Role | Completed round trips | Sent/received payload bytes per direction |
| --- | --- | --- |
| Graviton MQTT client↔Mosquitto | 9,000 | 18,432,000 |
| Paho↔Graviton MQTT broker | 8,988 | 18,407,424 |
| Graviton AMQP client↔Artemis | 9,000 | 18,432,000 |
| Proton↔Graviton AMQP broker | 8,988 | 18,407,424 |

The 900,174 ms run passed. Across 866 samples, BEAM memory peaked at 57,527,432 B,
VM RSS at 171,040,768 B (initial sample; final 121,569,280 B), 115 processes,
11 ports, 31 FDs and coordinator mailbox 1. All broker queues, inflight data,
connections, Links and MQTT sessions drained to zero. Worker, broker, Python
peer and wrapper cleanup all reported true.

Evidence is `g7-soak-900/{samples.jsonl,summary.json}` and
`g7-soak-900-wrapper/evidence.json`. These are bounded local memory/TCP
results. They do not qualify TLS load, durable sustained load, hardware
power loss, unbounded admission or a multi-hour production soak.

## Final integrated gates and installed consumers

All eight required root commands passed under Elixir 1.18.4 / OTP 28.5.0.1 with
`ERL_FLAGS='+S 4:4'`. The final `mix test` run used seed 3841 with all independent
peer groups enabled: **1,117 tests, zero failures**.

| Application | Passing tests |
| --- | ---: |
| core | 38 |
| AMQP1 pure protocol | 453 |
| MQTT pure protocol | 264 |
| storage | 37 |
| independent clients | 95 |
| broker runtime | 78 |
| broker composition/network | 152 |

Compilation had zero warnings. The compiler-xref/parsed-syntax architecture
check passed for **6,738 source references across seven applications**; the
independent xref graph found no cycles. The source remained unchanged through
the complete sequence. Results and exact commands are in
`/tmp/graviton-goal-20261029-v4ozpgqs/g7-qualified-gates.json` and
`g7-qualified-gate-01.log` through `08.log`; peer versions, ports, verified
Artemis TLS and successful cleanup are in `g7-qualified-gate-peers/evidence.json`.

The gated working-tree source manifest is `g7-qualified-source-manifest.json`,
with SHA256 digest:

```text
bbf3959e0b7e5ce381fe45eae003ba456551ad49579f9edc2581ad8c8117f463
```

This identifies 320 source/config/test/script/workflow files at the rechecked
HEAD; it does not identify a published commit or include later Markdown edits.
The earlier `g7-selected-profile-final` attempt remains **FAIL**: 1,117 tests,
one CONNECT assertion failed before its intended publication fault injection.
A controlled 1,100 ms first-WAL-sync hold reproduced CONNACK 136 with storage
still ready: durable CONNECT preparation consumed the default 1,000 ms
authentication deadline. The same hold with the test fixture's supported
3,000 ms budget returned CONNACK 0. Production deadline enforcement and all
ACK/barrier/failure assertions remain unchanged. The original seeded 13-test
file passed after the two-line fixture adjustment. Diagnostic and final logs
are `g7-mqtt-sync-{default-deadline-red,deadline-green-3000,final-file}.log`.

Seven archives and all three clean consumers passed from
`g7-qualified-hex-staging`: default standalone broker, host-supervised embedding
and client-only installation. Installed MQTT/AMQP exchanges used the pinned
independent peers. Additional installed checks passed MQTT durable-through
and retained recovery, Proton durable enqueue/replay/retirement, and client
host-owned QoS2 recovery against Mosquitto. The client-only dependency closure
contains no broker composition, runtime or storage package and starts no broker.

All 145 archived library files match the gated source byte for byte. Actual
archive metadata verifies all seven package identities, version 0.1.0 and exact
internal dependency requirements. SHA256 archive hashes and source matching
are in `g7-qualified-package-manifest.json`; execution results are in
`g7-qualified-packages.json`, `g7-qualified-packages.log` and
`g7-qualified-package-peers/evidence.json`. Both package peers stopped and
their temporary instance was removed; no package was published.

## Final sustained traffic

The new final-source run passed in **900,182 ms**, using separate temporary
peers and `MIX_BUILD_PATH=.../g7-soak-final-build`. Its inner command was:

```bash
mix run --no-start scripts/interop/soak.exs --duration 900 --rate 10 \
  --payload-bytes 2048 --logs /tmp/graviton-goal-20261029-v4ozpgqs/g7-qualified-soak
```

`scripts/interop/run.py` supplied the pinned peers; its wrapper evidence is
`g7-qualified-soak-peers/evidence.json`. The workload uses the same memory/TCP
profile and assertions as the earlier checkpoint, now on the final source:

| Independent network path | Completed round trips | Payload bytes sent/received per direction |
| --- | ---: | ---: |
| Graviton MQTT client ↔ Mosquitto | 9,000 | 18,432,000 |
| Paho ↔ Graviton MQTT broker | 8,988 | 18,407,424 |
| Graviton AMQP client ↔ Artemis | 9,000 | 18,432,000 |
| Proton ↔ Graviton AMQP broker | 8,988 | 18,407,424 |

All **35,976** round trips checked exact payload bytes/order and rolling
digests. MQTT alternated QoS1/2; AMQP explicitly settled each delivery. Across
866 samples, BEAM memory peaked at 56,262,328 B, VM RSS at 166,600,704 B
(final sample 112,197,632 B), 116 processes, 11 ports, 31 FDs and coordinator
mailbox 2. Final broker queue bytes/messages/inflight, operations, connections,
Links, MQTT pending/prepared/retained data and sessions all reached zero.
Workers, broker and Python peers stopped; the wrapper also stopped both
independent brokers and removed its temporary instance.

Exact samples and completion are `g7-qualified-soak/{samples.jsonl,summary.json}`.
`g7-qualified-soak-manifest.json` fixes 168 production/package/interop files;
all match their corresponding gated-source hashes. Its narrower manifest
digest is `15c4b8d1b70a073de79f919ad0d329ca71b542454f69075a2f5b20ea6cf8c37f`.
The source remained unchanged for the full run. This bounded result does not
establish TLS load, durable sustained load, multi-hour production operation,
hardware power-loss behavior or unbounded admission.

## Selected-profile acceptance boundary

The [MQTT clause audit](2026-10-30-mqtt5-clause-audit.md) and
[AMQP clause audit](2026-10-30-amqp10-clause-audit.md) map selected-profile
requirements to actual code, unit, independent-peer and recovery evidence.
The individual MQTT inventory covers 252 literal requirement IDs and 74
unnumbered requirements; the AMQP inventory covers 220 modal sentences and 302
grammar entries. The audits and their repairs retain exact source, unit,
independent-peer and recovery locators and execution limits. Recommendations,
inactive conditions and refused optional profiles are distinct from accepted
mandatory semantics. No confirmed source defect remains pending in these
reviews. Final gates, installed consumers and source-fixed sustained traffic
close the **local selected-profile G0–G7 exit**. Hosted CI remains NOT RUN;
the four-role workflow, including crash recovery, soak and clean installation,
has been validated locally. No commit, push, package publication or deployment
was performed. These results do not establish every optional AMQP/MQTT feature
or every possible byte/state permutation.

The consolidated machine-readable result is
`/tmp/graviton-goal-20261029-v4ozpgqs/g7-qualified-acceptance.json`, linking the
source digest, root gates, installed consumers, sustained traffic and all three
independent-peer cleanup records. The [support matrix](../CAPABILITIES.md)
records each role's requirements, implementation, unit/interop/recovery
evidence and profile limits.

AMQP client reconnect/resumption and process/VM recovery remain
unimplemented. Enhanced MQTT authentication, MQTT3.1.1 runtime, AMQP SASL,
dynamic/durable termini, filters, TTL/absolute expiry and transactions retain
their explicit profile/rejection boundaries. Clustering/Raft, management UI,
bridges and QUIC remain outside this Goal.
