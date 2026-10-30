# MQTT 5 selected-profile clause audit — 2026-10-30

This audit traces the uncommitted working tree based on
`66b4a6c161f521cb6b6379ad4f952dcdc7f3447a`, including the G5/G6 increments.
It is a clause-group ledger for the implemented MQTT 5 client and single-node
broker profiles, not an all-clause conformance certificate. Historical G1/G2
records describe their earlier QoS0/1 profiles; they do not override current
QoS2 and opt-in persistence source or qualify later behavior automatically.

The normative reference is [OASIS MQTT Version 5.0, OASIS Standard,
7 March 2019](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
The review used its cached text at
`/tmp/graviton-goal-20261029-v4ozpgqs/mqtt-v5.0-os.txt`.
Section references below include their conditional requirements, rather than
treating every optional property as an obligation to implement its feature.

## Profile and evidence notation

Both operational roles use MQTT 5, QoS0/1/2, ordinary subscriptions, TCP/TLS,
bounded resources, explicit ownership and configurable memory or local durable
state. Broker Maximum QoS can be restricted to 0 or 1. Topic Alias Maximum is
zero. The broker advertises Shared Subscriptions and Subscription Identifiers
unavailable. Enhanced authentication is rejected explicitly. MQTT3.1.1 remains
CONNECT/CONNACK codec compatibility only; its operational role is unqualified.
No external MQTT implementation supplies production client or broker behavior.

Evidence labels distinguish what was established:

- **S**: source trace, including an explicit failure path; not execution proof.
- **T**: deterministic regression exists; actual execution is stated separately.
- **I**: independent-peer evidence recorded in the linked qualification/test.
- **R**: process/VM loss or storage-fault evidence; transport reconnect alone is
  not persistent recovery.
- **GAP**: missing behavior or missing qualification, identified explicitly.

Repository path abbreviations keep the ledger readable:

- `M` = `apps/graviton_mq_mqtt`; pure `client.ex`, `broker.ex`, `topic.ex` are
  under `M/lib/graviton_mq/mqtt/`; codecs are under its `codec/` directory.
- `C` = `apps/graviton_mq_client`; owner `lib/graviton_mq/client/mqtt.ex`, with
  client-owned `mqtt/{config,checkpoint,store}.ex` and `store/io.ex`.
- `B` = `apps/graviton_mq_runtime`; broker shells under
  `lib/graviton_mq/runtime/mqtt/`, including `broker.ex`, `connection.ex`,
  `writer.ex`, `auth.ex` and `persistence.ex`.
- `P` = `apps/graviton_mq`; broker network tests under
  `test/graviton_mq/`. Test paths in other applications are under `test/`.

## Mandatory and conditional clause-group ledger

| Clause group / applicability | Requirement and implementation trace | Regression / independent or recovery evidence | Limits and gaps |
| --- | --- | --- | --- |
| §§1.5, 2.1–2.2: mandatory wire representation | **S:** `codec/{wire,frame,packet,properties,operational}.ex` validate lengths, canonical variable integers, fixed flags, UTF-8/NUL, nonzero Packet IDs, property scopes, singleton duplicates and repeatable properties. Frame boundaries remain separate from packet interpretation. | **T:** `M/test/codec/{frame,properties,operational_packets,operational_validation,qos2,version_boundary}_test.exs`; historical CONNECT/CONNACK suites. `g7_fuzz_test.exs` uses independent vectors, prefixes, invalid headers and bounded adversarial decoding. | Control characters/noncharacters discouraged with SHOULD NOT are distinct from mandatory malformed UTF-8/NUL rejection. Fuzz coverage is bounded, not proof over all byte strings. |
| §§3.1–3.2, 3.1.4: mandatory CONNECT ordering/version/identity | **S:** broker `receive_packet/5`, `attach/4`, `available_client_id/2`; client `connect_profile/1` and CONNACK handling. CONNECT precedes other traffic; authentication precedes attach/takeover; empty IDs are assigned without collision; Clean Start/Session Present and assigned-ID rules are checked. | **T:** `M/test/{client,broker}_test.exs`, codec CONNECT/CONNACK tests. **I:** `C/test/mqtt_mosquitto_test.exs` real negotiation/negative CONNACK; `P/test/graviton_mq/mqtt_mosquitto_test.exs` real authentication and sessions. | MQTT3.1 is unsupported; legacy codec tests do not qualify a legacy network role. Enhanced auth follows the explicit refusal row below. |
| §§3.1.2.11, 3.2.2.3: conditional negotiated limits/defaults | **S:** client `connected/3`, admission and resume checks; broker CONNACK capabilities, `outbound_fits?/3`, `send_packet/5`. Enforce Receive Maximum, packet size, Maximum QoS and Retain Available. Zero/duplicate illegal values reject in properties codec. Server Keep Alive overrides CONNECT. | **T:** `M/test/client_test.exs` negotiated send bounds, smaller limits on resume, defaults and actual-CONNECT Session Expiry fallback; broker profile and oversize tests. New `protocol_error_reason_test.exs` maps the decoder's exact oversized-packet error to 0x95 in both roles. **I:** Mosquitto local oversize rejection and real QoS grants. | The audit's specific size-diagnostic gap was repaired below. Other malformed/unknown decoder errors retain existing 0x81 behavior; this is not a claim of complete error-code diagnostics. |
| MQTT-3.1.2-24/25, MQTT-3.2.2-15: mandatory peer packet bound | **S:** every encoded client send uses peer bound; broker separately discards an oversized onward Application Message as locally completed, while control writes remain bounded. | **T:** client stop/automatic ACK/PING/resume size regressions; `broker_qos2_test.exs` oversize recipient remains connected. **I:** Mosquitto negotiated packet-size exchange. | Capacity refusal is explicit. Packet-size defense is not a memory bound for arbitrary application-owned payloads before local API admission. |
| §§3.1.2.10, 3.12–3.13: conditional Keep Alive | **S:** pure client `keep_alive/2`, broker `expire/2`; owners supply clocks/timers. Client PINGREQ requires PINGRESP; server closes after 1.5× interval; zero disables keep alive; stale generations cannot advance state. | **T:** `M/test/client_test.exs`; `P/test/graviton_mq/mqtt_connection_test.exs` fragmented/coalesced input and real timeout. **I:** both Mosquitto roles exercise real PINGREQ/PINGRESP. | Timers use bounded runtime ticks, not hard real-time scheduling. Partial input does not count as a complete Control Packet. |
| §§3.3–3.7, 4.3.1–4.3.3: mandatory QoS responsibility | **S:** client publication/incoming ledgers and host ACK; broker publication prepare/commit, incoming QoS2 markers and outgoing phases. QoS0 is socket/memory acceptance; QoS1 terminates at PUBACK; QoS2 at PUBCOMP. Negative acknowledgements retain failure semantics. Duplicate QoS2 cannot forward twice. | **T:** `M/test/{client_qos2,broker_qos2,client_terminal}_test.exs`. **I:** `C/test/mqtt_qos2_mosquitto_test.exs`, `P/test/graviton_mq/{mqtt_mosquitto,mqtt_qos2_paho}_test.exs`, including lost acknowledgement cut points. | QoS2 does not promise business exactly-once processing. Client host delivery demand and ACK are explicit. Memory ACK is not a durability receipt. |
| §§2.2.1, 4.3–4.4: mandatory Packet ID lifetime/retry | **S:** client IDs cover pending operations; broker separates incoming request/QoS2 IDs from outgoing IDs. Resume retains IDs and DUP PUBLISH; positive PUBREC resumes PUBREL only; healthy connections have no periodic publication retry. A terminal unknown host result retains an unfinished protocol obligation while its session survives. | **T:** client terminal/timeout/late-write-ID-reuse and broker collision tests. **I:** lost PUBACK/PUBREC/PUBCOMP against Mosquitto and Paho. **R:** `C/test/mqtt_persistence_mosquitto_test.exs` terminal result followed by protocol-only recovery. | Finite session loss/expiry can end protocol responsibility and is reported as unknown, never synthesized success. |
| §4.6, MQTT-4.6.0-1–6: mandatory ordering | **S:** source-ordered authorization commits, original publication order and separate ACK order; both roles retain first-PUBREC release order independently from delivery order. | **T:** `client_test.exs` reverse host ACK order; both QoS2 suites exercise release order; `broker_test.exs` reverse auth completion; `broker_checkpoint_test.exs` restores release order. **I:** Paho two-inflight PUBREL transport-cut/resume observes identical release order, no repeated PUBLISH, single host delivery and completion with Receive Maximum reduced2→1. | Reverse-PUBREC unit inputs are adversarial: MQTT-4.6.0-3 requires a conforming receiver to send PUBREC in PUBLISH order. They test defensive receipt-order retention; the independent case follows that requirement. Final integrated gates remain separate. |
| §4.9, MQTT-4.9.0-1–3: mandatory send/receive quota | **S:** both endpoints reset quotas at each connection, count QoS1/2 together, free on PUBACK/PUBCOMP or negative PUBREC, and retain QoS2 quota at positive PUBREC. PUBREL runs without PUBLISH quota. | **T:** both QoS2 suites exercise new-connection quotas and older PUBREL; broker regression places PUBREL behind two earlier PUBLISH obligations under resumed Receive Maximum=1. **I:** Paho mixed-QoS1/2 resume with Receive Maximum reduced3→1, manual host ACK and PUBREL before held QoS1 frees quota; two pending PUBRELs resume with Receive Maximum reduced2→1. | Host backlog bounds are additional local limits, distinct from protocol Receive Maximum. |
| §§3.3.1, 3.8–3.11, 4.8.1: mandatory ordinary subscription behavior | **S:** `topic.ex`, operational subscription options, broker `route/2`, `commit_subscribe_snapshot/3`, unsubscribe commit; client validates grant count/QoS. Overlaps coalesce at maximum granted QoS bounded by publication QoS; retained RH=0/1/2, RAP, No Local and per-filter reasons are explicit. Repeated filters in a SUBSCRIBE apply in order against successful prior occurrences. | **T:** `M/test/{topic,broker,client}_test.exs` including replacement incarnation, late authorization, duplicate filter/grant/replay order, inflight preservation, UNSUBSCRIBE serialization and SUBACK/UNSUBACK cardinality. **I:** Mosquitto grant/unsubscribe, retained replay/delete and offline delivery; Paho duplicate-filter retained suppression and original replay-QoS cases. | One copy for overlapping ordinary subscriptions is allowed. Host subscription identities are outside MQTT sessions and do not survive owner loss. |
| §4.7, MQTT-4.7.2-1: mandatory topic/filter comparison | **S:** `topic.ex` uses exact UTF-8 bytes/levels, preserves empty levels and case, validates wildcard placement and excludes `$` topics from root wildcard matches. | **T:** `M/test/topic_test.exs` literal Unicode, empty-level, wildcard and system-topic vectors. **I:** real ordinary wildcard ACL/routing cases. | No Unicode normalization or application-specific topic remapping. Shared syntax is refused according to advertised capability. |
| §§3.3.2.3.2/.5/.6/.7/.9: conditional metadata forwarding | **S:** broker `message/4` retains the ordered properties; `recipient_packet/3` removes only hop-local alias/subscription IDs and reduces expiry. PFI, Content Type, Response Topic, Correlation Data and repeated User Property order are preserved. Client delivers ordered properties to the host. | **T:** property codec scope/order/type, PFI rejection and expiry regressions. **I/R:** `C/test/mqtt_metadata_mosquitto_test.exs` exchanges both directions with Paho via Mosquitto; `P/test/graviton_mq/mqtt_metadata_paho_test.exs` compares Paho→broker→Paho live, retained and durable owner-kill/recovered retained delivery, each at QoS1/2. | All five non-expiry metadata fields, binary correlation bytes and repeated User Property order are compared exactly; retained/recovered expiry must decrease. These bounded vectors do not qualify every possible property/payload combination. |
| §§3.3.2.3.3, 3.3.1.3: conditional message/retained expiry | **S:** broker records deadline at admission, removes expired unsent copies/retained entries, reduces onward expiry and preserves already-started QoS handshakes. Client suppresses expired undelivered incoming messages but acknowledges their QoS responsibility; reconnect reduces outgoing lifetime. | **T:** client expiry/order, broker offline/retained/prepared expiry and QoS2-after-expiry regressions. **I:** Mosquitto later retained subscriber. **R:** checkpoint downtime and Paho retained/offline expiry; Mosquitto client retransmission expiry. | No promise of hard real-time deletion; owner ticks/admission prune state. Already delivered host ownership does not expire silently. |
| §§3.1.2.5–.9, 3.1.3.2, 3.14: conditional Will | **S:** broker authenticates and authorizes the Will topic, stores its job, publishes at min(Will Delay, Session Expiry), cancels when the session resumes in time, and starts Will Message Expiry at publication. Only normal DISCONNECT0 suppresses Will; other accepted reasons retain it. | **T:** `broker_test.exs` Will pressure, principal/byte bounds, reconnect cancellation and original expiry across retry. **I:** abnormal Mosquitto death and Paho DISCONNECT4/128 delay versus DISCONNECT0 suppression. **R:** interrupted authorization and scheduled deadline recovery tests. | A due Will under finite capacity remains an explicit bounded retry obligation; no unbounded buffer or fabricated successful delivery. |
| §§3.1.2.11.7, 3.14, 4.13: mandatory direction/error handling | **S:** client rejects forbidden problem-information properties when requested off, client-only server DISCONNECT reason4 and server-sent Session Expiry. Broker checks client-direction reason codes and cannot increase initial zero Session Expiry on DISCONNECT. Malformed/unexpected packets close the affected connection; negative completion is not success. | **T:** `client_problem_information_test.exs`, client DISCONNECT/cardinality/negative ACK tests; broker invalid-state tests; specific-size diagnosis regression. **I:** actual auth denial, negative PUBACK/PUBREC, shared-off DISCONNECT. | Complete malformed-packet/reason-code combinations are not independently exercised. §4.13.1 requires the server to close (MUST); it recommends sending the specific DISCONNECT diagnosis (SHOULD). Reason String is diagnostic, not application success data. |
| §§3.1.2.11.5, 3.2.2.3.8, 3.3.2.3.4: optional aliases, conditional acceptance | **S:** both roles advertise Topic Alias Maximum=0; no alias mapping is established or carried across connections; received alias use is rejected with 0x94 and outgoing local requests reject aliases. | **T:** client inbound alias and broker alias/capability branches; property codec alias zero/duplicate bounds. | Alias compression is unavailable. Arbitrary positive peer Topic Alias Maximum need not be used by a sender. |
| §§3.2.2.3.12–.13, 4.8.2: optional shared subscriptions / IDs | **S:** broker advertises both unavailable; Shared SUBSCRIBE disconnects0x9E before authorization, Subscription ID use disconnects0xA1. Client enforces peer capability flags before admission; shared No Local is invalid at codec level. | **T:** broker shared-off and subscription capability tests, operational option validation. **I:** `P/test/graviton_mq/mqtt_paho_test.exs` observes actual compact DISCONNECT `9e`. | Broker has no shared scheduler or Subscription Identifier delivery semantics. These are excluded by explicit negotiation, not accepted and silently ignored. |
| §§3.1.2.11.9–.10, 3.15, 4.12: optional enhanced authentication | **S:** client configuration/new profile rejects method/data; broker CONNECT rejects with 0x8C; AUTH has no supported operational schema and cannot create a successful session. | **T:** client unsupported-profile and broker/codec failure paths. Username/password and explicit auth policy have separate real-peer evidence. | Enhanced authentication/re-authentication is unsupported. TLS/auth callback behavior is infrastructure/security policy, not an AUTH implementation. |
| §§3.1.2.11.6, 3.2.2.3.15–.16, 4.10–4.11: optional response/redirection assistance | **S:** Response Topic/Correlation Data are preserved application metadata. Broker omits Response Information; client does not redirect automatically on Server Reference. | Specification permits omission even if Request Response Information=1, and explicitly permits a client to ignore Server Reference (§4.11). | These omissions are not mandatory behavior gaps. No automatic request/reply application or redirect/reconnect policy is claimed. |
| §§3.1.2.11.2, 4.1–4.2: mandatory session state, selected durability policy | **S:** pure session state contains subscriptions and inflight QoS phases. `broker/checkpoint.ex`, `B/persistence.ex`, client Checkpoint/Store retain protocol responsibility without runtime handles/credentials. Owners commit before effects; WAL append differs from synchronized durable-through. | **T/R:** broker checkpoint schema/time tests, `P/test/graviton_mq/{mqtt_persistence,mqtt_persistence_paho}_test.exs`; client checkpoint/store/fault and Mosquitto process/VM-recovery tests. Actual barriers withhold PUBACK/PUBREC/PUBCOMP until durable publication. [G6 evidence](2026-10-30-g6-local-durability.md). | Memory mode does not survive owner loss. Local durable mode is bounded Linux storage with explicit failed/unknown results, no distributed storage, compaction or hardware power-loss guarantee. Offline timers count downtime; previously online loss is detected at recovery. |

## Reproduced broker defect and repair evidence

The audit executed current `broker.ex` in an isolated Elixir process against
cached codec beams, compiling only that source in memory. It did not change
production files or use the shared build. The first diagnostic printed:

```text
received_pubrec: [2, 1]
first_pubrel: [2, 1]
resumed_pubrel: [1, 2]
```

Sequence: establish a session with Session Expiry=60; subscribe at QoS2;
admit two outgoing QoS2 publications (IDs1,2); receive PUBREC2 then PUBREC1;
lose the connection; resume with Clean Start=false. Broker delivery order
retained original PUBLISH order but did not record PUBREC receipt order.
MQTT-4.6.0-4 requires PUBREL in the corresponding PUBREC order. This deliberate
reverse-PUBREC input is adversarial: a compliant receiver itself sends PUBREC
in PUBLISH receipt order (MQTT-4.6.0-3). The regression checks receipt-order
retention under that input; it is not a legal independent-peer flow claim.

A second diagnostic used three outgoing publications and PUBREC3, then resumed
with Receive Maximum=1. It sent DUP PUBLISH1 and stopped at PUBLISH2, leaving
PUBREL3 unsent at quota0. This violated control-flow progress independently
of PUBLISH quota (MQTT-4.9.0-3). Both symptoms came from scanning only the first
unsent entry in original publication order.

The narrow repair adds a stable first-PUBREC `release_order` using the existing
monotonic broker identity counter. Duplicate PUBREC does not change it.
`pump_session/3` selects pending PUBREL in that order before PUBLISH, while
PUBLISH retains original admission order and reserved Packet IDs. Checkpoint
capture/restore strictly retain the field, reject missing/zero/out-of-range or
duplicate release orders, and reject a release order on a PUBLISH-phase entry.
This changes the unreleased checkpoint schema: an older snapshot lacking the
field is rejected explicitly rather than guessed into a possibly false order.

The two broker resume tests and checkpoint roundtrip test were added first.
The assigned isolated build directory was
`/tmp/graviton-goal-20261029-v4ozpgqs/g5-mqtt-pubrel-order-build`:

```sh
mix test apps/graviton_mq_mqtt/test/broker_qos2_test.exs \
  apps/graviton_mq_mqtt/test/broker_checkpoint_test.exs --seed 0
```

Red: **20 tests, 3 expected assertion failures**, matching the two diagnostic
symptoms and checkpoint replay order. Green after the repair: **20 tests,
0 failures**. Logs are `g5-mqtt-pubrel-order-red.log` and
`g5-mqtt-pubrel-order-green.log` under the shared evidence directory above.
The complete pure MQTT suite then passed **254 tests, 0 failures**, seed
20261030, in `g5-mqtt-pubrel-order-suite.log`. Formatting and formatting checks
passed for all four changed source/test files. These tests are not an
independent-peer reproduction of the exact new ordering sequence. The final
root gates belong to the coordinating qualification record.

The coordinating task subsequently added the independent Paho test
`P/test/graviton_mq/mqtt_qos2_paho_test.exs` and
`P/test/support/mqtt_pubrel_quota_peer.py`. It sends two QoS1 messages followed
by a QoS2 message, holds host ACKs, loses the transport after PUBREL, and resumes
with Receive Maximum reduced from3 to1. Paho observes the resumed PUBREL before
any held QoS1 ACK is allowed to replenish quota, then drains all responsibility.
The complete Paho QoS2 file passed **6 tests, 0 failures** in
`g5-mqtt-quota-paho.log`; peer and instance cleanup were confirmed. This is
real mixed-phase quota progress, not reversed-PUBREC ordering evidence.

The later conforming Paho peer `P/test/support/mqtt_pubrel_order_peer.py`
receives two QoS2 PUBLISH packets, lets Paho send PUBREC in receipt order,
and cuts transport after both corresponding PUBRELs, before processing either
or sending PUBCOMP. On Session Present resume with Receive Maximum reduced
from2 to1 it asserts the same two PUBREL Packet IDs/order, no new PUBLISH,
exactly one host delivery per payload, and both PUBCOMPs. The full Paho QoS2
file passed **7 tests, 0 failures**, seed0, recorded in
`g7-mqtt-order.log`; `g7-mqtt-order-peers/evidence.json` confirms exit0 and
peer/temporary-instance cleanup. No intentionally reordered PUBREC is used.

## Independent metadata qualification

`C/test/support/mqtt_metadata_peer.py` uses pinned Paho2.1.0 to assert
Payload Format Indicator1, Content Type, a Unicode Response Topic, binary
Correlation Data `00ff80002a7e`, and four ordered User Properties including a
repeated key/value. Payload bytes, QoS and retained flags are also checked.
Both QoS1 and QoS2 travel in each client direction through Mosquitto2.1.2.
The broker cases use independent Paho on both sides for live, later retained
and durable owner-SIGKILL/restarted retained delivery. Retained/recovered cases
wait1200ms and require the original60-second expiry to decrease to1..59;
live cases accept1..60. The host independently checks client-received fields.

The five tests passed with seed20261030 in `g7-metadata-first.log` and isolated
`g7-metadata-build`. `g7-metadata-first-wrapper/evidence.json` records exit0,
stopped peers and removed temporary instance. Format checks, strict compile
and Python syntax passed. Six altered-field/order/expiry probes were rejected
by the assertion helper, with an unmodified control accepted. These are
targeted results; later integrated root gates qualify the combined state.

## Duplicate SUBSCRIBE filter sequencing repair

MQTT-3.8.4-5 requires a multi-filter SUBSCRIBE to behave like the corresponding
sequence of SUBSCRIBEs, with combined SUBACK. With a retained message on `b`,
no existing subscription, and `[(b, RH2), (b, RH1)]`, the first occurrence
creates the subscription without retained replay; the second replaces an
existing subscription and also suppresses replay (MQTT-3.3.1-10). The old
snapshot compared both occurrences against the original subscription map and
incorrectly sent retained content. Reversing the options also revealed that a
retained replay prepared at QoS1 was downgraded by the later final QoS0 option.

`network_retained_candidates/5` now walks successful occurrences against an
evolving subscription-existence map, retaining each prepared replay's own
QoS. Commit recomputes this snapshot from actual ordered authorization grants;
a changed snapshot is authorized again before it can produce effects. The
single-filter host path keeps its existing semantics. Failed occurrences do
not create subscriptions, and replacing a filter does not erase already
started QoS responsibility.

Three new pure cases cover both option orders, denied-first/allowed-second,
allowed-first/denied-second and inflight preservation. Red was52 tests with2
expected failures; green52/0; the full pure MQTT suite passed260/0. Two pinned
Paho2.1.0 tests use valid duplicate-filter SUBSCRIBE packets and independently
check no unexpected retained delivery, the earlier retained QoS and the final
subscription's QoS. Red2/2 became green2/0. Logs are
`g7-mqtt-duplicate-{red,green,peer-red,peer-green,suite,compile}.log` under the
shared evidence directory; scoped formatting, Python syntax and strict compile
passed. These results precede final combined gates.

Duplicate UNSUBSCRIBE was separately observed to return `[0,0]` for one
existing filter repeated twice. The specification does not mandate choosing
0x11 rather than Success for the second occurrence; this observation is not
classified as a mandatory defect and caused no production change.

## Specific packet-size diagnostic repair

The audit also traced a specific error-diagnostic defect. A header
`<<0x30, 127>>` announces a 129-byte packet; with Maximum Packet Size=128,
the real decoder immediately returns `class: :limit_exceeded` and
`reason: :packet_too_large` without allocating or waiting for the body.
Both pure endpoints previously mapped their shells' protocol-error event to
DISCONNECT0x81. They now retain the specific DISCONNECT0x95 diagnosis using
the existing event shapes:

```text
client: {:protocol_error, generation, {:decode, :limit_exceeded, :packet_too_large}, now}
broker: {:protocol_error, id, generation, {:limit_exceeded, :packet_too_large}, now}
```

§§3.1.2.11.4 and 3.2.2.3.6 specify the 0x95 diagnosis and refer to §4.13.1.
The latter says the server MUST close the connection, while sending a
DISCONNECT containing the applicable reason is a SHOULD; the client close
and diagnostic are SHOULD. The old code already closed; the repair corrects
the specific diagnostic, not a newly discovered missing mandatory close.
Only this exact error maps to 0x95. Other malformed/unknown errors retain
0x81, and peer packet bounds still control whether a DISCONNECT can be sent.

`M/test/protocol_error_reason_test.exs` derives its event from the real decoder
error and checks both endpoints plus unchanged malformed/unknown behavior.
Red: **3 tests, 2 expected assertion failures** (actual129 versus expected149).
Green: **3 tests, 0 failures**. The complete pure MQTT suite then passed
**257 tests, 0 failures**, seed20261030. Targeted formatting/checks and
`mix compile --warnings-as-errors` exited zero, without warnings. Logs are
`g5-mqtt-packet-size-{red,green,suite,compile}.log` in the evidence directory.
These are deterministic protocol-machine results; actual socket acceptance
was then added by the coordinating task in
`C/test/mqtt_client_test.exs` and
`P/test/graviton_mq/mqtt_connection_test.exs`. Both use a real established TCP
connection, send only the oversize header and assert DISCONNECT0x95 and close.
Those complete two files passed **18 tests, 0 failures** (client11, broker7),
recorded in `g5-packet-size-sockets.log`. They are raw socket boundary tests,
not external-peer interoperability. Final combined gates remain separate.

## Will payload-format admission repair

The requirement in §3.1.3.2.3 is unnumbered: a Will with Payload Format
Indicator1 MUST contain well-formed UTF-8 character data. The independent
client's `connect_profile/1` previously accepted an invalid binary Will
because the CONNECT codec correctly treats its payload as binary data. A
standalone reproduction emitted CONNECT with `payload=ff` and indicator1;
`g7-mqtt-will-pfi-diagnostic.log` records that result and valid binary/Unicode/NUL
controls. The broker already rejected the same invalid incoming Will.

The pure client now rejects that local profile with
`{:error, :payload_format_invalid}` before starting its owner or opening a
socket. Codec representation and broker refusal behavior are unchanged.
New pure regressions reject invalid leading, overlong, surrogate and incomplete
UTF-8 sequences, while preserving Unicode, empty payloads, NUL character data
and arbitrary binary data when the indicator is0 or absent. A public-start
regression covers keyword and typed Will input and verifies that no TCP
connection is opened. Red was49 tests with2 expected assertion failures;
green was49/0 in `g7-will-pfi-{red,green}.log`.

`g7-will-pfi-peer-green.log` then passed262 pure MQTT tests and23 client tests
under the pinned peer wrapper. This includes a valid UTF-8 Will sent by our
client to Mosquitto2.1.2: after actual client-owner kill, an independent
Mosquitto subscriber received the exact Unicode payload at QoS1. The wrapper
recorded exit0, stopped peers and removed the temporary instance in
`g7-will-pfi-peers/evidence.json`. These targeted results precede the next
combined root gates; the earlier1,097-test checkpoint does not qualify later
source changes.

## Evidence boundaries and remaining qualification

The linked [G0/G1](2026-10-29-g0-g1.md), [G2](2026-10-30-g2.md),
[G5](2026-10-30-g5-protocol-increment.md) and
[G6](2026-10-30-g6-local-durability.md) records provide test/peer provenance:
Mosquitto2.1.2, Paho2.1.0 and exact local crash/fault cases. Their earlier
counts are dated snapshots, not a claimed final count for this working tree.
Peer suites are gated by explicit environment variables; excluded tests are
not interoperability evidence. CI setup/source existence alone is not a
hosted CI result.

The G7 MQTT fuzz log (`g7-model-final.log`) records four passing tests,
1,861 adversarial inputs across four decoder entries (7,444 calls),
96 independent vectors, 1,364 strict prefixes, all 256 fixed headers and
64 nonminimal VBI cases. This exercises parser bounds; it does not establish
all endpoint transitions, all metadata forwarding or complete protocol
conformance. Sustained soak tests exercise exact bytes/order on bounded
concurrent paths; their memory profile is distinct from crash recovery.

Final root formatting/compile/test/architecture/xref and installed-consumer
gates remain the coordinating task's responsibility. The audit does not infer
their success from focused checks. The new multi-inflight conforming PUBREL
sequence and live/retained/recovered metadata cases now have the targeted
independent evidence recorded above. Complete error-diagnostic combinations
beyond the corrected size diagnosis remain unqualified. No additional confirmed
silently ignored mandatory semantics were found in the clause groups traced
here; unreviewed combinations and optional features outside this profile remain
unqualified.
