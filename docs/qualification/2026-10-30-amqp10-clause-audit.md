# AMQP 1.0 selected-profile clause audit — 2026-10-30

This review traces the uncommitted working tree based on rechecked HEAD
`66b4a6c161f521cb6b6379ad4f952dcdc7f3447a`, including the G5 settlement and
Close-channel repairs and opt-in G6 local queue persistence. It is a bounded
clause-group audit, supplemented by the completed
[220-sentence and302-grammar-entry inventory](2026-10-30-amqp10-requirement-inventory.md).
It is not a complete AMQP conformance certificate. Concrete A01–A08 defects
found by the audit/inventory have bounded repairs and the evidence below.
Individual schema alternatives, recommendations and unselected conditional
profiles retain exact stated execution limits; no unresolved confirmed source
defect remains in this review.

The normative references are the OASIS AMQP 1.0 Standard, Parts
[1: Types](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html),
[2: Transport](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html)
and [3: Messaging](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html).
This audit read the matching cached HTML under
`/tmp/graviton-goal-20261029-v4ozpgqs/amqp-core-{types,transport,messaging}-v1.0-os.html`.
Their SHA256 values, in that order, are:

```text
f4e02dbac8939814f6eb5f07863a8e310c8542ac01af498bccc9b7cd67b5d530
740ccfad54eabf8b4c203f852849a6cb357b324f8420c5339c70bd2de3cd9542
223f53d2d3f4f9793c09556e04e61c83c238716203d3a6afba9893654a86051d
```

## Selected roles and evidence boundaries

Both roles use raw AMQP protocol ID0/version1.0.0 over TCP or configured TLS,
format0 messages, nontransactional initially unsettled senders, receiver-first
settlement, finite resources, volatile non-dynamic termini and explicit host
responsibility. The independent client has no broker Application, runtime or
storage dependency. Broker queues are named, preconfigured move nodes, with
memory or opt-in local durable storage. Durable messages are rejected in memory
mode. Queue persistence does not implement durable termini or Link resumption.

`S` means a source trace; `T` identifies deterministic regressions; `I` means
recorded independent-peer execution; `R` identifies actual failure/recovery
evidence. A test filename identifies coverage, not proof that every case within
the cited section ran or that every normative statement is covered. Missing
external/recovery evidence is stated separately from missing behavior.

Path abbreviations below:

- `A`: `apps/graviton_mq_amqp10`; pure source under
  `lib/graviton_mq/amqp10/`, with codecs under `codec/`.
- `C`: `apps/graviton_mq_client`; AMQP shell `lib/graviton_mq/client/amqp10.ex`
  and public `lib/graviton_mq/amqp10_client.ex`.
- `B`: `apps/graviton_mq_runtime`; AMQP owners under
  `lib/graviton_mq/runtime/amqp10/`.
- `Q`: `apps/graviton_mq_core`; `lib/graviton_mq/queue/machine.ex` and message,
  identity, outcome, event and commit-reference contracts.
- `P`: `apps/graviton_mq`; network tests under `test/graviton_mq/`.
  Test paths in the other applications are relative to their `test/` directory.

## Shared wire and endpoint obligations

| Specification requirement | Implementation | Unit evidence | Independent peer | Recovery / remaining gap |
| --- | --- | --- | --- | --- |
| Types §§1.1–1.2, 1.6: exact primitive identities and constructor widths; binary/string/symbol and integer families remain distinct | **S:** `A/value.ex`, `codec/value.ex`; float/double/decimal retain exact bits; char validates Unicode scalars | **T:** `value_*_test.exs`, `codec/value_{primitive,scalar_extension,wire_completion}_test.exs` | **I:** Proton0.40.0 independent encode/decode vectors; recorded 73 scalar/array fixtures in G6 record | Bit-vector qualification is not every scalar/body combination crossing both running roles. |
| Types §§1.2.3–1.2.4, 1.4, 1.6.23–.24: compound counts, shared array constructors, map ordering and semantic keys, composite defaults | **S:** `codec/value.ex`, schema codecs; generic null map keys are separate from schema-specific symbol/binary keys; pre-allocation size/depth/item bounds | **T:** `codec/value_{compound,null_key_schema,budget,wire_completion}_test.exs`; historical Open/Begin/Error/Detach tests | **I:** independent Proton vectors and scalar/compound/described/nested-array fixtures | Resource fuzz is bounded; it is not proof over all possible encoded inputs. |
| Transport §§2.2–2.3, 2.4.1–.2: protocol-header exchange, Open first/on channel0, minimum512 before negotiation, framing and directional limits | **S:** `codec/{protocol_header,frame,stream}.ex`, `endpoint.ex`; shell owns incremental reads and socket lifecycle | **T:** `codec/{protocol_header,frame,frame_encode,stream}_test.exs`, `endpoint_test.exs`, both shell tests | **I:** Artemis2.41.0 client path and Proton0.40.0 broker path; real fragmented/coalesced input | Malformed/version/SASL tests establish explicit failure, not support for those rejected profiles. Full error/permutation matrix remains unqualified. |
| Transport §§2.4.3–.4: Close is final write; accept Close on any negotiated valid channel, including an unassigned one | **S:** `Endpoint.route_frame/4`, `peer_close`, reserved Close capacity; all outgoing Close uses channel0 | **T:** `endpoint_test.exs` two nonzero-channel regressions and Close capacity/timeout tests | **I:** normal lifecycle through Artemis and Proton; no dedicated independent nonzero-channel Close test identified | Parent repair logs `g5-close-channel-{red,green}.log`:30 tests/2 failures →30/0. Transport failure after possible Close remains unknown. |
| Transport §2.4.5: independent receive timeout, peer heartbeat obligation, empty frames on any valid channel and none after Close | **S:** pure endpoint deadlines and heartbeat effects; shells supply monotonic time and generation-fenced timers | **T:** `endpoint_test.exs`, `C/amqp10_client_test.exs`, `B/amqp10/connection_test.exs` | **I:** Artemis idle/lifecycle test; ordinary Proton traffic | No claim of hard real-time scheduling; all simultaneous heartbeat/write/Close interleavings are not independently enumerated. |
| Transport §§2.5.1–.6, 2.8.8–.10: separate local/remote channels, Begin/End ownership, per-frame windows and RFC1982 arithmetic | **S:** `connection_state.ex`, `session_state.ex`, `endpoint.ex`, `session_engine.ex`, `serial.ex` | **T:** `protocol_state_test.exs`, `endpoint_test.exs`, `session_engine_test.exs`, `serial_test.exs` | **I:** Artemis uint32 wrap and lifecycle; Proton fragmented/credit paths | Channel/window state is transient. Reconnecting to restore Session state is not implemented. Independent coverage is not a complete simultaneous-End/window-error matrix. |
| Transport §§2.6.2–.3, 2.7.3: distinct handles, exact Attach fields, first sender delivery count, no handle reuse | **S:** Session-owned Link indexes; negotiated Source/Target/outcomes, error Attach/Detach refusal | **T:** `codec/{attach,terminus}_test.exs`, `session_engine_test.exs`, `broker_admission_test.exs` | **I:** actual independent Link attach/refusal, unknown address and authorization paths | Session-local identity remains canonical. Endpoint scans other Sessions, broker owns bounded active-name reservations across connections, and independent client owners reserve exact container-pair/direction/name keys before effects; repaired AMQP-A01 below. |
| Transport §§2.6.7–.10, 2.7.4: credit is per delivery, windows per frame, remote delivery-count math, drain and echo | **S:** SessionEngine flow transitions and owner-confirmed broker exhaustion | **T:** SessionEngine credit/window/wrap/drain/stale-token tests | **I:** `P/amqp10_g5_flow_test.exs`: preloaded fragmented bodies, unused/empty credit and subsequent publication | Zero-credit pressure and bounded writer tests exist. Every crossing Flow/Transfer/drain sequence is not independently model-qualified. |
| Transport §§2.6.11–.14, 2.7.5: delivery tags, lazy fragments, continuation identity, aborted payload ignored, bounded max-message-size | **S:** separate incoming/outgoing delivery ledgers, partial deadlines and bounded buffers; raw bytes remain authoritative | **T:** SessionEngine fragment/interleave/abort/timeout/size/ID tests; performative codecs | **I:** large Artemis/Proton bodies; real512-byte frame bound; early peer settlement cases below | Resume is explicitly unavailable. Early settlement retains a bounded abort obligation before dropping unfinished outgoing content. |
| Transport §§2.6.12, 2.7.6, 2.8.2–.3: settlement independent of state; initially unsettled does not prohibit later sender settlement | **S:** separate incoming peer state/settlement versus local host token/outcome; outgoing nil/nonterminal settlement returns unknown without ending Session | **T:** six late-sender tests and four receiver-without-outcome tests in `session_engine_test.exs`; wrapped exact-ID ranges, partial delivery, queued/written local settlement and duplicate completion | **I:** four `P/amqp10_g5_late_settlement_test.exs` Proton cases cover both roles/directions, host responsibility and unrelated same-Session Links | Terminal state remains immutable. No Source default-outcome is negotiated, so no Accepted/Released is invented. Late write receipts remain bounded and cannot complete twice. |
| Messaging §§3.2, 3.2.1–.10: nine sections/order/types, mandatory annotation understanding, immutable bare message and content-encoding rules | **S:** `codec/message.ex`, `annotation_policy.ex`, `forwarding.ex`; receiver rejects unknown mandatory annotations before host delivery | **T:** `codec/message_test.exs`, `annotation_policy_test.exs`, `forwarding_test.exs`, both receiver-policy SessionEngine cases | **I:** Proton mandatory/optional annotations; Modified replacement/merge and body digest | A03 first-acquirer correction and12 independent exact-bare-message fixtures now qualify competing-Link metadata and the recorded body/property corpus. This is not every possible field combination or recovered-consumer crossing state. |

## Independent client role

| Specification requirement / role boundary | Implementation | Unit evidence | Independent peer | Recovery / remaining gap |
| --- | --- | --- | --- | --- |
| Transport §§2.2, 2.4–2.5: outgoing connection and host-supervised Session lifecycle | **S:** `C/client/amqp10.ex` interprets Endpoint effects; public client starts no broker | **T:** `amqp10_client_test.exs` connection refusal, split input, bounded pending Begin and shutdown | **I:** `amqp10_artemis_test.exs`; installed independent consumer record in G3/G5 | Observer death closes socket. Archive independence is separate evidence from broker embedding; client reconnect/VM recovery remains unavailable. |
| Transport §§2.6.1–.3: host Link initiation must obey container-pair name uniqueness | **S:** Endpoint checks other Sessions; C reserves validated names through existing OTP `:global` owner locks on the local node, before executing wire effects | **T:** cross-Session Endpoint regression; incoming cross-owner refusal, pending End, Session incarnation and socket-owner shell regressions | **I:** Proton server observes one outgoing Attach for duplicate independent client owners, accepts opposite direction/different remote container, then real traffic after original owner death and Detach/reuse | **REPAIRED A01:** duplicates reject before wire; incoming duplicates null Attach then resource-locked Detach. Locks survive pending Detach/End/Close, retire from canonical state and disappear on owner DOWN. Different BEAM nodes with the same configured container identity rely on remote-peer arbitration; no distributed container implementation is claimed. |
| Transport §§2.6.7, 2.7.5–.6: peer credit precedes sending; local writes do not prove terminal peer outcome | **S:** SessionEngine outgoing admission and C operation dispatch; terminal+settled gives its actual outcome, otherwise uncertainty is preserved | **T:** send-window tests, known-outcome/unknown loss and non-reading-peer shell regression | **I:** Artemis publish→independent Proton receiver; lost-outcome proxy; receiver-without-outcome Proton server then second Link | Possibly emitted timeouts report unknown; never-emitted commands fail. No automatic replay or business exactly-once claim. |
| Transport §§2.6.12, 2.7.6: received host ownership survives spontaneous peer sender settlement | **S:** delivered host reference remains until explicit host settlement, then finishes locally without stale Disposition | **T:** late-sender, partial, wrapped range and queued/written settlement regressions | **I:** independent Proton server initially sends unsettled, later settles; client consumes/accepts and uses a second Link | Stale host token rejects after one settlement. Process/VM loss does not restore the host delivery reference. |
| Messaging §§3.2–3.5: preserve encoded format0 content; host chooses valid negotiated receiver outcome | **S:** public send_message/settle API, shared schemas and Source outcomes checks | **T:** byte-preservation, source-outcome rejection, annotation-policy tests | **I:** Artemis large body/release/redelivery; Proton late sender tests | No application body deserialization or hidden business ACK. Independent broker corpus now covers12 noncanonical full-bare-message fixtures; root `g7-corpus-client-check.log`9/0 separately qualifies both-directions12-message Artemis corpus; root `g7-corpus-check.log`10/0 covers exact durable replay12 fixtures. These precede final root gates. |
| Conditional transport security | **S:** client-owned transport validates trusted TLS/hostname and rejects insecure owner-option overrides; SASL config unavailable | **T:** shell config/transport tests | **I:** `amqp10_artemis_test.exs` trusted TLS send/receive and wrong-CA/hostname rejection ran in [G6 qualification](2026-10-30-g6-local-durability.md); final root gates enable the same TLS peer path | Security Part5 mechanisms are outside this three-part audit. TLS support does not imply SASL or authenticated AMQP user identity. |

## Broker role

| Specification requirement / node policy | Implementation | Unit evidence | Independent peer | Recovery / remaining gap |
| --- | --- | --- | --- | --- |
| Transport §§2.4–2.6: standalone/embedded network owners, finite admission and explicit Link refusal | **S:** `B/{supervisor,listener,connection,writer,broker}.ex`; host composition uses same public lifecycle; authorization precedes accepted Attach/credit | **T:** broker admission, connection/config and composition boundary tests | **I:** Proton unknown queue, auth denial, TCP/TLS and actual two-instance isolation | Manager death closes old sockets, surviving queues release leases. A01 now refuses active duplicates with null Attach/resource-locked Detach. Bounded name reservations outlive queue routes until actual endpoint retirement; no stealing/resume support is claimed. |
| Messaging §§3.3, 3.5.2: move-node AVAILABLE→ACQUIRED→retire/release; claims cannot be concurrently acquired | **S:** Q Queue.Machine; B Queue owns claims/leases, Manager serializes final outcomes before DOWN | **T:** `Q/queue_machine_test.exs`, queue model, `B/amqp10/queue_test.exs`, composition boundaries | **I:** Proton credit0, Released redelivery, unsettled disconnect and final Accepted while Manager suspended | Queue leases are stable broker responsibility, not protocol handles. Memory owner loss has no persistence guarantee. |
| Messaging §§3.4.2–.5: Accepted retires, Released does not increment count, Rejected follows node discard policy, Modified merges/marks failed/excludes endpoint | **S:** `outcome_mapper.ex`, Q machine and B complete/send; `forwarding.ex` creates outbound view without replacing record | **T:** outcome mapper, forwarding, machine, queue and durable queue tests | **I:** Proton Modified annotations/count, Rejected discard, release/redelivery; G6 recovery cases | Unknown receiver outcome returns claim to available and detaches only affected Link. Dead-letter policy is not supported. All endpoint-exclusion lifetime combinations are not independently qualified. |
| Transport §§2.6.12, 2.7.6: late sender settlement cannot erase an incoming queue-admission obligation | **S:** incoming ledger retains host token/budget; Manager admits bytes then settles locally if peer already forgot | **T:** pure ownership and broker admission regressions | **I:** broker Manager suspended while Proton Transfer then late settlement arrives; both bodies subsequently consumed, second Link shares original Session | A peer forget announcement is not successful host processing. This is memory responsibility unless selected queue sync succeeds. |
| Messaging §3.2.1: memory node MUST NOT accept durable=true; choose Rejected or required precondition-failed Detach according to allowed outcomes | **S:** B profile_message rejects durable; normal rejection carries precondition-failed | **T:** memory rejection and negotiated-outcomes tests; B carries the original durability rejection error through failed settlement | **I:** accepted-only Source receives precondition-failed Detach, then unrelated Link sends/receives; queued-Detach retirement case independently repeats this condition | **REPAIRED A02:** required conditional error is preserved. TTL/absolute-expiry handling remains a separate explicit profile refusal. |
| Messaging §§3.2.1, 3.3: selected durable queue Accepted follows actual durable responsibility, not append ordering | **S:** B Queue/Persistence/Store; storage WAL append/sync/durable-through; effects wait for successful boundary | **T:** `B/amqp10/durable_queue_test.exs`; storage fault/prefix/streaming tests | **I/R:** `P/amqp10_persistence_proton_test.exs`: independent durable publish/retire, owner/whole-BEAM loss, Modified/rejection restart, actual sync barrier and failed sync | Local Linux/inode owner, bounded log; no compaction, distributed durability or power-loss hardware qualification. Audit did not rerun these persistence tests. Root integrated G6 evidence is separate. |
| Messaging §§3.2.1, 3.2.4: accepted TTL/absolute expiry cannot be ignored | **S:** B rejects either field before queue enqueue with explicit precondition failure | **T:** profile rejection tests | Dedicated independent expiry-rejection/outcome-exclusion combinations not identified | **EXPLICITLY UNAVAILABLE**, not an implemented expiry service. Selected profile does not accept these semantics; an expiry implementation would require new acceptance. |
| Messaging §§3.2–3.2.10: forward immutable bare bytes; remove hop-local delivery annotations; apply permitted mutable changes | **S:** Forwarding removes delivery annotations and merges count/Modified deltas; stored source bytes remain unchanged | **T:** noncanonical bare-tail, repeated data/footer, mutation order and pre-allocation budget regressions | **I:** Proton body digest and Modified annotation cases | **I:** `P/amqp10_message_proton_test.exs`12 independent exact bare fixtures: data/sequence/value ×4 message/correlation-ID types, accepted properties, ordered application/infrastructure/footer values, full-ulong noncanonical descriptors, valid gzip data split across two sections. Hop delivery annotations alone are removed. A03 explicitly clears first-acquirer true on onward view; original record and bare tail stay unchanged. Root `g7-corpus-check.log`10/0 now covers exact durable replay of the12 fixtures; `g7-corpus-client-check.log`9/0 covers the bidirectional Artemis corpus. Other unrecorded field combinations remain unqualified. |

## Reproduced mandatory/conditional defects and subsequent bounded repairs

### AMQP-A01 — container-pair Link naming and active attachment

Transport §2.6.1 says: “Link names MUST uniquely identify the link amongst all
links of the same direction between the two participating containers.” The
global key is the ordered `(source-container, target-container, name)` tuple.
For an active-link stealing path it says “the link can be 'stolen', i.e., the
second attach succeeds and the first attach MUST then be closed with a link
error of stolen.” This reaches across Sessions and connections; it is not
merely a handle collision rule. The unconditional requirement is uniqueness.
The stolen-close requirement applies if the second attachment succeeds; this
text does not require a profile to grant every attempted second attachment.

Read-only reproduction loaded current `endpoint.ex` and `session_engine.ex`
into an ephemeral Elixir VM against the isolated existing beams, without
running Mix or writing a build. Start local container `local`, receive Open
from `same-peer`, open two Sessions with independent remote channels21/22,
then attach local sender name `same-direction-duplicate` in both. Each peer
Attach has supported volatile Source/Target, modes0/0, and handle9 in its own
Session. Actual result:

```text
endpoint.phase = :active
Session1: {same-direction-duplicate, :sender} => :attached; attach result :ok
Session2: {same-direction-duplicate, :sender} => :attached; attach result :ok
no stolen Detach; both links remain active
```

The peer's two successful Attach replies are deliberately used to expose the
outgoing container's own missing uniqueness check; this replay is not claimed
as compliant external-peer behavior. Source confirms no broader guard:
SessionEngine checks `links_by_identity` only for its Session; Endpoint routes
to that Session; broker route keys are `{connection, session_id, identity}`.
For incoming broker ownership, cross-Session/connection active-name admission
likewise has no container-pair ownership registry or regression. Same-Session duplicate
identity/handle errors, refusal of nonempty unsettled maps and explicit
no-resume policy do not enforce this mandatory initial-name boundary.

Transport §2.6.3 supplies a bounded refusal alternative: the application can
choose not to create/associate a terminus, send Attach with its local terminus
null, then MUST immediately detach that new endpoint. An explicit
`amqp:resource-locked` Detach before granting the second attachment can preserve
the active first Link without implementing stealing/resumption. The generic
error in §2.8.15 describes an entity unavailable because another client is
working with it. Outgoing host admission can reject before emitting a second
Attach. A repair must enforce the container-pair/direction/name boundary across
Sessions and connections, preserve opposite directions, and release ownership
only when the actual active Link ends. Granting both and omitting stolen-close
cannot be justified as a no-resume restriction.

**Repair status:** Endpoint passes occupied names as explicit pure transition
facts, including names retained by ending Session controls. SessionEngine
rejects an outgoing duplicate before Attach emission and refuses incoming
attachments with null termini then `amqp:resource-locked` Detach. Broker
reserves names before authorization and retains a bounded reservation separately
from queue routes until actual detached/refused/Session-ended/disconnected
notifications. Session errors can retire queue responsibility while preserving
the name until End/disconnect. The first Link remains usable.

Independent client owners use existing OTP `:global` local-node owner locks,
keyed by exact local/remote container IDs and direction/name, only after pure
validation and before effects escape. Reservations retain Session incarnation,
survive pending Detach/End/Close, release from canonical state and owner DOWN,
and use the requesting PID for exact cleanup. No new registry process or
Application is introduced; protocol modules remain pure. Same configured
container identity in separate BEAM nodes still requires the remote peer to
arbitrate duplicates; cross-node container composition is not claimed.

`endpoint_test.exs` and `broker_admission_test.exs` first reproduced51 tests/2
expected failures, then51/0. `C/amqp10_client_test.exs` now passes14/0,
including incoming cross-owner null-terminus refusal and pending End/Session
incarnation cleanup. The independent `P/amqp10_g5_clause_test.exs` passes4/0,
covering cross-Session/cross-connection broker refusal, opposite directions,
different remote containers, broker instance isolation, first-Link traffic,
Detach reuse, actual peer process loss and independent client-owner death.
An additional deterministic case suspends the original Connection while a
required durable Detach is still queued: queue routes are already retired,
but another Connection still cannot acquire the name. After actual endpoint
retirement, reuse succeeds with real traffic. Stealing/resumption remain
explicitly unavailable; Session-owned Links and both handle indexes remain.

### AMQP-A02 — durable rejection when Rejected is excluded

Messaging §3.2.1: a target unable to guarantee durable storage “MUST NOT accept”
durable messages. If the source permits Rejected it SHOULD reject with
precondition-failed; otherwise “the link MUST be detached by the receiver with
the same error.” Normal memory-profile rejection does not cover that latter
conditional branch.

An isolated memory broker was started from the already compiled scoped build.
Independent Proton0.40.0 created an initially unsettled/receiver-first sender,
Source outcomes `[amqp:accepted:list]`, then sent Message(durable=True). Actual
observation, checked against expected `amqp:precondition-failed`:

```text
actual_detach_condition=amqp:resource-limit-exceeded
description='link operation failed'
independent conditional durability rejection exit: 1
```

B `admit_message` creates Rejected(precondition-failed); SessionEngine correctly
refuses that disallowed outcome; the generic B failed-settlement completion
then calls `detach`, which substitutes resource-limit-exceeded. The node does
not falsely accept the message, but the required conditional error is lost.
No source/build changes were made for the original diagnosis, and its isolated
broker and Proton process were stopped. **Repair status:** B now distinguishes
memory durability rejection from other profile rejections, carries the exact
precondition-failed error through settlement, and detaches with that same error
if Rejected was not negotiated. Independent Proton accepted-only Source now
observes the required condition; an unrelated Link still sends/receives and
queues remain empty. The suspended-Connection case also checks this condition.
TTL/absolute expiry were not reclassified or implemented by this repair.

### AMQP-A03 — first-acquirer forwarding view

Parent-owned independent tests reproduced a broker preserving incoming
`first-acquirer=true` after another Link had acquired/released the message:
unit12/1 and Proton1/1 red. The minimal `Forwarding.header` repair conservatively
changes only explicit true to false on the onward broker view; default/false
and delivery-count behavior retain their prior representation. It never
rewrites the stored original message or immutable bare-message tail.
`g5-first-acquirer-green.log` records unit12/0 plus Proton1/0. Final independent
`g7-message-corpus-final.log` records unit12/0 plus peer2/0 and the12-fixture
exact-bare corpus described above, including a valid gzip stream split across
two data sections and4096 deterministic payload bytes. This is bounded
evidence, not exhaustive first-acquirer/restart or all-field conformance.

### AMQP-A04 — handle-max violation must close the Connection

Transport §2.7.2 states: “A peer that receives a handle outside the supported
range MUST close the connection with the framing-error error-code.” The
standalone existing-beam diagnostic `amqp-inventory-diagnostic.log` showed
Attach(handle2) after advertised handle-max1 producing End with
`amqp:session:handle-in-use` while the Connection stayed active. The bounded
repair checks all four handle-bearing inputs (Attach, Flow, Transfer and Detach),
terminates the local SessionEngine to suppress queued data, and emits an
explicit pure Connection-error effect. Endpoint interprets it as
Close(`amqp:connection:framing-error`) on channel0.
It does not replace ordinary handle-in-use Session errors. Endpoint regression
and independent Proton-value raw peer both verify the exact boundary.

### AMQP-A05 — input referencing an errored endpoint must be errant-link

Transport §2.6.5 states: “If any input (other than a detach) related to the
endpoint either via the input handle or delivery-ids be received, the session
MUST be terminated with an errant-link session-error.” It separately requires
pipelined reattachment to terminate the Session with that same error.
`amqp-inventory-diagnostic.log` reproduced wrong unattached-handle End for
Flow/Transfer, wrong handle-in-use End for same-handle Attach, and a silently
applied role-correct Disposition on a live delivery after local error Detach.
New-handle/same-name Attach already produced the correct errant-link error.

Link now retains a pure error marker and incoming/outgoing uint delivery-ID
sets before local error Detach or internal refusal releases ledger entries.
The pre-dispatch guard checks the remote handle, exact Link identity on Attach,
and role-correct bounded Disposition ranges. It never scans a peer-specified
integer range. Each set is bounded by the corresponding configured Session
delivery limit and the total by max-links times those limits; only ID values
are retained, with no copied payload. Existing live-ledger responsibility is
not silently retired. Peer Detach removes the marker with the Link; bounded
Link timeout, End and transport teardown clear it with the Session. A current
live delivery on another Link takes precedence after full uint32 ID reuse.
Normal Detach remains outside this error marker. Resume remains unavailable.

`g5-amqp-error-boundary-red.log` records77 tests/3 expected failures;
`g5-amqp-error-boundary-green.log` records77/0. Added repeated Detach/ack/reuse
and wrap-range coverage then passed46 SessionEngine tests, including both
local roles, role-correct/incorrect Disposition, cleared marker state, one-Link
and one-delivery bounds, unrelated healthy Link traffic before violation,
internally refused payload cleanup and transport teardown. The independent
Proton-value peer verifies Connection Close above max-handle, then five
separate error-Detach sessions with Flow, Transfer, same/new-handle Attach and
Disposition; each produces exact errant-link End, and all sockets/queues clean
up. The clause peer suite is now5/0.

### AMQP-A06 — null/empty Source outcomes and sender authority

Types §1.4 requires null and a correctly typed zero-length multiple array to
mean the same absence. Messaging §3.5.3 permits empty outcomes, with default
outcome applying; when both outcomes and default-outcome are absent, Accepted
must be supported. The sender owns the actual Source; an absent receiver
preference and an absent authoritative sender declaration are different roles.
Previously nil expanded to four outcomes and empty to Accepted, including when
our receiver interpreted an authoritative remote Source.

SessionEngine now explicitly advertises its four actual supported outcomes in
its initial Source. When our sender receives an absent receiver preference it
selects those four; when our receiver receives absent/typed-empty sender
outcomes it conservatively retains Accepted-only. Explicit scalar/nonempty
restrictions remain. Both peer_new_link and attach_reply store the selected
Source consistently with the emitted Attach. No default-outcome is invented.

`g5-amqp-source-outcomes-red.log` records47 tests/1 expected failure.
The paired regression covers nil/typed-empty × both local roles × host/peer
Attach initiators, asserts stored and emitted Source, then exercises actual
peer Released settlement or host Released refusal followed by Accepted.
Generic pure fixtures which intend four-outcome remote support now explicitly
advertise that support; existing Accepted-only refusal tests remain intact.
No independent-peer fixture was changed. Focused Endpoint34 + SessionEngine47
passed81/0; the broad current-source checkpoint below preserves all20 real
Proton composition tests.

### AMQP-A07 — discard Session input while End is pending

Transport §§2.5.4–.5 require incoming Session input to be discarded until End.
The followup diagnostic showed Begin and malformed Session bodies taking
Connection-error paths before the prior ending-Session guard. Endpoint now
validates the frame header/channel/size first, reads only a bounded descriptor
for an ending channel, validates/routes recognized Connection Open/Close, and
accepts a valid End acknowledgment. Other Session bodies, including Begin,
unsupported/malformed bodies and out-of-range handle inputs, are discarded.
Malformed Connection controls and frame headers keep Connection validation.

`g5-amqp-ending-input-red.log` records33 Endpoint tests/2 expected failures;
`g5-amqp-ending-input-green.log` records Endpoint33 + SessionEngine46 =79/0.
The independent clause peer passes5/0, with another Session Flow remaining
healthy and Connection Close still routed. These cases are included in the
final broad452/0 pure protocol and20/0 Proton checkpoint.

The exact Connection DISCARDING regression additionally issues error Close,
then valid Begin/Flow/Transfer and unsupported Session input, asserting no
state/wire/host effect. Valid Close closes transport once; a25ms close deadline
provides a separate finite exit. Root's actual-socket unsupported-version/ID
and malformed-magic tests verify supported-header response and closure,
passing24/0 focused Connection tests and57/0 broad runtime tests.

## Repairs qualified before this audit's source freeze

- Transport §2.4.3 nonzero valid-channel Close: parent-owned Endpoint repair;
  reviewed logs `g5-close-channel-red.log`30/2 → `g5-close-channel-green.log`30/0.
- Transport §§2.6.12, 2.7.6 late sender settlement: initial35 SessionEngine
  tests/3 expected red failures →35/0, then expanded partial/range coverage;
  independent broker and public client Proton cases passed.
- Symmetric receiver settlement without terminal outcome:42 SessionEngine
  tests/4 expected red failures →42/0. Nil, prior Received plus nil, and direct
  Received+settled all preserve Session and other deliveries. An incomplete
  outgoing message waits for its bounded zero-payload aborted continuation,
  then reports `{:unknown, :receiver_settled_without_outcome}` exactly once.
  No unnegotiated default outcome is fabricated. Independent Proton verifies
  client unknown followed by healthy second Link, and broker requeue followed
  by independent redelivery and healthy same-Session traffic.

Final scoped pre-freeze commands used `.tool-versions` Elixir1.18.4/OTP28.5.0.1,
runner `/tmp/graviton-goal-20261029-v4ozpgqs/run`, isolated
`MIX_BUILD_PATH=.../g5-late-settlement-build` and Proton0.40.0. Results were
**441 AMQP tests, 12 client shell tests, 55 AMQP runtime tests and 15 Proton
composition tests, all zero failures**. Strict compile and owned-file format
checks passed. These numbers include the parent Close repair. The subsequent
read-only audit intentionally ran no shared build. A900-second soak compiled
before the settlement repairs is not evidence for their integrated state.

## A01/A02 scoped integration checkpoint

`g5-amqp-clause-qualified.log` uses the declared Elixir1.18.4/OTP28.5.0.1
runner, separate `MIX_BUILD_PATH=.../g5-amqp-clause-build` and pinned Proton0.40.0.
It records **444 pure AMQP tests,14 client shell tests,55 AMQP runtime tests
and19 independent Proton composition tests, all zero failures**. Strict
compilation with warnings-as-errors and owned-file format checks passed.
The19-peer total includes the four new clause cases, not the separate parent
full-message corpus. Root owns complete umbrella gates, archives/consumers and
source-digest-fenced soak qualification; earlier soaks do not qualify later
source edits automatically.

Final review added the Session-error ending variant: a malformed unknown-handle
Detach clears SessionEngine Link indexes, but its names must remain reserved
until End acknowledgment. `g5-amqp-ending-name-red.log` records31 Endpoint
tests/1 expected failure. `Endpoint.engine_step` now captures previous names
before interpreting the Session error; the existing ending-control reservation
releases them only when Session removal occurs. Final
`g5-amqp-ending-name-green.log` records444 pure AMQP tests,14 client shell
tests and4 independent clause peer tests, all zero failures. Strict compile
and owned-file format checks passed again. The preceding300-second
`g7-audit-soak` is a prior-source checkpoint; it does not qualify this final
pure-state retention edit.

Final A04/A05 production checkpoint `g5-amqp-error-boundary-qualified.log`
records448 pure AMQP tests,14 client shell tests,55 runtime tests and20
independent Proton composition tests, all zero failures. The subsequent
additional bounded-cycle SessionEngine regression passed46/0; it changed no
production source. Strict compile and owned-file formatting passed. Complete
umbrella gates, package consumers and any later fixed-source soak remain the
parent's responsibility. These two defects were identified by the separate
normative requirement inventory and were repaired without changing the
selected profile or adding protocol resumption.

## A08 — unknown extension terminus refusal

The Attach codec retains unknown described Source/Target values as protocol
data. This does not grant runtime support for their semantics. Source outcome
normalization initially evaluated an extension Source before the selected
profile check, raising instead of returning the required bounded refusal.
Both new-Link admission and queued refusal now validate before normalization;
refusal constructs its null Attach from nil termini before invoking the normal
Attach builder. No catchall converts an unsupported terminus into an accepted one.

The wire encode/decode regression covers Source/Target, both local roles and
automatic/deferred admission. It asserts null Attach then not-implemented
Detach without an attached/admission event. With writes initially blocked,
peer Detach receives the pending null Attach followed by the closing Detach
acknowledgment, and both handle indexes are cleared. The correction retains
the role-aware null/empty outcome semantics of A06.

`g7-extension-recovery/{focused,broad,compile,format,format-check}.log` records
48 focused SessionEngine tests and **453 pure AMQP,14 client,57 runtime and20
independent Proton tests, all zero failures**, plus strict compilation and
format checks. These are scoped results; the later complete root gates qualify
the final combined tree.

## Source/test freeze and inventory checkpoint

The declared Elixir1.18.4/OTP28.5.0.1 runner with isolated
`MIX_BUILD_PATH=.../g5-amqp-clause-build` and pinned Proton0.40.0 records
`g5-amqp-source-outcomes-qualified.log`: **452 pure AMQP,14 client shell,57
AMQP runtime and20 independent Proton composition tests, all zero failures**.
Strict compile with warnings-as-errors and owned-file format checks passed.
All worker commands and owned peer processes finished. Earlier logs above
remain historical evidence, not the final-source qualification substitute.

The companion Markdown/JSON inventory retains220 exact modal-bearing
sentences and302 grammar entries, source hashes and source/test/peer/recovery
locators. Final row dispositions are115 scoped unit/peer evidence,50 explicit
conditional refusals,41 recommendations/conditional choices,5 one-FIFO-class
conditions unselected,1 inactive crossed-nonclosing-Detach condition,5 explicit source/profile invariants with exact execution
limits, and3 descriptive/nonadditional requirements. The Link Flow.available
estimate is not maintained or exposed; Session window clamping is not cited
as evidence for that distinct variable. en-US and no-extension forms remain
explicit source/profile invariants rather than invented independent tests.

The original `amqp10_message_artemis_test.exs` and its independent Proton
helper verify12 noncanonical fixtures in each client direction with exact
immutable bare bytes and explicit semantic/type assertions. The broker's
`amqp10_persistence_proton_test.exs` adds12 durable fixtures, exact encoded
forwarding after WAL replay and a second restart proving synchronized
retirement. These remain separate from publication, hosted CI and sustained
traffic; the final complete gates are recorded in the G7 local qualification.

## Explicit exclusions versus unfinished acceptance

SASL, transactions, nonzero message formats, durable/dynamic termini,
nonempty/incomplete unsettled resume maps, receiver-second, initially settled
senders, filters and required extension capabilities are explicitly refused by
this profile. Client has no automatic reconnect/replay or Link/Session recovery.
Broker rejects copy distribution, TTL and absolute expiry; memory also rejects
durable messages. Priority-specific scheduling is optional and not claimed.
Clustering/Raft, management UI, cross-protocol bridge and deferred QUIC remain
outside the Goal. These exclusions must not be relabeled as implemented
features, and do not exempt mandatory semantics of accepted Links/messages.

The completed inventory records the selected mandatory/conditional behavior
and the exact limits of its evidence. A01–A08 repairs do not imply exhaustive
byte/transition or crossing-state coverage, nor every possible error-condition
permutation. The exact durable replay and Artemis corpus establish only their
recorded fixtures. No unimplemented mandatory behavior is declared optional
merely to close G5.
Root owns final integrated gates, installed-consumer/CI qualification and any
later repairs; historical codec/memory/G3–G4 evidence remains intact.
