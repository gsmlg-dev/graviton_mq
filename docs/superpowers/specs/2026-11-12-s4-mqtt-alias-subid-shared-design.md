# S4 — MQTT 5.0 Topic Alias, Subscription Identifiers and Shared Subscriptions

Date: 2026-11-12. Status: **approved by the owner 2026-11-12** (decisions
below). Nothing here is implemented. Programme row S4 of
[the P0–P3 design](2026-11-12-http-fetch-transport-and-p0-p3-design.md)
(depends on S1; exit evidence "Mosquitto/Paho both directions per feature").
S4 touches MQTT files only and must not overlap S2/S3 (AMQP). S6/S7 run after
S4. Commit, release and version bump are not authorized by this design.

Normative reference: OASIS MQTT 5.0 OS (7 March 2019), cached text
`/tmp/graviton-goal-20261029-v4ozpgqs/mqtt-v5.0-os.txt` (SHA256 in the
[requirement inventory](../../qualification/2026-10-30-mqtt5-requirement-inventory.md)).
Requirement IDs below are the inventory's literal `MQTT-*` IDs and its `U-*`
unnumbered entries.

## Owner decisions (2026-11-12)

- Q1 default enablement: once qualified, shared subscriptions, Subscription
  Identifiers and inbound Topic Alias are **on by default** on the broker,
  each with a config switch; broker outbound aliases stay off by default.
- Q9 durable format: **accepted** — broker snapshot v2 with read-time v1
  upgrade, same WAL record type, no downgrade; documented as an upgrade note.
- All other open questions (§10, Q2–Q8 and Q10–Q15): **the recommended
  default is accepted** as written in §10.

## 1. Current state (evidence)

| Area | Today | Source |
| --- | --- | --- |
| Codec | Properties `0x0B` Subscription Identifier (PUBLISH repeatable, SUBSCRIBE once, VBI 1..268,435,455), `0x22` Topic Alias Maximum, `0x23` Topic Alias, `0x29`/`0x2A` availability flags already encode/decode. `$share/{name}/{filter}` grammar validated; empty topic allowed only with alias > 0; No Local on shared rejected at decode | `codec/properties.ex:21,36,37,59,60,206,239`; `codec/operational.ex:339,362,410-416,439-474` |
| Topic | `Topic.valid_filter?/1` returns false for every `$share/` filter; `matches?/2` therefore never matches shared filters | `mqtt/topic.ex` |
| Pure broker | CONNACK fixes `shared_subscription_available: 0`, `subscription_identifier_available: 0`, `topic_alias_maximum: 0`. Inbound alias → DISCONNECT 0x94; inbound PUBLISH Sub-ID → 0x82; SUBSCRIBE with Sub-ID → 0xA1; any `$share/` filter → 0x9E. `recipient_packet/3` strips alias/Sub-ID. Routing makes **one copy per session** at the max QoS of all matching subscriptions | `mqtt/broker.ex:381-385,458-462,924-930,1021-1080,1263-1287` |
| Broker checkpoint | Snapshot `version: 1`; exact-key schemas: subscription `[:id, :options]`, delivery `@delivery` (12 keys); subscription filters validated by `Topic.valid_filter?` | `mqtt/broker/checkpoint.ex` |
| Runtime persistence | WAL record type `"mqtt-broker-snapshot/v1"`, full snapshot per commit, latest wins; `CheckpointCodec` decodes only existing atoms (`Code.ensure_loaded?(Checkpoint)`) | `runtime/mqtt/persistence.ex` |
| Host API | `GravitonMQ.subscribe/3` options `qos/no_local/retain_as_published/retain_handling`; `Topic.valid_filter?` gate rejects shared | `runtime/mqtt/broker.ex:620`; `mqtt/broker.ex:2020` |
| Pure client | Forces CONNECT `topic_alias_maximum: 0`; inbound alias → 0x94; caller alias → `:topic_aliases_unsupported`; Sub-ID / `$share/` gated on peer flags; subscriptions map `filter => granted_qos` | `mqtt/client.ex:80,111-118,338,743-774,452-476` |
| Client owner | Persistent client rejects `$share/` with `:shared_subscription_persistence_unsupported` (0.2.1) | `client/mqtt.ex:97-99`; `docs/releases/0.2.1.md` |
| Why the 0.2.1 rejection exists | Client checkpoint validates `subscriptions` with `Topic.valid_filter?`, which is false for `$share/`; a successful shared SUBACK made the next capture invalid and closed the client | `client/mqtt/checkpoint.ex` `retained?/3` |
| Interop | Mosquitto 2.1.2 config: `listener`, `allow_anonymous`, `persistence false`, `max_qos` (+ per-test `configuration:`); Paho 2.1.0 Python peers; only shared-related peer test is `SHARED_OFF` (Paho gets DISCONNECT 0x9E) | `scripts/interop/run.py:159`; `apps/graviton_mq_client/test/support/mosquitto.exs`; `apps/graviton_mq/test/graviton_mq/mqtt_paho_test.exs:37` |

Peer capabilities (checked 2026-11-12, must be re-confirmed against the pinned
build during T0):

- Mosquitto (current `mosquitto.conf(5)`): shared subscriptions and
  Subscription Identifiers always on (no option); `max_topic_alias` (client →
  broker, default 10) and `max_topic_alias_broker` (broker → client, default
  10). Presence of `max_topic_alias_broker` in **2.1.2** is unverified (the
  cached source tree in `/tmp` is empty). `mosquitto_pub/sub -V 5 -D <cmd>
  <prop> <value>` set `topic-alias`, `subscription-identifier`,
  `topic-alias-maximum`.
- Paho Python 2.1.0 (`src/paho/mqtt/client.py`, tag v2.1.0): no topic alias
  table in either direction. Outbound aliases are manual (`TopicAlias`
  property, then empty topic). Inbound aliases are delivered raw
  (`message.topic == ""` plus `TopicAlias`), so a Paho test script must
  resolve them itself. Sub-IDs supported as `SubscriptionIdentifier`
  (received as a list).

## 2. Scope and non-goals

### 2.1 In scope

| Feature | Broker | Client |
| --- | --- | --- |
| Topic Alias inbound | Advertise configured CONNACK Topic Alias Maximum; per-connection alias → topic table; resolve before every other PUBLISH check | Advertise configured CONNECT Topic Alias Maximum; per-connection table; resolve before delivery/duplicate checks |
| Topic Alias outbound | Optional; bounded by `min(client CONNECT TAM, broker cap)`; machine-owned assignment | Optional; bounded by `min(CONNACK TAM, client cap)`; machine-owned assignment; caller-supplied `topic_alias` stays rejected |
| Subscription Identifiers | Advertise availability; store per subscription; attach to PUBLISH (all matching IDs on the single copy; only the receiving member's IDs for shared); persist and recover | Allow `subscription_identifier` on SUBSCRIBE when peer allows (existing); deliver received IDs to the host (existing property path); add a dedicated option |
| Shared subscriptions | `$share/{ShareName}/{filter}`; groups scoped to the broker; one member per message; QoS/ack/redelivery rules of §4.8.2; no retained on subscribe; persist memberships and responsibility | Memory and **persistent** profiles may subscribe; checkpoint accepts shared filters |

### 2.2 Non-goals

- Host-API shared subscriptions or Subscription Identifiers (see Q11). Host
  subscriptions keep their current shape. Host PUBLISH keeps rejecting
  `topic_alias`/`subscription_identifier`.
- Topic aliases on Will messages (CONNECT carries the full Will topic; not
  applicable).
- Persisting alias tables (forbidden by MQTT-3.3.2-7) or the share-distribution
  cursor (fairness state only, not responsibility).
- Clustering-wide groups, cross-protocol groups, configurable/pluggable
  distribution strategies beyond one built-in policy (Q4), ordered delivery on
  shared subscriptions (§4.6 only requires ordering on non-shared).
- MQTT 3.1.1 (S7) and WebSocket (S6).

## 3. Requirements traced

Applicability in the inventory is written `client / broker`.

### 3.1 Topic Alias (§3.1.2.11.5, §3.2.2.3.8, §3.3.2.3.4, §3.3.4)

| ID | Obligation | Today | S4 target |
| --- | --- | --- | --- |
| U-012 / U-034 | TAM at most once; absent = 0 | codec | unchanged |
| MQTT-3.1.2-26/27 | Server MUST NOT send alias > client TAM, none if 0/absent | NOT_EMITTED | broker outbound bounded by client TAM; T + I |
| MQTT-3.2.2-17/18 | Client MUST NOT send alias > server TAM, none if 0/absent | NOT_EMITTED | client outbound bounded by CONNACK TAM; T + I |
| MQTT-3.3.2-7 | Mappings per Network Connection only; MUST NOT carry forward | CONDITION_INACTIVE | tables live in the transient connection map / per client generation; reset on every transport change; T |
| MQTT-3.3.2-8 | Alias 0 never sent | codec | unchanged |
| MQTT-3.3.2-9 / -11 | Never exceed peer TAM | NOT_EMITTED | as above |
| MQTT-3.3.2-10 / -12 | MUST accept every 1..advertised TAM | CONDITION_INACTIVE | full acceptance; capacity guaranteed statically (§4.5), never refused dynamically |
| U-048 | Empty topic without alias is a Protocol Error | codec | unchanged |
| U-050 | Alias at most once per PUBLISH | codec | unchanged |
| U-055 | Alias 0 or > TAM → DISCONNECT **0x94** | always 0x94 | 0x94 only for out-of-range |
| U-056 | Unknown alias with empty topic → DISCONNECT **0x82** | n/a | 0x82 |
| §3.3.4 2b/3b | Non-empty topic with alias sets/replaces mapping | n/a | implemented |

### 3.2 Subscription Identifiers (§3.2.2.3.12, §3.8.2.1.2, §3.3.2.3.8, §3.3.4, §3.8.4)

| ID | Obligation | Today | S4 target |
| --- | --- | --- | --- |
| U-038 | Availability byte 0/1, once | codec | broker advertises `1` when enabled (omit = 1; send explicitly for clarity) |
| U-039 | Unsupported + SUBSCRIBE with ID → DISCONNECT **0xA1** | always 0xA1 | only when disabled |
| U-061 | 1..268,435,455, once per SUBSCRIBE; applies to every filter in that packet | codec | stored per subscription; absence stored too |
| U-053 | Multiple IDs allowed on PUBLISH; order not significant | codec | emitted |
| MQTT-3.3.4-3 / -4 | Single copy carries the IDs of all matching subscriptions that have one (identical IDs repeated) | OPTIONAL_REJECTED | implemented on the existing single-copy route |
| MQTT-3.3.4-5 | If multiple copies, each carries its own ID | OPTIONAL_REJECTED | applies to the separate shared copy |
| MQTT-3.3.4-6 | Client→server PUBLISH MUST NOT carry IDs; only IDs from the causing SUBSCRIBE | SOURCE_AND_REGRESSION | broker keeps 0x82 on inbound ID; client keeps local rejection |
| §3.8.4 (non-normative) | IDs removed on UNSUBSCRIBE, on re-SUBSCRIBE without/with different ID, on Session Present 0; retransmission may use old or new IDs but never revert | — | IDs snapshotted onto the delivery entry at install; retransmission reuses them (allowed) |
| §3.3.4 last ¶ | Shared: only the receiving client's IDs | — | per-member shared subscription record |

### 3.3 Shared subscriptions (§4.8.2, §3.2.2.3.13, §3.8.3.1, §3.8.4, §3.10.4)

| ID | Obligation | Today | S4 target |
| --- | --- | --- | --- |
| U-040 | Availability byte | codec | broker advertises `1` when enabled |
| U-041 | Unsupported + shared SUBSCRIBE → DISCONNECT **0x9E** | always | only when disabled (existing `SHARED_OFF` test kept with config off) |
| MQTT-4.8.2-1 / -2 | `$share/` + nonempty ShareName without `/ + #`, then `/` and a filter | codec | `Topic` gains a shared parser reused everywhere |
| MQTT-3.8.3-4 | No Local = 1 on shared is a Protocol Error | decode rejects (class → 0x81 today) | see Q14 for 0x82 |
| §3.8.4 | Join existing or create group; **no retained messages sent** | — | no retained candidates for shared filters, regardless of Retain Handling |
| §3.10.4 | UNSUBSCRIBE detaches; last member deletes group | — | groups derived from memberships; deleted implicitly |
| §4.8.2 | Session termination detaches; group ends with its last session and its undelivered messages are deleted | — | §4.4 |
| MQTT-4.8.2-3 | Respect each member's granted QoS | — | per-member grant |
| MQTT-4.8.2-4 | QoS2 in progress: complete with the same client on reconnect | — | entry stays in member session (existing QoS2 resume) |
| MQTT-4.8.2-5 | Session ends before reconnect: MUST NOT send that QoS2 message elsewhere | — | ever-sent QoS2 entries discarded at session end |
| §4.8.2 QoS1 note | MAY wait; SHOULD send to another member if session ends; MAY redistribute on connection loss | — | §4.4, Q6 |
| MQTT-4.8.2-6 | PUBACK/PUBREC ≥ 0x80 → discard, never resend to another member | — | existing discard path; redistribution never applies to acked entries |
| §3.1.2.11.4 note | Oversized for some members: discard or pick a member that can receive | — | pick among members whose Maximum Packet Size fits; else complete as discarded (MQTT-3.1.2-25) |
| MQTT-4.6.0-6 | Ordering obligation only for non-shared | supported | shared copies unordered across members |
| §4.8.2 bullet | Second SUBSCRIBE of the same shared filter by the same session does not add membership; first UNSUBSCRIBE leaves | — | membership keyed by session + full filter (map key) |

Clause-audit and inventory rows to amend after evidence: `CPROFILE`,
`BASIC`, `CALIAS`, `OPER`, `SHARED_OFF` catalogs; `MQTT-3.1.2-26/27`,
`MQTT-3.2.2-17/18`, `MQTT-3.3.2-7..12`, `MQTT-3.3.4-3..6`, `MQTT-3.8.3-4`,
`MQTT-4.8.2-1..6`, `U-012`, `U-034`, `U-038..041`, `U-048`, `U-050`, `U-053`,
`U-055`, `U-056`, `U-061`. Add a dated maintenance amendment; do not rewrite
the 2026-10-30 ledger.

## 4. Pure-layer design (`apps/graviton_mq_mqtt`)

All new state is plain data; no clock, process or socket. Time-dependent
behaviour continues to use the caller-supplied `now`.

### 4.1 `GravitonMQ.MQTT.Topic`

- `parse_filter(filter) :: {:ordinary, filter} | {:shared, share_name, filter} | :error`
  built on `Operational.topic_filter?/1` (single grammar source).
- `valid_subscription_filter?/1` accepts both forms; `valid_filter?/1` keeps
  its current meaning (ordinary only) so existing callers/tests do not change
  silently.
- `matches?/2` stays ordinary-only; shared matching calls it with the inner
  filter. `$`-topic exclusion (MQTT-4.7.2-1) therefore applies to the inner
  filter (`$share/g/#` does not match `$SYS/x`).

### 4.2 Topic Alias tables

Broker connection map (transient, `connections[id]`) gains:

```elixir
inbound_aliases: %{1..tam => topic},        # tam = advertised CONNACK value
outbound_aliases: %{topic => alias},        # bounded by min(client TAM, cap)
outbound_alias_max: non_neg_integer()       # computed at CONNACK
```

Client machine gains the same three fields, reset in `transport_up` and
`transport_down` (generation change).

Inbound resolution (both roles), run first in PUBLISH handling (and before
the broker's held-packet replay reaches `incoming_publish/5`, which already
preserves arrival order):

1. alias absent → unchanged.
2. alias `0` or `> advertised` → DISCONNECT 0x94 (`:topic_alias_invalid`).
3. topic `""` and alias known → substitute topic; topic `""` and unknown →
   DISCONNECT 0x82 (`:topic_alias_unknown`).
4. topic nonempty → set/replace mapping.
5. Remove `:topic_alias` from the properties of the resolved packet.

The resolved packet is what `same_publish?/2` (broker QoS1/2 DUP check),
`same_publication?/2` (client QoS2), `message/4`, authorization, retained
storage, checkpoints and host deliveries see. No alias value reaches durable
state.

Outbound assignment (both roles) is applied at send time on a copy of the
stored packet:

- topic in table → send `topic: ""` + alias;
- else if `map_size(table) < outbound_alias_max` → allocate next free alias,
  send full topic + alias, record mapping;
- else → full topic, no alias (policy "stable first-N", Q3).

The mapping is recorded only when the send effect is emitted. Size checks
(`outbound_fits?/3` on the broker, `peer_fits?/2` on the client) use the
aliased bytes. Retransmission after reconnect starts from an empty table and
therefore re-establishes mappings with full topics. Stored outbound packets
(client `outbound`, broker delivery `message`) always hold the full topic.

### 4.3 Subscription Identifiers (broker)

- Subscription record: `%{id: incarnation, options: …, subscription_identifier: nil | 1..268_435_455}`.
  The SUBSCRIBE's single ID is applied to every filter granted in that packet;
  a re-SUBSCRIBE replaces it (including with `nil`), as MQTT-3.8.4-3 already
  replaces options.
- `route/2` non-shared target: IDs = `for {_, sub} <- matches, sub.subscription_identifier != nil`,
  duplicates kept. `refresh_target/2` recomputes them from surviving
  incarnations. Retained-on-subscribe targets use the IDs of `target.filters`.
- Delivery entry gains `subscription_identifiers: [pos_integer()]`, set at
  `install_targets/2`. `recipient_packet/3` still strips any message-borne
  alias/ID and appends one `{:subscription_identifier, id}` per entry ID.
  Retransmissions reuse the snapshot (permitted; never reverts).
- When disabled: CONNACK `0`, SUBSCRIBE with ID → 0xA1 (unchanged path).

### 4.4 Shared subscriptions (broker)

**Membership.** A member is a session holding key `"$share/g/f"` in
`session.subscriptions` (the spec places it in session state; it already
persists, expires and is cleared on Session Present 0). The record adds
`shared: {share_name, inner_filter}`; `no_local` is always false. Groups are
derived (`full filter → sorted member client IDs`) on each route; no separate
group table is persisted. A transient `share_cursor: %{full_filter => client_id}`
lives in the broker struct (reset on restart).

**SUBSCRIBE.** When enabled, `prepare_subscribe/4` no longer rejects
`$share/`. Authorization request: `{:authorize, principal, :subscribe, "$share/g/f"}`
(full filter; Q12). Grant = `min(requested, maximum_qos)` per member
(MQTT-4.8.2-3). No retained candidates are produced for shared filters
(§3.8.4), independent of Retain Handling. Capacity: shared memberships count
toward `max_subscriptions`/`max_subscription_bytes` like any filter.

**Routing.** `route/2` returns, in addition to one non-shared target per
session (unchanged), one target per matching group:

```elixir
%{kind: :network, recipient: chosen_client_id, token: …, principal: …,
  incarnations: [{"$share/g/f", sub.id}], shared: "$share/g/f",
  qos: min(message.qos, sub.options.qos),
  retain: message.retain and sub.options.retain_as_published,
  subscription_identifiers: List.wrap(sub.subscription_identifier)}
```

A client in both a shared group and an overlapping non-shared subscription
may receive two copies (§4.8.2 last bullet). Member selection is deterministic
round-robin over sorted client IDs after the cursor, in tiers:

1. connected members whose Maximum Packet Size fits and (QoS0 or `send_quota > 0`);
2. other connected members whose Maximum Packet Size fits;
3. QoS > 0 only: offline members with nonzero session expiry, within
   `max_offline_deliveries/bytes`.

No eligible member → no target for that group (QoS0 and oversized: completed
as discarded, consistent with MQTT-3.1.2-25). Publication-time
`{:authorize, …, :deliver, topic}` is requested for the chosen member only; a
denial drops that group's copy (no fallback, Q12). Dispatch-time
re-authorization (`request_dispatch/6`) is unchanged.

**Delivery entry** gains `shared: nil | full_filter`.

**Redistribution** (new pure function, invoked from `end_session/3`,
`down_existing/4`, UNSUBSCRIBE commit and restore's recovered loss):

| Trigger | Entries with `shared != nil` | Action |
| --- | --- | --- |
| Session end (expiry, clean start, takeover end, expiry-0 loss) | QoS2 `ever_sent` (phase publish or pubrel) | discard — MQTT-4.8.2-5 |
| Session end | QoS1 (any) and QoS2 never sent | re-route to another member (SHOULD) |
| Connection loss, session survives | never sent (`ever_sent == false`) | re-route to a **connected** member (MAY; Q6) |
| Connection loss | `ever_sent` | stay; resume on reconnect (MQTT-4.8.2-4) |
| UNSUBSCRIBE of the group by the member | never sent | re-route; `ever_sent` stay with the member to completion |
| PUBACK/PUBREC ≥ 0x80 | — | existing discard; never re-routed (MQTT-4.8.2-6) |

Re-routing reuses the selection tiers, keeps `message`, assigns a new entry
ID and the new member's IDs/QoS, and clears `packet_id/ever_sent`. Moving an
entry never increases pending totals; moving to an offline member must fit
offline limits. If no member can take it, it is deleted (group ended or no
capacity) and counted in `status/1` (`shared_discarded`) so loss is never
silent. Durable effect: the same transition commits through the existing
whole-snapshot barrier before effects are interpreted.

**Takeover/principal.** Unchanged: membership belongs to the session, so a
same-principal takeover keeps it; clean start drops it (after redistribution).

### 4.5 Limits and options (pure broker `new/1`)

`new/1` accepts only non-negative integers; keep that by using integers:

| Key | Range | Proposed default | Meaning |
| --- | --- | --- | --- |
| `topic_alias_maximum` | 0..65,535 | 10 (Q2) | advertised inbound TAM |
| `max_outbound_topic_aliases` | 0..65,535 | 0 (Q2) | broker cap for aliases it sends |
| `shared_subscriptions` | 0..1 | 1 (Q1) | Shared Subscription Available |
| `subscription_identifiers` | 0..1 | 1 (Q1) | Subscription Identifier Available |

Alias memory is bounded statically, never by refusing an alias (MQTT-3.3.2-12):
worst case per connection `tam × min(65_535, max_packet_size)` bytes plus the
outbound table of the same shape. `new/1` rejects options where
`max_connections × (topic_alias_maximum + max_outbound_topic_aliases) ×
min(65_535, max_packet_size)` exceeds a new `max_alias_bytes` (default
16 MiB). Alias bytes are not charged to `max_incoming_bytes`.

### 4.6 Broker checkpoint format (`broker/checkpoint.ex`)

- Capture writes `version: 2`. Changes vs v1:
  - subscription map keys `[:id, :options, :subscription_identifier, :shared]`
    (`shared: nil | {name, filter}`);
  - delivery keys `@delivery ++ [:subscription_identifiers, :shared]`;
  - subscription filters validated with `Topic.valid_subscription_filter?/1`,
    and `shared` must equal the parsed filter.
- Restore accepts v1 and v2. v1 is upgraded in memory (`subscription_identifier: nil`,
  `shared: nil`, `subscription_identifiers: []`) before validation; the next
  commit writes v2. No offline migration tool.
- WAL record type stays `"mqtt-broker-snapshot/v1"` (envelope only; the
  payload carries its version). Changing it would make `Persistence.recover/5`
  reject logs that contain both types (`:checkpoint_record_type`).
- New atoms (`:subscription_identifier`, `:subscription_identifiers`,
  `:shared`) must appear as literals in `Checkpoint` so the safe ETF decoder
  finds them after `Code.ensure_loaded?(Checkpoint)`.
- Capability mismatch on restore (snapshot has shared memberships or IDs but
  the configured machine disables the feature) → `{:error, :checkpoint_capability_mismatch}`,
  failing startup explicitly (Q10). Downgrade to a pre-S4 release fails closed
  with `:invalid_broker_checkpoint` (Q9).
- Alias tables and `share_cursor` are never captured.

### 4.7 Pure client (`mqtt/client.ex`)

- `new/2`: remove the `topic_alias_maximum != 0 → :topic_aliases_unsupported`
  profile rule; accept options `:topic_alias_maximum` (inbound, advertised in
  CONNECT) and `:max_outbound_topic_aliases`. Caller-supplied `topic_alias` on
  `publish` stays rejected (machine-owned aliases only).
- `connected/3`: `outbound_alias_max = min(peer.topic_alias_maximum, cap)`.
- Inbound resolution per §4.2 before `incoming_publication/3`. Out-of-range
  keeps 0x94; unknown empty-topic alias → 0x82. Delivered packets carry the
  resolved topic and no `topic_alias` property (Q15).
- Sub-IDs: existing `properties: [subscription_identifier: n]` path stays;
  received IDs already reach the host in delivery properties. No receiver-side
  enforcement of MQTT-3.3.4-6 (Q13).
- Shared: existing peer-flag gate stays.

## 5. Runtime (`apps/graviton_mq_runtime/.../mqtt/*`)

- `Config`: add the §4.5 keys to `@machine_keys` with validation (booleans at
  the runtime surface, `shared_subscriptions: true | false`,
  `subscription_identifiers: true | false`, mapped to 0/1 for the machine).
  No `share_strategy` option in S4 (Q4).
- `Broker` (runtime owner): pass-through; `status/1` adds
  `shared_groups`, `shared_discarded`, alias-table counts. No new process.
  Writer/Connection/Listener/HellHandoff are untouched (aliasing happens in the
  pure machine before encoding).
- Backpressure: per-member Receive Maximum (`send_quota`) is tier 1 of member
  selection; the global `max_pending_*` and per-connection writer limits are
  unchanged because a group adds exactly one copy per message. Host demand is
  unaffected (hosts are not group members).
- Persistence: no code change beyond the checkpoint module; recovery of
  shared memberships, IDs and shared delivery entries must be covered by an
  owner-kill/VM-restart test (`mqtt_persistence_test.exs` / Paho variant).
- Host API (`GravitonMQ.subscribe/3`): unchanged in S4 (Q11).

## 6. Client runtime (`apps/graviton_mq_client`)

- `Config`: dedicated options `:topic_alias_maximum` (default 0, Q2) and
  `:max_outbound_topic_aliases` (default 0); the wire property stays forbidden
  in `:properties` (existing rule) and is no longer forced to 0 when the option
  is set.
- `MQTTClient.subscribe/3`: add `subscription_identifier:` option
  (sugar over `properties`); deliveries unchanged.
- Persistent shared subscriptions: delete the `client/mqtt.ex:97-99` clause;
  `Checkpoint.retained?/3` validates subscriptions with
  `Topic.valid_subscription_filter?/1`. Checkpoint version stays `1` (the
  schema is unchanged; only accepted filter values widen). A pre-S4 binary
  reading such a checkpoint fails closed (`:invalid_checkpoint_schema`).
  Checkpoint validation additionally rejects any persisted outbound PUBLISH
  carrying `topic_alias` (invariant of §4.2).
- `mqtt_shared_subscription_test.exs` first test inverts: persistent shared
  SUBACK is captured, survives owner restart, and is reported in
  `{:subscriptions_lost, …}` on Session Present 0 like any filter.
- Docs: `MQTT_CLIENT.md` "Shared subscriptions are outside the local
  persistence profile" paragraph, `MQTTClient` moduledoc.

## 7. Interop acceptance

Independent-peer evidence is required both directions per feature;
self-to-self (our client ↔ our broker) tests are additional only. Where a peer
cannot show the wire form (Paho resolves nothing; libmosquitto resolves
silently), add a test-support byte relay (`test/support/mqtt_wire_tap.exs`)
that forwards bytes unchanged and records decoded packets with our codec. It
proves what was on the wire; it is not a peer.

| Feature | Peer → our broker | Our client → Mosquitto |
| --- | --- | --- |
| Inbound alias | Paho publishes `TopicAlias=1` with topic, then `topic=""` + alias; Paho/host subscriber gets full topic, QoS0/1/2. Negatives: alias > TAM → Paho sees DISCONNECT 0x94; unknown alias with empty topic → 0x82; mapping not carried across reconnect. `mosquitto_pub -V 5 -D publish topic-alias 1 -l` as second peer (verify it emits empty topics; else Paho only) | — (client inbound: see next row) |
| Broker/Mosquitto → client alias | Paho subscriber connects with `TopicAliasMaximum=N`, script resolves aliases and asserts empty-topic packets after the first, ≤ N, reset after reconnect; `mosquitto_sub -V 5 -D connect topic-alias-maximum N` receives correct topics | Mosquitto `max_topic_alias_broker` (if present in 2.1.2) sends aliases to our client advertising TAM; wire tap shows alias-only PUBLISH; deliveries have full topics, QoS1/2 incl. reconnect |
| Client outbound alias | — | Our client `max_outbound_topic_aliases: N` publishes to Mosquitto (`max_topic_alias` 10, then 1 to prove the bound); Paho subscriber verifies topics; wire tap shows aliases |
| Subscription IDs | Paho subscribes `sub/#` ID 7 and `sub/+/x` ID 9 → single PUBLISH with IDs {7,9}; re-subscribe without ID removes it; retained-on-subscribe carries ID; durable owner-kill recovery keeps IDs. `mosquitto_sub -D subscribe subscription-identifier 7 -F %P`/`-v` as second peer | Our client subscribes with IDs to Mosquitto (overlapping filters); delivery properties contain the IDs |
| Shared | Two Paho members in `$share/g/t/#` + Paho publisher, N QoS1 messages: each delivered exactly once across the group, both receive; no retained on subscribe; member disconnect mid-stream → unsent QoS1 delivered to the other; QoS2 in flight completes with the same member on reconnect; session expiry with in-flight QoS2 → not delivered elsewhere; No Local on shared → disconnect; config `shared_subscriptions: false` keeps `SHARED_OFF`. `mosquitto_sub -t '$share/g/t/#'` as second member type | Two of our clients (memory and persistent profiles) in a Mosquitto group: messages split, none duplicated; persistent client restart keeps membership (Session Present 1) |

All new peer tests use the existing tags/fixtures (`mqtt_peer.py` modes,
`mqtt_metadata_peer.py`-style scripts, `test/support/mosquitto.exs`
`configuration:`). Linux-only durability variants follow the existing tags.

## 8. Files and task breakdown

Likely touched (MQTT only; no AMQP, storage, mix.exs or lockfiles):

- `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/{topic.ex,broker.ex,client.ex,broker/checkpoint.ex}`;
  possibly `codec/operational.ex` (Q14 only).
- `apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/{config.ex,broker.ex}`.
- `apps/graviton_mq_client/lib/graviton_mq/client/mqtt.ex`,
  `client/mqtt/{config.ex,checkpoint.ex}`, `mqtt_client.ex` (docs).
- Tests: `apps/graviton_mq_mqtt/test/{topic_test,broker_test,client_test,broker_checkpoint_test}.exs`
  (+ new `broker_shared_test.exs`, `alias_test.exs`); `apps/graviton_mq/test/graviton_mq/mqtt_{paho,persistence,persistence_paho,mosquitto,host}_test.exs`,
  `test/support/mqtt_peer.py`; `apps/graviton_mq_client/test/{mqtt_shared_subscription,mqtt_checkpoint,mqtt_mosquitto,mqtt_client}_test.exs`,
  `test/support/`.
- Docs after evidence: `docs/CAPABILITIES.md`, `MQTT_BROKER.md`,
  `MQTT_CLIENT.md`, inventory/audit amendments, release notes.

Tasks (each TDD, each ends with the required sequence for touched apps):

| # | Task | Depends on |
| --- | --- | --- |
| T0 | Re-verify peer capabilities on pinned builds (Mosquitto 2.1.2 `max_topic_alias_broker`, `mosquitto_pub` alias behaviour); record in this spec | — |
| T1 | `Topic.parse_filter/1`, `valid_subscription_filter?/1`; pure vectors | — |
| T2 | Alias resolver/assigner (pure helper shared by both roles, e.g. `GravitonMQ.MQTT.TopicAlias`) with vectors for every U-055/U-056 branch | T1 |
| T3 | Broker inbound/outbound alias + limits/CONNACK | T2 |
| T4 | Broker Sub-IDs (store, route, retained, refresh, recipient packet) | T1 |
| T5 | Broker shared: membership, routing tiers, no retained, redistribution table | T1, T4 |
| T6 | Broker checkpoint v2 + v1 upgrade + mismatch error; recovery tests incl. v1 fixture bytes | T4, T5 |
| T7 | Runtime config/status; persistence/recovery tests | T3–T6 |
| T8 | Pure client aliases, options | T2 |
| T9 | Client owner: persistent shared subscriptions, checkpoint filter validation, `subscription_identifier:` option | T1, T8 |
| T10 | Interop: wire tap, Paho/mosquitto modes, broker-direction tests | T7 |
| T11 | Interop: client → Mosquitto tests | T9, T10 (tap) |
| T12 | Docs, CAPABILITIES with executed evidence only; inventory amendment | T10, T11 |

T3/T4 and T8 can run in parallel (disjoint files: `broker.ex` vs `client.ex`);
T5 and T6 serialize on `broker.ex`/`checkpoint.ex`.

## 9. Risks

- `route/2` is O(sessions × subscriptions) per message; shared grouping adds
  a sort per group. Acceptable at current limits (128 sessions, 1,024
  subscriptions); S5 benchmarks should include a shared-group case.
- Redistribution inside `end_session/3` touches every expiry/takeover path;
  each needs a regression (the 0.2.1/2026-11-08 takeover fixes are precedent).
- Enabling inbound aliases by default changes every existing Mosquitto→client
  path if the client default is nonzero; hence client default 0 (Q2).

## 10. Open questions for the owner

1. **Default enablement.** Should shared subscriptions, Subscription
   Identifiers and inbound Topic Alias be on by default on the broker once
   qualified? *Recommendation:* on by default (matches Mosquitto and client
   expectations; spec defaults for absent availability flags are "supported"),
   each with a config switch; broker outbound aliases off by default.
   *Trade-off:* on-by-default changes CONNACK for every existing deployment and
   widens the attack/memory surface; opt-in keeps the qualified 0.3 profile
   byte-identical but leaves most clients unable to use features they assume.
2. **Default Topic Alias Maximum values.** *Recommendation:* broker inbound 10,
   broker outbound cap 0, client inbound 0, client outbound cap 0 (all
   configurable). *Trade-off:* higher values save bandwidth for long repeated
   topics but cost up to `TAM × 64 KiB` per connection; client-side nonzero
   defaults would silently change the wire path of all existing Mosquitto
   tests and deployments.
3. **Outbound alias assignment policy.** *Recommendation:* "stable first-N" —
   alias the first N distinct topics per connection, never replace.
   *Trade-off:* simple, deterministic, no remap churn; LRU replacement serves
   high-cardinality topic sets better but re-sends full topics on every miss
   and is harder to test.
4. **Share distribution strategy.** *Recommendation:* one built-in,
   deterministic, quota-aware round-robin (tiers in §4.4), no configuration
   option in S4. *Trade-off:* random or least-loaded strategies may balance
   better under skew; a config option is cheap to add later but expensive to
   remove once public.
5. **Binding time.** *Recommendation:* bind each message to a member at
   publication time (reuses per-session queues, persistence and QoS2 resume),
   plus redistribution. *Trade-off:* a group-level queue with late binding at
   dispatch gives better fairness and no "stuck on offline member" cases, but
   needs a new durable owner, new checkpoint section and new backpressure
   accounting — a much larger change.
6. **Redistribution on member connection loss (spec MAY).** *Recommendation:*
   immediately move never-sent QoS1/2 entries to connected members; keep
   already-sent ones with the member until session end (QoS1 then moves,
   sent QoS2 is discarded per MQTT-4.8.2-5). *Trade-off:* faster delivery vs
   more state movement and possible reordering; waiting for reconnect is
   simpler but can stall a group behind one flapping member.
7. **No connected member available.** *Recommendation:* queue QoS1/2 to an
   offline persistent member (round-robin, within offline limits); drop QoS0.
   *Trade-off:* preserves at-least-once when the whole group is briefly
   offline, but may bind to a member that never returns (mitigated by Q6
   redistribution on its session end).
8. **Retained messages for shared subscriptions.** *Recommendation:* follow the
   spec — never send retained messages on shared subscribe, no option.
   *Trade-off:* some brokers offer an opt-in; adding one is non-standard and
   was not requested.
9. **Durable-format migration.** *Recommendation:* broker snapshot v2 with
   read-time upgrade of v1, same WAL record type, no downgrade support
   (older releases fail closed on v2). Client checkpoint stays v1 (wider
   filter values only). *Trade-off:* zero-step upgrade for operators; rollback
   to a pre-S4 release requires discarding or keeping the old data directory.
10. **Recovery with a feature disabled.** If persisted state contains shared
    memberships or Sub-IDs but the restarted broker disables the feature:
    *Recommendation:* fail startup with `:checkpoint_capability_mismatch`.
    *Trade-off:* explicit and lossless, but an operator must re-enable or wipe;
    silently dropping memberships would lose accepted responsibility.
11. **Host API.** Should `GravitonMQ.subscribe/3` gain shared groups (host
    subscribers as group members) or Subscription Identifiers?
    *Recommendation:* not in S4. *Trade-off:* host members would need demand-based
    selection and a separate redistribution path; Sub-IDs have no meaning for
    an in-process subscriber that already holds its subscription handle.
12. **Authorization semantics.** *Recommendation:* the policy's `:subscribe`
    check receives the full `$share/{name}/{filter}` string (a new value
    existing policies never saw); publication-time `:deliver` is requested
    only for the chosen member and a denial drops that group copy without
    fallback. *Trade-off:* passing the inner filter would let existing ACLs work
    unchanged but hides the group name from policy; fallback to another member
    multiplies callback load per message.
13. **Client receiver enforcement of MQTT-3.3.4-6.** *Recommendation:* do not
    reject incoming PUBLISH with unexpected Subscription Identifiers; pass them
    to the host. *Trade-off:* strictness would need IDs in the client
    checkpoint and could disconnect from lenient brokers.
14. **Reason code for No Local on a shared filter.** Today decode treats it as
    malformed (DISCONNECT 0x81); MQTT-3.8.3-4 calls it a Protocol Error (0x82).
    *Recommendation:* change to 0x82 in S4. *Trade-off:* touches the historical
    codec error class and its regression tests.
15. **Client delivery of `topic_alias`.** *Recommendation:* deliver the resolved
    topic and strip the hop-local `topic_alias` property (as the broker already
    strips it when forwarding). *Trade-off:* hosts lose visibility into wire
    aliasing, which is a transport detail.
