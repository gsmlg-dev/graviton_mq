# MQTT M0 CONNECT/CONNACK Codec Implementation Plan

> **For agentic workers:** Execute the approved M0 slice task by task using `scoped-prd-execution` and `test-driven-development`. Independent integration and codec work may run in the current checkout with explicit file ownership and separate build directories. No branch/worktree creation, commits, publication, or deployment is authorized.

**Goal:** Add a sixth library application containing a bounded, pure MQTT packet envelope and MQTT 3.1.1/5.0 CONNECT/CONNACK codec, with architecture and standalone-package qualification.

**Architecture:** `graviton_mq_mqtt` depends only on core. Runtime composes the new library; the public application still depends only on runtime and owns the sole Application callback. Existing supervisors remain empty. Decode packet envelopes without implicitly decoding protocol schemas.

**Tech stack:** Elixir 1.18.4, OTP 28.5.0.1, ExUnit, existing Mix/compiler-xref/parsed-syntax checks, existing Hex archive staging. No external runtime dependency.

## Execution contract and ownership

- Root owns this plan, the approved design, all new `apps/graviton_mq_mqtt/**` files, and the final six-package release-test fixture update.
- Integration worker owns `apps/graviton_mq_runtime/mix.exs`, the public `apps/graviton_mq/mix.exs` description, architecture rules/analyzer/task and their tests, existing dependency/lifecycle tests, `scripts/stage_hex.exs`, `scripts/build_hex.exs`, `AGENTS.md`, `README.md`, `docs/ARCHITECTURE.md`, `docs/ROADMAP.md`, and MQTT scope documentation.
- Root reviews the combined diff and runs all final checks serially.
- M1 packet schemas, protocol transitions, authentication execution, sockets, broker APIs, queues, concrete storage, and live broker compatibility are excluded.

## Codec contract

The public pure APIs are:

```elixir
GravitonMQ.MQTT.Codec.Frame.decode(bytes, limits \\ Limits.default())
GravitonMQ.MQTT.Codec.Frame.encode(frame, limits \\ Limits.default())
GravitonMQ.MQTT.Codec.Packet.decode(bytes, version \\ :auto, limits \\ Limits.default())
GravitonMQ.MQTT.Codec.Packet.encode(packet, limits \\ Limits.default())
```

Versions are `:v3_1_1` and `:v5`. `:auto` recognizes CONNECT's explicit level;
CONNACK requires the caller's version. A mismatched explicit CONNECT version
is an error. Decode returns `{:ok, value, rest}`, `{:more, positive_count}`,
or `{:error, %Codec.Error{}}`; encode returns `{:ok, binary}` or an error.
Decode complete envelopes with truncated internal fields as malformed,
rather than requesting bytes belonging to the next packet.

Data modules:

```elixir
%GravitonMQ.MQTT.Frame{type: 1, flags: 0, body: <<>>, header_size: nil}
%GravitonMQ.MQTT.Packet.Connect{
  version: :v5, client_id: "client", clean_start: true,
  keep_alive: 0, properties: [], will: nil, username: nil, password: nil
}
%GravitonMQ.MQTT.Packet.Will{topic: "status", payload: <<>>, qos: 0, retain: false, properties: []}
%GravitonMQ.MQTT.Packet.Connack{version: :v5, session_present: false, reason_code: 0, properties: []}
```

`clean_start` stores the wire clean flag; for level 4 it denotes Clean Session,
without implementing either version's session lifetime. Properties are an
ordered list of `{predefined_property_name, value}` pairs. User Property
values are `{utf8_name, utf8_value}` pairs; duplicates and order survive.
Password, Authentication Data, Correlation Data, and Will payload are binary.
Do not add packet values for the later M1 schemas.

Limits are `max_packet_size` (default 1,048,576; includes the fixed header),
`max_field_bytes` (default 65,535), and `max_properties` (default 256 per section).
Validate limits before parsing/encoding. Error classes are `:malformed`,
`:unsupported`, `:limit_exceeded`, and `:invalid_value`; operations distinguish
frame and packet encode/decode. Avoid including credentials in error details.

The common envelope enforces the four-byte Remaining Length bound and fixed
header rules, preserves opaque body and suffix, and records header size.
MQTT 5 requires minimum-length VBI encoding. MQTT 3.1.1 has no such explicit
minimum requirement; the packet facade can use decoded version and header
size to enforce the MQTT 5 rule while preserving the legacy envelope form.
Encoding always emits minimum-length VBIs. MQTT 5 Property Length and Property
Identifiers must also use minimum-length encoding.

Properties supported by this slice:

| Name | Wire type | Locations | Additional validation |
| --- | --- | --- | --- |
| session_expiry_interval | u32 | CONNECT, CONNACK | Singleton |
| receive_maximum | u16 | CONNECT, CONNACK | Nonzero, singleton |
| maximum_packet_size | u32 | CONNECT, CONNACK | Nonzero, singleton |
| topic_alias_maximum | u16 | CONNECT, CONNACK | Singleton |
| request_response_information | byte | CONNECT | 0 or 1, singleton |
| request_problem_information | byte | CONNECT | 0 or 1, singleton |
| authentication_method | UTF-8 | CONNECT, CONNACK | Singleton |
| authentication_data | binary | CONNECT, CONNACK | Singleton; CONNECT requires method in its properties; CONNACK correlation requires future connection context |
| user_property | UTF-8 pair | CONNECT, CONNACK, Will | Repeatable, ordered |
| will_delay_interval | u32 | Will | Singleton |
| payload_format_indicator | byte | Will | 0 or 1, singleton |
| message_expiry_interval | u32 | Will | Singleton |
| content_type | UTF-8 | Will | Singleton |
| response_topic | UTF-8 | Will | Nonempty Topic Name; no wildcards, singleton |
| correlation_data | binary | Will | Singleton |
| maximum_qos | byte | CONNACK | 0 or 1, singleton |
| retain_available | byte | CONNACK | 0 or 1, singleton |
| assigned_client_identifier | UTF-8 | CONNACK | Nonempty, singleton |
| reason_string | UTF-8 | CONNACK | Singleton |
| wildcard_subscription_available | byte | CONNACK | 0 or 1, singleton |
| subscription_identifier_available | byte | CONNACK | 0 or 1, singleton |
| shared_subscription_available | byte | CONNACK | 0 or 1, singleton |
| server_keep_alive | u16 | CONNACK | Singleton |
| response_information | UTF-8 | CONNACK | Singleton |
| server_reference | UTF-8 | CONNACK | Singleton |

All UTF-8 Encoded Strings reject invalid UTF-8, surrogate encodings, U+0000,
and lengths exceeding the protocol/configured bounds. Preserve BOM and Unicode
normalization. Optional disallowed Unicode control/noncharacters are retained;
their rejection policy is not imposed by this codec. Will payload is opaque
binary; Payload Format Indicator is retained for later broker acceptance
policy. Properties are rejected when unknown, in the wrong location,
duplicated as singletons, truncated, or invalid for their declared wire type.

## Task 1: Integrate the sixth application boundary

- [x] Extend existing graph/lifecycle/architecture regression tests to expect `graviton_mq_mqtt`, forbid MQTT-to-AMQP/runtime/storage references, and reject process, transport, and clock references from pure MQTT modules. Keep AMQP rules unchanged.
- [x] Observe expected failures before editing production boundary rules. Root creates the new child Mix project using the existing shared build/version/package functions; library application has no Application callback.
- [x] Add runtime's MQTT dependency, the new namespace/application to xref ownership and task inputs, and six-package dependency-order staging. No generated registry atoms or extra supervisors.
- [x] Run targeted boundary tests under the pinned toolchain in a worker-specific build directory. Coordinate compilation with the root's new app files.

Expected dependency declaration:

```elixir
# New MQTT child's only internal dependency:
[{:graviton_mq_core, in_umbrella: true}]
# Runtime adds this fourth internal dependency:
{:graviton_mq_mqtt, in_umbrella: true}
```

## Task 2: Independent wire fixtures and incremental envelope

- [x] Add ExUnit fixtures for the following literal standard packets, and observe failures because the MQTT codec is missing:

```elixir
connect311 = <<0x10, 0x0D, 0, 4, "MQTT", 4, 2, 0, 60, 0, 1, "c">>
connect5 = <<0x10, 0x0E, 0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c">>
connack311 = <<0x20, 2, 0, 0>>
connack5 = <<0x20, 3, 0, 0, 0>>
```

- [x] Create focused data/error/limit/wire/envelope modules. Before adding packet schemas, prove the envelope returns opaque body and exact suffix for any recognized packet type.
- [x] Add prefix tests for every strict prefix of fixture packets; test Remaining Length widths 1–4, fifth-byte/continuation overflow, invalid flags/type, maximum size rejection from a header alone, and complete versus incomplete envelopes.
- [x] Run `mix test apps/graviton_mq_mqtt/test/codec/frame_test.exs`; expect all assertions to pass after the minimal envelope implementation.

## Task 3: CONNECT fields and MQTT 5 properties

- [x] Add decoding/encoding tests for the literal CONNECT fixtures and every strict prefix, concatenated input, Will/username/password fields, empty fields, non-normalized Unicode/BOM, and binary credentials/payload.
- [x] Implement CONNECT level dispatch, explicit version checks, exact reserved/Will flag validation, v3 password/user dependency and empty Client ID/clean-flag rules. Keep server Client ID assignment/admission policy out of the codec.
- [x] Add independent MQTT 5 property fixtures that exercise every row above at CONNECT/Will locations, ordered duplicate User Properties, zero forbidden values, duplicate singletons, unknown IDs, wrong locations, invalid UTF-8, encoded lengths, property limits, and Authentication Data without Method.
- [x] Implement shared typed property parsing/encoding with an explicit ID/type/location catalog. Encode semantic field failures as `:invalid_value` and malformed wire fields as `:malformed`. Do not rescue errors or ignore unknown fields.
- [x] Run focused CONNECT tests, then the MQTT application's complete test set. Diagnose each failure before retrying; do not alter correct fixtures to fit implementation.

## Task 4: CONNACK schema and adversarial boundaries

- [x] Add CONNACK literal fixtures for both versions, explicit-version requirement, version-specific reason-code sets, reserved flags, Session Present on failure, exact packet exhaustion, and every permitted CONNACK property.
- [x] Implement CONNACK encoding/decoding and packet facade dispatch for exactly CONNECT/CONNACK. Other well-framed schemas return explicit unsupported results; Frame decoding remains opaque.
- [x] Add encode-side invalid values, limits with incorrect types, field maximum/excess boundaries, valid packets at all limit boundaries, malformed complete field lengths, canonical MQTT 5 VBIs, legacy Remaining Length behavior, and deterministic binary mutation/truncation coverage.
- [x] Run the full MQTT test directory and confirm errors never escape as exceptions for arbitrary peer bytes; do not introduce property-testing dependencies for this slice.

## Task 5: Scope documentation and distribution qualification

- [x] Update active repository scope and dependency diagrams to six apps. Preserve historical AMQP milestones and exact data semantics. Mark the approved design's M0 progress only with final evidence.
- [x] Document actual codec API, tagged versions, limits, ordered properties, standalone/embedding lifecycle, and the lack of live broker operation in this slice. Describe later M1–M5 stages as future work.
- [x] Extend package smoke verification to load the MQTT module from unpacked archives, decode an independent fixture, and exercise a host-supervised instance. Include MQTT in the library-app/no-callback assertions.
- [x] Build and inspect six package archives in a new temporary directory using `elixir scripts/stage_hex.exs <directory>` and `elixir scripts/build_hex.exs <directory>`. Verify exact internal versions and the native package module, without publishing.

## Task 6: Final integrated checks and review

- [x] Run the required root sequence serially under `.tool-versions`: `mix deps.get`, `mix format`, `mix format --check-formatted`, `mix compile --warnings-as-errors`, `mix test`, `mix graviton_mq.check_architecture`, `mix help xref`, `mix xref graph --format cycles --fail-above 0`.
- [x] Review complete tracked and new-file changes against M0. Use an independent read-only review for specification compliance and codec robustness, resolve actionable findings, and rerun affected checks after code changes.
- [x] Confirm no new runtime workers/listeners or fake broker operations, no changes to existing AMQP behavior, no unrelated files, and no commit/publication. Report exact test/archive outcomes and remaining milestone boundaries.

## Completion evidence — 2026-10-26

- PASS: complete eight-command root sequence with Elixir 1.18.4 / OTP 28.5.0.1; warning-free compilation; 290 tests, zero failures, including 76 MQTT tests.
- PASS: compiler-xref/parsed-syntax architecture check, 1,698 source references across six applications; no xref cycles.
- PASS: six real `0.1.0` Hex archives built with `scripts/stage_hex.exs` and `scripts/build_hex.exs` in `/tmp/graviton-mqtt-m0-hex-tRN3zr/packages`; actual archive metadata preserves exact internal versions.
- PASS: independent clean standalone and `runtime: false` embedding consumers compile with warnings as errors, decode the literal CONNECT fixture, and exercise startup/host-supervised shutdown.
- PASS: independent read-only codec review and final combined source/diff review. Property-length overflow guard and the stale five-app release fixture were corrected; integrated verification passed afterward.
- NOT IMPLEMENTED: M1–M5 packet expansion, broker behavior, sockets, and durability. No commit, publication, release, or deployment performed.
