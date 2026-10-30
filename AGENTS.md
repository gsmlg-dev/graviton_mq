# GravitonMQ repository instructions

These instructions apply to the entire repository.

## Product and clean-room boundaries

- GravitonMQ targets AMQP 1.0 and MQTT 5.0 only. Do not use AMQP 0-9-1
  methods, exchanges, bindings, channels, `basic.publish`, or `basic.consume`
  as the protocol foundation.
- MQTT 3.1 is unsupported. Preserve historical MQTT 3.1.1 CONNECT/CONNACK
  codec compatibility and regressions; its later runtime needs separate
  acceptance. Operational roles in the active Goal target MQTT 5.0.
- The MQTT product target includes our own network client and broker, with
  independent lifecycles and a shared pure protocol layer. Implement MQTT
  behavior from the specification; do not wrap another MQTT implementation
  or substitute an external service for our broker. Standard Elixir/OTP networking
  and TLS primitives are infrastructure; independent MQTT peers are allowed
  for interoperability tests. The active G0–G7 Goal advances beyond historical M0.
- RabbitMQ is an architectural reference only, for process ownership,
  supervision, connection and session boundaries, queue-type abstraction,
  state machines and effects, storage boundaries, and fault isolation.
- Do not copy, translate, transliterate, mechanically port, or disguise
  RabbitMQ source. Protocol behavior comes from the AMQP 1.0 and MQTT
  specifications and independent tests.
- Do not select or add a project license without an explicit owner decision.

## Umbrella dependency direction

An arrow points from a consumer toward the child application it depends on:

```text
graviton_mq         -> graviton_mq_runtime
graviton_mq_runtime -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_storage -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_amqp10  -> graviton_mq_core
graviton_mq_runtime -> graviton_mq_mqtt    -> graviton_mq_core
graviton_mq_client  -> graviton_mq_core
graviton_mq_client  -> graviton_mq_amqp10
graviton_mq_client  -> graviton_mq_mqtt
```

- `graviton_mq_core` has no dependency on another GravitonMQ child.
- `graviton_mq_storage` depends only on core.
- `graviton_mq_amqp10` depends only on core.
- `graviton_mq_mqtt` depends only on core and owns `GravitonMQ.MQTT`.
- `graviton_mq_runtime` may depend on core, storage, AMQP 1.0, and MQTT.
- `graviton_mq` depends only on runtime.
- `graviton_mq_client` depends only on core and the two pure protocol libraries;
  it must not reference broker runtime, storage, or public composition.
  It owns `GravitonMQ.Client`, `GravitonMQ.MQTTClient`, and
  `GravitonMQ.AMQP10Client`, with no Application callback or implicit broker.
- Do not introduce dependency cycles. Only `graviton_mq` owns the top-level
  `Application` callback.

The source-level rules are stricter than declarations in `mix.exs`:

- core must not reference AMQP 1.0 or MQTT modules, concrete storage modules,
  runtime modules, or the public composition application;
- storage must not reference AMQP 1.0, MQTT, or runtime modules;
- AMQP and MQTT must not reference one another;
- all MQTT modules remain pure: no process, transport/socket, or clock
  dependencies, and no references to storage, runtime, or the public
  composition application;
- all AMQP protocol modules, including endpoint/session transitions, remain
  pure: no process, clock/timer, runtime, storage, socket, or transport
  dependencies. Running owners interpret their explicit effects in the client
  or broker runtime applications; and
- the application graph must remain acyclic.

Keep `mix graviton_mq.check_architecture` based on compiler xref manifests and
parsed Elixir syntax. Do not replace source dependency analysis with grep.

## Data and ownership rules

- Preserve AMQP semantic type identity. `binary`, `string`, and `symbol`, and
  every signed and unsigned integer type, are distinct values. Compact wire
  constructors such as `smalluint` are encoder choices, not semantic value
  types.
- The historical bounded performative codec supports Open, Begin, Detach, End,
  and Close. G3–G5 extend it with Attach, Flow, Transfer, and Disposition;
  preserve the foundation schemas and their historical acceptance.
  Decode their standard numeric and symbolic descriptors into dedicated
  immutable structs and encode the standard numeric descriptors. Detach, End, and Close
  carry an optional nested `GravitonMQ.AMQP10.Error`; Error is protocol data, not
  a top-level performative, and its condition, description, and info fields
  retain exact tagged AMQP values.
- Open fields are `container_id`, `hostname`, `max_frame_size`, `channel_max`,
  `idle_time_out`, `outgoing_locales`, `incoming_locales`,
  `offered_capabilities`, `desired_capabilities`, and `properties`. Begin
  fields are `remote_channel`, `next_outgoing_id`, `incoming_window`,
  `outgoing_window`, `handle_max`, `offered_capabilities`,
  `desired_capabilities`, and `properties`. Validate their exact positional
  schemas, mandatory fields, tagged types, and symbol-keyed property maps.
- Materialize Open's `max_frame_size` and `channel_max` defaults as tagged
  `uint(4_294_967_295)` and `ushort(65_535)`, and Begin's `handle_max` default
  as tagged `uint(4_294_967_295)`. Canonical encoding may remove trailing nulls
  but must preserve interior positional holes.
- End and Close each have one optional `error` field. A nested Error has
  mandatory tagged `symbol` condition, optional tagged `string` description,
  and optional ordered tagged `map` info with symbol keys. Accept numeric and
  symbolic Error descriptors only in a nested Error position; encode the
  numeric descriptor. Do not whitelist error-condition symbols. Error info uses the
  shared bounded value subset, including the approved Boolean scalar support.
- Detach fields are `handle`, `closed`, and `error`. Require an exact tagged
  `uint` handle, materialize omitted/null `closed` as tagged Boolean false,
  and retain the optional nested Error at outer field index `2`. End/Close
  keep Error at index `0`; Error's internal indexes remain `0`, `1`, and `2`.
  Canonical encoding may omit the false default while preserving its interior
  null when Error is present. Do not interpret handles or detach live Links.
- Boolean values decode from `0x41`, `0x42`, and `0x56` with payload `0` or `1`;
  reject other payload octets as malformed and encode canonically with `0x41`
  or `0x42`. Boolean joins the shared bounded subset recursively, including
  Open/Begin properties and Error info. The historical symbol-only array
  subset is extended in G5 to homogeneous scalar, compound and described
  arrays, retaining exact element type and descriptor identity. Float/double
  and decimal values retain their exact encoded bits; char is a Unicode scalar.
- Keep frame and authoritative message content opaque. Frame decoding must not
  invoke performative or message decoding implicitly. Malformed, unsupported,
  limit-exceeded, invalid-value, and incomplete results remain explicit; do
  not rescue malformed peer bytes into success.
- A Link has independently allocated local and remote handles. Its stable
  Session-local identity is `{link_name, local_role}`. Incoming frames resolve
  through the remote-handle index; outgoing frames use the local handle.
- Link remains Session-owned state initially. Do not introduce one process per
  Link without a later, evidence-backed architecture decision.
- Sessions keep separate local- and remote-handle indexes. Connections keep
  separate local- and remote-channel indexes.
- Protocol delivery IDs, link handles, and channel numbers are not durable
  queue identities. PIDs, ports, references, and arbitrary terms are not
  durable identities either.
- Core owns stable broker identities and protocol-independent logical
  `GravitonMQ.Queue.Event` values. Storage owns physical
  `GravitonMQ.Storage.Record` values, encoding, positions, checksums, and
  durability.
- Preserve the original encoded AMQP message as opaque content. The core may
  keep a small protocol-neutral broker index, but it must not replace the
  message of record with `body: term()` or import AMQP message structs.
- Core settlement instructions must distinguish successful retirement, plain
  release, rejection, and modified release, retaining rejection policy,
  delivery-failed, undeliverable-here, and opaque mutation data.

## State machines, durability, and failures

- `GravitonMQ.Queue.Machine` will be deterministic and return `{new_state,
  effects}`. Do not perform network, socket, storage, timer, or process side
  effects inside pure state machines.
- Runtime interprets effects. Core emits logical queue events; storage converts
  them to physical records.
- The core storage behaviour must retain mandatory append, synchronize,
  durable-through, and streaming fold operations. Append ordering is not proof
  of durability. An append result must be correlatable with a later
  durable-through boundary.
- Publisher acceptance must wait until the durability boundary selected by
  queue policy has been reached. Never turn a failed or uncertain durability
  result into success.
- The first delivery guarantee target is at-least-once. Do not claim
  exactly-once delivery.
- Do not add fake client or broker functions that return `:ok`, empty success
  data, or other values implying that an unimplemented operation works.

## Lifecycle and naming

- Broker standalone and embedded operation use `GravitonMQ.start_link/1` and
  `GravitonMQ.child_spec/1`. `GravitonMQ.Application` starts its default
  instance through the same API.
- `GravitonMQ.MQTTClient` and the later `GravitonMQ.AMQP10Client` belong to
  the independent client library. Running outgoing owners live under
  `GravitonMQ.Client`; broker owners remain under `GravitonMQ.Runtime`.
  Keep protocol namespaces pure. See ADR 0007 for the client ownership amendment.
- Instance `:name` and `:runtime_supervisor_name` options are exact OTP names.
  Do not create atoms dynamically from external strings. Two registered
  instances need distinct names at both levels.
- Historical M0 supervisors are empty. Introduce running children only with
  functioning behavior and stage evidence; never add one merely to occupy a slot.

## Active Goal and historical milestone exclusions

The owner authorized G0–G7 on 2026-10-29 in `docs/ACTIVE_SCOPE.md`. Implement
the four network roles under that scope and ADR 0007; update
`docs/CAPABILITIES.md` with actual evidence. Historical exclusions below
describe completed slices and do not prohibit authorized later stages.
Preserve their acceptance records and all architecture/data invariants.
Clustering/Raft, management UI, cross-protocol bridge, AMQP transactions,
AMQP 0-9-1, and the deferred QUIC TODO remain outside the active Goal.

The completed AMQP foundation supports only Open, Begin, Detach, End, and Close
performative schemas, nested Error, and its bounded value subset. Preserve its
existing semantics and historical milestone records. Milestone 1 remains the
completed Open/Begin foundation; the completed Boolean/Detach slice is recorded
in `docs/superpowers/specs/2026-10-09-boolean-detach-codec-design.md`.

The owner-approved MQTT M0 scope is recorded in
`docs/superpowers/specs/2026-10-26-mqtt-broker-embedding-design.md` and
`docs/superpowers/plans/2026-10-26-mqtt-connect-connack-codec.md`. It adds only a
sixth library application, a pure opaque packet envelope, MQTT 5.0 only
CONNECT/CONNACK data and codecs, limits, architecture checks, and package/scope
integration. Decode does not authenticate, register clients, create sessions,
run timers, or invoke broker behavior. Keep credentials out of error details.

Do not extend M0 to other MQTT packet schemas, a network-client runtime/API,
publish/subscribe APIs, topic matching, session transitions, listeners,
TCP/TLS/WebSocket, queues, concrete
storage, or runtime workers. Existing AMQP exclusions remain: other
performative schemas, message sections, protocol/SASL negotiation,
Connection/Session/Link behavior, scheduling, delivery, filesystem
WAL/segments/fsync/recovery, Raft, clustering, management APIs/UI, or AMQP
0-9-1. Do not add Phoenix, Ra, Khepri, Ranch, Bandit, or speculative runtime
dependencies. Do not claim a working MQTT client, broker, or full protocol
compatibility. Future client and broker stages require independent-peer
interoperability evidence in both directions; self-to-self tests alone do
not qualify either role.

## Required verification

Use the repository toolchain declared in `.tool-versions`. From the umbrella
root, run the complete sequence before reporting success:

```bash
mix deps.get
mix format
mix format --check-formatted
mix compile --warnings-as-errors
mix test
mix graviton_mq.check_architecture
mix help xref
mix xref graph --format cycles --fail-above 0
```

Keep compilation at zero warnings. Inspect the complete diff, preserve
unrelated user changes, and do not commit unless the user explicitly asks.
