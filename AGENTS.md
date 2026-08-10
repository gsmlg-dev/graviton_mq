# GravitonMQ repository instructions

These instructions apply to the entire repository.

## Product and clean-room boundaries

- GravitonMQ implements AMQP 1.0. Do not use AMQP 0-9-1 methods, exchanges,
  bindings, channels, `basic.publish`, or `basic.consume` as the protocol
  foundation.
- RabbitMQ is an architectural reference only, for process ownership,
  supervision, connection and session boundaries, queue-type abstraction,
  state machines and effects, storage boundaries, and fault isolation.
- Do not copy, translate, transliterate, mechanically port, or disguise
  RabbitMQ source. Protocol behavior comes from the AMQP 1.0 specification and
  independent tests.
- Do not select or add a project license without an explicit owner decision.

## Umbrella dependency direction

An arrow points from a consumer toward the child application it depends on:

```text
                         graviton_mq_core
                         ▲             ▲
                         │             │
              graviton_mq_storage   graviton_mq_amqp10
                         ▲             ▲
                         └──────┬──────┘
                                │
                      graviton_mq_runtime
                                ▲
                                │
                         graviton_mq
```

- `graviton_mq_core` has no dependency on another GravitonMQ child.
- `graviton_mq_storage` depends only on core.
- `graviton_mq_amqp10` depends only on core.
- `graviton_mq_runtime` may depend on core, storage, and AMQP 1.0.
- `graviton_mq` depends only on runtime.
- Do not introduce dependency cycles. Only `graviton_mq` owns the top-level
  `Application` callback.

The source-level rules are stricter than declarations in `mix.exs`:

- core must not reference AMQP 1.0 modules, concrete storage modules, runtime
  modules, or the public composition application;
- storage must not reference AMQP 1.0 or runtime modules;
- pure AMQP type and codec modules must not reference runtime, storage, TCP,
  sockets, or transport libraries; and
- the application graph must remain acyclic.

Keep `mix graviton_mq.check_architecture` based on compiler xref manifests and
parsed Elixir syntax. Do not replace source dependency analysis with grep.

## Data and ownership rules

- Preserve AMQP semantic type identity. `binary`, `string`, and `symbol`, and
  every signed and unsigned integer type, are distinct values. Compact wire
  constructors such as `smalluint` are encoder choices, not semantic value
  types.
- The bounded performative codec supports only Open and Begin. Decode their
  standard numeric and symbolic descriptors into dedicated immutable structs;
  encode the standard numeric descriptors. Keep every present field as an
  exact tagged AMQP value and use `nil` only for absent or explicitly null
  optional fields.
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
- Do not add fake broker functions that return `:ok`, empty success data, or
  other values implying that an unimplemented operation works.

## Lifecycle and naming

- Standalone and embedded operation use `GravitonMQ.start_link/1` and
  `GravitonMQ.child_spec/1`. `GravitonMQ.Application` starts its default
  instance through the same API.
- Instance `:name` and `:runtime_supervisor_name` options are exact OTP names.
  Do not create atoms dynamically from external strings. Two registered
  instances need distinct names at both levels.
- Supervisors remain empty below the public and runtime
  supervision boundaries. Do not start a listener, queue, storage worker, or
  effect executor merely to occupy a supervision-tree slot.

## Current milestone exclusions

Milestone 1 is limited to process-free raw AMQP 1.0 header recognition, frame
envelope validation, the documented bounded value subset, and the Open/Begin
schema codec above. Do not extend it to protocol or SASL negotiation, other
performative schemas, message-section parsing, Connection/Session/Link
behavior, Flow, Transfer, Disposition, queue transitions or scheduling,
publisher or consumer delivery, TCP/TLS/WebSocket, filesystem
WAL/segments/fsync/recovery, Raft, clustering, Phoenix, management APIs or UI,
MQTT, or AMQP 0-9-1. Do not add Phoenix, Ra, Khepri, Ranch, Bandit, or
speculative runtime dependencies. Do not claim full AMQP 1.0 compatibility.

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
