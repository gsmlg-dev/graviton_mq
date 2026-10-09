# Boolean and Detach codec slice

Status: completed after the owner's `LGTM`, independent design, specification,
and code-quality reviews, and the full repository verification sequence.
All 208 tests pass under the pinned toolchain, with zero warnings and no
dependency cycles. This slice follows the completed End/Close/Error codec
without redefining Milestone 1. The initial handoff left the work uncommitted.
On 2026-10-10, the owner authorized committing, merging into `main`, cleaning
up the worktree, and pushing to the remote.

## Goal and boundary

Add Boolean scalar wire support and the AMQP 1.0 Detach schema to the existing
pure codec. Retain exact tagged values, explicit errors, incomplete-input
results, exact remainders, caller-supplied limits, and opaque frame bodies.

Boolean is added to the shared bounded value subset. It is consequently allowed
recursively inside lists, maps, described values, Open/Begin properties, and
nested Error info. This expansion is part of the approved slice; no separate
per-schema value restriction is introduced. Arrays remain symbol-only, and
descriptors remain tagged `ulong` or `symbol`.

Only Detach is added to the performative facade. Attach, Flow, Transfer,
Disposition, Source/Target schemas, message sections, protocol negotiation,
SASL, Connection/Session/Link transitions, networking, OTP protocol processes,
queues, storage, and new runtime dependencies remain future work. Decoding
Detach does not detach a live Link or interpret its handle in Session state.

## Boolean scalar values

Use the existing `GravitonMQ.AMQP10.Value.boolean/1` semantic constructor.
All wire forms normalize to `%Value{type: :boolean, value: true | false}`:

| Wire bytes | Semantic value |
| --- | --- |
| `41` | tagged Boolean true |
| `42` | tagged Boolean false |
| `56 00` | tagged Boolean false |
| `56 01` | tagged Boolean true |

Canonical encoding chooses the width-zero constructors `41` and `42`. These
are wire choices, not additional semantic types. `56` without its payload
returns `{:more, 1}`. Payload octets `02` through `FF` return a structured
`:value_decode`, `:malformed`, `:invalid_boolean` error with offset `1` and
`details: %{value: octet}`. Malformed tagged outbound Booleans use the existing
`:value_encode`, `:invalid_value`, `:invalid_semantic_value` result rather than
raising or coercing integers, atoms, or strings to truth values.

Existing limit validation runs before decoding and encoding. Boolean scalars
have no unbounded payload; recursive compound size, count, and depth limits
continue to apply. Boolean arrays are outside this slice.

## Detach data and schema

Add `GravitonMQ.AMQP10.Performative.Detach` as an immutable data struct in the
AMQP application, and include it in `Performative.t()` and the facade specs.
Its fields are exactly these three specification positions:

| Index | Field | Tagged type | Presence/default |
| --- | --- | --- | --- |
| 0 | `handle` | `uint` | mandatory |
| 1 | `closed` | `boolean` | defaults to tagged false |
| 2 | `error` | `GravitonMQ.AMQP10.Error` | optional, normalizes to `nil` |

The struct uses `nil` for unset fields, consistent with the Open/Begin models.
The decoder materializes omitted or explicitly null `closed` as
`Value.boolean(false)`. The encoder also accepts omitted/defaulted `closed`
and normalizes it through the schema. Required `handle` cannot be missing or
null; `ushort`, `ulong`, and bare integers are not interchangeable with `uint`.
The existing value encoder checks malformed tagged scalar payloads and the
full uint range. Handle allocation and negotiated handle limits are protocol
context, outside this codec.

Decode the standard numeric descriptor `0x16` and symbolic descriptor
`amqp:detach:list`, including supported compact and full-width encodings of the
numeric descriptor. Encode the numeric descriptor. The body must be a list
with no more than three fields. Defaults may be omitted canonically; preserve
an interior null when `closed` is false and the later `error` is present.

Independent minimal wire examples are:

```text
00 53 16 C0 02 01 43       # handle=0, closed defaults false
00 53 16 C0 03 02 43 41    # handle=0, closed=true
00 53 16 C0 0C 03 43 40 00 53 1D C0 04 01 A3 01 78
                          # handle=0, closed defaults false, error condition="x"
```

These fixtures are assembled from the specification, independently of the
encoder. Symbolic descriptor inputs canonically re-encode with the numeric
descriptor. Explicit false (`42` or `56 00`) and null in `closed` normalize to
the same default and may disappear from the canonical wire representation.

## Reuse of nested Error

Keep Error as nested protocol data. It retains its mandatory tagged symbol
condition, optional tagged string description, and optional ordered symbol-keyed
info map. Accept numeric and symbolic Error descriptors; emit the numeric one.
Keep extension condition symbols, entry order, interior nulls, and the bounded
value subset plus Boolean. Top-level Error remains unsupported.

The existing private Error helpers may be renamed and parameterized with the
outer field index: `0` for End/Close and `2` for Detach. Do not duplicate Error
validation or introduce a new public binary API. Wrong outer Error types,
wrong descriptors, and invalid Error bodies must report the correct parent
performative and outer field index. Error's internal condition, description,
and info indexes remain `0`, `1`, and `2` with `nested_schema: :error`.

Detach schema failures identify `performative: :detach`, with field/index/type
details where applicable. Inbound schema errors are `:malformed`; outbound
schema errors are `:invalid_value`. Preserve value-layer errors unchanged,
including their operation, class, reason, offset, and details. Unsupported
descriptors and primitives remain explicitly unsupported.

## File scope

Production changes are limited to:

- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/value.ex`
- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/performative.ex`
- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative.ex`
- new `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative/detach.ex`

Tests are limited to AMQP codec tests and `boundaries_test.exs`. Documentation
changes are limited to this design and its plan, `AGENTS.md`, `README.md`, and
the existing AMQP scope, architecture, roadmap, and research documents. Preserve
historical Milestone 1 and End/Close design boundaries; describe the new slice
separately. No application graph, toolchain, dependencies, or public broker
API changes are required.

## Acceptance and verification

- Independent Boolean fixtures cover all constructors, every strict prefix,
  exact suffixes, all invalid payload octets, canonical bytes, and invalid
  outbound semantic values.
- Boolean is exercised recursively in lists/maps/described values and in
  Open/Begin properties and End/Close/Detach Error info. Existing unsupported
  fixtures move to a still-unsupported primitive, retaining error propagation.
- Detach fixtures cover numeric/symbolic descriptors, list8/list32 forms,
  required handle, false default, exact types, field counts, canonical bytes,
  null holes, handle boundaries, and optional nested Error.
- Error diagnostics retain End/Close indexes and correctly use Detach's outer
  index `2`, with unchanged internal indexes and value errors.
- Every strict performative prefix is incomplete; suffixes remain byte-exact.
  Existing size/count/depth limits apply on both encode and decode.
- Frames remain opaque, all codec modules remain process-free, and the five
  application dependency graph remains acyclic.
- All scoped tests and the repository-required sequence pass under the pinned
  Elixir 1.18.4 / Erlang 28.5.0.1 pair, without warnings.
- Inspect tracked and untracked changes and complete independent specification
  and code-quality reviews. Leave new work uncommitted unless the owner asks.

## Normative references

- [OASIS AMQP 1.0 Part 1: Types](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html), sections 1.4 (composites) and 1.6.2 (Boolean).
- [OASIS AMQP 1.0 Part 2: Transport](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html), sections 2.7.7 (Detach), 2.8.4 (handle), 2.8.13 (fields), and 2.8.14 (Error).
