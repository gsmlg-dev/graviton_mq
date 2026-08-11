# End, Close, and Error Codec Design

> Status: Completed historical implementation record. The current
> authoritative scope is `AGENTS.md` and `README.md`; this document records the
> design approved and implemented on 2026-08-11.

## Scope

This design names its governing boundary the **post-Milestone-1 End/Close codec
slice**. It is the first bounded codec slice after Milestone 1, not a redefinition
of Milestone 1 and not a claim that all work for a numbered Milestone 2 is
specified. It adds process-free schema encoding and decoding for the AMQP 1.0
`end` and `close` performatives and their shared `error` composite. It builds on
the existing semantic value codec and the Open/Begin performative facade.

The slice does not add Connection or Session transitions, protocol or SASL
negotiation, frame dispatch, message-section parsing, Link behavior, Flow,
Transfer, Disposition, transport, OTP processes, queue behavior, storage, or a
claim of complete AMQP 1.0 compatibility.

The schemas come from the OASIS AMQP 1.0 transport composite definitions:

- `end` has numeric descriptor `0x17`, symbolic descriptor `amqp:end:list`, and
  one optional `error` field;
- `close` has numeric descriptor `0x18`, symbolic descriptor
  `amqp:close:list`, and one optional `error` field; and
- `error` has numeric descriptor `0x1D`, symbolic descriptor
  `amqp:error:list`, and the ordered fields `condition`, `description`, and
  `info`.

Composite fields remain positional. An omitted or explicitly null optional
field normalizes to `nil`, and trailing null fields may be omitted when
encoding.

## Public data model

Add two immutable performative structs:

- `GravitonMQ.AMQP10.Performative.End`
- `GravitonMQ.AMQP10.Performative.Close`

Each struct has one field:

```elixir
defstruct error: nil
```

The field type is `GravitonMQ.AMQP10.Error.t() | nil`. The existing protocol
error struct remains the single public representation of an AMQP error:

```elixir
%GravitonMQ.AMQP10.Error{
  condition: %GravitonMQ.AMQP10.Value{type: :symbol, value: binary()},
  description: %GravitonMQ.AMQP10.Value{type: :string, value: binary()} | nil,
  info: %GravitonMQ.AMQP10.Value{type: :map, value: list()} | nil
}
```

`condition` is mandatory. `description` and `info` use `nil` for an absent or
explicitly null field. The fields retain exact tagged AMQP values; the error's
numeric or symbolic wire descriptor normalizes to the same protocol struct in
the same way that Open and Begin descriptors normalize today.

The codec must not whitelist the standard AMQP, Connection, Session, or Link
error-condition symbols. The AMQP type requirement is a symbol, and extension
conditions must remain representable without project-specific registration.
Interpreting whether an error condition is valid in a particular protocol state
belongs to later transition logic.

Extend `GravitonMQ.AMQP10.Performative.t()` to include End and Close. The pure
codec facade remains unchanged:

```elixir
GravitonMQ.AMQP10.Codec.Performative.decode(bytes, limits)
GravitonMQ.AMQP10.Codec.Performative.encode(performative, limits)
```

No separate public binary codec for protocol errors is introduced. Private
schema helpers inside `Codec.Performative` translate between the generic tagged
described value and `GravitonMQ.AMQP10.Error`. This is the minimum abstraction
needed by End and Close and can be extracted only when a later approved slice
demonstrates another module-level consumer.

## Descriptor dispatch and data flow

Decoding continues to have two explicit layers:

1. `Codec.Value.decode/2` parses one generic AMQP value, enforces configured
   limits, and returns the exact unconsumed suffix.
2. `Codec.Performative` recognizes the top-level descriptor and validates the
   corresponding schema.

The performative decoder accepts numeric and symbolic descriptors for Open,
Begin, End, and Close. End and Close require a list body with at most one
field. Their optional field is either null/absent or a described AMQP `error`
value. A present error accepts its standard numeric or symbolic descriptor and
requires a list body with at most three fields.

The nested error schema is:

1. `condition`: mandatory exact tagged `symbol`;
2. `description`: optional exact tagged `string`; and
3. `info`: optional exact tagged `map` whose keys are tagged `symbol` values.

The `info` values remain limited to the existing bounded semantic value codec.
That subset is `null`, `ushort`, `uint`, `ulong`, `string`, `symbol`, `list`,
ordered `map`, symbol arrays, and recursively composed described values with a
`ulong` or `symbol` descriptor. This slice does not widen `Codec.Value` to
boolean, ubyte, binary, or any other currently unsupported semantic type. An
otherwise valid peer error whose info contains an unsupported value constructor
remains explicitly unsupported at the value-codec boundary.

The `info` map retains input entry order and exact tagged keys and values. Its
keys must be symbols, and duplicate exact tagged keys remain invalid under the
existing ordered-map contract. Canonical encoding preserves the retained entry
order; it does not sort the map.

The caller's existing `Codec.Limits` value applies while decoding the complete
generic described value, including the nested error list and info map. Nesting
depth is threaded through recursive values. `max_compound_items` remains a
per-compound limit checked independently for each list, map, or array; this
slice does not introduce a cumulative item budget. Schema validation adds no
second set of limits.

Frame decoding remains separate. It continues to return an opaque body and
never invokes the performative codec implicitly.

## Canonical encoding

Encoding validates the public struct before constructing tagged AMQP values.
It always emits the standard numeric descriptors:

- End: `0x17`;
- Close: `0x18`; and
- Error: `0x1D`.

End or Close without an error encodes with an empty list. A present error is
encoded as the sole field. The error's mandatory condition is always present;
an absent description is encoded as an interior null when `info` is present;
and absent trailing description or info fields are omitted. The existing value
encoder chooses the canonical constructor widths.

The encoder must not accept a raw described `Value` in the `error` field. It
accepts only `GravitonMQ.AMQP10.Error` or `nil`, ensuring that outbound known
schemas are validated rather than bypassed.

`error` is nested protocol data, not a performative. A top-level `amqp:error:list`
or `0x1D` described value passed to `Codec.Performative.decode/2` remains an
unsupported top-level descriptor. Only an Error value nested in a valid End or
Close field is normalized to `GravitonMQ.AMQP10.Error`.

## Error contract

The public result shapes and codec operations remain unchanged. End, Close,
and nested error schema failures use `:performative_decode` or
`:performative_encode` in `Codec.Error`.

Errors retain the current public `%Codec.Error{operation, class, reason, offset,
details}` shape. This slice adds no fields or result variants. Schema failures
put the performative, field name, positional index, expected type, and actual
semantic type in `details` where those facts apply.

For a known End or Close schema, `details.performative` is always the outer
`:end` or `:close`. Failures inside its Error composite additionally set
`details.nested_schema` to `:error`; `details.field` and `details.index` then
refer to the Error field and its zero-based position. A non-error value in the
outer field reports `field: :error` and `index: 0` without the nested marker.
Errors that occur before a top-level descriptor is recognized do not invent a
performative value.

On decode:

- an unknown, well-formed top-level performative descriptor is `:unsupported`;
- a known End or Close descriptor with a non-list body, too many fields, or a
  present non-error field is `:malformed`;
- an unknown described-value descriptor in the End or Close error position is
  a malformed non-error field, not an unsupported top-level performative;
- a known error descriptor with a non-list body, too many fields, a missing or
  null condition, a wrong exact field type, or a non-symbol info key is
  `:malformed`; and
- incomplete input remains `{:more, positive_integer}`.

On encode, an invalid End, Close, or protocol error struct is
`:invalid_value`. Schema-error details identify the performative, field name,
position, expected type, and actual semantic type where applicable.

Constructor, UTF-8, compound-size, configured-limit, and unsupported-value
errors from `Codec.Value` propagate unchanged. Expected peer input never raises
an exception or becomes a successful partial decode.

## Implementation boundaries

The implementation is limited to the AMQP 1.0 child application plus the
documentation and architecture rules that publish the new boundary. Expected
code changes are:

- add the End and Close struct modules;
- extend the performative union and the existing codec descriptor dispatch;
- add private validation and construction helpers for the AMQP error composite;
- add focused contract, End, Close, and error-schema tests; and
- update `AGENTS.md`, README, AMQP scope, architecture, research notes, and
  roadmap language from the current Open/Begin-only status to this exact bounded
  extension. Milestone 1 remains documented as the completed historical
  Open/Begin boundary; current-status and current-exclusion text advances to the
  post-Milestone-1 End/Close slice.

Do not add a generic schema framework, metaprogrammed composite registry,
transport dependency, runtime worker, connection/session state machine, or
speculative support for Detach, Attach, Flow, Transfer, Disposition, message
sections, or SASL.

The architecture checker continues to reject runtime, storage, socket,
transport, and process facilities from the codec namespace. No child
application dependency changes are required.

## Test design

Tests use independently assembled AMQP wire fixtures rather than relying only
on codec round trips. They cover:

- numeric and symbolic End and Close descriptors;
- empty lists and explicitly null error fields;
- numeric and symbolic nested error descriptors;
- condition-only and full error values;
- encode and decode of a non-standard extension condition symbol, proving that
  the codec does not whitelist registered conditions;
- an info map with symbol keys and bounded supported values;
- exact tagged field identities and exact decoder remainder preservation;
- canonical numeric-descriptor bytes and encode/decode round trips;
- every strict prefix of representative empty and full values;
- non-list bodies, excess fields, missing/null conditions, wrong exact types,
  non-symbol info keys, and non-error performative fields;
- unknown top-level descriptors, a top-level Error descriptor, an unknown
  nested error descriptor, and invalid outbound structs;
- exact schema-error detail assertions for the outer performative, nested Error
  marker, field name, and zero-based index;
- unchanged propagation of value-codec malformed, unsupported, and limit
  errors; and
- architecture enforcement of the process-free codec boundary.

Focused test execution remains within `apps/graviton_mq_amqp10/test/codec/`
during implementation. Before reporting the slice complete, run the complete
verification sequence required by `AGENTS.md`, inspect the entire diff, and run
`git diff --check`.

## Completion boundary

This slice is complete only when:

- End and Close numeric and symbolic forms decode to their dedicated immutable
  protocol structs, and numeric and symbolic Error values nested within them
  decode to `GravitonMQ.AMQP10.Error`;
- canonical encoding emits numeric descriptors and preserves required
  positional nulls;
- malformed, incomplete, unsupported, limit-exceeded, and invalid outbound
  values remain distinguishable;
- existing Open/Begin and value-codec behavior remains unchanged;
- documentation describes the exact new surface and exclusions;
- all focused and repository verification gates pass without warnings; and
- no protocol behavior or later codec surface has been introduced.

Passing these checks does not mean that GravitonMQ can close a live AMQP
connection or end a live Session. It proves only the bounded binary and schema
transformations described here.

## Normative reference

- [OASIS AMQP Version 1.0, Part 2: Transport](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html), sections 2.7.8, 2.7.9, and 2.8.14.
