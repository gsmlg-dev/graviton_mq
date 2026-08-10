# Open and Begin Performative Codec Design

> Status: Completed historical implementation record. The current
> authoritative scope is `AGENTS.md` and `README.md`; this document records the
> design as approved at the time.

## Scope

This slice adds a bounded, process-free schema codec for the AMQP 1.0 `open`
and `begin` performatives. It builds on the existing semantic value codec and
does not add protocol state, negotiation, transport, message parsing, queue
behavior, or storage behavior.

The codec is derived from the AMQP 1.0 transport composite definitions:

- `open` has numeric descriptor `0x10`, symbolic descriptor
  `amqp:open:list`, and ten ordered fields;
- `begin` has numeric descriptor `0x11`, symbolic descriptor
  `amqp:begin:list`, and eight ordered fields; and
- composite fields are positional, interior omissions are encoded as `null`,
  and trailing `null` fields may be omitted.

## Public API and data model

Add immutable protocol data under `GravitonMQ.AMQP10.Performative`:

- `GravitonMQ.AMQP10.Performative.Open`
- `GravitonMQ.AMQP10.Performative.Begin`

Each struct names the fields from the specification in field order. Field
values remain exact tagged `GravitonMQ.AMQP10.Value` values. `nil` represents
an absent or explicitly null optional field. Ordered property maps remain
tagged AMQP maps rather than Elixir maps, and multiple symbol fields retain
whether the peer supplied one symbol or a symbol array.

The pure codec facade is:

```elixir
GravitonMQ.AMQP10.Codec.Performative.decode(bytes, limits)
GravitonMQ.AMQP10.Codec.Performative.encode(open_or_begin, limits)
```

Decoding returns `{:ok, performative, rest}`, `{:more, needed}`, or a
structured `Codec.Error`. Encoding returns `{:ok, bytes}` or a structured
error. The untouched remainder after exactly one described value is returned
to the caller. Frame decoding remains a separate operation and frame bodies
remain opaque unless a caller explicitly invokes this codec.

## Descriptor dispatch

The decoder delegates binary constructor parsing to `Codec.Value`, then
recognizes either the numeric or symbolic standard descriptor for Open and
Begin. Both descriptor forms normalize to the same struct. The encoder uses
the standard numeric descriptor and the existing value encoder's canonical
wire-width choices.

An unknown, well-formed described-value descriptor is `:unsupported`. A known
descriptor with a non-list body is malformed. The generic value codec remains
lossless for unknown described values; only this bounded performative facade
rejects descriptors outside Open and Begin.

## Field validation

Open validates these positions:

1. `container_id`: mandatory `string`
2. `hostname`: optional `string`
3. `max_frame_size`: optional/default `uint`, with a minimum of 512
4. `channel_max`: optional/default `ushort`
5. `idle_time_out`: optional `uint`
6. `outgoing_locales`: optional multiple `symbol`
7. `incoming_locales`: optional multiple `symbol`
8. `offered_capabilities`: optional multiple `symbol`
9. `desired_capabilities`: optional multiple `symbol`
10. `properties`: optional `map` whose keys are `symbol` values

Begin validates these positions:

1. `remote_channel`: optional `ushort`
2. `next_outgoing_id`: mandatory `uint`
3. `incoming_window`: mandatory `uint`
4. `outgoing_window`: mandatory `uint`
5. `handle_max`: optional/default `uint`
6. `offered_capabilities`: optional multiple `symbol`
7. `desired_capabilities`: optional multiple `symbol`
8. `properties`: optional `map` whose keys are `symbol` values

For a multiple field, `nil`, one tagged symbol, or a tagged array whose
element type is `:symbol` is valid. Empty symbol arrays remain exact array
values even though the specification assigns them absence semantics.

Missing or null mandatory fields, exact-type mismatches, invalid property
keys, too many fields, and an Open `max_frame_size` below 512 are malformed on
decode. Invalid outbound structs are `:invalid_value`. Context-dependent
rules, especially whether Begin's `remote_channel` is required for a
particular session transition, are deliberately deferred to later pure
protocol-state work.

## Defaults and canonical encoding

Decoding materializes the specification defaults:

- Open `max_frame_size`: tagged `uint(4_294_967_295)`
- Open `channel_max`: tagged `ushort(65_535)`
- Begin `handle_max`: tagged `uint(4_294_967_295)`

The encoder treats those default values as absent, inserts explicit nulls only
when a later field requires its positional slot, and removes trailing nulls.
It never removes an interior placeholder. This gives one canonical encoding
without introducing compact wire constructors into the semantic data model.

## Error boundary

Add `:performative_decode` and `:performative_encode` codec operations.
Schema errors use those operations. Constructor, UTF-8, compound-size,
resource-limit, and unsupported-value errors from `Codec.Value` propagate
unchanged so callers can distinguish a wire-value failure from a
performative-schema failure.

No exception represents malformed peer input. Incomplete prefixes continue to
return `{:more, positive_integer}`.

## Verification boundary

Specification-derived tests cover numeric and symbolic descriptors, exact
field order and types, defaults, trailing-null normalization, mandatory
fields, symbol arrays, symbol-keyed property maps, exact remainder
preservation, incomplete prefixes, malformed schema values, unsupported
descriptors, and invalid outbound structs.

Architecture checks continue to enforce that codec modules do not reference
runtime, storage, TCP/socket, or process facilities. The slice does not claim
that Open or Begin protocol behavior is operational or that GravitonMQ has a
complete AMQP 1.0 codec.
