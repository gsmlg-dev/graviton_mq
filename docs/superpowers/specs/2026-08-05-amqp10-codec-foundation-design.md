# Bounded AMQP 1.0 Codec Foundation Design

> Status: Completed historical implementation record. The current
> authoritative scope is `AGENTS.md` and `README.md`; this document records the
> design as approved at the time.

## Status and scope

This design defined the initial process-free Milestone 1 foundation:
protocol-header recognition, AMQP frame-envelope validation, and semantic
value encoding and decoding for the types needed by Open and Begin. That
initial slice deliberately omitted performative-schema parsing. The separately
approved 2026-08-08 slice subsequently added the bounded Open/Begin schema
codec without changing this foundation's process-free boundary.

The implementation must not add TCP or other transports, OTP protocol
processes, SASL negotiation, protocol state transitions, message-section
parsing, queue behavior, storage behavior, or a claim of complete AMQP 1.0
compatibility.

## Public boundaries

The codec is split into small pure modules under `GravitonMQ.AMQP10.Codec`:

- `ProtocolHeader.recognize/1` consumes a complete eight-byte AMQP protocol
  header and returns the untouched input remainder. It recognizes only raw
  AMQP 1.0 (`protocol-id = 0`, version `1.0.0`). SASL and other protocol IDs or
  versions are reported as unsupported; no negotiation occurs.
- `Frame.decode/2` validates one AMQP frame envelope and returns its declared
  size, data-offset words, channel, exact extended-header bytes, exact body
  bytes, and untouched input remainder. The frame layer does not decode a
  performative or payload.
- `Value.decode/2` consumes one supported AMQP value and returns the existing
  Milestone 0 `GravitonMQ.AMQP10.Value` representation plus the exact input
  remainder.
- `Value.encode/2` encodes one supported semantic value. Compact constructor
  choice is private to the encoder and never changes semantic value identity.

A shared `Codec.Error` struct describes expected failures. A shared
`Codec.Limits` value bounds frame size, variable-width value size, compound
item count, and nesting depth without creating process-owned state.

The default limits are 16,777,216 bytes for a frame, 16,777,216 bytes for one
variable-width value, 65,536 members in one compound value, and 32 nested
compound or described values. Callers may supply stricter positive limits;
larger limits require an explicit caller choice. These are implementation
safety limits, not negotiated protocol state or a compatibility promise.

## Result and error contract

Decode operations return one of:

```elixir
{:ok, decoded, rest}
{:more, minimum_additional_bytes}
{:error, %GravitonMQ.AMQP10.Codec.Error{}}
```

`{:more, n}` means the caller must retain and retry the original input after
adding at least `n` bytes. A truncated prefix is not malformed. Structured
errors identify the operation, error class, stable reason, byte offset where
available, and bounded diagnostic details. Error classes distinguish malformed
wire data, unsupported protocol or type surface, configured-limit violations,
and invalid semantic values supplied for encoding. Expected hostile input must
not raise.

## Supported semantic values

The supported subset is deliberately limited to:

- `null`;
- `ushort`, `uint`, and `ulong`;
- `string` and `symbol`;
- `list` and ordered `map`;
- arrays whose explicit element type is `symbol`; and
- described values whose descriptor is a `ulong` or `symbol` and whose value
  is in this supported subset.

The decoder accepts every standard compact constructor for these semantic
types, such as `uint0`, `smalluint`, and full-width `uint`, but all decode to
the same `%Value{type: :uint}` identity. The encoder chooses a canonical compact
constructor independently.

Signed integers, boolean, ubyte, binary, floating-point and decimal values,
char, timestamp, UUID, general arrays, and other Milestone 0 semantic types
remain representable in memory but return an explicit unsupported error at the
Milestone 1 codec boundary. `Attach` and later performatives will widen the
codec only in later, separately reviewed work.

Open and Begin `properties` values are unrestricted in the complete protocol.
This bounded foundation can encode or decode such map values only when they
belong to the supported subset; all other property values are explicitly
unsupported.

## Validation and preservation rules

- Strings must contain valid UTF-8; symbols must contain only seven-bit ASCII.
- Compound declared sizes and counts must agree exactly. A compound value
  cannot borrow bytes from the following top-level input.
- Map item counts must be even, exact typed keys must be unique, entry order is
  retained, and string and symbol keys remain distinct.
- Symbol arrays carry one shared symbol constructor, omit constructors from
  individual elements, and retain element-type identity even when empty.
- Unknown extension described values remain lossless when their descriptor and
  value use the supported subset. Reserved descriptor types are unsupported,
  not malformed.
- Frame size must be at least eight bytes, `DOFF` must be at least two, and
  `DOFF * 4` must not exceed the declared frame size. Size limits are checked
  before waiting for an attacker-declared body.
- AMQP frame type zero is supported. SASL frame type one and unknown frame
  types are explicitly unsupported. An empty AMQP frame body remains valid as
  a heartbeat.
- Extended-header bytes, frame-body bytes, decoder remainders, and existing
  opaque encoded message content are never rewritten by envelope validation.

## Tests and architecture enforcement

Implementation follows test-first slices for headers, frame envelopes,
primitive values, and compound values. Tests use independently assembled
fixtures derived from the OASIS AMQP 1.0 type and transport definitions rather
than relying only on encoder/decoder round trips. They cover valid exact wire
vectors, every strict prefix of representative values, alternate constructors,
malformed size/count fields, unsupported codes and protocols, UTF-8 and ASCII
validation, duplicate typed map keys, limits, nesting, exact remainders, and
opaque frame-body preservation.

The source architecture checker will additionally reject process and transport
dependencies from the codec namespace. Codec modules must not use
`GenServer`, `Supervisor`, `Agent`, `Task`, `Process`, registries, sockets, or
runtime/storage modules.

## Completion boundary

Milestone 1 is complete when the bounded API is documented, specification-
derived tests pass, compilation has no warnings, architecture and dependency
cycle checks pass, and no runtime behavior or later AMQP feature surface has
been introduced.

## Normative references

- [AMQP Version 1.0, Part 1: Types](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html)
- [AMQP Version 1.0, Part 2: Transport](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html)
