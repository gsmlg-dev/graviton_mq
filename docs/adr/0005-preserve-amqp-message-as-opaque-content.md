# ADR 0005: Preserve AMQP messages as opaque encoded content

- Status: Accepted
- Date: 2026-07-14

## Context

An AMQP 1.0 message can contain delivery annotations, message annotations,
properties, application properties, a footer, one of several body section
kinds, multiple body sections, exact nested AMQP semantic values, and unknown
or extension described values. The Transfer also carries a `message-format`
value that identifies how its message bytes are interpreted.

Decoding a message into a convenient broker struct and later reconstructing
it would risk changing unknown values, type identity, ordering, section
boundaries, or body shape. A protocol-independent core also must not import
AMQP message-section structs. Conversely, routing, expiration, policy,
durability, and management cannot require reparsing every opaque message for
every decision.

## Decision

The original encoded AMQP 1.0 message content is the authoritative message of
record. `GravitonMQ.Core.Message` contains:

- a stable core `MessageId`;
- `encoded_content`, the opaque original message bytes;
- a protocol-neutral `Message.Format` with a binary family identifier and the
  non-negative transfer message-format value;
- a derived `Message.Index` containing only fields needed for broker routing,
  policy, expiration, durability, and management; and
- a durability flag.

The AMQP adapter owns parsing and recognizes the AMQP message format. Core
does not import AMQP value, section, or performative structs. `encoded_content`
remains authoritative even when an index is present; `body: term()` is not an
alternative message of record.

Forwarding can therefore retain, without reconstruction or rewriting:

- unknown and extension described values;
- delivery annotations where the applicable transfer path retains them;
- message annotations;
- properties and application properties;
- footer sections;
- body section kind and multiple body sections;
- exact nested AMQP value identity; and
- the Transfer message-format value.

The index is derived and protocol-neutral. It may be rebuilt from the original
content by the appropriate adapter, but it must not silently become a complete
second representation of the message. Protocol-specific settlement mutations
that must cross core use `Message.Mutation`, which stores a binary format
identifier and opaque encoded bytes rather than an AMQP struct.

This ADR specifies preservation and ownership only. It does not implement
message parsing, validation, indexing, encoding, or forwarding.

## Consequences

Known and unknown AMQP content can survive store-and-forward paths without a
future broker needing to perfectly reconstruct the message. Core remains
protocol-independent, and broker policy can use a deliberately small parsed
index.

Opaque bytes may duplicate some data held in the index, and policy changes may
require rebuilding the index. Content mutation will require an explicit AMQP
adapter operation and a clear decision about whether new encoded content
supersedes the original. Management views cannot assume that every section is
present in the index. These costs are preferable to silent loss or rewriting
of protocol data.
