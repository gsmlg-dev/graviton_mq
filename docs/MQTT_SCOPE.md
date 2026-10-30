# MQTT M0 codec scope

This document records the historical M0 slice and subsequent design. The
2026-10-29 [active Goal](ACTIVE_SCOPE.md) authorizes G0–G7 beyond M0 and
[ADR 0007](adr/0007-independent-network-client-application.md) supersedes the
client-package ownership proposed below. Current implementation and evidence
are tracked in the [capability matrix](CAPABILITIES.md).

G1/G2 now provide separate MQTT5 memory client and broker roles, pure role
transitions, shared operational packet schemas and bounded TCP/TLS shells.
The [client guide](MQTT_CLIENT.md) and [broker guide](MQTT_BROKER.md) describe
their current APIs. G2 specializes broker session ownership as described in
the [implementation plan](superpowers/plans/2026-10-30-mqtt-memory-broker.md).
The remaining text records the historical M0 boundary; its empty-runtime
statements do not describe configured G2 instances.

The operational target is MQTT 5.0 (protocol level 5). The latest Goal also
preserves historical MQTT 3.1.1 CONNECT/CONNACK codec regressions; legacy
runtime requires separate acceptance. MQTT 3.1 is unsupported. The approved first
slice provides a pure packet envelope and CONNECT/CONNACK codec. It provides
no operational client, broker, or network listener. The sixth library application,
`graviton_mq_mqtt`, depends only on core and has no Application callback.
Runtime composes it without starting workers.

## API and values

```elixir
alias GravitonMQ.MQTT.Codec.{Frame, Limits, Packet}

limits = Limits.default()
Frame.decode(bytes, limits)
Frame.encode(frame, limits)
Packet.decode(bytes, :auto, limits)
Packet.decode(bytes, :v5, limits)
Packet.encode(packet, limits)

# Independent MQTT 5.0 CONNECT, Clean Start, keep alive 60, Client ID "c":
Packet.decode(<<0x10, 0x0E, 0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c">>)
```

Limits are optional. Packet decoding defaults to `:auto`, which discovers
CONNECT's explicit protocol level (5 or historical 3.1.1 level 4). CONNACK
requires an explicit `:v5` or `:v3_1_1`; its version is never inferred from its
body. MQTT3.1 remains rejected. New operational schemas require `:v5`, and
the network client accepts only MQTT5. Frame decoding preserves the
packet body as opaque bytes and never invokes packet-schema decoding.

Decoding returns `{:ok, value, rest}`, `{:more, positive_byte_count}`, or
`{:error, %GravitonMQ.MQTT.Codec.Error{}}`. Encoding returns `{:ok, bytes}` or
a structured error. Error classes are `:malformed`, `:unsupported`,
`:limit_exceeded`, and `:invalid_value`. Malformed complete packets do not
consume bytes from a following packet to complete an internal field.

The immutable packet data is:

- `Packet.Connect`: `version`, `client_id`, `clean_start`, `keep_alive`,
  `properties`, `will`, `username`, and `password`;
- `Packet.Connack`: `version`, `session_present`, `reason_code`, and
  `properties`;
- `Packet.Will`: `topic`, `payload`, `qos`, `retain`, and `properties`.

These names use the `GravitonMQ.MQTT` namespace. `clean_start` retains the
MQTT 5.0 Clean Start flag. It implements no session-lifetime policy. Properties
are ordered `{predefined_name, value}` pairs. User Property values are
`{name, value}` UTF-8 pairs; repeated User
Properties and their order survive decoding. Password, Will payload,
Authentication Data, and Correlation Data are binary.

UTF-8 Encoded Strings reject invalid encoding and U+0000 while preserving
Unicode normalization and BOM. Will Topic and Response Topic use Topic Name
validation. MQTT 5 property type, location, singleton cardinality, and declared
value restrictions are checked. Authentication Method/Data remain protocol
data; decoding does not execute authentication. CONNACK Authentication Data
correlation needs future connection context.

## Limits and boundaries

`Limits.default()` bounds the entire packet, including fixed header, to
1,048,576 bytes; each field to 65,535 bytes; and each property section to
256 properties. Limits are validated before encoding or decoding. The common
envelope enforces fixed header rules and the four-byte Remaining Length bound.
Encoding emits minimum-length variable-byte integers. Both opaque envelope
and packet decoding require minimum-length Remaining Length. Packet property
lengths and identifiers also require minimum-length encoding.

All MQTT modules are process-free and independent of clocks, transport,
concrete storage, runtime, and AMQP modules. Compiler xref manifests and parsed
syntax enforce ownership, typespec-only references, and direct Erlang process
and clock calls. The existing AMQP codec behavior remains separate.

## Lifecycle and distribution

Standalone startup still uses `GravitonMQ.Application`, which delegates to
`GravitonMQ.start_link/1`. A host uses `runtime: false` on its `graviton_mq`
dependency and places `{GravitonMQ, name: ..., runtime_supervisor_name: ...}`
in its own supervisor. Both names are exact OTP registrations and must differ
between instances. Both lifecycles create only empty composition boundaries;
multiple empty instances do not prove isolation of operational brokers.

The package scripts now stage seven exact-version archives in dependency order.
Archive smoke checks decode the literal CONNECT above from unpacked packages.
A separate host consumer disables product autostart, starts a host-supervised
instance, and verifies shutdown. These checks do not publish packages.

## Client and broker target; deferred stages

The owner requires our own MQTT 5.0 client and broker, sharing the pure
protocol layer while keeping client and broker state/operation ownership
separate. MQTT behavior is implemented from the standard, using OTP TCP/TLS
infrastructure. Third-party MQTT libraries and external brokers do not replace
either production role; independent implementations are interoperability
test peers.

The [design](superpowers/specs/2026-10-26-mqtt-broker-embedding-design.md)
separates M1 shared packet schemas, M2a pure client and M2b pure broker
semantics, M3a an independent network client and M3b an embedded broker with
QoS 0/1 and TCP/TLS, M4 QoS 2 on both roles, and M5 separately designed
durability/recovery for each role. M0 does not implement those stages.

The `GravitonMQ.MQTTClient` facade now belongs to the independent client
library and starts under a host supervisor without a local broker. It
delegates to `GravitonMQ.Client.MQTT`; `GravitonMQ.MQTT` remains pure.
`GravitonMQ.publish/subscribe` are implemented broker-local operations,
not the network client. Each operational role must pass interoperability
against independent peers as well as our client-to-broker exchange.

In particular, CONNECT decoding does not authenticate clients, assign or
register Client IDs, create sessions, set timers, execute a Will, route
messages, publish, subscribe, persist state, or start listeners. Enhanced
authentication, shared subscriptions, WebSocket, clustering, bridges,
management APIs/UI, and MQTT-SN are outside this slice. Codec evidence
does not establish live-client interoperability, broker compatibility,
production security, throughput, recovery, or durability.

## TODO: QUIC support (not implemented)

Recorded on 2026-10-26 for the MQTT client and broker. These are future
requirements only; no implementation is requested at this time.

- [ ] Reliable transport: MQTT 5.0 over QUIC streams, preserving MQTT packet
  encoding and QoS semantics on a reliable, ordered stream.
- [ ] Realtime state extension: use QUIC DATAGRAM only after explicit peer
  negotiation of the capability and extension version. Specify and version
  this extension independently; it is not a standard MQTT capability.
  DATAGRAM delivery does not inherit MQTT QoS guarantees.

Stream mapping, negotiation, extension format, loss/reordering behavior, and
acceptance tests require a later design. This TODO does not change M0 or the
planned M3a/M3b TCP/TLS acceptance scope. Do not add runtime code, dependencies,
public APIs, or support claims for QUIC as part of recording it.
