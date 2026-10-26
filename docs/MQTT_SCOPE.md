# MQTT M0 codec scope

The approved first MQTT slice provides a pure MQTT 3.1.1 (protocol level 4)
and MQTT 5.0 (level 5) packet envelope and CONNECT/CONNACK codec. It provides
no operational broker or network listener. The sixth library application,
`graviton_mq_mqtt`, depends only on core and has no Application callback.
Runtime composes it without starting workers.

## API and values

```elixir
alias GravitonMQ.MQTT.Codec.{Frame, Limits, Packet}

limits = Limits.default()
Frame.decode(bytes, limits)
Frame.encode(frame, limits)
Packet.decode(bytes, :auto, limits)
Packet.decode(bytes, :v3_1_1, limits)
Packet.decode(bytes, :v5, limits)
Packet.encode(packet, limits)

# Independent MQTT 3.1.1 CONNECT, clean session, keep alive 60, Client ID "c":
Packet.decode(<<0x10, 0x0D, 0, 4, "MQTT", 4, 2, 0, 60, 0, 1, "c">>)
```

Limits are optional. Packet decoding defaults to `:auto`, which discovers
CONNECT's explicit protocol level. CONNACK requires an explicit version;
version context is never inferred from its body. An explicit version that
disagrees with CONNECT's level fails. Frame decoding preserves the packet body
as opaque bytes and never invokes packet-schema decoding.

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
wire clean flag; in MQTT 3.1.1 it denotes Clean Session. It implements no
session-lifetime policy. Properties are ordered `{predefined_name, value}`
pairs. User Property values are `{name, value}` UTF-8 pairs; repeated User
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
Encoding emits minimum
length variable-byte integers; MQTT 5 decoding requires minimum length
Remaining Length and property encodings. MQTT 3.1.1 retains its distinct
Remaining Length rule.

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

The package scripts stage six exact-version archives in dependency order.
Archive smoke checks decode the literal CONNECT above from unpacked packages.
A separate host consumer disables product autostart, starts a host-supervised
instance, and verifies shutdown. These checks do not publish packages.

## Deferred stages

The [approved design](superpowers/specs/2026-10-26-mqtt-broker-embedding-design.md)
separates M1 packet schemas, M2 pure broker semantics, M3 an embedded MQTT 5
broker with QoS 0/1 and TCP/TLS, M4 QoS 2 and operational MQTT 3.1.1, and M5
separately designed durability. M0 does not implement any of those stages.

In particular, CONNECT decoding does not authenticate clients, assign or
register Client IDs, create sessions, set timers, execute a Will, route
messages, publish, subscribe, persist state, or start listeners. Enhanced
authentication, shared subscriptions, WebSocket, clustering, bridges,
management APIs/UI, MQTT-SN, and MQTT 3.1 are outside this slice. Codec evidence
does not establish live-client interoperability, broker compatibility,
production security, throughput, recovery, or durability.
