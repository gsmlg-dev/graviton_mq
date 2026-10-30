# ADR 0007: Independently installable network clients

- Status: Accepted under the owner-authorized G0–G7 Goal
- Date: 2026-10-29
- Amends: ADR 0001 application count and the MQTT embedding design's proposed client ownership

## Context

AMQP 1.0 and MQTT 5.0 each need a native network client and broker. A client
must be independently installed, started, and supervised without pulling in
broker storage, broker runtime, or the public broker Application. A host-local
publish/subscribe call cannot implement this network role. The original MQTT
design placed both running roles in `graviton_mq_runtime`, which would make a
client consumer install broker dependencies even without starting a broker.

## Decision

Add the library application `graviton_mq_client`. Its only GravitonMQ
dependencies are `graviton_mq_core`, `graviton_mq_mqtt`, and
`graviton_mq_amqp10`. It has no Application callback or implicit connections.
Hosts explicitly supervise each client through `start_link/1` and
`child_spec/1`. Client identities and exact OTP names are host supplied; no
atom is derived from endpoint strings, topics, Client IDs, or link names.

```text
graviton_mq_client -> graviton_mq_mqtt   -> graviton_mq_core
graviton_mq_client -> graviton_mq_amqp10 -> graviton_mq_core
graviton_mq_client -> graviton_mq_core

graviton_mq -> graviton_mq_runtime -> core / storage / amqp10 / mqtt
```

Broker applications do not depend on the client. Client application code must
not reference broker runtime, physical storage, or composition modules, even
through a typespec or an undeclared call. Compiler xref manifests and parsed
syntax retain source enforcement, namespace ownership, and cycle checks.

| Owner | Namespace and responsibility |
| --- | --- |
| `graviton_mq_mqtt` | `GravitonMQ.MQTT`: shared codecs plus separate pure client and broker machines |
| `graviton_mq_amqp10` | `GravitonMQ.AMQP10`: shared wire types and pure endpoint/session/link transitions |
| `graviton_mq_client` | `GravitonMQ.Client`, `GravitonMQ.MQTTClient`, `GravitonMQ.AMQP10Client`: facades and running outgoing owners |
| `graviton_mq_runtime` | `GravitonMQ.Runtime`: running broker owners, listeners, queues, effects, auth integration |
| `graviton_mq` | Broker lifecycle and host API; sole product Application callback |

Pure machines receive explicit events and elapsed-time facts and return
`{new_state, effects}`. Client running owners perform OTP TCP/TLS, scheduling,
host monitoring, and I/O completion. Connection generations fence old sockets
and timer events. Broker and client share genuinely identical validation and
wire logic; their role semantics, ownership, and policies remain distinct.

Start with bounded MQTT5 QoS0/1. Return local admission separately from peer
PUBACK/SUBACK/UNSUBACK completion. QoS0 socket acceptance is not remote receipt.
Timeout after a possible send is an unknown outcome; do not silently retry it
as a fresh host operation. QoS1 retransmission happens at valid session
resumption, not periodically on a healthy connection. Stop/intentional
disconnect cancels reconnect. Memory session state survives only while its
owner lives; G6 supplies independently qualified persistent recovery.

## Consequences and verification

The umbrella has seven applications once the client is implemented. Update
dependency fixtures, source ownership, architecture regressions, staging and
archive order together. No empty client worker or successful stub is introduced
to satisfy the graph. Only functioning roles are exposed publicly.

Build every archive and compile a clean client consumer whose dependency
closure contains neither `graviton_mq`, `graviton_mq_runtime`, nor
`graviton_mq_storage`. Start the client under a host supervisor and exchange
real packets with fixed-version Mosquitto; later do the equivalent against
Artemis. Verify broker and combined consumers separately. These installation
checks and independent peers are required in addition to pure round trips.

Historical M0/AMQP evidence and ADRs remain valid at their original scope.
This ADR authorizes architecture, not a claim that any network role is
already implemented. Status and evidence live in `docs/CAPABILITIES.md`.
