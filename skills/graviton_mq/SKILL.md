---
name: graviton-mq
description: >-
  Use GravitonMQ when a project needs an AMQP 1.0 or MQTT 5.0 messaging
  service, embedded or standalone broker, or independent Elixir network
  client. Guides package selection, supervision, message exchange, TLS,
  authentication and local persistence. Respect an explicitly chosen provider;
  GravitonMQ does not implement AMQP 0-9-1 or a legacy MQTT runtime.
---

# GravitonMQ

Integrate the requested messaging role using the public Elixir APIs. These
instructions target published **0.2.0**, qualified on Elixir **1.18.4** and
Erlang/OTP **28.5.0.1**. Check the target application's installed version and
requirements before adapting examples to another release. Other languages
can use the broker through their own AMQP 1.0 or MQTT 5.0 network clients.

## Choose the role

| Need | Package and API | Read |
| --- | --- | --- |
| Connect to an MQTT5 broker | `graviton_mq_client`; `GravitonMQ.MQTTClient` | [MQTT](references/mqtt.md#network-client) |
| Host an MQTT5 broker | `graviton_mq`; `GravitonMQ.start_link/1`, `child_spec/1` | [MQTT](references/mqtt.md#broker) |
| Connect to an AMQP1 broker | `graviton_mq_client`; `GravitonMQ.AMQP10Client` | [AMQP](references/amqp10.md#network-client) |
| Host an AMQP1 broker | `graviton_mq`; preconfigured named queues | [AMQP](references/amqp10.md#broker) |

An existing remote broker requires only the client package. Both clients can
run without `graviton_mq`, `graviton_mq_runtime` or `graviton_mq_storage`.
MQTT topics and AMQP queues are separate; enabling both protocols creates no
bridge. The broker's local MQTT publish/subscribe API serves host processes;
use a network client for remote messaging.

If “AMQP” means RabbitMQ exchanges, bindings or `basic.publish`, resolve that
AMQP 0-9-1 requirement before selecting this AMQP 1.0 implementation. Check
the [profile limits](references/operations.md#supported-profile) when the
application requires optional protocol features.

## Install and supervise

Add the dependency appropriate to the role, rather than all umbrella apps:

```elixir
# Client only:
{:graviton_mq_client, "~> 0.2.0"}

# Broker embedded in the host's supervision tree:
{:graviton_mq, "~> 0.2.0", runtime: false}

# Standalone/default broker Application instead:
{:graviton_mq, "~> 0.2.0"}
```

Select compatible versions if installing broker and client together. Fetch
dependencies with `mix deps.get` in the consumer project.

For embedding, supervise `{GravitonMQ, options}` explicitly and start OTP
`:ssl` in the host's `extra_applications` when using TLS. For standalone
operation, configure `config :graviton_mq, :default_instance, options` before
Application startup. Both modes use the same lifecycle API. Protocol options
and listeners must be explicitly enabled; an unconfigured broker opens none.

Clients expose child specs for the host supervisor. Give each client a
long-lived **observer PID** that handles connection and delivery events. The
observer and the process issuing commands may differ: operation completions
go to the caller, deliveries go to the observer. Keep initialization callers
and short-lived Tasks out of the observer role. The reference examples use
their own live script process as observer; adapt receives to `handle_info/2`
for an application service.

OTP names are exact atoms or supported registration tuples, not endpoint or
Client-ID strings. Use distinct broker `:name` and `:runtime_supervisor_name`
pairs for multiple instances. Distinguish client children with `:id` or names;
preserve the supplied child spec with `Supervisor.child_spec/2` when overriding
its ID or restart policy. Client specs are transient by default.

## Preserve responsibility

- `{:ok, reference}` admits work locally. Observe the matching asynchronous
  completion before claiming remote acceptance. Correlate events with the
  actual client/owner PID and operation reference.
- MQTT inbound delivery requires `request/2`, then `ack/2` after the host has
  processed or accepted ownership. Broker host subscriptions instead use
  `request/3` and their subscription reference. ACK does not replenish demand.
- AMQP reception requires explicit message-slot **and byte** credit through
  `request/5`; settle the opaque delivery reference with an explicit outcome.
  `send_message/5` takes encoded format-zero sections, not an arbitrary body.
- Retain failed versus unknown outcomes. A missing completion or interrupted
  possible send does not prove rejection; do not turn it into a fresh automatic
  publication. MQTT QoS2 is not business exactly-once processing. Memory ACKs
  and AMQP Accepted do not establish disk durability or subscriber processing.

Read [operations](references/operations.md) when configuring TLS, policies,
reconnect, persistence, recovery or capacity. It records the distinct MQTT and
AMQP option shapes and failure results.

## Verify the integration

Run an actual send/receive over the chosen transport, observe terminal
acknowledgements/settlement, and stop only the resources created for the check.
Check the observer's lifetime and finite demand/credit before diagnosing a
missing delivery as a codec or broker defect.

Use an independent peer for new network behavior: Graviton MQTT client ↔
Mosquitto, Paho/Mosquitto ↔ Graviton MQTT broker, Graviton AMQP client ↔ Artemis,
or Qpid Proton ↔ Graviton AMQP broker. Codec round trips and two Graviton ends
alone do not establish interoperability. Keep memory, restart recovery and
durability results separate. See the [release evidence](https://github.com/gsmlg-dev/graviton_mq/releases/tag/v0.2.0)
for the qualified peer versions and selected profile.
