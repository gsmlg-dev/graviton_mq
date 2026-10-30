# G2 MQTT5 memory broker implementation

This executes the approved [embedding design](../specs/2026-10-26-mqtt-broker-embedding-design.md)
under the [active Goal](../../ACTIVE_SCOPE.md), after the
[G0/G1 qualification](../../qualification/2026-10-29-g0-g1.md). The bounded G2
memory-profile exit is qualified in the
[2026-10-30 evidence](../../qualification/2026-10-30-g2.md).
No branch/worktree or publication is authorized.

## Ownership specialization

Use one supervised `GravitonMQ.Runtime.MQTT.Broker` owner per instance around
the pure `GravitonMQ.MQTT.Broker` aggregate. Logical sessions remain distinct
Client-ID-indexed records independent of socket lifetimes. This specializes
the design's illustrative session-process topology so publication admission,
all recipient obligations and retained mutation can commit in one transition.
It does not introduce a process per subscription, topic or publication.

Owner failure loses every session, retained publication and pending delivery
in that memory instance. Ordinary connection failure preserves valid session
state. Supervision must tear down old connections/writers before a fresh
owner serves traffic; listener/connection failure must preserve the owner.
The serial owner bounds throughput; G7 measures resource and performance limits.
This profile makes no restart-recovery or disk-durability claim.

Pure transitions receive explicit packet, original-byte, elapsed-time,
authentication/authorization-result, write-result and host-lifetime events.
Runtime owns sockets, PIDs/monitors, timers and host callbacks. Pure state uses
bounded counters/opaque supplied IDs and returns `{new_state, effects}`.

## Atomic responsibility and authorization

Prepare a finite routing plan, authorize publisher/filter scope and actual
recipient topics outside the owner, then commit retained changes and all
eligible pending deliveries together. Successful PUBACK/host completion
follows this commit. Capacity or callback infrastructure failure leaves no
partial route/retained mutation. Subscription removal, takeover and stale
asynchronous results are fenced by operation/lifetime/generation identities.
Commit order must preserve each publisher's message order.

Network authentication precedes Client-ID takeover; a different principal
cannot resume or destroy a retained session by knowing its Client ID.
Host calls use the explicitly configured host principal. Anonymous network
policy is opt-in. Callback denial/crash/invalid return/timeout/concurrency
exhaustion fails closed without leaking callback inputs or failure data.
Authentication tasks supervise the actual callback; separate monitors own
cancellation and deadlines even when callbacks trap exits.

Retained replay follows RH=0/1/2. Live routing applies No Local, granted QoS
and RAP without silently losing an accepted subscription's semantics. Expire
unsent and retained content using explicit time, reduce remaining expiry on
transmission, and keep sent QoS1 responsibility until terminal settlement.
Will delay, session end, reconnect cancellation, graceful DISCONNECT and
abnormal closure are distinct transitions. A due Will remains owned under
pressure; no silent success/drop is permitted.

## Public contract and finite boundaries

The existing `GravitonMQ.start_link/1`/`child_spec/1` remain the common broker
lifecycle. MQTT is opt-in; absent configuration retains empty historical
boundaries. Explicit listeners select TCP/TLS, bind IP, port, and auth policy.
Only exact caller-supplied names register processes; no instance shares
session, retained, listener or reference state with another.

Host `publish`/`subscribe` return local operation admission separately from
asynchronous memory-acceptance/subscription completion. `request`, `ack`, and
`unsubscribe` bind references to the instance and monitored caller lifetime.
Demand and outstanding responsibility have separate limits; ACK creates no
new demand. Unknown completion after timeout is never reported as rejection.
The public application delegates to runtime and constructs no MQTT structs.

Count and byte limits cover sessions/subscriptions, prepared publications,
pending/offline deliveries, retained content, Wills, writers and parser
buffers. Connections, inflight IDs, host demand/outstanding deliveries and
auth concurrency/deadlines are finite. Readers admit packets incrementally;
writers reserve finite credits before enqueueing and perform I/O separately
from the owner. Control packets can retire existing responsibility.

Advertise Maximum QoS=1, shared subscriptions unavailable, Subscription
Identifiers unavailable and inbound Topic Alias Maximum=0. Reject unsupported
use, QoS2 publications/Wills and enhanced auth explicitly. MQTT5 properties
that cannot be negotiated off require their specified semantics or a defined
failure path. MQTT3.1.1 remains historical codec-only.

## Implementation and exit evidence

1. Pure Topic and Broker functions with deterministic admission/expiry/Will/
   reconnect/resource tests; no processes, clocks or transport dependencies.
2. Running owner and bounded host routing, with monitored-caller and
   cross-instance reference regressions; explicit fail-closed callback runner.
3. Bounded TCP/TLS listeners/connections/writers and peer capability handling.
4. Fixed-version independent Mosquitto/Paho client send/receive, host↔network,
   retained/Will/session/expiry, auth negatives, limits, and two live instances.
5. All eight repository gates, reviewed integrated diff, rebuilt broker-only
   archive consumer with real behavior, and updated support/qualification rows.

Topic/config/auth unit tests alone do not establish an operational broker.
The independent network and rebuilt-installation evidence above closes this
bounded G2 exit. G3–G7, full selected-profile semantics and durable recovery
remain open under the active Goal.
