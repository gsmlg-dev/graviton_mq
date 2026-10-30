# Independent MQTT5 client

G1 implementation: `graviton_mq_client`, using the pure
`GravitonMQ.MQTT.Client` machine and shared packet codecs. This is an outgoing
network role. It neither installs nor starts a broker, listener, queue, or
broker storage application. G5 adds QoS2 with independent Mosquitto qualification;
G6 adds opt-in client-owned local recovery below. The AMQP client has separate
G3 evidence. MQTT3.1.1 runtime remains unsupported.

## Installation and host supervision

The client package depends only on core and the pure AMQP/MQTT libraries.
Source/package versions must be selected independently of these examples;
this Goal does not publish packages.

```elixir
# Host mix.exs, once that version is available from the selected source:
{:graviton_mq_client, "~> 0.1"}

# Host children; use an application-owned observer process:
{GravitonMQ.MQTTClient,
 name: MyApp.MQTTClient,
 observer: MyApp.MQTTReceiver.pid(),
 host: "broker.example",
 port: 8883,
 transport: :tls,
 tls_options: [cacertfile: "/etc/myapp/broker-ca.pem"],
 client_id: "myapp-client",
 keep_alive: 30,
 session_expiry_interval: 60,
 reconnect: [max_attempts: 5, delay: 250, max_delay: 5_000]}
```

Names are exact OTP names; no atom is derived from Client ID or endpoint.
Different unnamed client endpoints/IDs have distinct child IDs; `:id` can
explicitly distinguish otherwise identical children. A client has a transient
child spec: unexpected crashes restart, but normal observer death stops that
child without a restart loop. Hosts control restart policy with ordinary OTP
child-spec overrides. Credentials are hidden from supervisor start metadata
and client status reports. Keep actual credentials out of source files.

TCP is available with `transport: :tcp` and an explicit port. TLS verifies
the trust chain and DNS/IP identity. A supplied `server_name_indication`
selects the intended peer identity; without one, tuple IP addresses are
verified against IP SANs. No failed TLS handshake falls back to TCP.

## Operations and completion

```elixir
alias GravitonMQ.MQTTClient, as: Client

{:ok, subscription} = Client.subscribe(MyApp.MQTTClient, "devices/+/status", qos: 1)
{:ok, publication} = Client.publish(MyApp.MQTTClient, "devices/one/status", "online", qos: 1)
{:ok, removal} = Client.unsubscribe(MyApp.MQTTClient, "devices/+/status")
```

Each return value is local admission. The calling process later receives:

```elixir
{:graviton_mq, client_pid, {:operation, subscription, {:ok, [granted_qos]}}}
{:graviton_mq, client_pid, {:operation, publication, {:ok, :puback}}}
{:graviton_mq, client_pid, {:operation, removal, {:ok, [reason_code]}}}
```

QoS0 completion is `{:ok, :sent}` after socket acceptance, without proof of
remote receipt. QoS1 completion requires a real positive PUBACK; negative
PUBACK retains its reason code as `{:error, {:puback, code}}`. QoS2 completes
at PUBCOMP (`{:ok, :pubcomp}`); negative PUBREC/PUBCOMP retain their packet
name and reason code. This is no business exactly-once guarantee. SUBACK and
UNSUBACK preserve per-filter reason codes, including rejection or a lower
grant. Invalid/local unsupported requests return `{:error, reason}` without
an operation reference. Packet/inflight/operation limits reject admission
explicitly; requests are not buffered indefinitely while offline.

Timeout after a possible send, disconnect without a recoverable session, and
lost remote session state report `{:error, :unknown_outcome}`. A publication
is not silently retried as a fresh host operation. An operation timeout closes
the connection so a late acknowledgement cannot match a reused Packet ID.
No completion promises subscriber processing or disk durability.

## Receiving with bounded ownership

The observer grants delivery demand and explicitly acknowledges ownership:

```elixir
:ok = Client.request(client_pid, 1)

receive do
  {:graviton_mq, ^client_pid, {:delivery, delivery_ref, packet}} ->
    # Process or transfer ownership of packet.topic / packet.payload here.
    :ok = Client.ack(client_pid, delivery_ref)
end
```

PUBACK for inbound QoS1 follows the observer's acknowledgement. An unacknowledged
duplicate does not create another local delivery. Pending/outstanding deliveries
are finite; the client advertises Receive Maximum and Topic Alias Maximum=0.
Inbound QoS2 sends PUBREC only after host ACK and retains protocol ownership
until PUBREL/PUBCOMP, suppressing duplicate local delivery. It rejects aliases
because Topic Alias Maximum is zero. Received properties remain typed and ordered. Queued message
expiry uses supplied elapsed-time facts; expired queued publications do not
become host deliveries. Observer death cleans up owned sockets and timers.

## Negotiation, time and reconnect

The client enforces peer Maximum QoS, Receive Maximum, Maximum Packet Size,
Retain Available and subscription capabilities. It observes server keep alive
and requires PINGRESP. Enhanced authentication is explicitly unsupported;
username/password are CONNECT fields. Pure decode does not authenticate.

Transport loss can trigger the configured finite retry/backoff policy; the
default has zero retries. Connection generations fence stale socket/timer
events. Protocol/authentication failures and intentional `disconnect/1` stop
automatic retry. The stopped owner remains inspectable through `status/1`.

Only a live local session plus a successful CONNACK Session Present permits
inflight QoS1/2 PUBLISH retransmission, with the original Packet ID and DUP.
After positive PUBREC, QoS2 resumes PUBREL with that ID, without replaying
PUBLISH. Send/receive quotas reset at each connection; PUBREC does not free
PUBLISH quota, while PUBCOMP or negative PUBREC does. Retries
respect newly negotiated limits. If the remote session was lost, pending
possibly sent operations become unknown and the observer receives
`{:subscriptions_lost, grants}`; the host must explicitly resubscribe and
observe new SUBACKs. No periodic publication retry occurs on a healthy socket.
Opt-in owner/VM restart recovery uses the G6 local store below; ordinary
transport reconnect alone is not evidence of persistent recovery.

Default bounds are 16 outbound QoS1/2 inflight, 32 operations, 32 inbound
deliveries, 1,024 subscription filters, and a 1 MiB packet. Options
`:max_inflight`, `:max_operations`, `:max_deliveries`, `:max_subscriptions`,
`:receive_maximum`, and `:max_packet_size` adjust those bounds. `:properties`
accepts supported CONNECT properties; configure the size/receive/alias bounds
through dedicated options rather than overriding their wire properties.

## Local persistence and recovery ownership

Configure a nonempty stable `:client_id` and
`persistence: [directory: "/var/lib/my-app/mqtt-client", max_bytes: 8_388_608]`.
The parent directory must exist. The client package owns this Linux adapter;
it installs no broker storage/runtime/composition application. A shared
inode/device lock excludes aliases, other clients, broker WALs and independent
BEAMs from the same directory. Use distinct directories per owner.

Versioned checksummed checkpoints retain subscriptions, inflight QoS1/2 packet
IDs and phases, incoming ownership, stable operation IDs, deadlines and a bounded
terminal-result outbox. Credentials, CONNECT, callbacks, caller PIDs/references,
sockets and timers are supplied afresh. A bounded term parser rejects compressed
ETF, new atoms, runtime handles, excessive bytes/depth/items and invalid role
schemas before recovery. Startup also rejects a different Client ID.

File synchronization, atomic replacement and directory synchronization precede
wire and host effects. Every validated reopen re-establishes that boundary
before exposing recovered ownership or terminal success. Partial/unpublished
temporary files are not recovery checkpoints. Failed/uncertain publication
poisons the owner, closes its connection and emits `{:storage_error, reason}`;
it cannot send ordinary success. A possible operation remains unknown.

Persistent admission sends the caller
`{:operation_identity, public_ref, {store_uuid, sequence}}` inside the usual
`{:graviton_mq, client, event}` envelope. Recovery sends the new observer
`{:recovered_operation, stable_id, kind}` and terminal
`{:recovered_operation_result, stable_id, result}` events. Previously delivered
incoming ownership uses
`{:recovered_delivery, {new_client_pid, delivery_id}, packet, qos2_phase}`;
it is not silently repeated as an ordinary delivery. Hosts acknowledge the
new ownership reference and deduplicate recovery events by stable identity.
Another crash can repeat an outbox event; this is no business exactly-once claim.

A terminal unknown host result does not retire MQTT protocol responsibility.
Unacknowledged QoS1/2 PUBLISH/PUBREL and their Packet Identifiers remain saved
until handshake completion or actual session loss/expiry, including after
retry exhaustion, explicit disconnect and supervisor shutdown. Recovery does
not recreate an expired host timer or turn its prior unknown result into
ordinary success. It reports later protocol progress separately as
`{:recovered_protocol_result, stable_id, result}`. This event describes the
remote handshake and cannot establish whether a subscriber processed a message.

Message expiry and operation deadlines include wall-clock downtime. Known
offline Session Expiry retains its exact age. An online snapshot cannot know
when a later process/VM crash disconnected its socket; recovery starts a finite
local window then and asks the peer through Session Present, rather than
counting healthy online time as offline expiry.
Backward wall-clock movement rejects recovery. Remote Session Present still
determines whether inflight PUBLISH/PUBREL can resume; local disk state cannot
fabricate a remote session. The qualified cases include real Mosquitto 2.1.2
QoS2 ownership, process kill, SIGKILL followed by a fresh BEAM, phase-specific
replay, expiry, partial writes and sync/rename/directory-sync faults.

## Qualification

See [capabilities](CAPABILITIES.md) for current PASS/NOT RUN evidence. Run
the repository gates first, then the pinned Mosquitto suite:

```sh
GRAVITON_MOSQUITTO_BIN=/absolute/path/to/mosquitto-2.1.2/bin/mosquitto \
  mix test apps/graviton_mq_client/test/mqtt_mosquitto_test.exs \
    apps/graviton_mq_client/test/mqtt_session_mosquitto_test.exs \
    apps/graviton_mq_client/test/mqtt_qos2_mosquitto_test.exs --trace --seed 0
```

The helper verifies version 2.1.2, uses ephemeral loopback listeners, and
cleans up its own peers. Tests exercise independent Mosquitto publishers and
subscribers, real grants/acknowledgements, limits, authentication/ACL rejection,
keep alive, reconnect, TLS trust/hostname failures, and host cleanup. Without
the explicit binary the tagged tests are excluded, so ordinary `mix test`
alone does not establish interoperability. Archive consumers additionally
verify the client-only dependency closure and network exchange when
`GRAVITON_MQTT_TEST_PORT` names a running qualification peer.

The live-session test drops an actual broker PUBACK through a transparent TCP
proxy while the same Mosquitto process stays alive. Reconnect must report
Session Present, retransmit the original Packet ID/body with DUP, and finish
the original operation only after a second real PUBACK. This proves memory
session resumption, without proving process/VM recovery or exactly-once delivery.
