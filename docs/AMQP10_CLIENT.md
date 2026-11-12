# Independent AMQP 1.0 client

`graviton_mq_client` installs a native, host-supervised AMQP client without
broker runtime, storage, listeners, or a broker Application. It shares the
pure AMQP Endpoint and SessionEngine with the broker role. Starting
a client opens an outgoing network connection; it does not start a broker.

```elixir
alias GravitonMQ.AMQP10.{Outcome, Value}
alias GravitonMQ.AMQP10Client, as: Client

children = [
  {Client, container_id: "orders-worker", host: "broker.example", port: 5671,
    transport: :tls, observer: self(),
    tls_options: [cacertfile: "/etc/ssl/broker-ca.pem"]}
]

{:ok, host} = Supervisor.start_link(children, strategy: :one_for_one)
[{_, client, _, _}] = Supervisor.which_children(host)
# Wait for {:graviton_mq, client, {:connected, remote_container_id}}.
{:ok, opened} = Client.open_session(client, 1)
# Wait for {:graviton_mq, client, {:operation, opened, {:ok, 1}}}.
{:ok, attached} = Client.attach(client, 1, "orders", :receiver,
  address: Value.string("orders"), max_message_bytes: 65_536)
# Wait for attachment's operation completion before granting credit.
link = {Value.string("orders"), :receiver}
{:ok, requested} = Client.request(client, 1, link, 1, 65_536)
# Deliveries include {client, session_id, token}; retain that opaque reference.
# {:ok, settled} = Client.settle(client, delivery_reference, %Outcome.Accepted{})
```

The host owns the observer process and must keep it alive. Under a supervisor,
set `:observer` explicitly to the intended application receiver. The child
specification captures its creator as the default observer; it never chooses
the child-starting supervisor. `:name` is an exact OTP name and `:id` an
optional supervisor child identity. No string is converted to an atom.

## Operation and delivery responsibility

All commands return admission `{:ok, reference}` or an immediate local
`{:error, reason}`. Admission does not imply peer acceptance. Completion goes
to the calling process:

```elixir
{:graviton_mq, client, {:operation, reference, result}}
```

`open_session`, `attach`, `detach`, `end_session`, and `close` wait for the
corresponding peer handshake. `send_message/5` accepts authoritative encoded
format-zero message bytes and completes with a terminal, peer-settled
`Outcome.Accepted`, `Rejected`, `Released`, or `Modified`. `Received` progress
and successful socket writes do not complete publication. No outcome proves
consumer business processing or disk durability.

A never-emitted operation fails with `{:error, reason}`. A possibly emitted
operation interrupted by timeout, transport loss, or endpoint closure returns
`{:unknown, reason}`. If a terminal outcome is already known but settlement
is interrupted, `{:error, {:outcome_known, outcome, reason}}` preserves it.
The client never automatically replays an uncertain publication.

The observer receives:

```elixir
{:graviton_mq, client,
 {:delivery, delivery_reference, session_id, link_identity, original_bytes,
  %{message_format: 0}}}
```

`request/5` reserves finite delivery slots and worst-case message bytes before
granting Link credit. Session windows separately count Transfer frames.
`settle/3` explicitly accepts, releases, rejects, or modifies a delivery.
Original encoded message bytes remain authoritative, including across
fragment assembly. Local/remote channels and handles are independent;
Links remain Session-owned data.

## Limits, transport and initial profile

Defaults are 64 host operations, 16 Sessions, 16 Links per Session, 64 outgoing
and 64 incoming deliveries per Session, a 1 MiB message bound, 4 MiB retained
bytes per direction per Session, a 64 KiB incoming frame bound, and a 1 MiB
input buffer. A turn processes at most 32 envelopes; socket reads remain
passive while buffered envelopes drain. Finite writer pools retain capacity
for control/Close frames. Fragmentation follows the peer's advertised frame
bound; the local bound applies only to incoming frames.

Use `session_options` for Session limits, `:timeout` on individual operations,
and `operation_timeout_ms` for the endpoint default (10 seconds). Session
`operation_timeout` overrides that inherited default. Idle deadlines and
heartbeat writes use generation/token-fenced timers. Owner/observer shutdown
closes the actual socket and cancels timers. `status/1` exposes bounded counts,
without payloads or credentials; OTP status and failure reports are redacted.

TCP defaults to port 5672, TLS to 5671. TLS verifies the certificate chain and
hostname using system CAs or a supplied CA. `verify_none`, disabling SNI, and
overriding owner framing/read/write options are rejected.

The client supports raw AMQP protocol ID 0 (`sasl: :none`, default) or SASL
ANONYMOUS/PLAIN (S2, 2026-11-12, unreleased): `sasl: :anonymous |
{:anonymous, trace} | {:plain, user, password} | {:plain, user, password,
authzid: id}`. The password may be a zero-arity function; it runs once per
connect in the owner, after the socket connects and before the `sasl_timeout`
deadline (default 5,000 ms, 1..10,000) is armed, so it must return promptly.
PLAIN over `transport: :tcp` requires `allow_insecure_plain: true`; invalid
values and PLAIN credentials whose sasl-init would exceed the 512-octet SASL
frame fail `start_link/1` with `{:error, :invalid_options}`. The sasl-init
hostname is the configured Open `hostname` (or absent). A SASL client never
falls back to raw AMQP: it disconnects with `{:sasl, :auth | :sys | :sys_perm |
:sys_temp | {:unknown, code}}`, `:sasl_not_offered` (the peer answered
`AMQP 0 1 0 0`), `:sasl_mechanism_not_offered`, `:sasl_protocol_error` (for
example a sasl-challenge), `:sasl_timeout` or `:invalid_sasl_credentials` (a
provider raised or returned a non-binary), and `{:connected, _}` is sent only
after Open. The credential, or its provider, stays in redacted owner state for
the connection lifetime; an evaluated provider result does not outlive
sasl-init. EXTERNAL, SCRAM and the TLS-upgrade header are not implemented.

The current profile also uses format-zero messages, nontransactional unsettled sender/receiver-first settlement,
volatile non-dynamic Source/Target, no filters, and no Link/session resume.
Unsupported negotiation is rejected explicitly. Client reconnect/resumption
and process/VM recovery remain unimplemented; broker queue persistence is a
separate responsibility. General homogeneous arrays and all scalar wire
families are available; application properties still require simple scalar/null
values. Exact flow/sequence/failure evidence and its limits are recorded in
the [clause inventory](qualification/2026-10-30-amqp10-requirement-inventory.md).
This bounded profile does not claim every AMQP 1.0 feature.

External role qualification uses Apache ActiveMQ Artemis 2.41.0 and independent
Qpid Proton 0.40.0 senders/receivers. The interop suite is enabled by
`ARTEMIS_HOST`, `ARTEMIS_PORT`, and `GRAVITON_PROTON_PYTHON`. See the
[capability matrix](CAPABILITIES.md) for exact evidence and remaining stages.
