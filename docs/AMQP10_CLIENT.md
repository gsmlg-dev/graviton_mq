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
Unsupported negotiation is rejected explicitly. Reconnect is opt-in (below);
Link/delivery resumption and client process/VM recovery remain unimplemented;
broker queue persistence is a separate responsibility. General homogeneous arrays and all scalar wire
families are available; application properties still require simple scalar/null
values. Exact flow/sequence/failure evidence and its limits are recorded in
the [clause inventory](qualification/2026-10-30-amqp10-requirement-inventory.md).
This bounded profile does not claim every AMQP 1.0 feature.

## Reconnect (S3, 2026-11-13, unreleased)

A reconnect is a **new AMQP Connection**: new socket, a full TLS handshake,
a new SASL exchange, a new protocol header and Open. The Sessions and Links on
it are new endpoints. There is no Link or delivery resumption (Part 2
§2.6.13): termini are volatile, so the peer discards link state when the
Connection is lost. GravitonMQ claims at-least-once delivery, never
exactly-once.

```elixir
{GravitonMQ.AMQP10Client,
  container_id: "orders-worker", host: "broker.example", transport: :tls,
  observer: self(),
  reconnect: [max_attempts: 5,     # 0..100, default 0
              delay: 250,          # 1..60_000 ms, default 250
              max_delay: 5_000]}   # delay..60_000 ms, default 5_000
```

The delay before attempt *n* is `min(max_delay, delay * 2^(n-1))`, the MQTT
client's formula, with no jitter. The attempt counter resets to 0 when the
peer's Open arrives, not at TCP connect, so a peer that accepts TCP and then
refuses SASL or Open cannot reset the budget. An unknown key or out-of-range
value fails `start_link/1` with `{:error, :invalid_options}`. The default
`max_attempts: 0` keeps the previous behaviour: one connection, then a stopped
but inspectable owner. `connect_timeout`, `sasl_timeout` and the Open deadline
apply to each attempt.

### Retry classification

| Disconnect reason | Action |
| --- | --- |
| `:closed`, `{:transport, _}`; `{:connect, r}` for `r` in `econnrefused`, `econnreset`, `econnaborted`, `ehostunreach`, `enetunreach`, `nxdomain`, `timeout`, `etimedout`, `closed`; `{:write_failed, _}` except an encode failure | retry |
| receive idle timeout, Open timeout, `:sasl_timeout`, `{:sasl, :sys_temp}` | retry |
| peer Close with `amqp:connection:forced`, or a peer Close without error that the host did not request | retry |
| `{:sasl, :auth \| :sys \| :sys_perm \| {:unknown, _}}`, `:sasl_not_offered`, `:sasl_mechanism_not_offered`, `:sasl_protocol_error`, `:invalid_sasl_credentials` | terminal |
| any other `{:connect, _}`, including TLS alerts (verification, hostname, protocol) | terminal |
| decode or protocol errors, a peer Close with any other condition, an encode `{:write_failed, _}`, `:owner_stopped`, host `close/1` | terminal |

The Endpoint reports a Close the client itself started as
`{:disconnected, :normal | :close_timeout | :closed}`, never as
`:idle_timeout` or `:open_timeout`. The owner therefore records why it began
closing (host close, receive idle, Open timeout or protocol error) and
classifies on both; the `{:disconnected, reason}` event keeps the Endpoint's
reason. A terminal reason or an exhausted budget leaves the owner with
`owner: :stopped`: it holds no socket or timer, keeps answering `status/1`
and does not exit. The host decides whether to stop or restart it.

### Owner phases and commands

`status/1` reports `owner: :connecting | :active | :restoring | :offline |
:stopped`, beside `phase` (the Endpoint phase, unchanged), `generation`,
`attempt`, `next_retry_ms` (remaining backoff, or `nil`), `intent: %{sessions:
s, links: l}` and `restoring` (outstanding restoration items). It shows no
host, credential, address or payload.

- During the initial connection, commands keep the Endpoint's answers
  (`{:error, :not_connected}`). After a loss, in `:offline` and while
  reconnecting, every command returns `{:error, :disconnected}` at once;
  nothing is queued.
- While restoring, a command on a Session or Link still waiting for
  restoration returns `{:error, :restoring}`; restored items work normally.
- `close/1` in any phase cancels the retry timer and stops the owner. While
  offline or reconnecting it replies `{:ok, ref}` and sends
  `{:operation, ref, {:ok, :closed}}` at once. A `close/1` issued while the
  Endpoint is already closing (after an idle or Open timeout or a protocol
  error) is accepted, is final and completes with `{:ok, :closed}` once the
  transport is released; the owner does not reconnect (`0312b11`).
- Delivery references carry their generation. `settle/3` with a reference
  from an earlier Connection returns `{:error, :stale_delivery}` immediately
  and writes nothing.

### Restoration

The owner keeps a bounded in-memory **intent**: a Session's identity and
`open_session` options, and a Link's `{name, role}` identity and `attach/5`
options. An entry is added only when the host's `open_session/3` or
`attach/5` completes `{:ok, _}`, and removed when the host admits
`end_session/2` or `detach/3` or the peer ends or detaches that item (with or
without an error). An operation in flight at the loss gets its own
`{:unknown | :error, _}` result and is not restored. No message bytes, credit,
delivery or credential is stored; size is bounded by `max_sessions` ×
`max_links`.

After each successful Open of a reconnect, Sessions are reopened in ascending
identity order, and each Session's Links re-attached in their completion
order with the same identity and options. At most `min(8, free operations)`
restoration operations are outstanding, so host commands are not starved.
The observer receives:

```elixir
{:graviton_mq, client, {:disconnected, reason}}
{:graviton_mq, client, {:reconnecting, attempt}}       # only when a retry is scheduled
{:graviton_mq, client, {:connected, remote_container_id}}
{:graviton_mq, client, {:restored, {:session, sid}, result}}
{:graviton_mq, client, {:restored, {:link, sid, link_identity}, result}}
{:graviton_mq, client, {:restore_complete, %{restored: n, failed: m}}}
{:graviton_mq, client, {:reconnect_exhausted, attempts}}  # only with max_attempts > 0
```

`result` is `{:ok, _}`, `{:error, reason}` or `{:unknown, reason}`. A failed
item is dropped from intent. The Links of a Session that failed to restore are
reported `{:error, :session_not_restored}` and dropped. `{:restore_complete,
_}` follows every reconnect, also with empty intent, and never the first
successful connection. If no `{:reconnecting, _}` follows a
`{:disconnected, _}`, the owner has stopped.

A restoration interrupted by another transport loss is not reported; its
entries stay declared and are retried on the next connection. This includes
an operation deadline or session error that arrives while the Endpoint is
already closing (for example after a receive idle timeout): it counts as an
interruption, not as the peer's decision, and the declaration is kept
(`f96595c`).

Restored automatically: Connection, SASL, TLS, Sessions and Links. **Credit is
not restored**: call `request/5` again after `{:restored, {:link, …}, {:ok,
_}}`. The host also resends any `{:unknown, _}` publication if it wants to
(the client never replays one; a resend may duplicate the message, which
at-least-once allows; use a stable application identity such as Properties
`message-id` to deduplicate), settles redelivered messages through their new
references, and re-declares failed restorations.

Each attempt builds a new SASL exchange: a password provider runs once per
attempt, so it may return a rotated secret. Each TLS attempt performs a full,
verified handshake without session reuse; a changed or untrusted server
certificate on reconnect is a terminal TLS alert. One socket and one retry
timer exist at a time; the old socket is closed before the retry is
scheduled, and the timer is cancelled by `close/1`, observer death and
termination.

Limitation (half-open loss): after a silent network loss, a peer may keep the
old Link with the same name and container id until its own connection TTL
expires. Re-attach may then be refused or redelivery delayed; the refusal is
reported per Link through `{:restored, …, {:error, _}}`.

Interop note (Artemis address routing): Artemis 2.41.0 auto-creates a bare
address as MULTICAST when no `default-address-routing-type` is set. A receiver
then gets a temporary subscription queue that is deleted together with its
connection, including an unsettled message, so nothing is redelivered after a
reconnect. Use an ANYCAST queue for redelivery across reconnects, for example
an `anycast://` address with the acceptor's `anycastPrefix`, as the R2 test
does. The client sends no source capabilities to select routing.

Evidence: local tests `amqp10_reconnect_test.exs`, `amqp10_intent_test.exs`,
`privacy_test.exs` and `tls_transport_test.exs` (OTP `:ssl` test peer) PASS
in Tests run 43223447801 and the `checks` job of CI run 43223448120. Against
Artemis 2.41.0 with independent Proton 0.40.0 senders and receivers, all nine
reconnect cases R1–R6 and F1–F3 (`amqp10_artemis_reconnect_test.exs`, through
a multi-accept cut proxy) passed in the `interoperability` job of the same
run for `375f677`.

External role qualification uses Apache ActiveMQ Artemis 2.41.0 and independent
Qpid Proton 0.40.0 senders/receivers. The interop suite is enabled by
`ARTEMIS_HOST`, `ARTEMIS_PORT`, and `GRAVITON_PROTON_PYTHON`. See the
[capability matrix](CAPABILITIES.md) for exact evidence and remaining stages.
