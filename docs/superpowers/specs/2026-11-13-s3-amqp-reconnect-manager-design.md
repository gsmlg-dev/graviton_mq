# S3 — AMQP 1.0 client reconnect; remove the 5 s broker Manager block

Date: 2026-11-13. Status: **approved by the owner 2026-11-13** (decisions
below); nothing here is implemented. Programme row: [P0–P3 design §2, S3](2026-11-12-http-fetch-transport-and-p0-p3-design.md)
(depends on S1; exit evidence "Fault-injection tests; Artemis reconnect").
Authorized by [ACTIVE_SCOPE 2026-11-12](../../ACTIVE_SCOPE.md) (row "S3 AMQP
client reconnect, Manager block: Pending"). Commit, push, version bump and
release are not authorized by this design. Decisions marked **(owner Qn)** are
listed in §9 with a recommended default. Baseline: `c9c64f8` plus the
uncommitted S1a/S2 tree (S2 SASL client code is present in
`client/amqp10.ex`).

## Owner decisions (2026-11-13)

- Q1 restoration: **the client restores** the Sessions and Links the host
  successfully declared, from bounded in-memory intent, reporting each result.
- Q5 enqueue that definitively never executed: **settle with `Released`**;
  the Connection survives and the sender may resend.
- All other open questions (§9, Q2–Q4, Q6, Q7): **the recommended default is
  accepted** as written in §9.

## 1. Scope

S3 has two independent parts that touch disjoint applications:

- **A. Client reconnect** (`apps/graviton_mq_client`): after transport loss,
  the running `GravitonMQ.AMQP10Client` owner opens a *new* AMQP Connection
  under a bounded retry policy, re-authenticates, and re-establishes the
  Sessions and Links the host had successfully declared. It reports every
  interrupted operation and every voided delivery explicitly.
- **B. Broker Manager** (`apps/graviton_mq_runtime`): the instance Manager
  (`GravitonMQ.Runtime.AMQP10.Broker`) stops making blocking calls to queue
  owners. An unresponsive queue then delays only its own Links. Today it
  stalls every Connection and queue for 5 s.

Non-goals (all parts):

- AMQP link recovery or delivery resumption (Part 2 §2.6.13, Attach
  `unsettled` maps, durable termini Part 3 §3.5). The profile keeps volatile
  termini. A peer discards link state when the Connection is lost, so there is
  nothing to resume (§3.1).
- Automatic retransmission of a publication whose outcome is uncertain. The
  existing rule stands: "the client never automatically replays an uncertain
  publication". GravitonMQ claims at-least-once delivery, never exactly-once.
- Client process or VM recovery, a client-side durable store, and
  queue-and-forward of host commands while offline. Once the owner process
  dies its state is gone; CAPABILITIES keeps "process/VM recovery
  UNIMPLEMENTED".
- Broker-side changes to Connection, Writer, Listener, HellHandoff or TLS
  paths (S1b owns those, §7). The 25 ms `Connection.capacity/4` query stays
  as it is: it is already bounded and retried (`amqp10_availability_test.exs`).
- Telemetry events (S5). S3 adds only bounded `status/1` counters.

## 2. Current state (evidence)

### 2.1 Client

- `GravitonMQ.Client.AMQP10` (`client/amqp10.ex`) creates one
  `Endpoint` and one `Stream` in `start_link/1` and connects in
  `handle_continue(:connect)`. On transport loss, `Endpoint.shutdown/2`
  finishes every pending operation: emitted ones get `{:unknown, reason}`,
  never-emitted ones get `{:error, reason}`, and a known terminal outcome
  whose settlement was interrupted gets `{:error, {:outcome_known, outcome,
  reason}}` (`session_engine.ex` `failure_result/3`, `close/3`). It then
  emits `{:event, {:disconnected, reason}}` and `{:close_transport, …}`. The
  endpoint stays in `phase: :closed`, and later commands get
  `{:error, :closed}` (`endpoint.ex` l.242). The owner stays alive and
  inspectable. It never reconnects.
- Generation fencing is already in place. `Endpoint` has a `generation`
  option. Timers (`{:wake, generation, key, token}`), write receipts
  (`{:write_result, generation, …}`), the SASL machine and incoming delivery
  tokens (`{generation, sid, incarnation, n, id}`, `session_engine.ex`
  l.1295) all carry it. Socket messages match only `state.socket`. So a
  reconnect can build a fresh `Endpoint` with `generation + 1`, and stale
  events and references are rejected without new machinery.
- S2 keeps the SASL credential, or a zero-arity password provider, in
  redacted owner state "so a later reconnect can re-authenticate" (S2 Q11,
  `config.ex` comment). `credentials/1` evaluates the provider once per
  `begin/1`, after the socket connects and before the SASL deadline is armed.
- The Link-name `:global` locks are keyed by local and remote container ids
  and are released when the endpoint reaches `:closed` (`retire_link_names/2`).

### 2.2 MQTT client patterns reused

`client/mqtt/config.ex` has `reconnect: [max_attempts: 0..100 (default 0),
delay: 1..60_000 (250), max_delay: delay..60_000 (5_000)]`.
`client/mqtt.ex` `effect({:retry, attempt})` computes
`min(max_delay, delay * 2^(attempt-1))` and schedules one token-fenced
`:retry` timer. The pure `GravitonMQ.MQTT.Client.transport_loss/4` decides
retry or stop. It emits `{:disconnected, reason}` and then `{:retry,
attempt}`, or reaches `phase: :stopped`, and it resets `reconnect_attempts`
on a successful CONNACK. Protocol/authentication failures and `disconnect/1`
stop retrying. The transport is released before the retry decision, so a
stopped owner holds no socket or timer. When the session is lost the host
gets `{:subscriptions_lost, grants}` and must resubscribe. S3 mirrors the
config shape, the backoff formula, the generation fencing and the
"stopped but inspectable" end state.

### 2.3 Broker Manager: why it blocks

Every `GravitonMQ.Runtime.AMQP10.Queue` API function goes through
`Queue.request/2` (`queue.ex` l.476). That function creates a caller-owned
public ETS exchange table, inserts `{token, message}`, and calls
`GenServer.call(queue, {:execute, table, token})` with the **default 5,000 ms
timeout**. It maps `:exit {:timeout, _}` to `{:error, :unknown_outcome}`,
`noproc`/`normal`/`shutdown` to `{:error, :closed}`, and anything else to
`{:error, :queue_call_failed}`. The queue takes the request atomically
(`take_request/3`). It executes, writes the result back under the same
token, and replies `:ok`. If the caller already deleted the table, the queue
finds nothing and the request is `:abandoned`, not executed.

The Manager is one GenServer for the whole instance. It handles every
Connection event, authorization result, pump wave and queue notification.
Each synchronous queue call below therefore stops all of that work for up to
5 s:

| # | Site (`broker.ex`) | Call | Result on uncertain (`@uncertain = [:unknown_outcome, :queue_call_failed, :closed]`) |
| --- | --- | --- | --- |
| 1 | `handle_continue(:wire)` | `watch` per queue | `start_probe` (queue treated as fenced) |
| 2 | `handle_call({:status, …})` | `status` for **every** queue, one after another | value reported as-is; no fence. k stuck queues take k×5 s, which exceeds the public caller's own 5 s `GenServer.call`, so `amqp10_status/1` exits in the caller |
| 3 | `info({:amqp10_queue_changed, q})` | `watch_ack` | fence |
| 4 | `decide/3` → `register/2` | `register` (lease) | reject Link + fence |
| 5 | `event({:delivery, …})` → `admit_message/4` | `enqueue` | `:unknown`: close the publishing Connection, no disposition, no fence |
| 6 | `complete/3` (`:send`) | `settle` (peer outcome) | fence |
| 7 | `pump_route/2` receiver | `status` (free capacity) | fence |
| 8 | `pump_route/2` sender drain | `status` (`ready`) | fence |
| 9 | `pump_sender/4` | `status` (limits) | fence |
| 10 | `claim/5` | `claim` | fence ("the claim may have executed after the timeout") |
| 11 | `claim/5` unfit message | `settle` `MakeAvailable` | fence |
| 12 | `remove_route/2` | `disconnect` (detach/session/connection cleanup) | fence (unless `:stale_consumer`) |
| 13 | `restore/3` | `watch` + `watch_ack` | re-probe |

The probe (`probe/1`) already runs `Queue.status` in a spawned, monitored
process and does not block the Manager.

**Current fencing semantics (keep them).** `fence/2` kills the queue's lease
anchor. The queue monitors that anchor, so when it runs again it returns
every claim this coordinator holds there. `fence/2` then detaches that
queue's non-authorizing Links with `amqp:internal-error` "queue is
unavailable", and it refuses new attaches via `fenced?/2` (no anchor). The
queue reports `:unavailable` in status and is probed every 1 s. A successful
`status` + `watch` + `watch_ack` restores a fresh anchor. Messages stay in
the queue process. An uncertain enqueue closes only its Connection, because
enqueue holds no lease. A real queue crash restarts later owners through
`rest_for_one`. The Manager serializes final known outcomes before it
processes Connection DOWN, so a settle reaches the queue before the
cleanup `disconnect`.

Tests that pin today's limit: `amqp10_availability_test.exs` "an unanswered
queue fences only its own Links…" (8 s waits) and
`amqp10_boundaries_test.exs` "uncertain detach cleanup fences only its queue…"
("The Manager stays blocked for the queue's 5 s call timeout…", 7 s wait).

## 3. Part A — client reconnect

### 3.1 What reconnect means for AMQP 1.0

A reconnect is a **new Connection**: new socket, optional new TLS handshake,
new SASL exchange, new protocol header, new Open. The Sessions and Links on
it are new endpoints. AMQP 1.0 can resume a Link only by re-attaching the
*same* Link with `unsettled` maps (§2.6.13), and only while the peer still
holds that Link's terminus state. With volatile termini (our profile, and
Artemis queue consumers/producers in our tests), the peer discards that state
when the Connection is lost. The consequences:

- An outgoing transfer that was in flight has an **unknown outcome**. The
  peer may already have accepted it. Retransmitting it with the same
  delivery-tag on the new Link does not deduplicate anything: tags are scoped
  to a link endpoint, and the new attach is a fresh endpoint. Automatic
  retransmission would therefore only add hidden duplicates. S3 reports
  `{:unknown, reason}` to the host, which already happens, and does not
  resend. If the host resends, the message may be duplicated, which
  at-least-once allows. Applications that need deduplication use their own
  stable identity, such as Properties `message-id`. Broker-specific duplicate
  detection is not relied upon.
- A received but unsettled delivery is **voided**. The peer keeps
  responsibility and releases the delivery when it sees the Connection go
  down. After a reconnect and new credit it may be delivered again, which is
  the at-least-once redelivery. The old delivery reference carries the old
  generation. `settle/3` with it returns `{:error, :stale_delivery}`
  immediately and never reaches the wire.
- **Receiver credit and reservations** belong to the old Session and are
  released with it (`SessionEngine.close/3`). New credit requires a new
  `request/5` (**owner Q2**).
- **Pending operations** were already finished at loss time (`{:unknown, _}`,
  `{:error, _}`, `{:error, {:outcome_known, …}}`). Reconnect never re-runs
  them.

### 3.2 Configuration (mirrors MQTT)

```elixir
{GravitonMQ.AMQP10Client,
  container_id: "orders-worker", host: "broker.example", transport: :tls,
  observer: self(),
  reconnect: [max_attempts: 0,     # 0..100; 0 = today's behaviour (default)
              delay: 250,          # 1..60_000 ms, first retry delay
              max_delay: 5_000]}   # delay..60_000 ms cap
```

- The delay before attempt *n* is `min(max_delay, delay * 2^(n-1))`. This is
  exactly the MQTT formula, with no jitter (**owner Q3**).
- The attempt counter resets to 0 when the peer's Open is received, not at
  TCP connect. This prevents a peer that accepts TCP and then refuses SASL or
  Open from resetting the budget forever.
- An invalid value fails `start_link/1` with `{:error, :invalid_options}`,
  in the existing style. The default `max_attempts: 0` keeps the current
  behaviour byte-for-byte: one connection, then the stopped but inspectable
  owner.
- `connect_timeout`, `sasl_timeout` and the Endpoint open deadline
  (`operation_timeout_ms`, `:open_timeout`) apply to each attempt unchanged.

### 3.3 Retry classification

The shell classifies the `{:disconnected, reason}` produced by the Endpoint
or the SASL path:

| Reason | Action |
| --- | --- |
| `:closed`, `{:transport, _}`, `{:connect, r}` with `r` not a TLS alert (refused, timeout, nxdomain, closed) | retry |
| `:idle_timeout` (receive idle), `:open_timeout`, `:sasl_timeout`, `{:sasl, :sys_temp}` | retry |
| `{:peer_close, error}` with condition `amqp:connection:forced`, or a peer Close without error that the host did not request | retry (**owner Q4**) |
| `{:sasl, :auth \| :sys \| :sys_perm \| {:unknown, _}}`, `:sasl_not_offered`, `:sasl_mechanism_not_offered`, `:sasl_protocol_error`, `:invalid_sasl_credentials` | terminal: retrying does not fix credentials or peer policy, and avoids lockouts |
| `{:connect, {:tls_alert, _}}` (verification, hostname, protocol) | terminal: a trust failure is not transient |
| decode/protocol errors, `{:peer_close, other condition}`, `:owner_stopped`, host `close/1` | terminal |

A terminal reason, or an exhausted budget, puts the owner into
`phase: :stopped`. It holds no socket or timer and keeps answering `status/1`
(MQTT parity). It does not exit. The host decides whether to stop it or
restart it.

### 3.4 Owner phases and host commands

The phases are `:connecting → :active → :offline → :connecting → … →
:stopped`, with `:restoring` as a sub-state of `:active` (§3.5).

- In `:offline` and `:connecting`, every host command returns
  `{:error, :disconnected}` immediately. Commands are not queued, and nothing
  reports a success that has not happened.
- In `:restoring`, a command that targets a Session or Link still waiting for
  restoration returns `{:error, :restoring}`. Commands on restored items work
  normally.
- `close/1` in any phase cancels the retry timer and stops the owner. In
  `:offline` it completes immediately with `{:ok, :closed}`, because there is
  no Connection to close.

### 3.5 Declared intent and restoration (**owner Q1**)

The shell keeps a bounded **intent** record in a new pure module
`GravitonMQ.Client.AMQP10.Intent`, inside the client app. It has no process
and no clock.

- An entry is added only when the host's `open_session/3` or `attach/5`
  operation **completes** `{:ok, _}`. An operation that is in flight when the
  connection is lost gives the host its `{:unknown | :error, _}` result and is
  not restored, so the same Link is never declared twice.
- An entry is removed when the host admits `detach/3` or `end_session/2`, or
  when the peer detaches or ends with an error. A peer refusal is final; the
  host has already received the existing `{:session, sid, {:detached, link,
  error}}` event.
- Stored data: Session identity and its `open_session` options; Link
  identity `{name, role}` and its `attach/5` options (address, terminus
  options, `max_message_bytes`). No message bytes, credit, delivery or
  credential is stored. Size is bounded by the existing `max_sessions` (16)
  × `max_links` (16).

After a successful Open on a new generation, the shell restores the intent.
Sessions are reopened in ascending identity order. Each Session's Links are
re-attached in their original admission order with the same identity and
options. Restoration uses internal operation references whose completions go
to the shell, not to a host process. These references count against
`max_operations`, and at most `min(8, free operations)` are outstanding, so
host commands are never starved. The observer receives:

```elixir
{:graviton_mq, client, {:disconnected, reason}}           # existing
{:graviton_mq, client, {:reconnecting, attempt}}          # new; only when a retry is scheduled
{:graviton_mq, client, {:connected, remote_container_id}} # existing, on every connect
{:graviton_mq, client, {:restored, {:session, sid}, result}}
{:graviton_mq, client, {:restored, {:link, sid, link_identity}, result}}
{:graviton_mq, client, {:restore_complete, %{restored: n, failed: m}}}
{:graviton_mq, client, {:reconnect_exhausted, attempts}}  # new; only if max_attempts > 0
```

`result` is `{:ok, _}`, `{:error, reason}` or `{:unknown, reason}`, as for
host operations. A failed Session or Link is dropped from intent, so the host
decides whether to re-declare it. If no `{:reconnecting, _}` follows a
`{:disconnected, _}`, the owner has stopped. `status/1` reports this too.

Restored automatically: Connection, SASL, TLS, Sessions, Links.
Requires host action: receiver credit (`request/5`), resending any
`{:unknown, _}` publication, settling redelivered messages (new references),
and re-declaring failed restorations.

### 3.6 SASL and TLS on reconnect

- Every attempt builds a new `SASL.Machine` with the new generation. A
  password provider runs once per attempt, in the owner, before the SASL
  deadline is armed (S2 behaviour unchanged). Its result lives only in the
  machine until sasl-init has been written. A provider can return a rotated
  secret. A provider that raises or returns a non-binary gives
  `:invalid_sasl_credentials`, which is terminal (§3.3).
- TLS performs a full, verified handshake on every attempt: chain, hostname
  and SNI as configured. S3 configures no TLS session reuse. A changed or
  untrusted server certificate on reconnect is a terminal TLS alert.
  `client/transport.ex` is reused without change.

### 3.7 Bounded resources and privacy

- One retry timer, generation/token fenced and cancelled by `close/1`,
  observer DOWN and `terminate/2`. One socket at a time; the old socket is
  closed before the retry is scheduled (MQTT "release first" order).
- Intent is bounded (§3.5). Internal restoration operations are bounded by
  `max_operations`. Nothing grows per attempt.
- `status/1` adds `phase`, `generation`, `attempt`, `next_retry_ms`
  (remaining time or `nil`), `intent: %{sessions: s, links: l}` and
  `restoring: n`. It shows no host, credential, address or payload.
  `format_status/1` stays redacted, and the process stays `:sensitive`.
- Events carry only the reason classes in §3.3. Peer error descriptions
  already appear in `{:peer_close, error}` as today. No credential, provider
  result or TLS material appears in any event, reason or crash report.

### 3.8 Pure layer impact

None is required. Reconnect calls
`Endpoint.new(Keyword.put(endpoint_options, :generation, g))` with a fresh
`Stream`. `endpoint.ex` and `session_engine.ex` keep their "no reconnect or
resume" moduledocs; those statements stay true for the pure layer, and
reconnect is shell policy. If a stale-delivery check needs the token's
generation, the shell compares it before stepping the Endpoint.

## 4. Part B — asynchronous Manager → Queue requests

### 4.1 Options considered

| Option | Verdict |
| --- | --- |
| **(a) `:gen_server.send_request/2` from the Manager with a per-queue FIFO pending table and deadline** | **Recommended.** The Queue keeps its single `handle_call({:execute, table, token})`. The Manager stays the caller, so the queue's `registration_controller/3` and `lease_controller/4` checks hold unchanged. `send_request` monitors the queue and replies through an alias (OTP 28, `.tool-versions`), so queue death is observed and late replies to abandoned requests are dropped (`check_response/3` → `:no_reply`, then the catch-all `info/2`) |
| (b) One proxy process per queue that performs the blocking call | Rejected. The proxy becomes the caller, which breaks lease-controller identity and the anchor design. It adds a hop, and the proxy itself blocks |
| (c) Casts to the queue with reply references | Rejected. It duplicates monitor and alias handling, changes the queue protocol, and drops the existing abandoned-request semantics |

### 4.2 Request exchange and the race classification

- The Manager owns one `:public` ETS exchange table per queue for the
  lifetime of the Manager. The queue checks `:ets.info(table, :owner) ==
  caller`, unchanged. A request is inserted as `{token, {:request, message}}`,
  and the queue writes `{token, {:result, result}}`. `queue.ex`
  `take_request/3` and `put_result/3` change only to recognise these
  wrappers. The current unwrapped format cannot tell a pending request from
  a written result (§4.6).
- When a request's deadline expires, the Manager does an atomic
  `:ets.take(table, token)`:
  - `{:request, _}`: the queue never took it, and it will never execute
    (**definitively not executed**).
  - `{:result, r}`: it executed and the reply is still on its way, so `r` is
    used as a normal reply (**known**).
  - `[]`: the queue took it and has not finished (**uncertain**).

  Exactly one of the Manager's take and the queue's take wins. This replaces
  today's single `:unknown_outcome` timeout result with three outcomes, and
  each consumer below handles each one explicitly.
- If the Manager dies, its tables die with it. Any request the queue has not
  yet taken becomes `:abandoned`. The killed anchor makes the queue return
  every claim, so at-least-once is preserved exactly as today.

### 4.3 Ordering and pending state

- Erlang guarantees FIFO delivery between two processes, and the queue serves
  calls in mailbox order. Requests from the Manager to one queue therefore
  execute in issue order. Replies are handled in arrival order, which is the
  same order. This preserves the "final outcomes before Connection DOWN
  cleanup" rule: the settle request is issued before the cleanup
  `disconnect`, so the queue applies it first. No reordering buffer is
  needed.
- A new pure tracker `GravitonMQ.Runtime.AMQP10.QueueRequests` keeps a FIFO
  of pending entries per queue: `%{request_id, token, kind, key, route_token,
  data, deadline}`. Its functions are add, resolve, expire-head and
  abandon-all, and it returns decisions, not effects. It is unit-tested
  without processes.
- Because deadlines within one queue are monotonic, one timer per queue is
  armed for the head entry's deadline. There is no periodic polling, which
  matches the project's no-polling input rule.
- The deadline is 5,000 ms, the same value as today's call timeout, as an
  internal constant. It is the point at which the Manager fences.
- Bounds:
  - each route has at most one outstanding queue request (`route.queue_ref`,
    alongside the existing `busy` Connection operation);
  - each queue has at most one `status` and one `watch`/`watch_ack` request;
  - cleanup `disconnect` entries are bounded by removed routes per deadline
    window;
  - a global `max_queue_requests` cap (default 4,096, internal) refuses new
    Link admission with `amqp:resource-limit-exceeded` and defers pump work
    while full.

### 4.4 Per-path behaviour (numbers refer to §2.3)

| Path | New behaviour | Reply ok | Not executed (deadline, take won) | Uncertain (taken, deadline) / queue DOWN |
| --- | --- | --- | --- | --- |
| 1 boot `watch` | **stays synchronous** in `handle_continue` (runs once, before the Manager serves messages; queues have just started under `rest_for_one`). This is the only remaining blocking call, and it is documented | anchor | — | `start_probe` |
| 2 public status | Manager replies at once with its counters and, per queue, `:unavailable` (fenced) or the queue pid (through the token/ETS exchange; pids are not returned). `Broker.status/1`, in the **caller**, sends concurrent `send_request` status requests with an overall 1,000 ms deadline. A non-answer reports `:unresponsive` (**owner Q6**). No fence | map | `:unresponsive` | `:unresponsive` |
| 3 `watch_ack` | async | `schedule` | fence | fence |
| 4 `register` | route phase `:registering`; attach decision on reply. **New invariant:** if the route is gone or its token changed when `{:ok, gen}` arrives, issue `disconnect(consumer, gen)` so no lease is orphaned | accept Link | reject Link + fence (queue unresponsive) | reject Link + fence (existing) |
| 5 `enqueue` | async; the publishing Link keeps `credit: false, receiving: true` until the reply | disposition as today | **known failure:** settle `Released` (the message was not and will not be acted on; the sender may resend) (**owner Q5**) | close Connection without disposition (existing unknown-outcome rule) |
| 6 `settle` (peer outcome) | async; route `busy` until reply | `busy: nil` (detach if the delivery was interrupted, as today) | fence: the outcome is lost, claims return and the message is redelivered (allowed) | fence (existing) |
| 7–9 pump `status` | coalesced: one in-flight status per queue per pump wave; waiting route keys are continued on reply; `max_pump_actions` unchanged | continue route | fence | fence |
| 10 `claim` | async; route `queue_ref` set; on reply run `Forwarding.prepare` → `pending_delivery` → send, as today | as today | fence (unresponsive; nothing diverged) | fence (existing) |
| 11 unfit `settle MakeAvailable` | async | detach "message cannot fit this link" | fence | fence |
| 12 cleanup `disconnect` | the route is removed at once; the request is tracked by consumer id | done (`:stale_consumer` ok) | fence | fence (existing) |
| 13 restore `watch`+`watch_ack` | async pair after the probe answers | new anchor + `schedule` | re-probe | re-probe |

Fencing (`fence/2`) gains one step. It calls `QueueRequests.abandon_all`
for that queue: requests the Manager takes back are definitively not
executed; any others are covered by the killed anchor. Then, for abandoned
`enqueue` entries, it applies the "not executed" or "uncertain" column per
entry. Late replies from a fenced queue are ignored. A late successful
`enqueue` reply after its Connection was closed means the message is in the
queue and the publisher saw no disposition. That is the documented
unknown-outcome case, the same as today.

### 4.5 Status and observability

`amqp10_status/1` answers within its 1 s deadline even while queues are
stuck. It adds `queue_requests` (pending count) and `queue_fences`
(cumulative count). A queue's value is a status map, `:unavailable` (fenced)
or `:unresponsive` (not fenced, missed the status deadline). The
`format_status/1` redaction and the `:sensitive` flag are unchanged. Request
contents, including enqueued bytes, appear only in the exchange tables and
never in debug events. This is the same exposure as today's per-call public
tables.

### 4.6 Files

- `runtime/amqp10/broker.ex`: paths 2–13.
- `runtime/amqp10/queue.ex`:
  - `send_request/3` and `classify/2` helpers;
  - `{:request, _}`/`{:result, _}` exchange wrappers;
  - `request/2` keeps its synchronous API for tests, the probe and the
    status caller.
- New `runtime/amqp10/queue_requests.ex` (pure tracker).
- `runtime/amqp10.ex`, the status facade, only if it must forward the
  caller-side deadline.

## 5. Acceptance evidence

### 5.1 Broker (B): fault injection, local `mix test`

Test-pitfall rules, learned in S1a/S2:

- `:sys.suspend/1` blocks only ordinary messages. System messages and a
  trapped parent `EXIT` are still served (the sys suspend loop), so
  `GenServer.stop`, supervisor teardown and `:sys` calls still reach the
  process.
- Take every `Process.monitor/1` *before* inducing a fault.
- Always `:sys.resume/1` in `after`, before asserting lease release.
- Never use `:erlang.suspend_process/1`: it also blocks teardown.
- Suspending a Connection also requires suspending its Hell handlers
  (boundaries test note).
- Do not inspect the Manager with `:sys.get_state`; it is sensitive and
  redacted. Use `amqp10_status/1` and `Queue.status/1`.
- A suspended queue never *takes* its request. `:sys.suspend` alone
  therefore exercises only the "not executed" branch.

Tests:

- **B1 (update the availability test).** With `orders` suspended, settle a
  delivery. *Before* the 5 s fence:
  - a bystander publish on `other`, an attach, a delivery and a settle each
    complete within 500 ms;
  - `amqp10_status/1` returns within 1,000 ms with `"orders" =>
    :unresponsive`;
  - the Manager pid is unchanged.

  After the deadline:
  - the `orders` Link is detached with `amqp:internal-error`;
  - status shows `:unavailable`.

  After resume:
  - `ready: 2, inflight: 0, consumers: 0` (lease released, nothing lost);
  - probe restore and re-attach work.
- **B2.** Two suspended queues: status latency stays below 1 s, not 10 s.
  Both are fenced independently, and a third queue keeps working.
- **B3 (update the boundaries test).** Uncertain detach cleanup: the 7 s wait
  becomes "status within 1 s"; fencing and no-orphan assertions are kept.
- **B4: taken-but-unanswered branch.** A test-only stand-in queue child takes
  the ETS request and then blocks. It is a GenServer registered as the
  `{Queue, "orders"}` child of a test parent supervisor, read by
  `handle_continue(:wire)` through `Supervisor.which_children/1`. Assert:
  - an uncertain `claim` and `settle` fence;
  - an uncertain `enqueue` closes only the publishing Connection, with no
    disposition and no fence.
- **B5.** "Not executed" `enqueue`: a suspended queue plus a publish. The
  publisher receives `Released` (or the Q5 alternative). After resume the
  queue contains no copy.
- **B6.** A late `register` reply after the route was removed (peer detaches
  during `:registering`) leaves `consumers: 0`.
- **B7.** Manager kill with pending requests: the queue returns all claims,
  and abandoned requests do not execute (`messages` unchanged).
- **B8.** Pure `queue_requests_test.exs`: FIFO, head-deadline arming, the
  three-way classification, abandon-all and the cap.
- Wire behaviour is unchanged except Q5. The existing Proton → broker suites
  (`amqp10_*proton*`, durable queue path) are rerun as independent-peer
  requalification in hosted CI.

### 5.2 Client (A): local and Artemis interop

Local (`amqp10_client_test.exs` or a new `amqp10_reconnect_test.exs`):

- config validation;
- backoff timing bounds;
- classification table cases;
- `{:error, :disconnected}` / `:restoring` admission;
- `:stale_delivery`;
- a no-socket, no-timer stopped owner;
- `close/1` while offline;
- observer death while offline;
- the provider is called once per attempt;
- privacy (`privacy_test.exs`: password, provider result and host absent
  from status, events and crash reports);
- TLS reconnect against the existing OTP `:ssl` test peer
  (`tls_transport_test.exs`).

Self-to-self against our broker counts only as extra evidence.

Interop (`@moduletag :artemis_interop`, new
`amqp10_artemis_reconnect_test.exs`). It uses a new **multi-accept** TCP
proxy, `test/support/amqp_cut_proxy.exs`. The existing single-accept
`amqp_outcome_loss_proxy.exs` stays untouched. The proxy has these modes:

- `relay`;
- `cut` (close both sockets);
- `refuse` (accept and close at once, or stop listening);
- `blackhole` (stop relaying and keep the sockets);
- `cut_on: :disposition` (drop the first broker Disposition, then cut).

It reports `{:proxy_connection, n}`. Artemis 2.41.0 is the shared run.py
instance: security enabled, persistence disabled. Proton 0.40.0 provides the
independent senders and receivers.

| ID | Case | Pass criterion |
| --- | --- | --- |
| R1 | `cut` while active | `{:disconnected, :closed}`, `{:reconnecting, 1}`, `{:connected, _}`; Session and Links `{:restored, …, {:ok, _}}`; a new publication is Accepted by Artemis and received by a Proton receiver |
| R2 | Delivery received and unsettled at `cut` | old reference → `{:error, :stale_delivery}`; after host `request/5` Artemis redelivers the same bytes; Accept empties the queue (checked by Proton) |
| R3 | `cut_on: :disposition` during a send | `{:unknown, _}`; no Transfer crosses the proxy after reconnect until the host sends; Proton drain finds the message at most once |
| R4 | `refuse` for k attempts, then `relay` | `{:reconnecting, 1..k+1}`; measured gaps ≥ the backoff formula (tolerance documented); success after recovery |
| R5 | SASL PLAIN over verified TLS through the proxy (`host: "localhost"`) | the provider is invoked once per attempt; authentication on reconnect; restoration succeeds |
| R6 | `blackhole` with a short `idle_timeout_ms` | idle-timeout detection → reconnect (redelivery is not asserted here: Artemis keeps the half-open connection until its own TTL) |
| F1 | permanent `refuse`, `max_attempts: 3` | three `{:reconnecting, n}`, `{:reconnect_exhausted, 3}`, `phase: :stopped`, no socket or timer |
| F2 | provider returns a wrong password on the 2nd attempt | `{:disconnected, {:sasl, :auth}}`; no further `{:reconnecting, _}` |
| F3 | `close/1` during the backoff wait | no further connection reaches the proxy |

Restarting Artemis itself (**owner Q7**) is not required by default: the
proxy covers transport loss deterministically without disturbing the shared
instance.

## 6. Risks

- **Stale peer Link under the same name (half-open).** After a blackhole
  loss, Artemis may still hold the old Link with the same name and container
  id until its connection TTL expires. Re-attach may then be refused, or
  redelivery may be delayed. Mitigation: the refusal is reported per Link
  through `{:restored, …, {:error, _}}`. R6 asserts only detection, and the
  limit is documented.
- **Duplicate publications after `{:unknown, _}`.** These are inherent to
  at-least-once. Mitigation: documentation, R3 evidence, and no automatic
  replay.
- **Reconnect storms.** Many clients reconnecting at once and in phase (there
  is no jitter, matching MQTT). Mitigation: the bounded budget and cap; jitter
  is offered as Q3.
- **Async Manager complexity.** New windows appear, such as register replies
  for removed routes and late enqueue replies. Mitigation: the pure tracker
  with explicit classification, invariant tests B4–B7, and one request per
  route.
- **Exchange-format change in `queue.ex`.** The other synchronous callers
  (probe, tests, status caller) go through the same `request/2`, so the
  change stays inside one module.
- **Status semantics change.** The new `:unresponsive` value could surprise
  hosts that match only `:unavailable` or maps (Q6).
- **Conflict with concurrent doc edits.** Documentation updates must wait for
  the agent currently editing CAPABILITIES, ACTIVE_SCOPE, the AMQP/MQTT docs,
  the clause audits and the plan logs (§7).

## 7. File ownership and task sketch

S1b owns the broker listeners, `HellHandoff`, and the Connection/Writer TLS
paths. S3 **does not edit** `runtime/amqp10/{listener,connection,writer}.ex`,
`runtime/mqtt/*`, `hell_handoff*`, `client/transport.ex`, `mix.exs`,
`mix.lock` or any pure `graviton_mq_amqp10` module.

| Area | S3 files |
| --- | --- |
| Broker | `runtime/amqp10/broker.ex`, `runtime/amqp10/queue.ex`, new `runtime/amqp10/queue_requests.ex`, possibly `runtime/amqp10.ex` (status facade); tests `apps/graviton_mq/test/graviton_mq/amqp10_{availability,boundaries}_test.exs`, new `apps/graviton_mq_runtime/test/amqp10/queue_requests_test.exs`, `queue_test.exs` additions |
| Client | `client/amqp10.ex`, `client/amqp10/config.ex`, new `client/amqp10/intent.ex`, `amqp10_client.ex` (docs/types); tests `amqp10_client_test.exs` or new `amqp10_reconnect_test.exs`, `privacy_test.exs`, `tls_transport_test.exs` (additive), new `amqp10_artemis_reconnect_test.exs`, new `test/support/amqp_cut_proxy.exs` |
| Docs (last, after the concurrent doc agent finishes) | `docs/{AMQP10_CLIENT,AMQP10_BROKER,FAILURE_MODEL,CAPABILITIES}.md`, requirement inventory and clause audit, with executed evidence only |

Tasks (each test-first). Track B (T1–T4) and track A (T5–T9) touch disjoint
apps and may run in parallel. No two agents write the same file.

- **T1** Pure `QueueRequests` tracker + B8.
- **T2** `queue.ex` exchange wrappers + async helpers; `queue_test.exs`.
- **T3** Manager conversion in three steps:
  - (a) settle, claim and pump status;
  - (b) register and cleanup disconnect;
  - (c) enqueue, watch_ack, restore and the public status path.
- **T4** B1–B7 and the updated boundary tests.
- **T5** Client config `reconnect` + validation.
- **T6** Shell reconnect loop: classification, generation, fresh
  Endpoint/Stream/SASL, timers, phases, `:stale_delivery`.
- **T7** `Intent` + restoration + events.
- **T8** Local client tests (privacy, TLS, stopped-owner cleanup).
- **T9** Cut proxy + Artemis R1–R6 and F1–F3.
- **T10** Full required sequence from AGENTS.md, then hosted CI interop run
  for evidence. Docs are updated after the concurrent doc edits land.

## 8. Compatibility

- Default `reconnect: [max_attempts: 0]` leaves client behaviour unchanged.
- New observer events appear only when retries are configured or a reconnect
  happens; `{:restored, …}` follows any reconnect.
- `{:connected, _}` keeps its shape.
- Broker wire behaviour is unchanged except Q5 (`Released` for a definitively
  non-executed enqueue).
- `amqp10_status/1` gains `queue_requests`, `queue_fences` and the
  `:unresponsive` queue value (Q6).
- The documented limit "the first unanswered queue call still blocks the
  Manager for 5 s" is removed. The fence still happens at the 5 s deadline,
  but only the affected queue's Links wait.

## 9. Open questions for the owner

1. **Restoration model.** Recommended: the client restores the Sessions and Links the host successfully declared, from bounded in-memory intent, and reports each result (`{:restored, item, result}`, `{:restore_complete, …}`). Alternative: host-driven re-declaration after `{:connected, _}`, with a `{:topology_lost, %{sessions, links}}` event, like MQTT's `{:subscriptions_lost, grants}`. That is a smaller client, but every host must re-implement the same replay.
2. **Receiver credit after restoration.** Recommended: not restored automatically; the host calls `request/5` again after `{:restored, {:link, …}, {:ok, _}}`. Reservations and flow stay explicit, and there is no surprise burst of redeliveries. Alternative: automatically re-grant the unconsumed remainder of the previous grant. More convenient, but it reserves bytes and slots the host did not re-confirm.
3. **Retry policy bounds and jitter.** Recommended: mirror MQTT exactly (`max_attempts` 0..100, default 0; exponential `delay`/`max_delay`; no jitter), so both clients share one documented policy. Alternative: allow `max_attempts: :infinity` and add optional jitter. Better for long-running services and herd avoidance, but it diverges from the MQTT client unless MQTT is changed too.
4. **Peer Close classification.** Recommended: retry on a peer Close carrying `amqp:connection:forced` and on an unrequested peer Close without error (typical broker shutdown/restart); treat every other peer error condition as terminal. Alternative: treat every peer Close as terminal. Safer against a peer that deliberately rejects the client, but a broker restart then needs host intervention.
5. **Enqueue that definitively never executed (deadline, request taken back).** Recommended: settle the incoming Transfer with `Released`. The message was not acted on, the Connection survives, and the sender may resend (at-least-once). Alternative: keep today's behaviour and close the publishing Connection without a disposition. Uniform and conservative, but it discards knowledge the broker now has and drops all of the Connection's Links.
6. **Public status value for a slow but unfenced queue.** Recommended: a new `:unresponsive` value, distinct from `:unavailable` (fenced), returned when the queue misses the 1 s status deadline. Alternative: report `:unavailable` for both. No new value, but operators cannot tell "fenced" from "currently slow".
7. **Artemis restart evidence.** Recommended: qualify reconnect with the deterministic multi-accept proxy against the shared Artemis instance (R1–R6, F1–F3). Alternative or addition: run.py starts a second, dedicated Artemis instance that the test stops and restarts. Stronger "real broker restart" evidence (`amqp:connection:forced` path), at the cost of CI time, run.py changes and a second set of ports.
