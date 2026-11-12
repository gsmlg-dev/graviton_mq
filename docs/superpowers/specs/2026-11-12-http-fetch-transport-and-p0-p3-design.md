# P0–P3 programme and S1 owned-transport migration design

Date: 2026-11-12. Status: design approved by the owner (approach A); the
written spec awaits owner review. **S1a: implemented locally, uncommitted,
peer interop pending CI** (amended 2026-11-12, see §3.3 and §3.5.1; recorded in
[ADR 0009](../../adr/0009-owned-network-stack.md)). S1b is blocked on
gsmlg-dev/http_fetch#161. Commit, push, version bump, release and publication
are **not** authorized by this design.

## 1. Owner decisions recorded here

| Decision | Choice |
| --- | --- |
| Programme | Implement the P0–P3 review plan of 2026-11-11 as sub-projects S0–S7 |
| Network stack | Use the owner's `gsmlg-dev/http_fetch` packages: `hell` (TCP/TLS server), `ex_ssl` (TLS) |
| WebSocket server | Provided by `gsmlg-dev/gaoxy` (`gaoxy_http` WebSock upgrade); client by `http_web_socket` |
| S1 approach | A: broker TCP/TLS on native `Hell.Handler`; client TLS on `SSL.connect`; client plain TCP stays on `:gen_tcp` |
| TLS versions | TLS 1.3 only on the broker (ex_ssl has no TLS 1.2 server); breaking change, documented. Takes effect with S1b |
| Client TLS versions (2026-11-12) | Client offers `[:"tlsv1.3", :"tlsv1.2"]` by default (pre-ex_ssl compatibility); a caller `versions` wins |
| Broker listener layout (2026-11-12) | Hell owns plain-TCP accept only and hands the socket to the existing Connection (§3.3); a listener failure closes its own connections |
| UDP / QUIC | Out of scope; the MQTT QUIC TODO stays deferred |

These decisions amend AGENTS.md's "no speculative runtime dependencies" rule
for exactly the packages above. Hell is a Thousand Island fork; the existing
explicit Ranch/Bandit/Phoenix exclusion is unchanged for everything else.
ADR 0009 records the amendment (S1 deliverable).

## 2. Programme decomposition

Each sub-project gets its own spec → plan → implementation → evidence cycle and
updates `docs/CAPABILITIES.md` only with executed evidence. Network-role
changes keep the independent-peer rule: both directions, self-to-self alone
does not qualify.

| # | Sub-project | Depends on | Exit evidence |
| --- | --- | --- | --- |
| S0 | Hygiene: deflake Writer DOWN assertions, make the compaction test independent of fsync latency, fix stale CAPABILITIES/ACTIVE_SCOPE statements, add a current-status summary, file Low review findings as GitHub issues | — | Full required sequence on a slow-fsync host; issue links |
| S1 | Owned transport migration (this document §3) | S0 | §3.7 |
| S2 | AMQP SASL ANONYMOUS/PLAIN, client and broker | S1 | Artemis and Proton with SASL both directions; bad-credential refusal |
| S3 | AMQP client reconnect; remove the 5 s Manager block on an unanswered queue call | S1 | Fault-injection tests; Artemis reconnect |
| S4 | MQTT Topic Alias, Subscription Identifiers, shared subscriptions (`$share`) | S1 | Mosquitto/Paho both directions per feature |
| S5 | Throughput/latency baseline, WAL group commit preserving durable-through acceptance, `:telemetry` events, AMQP TTL/absolute-expiry | S1 | Benchmark record; durability fault tests unchanged; TTL interop |
| S6 | MQTT over WebSocket: client via `http_web_socket`, broker via `gaoxy_http` | S1 | Paho WS client → broker; our WS client → Mosquitto WS |
| S7 | MQTT 3.1.1 runtime on both roles | S1, S4 | Mosquitto/Paho in 3.1.1 mode both directions |

Parallelism: after S1, S2/S3 (AMQP files) and S4 (MQTT files) touch disjoint
files and may proceed concurrently. S5 touches storage and both runtimes and
runs after S2–S4 merge. S6 and S7 touch MQTT runtime and run sequentially after
S4. The coordinating agent owns integration and the final required sequence.

Out of scope for the whole programme: clustering/Raft, management UI,
cross-protocol bridge, AMQP transactions, AMQP 0-9-1, QUIC/UDP.

## 3. S1 — owned transport migration

### 3.1 Goal

Replace the hand-written broker listeners/accept loops and all `:ssl` use with
the owner's network stack, with no change to protocol semantics, connection
state machines, backpressure, durability or public broker/client APIs other
than the TLS 1.3 restriction.

### 3.2 Current state (2026-11-12, `868b5b4`)

- Broker: `runtime/mqtt/listener.ex` and `runtime/amqp10/listener.ex` own a
  `:gen_tcp`/`:ssl` listen socket and poll `transport_accept` (50 ms). Accepted
  sockets go to `Connection` (owner, `active: :once` input) and `Writer`
  (separate process, sends). The MQTT connection reads `:ssl.peercert/1` for
  `verify: :verify_peer` principals.
- Client: `client/transport.ex` wraps `:gen_tcp`/`:ssl` connect, send,
  `active: :once` and close.
- The umbrella has no third-party dependencies.

### 3.3 Design

**Broker listeners — accepted layout (2026-11-12, S1a).** Each configured
plain-TCP MQTT/AMQP listener starts one `Hell` server (`Hell.Transports.TCP`)
linked to its `Listener` process. Hell owns accept only. The runtime handler
`GravitonMQ.Runtime.HellHandoff` starts the protocol Connection under the
existing `:connections` DynamicSupervisor, transfers socket ownership, sends
it `{:accepted, raw}` and monitors it, living exactly as long as it. The
Connection arms `active: :once` and keeps its Writer, backpressure and close
handling unchanged. Because no input is read in a Hell handler, the
drain-pause workaround for gsmlg-dev/http_fetch#162 is not needed for this
layout. TLS listeners stay on OTP `:ssl` until S1b.

Listener failure (owner decision): Hell closes handler sockets when it exits
and a stopping handler stops its Connection, so a TCP listener failure closes
that listener's live connections; the owner, queues and retained messages
survive. Listeners sit under a generic one_for_one
`GravitonMQ.Runtime.ListenerSupervisor` (child `:listeners`, only when
listeners are configured, last child of each protocol's rest_for_one
supervisor), so a sibling listener and its connections are unaffected; more
than 3 listener crashes in 5 s restart all of that protocol's listeners, never
the owner. Budgets and known limits are in ADR 0009.

**Broker listeners — original proposal (superseded by the layout above).**
Each configured MQTT/AMQP listener becomes one
`Hell` server started by the existing per-instance runtime supervisor, using
`Hell.Transports.TCP` or `Hell.Transports.SSL`. The Hell handler module is a
thin runtime-owned adapter per protocol that:

1. on `handle_connection/2` starts (or becomes) the existing Connection owner
   and Writer, handing them a transport handle;
2. forwards `handle_data/3` bytes to the Connection exactly as the current
   `{:tcp | :ssl, socket, data}` messages are handled today;
3. uses `handle_info/2` for owner messages, so broker-initiated output
   (subscriber delivery, AMQP flow/transfer) keeps its current path;
4. maps Hell close/error/timeout callbacks to the Connection's existing
   transport-loss handling.

Whether the Connection logic runs inside the Hell handler process or remains a
separate process is decided by the S1 plan after the §3.5 spike; the
constraint is that Connection/Writer state machines, limits and
backpressure tests remain unchanged.

**Transport adapter** (not introduced in S1a; see §3.5.1 decision 2).
Runtime and client each get one small module exposing
`send/2`, `close/1`, `peername/1`, `peercert/1` and (client only) `connect/1`
and `active_once/1` over `{:tcp, socket}` / `{:tls, socket}`. Connection and
Writer code calls only the adapter. No adapter function returns success for an
operation it did not perform.

**Client.** `:ssl.connect/send/setopts/close` become `SSL.connect/send/setopts/close`
from ex_ssl (TLS 1.3 and its bounded TLS 1.2). Plain TCP stays on `:gen_tcp`.
Hostname and CA verification remain mandatory where configured today.

**Configuration** (S1b; broker TLS listeners are unchanged in S1a). Broker
TLS options are validated against the ex_ssl server
subset before any socket opens: `certfile`/`keyfile`, `cert`/`key`,
`certs_keys`, `password`, `cacerts`/`cacertfile`, `verify`,
`fail_if_no_peer_cert`, `depth`, `ciphers`, `alpn_preferred_protocols`,
`sni_hosts`/`sni_fun`, and `versions` only as `[:"tlsv1.3"]`. Other
previously accepted keys fail with an explicit `{:error, {:unsupported_tls_option, key}}`
configuration error; they are never silently dropped.

### 3.4 Architecture rules

- `mix graviton_mq.check_architecture` gains rules (xref/AST based, not grep):
  `Hell.*` only in `graviton_mq_runtime`; `SSL.*` only in runtime and client;
  core, storage, AMQP 1.0 and MQTT keep forbidding all transport modules.
- Dependency declarations: `graviton_mq_runtime` adds `hell` (brings `ex_ssl`,
  `telemetry`); `graviton_mq_client` adds `ex_ssl`. Exact `==` version pins to
  the published coordinated release in use when S1 starts (currently
  `1.0.0-rc.13`).
- The application graph stays acyclic; `graviton_mq_client` still references no
  runtime/storage/composition module.

### 3.5 Spike before implementation (blocking check)

Prove with a throwaway test, not product code:

1. A non-owner process (our Writer) may call `SSL.send/2` on an accepted
   ex_ssl server socket concurrently with the Hell handler receiving.
2. `Hell.Socket.peercert/1` returns the client leaf under `verify: :verify_peer`.
3. Hell exposes enough to pause input for backpressure (equivalent of not
   re-arming `active: :once`).

If any check fails, the S1 plan records it, the upstream-issue-routing policy
applies (http_fetch is a covered organization), and a confirmed blocker stops
S1 without a local workaround.

### 3.5.1 Spike results (2026-11-12)

Throwaway project `/tmp/gmq-spike`, `hell`/`ex_ssl` `1.0.0-rc.13`, telemetry
1.4.2, Elixir 1.18.5/OTP 28, TLS 1.3 with mTLS on loopback. Three base checks
passed 5/5 runs; the 18-test extended suite passed 5/5 with different seeds.

| Check | Result |
| --- | --- |
| (a) Writer sends on accepted ex_ssl socket while handler receives | PASS: 200+200 and 5,000 × 1 KiB each direction, exact and ordered (≈1 s). Concurrent senders are rejected with `{:error, :busy}` (`ex_ssl/lib/ssl.ex:141-147`, `ssl/server_connection.ex:331-363`), not queued, so exactly one process may write |
| (b) `Hell.Socket.peercert/1` under `verify: :verify_peer` | PASS: returns the client leaf DER; a client without a certificate is reset before `handle_connection/2` |
| (c) pause/resume input | Naive `setopts(active: false)` from `handle_info/2` FAILS under continuous input (341–350 chunks leaked) because Hell re-arms `active: :once` after every `handle_data/3` return (`hell/lib/hell/handler.ex:706-727`). "Drain-pause" PASSES: `active: false`, then selectively receive already-delivered `{:ssl, raw, data}` into a stash, read timeout `:infinity`; resume processes the stash, then `active: :once`. 0 bytes while paused; 40 toggles mid-stream, every byte once, in order |
| TLS 1.2-only OTP `:ssl` client | Refused: client `{:tls_alert, {:protocol_version, …}}`; server emits `[:hell, :connection, :stop]` with `tls13_required`; no `handle_connection/2` |
| **Blocked server write** | **FAIL (upstream):** `SSL.ServerConnection` writes with `:gen_tcp.send/2` inside its `gen_statem` (`server_connection.ex:9-11`, `send_application/3`). While the Writer is blocked by a non-reading peer the server delivers no input within 5 s and the handler's `setopts` call blocks > 5 s. OTP `:ssl` keeps reading in this state |

Upstream routing (policy `upstream-issue-routing`):

- [gsmlg-dev/http_fetch#161](https://github.com/gsmlg-dev/http_fetch/issues/161)
  — Bug, **blocker**: ex_ssl server blocked write stops receive processing and
  socket calls. Mutual-write MQTT/AMQP traffic would deadlock into a
  `send_timeout` close, a regression from OTP `:ssl`. No local workaround
  (the owner chose ex_ssl-only TLS).
- [gsmlg-dev/http_fetch#162](https://github.com/gsmlg-dev/http_fetch/issues/162)
  — Feature, **needed**: Hell handler result to pause input without automatic
  re-arm. Drain-pause was the planned temporary workaround for the byte-pump
  layout; the accepted accept-only layout (decision 1) does not need it, so
  no `WORKAROUND(upstream)` code exists.

Decisions for S1:

1. **Process layout:** keep the separate Connection owner and single Writer.
   Reasons: only one sender is allowed; a blocked write must not freeze
   protocol state, timers or keepalive. *Amended 2026-11-12:* the Hell handler
   is not a byte pump. It owns accept only and hands the socket to the
   Connection (§3.3), so the #162 drain-pause workaround is not needed for this
   layout; #162 stays open for any future in-handler input. A listener failure
   closes that listener's connections; siblings are isolated.
2. **Split:** S1a migrates broker plain-TCP listeners to `Hell.Transports.TCP`,
   introduces the transport adapters (not needed in the accepted layout:
   Connections keep the raw `:gen_tcp` socket, and the client keeps its
   existing `Client.Transport`), client TLS on `SSL.connect` (after
   verifying the client-side duplex-read behaviour, `ssl/connection.ex:1003-1022`,
   under the same non-reading-peer test), architecture rules, ADR 0009 and the
   AGENTS.md amendment. *Status 2026-11-12: implemented locally, uncommitted;
   full `mix test` 0 failures; independent-peer interop pending CI; local packaging (seven archives, three installed consumers) PASS on a rerun; the first local run failed once in the embedding consumer's durable recovery script ("installed recovered MQTT subscription did not complete", 5 s timeout) while synchronous disk writes took 57-86 ms; the committed baseline passed once.* The client offers TLS 1.3 and 1.2 by default; unsupported caller
   TLS options fail at connect with an explicit error. **S1b** (broker TLS on `Hell.Transports.SSL`) is
   **blocked on #161**: resume only after a published release fixes it, the
   pin is updated and the reproduction plus TLS interop suites pass. Until then
   broker TLS listeners keep their current implementation unchanged.
3. Pausing input must also set the Hell read timeout to `:infinity`
   (`Hell.Handler.set_read_timeout/2`) and restore it on resume; the default
   60 s read timeout would otherwise close a paused connection.

### 3.6 Error handling

- Handshake failures, TLS alerts and certificate rejections close only that
  connection and surface through existing listener/connection telemetry and
  logs, without logging credentials or packet bytes (existing privacy tests
  continue to apply).
- A Hell listener that fails to bind fails `GravitonMQ.start_link/1` with an
  explicit error, as today.
- Shutdown drains through Hell's shutdown path; existing cleanup tests must
  still observe closed sockets and released timers.

### 3.7 Exit evidence

- Full AGENTS.md sequence with zero warnings and no cycles.
- All existing runtime/client tests, adapted only where they constructed raw
  `:ssl` sockets.
- Independent peers on TLS 1.3: Mosquitto/Paho → broker (TCP and TLS, mTLS
  principal), client → Mosquitto, client → Artemis (verified TLS, wrong CA and
  wrong hostname rejected), Proton → broker (TCP and verified TLS).
- New negative test: a TLS 1.2-only client is refused by the broker.
- Seven archives and three clean consumers, now resolving `hell`/`ex_ssl` from
  Hex; the client-only consumer still installs no broker runtime/storage.
- ADR 0009, AGENTS.md dependency amendment, CAPABILITIES rows and an upgrade
  note stating the TLS 1.3-only broker change.

### 3.8 Known upstream observation

`gaoxy` 0.15.x reports `tls_ex_ssl: :unsupported` in `Gaoxy.capabilities/0`
while its locked `hell` 1.0.0-rc.13 SSL transport runs on ex_ssl. S1 does not
depend on gaoxy; S6 must resolve this through the upstream-issue-routing policy
before relying on gaoxy TLS for `wss`.
