# S1b — Broker TLS listeners on `Hell.Transports.SSL` / `ex_ssl`

Date: 2026-11-13. Status: **approved by the owner 2026-11-13** (decisions
below); nothing here is implemented. Programme row: [P0–P3 design §2, S1](2026-11-12-http-fetch-transport-and-p0-p3-design.md)
(§3.3 configuration, §3.5.1 decision 2, §3.7 exit evidence). Records:
[ADR 0009](../../adr/0009-owned-network-stack.md) (S1b unblocked by `ex_ssl`
rc.14 fixing gsmlg-dev/http_fetch#161). Authorized by the G0–G7 Goal in
`docs/ACTIVE_SCOPE.md` and the S1 approach A owner decision. Commit, push,
version bump, release and publication are **not** authorized by this design.
Source facts below were read from the locked `hell`/`ex_ssl` `1.0.0-rc.14`
sources in `deps/` and from the tree at `2d6ad27`.

## Amendment 2026-11-13 — upstream fixed (supersedes conflicting text below)

`hell`/`ex_ssl` are pinned to **1.0.0-rc.17** (commit after `57e7914`), which
fixes both S1b prerequisites; the coordinator verified each with a throwaway
behavioural test before pinning:

- **gsmlg-dev/http_fetch#168 fixed.** Hell redacts `password`, `key`,
  `sni_fun` and nested `certs_keys`/`sni_hosts` secrets in listener telemetry
  (`Hell.ServerConfig.redact_transport_options/1`). Therefore: **do not**
  reject in-memory private keys and **do not** add the
  `WORKAROUND(upstream): gsmlg-dev/http_fetch#168`; `TLSOptions` accepts
  `key`/`cert` (top level, in `certs_keys` and `sni_hosts`) like the other
  ex_ssl server options. Keep passing `password` as a closure (defence in
  depth, cheap). T9 (privacy) must cover an in-memory key and a password:
  neither may appear in `[:hell, :listener, :start|:stop]` metadata, Listener
  / Connection / HellHandoff status, or crash logs.
- **gsmlg-dev/http_fetch#169 fixed.** ex_ssl server option
  `active_chunk_size` (positive integer, default `:infinity`; accepted by
  `listen` and `setopts`) bounds every active-once delivery. Therefore: always
  set `active_chunk_size` for TLS listeners to the protocol's per-delivery
  bound equivalent to plain TCP — 16_384 bytes (the `buffer` the TCP listener
  uses), so existing input-limit semantics are unchanged. T6 (pipelining while
  slow) is a **normal** test that must pass: **no** `:requires_http_fetch_169`
  tag, no test_helper exclusion, no "expected failure" step; it asserts that no
  delivered chunk exceeds 16 KiB and that a well-behaved pipelining client is
  not disconnected. `active_chunk_size` is owned by the runtime (not a user
  TLS option) — reject it in user `tls_options` with
  `{:unsupported_tls_option, :active_chunk_size}`.
- Coordinator task C6 reduces to: none for #168/#169 (done by this amendment).
- **Release block stays** until S1b is qualified in hosted CI (owner decision
  2026-11-13, `docs/ACTIVE_SCOPE.md`).

## Owner decisions (2026-11-13)

- Q2 coalesced TLS input: **option B** — keep the MQTT `max_packet_size + 16 KiB`
  and AMQP `max_input_bytes` semantics unchanged; U2 filed as
  [gsmlg-dev/http_fetch#169](https://github.com/gsmlg-dev/http_fetch/issues/169)
  (blocker for S1b **qualification**). S1b may be implemented, but broker TLS
  on ex_ssl is not qualified or enabled until #169 ships, the pin is updated
  and the pipelining-while-slow regression plus TLS interop pass.
- Q1, Q3 and all other recommendations accepted: in-memory private keys are
  rejected with `{:unsupported_tls_option, :key}` and `password` is passed as a
  closure until U1 —
  [gsmlg-dev/http_fetch#168](https://github.com/gsmlg-dev/http_fetch/issues/168)
  (needed) — is fixed (`WORKAROUND(upstream): gsmlg-dev/http_fetch#168`); the
  runtime may no longer reference OTP `:ssl` (AGENTS.md amendment,
  `extra_applications: [:crypto]`).

## 1. Goal and non-goals

Goal: broker TLS listeners (MQTT and AMQP 1.0) accept through a `Hell` server
using `Hell.Transports.SSL` (ex_ssl), with the same accept-only `HellHandoff`
layout as S1a TCP; the OTP `:ssl` accept loop and every OTP `:ssl` reference in
`graviton_mq_runtime` are removed. Protocol state machines, Writers,
backpressure, durability and the public broker API are unchanged except for the
TLS restrictions in §6.

Owner decisions already recorded (P0–P3 §1, ADR 0009) and applied here:

- The broker is **TLS 1.3 only** (ex_ssl has no TLS 1.2 server). This is a
  documented breaking change (§6).
- Hell owns accept only and hands the socket to the existing Connection; a
  listener failure closes its own connections, siblings are isolated.

Non-goals:

- TLS 1.2 on the broker, session tickets/resumption, 0-RTT, renegotiation,
  client-certificate revocation (CRL/OCSP) — none exist in the ex_ssl server.
- ALPN- or SNI-based routing, MQTT-over-443 ALPN multiplexing, AMQP TLS-upgrade
  header (protocol id 2), SASL EXTERNAL (S2 Q12 stays deferred; this design
  only settles the certificate source it would use).
- WebSocket/`wss` (S6), QUIC.
- Changing the client: it already uses ex_ssl and offers TLS 1.3+1.2.
- Fixing the S1a known limit "two-sender ordering of `{:accepted, raw}` vs the
  first data message" (§4.1); it applies unchanged to TLS.

## 2. Current state (`2d6ad27`)

- `runtime/{mqtt,amqp10}/listener.ex`: `:tcp` → `HellHandoff.start_link/4`;
  `:tls` → `:ssl.listen/2` (mqtt:44, amqp10:58) and a polled
  `:ssl.transport_accept/2` loop (mqtt:150–180 with 50 ms, amqp10:169–200 with
  25 ms) that starts the Connection under `:connections`, transfers ownership
  and sends `{:accepted, socket}`. `terminate/2` closes the listen socket only,
  so today a TLS listener crash leaves its accepted connections running.
- `runtime/{mqtt,amqp10}/connection.ex`: on `{:accepted, socket}` the TLS
  Connection runs `:ssl.handshake(socket, connect_timeout)` itself; uses
  `:ssl.setopts/peername/peercert/close`; handles `{:ssl, s, d}`,
  `{:ssl_closed, s}`, `{:ssl_error, s, r}` (AMQP also `{:ssl_passive, s}`). The
  MQTT Connection calls the Broker's `connection_open` in `init/1`, i.e. before
  the handshake; AMQP reports a failed handshake as endpoint event
  `{:disconnected, {:handshake, reason}}`. AMQP `configure_socket/1` sets
  `[:binary, packet: :raw, active: false, send_timeout: t, send_timeout_close: true]`.
- `peer/1` (mqtt:184, amqp10:522) reads `:ssl.peercert/1`;
  `certificate_verified = certificate != nil and client_certificate_verified`,
  the latter computed by the Listener from `tls_options[:verify] == :verify_peer`.
- Writers (`mqtt/writer.ex:109`, `amqp10/writer.ex:111`) send with `:ssl.send/2`.
- TLS option validation: MQTT allowlists `certfile keyfile password cacertfile
  cacerts certs_keys versions verify fail_if_no_peer_cert depth ciphers
  secure_renegotiate reuse_sessions honor_cipher_order` and requires
  certfile+keyfile or `certs_keys`; AMQP accepts **any** keyword except its
  owned socket options and requires `cert|certfile` and `key|keyfile`. Both
  return a generic error (`:invalid_mqtt_options`, `:invalid_listener`).
- Other OTP `:ssl` references in runtime/composition lib code: **none** besides
  the files above (`architecture.ex` only names `:ssl` as data). Runtime
  `mix.exs` has `extra_applications: [:ssl]` (pinned by `release_test.exs:95`),
  and runtime lib calls `:crypto` directly (MQTT broker, AMQP queue/broker/persistence).
- `architecture.ex:82`: `@forbidden_otp_transport_by_app %{graviton_mq_client: [:ssl]}`;
  `architecture_test.exs:657` asserts "client may not reference OTP :ssl; runtime still may".

## 3. Verified dependency behaviour (rc.14 source)

| Question | Answer (source) |
| --- | --- |
| Where does the TLS handshake run? | In the **Hell handler process**, in `Hell.Handler.handle_ready/5` (`hell/lib/hell/handler.ex:537–571`), before `handle_connection/2`, bounded by the server option `handshake_timeout` (default 5_000, `server_config.ex:41`). A failure stops the handler with `{:shutdown, {:handshake, reason}}`; `handle_connection/2` is never called. |
| Ownership chain | `SSL.transport_accept/2` starts the TLS server process with the **caller (acceptor) as owner** (`ssl.ex:354–358`); the acceptor transfers to the handler (`hell/connection.ex:57`); so the handler is the owner when `handle_connection/2` runs. |
| Can the handler transfer to our Connection? | Yes. `SSL.controlling_process/2` on a server socket succeeds only when **called by the current owner** (`server_connection.ex:241–256`, else `{:error, :not_owner}`; dead target → `:noproc`). `HellHandoff.hand_off/4` already calls `socket.transport_module.controlling_process(socket.socket, connection)` from the handler, so it works unchanged. The TLS process monitors its owner and closes (close_notify) when the owner dies (`:302–311`). |
| Messages to the owner | `{:ssl, %SSL.Socket{}, binary}` (`:899–902`), `{:ssl_closed, socket}` after close_notify, `{:ssl_error, socket, reason}` otherwise (`:877–878`; a TCP EOF without close_notify is `:econnreset`, reported after buffered plaintext drains, #158). No `{:ssl_passive, _}` (no `{:active, N}`). |
| Active mode | `setopts` accepts only `active: false | :once`, `send_timeout`, `send_timeout_close: true` and mutable carrier options (`nodelay keepalive sndbuf recbuf buffer tos tclass priority`) (`options.ex:204–210`, `tcp_options.ex:4`). `:binary`/`mode`/`packet` are **rejected** by `setopts`. `setopts` has **no owner check**. |
| Active-once chunk size | **Coalesced.** While the application is not armed the server keeps reading the carrier until ≈ 1 MiB − 64 KiB of plaintext is buffered (`@max_plaintext 1_048_576`, `@rearm_reserve 65_536`, `arm?/2` `:880–883`); one active-once delivery sends the **whole** buffer (`deliver/1` `:899`). `:gen_tcp` and OTP `:ssl` deliver roughly one carrier read (bounded by `buffer: 16_384`). See §4.3. |
| Concurrent send | One write at a time. A reservation made while another write is in flight **waits** for it; one made while a write is reserved but not yet sent returns `{:error, :busy}` (`server_connection.ex:374–391`, `ssl.ex:142–152`). A blocked write no longer stops receive delivery or `setopts/peercert/close` (#161 fixed; ADR 0009 3/3 reproduction). `send_timeout` (carrier default 30 s, `send_timeout_close: true`) closes the connection. |
| `peercert` | Returns the **validated** client leaf DER, or `{:error, :no_peercert}` (`:453–456`; set from the authenticated handshake core, `:692`). `verify: :verify_none` sends no CertificateRequest. `connection_information/1` reports `protocol`, `selected_cipher_suite`, `session_resumption` and `peer_authenticated: :client | false` (`:458–464`). |
| Server option subset | Own keys `certfile keyfile cert key password certs_keys alpn_preferred_protocols ciphers versions sni_hosts sni_fun verify cacerts cacertfile fail_if_no_peer_cert depth` (`server_options.ex:28–42`); a list of OTP-only keys → `{:unsupported_option, key}`; `versions` ≠ `[:"tlsv1.3"]`, `session_tickets` ≠ `:disabled`, truthy `reuse_sessions` → unsupported; `depth` must be > 0; **every other unknown key is passed to `:gen_tcp`** (`:108`, `:151`). `password` may be a 0-arity fun (`client_identity.ex:291`). `sni_fun` runs in a helper bounded by the handshake deadline. |
| Telemetry/state | `Hell.Listener` emits `[:hell, :listener, :start]` with metadata `transport_options: server_config.transport_options` (`hell/listener.ex:32–40`, documented in `telemetry.ex:20–47`), retains it as `listener_span.start_metadata` in its GenServer state (`telemetry.ex:410–416`) and re-emits it on `:stop` (`:447`). Hell processes define no `format_status/1`. See §4.10. |
| TLS 1.2-only client | Refused before `handle_connection/2` (spike §3.5.1: client alert `protocol_version`; `[:hell, :connection, :stop]` with `tls13_required`). |

## 4. Design

### 4.1 Layout and lifecycle (single path for TCP and TLS)

```text
ListenerSupervisor (one_for_one) ── Listener (GenServer, per configured listener)
                                     └─ linked Hell server (TCP or SSL transport)
                                          acceptors ── HellHandoff handler (one per socket)
:connections DynamicSupervisor ── Connection ── Writer (linked)
TLS only: SSL.ConnectionSupervisor ── SSL.ServerConnection ── ConnectionWriter
```

1. Listener `init/1` builds Hell options from the validated listener and calls
   `HellHandoff.start_link/4` for **both** transports. `HellHandoff` gains the
   transport module, the validated TLS options and the per-transport Hell
   server options (§4.8); everything else (ETS publication, `:connection_spec`
   wait, rebind retry, drain/release/stop budgets) is unchanged.
2. A TLS acceptor accepts; the handler performs the handshake under
   `handshake_timeout`. Failure: the handler stops, Hell closes the socket, no
   Connection is started and the protocol owner sees nothing (§5).
3. On success `handle_connection/2` starts the Connection, transfers ownership
   (handler is the owner, so `SSL.controlling_process/2` succeeds), sends
   `{:accepted, %SSL.Socket{}}`, monitors the Connection and returns
   `{:continue, state, {:persistent, :infinity}}` exactly as for TCP. Hell then
   calls `setopts(active: :once)` from the handler; this is permitted (no owner
   check) and delivers to the new owner — the same harmless early arm as TCP.
4. The Connection treats `{:accepted, socket}` for `:tls` as an already
   handshaken socket: `handshake/3` becomes `{:ok, socket}` for both
   transports and is deleted together with the now-dead AMQP
   `{:handshake, reason}` branch.
5. Lifetime and release are TCP's: the handler lives exactly as long as its
   Connection; Hell stop/handler exit → `SSL.close/1` from the handler (close
   needs no ownership), which sends the owner no `{:ssl_closed, _}`, so
   `HellHandoff.release/1` stops the Connection itself (already implemented).
   A Connection exit closes the TLS process via its owner monitor.
6. `Listener.info/1` uses `Hell.listener_info/1` for both transports; the
   `:accept` loop, `:ssl.listen/2`, `:ssl.sockname/1`, `:ssl.close/1` and the
   `transport`-specific `terminate/2` and `handle_call(:connection_spec, …)`
   guards are deleted.

Known limit carried over from S1a (unchanged, now also TLS): `{:accepted, s}`
comes from the handler while the first `{:ssl, s, d}` comes from the TLS
process; Erlang does not formally order two senders. The handler's
`setopts` call is causally after its `send/2`, which is practically safe on
one node; a Connection whose socket is still `nil` drops such data.

### 4.2 Connection transport calls

| Operation | TCP (unchanged) | TLS (new) |
| --- | --- | --- |
| arm input | `:inet.setopts(s, active: :once)` | `SSL.setopts(s, active: :once)` |
| AMQP `configure_socket` | `[:binary, packet: :raw, active: false, send_timeout: t, send_timeout_close: true]` | `[active: false, send_timeout: t, send_timeout_close: true]` (ex_ssl rejects `:binary`/`packet`; the carrier is already binary raw) |
| peer address | `:inet.peername/1` | `SSL.peername/1` |
| peer certificate | — | `SSL.peercert/1`: `{:ok, der}` → `der`; `{:error, :no_peercert}` / other → `nil` |
| close | `:gen_tcp.close/1` | `SSL.close/1` |
| Writer send | `:gen_tcp.send/2` | `SSL.send/2` |

The message clauses already match `{:ssl, s, d}`, `{:ssl_closed, s}` and
`{:ssl_error, s, r}` against the stored socket; a `%SSL.Socket{}` compares by
value, so they work unchanged. The AMQP `{:ssl_passive, _}` arm becomes
unreachable for TLS and stays only for TCP's tuple shape (it is one clause
shared with `:tcp_passive`). `transport_reason/1` already folds unknown error
shapes to `:transport_error` and `{:tls_alert, {a, _}}` to `{:tls_alert, a}`.

### 4.3 Input bound under coalesced active-once delivery (new finding)

Both Connections bound held input **including the arriving chunk**: MQTT
rejects `buffer + chunk > max_packet_size + 16_384` with `:parser_limit`;
AMQP rejects `chunk + input + stream.buffer > max_input_bytes`
(64 KiB–16 MiB, default 1 MiB) with `:input_buffer_limit`. Those bounds assume
a chunk ≤ one carrier read (`buffer: 16_384`). ex_ssl delivers up to ≈ 1 MiB in
one message after a Connection lagged behind a pipelining peer (§3). With
defaults, or with a smaller `max_packet_size`/`max_input_bytes`, a
well-behaved peer can therefore be disconnected for a local limit it did not
exceed. This must be resolved before S1b qualifies (Q2):

- **Option B (recommended):** route an `ex_ssl` request (§8 U2) for a server
  receive bound — deliver at most one carrier read/record per active-once
  message (as OTP `:ssl` does) and/or a configurable plaintext buffer — and
  keep both Connections' limits unchanged. S1b implementation proceeds; S1b
  qualification waits for the release and pin update.
- **Option A (fallback):** keep ex_ssl as is and change both Connections to
  bound the held bytes **before** appending the chunk (held input is never
  re-armed above the limit, so memory stays bounded), documenting the TLS
  per-connection worst case ≈ limit + 2 MiB (one delivered chunk plus ex_ssl's
  own read-ahead) instead of limit + kernel buffer.

Either way a new regression test (§7.2 T6) pipelines > 1 MiB of small packets
to a suspended Connection over TLS and requires every packet processed, in
order, with no limit close.

### 4.4 Output

The single Writer remains the only sender. It never races another sender, so
`:busy` is unexpected; it is still mapped like any other send error
(`{:error, _}` → `{:stop, {:shutdown, :socket_write_failed}}`, write result
reported to the owner) and never retried or dropped silently. The carrier
`send_timeout`/`send_timeout_close: true` are set at listen time
(`HellHandoff` transport options, as for TCP) and by AMQP `configure_socket`;
the existing Writer credit limits and `send_timeout` semantics are unchanged.
The Connection keeps reading while its Writer is blocked (rc.14 #161 fix,
regression test T5).

### 4.5 Peer identity and mTLS principal

`peer/1` keeps its map shape `%{ip, port, transport, certificate,
certificate_verified}`. `certificate` comes from `SSL.peercert/1`, which only
ever yields a chain-validated leaf; `certificate_verified` stays
`certificate != nil and client_certificate_verified` (listener `verify:
:verify_peer`). Under ex_ssl both conditions coincide, so semantics match OTP:
optional certificates (`fail_if_no_peer_cert: false`) give `nil/false` when
absent and `der/true` when presented. Tests additionally assert
`SSL.connection_information/1` `peer_authenticated: :client` exactly when
`certificate_verified` is true (no product dependency on that key). The AMQP
`sasl.client_certificate_verified` flag is computed the same way and remains
unused until a later EXTERNAL decision. Client chains get **no revocation
check**; the docs state it.

### 4.6 SNI and ALPN

`sni_hosts`, `sni_fun` and `alpn_preferred_protocols` are accepted because
ex_ssl supports them (multiple identities, certificate selection); the broker
does not read `negotiated_protocol/1` or the SNI name and does not route on
them. `sni_fun` is bounded by the handshake deadline. A default identity is
still required (§4.7), so SNI cannot leave a listener without a certificate.

### 4.7 TLS option validation (both protocols, one shared private module)

Validation runs in `Config.build/1` before any socket opens; ex_ssl validates
content again at listen.

| Key | Accepted value (structural check) |
| --- | --- |
| `certfile`, `keyfile`, `cacertfile` | non-empty binary or charlist path |
| `cert` | DER binary |
| `key` | ex_ssl key term (see Q1 for the in-memory-key privacy decision) |
| `certs_keys` | non-empty list (≤ 16) of maps with only `cert certfile key keyfile password` |
| `password` | binary/charlist, or 0-arity fun; wrapped in a closure before reaching Hell (§4.10) |
| `cacerts` | non-empty list of DER binaries |
| `verify` | `:verify_none` or `:verify_peer`; `:verify_peer` requires `cacerts` or `cacertfile` |
| `fail_if_no_peer_cert` | boolean; only with `verify: :verify_peer` |
| `depth` | integer 1..100 |
| `ciphers` | non-empty duplicate-free list; ex_ssl must resolve each entry to a TLS 1.3 suite (`tls_aes_128_gcm_sha256`, `tls_aes_256_gcm_sha384`, `tls_chacha20_poly1305_sha256`), checked at listen; the plan fixes the accepted entry forms after reading `Capabilities.matches?/3` |
| `versions` | exactly `[:"tlsv1.3"]` |
| `alpn_preferred_protocols` | non-empty duplicate-free list of 1..255-byte binaries |
| `sni_hosts` | list of `{charlist, identity_keyword}` with identity keys only |
| `sni_fun` | 1-arity fun |

Rules: a default identity is required (`certfile`+`keyfile`, `cert`+`key`,
or `certs_keys`); `certfile` alone is not accepted (keeps today's rule).
Results, surfaced through `GravitonMQ.start_link/1`:

- any other key (including OTP-only keys, `verify_fun`, `partial_chain`,
  `crl_check`, `honor_cipher_order`, `secure_renegotiate`, `reuse_sessions`,
  `session_tickets`, and every socket option such as `active`, `mode`,
  `packet`, `header`, `deliver`, `packet_size`, `send_timeout`,
  `send_timeout_close`, `buffer`, `ip`) → `{:error, {:unsupported_tls_option, key}}`;
- `versions` other than `[:"tlsv1.3"]` → `{:error, {:unsupported_tls_option, :versions}}`;
- a known key with a malformed value, a missing identity or a dependent key
  without its prerequisite → `{:error, {:invalid_tls_option, key}}`.

The allowlist matters beyond compatibility: ex_ssl forwards unknown keys to
`:gen_tcp`, so without it a TLS option could change carrier framing. Error
terms carry the key atom only, never values. Listen-time content failures
(unreadable file, bad key, wrong password) keep today's
`{:listener_setup_failed, :tls}` start failure. MQTT drops its three OTP-only
keys; AMQP moves from a denylist to the allowlist (§6).

### 4.8 Timeouts and pre-handshake bounds

- `handshake_timeout` (Hell server option) = the protocol's `connect_timeout`
  (MQTT default 3_000; AMQP default 2_000, bounded 1..3_000) — the same value
  the Connection used for `:ssl.handshake/2`. It also bounds the server flight
  write and `sni_fun`. Because the Connection now starts after the handshake,
  MQTT's CONNECT timer (Broker `connect_timeout` from `connection_open`) and
  AMQP's `accept_timer` start after it; a TLS client gets at most
  2 × `connect_timeout` to finish TLS plus CONNECT/header (documented).
- `num_connections` (Hell server option) for TLS listeners = the protocol's
  `max_connections` (the `:connections` DynamicSupervisor `max_children`).
  Today the OTP accept loop starts a Connection per accepted TLS socket, so
  in-progress handshakes are bounded by `max_connections`; Hell's default
  16_384 would let one listener run 16_384 concurrent handshakes (a handler
  and a TLS server process each). At the limit Hell's acceptor retries (5 × 1 s) instead of
  accepting, leaving excess clients in the backlog. TCP listeners keep the
  S1a value.
  *Amendment (Task 5, 2026-11-12):* Hell applies `num_connections` per
  acceptor (listener-wide maximum = `num_acceptors × num_connections`).
  HellHandoff therefore gives each of its 4 acceptors
  `ceil(max_connections / 4)`, bounding concurrent TLS connections,
  handshakes included, at `max_connections + 3` (exact when divisible by 4).
  Established Connections remain capped at `max_connections` by the
  `:connections` DynamicSupervisor.
- `read_timeout: :infinity` and `shutdown_timeout` stay as in S1a.

### 4.9 Listener failure, rebind and shutdown

All S1a semantics now apply to TLS listeners: a TLS listener failure closes
that listener's live connections (previously they survived); the protocol
owner, queues, sessions and retained messages survive; siblings are isolated;
> 3 crashes in 5 s restart that protocol's listeners only. The rebind retry
(20 ms, ≤ 1 s, `:eaddrinuse` only) applies; the plan must confirm the TLS
start error shape matches
`{:shutdown, {:failed_to_start_child, :listener, :eaddrinuse}}` (ex_ssl
returns the `:gen_tcp.listen/2` error unchanged). Shutdown budgets are reused:
Connection release 5 s + 0.5 s kill, handler drain 6 s, residual `Hell.stop`
7 s, listener child shutdown 14 s, N listeners ≤ N × 14 s. `SSL.close/1` is an
`:infinity` call into the TLS process; it is reached only inside those bounded
release paths.

### 4.10 Privacy

Our processes: Listener state keeps only the allowlisted non-secret config
(MQTT `Map.take`, AMQP `@connection_keys`); TLS options exist only in the
listener `init/1` closure argument and are passed to Hell. Handler and
Connection states hold `%SSL.Socket{}` (pid, ref, atomics — no key material);
they are `:sensitive` and redact `format_status/1`. ex_ssl's TLS process is
`:sensitive` with redacted status; `SSL.ListenSocket` derives `Inspect` with
only its carrier.

Finding (§8 U1): Hell exposes the raw `transport_options` — including
`password`, in-memory `key` DER and `certs_keys` keys — in
`[:hell, :listener, :start | :stop]` telemetry metadata and in the
`Hell.Listener` GenServer state (so in its crash report and `:sys.get_state/1`),
and in its supervisor child specs. In-scope mitigation without upstream
changes: always pass `password` to Hell as a 0-arity closure (ex_ssl accepts
it; a fun prints as `#Function<…>`). In-memory private keys cannot be wrapped;
Q1 decides how S1b treats them until Hell redacts. `certfile`/`keyfile` paths
are not secrets. A privacy test (T9) attaches a telemetry handler to
`[:hell, :listener, :start]`, forces a listener crash under `capture_log`, and
asserts the password bytes and key DER appear in neither.

### 4.11 Architecture rules and application declarations

After S1b no runtime or composition lib module references OTP `:ssl`. Proposed
(Q3, an AGENTS.md amendment): `@forbidden_otp_transport_by_app` becomes
`%{graviton_mq_client: [:ssl], graviton_mq_runtime: [:ssl]}`, with the
violation text "graviton_mq_runtime may not reference :ssl (use Hell/SSL)";
`architecture_test.exs:657` is updated to assert the runtime violation; the
rule stays xref/AST-based. Runtime `application/0` replaces
`extra_applications: [:ssl]` with `[:crypto]` (direct `:crypto` calls; the plan
verifies the compiler's dependency check under `--warnings-as-errors`), and
`release_test.exs` pins the new list. Tests may keep using OTP `:ssl` as an
independent peer (`Application.ensure_all_started(:ssl)` in the test).

## 5. Error handling and observability changes

- Handshake failures, alerts, TLS 1.2-only clients and certificate rejections
  close only that socket inside Hell; they no longer start a Connection, call
  the MQTT Broker's `connection_open` or emit the AMQP endpoint event
  `{:disconnected, {:handshake, reason}}`. They are observable through Hell's
  `[:hell, :connection, :stop]` telemetry (error metadata carries alert atoms
  only). No new log line is added (no credential or byte content is logged).
- Post-handshake transport loss keeps today's mapping (`{:ssl_closed, _}` →
  `:closed`; `{:ssl_error, _, r}` → sanitized reason).
- A listener that fails to bind or load its identity fails
  `GravitonMQ.start_link/1` as today.

## 6. Compatibility and upgrade note (breaking)

To be written to the next release note under `docs/releases/` and to
`docs/MQTT_BROKER.md` / `docs/AMQP10_BROKER.md` by the S1b plan (not by this
design):

- Broker TLS listeners negotiate **TLS 1.3 only**; TLS 1.2 (and older)
  clients are refused during the handshake. The client library is unchanged.
- TLS options outside §4.7 fail start with `{:unsupported_tls_option, key}`;
  previously MQTT accepted `secure_renegotiate`, `reuse_sessions`,
  `honor_cipher_order`, and AMQP passed any OTP `:ssl` option (for example
  `verify_fun`, `partial_chain`, `crl_check`, TLS 1.2 `versions`, `depth: 0`).
  `{:invalid_tls_option, key}` replaces the generic error for malformed values.
- No session resumption: every reconnect performs a full handshake.
- No client-certificate revocation checking.
- A TLS listener failure now closes its own live connections.
- TLS handshake failures are no longer reported to the protocol owner.
- Per-connection TLS memory: see Q2 outcome.

## 7. Verification

### 7.1 Tests to migrate

| Test | Change |
| --- | --- |
| `runtime/test/amqp10/connection_test.exs` "TLS handshake with a silent peer respects the finite connection timeout" | Becomes a listener-level test (both protocols): a silent TCP peer to a TLS listener is closed within `connect_timeout` (+ margin), no child appears under `:connections`, and the owner records no connection. The Connection-level handshake path no longer exists. |
| `runtime/test/amqp10/connection_input_test.exs` "a TLS connection is driven by active-once input" | Server side uses `SSL.listen/2` → `SSL.transport_accept/2` → `SSL.handshake/2` in the test process (the owner), then `SSL.controlling_process/2` to the Connection; the OTP `:ssl` client peer stays. A shared test helper builds the server socket. |
| `runtime/test/{amqp10,mqtt}/config_test.exs` TLS cases | The owned-socket-option cases now expect `{:unsupported_tls_option, key}`; add allowlist, `versions`, value and prerequisite cases (§4.7). |
| `runtime/test/hell_handoff_test.exs` | Parametrize handoff, release-on-stop, slow-child release, refusal, pre-publication wait and rebind over `Hell.Transports.TCP` and `Hell.Transports.SSL`. |
| `graviton_mq/test/graviton_mq/architecture_test.exs:657`, `release_test.exs:95` | Per §4.11. |
| Independent-peer suites (`mqtt_mosquitto_test.exs`, `amqp10_proton_test.exs`, `amqp10_sasl_proton_test.exs`, `amqp10_sasl_test.exs`) | No source change expected; they now exercise the ex_ssl server. RSA-2048 test certificates are fine (ex_ssl signs `rsa_pss_rsae_*`). |

### 7.2 New local tests (runtime / composition)

- T1 ownership: after handoff `SSL.controlling_process/2` from the handler
  returns `{:error, :not_owner}`; from the Connection it succeeds; Connection
  exit closes the TLS socket (peer sees close).
- T2 TLS 1.2-only OTP `:ssl` client (`versions: [:"tlsv1.2"]`) is refused with a
  `protocol_version` alert; no Connection started.
- T3 mTLS facts: no certificate / trusted certificate / untrusted CA under
  `verify_peer` with `fail_if_no_peer_cert` false and true; `peer/1` facts and
  `connection_information` agree (§4.5).
- T4 handshake budget: silent peer and stalled-mid-handshake peer closed within
  `connect_timeout`; `num_connections` bound holds (`max_connections` + 1
  concurrent handshakes: the extra one is not accepted until one finishes).
- T5 #161 regression at product level: a non-reading TLS peer blocks the
  Writer; the peer's later bytes (MQTT PINGREQ/DISCONNECT, AMQP Close) are still
  received; the connection closes by `send_timeout`, never hangs.
- T6 coalesced input (§4.3): > 1 MiB of small pipelined packets/frames to a
  `:sys.suspend`ed Connection, then resume; all processed in order, no limit
  close; an oversize packet/frame is still rejected.
- T7 TLS listener crash closes its connections; sibling TCP/TLS listener and
  its connections survive; owner state survives.
- T8 TLS rebind after a listener kill on a fixed port succeeds within budget.
- T9 privacy (§4.10): no password/key bytes in our `format_status`, logs on
  listener crash, or (after U1) Hell listener telemetry metadata.

### 7.3 Independent-peer acceptance (hosted CI; NOT RUN until then)

All against the broker, TLS 1.3, each recording the broker-side
`connection_information` `protocol: :"tlsv1.3"` and the peer's `openssl version`:

- Mosquitto 2.1.2 (`mosquitto_pub/sub`) → MQTT broker: verified TLS
  publish/subscribe; untrusted CA refused before CONNACK; optional client
  certificate absent/present with exact `certificate_verified` facts (existing
  tests).
- Paho 2.1.0 → MQTT broker: verified TLS round trip and mTLS principal (new
  `tls_set_context` peer cases in `mqtt_feature_paho_test.exs` or a new file).
- Proton 0.40.0 → AMQP broker: verified TLS round trip; wrong trust root
  refused (existing); SASL PLAIN over verified TLS, wrong password refused
  (existing P3/P4).
- TLS 1.2-only independent peer refused: Paho with a Python `SSLContext`
  whose `maximum_version = TLSv1_2` (definitive); `mosquitto_pub --tls-version
  tlsv1.2` only if the plan verifies it caps the version rather than setting a
  minimum.
- Our MQTT/AMQP clients → our broker over TLS stay as extra (self-to-self)
  evidence only.

CI peer TLS 1.3 support: the interop job runs on `ubuntu-24.04` (OpenSSL 3.0
from `libssl-dev`); Mosquitto is built from source `-DWITH_TLS=ON`, Proton is
built from source (`--no-binary python-qpid-proton`, `SSL.present()` asserted)
and Paho uses CPython `ssl` — all TLS 1.3 capable with default suites
(AES-GCM, ChaCha20) and groups (X25519, P-256) that ex_ssl implements. A
future OpenSSL default hybrid key share (X25519MLKEM768) would rely on
ex_ssl's HelloRetryRequest path; the recorded `openssl version` makes such a
change visible.

### 7.4 Packaging and clean consumers

`scripts/build_hex.exs`: the installed broker consumer additionally starts one
TLS listener (certificate generated with `openssl` in the staging dir), runs
one TLS round trip, and asserts `:ssl` is absent from
`Application.started_applications/0` (no hidden OTP `:ssl` runtime
dependency). The client-only consumer is unchanged and still installs no
broker runtime/storage. Seven archives and three consumers must pass.

### 7.5 Required sequence

The full AGENTS.md sequence with zero warnings and no cycles, plus the hosted
interop/soak/packaging jobs. CAPABILITIES rows, ADR 0009 status and the
upgrade note record only executed evidence.

## 8. Upstream items (route via `upstream-issue-routing` before implementation)

This design task was restricted to one file, so nothing was filed. Neither
item exists in gsmlg-dev/http_fetch (searched 2026-11-13; latest closed #167).

- **U1 — `hell`: TLS secrets in listener telemetry metadata and state.**
  `[:hell, :listener, :start|:stop]` metadata and `Hell.Listener` state carry
  raw `transport_options` (password, in-memory key DER). Request: redact or
  omit TLS identity keys (or all transport options) from telemetry metadata
  and retained span metadata, and add `format_status/1` to Hell processes.
  Proposed classification: `needed` (mitigation: password closure; Q1 for keys).
- **U2 — `ex_ssl` server: bounded active-once delivery.** A server socket
  coalesces up to ≈ 1 MiB of plaintext into one active-once message and reads
  ahead regardless of the application's arming. Request: deliver at most one
  carrier read/record per active-once message (OTP `:ssl` behaviour) and/or a
  server option for the plaintext read-ahead bound. Proposed classification:
  `needed` for S1b qualification (Q2).

## 9. Risks

- ex_ssl is experimental: its README says independent security review is
  incomplete and it is not full OTP parity. The broker's whole TLS surface
  now rests on it; there is no fallback (owner decision).
- Performance: TLS is pure Elixir over `:crypto`; each TLS connection adds two
  processes (server connection and its writer) and up to ≈ 1 MiB of plaintext
  read-ahead; no resumption means full handshakes on reconnect storms. S5's
  baseline must include TLS throughput, handshake rate and memory per
  connection; no performance claim is made by S1b.
- `peercert` semantics: validated leaf only, no revocation; `depth` default 10
  as OTP but `0` rejected.
- Peer TLS 1.3 compatibility depends on CI OpenSSL defaults (§7.3).
- The two-sender ordering known limit now covers TLS too.
- U1/U2 turnaround: S1b qualification depends on them (Q1/Q2).

## 10. Files and task sketch

- `runtime/hell_handoff.ex`: transport module and TLS options parameters;
  `handshake_timeout`; TLS `num_connections`; password closure.
- New private `runtime/tls_options.ex` (shared allowlist/validation, §4.7).
- `runtime/{mqtt,amqp10}/config.ex`: use it; error terms.
- `runtime/{mqtt,amqp10}/listener.ex`: single Hell path; delete accept loop.
- `runtime/{mqtt,amqp10}/connection.ex`, `writer.ex`: `SSL.*` calls; delete
  in-Connection handshake; AMQP `configure_socket` per transport; §4.3 per Q2.
- `runtime/mix.exs`, `graviton_mq/lib/graviton_mq/architecture.ex` and tests
  (§4.11, Q3).
- Tests §7; interop peers (Paho TLS cases); `scripts/build_hex.exs` §7.4.
- Docs after evidence: ADR 0009 S1b status, CAPABILITIES, MQTT/AMQP broker
  docs, release upgrade note, AGENTS.md (Q3).

Order: (1) option validation + config tests; (2) HellHandoff/Listener
unification with TLS handoff tests T1/T2/T4/T7/T8; (3) Connection/Writer
`SSL.*` migration and migrated tests; (4) T5/T6/T9; (5) architecture rule and
application list; (6) packaging; (7) hosted interop.

## 11. Open questions for the owner

- **Q1 — In-memory private keys until Hell redacts transport options (U1).**
  Recommended default: file U1 as `needed`; until it is fixed and pinned,
  reject an in-memory `key` (top level or in `certs_keys`/`sni_hosts`
  entries) with `{:unsupported_tls_option, :key}` (linked to U1, removed when
  the fix lands) and always pass `password` as a closure. Trade-off: hosts
  that load keys from memory (AMQP accepts `key` today; MQTT via `certs_keys`)
  must use `keyfile` temporarily, versus accepting the key and knowingly
  exposing it to Hell listener telemetry handlers and listener crash logs.
- **Q2 — Coalesced TLS input versus the Connection input limits (§4.3).**
  Recommended default: Option B — file U2 as `needed`, keep the MQTT
  `max_packet_size + 16 KiB` and AMQP `max_input_bytes` semantics unchanged,
  and hold S1b qualification until the ex_ssl fix is released and pinned.
  Trade-off: exact existing memory bounds and no limit-semantics change versus
  schedule; Option A ships without waiting but changes both limits to "held
  bytes before the chunk" and raises the TLS per-connection worst case to
  roughly limit + 2 MiB.
- **Q3 — Forbid OTP `:ssl` in `graviton_mq_runtime` (AGENTS.md amendment).**
  Recommended default: yes — add `graviton_mq_runtime: [:ssl]` to the
  xref/AST rule, update the AGENTS.md line "ex_ssl `SSL` only in runtime and
  client; the client must not reference OTP `:ssl`" to cover the runtime, and
  replace `extra_applications: [:ssl]` with `[:crypto]`. Trade-off: prevents an
  OTP `:ssl` path from silently returning and proves clean consumers start
  without `:ssl`, at the cost of needing another owner amendment if a future
  feature (for example `wss` through a third-party server) wants OTP `:ssl` in
  the runtime.
