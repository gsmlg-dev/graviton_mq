# ADR 0009: Owned network stack (`hell` and `ex_ssl`)

- Status: Accepted by the owner, 2026-11-12
- Date: 2026-11-12
- Implementation/acceptance: S1a implemented locally and uncommitted on `main`
  (after `a9fd9cd`); independent-peer requalification NOT RUN (requires CI);
  local packaging (seven archives, three installed consumers) PASS on a rerun; the first local run failed once in the embedding consumer's durable recovery script ("installed recovered MQTT subscription did not complete", 5 s timeout) while synchronous disk writes took 57-86 ms; the committed baseline passed once. Design and spike record:
  [P0–P3 / S1 design](../superpowers/specs/2026-11-12-http-fetch-transport-and-p0-p3-design.md);
  plan: [S1a owned transport](../superpowers/plans/2026-11-12-s1a-owned-transport.md)

## Context

Until 0.3.0 the umbrella had no third-party dependency. Broker listeners owned a
`:gen_tcp`/`:ssl` listen socket and polled `transport_accept`; the client
wrapped `:gen_tcp`/`:ssl`. The owner chose the network packages of
`gsmlg-dev/http_fetch` as the project's network stack: `hell` (TCP/TLS server,
a Thousand Island fork) and `ex_ssl` (TLS). AGENTS.md previously treated only
standard Elixir/OTP networking and TLS as infrastructure and excluded
speculative runtime dependencies.

A throwaway spike (`hell`/`ex_ssl` `1.0.0-rc.13`, telemetry 1.4.2, Elixir
1.18.5/OTP 28, TLS 1.3 mTLS on loopback; design §3.5.1) found:

- a single non-owner Writer can send on an accepted ex_ssl server socket while
  the handler receives; concurrent senders get `{:error, :busy}`;
- `Hell.Socket.peercert/1` returns the client leaf under `verify_peer`;
- a TLS 1.2-only client is refused by the TLS 1.3-only ex_ssl server;
- Hell re-arms `active: :once` after every `handle_data/3`, so pausing input
  from a Hell handler needs a "drain-pause" workaround
  ([gsmlg-dev/http_fetch#162](https://github.com/gsmlg-dev/http_fetch/issues/162),
  needed);
- **blocker:** an ex_ssl server whose write is blocked by a non-reading peer
  stops receive processing and socket calls; OTP `:ssl` keeps reading
  ([gsmlg-dev/http_fetch#161](https://github.com/gsmlg-dev/http_fetch/issues/161)).
  Mutual-write MQTT/AMQP traffic would deadlock into a `send_timeout` close.

## Decision

`hell` and `ex_ssl` are owner-authorized infrastructure, like OTP networking.
They are the first third-party dependencies and are pinned exactly:
`graviton_mq_runtime` declares `{:hell, "== 1.0.0-rc.13"}` (bringing `ex_ssl`
and telemetry 1.4.2 transitively); `graviton_mq_client` declares
`{:ex_ssl, "== 1.0.0-rc.13"}`. The umbrella now has a `mix.lock`. The
Ranch/Bandit/Phoenix/Ra/Khepri exclusion is unchanged for everything else.
Dependency policy tests pin this exact external set.

S1 is split:

- **S1a (this decision's implementation).** Broker plain-TCP listeners (MQTT
  and AMQP 1.0) accept through a Hell server using `Hell.Transports.TCP`. Hell
  owns accept only. `GravitonMQ.Runtime.HellHandoff`, the Hell handler, starts
  the protocol Connection under the existing `:connections`
  DynamicSupervisor, transfers socket ownership, sends `{:accepted, raw}` and
  monitors the Connection, living exactly as long as it. Connection, Writer,
  `active: :once` arming and backpressure are unchanged. Because input is never
  read in a Hell handler, the drain-pause workaround for #162 is not needed for
  this layout; it replaces the earlier "byte pump" layout of the design.
  Client TLS uses ex_ssl `SSL`; client plain TCP stays on `:gen_tcp`; the
  client no longer uses OTP `:ssl`.
- **S1b (blocked on #161).** Broker TLS listeners stay on OTP `:ssl` until a
  published release fixes #161, the pin is updated and the reproduction and
  TLS interop suites pass. Once S1b lands the broker is TLS 1.3-only (ex_ssl
  has no TLS 1.2 server); that is a documented breaking change.

The client offers `versions: [:"tlsv1.3", :"tlsv1.2"]` by default, preserving
the pre-ex_ssl compatibility; a caller-supplied `versions` wins.

## Consequences

Listener failure semantics change for plain-TCP listeners: Hell closes its
handlers' sockets when the server exits and a stopping handler stops its
Connection, so a TCP listener failure closes that listener's live
connections. The protocol owner (MQTT Broker, AMQP Manager), queues and
retained messages survive. TLS listeners, still on OTP `:ssl`, keep their
previous behaviour. Listeners are isolated from one another under a generic
one_for_one `GravitonMQ.Runtime.ListenerSupervisor` (child `:listeners`,
started only when listeners are configured, last child of each protocol's
rest_for_one supervisor). More than 3 listener crashes within 5 s escalate to
a restart of that protocol's listeners only; the owner is untouched.

Client TLS options that ex_ssl does not support fail at connect with an
explicit error, for example
`{:error, {:option, :server_only, :honor_cipher_order}}`; they are never
silently dropped.

`mix graviton_mq.check_architecture` (xref/AST based) allows `Hell`/`Hell.*`
only in `graviton_mq_runtime` and ex_ssl `SSL`/`SSL.*` only in runtime and
client. Protocol applications reject both as transport modules; core, storage
and the public composition cannot reference them; `graviton_mq_client` may
not reference OTP `:ssl`.

Hex staging now passes external version requirements through unchanged and
rejects other non-umbrella dependency forms; archives and installed consumers resolve `hell`/`ex_ssl`
from Hex. Packaging evidence: local packaging (seven archives, three installed consumers) PASS on a rerun; the first local run failed once in the embedding consumer's durable recovery script ("installed recovered MQTT subscription did not complete", 5 s timeout) while synchronous disk writes took 57-86 ms; the committed baseline passed once.

Operational budgets and limits (detailed in the `HellHandoff` source comment):

- Rebinding a fixed port retries `:eaddrinuse` every 20 ms for at most 1 s,
  covering a restart that races the previous Hell tree's socket close. The
  retry runs in listener init and blocks the `ListenerSupervisor` (and
  `GravitonMQ.status/1`) meanwhile; a genuine conflict fails start explicitly.
- Shutdown: Connection release 5 s plus 0.5 s kill wait; handler drain 6 s;
  residual `Hell.stop` 7 s; listener child shutdown 14 s. N TCP listeners stop
  sequentially, so worst-case wall time is N × 14 s.
- Listener wiring does not block: it uses `send_request` to the protocol
  supervisor. The connection spec is published in a listener-owned
  `:protected` ETS table with allowlisted keys only; handlers accepted before
  wiring wait for it.
- `nodelay: false` preserves the pre-Hell `:gen_tcp` behaviour.
- Known limits: ordering of the handler's `{:accepted, raw}` before the port's
  first `{:tcp, …}` is not formally guaranteed by Erlang (two senders),
  although locally it is practically safe; a `:kill`ed handler skips its
  terminate and leaves its Connection outside Hell's accounting.

Evidence (2026-11-12, local, uncommitted): full umbrella `mix test` with 0
failures; client TLS 1.3/1.2 tests against an OTP `:ssl` peer pass. Independent
peers (Mosquitto, Paho, Artemis, Proton) are NOT RUN; they require CI.
