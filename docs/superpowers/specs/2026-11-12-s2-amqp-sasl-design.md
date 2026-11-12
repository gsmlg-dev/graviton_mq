# S2 — AMQP 1.0 SASL (ANONYMOUS and PLAIN) for client and broker

Date: 2026-11-12. Status: **approved by the owner 2026-11-12** (decisions
below); nothing here is implemented. Programme row: [P0–P3 design §2, S2](2026-11-12-http-fetch-transport-and-p0-p3-design.md)
(depends on S1; exit evidence "Artemis and Proton with SASL both directions;
bad-credential refusal"). Authorized by [ACTIVE_SCOPE 2026-11-12](../../ACTIVE_SCOPE.md).
Commit, push, version bump and release are not authorized by this design.
Decisions marked **(owner)** are listed in §10 with a recommended default.

## Owner decisions (2026-11-12)

- Q7 broker header deferral: **the broker always waits for the client's
  protocol header** before sending its own (single code path), for every
  `sasl` mode.
- All other open questions (§10, Q1–Q6 and Q8–Q13): **the recommended
  default is accepted** as written in §10.

## 1. Scope

In scope:

- SASL security layer (OASIS AMQP 1.0 Part 5 §5.3) in both running roles:
  our client as SASL client (TCP client), our broker as SASL server (TCP server).
- Mechanisms ANONYMOUS (RFC 4505) and PLAIN (RFC 4616), nothing else.
- Pure codec + pure negotiation state machine in `graviton_mq_amqp10`;
  shells in `graviton_mq_runtime` (broker) and `graviton_mq_client`.
- Broker authentication callback, principal propagation into the existing
  `authorize/3` Link admission, privacy/redaction, bounds and timeouts.
- SASL over plain TCP and over the existing implicit-TLS listeners/connections
  (port 5671 style, Part 5 §5.2.1 "pure TLS server").

Non-goals (unchanged exclusions or deliberate deferrals):

- SCRAM-*, EXTERNAL, GSSAPI, DIGEST-MD5, any multi-round mechanism, and any
  SASL security layer (integrity/confidentiality wrapping). EXTERNAL is the
  natural follow-up for mTLS principals (MQTT already exposes the certificate);
  it is deferred because it needs S1b (`Hell.Socket.peercert/1` on broker TLS)
  to settle the certificate source. **(owner, Q12)**
- AMQP TLS-upgrade header (protocol ID 2, Part 5 §5.2); implicit TLS only.
- Credential storage, password hashing, account lockout, rate limiting. The
  host callback owns credential verification (constant-time comparison is its
  responsibility; documented, not enforced).
- ACL revalidation on admitted Links (unchanged limitation in AMQP10_BROKER.md).
- Client reconnect (S3). S2 must not pre-empt S3's design but must not make
  re-authentication impossible (§5.3).
- Any MQTT file (S4 runs in parallel). No new dependency; `mix.exs`/`mix.lock`
  are untouched.

TLS interplay (recommended policy, **owner Q3**): PLAIN sends the password in
clear. Broker configuration refuses to advertise PLAIN on a `transport: :tcp`
listener and the client refuses `{:plain, …}` with `transport: :tcp`, unless
an explicit `allow_insecure_plain: true` is set on that side. ANONYMOUS is
allowed on both transports.

## 2. Current state (evidence, `3407f3b` + uncommitted S1a)

| Area | Behaviour today | Source |
| --- | --- | --- |
| Protocol header | `AMQP 3 1 0 0` → `{:error, %Error{operation: :protocol_header, class: :unsupported, reason: :sasl_not_supported}}` | `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/protocol_header.ex:42-48` |
| Frame decode | type `0x01` → `:unsupported, :sasl_frame_not_supported` at offset 5 | `codec/frame.ex:179-181`; `test/codec/frame_test.exs:52` |
| Frame encode | `type: :sasl` → `:unsupported, :sasl_frame_not_supported` | `codec/frame.ex:89-90`; `test/codec/frame_encode_test.exs:208` |
| Stream | phases `:protocol_header → :frames`, one header only, no re-expect | `codec/stream.ex` |
| Descriptors | `0x40`–`0x44` already named in the terminus descriptor table (name lookup only) | `codec/terminus.ex:43-47` |
| SASL module | placeholder `GravitonMQ.AMQP10.SASL` with `@type mechanism` | `amqp10/sasl.ex` |
| Endpoint | rejects `sasl:` option (`:sasl_not_supported`); `:start` emits `AMQP 0 1 0 0` + Open in one step | `endpoint.ex:191-192, 227-233, 731-735` |
| Broker shell | on `{:accepted, socket}` steps Endpoint `:start` **immediately**, i.e. sends header+Open before reading the peer header | `runtime/amqp10/connection.ex:157-186` |
| Broker auth | `auth: :anonymous` mandatory (`:explicit_auth_required` otherwise); `authorize: fn principal, :send \| :receive, address -> :allow \| _ end`, always called with `:anonymous`, run under `:auth_tasks` Task.Supervisor (`max_auth_jobs` 1..128, `auth_timeout` 1..3000 ms), fail closed | `runtime/amqp10/config.ex:31-37, 57-60`; `broker.ex:439-502`; `supervisor.ex` |
| Broker SASL test | SASL header → `{:disconnected, :sasl_not_supported}` | `runtime/test/amqp10/connection_test.exs:404` |
| Client | config has no SASL key (`username:` rejected); SASL header from peer → disconnect | `client/amqp10/config.ex`; `client/test/amqp10_client_test.exs:41,106` |
| Redaction | both shells set `Process.flag(:sensitive, true)` and redact `format_status`; broker `protect/1` strips arguments from stacktraces | `connection.ex:43,588-623`; `client/amqp10.ex:25,422` |
| Peers | every Proton peer uses `sasl_enabled=False`; Artemis 2.41.0 created with `--allow-anonymous`, user `graviton-test`/`ephemeral-test-only`, `security-enabled=false`, TCP and TLS AMQP acceptors | `scripts/interop/run.py:109-127`; test support `*.py` |
| MQTT comparison | `auth: :anonymous \| {module, opts}`; `authenticate(credentials, opts)` / `authorize/4`; credentials carry peer `ip/port/transport/certificate/certificate_verified`; callbacks under bounded Task.Supervisor with a watcher that kills on deadline | `runtime/mqtt/auth.ex`, `runtime/mqtt/broker.ex:395-415`, `docs/MQTT_BROKER.md` |

Consequences: (a) the broker's eager header is incompatible with any SASL
server and must change (§4.1); (b) default Proton clients
(`sasl_enabled=True`) cannot connect to our broker today; (c) the requirement
inventory covers Parts 1–3 only (`TY`/`TR`/`MS`, 220 rows) — Part 5 has no IDs.

## 3. Protocol requirements (OASIS AMQP 1.0 Part 5 §5.3)

Source: `amqp-core-security-v1.0-os.html`, fetched 2026-11-12,
SHA-256 `d50d980bb55391037d6b6c306f0a715c64b09db5c880f81356b0ff53adcaca39`.
The inventory has no Part 5 rows; the IDs below are **provisional** in the
existing scheme (`<part>-<section>-b<block>-s<sentence>`, prefix `SE`) and are
re-issued by the inventory extractor when Part 5 is added (**owner Q10**).
Existing Part 2 rows whose notes change are listed at the end.

### 3.1 Header and layering

| ID (prov.) | Anchor | Requirement | Design |
| --- | --- | --- | --- |
| SE-5.3-b001-s01 | `#section-sasl` | "each peer MUST start by sending a protocol header" `AMQP 3 1 0 0` (SASL-MAJOR 1, MINOR 0, REVISION 0) | Client sends it first; broker replies after reading the client header (§4.1) |
| SE-5.3-b002-s01 | `#section-sasl` | header exchange "follows the same rules specified in the version negotiation section" (Part 2 §2.2) | Mismatch ⇒ send our acceptable header, close (TR-2.2-b003-s02, TR-2.2-b015-s03) |
| SE-5.3-fig5.4 | `#section-sasl` | after SASL: `AMQP 0 1 0 0` both ways, then Open | Endpoint `:start` runs only after outcome `ok` |
| SE-5.1-b003-s01 | `#section-security-layers` | when a security layer terminates "the TCP connection MUST be closed by first shutting down the outgoing stream and then reading the incoming stream until it is terminated" | Close after flushed outcome; read-until-EOF bounded by close timeout (SHOULD-level drain not separately qualified, same as TR-2.2-b003-s02) |
| SE-5.3.2-b002-s01 | `#doc-idp51040` | SASL client/server "MUST correspond to the TCP client and server respectively" | Roles fixed by shell: client = `:client`, broker = `:server` |

### 3.2 SASL frames (§5.3.1, `#doc-idp43536`)

- Type code `0x01`. Bytes 6–7 ignored (SHOULD send `0x0000`). Extended header
  ignored (SHOULD send DOFF `0x02`). ⇒ decode accepts any channel bytes and any
  valid DOFF (still bounds-checked as in Part 2 §2.3); encode always emits
  `DOFF=2`, channel `0`.
- "The maximum size of a SASL frame is defined by MIN-MAX-FRAME-SIZE" (512)
  in both directions, not negotiable. ⇒ incoming SASL frame `> 512` octets is
  `:limit_exceeded`; encoder refuses `> 512`.
- "The frame body of a SASL frame MUST contain exactly one AMQP type, whose
  type encoding MUST have provides="sasl-frame"" ⇒ exactly one described
  value from §3.3, no trailing bytes; a transport performative in a SASL
  frame, or a SASL body in an AMQP frame, is malformed.
- "Receipt of an empty frame is an irrecoverable error." ⇒ empty SASL body
  (heartbeat shape) closes the connection.

### 3.3 Frame bodies (§5.3.3)

All are `class="composite" source="list" provides="sasl-frame"`. Decode
accepts numeric (`0x00000000:0x000000NN` as `ulong`) and symbolic descriptors;
encode uses the numeric descriptor (same rule as transport performatives).
Fields keep exact tagged `GravitonMQ.AMQP10.Value` types.

| Body | Descriptor | Fields (index: name type attrs) |
| --- | --- | --- |
| sasl-mechanisms | `amqp:sasl-mechanisms:list` `0x40` | 0: `sasl-server-mechanisms` symbol, `multiple="true"`, mandatory. "It is invalid for this list to be null or empty." Server not requiring auth "SHOULD send a list of one element … ANONYMOUS". Ordered by decreasing preference. |
| sasl-init | `amqp:sasl-init:list` `0x41` | 0: `mechanism` symbol mandatory; 1: `initial-response` binary; 2: `hostname` string. Unsupported mechanism ⇒ receiver "MUST close the connection with the authentication-failure close-code" (term undefined in the spec; §3.4). "Each peer MUST authenticate using the highest-level security profile it can handle from the list provided by the partner." hostname: SHOULD be null or equal to TLS SNI when TLS is used. |
| sasl-challenge | `amqp:sasl-challenge:list` `0x42` | 0: `challenge` binary mandatory |
| sasl-response | `amqp:sasl-response:list` `0x43` | 0: `response` binary mandatory |
| sasl-outcome | `amqp:sasl-outcome:list` `0x44` | 0: `code` sasl-code mandatory; 1: `additional-data` binary ("If the authentication is unsuccessful, this field is not set"). On success "the peers MUST exchange protocol headers". |
| sasl-code | restricted `ubyte` | `ok`0, `auth`1 (credentials), `sys`2 (system error), `sys-perm`3 (needs intervention), `sys-temp`4 (transient) |

`multiple="true"` wire form: a single `symbol` or an `array` of `symbol`
(reuse `multiple_symbol_field/5` in `codec/performative.ex:327`); `null` and
an empty array are invalid **for this field only** (Open capabilities allow
null). `sasl-code` is decoded as tagged `ubyte` and **not whitelisted**: values
5..255 decode successfully and are treated as failure by the client machine
(mirrors the "do not whitelist error-condition symbols" rule).

### 3.4 Exchange and failure rules adopted

```
client                                  broker (server)
AMQP 3 1 0 0  ------------------------>
              <------------------------ AMQP 3 1 0 0
              <------------------------ sasl-mechanisms [PLAIN, ANONYMOUS] | [ANONYMOUS]
sasl-init(mech, initial-response, hostname) ->
              <------------------------ sasl-outcome(code)
(code = ok)   AMQP 0 1 0 0 + open ---->  (may arrive in the same segment)
              <------------------------ AMQP 0 1 0 0 + open
```

- ANONYMOUS and PLAIN are single-step: the server never sends
  sasl-challenge; a client receiving one closes (`:sasl_protocol_error`). The
  server rejects a sasl-response (no challenge was sent) the same way.
- "authentication-failure close-code" is undefined in Part 5. Adopted
  behaviour (matching Proton/Artemis practice): send `sasl-outcome` with
  `auth` (1), flush it, then close as SE-5.1-b003-s01.
- Out-of-order frame, malformed body, oversize, empty frame, second
  sasl-init: close **without** an outcome (no authentication result exists to
  report); shell event carries only a reason atom. **(owner Q8)**
- No downgrade: a SASL-configured client that receives `AMQP 0 1 0 0` closes
  (version mismatch, TR-2.2-b003-s02); it never falls back to no-SASL.

## 4. Pure layer (`graviton_mq_amqp10`)

Purity rules unchanged: no process, clock, socket, runtime or storage
reference. Time enters as `now_ms` facts; deadlines leave as effects.

### 4.1 Codec additions

| Module | Change |
| --- | --- |
| `Codec.ProtocolHeader` | `recognize/2` with `accept: [0] \| [3] \| [0, 3]`; `recognize/1` keeps today's result (historical regressions unchanged). Accepted ID 3 returns `%ProtocolHeader{protocol_id: 3, major: 1, minor: 0, revision: 0}`; ID 3 with another version → `:unsupported_version`. |
| `Codec.Frame` | `decode/3`, `encode/3` take `types: [:amqp] \| [:sasl]` (default `[:amqp]`, so `frame_test.exs:52` and `frame_encode_test.exs:208` keep their meaning). `:sasl` decode enforces size ≤ 512 independently of `Limits.max_frame_size`; encode requires `channel: 0`, `data_offset_words: 2`, empty extended header. |
| `Codec.Stream` | phase becomes `{:header, accept}` \| `{:frames, :amqp \| :sasl}`. `expect_header(stream, accept)` re-enters the header phase after an `ok` outcome; it fails (`:invalid_stream`) if a partial frame is buffered. Bytes already received after the outcome stay in the caller's remaining input (pipelined `AMQP 0` header + Open). SASL phase buffer bound = 512. |
| `Codec.SASL` (new) | `decode/1`, `encode/1` for the five bodies; exactly one value, no trailing bytes; numeric+symbolic descriptors; tagged types; reuse `Codec.Value` with fixed limits `%Limits{max_frame_size: 512, max_value_bytes: 504, max_compound_items: 64, max_nesting_depth: 4}`. Local bounds: ≤ 16 mechanisms, each symbol 1..20 octets of `[A-Z0-9_-]` (RFC 4422 §3.1 name syntax) — longer/invalid names are retained as data but never selectable. |
| `Codec.Error` | add operations `:sasl_decode`, `:sasl_encode`. Details for Init/Response/Outcome errors contain field index and class only — **never bytes, lengths of secret fields, or values**. |
| `AMQP10.SASL.{Mechanisms, Init, Challenge, Response, Outcome}` (new structs) | Fields exactly as §3.3 (`server_mechanisms`, `mechanism`, `initial_response`, `hostname`, `challenge`, `response`, `code`, `additional_data`). Custom `Inspect` for Init/Response/Challenge/Outcome prints `#AMQP10.SASL.Init<mechanism: "PLAIN", initial_response: :redacted>`. |
| `AMQP10.SASL.Plain` (new) | RFC 4616 `message = [authzid] NUL authcid NUL passwd`; authcid and passwd 1..255 octets, authzid 0..255, valid UTF-8, no NUL inside. `parse/1 → {:ok, %Plain{authzid, username, password}} \| {:error, :invalid_plain_message}` (no detail). `encode/1`. `Inspect` redacts `password`. |
| `AMQP10.SASL.Anonymous` (new) | RFC 4505 optional trace: absent/empty accepted; otherwise valid UTF-8 ≤ 255 characters, else `:invalid_anonymous_message`. Trace is informational only. |
| `AMQP10.SASL` | becomes the type/constant module: mechanism names, `code_name/1` (`0 → :ok … 4 → :sys_temp`, other → `{:unknown, n}`). |

### 4.2 Pure negotiation machine `GravitonMQ.AMQP10.SASL.Machine`

Same contract as `Endpoint.step/2`: generation-fenced facts
`%{generation, now_ms, event}` → `{state, effects}`; deadlines as
`{:wake_at, gen, key, token, at}` / `{:cancel_wake, …}`; writes as
`{:send_bytes, gen, {:sasl, n}, bytes}` with `{:write_result, {:sasl, n}, r}`
receipts (namespace distinct from Endpoint write IDs). The machine emits
already-encoded bytes (header or SASL frame); the Writer path is unchanged.

**Server role** — `new(role: :server, mode: :required | :optional | :disabled, mechanisms: [binary], timeout_ms: pos)`:

| State | Event | Effects / next |
| --- | --- | --- |
| `:await_header` (deadline `:negotiation`) | header 3 & mode ≠ disabled | send `AMQP 3 1 0 0` + `sasl-mechanisms` → `:await_init` |
| | header 0 & mode ≠ required | `{:established, :no_sasl}` (shell starts Endpoint, then feeds the header) → `:done` |
| | header 0 & required | send `AMQP 3 1 0 0`, `{:close, :sasl_required}` |
| | header 3 & disabled | send `AMQP 0 1 0 0`, `{:close, :sasl_not_supported}` (keeps today's reason) |
| | other header | send our preferred header, `{:close, :unsupported_protocol_header}` |
| `:await_init` | sasl-init, mechanism offered | parse; ANONYMOUS without callback ⇒ outcome ok; else `{:authenticate, ref, credentials}` → `:authenticating` |
| | sasl-init, mechanism not offered / message invalid | outcome `auth`, close after write receipt |
| `:authenticating` | `{:auth_result, ref, {:ok, principal_ref}}` | send outcome `ok`; `{:established, {:sasl, mechanism}}`; `{:expect_header, [0]}` → `:await_amqp_header` |
| | `{:auth_result, ref, {:error, code}}` | outcome `code` (`:auth`/`:sys`/`:sys_perm`/`:sys_temp`), close after receipt |
| `:await_amqp_header` | header 0 | `{:start_endpoint}` → `:done` |
| any | frame out of order, decode error, `:negotiation` deadline, transport down | `{:close, reason}` (no outcome, §3.4) |

The machine never holds the password beyond the `{:authenticate, …}` effect
(credentials are moved, not stored) and never stores the principal; the shell
keeps the principal. **Header deferral:** in every mode the broker now waits
for the client header before sending its own (spec-permitted; TR-2.2 rows
keep their evidence because the reply still precedes the close). **(owner Q7)**

**Client role** — `new(role: :client, credentials: :anonymous | {:anonymous, trace} | %Plain{}, hostname: binary | nil, timeout_ms)`:
`:start` → send `AMQP 3 1 0 0` → await header 3 (header 0 ⇒ close
`:sasl_not_offered`) → await mechanisms → if configured mechanism ∈ list
send sasl-init (PLAIN: initial-response = RFC 4616 message; ANONYMOUS: trace
or absent; hostname = configured Open hostname or null) else close
`:sasl_mechanism_not_offered` without init → await outcome: `ok` ⇒
`{:expect_header, [0]}` + `{:start_endpoint}` (Endpoint `:start` sends
`AMQP 0 1 0 0` + Open exactly as today); non-zero ⇒ `{:close, {:sasl, code_name}}`.
Challenge ⇒ `{:close, :sasl_protocol_error}`.

**Composition:** shells run the Machine first and create/step the Endpoint
only after `{:start_endpoint}` / `{:established, :no_sasl}`. `Endpoint` is
unchanged except its moduledoc; its `sasl:` option stays rejected (SASL is a
shell-composed layer, not an Endpoint option). `ConnectionState`,
`SessionEngine` and performative codecs are untouched.

## 5. Broker runtime (`graviton_mq_runtime`, AMQP files only)

### 5.1 Configuration (**owner Q1, Q2, Q5**)

```elixir
amqp10: [
  auth: :anonymous | {MyAuth, options},        # existing key, widened
  sasl: :optional | :required | :disabled,      # default derived, see below
  sasl_mechanisms: ["PLAIN", "ANONYMOUS"],      # subset, ordered; validated
  allow_insecure_plain: false,                  # PLAIN on :tcp listeners
  sasl_timeout: 5_000,                          # 1..10_000 ms, header + exchange
  max_sasl_jobs: 16,                            # 1..128 concurrent callbacks
  auth_timeout: 2_000,                          # existing bound 1..3000, reused
  authorize: fn principal, :send | :receive, address -> :allow | _ end
]
```

- `auth: :anonymous` ⇒ default `sasl: :optional`, mechanisms `["ANONYMOUS"]`
  (SHOULD list of one element), no callback; principal `:anonymous` for both
  ANONYMOUS and no-SASL peers. Existing configurations keep working and
  default Proton clients become able to connect.
- `auth: {module, options}` with new behaviour
  `GravitonMQ.Runtime.AMQP10.Auth.authenticate(credentials, options) :: {:ok, principal} | {:error, reason}`
  ⇒ `sasl: :required` (`:optional`/`:disabled` rejected as
  `{:error, :sasl_required_for_authentication}` — no unauthenticated bypass);
  default mechanisms `["PLAIN"]`; `"ANONYMOUS"` only if listed explicitly, and
  then the callback is still invoked with `mechanism: :anonymous`.
- Validation fails start (`GravitonMQ.start_link/1` returns the error) if a
  `:tcp` listener would advertise PLAIN without `allow_insecure_plain: true`
  (`:plain_requires_tls`), or would advertise an empty list.
- `authorize/3` keeps its arity; its first argument becomes the returned
  principal (`:anonymous` under `auth: :anonymous`, so existing funs that match
  `:anonymous` are unaffected).

### 5.2 Authentication execution

- New child `Task.Supervisor` `:sasl_tasks` (`max_children: max_sasl_jobs`)
  in `runtime/amqp10/supervisor.ex`, before `:connections`; passed to each
  Connection in its configuration. Separate from `:auth_tasks` so link
  authorization and connection authentication cannot starve each other, and
  credentials never enter the Broker (Manager) mailbox.
- The Connection interprets `{:authenticate, ref, credentials}`
  asynchronously: `Task.Supervisor.start_child` + monitor + deadline timer
  (`auth_timeout`, capped by the remaining `sasl_timeout`), kill on deadline,
  ignore late replies (pattern of `broker.ex:439-479` and the watcher in
  `mqtt/auth.ex`, re-implemented in a new `runtime/amqp10/auth.ex` because
  MQTT files are out of bounds; deduplication is a later cleanup). The
  Connection keeps processing transport loss and timers while waiting.
- Credentials map passed to the callback:
  `%{mechanism: :plain | :anonymous, username, password, authzid, trace, hostname, peer: %{ip, port, transport, certificate, certificate_verified}}`
  (`peer` built like `mqtt/connection.ex:190-206`; certificate from OTP
  `:ssl.peercert/1` while broker TLS stays on OTP `:ssl` — S1b will move it to
  `Hell.Socket.peercert/1`). `authzid` is passed through; the callback decides
  (**owner Q13**).
- Result mapping (never including callback data):

| Callback result | Outcome code | Shell reason |
| --- | --- | --- |
| `{:ok, principal}` | `ok` | — |
| `{:error, _}` | `auth` | `{:sasl, :auth}` |
| invalid return, exception, exit | `sys` | `{:sasl, :sys}` |
| deadline, `:max_children`, supervisor unavailable | `sys-temp` | `{:sasl, :sys_temp}` |
| mechanism not offered, invalid PLAIN/ANONYMOUS message | `auth` | `{:sasl, :auth}` |

- Principal propagation: on `ok` the Connection sends
  `{:amqp10_principal, self(), principal}` to the Broker before stepping the
  Endpoint `:start` (same sender ⇒ ordered before any Link event); the Broker
  stores it in the connection record and `authorize/2` passes it to the
  callback instead of the literal `:anonymous` (`broker.ex:453`). No-SASL and
  ANONYMOUS under `auth: :anonymous` record `:anonymous`.
- `amqp10_status/1` gains bounded counts: `authenticating` connections and
  `sasl_failures` by code (no usernames).

### 5.3 Privacy and limits

- Never log, emit as event, return, or put in error details: password,
  initial-response, response, authzid, username. The username is not in the
  Manager state, status or telemetry. Mechanism name and outcome code may be
  reported.
- Connection and `:sasl_tasks` children set `Process.flag(:sensitive, true)`;
  existing `format_status` redaction and `protect/1` stacktrace scrubbing
  cover the new state. Decoded `SASL.Init`/`Plain` structs have redacting
  `Inspect`; crash reports therefore cannot print secrets even if a struct
  leaks into a reason.
- Limits: SASL frame 512 octets (both directions); one sasl-init; zero
  challenges/responses; `sasl_timeout` covers header wait + exchange + the
  post-outcome AMQP header; `max_connections` and `max_sasl_jobs` bound
  concurrent work. The existing `connect_timeout` still covers accept/TLS
  handshake.

## 6. Client (`graviton_mq_client`, AMQP files only)

### 6.1 Configuration (**owner Q4**)

```elixir
{GravitonMQ.AMQP10Client,
  container_id: "orders-worker", host: "broker.example", transport: :tls,
  sasl: :none | :anonymous | {:anonymous, trace} |
        {:plain, username, password} | {:plain, username, password, authzid: authzid},
  allow_insecure_plain: false,   # required for {:plain, …} with transport: :tcp
  sasl_timeout: 5_000}           # 1..10_000 ms
```

- Default `sasl: :none` keeps today's wire behaviour (raw `AMQP 0 1 0 0`).
- Invalid values, PLAIN over TCP without opt-in, or out-of-range RFC 4616
  lengths fail `start_link/1` with `{:error, :invalid_options}` (existing
  style; no echo of the value).
- `password` may be a binary or a zero-arity function returning one (evaluated
  at connect), so hosts can avoid keeping it in their own child spec. The
  public `child_spec/1` already wraps options in a closure; `status/1` and
  `format_status` stay redacted; the process is sensitive.
- Credential retention: the client keeps the credential (or provider fun) in
  redacted state for the connection lifetime so S3 reconnect can
  re-authenticate (**owner Q11**).

### 6.2 Errors and events

Observer receives `{:graviton_mq, client, {:disconnected, reason}}` with
`reason ∈ {:sasl, :auth | :sys | :sys_perm | :sys_temp | {:unknown, code}}`,
`:sasl_mechanism_not_offered`, `:sasl_not_offered` (peer answered
`AMQP 0`), `:sasl_protocol_error`, `:sasl_timeout`, or the existing decode
reasons. Pending operations complete with `{:error, reason}` (nothing was
emitted before Open). `{:connected, container}` is sent only after the AMQP
Open exchange, as today. Peer-supplied mechanism lists are not echoed.

## 7. Compatibility

| Peer behaviour | `auth: :anonymous` (default `:optional`) | `auth: {m, o}` (`:required`) |
| --- | --- | --- |
| No SASL (`AMQP 0`), e.g. Proton `sasl_enabled=False` | accepted, principal `:anonymous` (all existing suites unchanged) | refused: `AMQP 3 1 0 0` then close |
| SASL ANONYMOUS | accepted | refused (`auth`) unless `"ANONYMOUS"` listed and callback allows |
| SASL PLAIN | refused (`auth`, not offered) | callback decides |
| Other mechanism | refused (`auth`) | refused (`auth`) |

`sasl: :disabled` reproduces today's refusal of `AMQP 3` (regression
`connection_test.exs:404` moves under an explicit `sasl: :disabled` config).
Client: `sasl: :none` unchanged; a SASL-configured client never downgrades.

## 8. Acceptance evidence

Independent peers (required, both directions; self-to-self is extra only):

| # | Direction | Case | Expected |
| --- | --- | --- | --- |
| P1 | Proton 0.40.0 → broker (TCP) | default `sasl_enabled=True`, no credentials, `auth: :anonymous` | ANONYMOUS; send/receive round trip |
| P2 | Proton → broker (TCP) | `sasl_enabled=False`, `auth: :anonymous` | unchanged round trip (existing suites) |
| P3 | Proton → broker (TLS) | `allowed_mechs="PLAIN"`, good credentials, `auth: {TestAuth, …}` | round trip; `authorize/3` observed the returned principal |
| P4 | Proton → broker (TLS) | wrong password | Proton reports authentication failure; no Link, no queue change, status `sasl_failures.auth == 1` |
| P5 | Proton → broker (TCP) | PLAIN with `allow_insecure_plain: true` (Proton `allow_insecure_mechs=True`) | round trip; without opt-in the broker config is rejected at start |
| P6 | Proton → broker | `sasl_enabled=False` against `:required` | refused, no Open from broker |
| P7 | Proton → broker | ANONYMOUS against PLAIN-only broker | refused |
| A1 | client → Artemis 2.41.0 (TCP) | `sasl: :anonymous` | round trip with Proton peer on the Artemis side |
| A2 | client → Artemis (TLS) | `{:plain, "graviton-test", "ephemeral-test-only"}` | round trip |
| A3 | client → Artemis (TLS) | wrong password | `{:disconnected, {:sasl, :auth}}`; no `{:connected, _}` |
| A4 | client → Artemis | `sasl: :none` | existing suite unchanged |

A3 needs Artemis `security-enabled=true` (currently `false` in
`scripts/interop/run.py:127`, under which Artemis accepts any PLAIN
credentials). Whether the existing no-SASL client/Proton suites still pass with
security enabled and `--allow-anonymous` is **unverified** and must be checked
first in the plan (**owner Q9**). Exact Proton Python keyword names
(`allowed_mechs`, `allow_insecure_mechs`, `user`, `password`) are verified
against the pinned 0.40.0 install in the plan, not assumed here.

Local (no peer): codec tables for all five bodies (numeric/symbolic
descriptors, single-symbol vs array mechanisms, null/empty mechanisms
invalid, unknown sasl-code retained, trailing bytes, 512/513 octets, empty
SASL frame, AMQP performative in SASL frame and vice versa, interior nulls);
RFC 4616/4505 parsers incl. property tests; Machine transition tables for
both roles including every out-of-order body, deadline, and late auth result;
shell socket tests: byte-by-byte fragmentation, SASL outcome + `AMQP 0` +
Open coalesced in one segment, callback timeout/exception/`:max_children`,
transport loss while authenticating (task killed), privacy (a sentinel
password never appears in `capture_log`, crash reports, `:sys.get_status`,
observer events, status, or codec errors). Self-to-self client PLAIN → broker
as extra evidence. Full AGENTS.md sequence, zero warnings, no cycles;
`mix graviton_mq.check_architecture` unchanged (no new cross-app references).

## 9. Files and task sketch

Pure (`apps/graviton_mq_amqp10`): `lib/graviton_mq/amqp10/sasl.ex`, new
`sasl/{mechanisms,init,challenge,response,outcome,plain,anonymous,machine}.ex`,
new `codec/sasl.ex`, `codec/{protocol_header,frame,stream,error}.ex`,
`frame.ex` and `endpoint.ex` (docs only); tests `codec/sasl_test.exs`,
`sasl/{plain,anonymous,machine}_test.exs`, additive cases in
`codec/{protocol_header,frame,frame_encode,stream}_test.exs`,
`boundaries_test.exs`.

Broker (`apps/graviton_mq_runtime`): `runtime/amqp10/{config,connection,broker,supervisor}.ex`,
new `runtime/amqp10/auth.ex`; tests `test/amqp10/connection_test.exs` (move
the SASL-refusal case under `sasl: :disabled`), new `test/amqp10/sasl_test.exs`.
`apps/graviton_mq/test/graviton_mq/amqp10_sasl_proton_test.exs` + support
peer script.

Client (`apps/graviton_mq_client`): `client/amqp10.ex`,
`client/amqp10/config.ex`, `amqp10_client.ex` (docs); tests
`amqp10_client_test.exs` (config cases), `amqp10_artemis_test.exs` or a new
`amqp10_artemis_sasl_test.exs` + support peer.

Interop/docs: `scripts/interop/run.py` and `README.md` (Artemis security),
`docs/{AMQP10_BROKER,AMQP10_CLIENT,AMQP10_SCOPE,CAPABILITIES}.md`,
requirement inventory (Part 5) and clause audit. No MQTT file, no `mix.exs`,
no `mix.lock`.

Tasks (each test-first): T1 codec (header/frame/stream options, SASL bodies,
errors) → T2 RFC 4616/4505 parsers → T3 Machine (server, client) → T4 broker
config + auth runner + supervisor child → T5 broker Connection composition,
header deferral, principal hand-off → T6 client config + composition → T7
Proton broker suite (P1–P7) → T8 run.py Artemis security change + client
suite (A1–A4) → T9 privacy suite → T10 docs, inventory, CAPABILITIES with
executed evidence only. T1–T3 are pure and independent of T4–T6; T5 and T6
touch disjoint apps. **Conflict:** S3 (client reconnect) also edits
`client/amqp10.ex`; T6 must be sequenced with S3, not run concurrently
(**owner Q11**).

## 10. Open questions for the owner

1. **Broker authentication callback shape.** Recommended: `auth: {module, options}` with a new `GravitonMQ.Runtime.AMQP10.Auth` behaviour (`authenticate/2` only), mirroring MQTT's module policy; keep `authorize/3` as a fun receiving the principal. Alternative: `authenticate:` fun/1 beside `authorize:` (consistent with today's AMQP fun style, but two policy styles across protocols and no `options` slot).
2. **No-SASL compatibility default.** Recommended: `auth: :anonymous` ⇒ `sasl: :optional` (no-SASL and ANONYMOUS both accepted, existing configs/peers unchanged, default Proton clients start working); `auth: {m, o}` ⇒ `:required`, with `:optional` rejected to prevent an unauthenticated bypass. Trade-off: an operator cannot run "authenticate if offered, else anonymous" — deliberate.
3. **PLAIN over plain TCP.** Recommended: refused on both roles unless `allow_insecure_plain: true` (broker: config error `:plain_requires_tls` at start; client: `:invalid_options`). Trade-off: extra opt-in for loopback/dev and for the P5 test vs never sending passwords in clear by accident.
4. **Client default mechanism.** Recommended: `sasl: :none` (byte-identical to today; Artemis accepts it). Alternative: default `:anonymous`, which more servers expect, but it changes wire behaviour for every existing client and fails against peers that reject SASL headers.
5. **ANONYMOUS under an authenticating policy.** Recommended: advertised only if listed in `sasl_mechanisms`, and the `authenticate/2` callback is still invoked with `mechanism: :anonymous` (lets hosts admit by peer address/certificate). Alternative: bypass the callback and admit as `:anonymous`.
6. **Principal for ANONYMOUS/no-SASL.** Recommended: the atom `:anonymous`, so existing `authorize/3` funs matching `:anonymous` keep working. Alternative: `{:anonymous, trace}` exposing the RFC 4505 trace (breaks those funs).
7. **Broker header deferral.** Recommended: broker always waits for the client header before sending its own (required for SASL; spec-permitted; a silent client is now closed by `sasl_timeout` rather than the Open deadline). Alternative: keep eager `AMQP 0` + Open only under `sasl: :disabled` (smaller wire change, two code paths).
8. **Failure signalling.** Recommended: authentication failures send `sasl-outcome` (`auth`/`sys`/`sys-temp` per §5.2) then close; protocol violations (malformed, oversize, out-of-order, empty frame) close without an outcome. Alternative: also send `sys`/`auth` outcome on violations (friendlier diagnostics, but answers malformed input).
9. **Artemis interop configuration.** Recommended: enable `security-enabled=true` on the single shared Artemis instance (anonymous mapped by `--allow-anonymous`), after proving the existing no-SASL suites still pass; fall back to a second security-enabled Artemis instance if they do not. Either option changes `scripts/interop/run.py` and CI time/evidence.
10. **Requirement inventory.** Recommended: extend the pinned inventory with Part 5 (security, SHA-256 `d50d980b…`) and issue `SE-` IDs with the existing extractor; until then §3 IDs stay provisional. Alternative: record SASL evidence only in the clause audit.
11. **S2/S3 sequencing and credential retention.** Recommended: run S2's client task (T6) before S3 starts editing `client/amqp10.ex` (or S3 first, then T6), not concurrently; keep the credential or provider fun in redacted client state so S3 can re-authenticate. Alternative: drop the password after the outcome (smaller exposure; S3 would then require a provider fun).
12. **Mechanism set.** Recommended: ANONYMOUS and PLAIN only; EXTERNAL (mTLS principal) as a follow-up after S1b; no SCRAM. Confirm no SCRAM requirement for this programme.
13. **PLAIN authzid.** Recommended: pass `authzid` to `authenticate/2` and let the host decide (it may reject non-empty authzid ≠ username). Alternative: reject any non-empty authzid ≠ username in the runtime before calling the host.
