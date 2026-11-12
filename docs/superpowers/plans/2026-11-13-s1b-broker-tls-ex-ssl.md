# S1b broker TLS listeners on `Hell.Transports.SSL` / `ex_ssl` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Broker TLS listeners (MQTT 5.0 and AMQP 1.0) accept through a `Hell`
server using `Hell.Transports.SSL` (ex_ssl) with the S1a accept-only
`HellHandoff` layout. The OTP `:ssl` accept loop and every OTP `:ssl` reference
in `graviton_mq_runtime` are removed. The runtime declares
`extra_applications: [:crypto]`, and the architecture check forbids OTP `:ssl`
in the runtime.

**Architecture:** This plan implements the approved spec
[`2026-11-13-s1b-broker-tls-ex-ssl-design.md`](../specs/2026-11-13-s1b-broker-tls-ex-ssl-design.md).
Read the whole spec, including "Owner decisions (2026-11-13)". One listener path
serves both transports. `HellHandoff.start_link/4` chooses
`Hell.Transports.TCP` or `Hell.Transports.SSL` and passes the validated TLS
options to Hell, with every `password` wrapped in a closure. Hell runs the
TLS 1.3 handshake in the handler, bounded by `handshake_timeout` (the protocol's
`connect_timeout`). Only after a successful handshake does the handler start the
Connection, transfer ownership and send `{:accepted, %SSL.Socket{}}`.
Connections and Writers call `SSL.*` for TLS and no longer run a handshake
themselves. A new private module `GravitonMQ.Runtime.TLSOptions` validates both
protocols' TLS options against one allowlist.

**Owner gates (do not cross them):**

- **gsmlg-dev/http_fetch#169 (U2, coalesced active-once input), option B.** S1b
  is implemented now. Broker TLS on ex_ssl is **not qualified and not enabled
  for release** until #169 is released, the `hell`/`ex_ssl` pins are updated,
  the pipelining-while-slow regression (T6, tag `:requires_http_fetch_169`) and
  the TLS interop pass. The MQTT `max_packet_size + 16_384` and AMQP
  `max_input_bytes` semantics are **unchanged**. T6 is expected to FAIL on rc.14.
  It is excluded by default and is run explicitly to record that failure (Task 11).
- **gsmlg-dev/http_fetch#168 (U1, secrets in Hell telemetry/state), `needed`.**
  In-memory private keys (`key`, at top level and inside `certs_keys`/`sni_hosts`)
  are rejected with `{:unsupported_tls_option, :key}`. Every `password` reaches
  Hell only as a 0-arity closure. Both are marked
  `WORKAROUND(upstream): gsmlg-dev/http_fetch#168`. Removal condition: a pinned
  `hell` release no longer puts raw TLS `transport_options` into
  `[:hell, :listener, :start | :stop]` telemetry metadata, `Hell.Listener` state
  or supervisor child specs (coordinator task C6).
- **OTP `:ssl` forbidden in `graviton_mq_runtime`** (Q3 = yes; an AGENTS.md
  amendment, coordinator task C3).

**Tech Stack:** Elixir 1.18.4 / OTP 28.5.0.1 (`.tool-versions`), `hell`
`== 1.0.0-rc.14` (runtime), `ex_ssl` `== 1.0.0-rc.14` (client, and runtime
through hell). Pins are unchanged until #169 ships (C6). OTP `:ssl`, `openssl`,
Mosquitto 2.1.2, Paho 2.1.0 and Proton 0.40.0 serve only as independent test
peers.

---

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

## Execution rules (read before Task 0)

### Parallel checkout with S3

S3 (AMQP client reconnect plus asynchronous broker Manager) runs **at the same
time in the same checkout on `main`**.

1. **S3 owns, and S1b must not edit,** these files:
   `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex` (Manager),
   the AMQP queue files (`amqp10/queue.ex`, `amqp10/store.ex`,
   `amqp10/persistence.ex`), `apps/graviton_mq_client/lib/graviton_mq/client/amqp10*`
   (every file except `client/transport.ex`, which S1b does not need either),
   `apps/graviton_mq_client/lib/graviton_mq/amqp10_client.ex`, everything
   under `apps/graviton_mq_amqp10/**`, and S3's own tests. S1b needs none of
   these files. If a step seems to need one of them, stop and report it to the
   coordinator; never edit it.
2. **S1b write scope (Tasks 0–15):**
   - `apps/graviton_mq_runtime/lib/graviton_mq/runtime/{hell_handoff.ex,tls_options.ex}`;
   - `apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/{listener,connection,writer,config}.ex`;
   - `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/{listener,connection,writer,config}.ex` (**shared-file protocol**, rule 3);
   - `apps/graviton_mq_runtime/mix.exs` (Task 13 only);
   - `apps/graviton_mq_runtime/test/{hell_handoff_test.exs,tls_options_test.exs,test_helper.exs,support/tls.exs}`,
     `apps/graviton_mq_runtime/test/mqtt/config_test.exs`;
   - `apps/graviton_mq_runtime/test/amqp10/{config_test,connection_test,connection_input_test}.exs` (**shared-file protocol**);
   - new `apps/graviton_mq/test/graviton_mq/{mqtt_tls_test,amqp10_tls_test,mqtt_tls_paho_test}.exs`
     and `apps/graviton_mq/test/support/mqtt_tls_peer.py`;
   - `apps/graviton_mq/test/graviton_mq/mqtt_mosquitto_test.exs` (Task 14);
   - `apps/graviton_mq/test/graviton_mq/amqp10_proton_test.exs` and `apps/graviton_mq/test/test_helper.exs` (**shared-file protocol**);
   - `apps/graviton_mq/lib/graviton_mq/architecture.ex`,
     `apps/graviton_mq/test/graviton_mq/{architecture_test,release_test}.exs` (Task 13);
   - `scripts/build_hex.exs` (Task 15);
   - this plan's execution log.
3. **Shared-file protocol** (for files S3 might also touch). Before the first
   edit of such a file in a task, run
   `git status --porcelain -- <file>`. It must print nothing. If it prints
   anything, S3 has uncommitted work in that file. Do not edit it; stop and
   report `S1b blocked: uncommitted S3 changes in <file>` to the coordinator.
   Once the check passes, do the edit, test and commit in one uninterrupted
   task, so the file never holds a mix of S1b and S3 hunks.
4. **Coordinator-serialized files (C1–C6 only, after S3 stops writing):**
   `AGENTS.md`, `docs/adr/0009-owned-network-stack.md`, `docs/CAPABILITIES.md`,
   `docs/ACTIVE_SCOPE.md`, `docs/MQTT_BROKER.md`, `docs/AMQP10_BROKER.md`,
   `docs/releases/*`, `docs/qualification/*` (clause audits and inventories),
   `scripts/interop/*`, `.github/workflows/*`, `mix.exs`/`mix.lock` pin changes.
5. **Isolated build directory.** `MIX_BUILD_PATH` does **not** work: it drops
   the per-environment suffix, so dev and test builds recompile each other.
   Prefix every mix command with
   `env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp`.
   `TMPDIR` keeps `System.tmp_dir!/0` names (which several tests derive from
   `System.unique_integer/1`) from colliding with S3's VM.
   `mix graviton_mq.check_architecture` honours the same prefix. Never run a
   bare `mix ...`.
6. **Never run umbrella-wide `mix format`, `mix deps.get` or the full
   `mix test` in Tasks 0–15.** Format only the files you touched. Run only the
   test files named in the task. If another app fails to compile because of
   S3's in-progress files, do not edit them: record the failure, wait and rerun.
7. **Commits.** Commit once per task only if the executing session is
   authorized to commit (the S2/S4 convention). Use conventional messages with
   **no `Co-Authored-By` trailer and no "Generated with" line**. Never push,
   tag, bump the version or release. Never use `git add -A` or `git add .`.
   zsh does not split a scalar `$P`, so always use an array:

   ```bash
   cd /home/gao/Workspace/gsmlg-dev/graviton_mq
   P=(path/one path/two)
   git add -- "${P[@]}"
   git commit --only -m "<message>" -- "${P[@]}"
   ```

   If `.git/index.lock` exists, wait a few seconds and retry. Never delete the lock.
8. **Peers.** Mosquitto, Paho and Proton are not installed locally. The tags
   `:interop`, `:paho_interop` and `:proton_interop` are excluded locally
   (`apps/graviton_mq/test/test_helper.exs`) and run only in hosted CI. Locally,
   OTP `:ssl` is the independent TLS peer. Test code must call
   `Mix.ensure_application!(:ssl)`, because Mix prunes `:ssl` from the code path
   once the runtime stops declaring it, and must declare
   `@compile {:no_warn_undefined, :ssl}`. Never claim CI evidence before the
   coordinator has a passing run.
9. **Test lessons to apply everywhere.**
   - After `Process.monitor(pid)`, make one synchronous call to `pid` (use
     `:sys.get_state(pid)`) **before** any stimulus that comes from another
     process or a port (a kill, a peer's bytes, a listener crash). Otherwise the
     `DOWN` can race the monitor and arrive as `:noproc`.
   - `GenServer.stop/3` and `:sys` messages are still served by a
     `:sys.suspend`ed process. A suspended Connection can be stopped or killed
     by `HellHandoff.release/1`, and `:sys.get_state/1` still works on it. Never
     `GenServer.call` a suspended process. Always `:sys.resume` in an `after`
     block.
   - Keep HellHandoff's budgets (5 s release + 0.5 s kill, 6 s drain, 7 s
     residual stop, 14 s child shutdown), its rebind retry (20 ms, ≤ 1 s,
     `:eaddrinuse` only), its `:protected` ETS spec publication with allowlisted
     keys, and its non-blocking `send_request` wiring. S1b changes none of them.
   - A listener failure closes that listener's own connections; TLS now does the
     same (T7).
   - MQTT outbound encode is bounded by the peer's Maximum Packet Size. The
     CONNECT packets in these tests carry no Maximum Packet Size property, so
     64 KiB publications are delivered to the peer and can block the Writer.
     A CONNECT that sets a small maximum would make the broker skip them instead.

### Command prefix and abbreviations

`ROOT` = `/home/gao/Workspace/gsmlg-dev/graviton_mq`, `R` = `apps/graviton_mq_runtime`,
`G` = `apps/graviton_mq`, `C` = `apps/graviton_mq_client`. Commands spell paths
out in full, for example:

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/tls_options_test.exs
```

---

## Decisions taken while planning (spec deltas; report them)

- **D1 Default identity.** `(certfile | cert) + keyfile` or `certs_keys`. The
  spec's `cert + key` cannot be used while #168 rejects `key`, so `cert`
  (non-secret DER) with `keyfile` is accepted. A `certs_keys` or `sni_hosts`
  identity must contain both a certificate source and `keyfile` itself (it does
  not inherit them from top-level keys, although a top-level `password` still
  applies). A missing identity yields `{:invalid_tls_option, :keyfile}` when a
  certificate source is present, otherwise `{:invalid_tls_option, :certfile}`.
- **D2 Cipher forms** (fixed from `SSL.Capabilities.matches?/3`). Each entry may
  be the integer ID (`0x1301`, `0x1302`, `0x1303`), the atom name
  (`:tls_aes_128_gcm_sha256`, `:tls_aes_256_gcm_sha384`,
  `:tls_chacha20_poly1305_sha256`), the upper-case binary name
  (`"TLS_AES_128_GCM_SHA256"`, …), or the OTP TLS 1.3 suite map
  `%{key_exchange: :any, cipher: c, mac: :aead, prf: h}`. Entries must resolve
  to distinct suites. Runtime availability (for example ChaCha20 in `:crypto`)
  is checked by ex_ssl at listen and fails with `{:listener_setup_failed, :tls}`.
- **D3 Error keys.** A malformed nested value inside `certs_keys`/`sni_hosts`
  reports the outer key (`:certs_keys`/`:sni_hosts`). An in-memory `key`
  anywhere reports `{:unsupported_tls_option, :key}`. A non-keyword
  `tls_options`, or one with a bare atom such as `:binary`, reports
  `{:invalid_tls_option, :tls_options}`. A duplicated key reports
  `{:invalid_tls_option, key}`. `sni_hosts` together with `sni_fun` reports
  `{:invalid_tls_option, :sni_fun}` (ex_ssl's rule). Two `sni_hosts` names that
  differ only in ASCII case report `:sni_hosts`.
- **D4 TCP listeners.** MQTT keeps rejecting non-empty `tls_options` on `:tcp`
  (`:invalid_mqtt_options`). AMQP keeps its current TCP behaviour: TLS keys are
  ignored and owned socket options are rejected (`:invalid_listener`). Hell
  receives TLS options only for `:tls`.
- **D5 `HellHandoff.start_link/4`.** The signature becomes
  `start_link(listener, settings, table, options \\ [])`. `listener` carries
  `transport` and `tls_options`; `settings` is
  `%{send_timeout:, handshake_timeout:, max_connections:}`. `handshake_timeout`
  is set for both transports (Hell's TCP handshake is a no-op).
  `num_connections: max_connections` is set **only for TLS**, so TCP keeps the
  S1a value of 16_384.
- **D6 Architecture message.** The runtime rule reuses the existing
  `:forbidden_otp_transport_dependency` violation and its text
  `"... may not reference :ssl (use the owned SSL package)"`; no new tuple.
- **D7 T3 for AMQP** observes `peer/1` through a raw SASL PLAIN exchange over
  TLS: the host `Auth` callback receives `credentials.peer`. T3 for MQTT
  observes it through the policy's `authenticate/2`.
- **D8 T5/T6 levels.** MQTT T5/T6 are product-level (`GravitonMQ` instance and
  a raw OTP `:ssl` peer). AMQP T5/T6 are Connection-level in
  `connection_input_test.exs`, with an ex_ssl server socket handed to the
  Connection, because AMQP output needs no Manager there. T6 uses small limits
  (MQTT `max_packet_size: 4_096`, which gives a 20_480-byte bound; AMQP
  `max_input_bytes: 65_536`). With the defaults the 960 KiB coalesced chunk
  usually fits (see "Spec issues").
- **D9 TLS 1.2-only independent peer.** Paho with
  `SSLContext.maximum_version = TLSv1_2` is the definitive case.
  `mosquitto_pub --tls-version tlsv1.2` is **not used**: libmosquitto applies the
  value as a *minimum* (it disables only older versions), so it would still
  negotiate TLS 1.3. This is not re-verified here; the coordinator may confirm
  it in CI.
- **D10 T9 now.** The password-closure mitigation can already be asserted:
  password bytes are absent from Hell telemetry metadata, `Hell.Listener` state
  and status, our Listener status, and logs on a listener kill. Key DER cannot
  reach Hell while D1/#168 reject `key`. C6 extends T9 to an in-memory key once
  #168 is pinned.
- **D11 Release gate.** The OTP `:ssl` TLS path is deleted, so from the S1b
  commits until #169 is pinned, `main`'s broker TLS is ex_ssl-only and
  unqualified. C4 records that **no release or publication may be cut** from
  `main` until C6 passes.

---

## File map

| File | Change |
| --- | --- |
| `R/lib/graviton_mq/runtime/tls_options.ex` | new: allowlist validation, Hell option shaping (password closures), `verify_peer?/1` |
| `R/lib/graviton_mq/runtime/hell_handoff.ex` | transport module per listener, TLS options, `handshake_timeout`, TLS `num_connections` |
| `R/lib/graviton_mq/runtime/mqtt/config.ex` | TLS validation through `TLSOptions`; error terms |
| `R/lib/graviton_mq/runtime/amqp10/config.ex` | same (shared-file protocol) |
| `R/lib/graviton_mq/runtime/mqtt/listener.ex` | single Hell path; delete the OTP accept loop |
| `R/lib/graviton_mq/runtime/amqp10/listener.ex` | same (shared-file protocol) |
| `R/lib/graviton_mq/runtime/mqtt/{connection,writer}.ex` | `SSL.*`; delete the in-Connection handshake |
| `R/lib/graviton_mq/runtime/amqp10/{connection,writer}.ex` | `SSL.*`; delete the handshake and its `{:handshake, _}` branch; per-transport `configure_socket` |
| `R/mix.exs` | `extra_applications: [:crypto]` |
| `R/test/support/tls.exs` | new: certificate fixture, OTP `:ssl` peer, ex_ssl server pair |
| `R/test/tls_options_test.exs` | new |
| `R/test/hell_handoff_test.exs` | parametrized over TCP/TLS; T1, T2, T4, T8 |
| `R/test/mqtt/config_test.exs`, `R/test/amqp10/config_test.exs` | TLS error terms |
| `R/test/amqp10/connection_input_test.exs` | migrated TLS test; AMQP T5, T6 |
| `R/test/amqp10/connection_test.exs` | delete the Connection-level TLS handshake test (moved) |
| `R/test/test_helper.exs`, `G/test/test_helper.exs` | exclude `:requires_http_fetch_169` |
| `G/test/graviton_mq/mqtt_tls_test.exs` | new: MQTT round trip, T2, T3, T4, T5, T6, T7, T8, T9 |
| `G/test/graviton_mq/amqp10_tls_test.exs` | new: AMQP round trip, T2, T3, T4 (migrated), T7, T8, T9 |
| `G/test/graviton_mq/mqtt_tls_paho_test.exs`, `G/test/support/mqtt_tls_peer.py` | new CI-only Paho TLS/mTLS/TLS1.2 peer |
| `G/test/graviton_mq/mqtt_mosquitto_test.exs`, `G/test/graviton_mq/amqp10_proton_test.exs` | record broker-side `protocol` and the peer's OpenSSL version |
| `G/lib/graviton_mq/architecture.ex`, `G/test/graviton_mq/{architecture,release}_test.exs` | runtime forbids `:ssl`; runtime application list |
| `scripts/build_hex.exs` | installed-consumer TLS round trip; `:ssl` not started |
| Coordinator only | `AGENTS.md`, ADR 0009, `docs/CAPABILITIES.md`, `docs/{MQTT,AMQP10}_BROKER.md`, `docs/releases/`, `docs/ACTIVE_SCOPE.md`, pins |

---

## Task 0: Workspace isolation, upstream check and baseline

**Files:** none (execution log only)

- [ ] **Step 1: Isolated directories and BASE**

```bash
mkdir -p /tmp/gmq-build-s1b /tmp/gmq-s1b-tmp
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && git rev-parse --short HEAD && git status --short
```

Record `BASE` (the short SHA) in the execution log. Uncommitted S3 files may appear; leave them alone.

- [ ] **Step 2: Upstream state (read-only)**

```bash
gh issue view 168 --repo gsmlg-dev/http_fetch --json state,title,labels
gh issue view 169 --repo gsmlg-dev/http_fetch --json state,title,labels
```

Expected: both `OPEN`. If either is closed, read its closing release and report
it to the coordinator before Task 2. A shipped #169 moves C6 forward; a shipped
#168 changes Task 2. Do not file or label anything: the routing was done by the
owner decision.

- [ ] **Step 3: Baseline of the files this plan changes**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/hell_handoff_test.exs test/mqtt/config_test.exs test/amqp10/config_test.exs test/amqp10/connection_input_test.exs test/amqp10/connection_test.exs
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/graviton_mq/mqtt_connection_test.exs test/graviton_mq/architecture_test.exs test/graviton_mq/release_test.exs
```

Expected: `0 failures` (the first run compiles deps into the isolated root).
Record the counts. A baseline failure is recorded verbatim and reported; do not fix it.

---

## Task 1: Shared TLS test fixture

**Files:** create `apps/graviton_mq_runtime/test/support/tls.exs`

The runtime has no `elixirc_paths(:test)`. Tests load this file with
`Code.require_file/2`, like the existing `client/test/support/mosquitto.exs`.

- [ ] **Step 1: Write the fixture**

```elixir
defmodule GravitonMQ.Runtime.Test.TLS do
  @moduledoc false
  # Test-only TLS material and peers. OTP :ssl is an independent TLS
  # implementation here; product code uses ex_ssl only. Mix prunes :ssl from
  # the code path because no GravitonMQ application lists it.
  import ExUnit.Assertions

  @compile {:no_warn_undefined, :ssl}
  @password "graviton-s1b-key-password"

  def password, do: @password

  def directory! do
    directory =
      Path.join(
        System.tmp_dir!(),
        "graviton-tls-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(directory)
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(directory) end)
    directory
  end

  # One trusted CA with a localhost server leaf and a client leaf, an
  # untrusted CA with its own client leaf, and an AES-256 PKCS#8 encrypted copy
  # of the server key protected by password/0.
  def certificates!(directory \\ directory!()) do
    path = &Path.join(directory, &1)
    ca = authority!(path, "ca", "Graviton S1b Test CA")
    untrusted = authority!(path, "untrusted", "Graviton S1b Untrusted CA")

    server =
      leaf!(path, "server", ca, "localhost", 1, """
      subjectAltName=DNS:localhost,IP:127.0.0.1
      extendedKeyUsage=serverAuth
      """)

    client = leaf!(path, "client", ca, "graviton-client", 2, "extendedKeyUsage=clientAuth\n")

    stranger =
      leaf!(path, "stranger", untrusted, "graviton-stranger", 3, "extendedKeyUsage=clientAuth\n")

    encrypted_key = path.("server-encrypted.key")

    openssl!([
      "pkcs8",
      "-topk8",
      "-v2",
      "aes-256-cbc",
      "-in",
      server.key,
      "-out",
      encrypted_key,
      "-passout",
      "pass:" <> @password
    ])

    %{
      ca: ca.certificate,
      ca_key: ca.key,
      ca_der: certificate_der!(ca.certificate),
      untrusted: untrusted.certificate,
      certificate: server.certificate,
      key: server.key,
      key_der: key_der!(server.key),
      encrypted_key: encrypted_key,
      client_certificate: client.certificate,
      client_key: client.key,
      client_der: certificate_der!(client.certificate),
      stranger_certificate: stranger.certificate,
      stranger_key: stranger.key
    }
  end

  # Broker listener tls_options for the server identity (paths, not secrets).
  def listener_options(certificates, extra \\ []),
    do: Keyword.merge([certfile: certificates.certificate, keyfile: certificates.key], extra)

  def client_identity(certificates),
    do: [certfile: certificates.client_certificate, keyfile: certificates.client_key]

  def stranger_identity(certificates),
    do: [certfile: certificates.stranger_certificate, keyfile: certificates.stranger_key]

  def ensure_ssl! do
    Mix.ensure_application!(:ssl)
    {:ok, _} = Application.ensure_all_started(:ssl)
    :ok
  end

  # A verified OTP :ssl client: trusts the test CA, checks the name
  # "localhost" and offers TLS 1.3 only unless `options` overrides it.
  def connect(port, certificates, options \\ [], timeout \\ 2_000) do
    ensure_ssl!()

    defaults = [
      active: false,
      verify: :verify_peer,
      cacertfile: String.to_charlist(certificates.ca),
      server_name_indication: ~c"localhost",
      versions: [:"tlsv1.3"]
    ]

    :ssl.connect({127, 0, 0, 1}, port, [:binary | Keyword.merge(defaults, options)], timeout)
  end

  # An ex_ssl server socket handshaken in, and owned by, the calling process,
  # plus a connected OTP :ssl client also owned by the caller.
  def server_pair!(certificates, client_options \\ []) do
    ensure_ssl!()

    {:ok, listen} =
      SSL.listen(0,
        ip: {127, 0, 0, 1},
        certfile: certificates.certificate,
        keyfile: certificates.key
      )

    ExUnit.Callbacks.on_exit(fn -> SSL.close(listen) end)
    {:ok, {_ip, port}} = SSL.sockname(listen)
    caller = self()

    client =
      Task.async(fn ->
        {:ok, peer} = connect(port, certificates, client_options)
        :ok = :ssl.controlling_process(peer, caller)
        peer
      end)

    {:ok, socket} = SSL.transport_accept(listen, 2_000)
    {:ok, socket} = SSL.handshake(socket, 2_000)
    {socket, Task.await(client)}
  end

  def openssl!(arguments) do
    executable = System.find_executable("openssl") || flunk("openssl is required for TLS tests")
    {output, status} = System.cmd(executable, arguments, stderr_to_stdout: true)
    assert status == 0, "openssl #{Enum.join(arguments, " ")} failed: #{output}"
    output
  end

  defp authority!(path, name, common_name) do
    certificate = path.(name <> ".pem")
    key = path.(name <> ".key")

    openssl!([
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-days",
      "1",
      "-subj",
      "/CN=" <> common_name,
      "-addext",
      "basicConstraints=critical,CA:TRUE",
      "-addext",
      "keyUsage=critical,keyCertSign,cRLSign",
      "-keyout",
      key,
      "-out",
      certificate
    ])

    %{certificate: certificate, key: key}
  end

  defp leaf!(path, name, authority, common_name, serial, extensions) do
    certificate = path.(name <> ".pem")
    key = path.(name <> ".key")
    request = path.(name <> ".csr")
    extension_file = path.(name <> ".ext")

    File.write!(
      extension_file,
      "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n" <>
        extensions
    )

    openssl!([
      "req",
      "-new",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-subj",
      "/CN=" <> common_name,
      "-keyout",
      key,
      "-out",
      request
    ])

    openssl!([
      "x509",
      "-req",
      "-in",
      request,
      "-CA",
      authority.certificate,
      "-CAkey",
      authority.key,
      "-set_serial",
      Integer.to_string(serial),
      "-days",
      "1",
      "-extfile",
      extension_file,
      "-out",
      certificate
    ])

    %{certificate: certificate, key: key}
  end

  defp certificate_der!(pem) do
    [{:Certificate, der, :not_encrypted}] = :public_key.pem_decode(File.read!(pem))
    der
  end

  defp key_der!(pem) do
    [{_type, der, :not_encrypted}] = :public_key.pem_decode(File.read!(pem))
    der
  end
end
```

- [ ] **Step 2: Smoke-load the fixture**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp MIX_ENV=test mix run --no-start -e 'Code.require_file("test/support/tls.exs"); ExUnit.start(autorun: false); c = GravitonMQ.Runtime.Test.TLS.certificates!(Path.join(System.tmp_dir!(), "s1b-smoke") |> tap(&File.mkdir_p!/1)); IO.inspect(Map.keys(c)); IO.inspect(byte_size(c.client_der) > 0)'
```

Expected: the key list and `true`. Delete `/tmp/gmq-s1b-tmp/s1b-smoke` afterwards.
`:public_key` is reachable through ex_ssl's application list. If `:public_key`
turns out to be pruned from the code path, add `Mix.ensure_application!(:public_key)`
in `ensure_ssl!/0` and call it at the top of `certificates!/1`, then record the change.

- [ ] **Step 3: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
P=(apps/graviton_mq_runtime/test/support/tls.exs)
env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix format "${P[@]}"
git add -- "${P[@]}"
git commit --only -m "test(runtime): add the S1b TLS certificate and peer fixture" -- "${P[@]}"
```

---

## Task 2: `GravitonMQ.Runtime.TLSOptions` (spec §4.7, §4.10, Q1)

**Files:** create `apps/graviton_mq_runtime/lib/graviton_mq/runtime/tls_options.ex`,
create `apps/graviton_mq_runtime/test/tls_options_test.exs`

- [ ] **Step 1: Write the failing test**

```elixir
defmodule GravitonMQ.Runtime.TLSOptionsTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Runtime.TLSOptions

  @identity [certfile: "server.pem", keyfile: "server.key"]
  @in_memory_key {:RSAPrivateKey, <<48, 1, 0>>}

  test "accepts the allowlisted options with structurally valid values" do
    assert :ok = TLSOptions.validate(@identity)
    assert :ok = TLSOptions.validate(certfile: ~c"server.pem", keyfile: ~c"server.key")
    assert :ok = TLSOptions.validate(cert: <<48, 1, 0>>, keyfile: "server.key")

    assert :ok =
             TLSOptions.validate(
               certs_keys: [
                 %{certfile: "a.pem", keyfile: "a.key", password: "p"},
                 %{cert: <<48>>, keyfile: "b.key"}
               ]
             )

    assert :ok =
             TLSOptions.validate(
               @identity ++
                 [
                   password: "secret",
                   cacertfile: "ca.pem",
                   verify: :verify_peer,
                   fail_if_no_peer_cert: true,
                   depth: 3,
                   versions: [:"tlsv1.3"],
                   alpn_preferred_protocols: ["mqtt", "amqp"],
                   sni_hosts: [{~c"example.test", [certfile: "e.pem", keyfile: "e.key"]}]
                 ]
             )

    assert :ok =
             TLSOptions.validate(
               @identity ++
                 [
                   password: fn -> "secret" end,
                   cacerts: [<<48, 1>>],
                   verify: :verify_peer,
                   sni_fun: fn _name -> :undefined end
                 ]
             )

    assert :ok =
             TLSOptions.validate(
               @identity ++ [ciphers: [:tls_aes_128_gcm_sha256, "TLS_AES_256_GCM_SHA384", 0x1303]]
             )

    assert :ok =
             TLSOptions.validate(
               @identity ++
                 [ciphers: [%{key_exchange: :any, cipher: :aes_128_gcm, mac: :aead, prf: :sha256}]]
             )
  end

  test "OTP-only, session and socket options are unsupported by key" do
    for key <-
          ~w(verify_fun partial_chain crl_check honor_cipher_order secure_renegotiate
             reuse_sessions session_tickets active mode packet header deliver packet_size
             send_timeout send_timeout_close buffer ip)a do
      assert {:error, {:unsupported_tls_option, ^key}} =
               TLSOptions.validate(@identity ++ [{key, :anything}])
    end
  end

  test "only TLS 1.3 may be configured" do
    for versions <- [[:"tlsv1.2"], [:"tlsv1.3", :"tlsv1.2"], [], :"tlsv1.3"] do
      assert {:error, {:unsupported_tls_option, :versions}} =
               TLSOptions.validate(@identity ++ [versions: versions])
    end
  end

  # WORKAROUND(upstream): gsmlg-dev/http_fetch#168 — flip to :ok when C6 lands.
  test "in-memory private keys are rejected until hell redacts transport options" do
    for options <- [
          [cert: <<48>>, key: @in_memory_key],
          [certs_keys: [%{cert: <<48>>, key: @in_memory_key}]],
          @identity ++ [sni_hosts: [{~c"h.test", [cert: <<48>>, key: @in_memory_key]}]],
          @identity ++
            [sni_hosts: [{~c"h.test", [certs_keys: [%{cert: <<48>>, key: @in_memory_key}]]}]]
        ] do
      assert {:error, {:unsupported_tls_option, :key}} = TLSOptions.validate(options)
    end
  end

  test "malformed values and missing prerequisites report only the key" do
    for {options, key} <- [
          {[certfile: "", keyfile: "k"], :certfile},
          {[certfile: "c", keyfile: 1], :keyfile},
          {[cert: "", keyfile: "k"], :cert},
          {@identity ++ [password: 42], :password},
          {@identity ++ [cacertfile: ""], :cacertfile},
          {@identity ++ [cacerts: []], :cacerts},
          {@identity ++ [cacerts: [:not_der]], :cacerts},
          {@identity ++ [verify: :verify_maybe], :verify},
          {@identity ++ [verify: :verify_peer], :verify},
          {@identity ++ [fail_if_no_peer_cert: true], :fail_if_no_peer_cert},
          {@identity ++ [cacertfile: "ca", verify: :verify_peer, fail_if_no_peer_cert: :yes],
           :fail_if_no_peer_cert},
          {@identity ++ [depth: 0], :depth},
          {@identity ++ [depth: 101], :depth},
          {@identity ++ [ciphers: []], :ciphers},
          {@identity ++ [ciphers: [:tls_ecdhe_rsa_with_aes_128_gcm_sha256]], :ciphers},
          {@identity ++ [ciphers: [0x1301, :tls_aes_128_gcm_sha256]], :ciphers},
          {@identity ++ [alpn_preferred_protocols: []], :alpn_preferred_protocols},
          {@identity ++ [alpn_preferred_protocols: ["a", "a"]], :alpn_preferred_protocols},
          {@identity ++ [alpn_preferred_protocols: [""]], :alpn_preferred_protocols},
          {@identity ++ [sni_fun: fn -> nil end], :sni_fun},
          {@identity ++ [sni_hosts: []], :sni_hosts},
          {@identity ++ [sni_hosts: [{"binary-name", @identity}]], :sni_hosts},
          {@identity ++ [sni_hosts: [{~c"h.test", [certfile: "c"]}]], :sni_hosts},
          {@identity ++ [sni_hosts: [{~c"H.test", @identity}, {~c"h.test", @identity}]],
           :sni_hosts},
          {@identity ++
             [sni_hosts: [{~c"h.test", @identity}], sni_fun: fn _name -> :undefined end],
           :sni_fun},
          {[certs_keys: []], :certs_keys},
          {[certs_keys: [%{certfile: "c"}]], :certs_keys},
          {[certs_keys: [%{certfile: "c", keyfile: "k", verify: :verify_peer}]], :certs_keys},
          {[certs_keys: List.duplicate(%{certfile: "c", keyfile: "k"}, 17)], :certs_keys},
          {[certfile: "c"], :keyfile},
          {[cert: <<48>>], :keyfile},
          {[keyfile: "k"], :certfile},
          {[], :certfile},
          {@identity ++ [certfile: "again"], :certfile}
        ] do
      assert {:error, {:invalid_tls_option, ^key}} = TLSOptions.validate(options),
             inspect(options)
    end

    assert {:error, {:invalid_tls_option, :tls_options}} = TLSOptions.validate(:not_a_list)
    assert {:error, {:invalid_tls_option, :tls_options}} = TLSOptions.validate([:binary | @identity])
  end

  # WORKAROUND(upstream): gsmlg-dev/http_fetch#168 — Hell copies transport
  # options into telemetry metadata and process state; a closure prints as
  # #Function<...>.
  test "passwords reach Hell only inside closures" do
    options =
      TLSOptions.hell_options(
        @identity ++
          [
            password: "first-secret",
            certs_keys: [%{certfile: "a", keyfile: "b", password: ~c"second-secret"}],
            sni_hosts: [{~c"h.test", [certfile: "c", keyfile: "k", password: "third-secret"]}]
          ]
      )

    printed = inspect(options, limit: :infinity, printable_limit: :infinity)
    for secret <- ["first-secret", "second-secret", "third-secret"], do: refute(printed =~ secret)
    assert options[:password].() == "first-secret"
    assert [%{password: entry}] = options[:certs_keys]
    assert entry.() == ~c"second-secret"
    assert [{~c"h.test", identity}] = options[:sni_hosts]
    assert identity[:password].() == "third-secret"
    assert Keyword.take(options, [:certfile, :keyfile]) == @identity

    callback = fn -> "provided" end
    assert TLSOptions.hell_options(password: callback)[:password] == callback
  end

  test "verify_peer?/1 reflects the listener's client-certificate request" do
    assert TLSOptions.verify_peer?(verify: :verify_peer)
    refute TLSOptions.verify_peer?(verify: :verify_none)
    refute TLSOptions.verify_peer?([])
  end
end
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/tls_options_test.exs
```

Expected: FAIL with `module GravitonMQ.Runtime.TLSOptions is not available`.

- [ ] **Step 3: Write the module**

```elixir
defmodule GravitonMQ.Runtime.TLSOptions do
  @moduledoc false
  # Broker TLS listener options (S1b, ex_ssl server). An allowlist, because
  # ex_ssl passes every unknown key to :gen_tcp, where a TLS option could change
  # carrier framing. Values are checked structurally here; ex_ssl loads and
  # checks content at listen. Errors carry the key only, never a value.

  @identity_keys ~w(cert certfile key keyfile password)a
  @keys @identity_keys ++
          ~w(certs_keys cacertfile cacerts verify fail_if_no_peer_cert depth ciphers
             versions alpn_preferred_protocols sni_hosts sni_fun)a
  @max_identities 16
  # The TLS 1.3 suites ex_ssl implements: {id, name, OTP cipher, OTP prf}.
  @suites [
    {0x1301, :tls_aes_128_gcm_sha256, :aes_128_gcm, :sha256},
    {0x1302, :tls_aes_256_gcm_sha384, :aes_256_gcm, :sha384},
    {0x1303, :tls_chacha20_poly1305_sha256, :chacha20_poly1305, :sha256}
  ]

  @type error :: {:unsupported_tls_option, atom()} | {:invalid_tls_option, atom()}

  @spec validate(term()) :: :ok | {:error, error()}
  def validate(options) do
    with :ok <- shape(options),
         :ok <- known(options),
         :ok <- values(options),
         :ok <- prerequisites(options),
         do: identity(options)
  end

  @spec verify_peer?(keyword()) :: boolean()
  def verify_peer?(options), do: Keyword.get(options, :verify, :verify_none) == :verify_peer

  # WORKAROUND(upstream): gsmlg-dev/http_fetch#168 — Hell keeps raw transport
  # options in [:hell, :listener, :start | :stop] telemetry metadata, its
  # Listener state and its child specs. ex_ssl accepts a 0-arity password fun,
  # which prints as #Function<...>. Keep until a pinned hell redacts them.
  @spec hell_options(keyword()) :: keyword()
  def hell_options(options), do: Enum.map(options, &protect/1)

  defp protect({:password, password}) when not is_function(password, 0),
    do: {:password, fn -> password end}

  defp protect({:certs_keys, entries}), do: {:certs_keys, Enum.map(entries, &protect_entry/1)}

  defp protect({:sni_hosts, hosts}),
    do:
      {:sni_hosts,
       Enum.map(hosts, fn {host, identity} -> {host, Enum.map(identity, &protect/1)} end)}

  defp protect(option), do: option

  defp protect_entry(%{password: password} = entry) when not is_function(password, 0),
    do: %{entry | password: fn -> password end}

  defp protect_entry(entry), do: entry

  defp shape(options) when is_list(options) do
    if Keyword.keyword?(options) do
      keys = Keyword.keys(options)

      case keys -- Enum.uniq(keys) do
        [] -> :ok
        [key | _] -> invalid(key)
      end
    else
      invalid(:tls_options)
    end
  end

  defp shape(_options), do: invalid(:tls_options)

  defp known(options) do
    case Enum.find(options, fn {key, _value} -> key not in @keys end) do
      nil -> :ok
      {key, _value} -> unsupported(key)
    end
  end

  defp values(options) do
    Enum.reduce_while(options, :ok, fn {key, value}, :ok ->
      case value(key, value) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp value(:certfile, value), do: check(path?(value), :certfile)
  defp value(:keyfile, value), do: check(path?(value), :keyfile)
  defp value(:cacertfile, value), do: check(path?(value), :cacertfile)
  defp value(:cert, value), do: check(der?(value), :cert)
  # WORKAROUND(upstream): gsmlg-dev/http_fetch#168 — an in-memory private key
  # would be exposed through Hell telemetry/state and cannot be wrapped.
  defp value(:key, _value), do: unsupported(:key)
  defp value(:password, value), do: check(password?(value), :password)
  defp value(:certs_keys, value), do: certs_keys(value)
  defp value(:cacerts, value), do: check(match?([_ | _], value) and Enum.all?(value, &der?/1), :cacerts)
  defp value(:verify, value), do: check(value in [:verify_none, :verify_peer], :verify)
  defp value(:fail_if_no_peer_cert, value), do: check(is_boolean(value), :fail_if_no_peer_cert)
  defp value(:depth, value), do: check(is_integer(value) and value in 1..100, :depth)
  defp value(:ciphers, value), do: check(ciphers?(value), :ciphers)
  defp value(:versions, [:"tlsv1.3"]), do: :ok
  defp value(:versions, _value), do: unsupported(:versions)
  defp value(:alpn_preferred_protocols, value), do: check(alpn?(value), :alpn_preferred_protocols)
  defp value(:sni_hosts, value), do: sni_hosts(value)
  defp value(:sni_fun, value), do: check(is_function(value, 1), :sni_fun)

  defp prerequisites(options) do
    verify_peer = verify_peer?(options)
    trust = Keyword.has_key?(options, :cacerts) or Keyword.has_key?(options, :cacertfile)

    cond do
      verify_peer and not trust -> invalid(:verify)
      Keyword.has_key?(options, :fail_if_no_peer_cert) and not verify_peer -> invalid(:fail_if_no_peer_cert)
      Keyword.has_key?(options, :sni_hosts) and Keyword.has_key?(options, :sni_fun) -> invalid(:sni_fun)
      true -> :ok
    end
  end

  # A default identity is required even with sni_hosts or sni_fun.
  defp identity(options) do
    certificate = Keyword.has_key?(options, :certfile) or Keyword.has_key?(options, :cert)

    cond do
      Keyword.has_key?(options, :certs_keys) -> :ok
      certificate and Keyword.has_key?(options, :keyfile) -> :ok
      certificate -> invalid(:keyfile)
      true -> invalid(:certfile)
    end
  end

  defp certs_keys(entries)
       when is_list(entries) and entries != [] and length(entries) <= @max_identities do
    Enum.reduce_while(entries, :ok, fn
      entry, :ok when is_map(entry) ->
        case identity_entry(Map.to_list(entry), :certs_keys) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end

      _entry, :ok ->
        {:halt, invalid(:certs_keys)}
    end)
  end

  defp certs_keys(_entries), do: invalid(:certs_keys)

  defp identity_entry(pairs, owner) do
    keys = Enum.map(pairs, &elem(&1, 0))

    cond do
      :key in keys -> unsupported(:key)
      not Enum.all?(keys, &(&1 in @identity_keys)) -> invalid(owner)
      not Enum.all?(pairs, fn {key, value} -> value(key, value) == :ok end) -> invalid(owner)
      not ((:cert in keys or :certfile in keys) and :keyfile in keys) -> invalid(owner)
      true -> :ok
    end
  end

  defp sni_hosts([_ | _] = hosts) do
    names =
      Enum.map(hosts, fn
        {host, _identity} when is_list(host) -> Enum.map(host, &downcase/1)
        _other -> nil
      end)

    if nil in names or names != Enum.uniq(names) do
      invalid(:sni_hosts)
    else
      Enum.reduce_while(hosts, :ok, fn {host, identity}, :ok ->
        result =
          if charlist?(host) and is_list(identity) and Keyword.keyword?(identity),
            do: sni_identity(identity),
            else: invalid(:sni_hosts)

        if result == :ok, do: {:cont, :ok}, else: {:halt, result}
      end)
    end
  end

  defp sni_hosts(_hosts), do: invalid(:sni_hosts)

  defp sni_identity(identity) do
    case Keyword.pop(identity, :certs_keys) do
      {nil, pairs} ->
        identity_entry(pairs, :sni_hosts)

      {entries, []} ->
        case certs_keys(entries) do
          :ok -> :ok
          {:error, {:unsupported_tls_option, _key}} = error -> error
          {:error, _invalid} -> invalid(:sni_hosts)
        end

      {_entries, _rest} ->
        invalid(:sni_hosts)
    end
  end

  defp ciphers?([_ | _] = entries) do
    ids = Enum.map(entries, &suite_id/1)
    nil not in ids and ids == Enum.uniq(ids)
  end

  defp ciphers?(_entries), do: false

  defp suite_id(entry) do
    Enum.find_value(@suites, fn {id, name, cipher, prf} ->
      forms = [
        id,
        name,
        name |> Atom.to_string() |> String.upcase(),
        %{key_exchange: :any, cipher: cipher, mac: :aead, prf: prf}
      ]

      if entry in forms, do: id
    end)
  end

  defp alpn?([_ | _] = protocols),
    do:
      Enum.all?(protocols, &(is_binary(&1) and byte_size(&1) in 1..255)) and
        protocols == Enum.uniq(protocols)

  defp alpn?(_protocols), do: false

  defp password?(value), do: is_binary(value) or charlist?(value) or is_function(value, 0)
  defp path?(value), do: (is_binary(value) and value != "") or (charlist?(value) and value != [])
  defp der?(value), do: is_binary(value) and value != ""
  defp charlist?(value), do: is_list(value) and Enum.all?(value, &(is_integer(&1) and &1 >= 0))
  defp downcase(char) when char in ?A..?Z, do: char + 32
  defp downcase(char), do: char
  defp check(true, _key), do: :ok
  defp check(false, key), do: invalid(key)
  defp invalid(key), do: {:error, {:invalid_tls_option, key}}
  defp unsupported(key), do: {:error, {:unsupported_tls_option, key}}
end
```

Note: the `[cert: "", keyfile: "k"]` case relies on `der?/1` rejecting `""`;
`password: ""` is accepted (ex_ssl decides at listen). `charlist?([])` is true,
so `password: []` is accepted as an empty charlist; `path?` rejects `[]`.

- [ ] **Step 4: Run the test to verify it passes**

Same command as Step 2. Expected: `7 tests, 0 failures`.

- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
P=(apps/graviton_mq_runtime/lib/graviton_mq/runtime/tls_options.ex apps/graviton_mq_runtime/test/tls_options_test.exs)
env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix format "${P[@]}"
git add -- "${P[@]}"
git commit --only -m "feat(runtime): validate broker TLS options against the ex_ssl allowlist" -- "${P[@]}"
```

---

## Task 3: MQTT configuration uses `TLSOptions`

**Files:** modify `apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/config.ex`
(`@tls_keys` :20-21, `build/1` else :70-71, `listeners/1` :155-177,
`valid_tls?/2` :181-188, `path?/1` :204), modify
`apps/graviton_mq_runtime/test/mqtt/config_test.exs` (:55-66)

- [ ] **Step 1: Failing test.** In `config_test.exs`, remove these two entries
from the `for options <- [...]` list of "rejects invalid resources, callbacks,
version and listener configuration": `[listeners: [[transport: :tls, ip: {127, 0, 0, 1}, port: 0]]]`
and the entry whose `tls_options` contain `active: true`. They move to the new
test below with their exact new error terms; this is not a weakening. Then add:

```elixir
  test "TLS listener options are validated by key with explicit error terms" do
    tls = fn options ->
      Config.build(
        auth: :anonymous,
        listeners: [[transport: :tls, ip: {127, 0, 0, 1}, port: 0, tls_options: options]]
      )
    end

    identity = [certfile: "cert.pem", keyfile: "key.pem"]

    assert {:ok, %{listeners: [%{transport: :tls, tls_options: ^identity}]}} = tls.(identity)
    assert {:error, {:invalid_tls_option, :certfile}} = tls.([])

    assert {:error, {:invalid_tls_option, :certfile}} =
             Config.build(
               auth: :anonymous,
               listeners: [[transport: :tls, ip: {127, 0, 0, 1}, port: 0]]
             )

    for key <- ~w(active packet send_timeout secure_renegotiate reuse_sessions honor_cipher_order)a do
      assert {:error, {:unsupported_tls_option, ^key}} = tls.(identity ++ [{key, true}])
    end

    assert {:error, {:unsupported_tls_option, :versions}} =
             tls.(identity ++ [versions: [:"tlsv1.3", :"tlsv1.2"]])

    # WORKAROUND(upstream): gsmlg-dev/http_fetch#168
    assert {:error, {:unsupported_tls_option, :key}} =
             tls.(certs_keys: [%{cert: <<48>>, key: {:RSAPrivateKey, <<48>>}}])

    assert {:error, {:invalid_tls_option, :fail_if_no_peer_cert}} =
             tls.(identity ++ [fail_if_no_peer_cert: true])

    # TCP listeners still take no TLS options at all.
    assert {:error, :invalid_mqtt_options} =
             Config.build(
               auth: :anonymous,
               listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0, tls_options: identity]]
             )
  end
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/mqtt/config_test.exs
```

Expected: the new test fails with `{:error, :invalid_mqtt_options}` in place of the TLS terms.

- [ ] **Step 3: Implement.** In `mqtt/config.ex`:
  - add `alias GravitonMQ.Runtime.TLSOptions` and
    `@tls_errors [:unsupported_tls_option, :invalid_tls_option]`; delete `@tls_keys`;
  - in `build/1`, change the outer `else` to

```elixir
    else
      {:error, {reason, _key}} = error when reason in @tls_errors -> error
      _invalid -> invalid()
    end
```

  - in `listeners/1`, replace `true <- valid_tls?(transport, tls) do` with
    `:ok <- tls(transport, tls) do`, and the inner `else` with

```elixir
      else
        {:error, {reason, _key}} = error when reason in @tls_errors -> {:halt, error}
        _invalid -> {:halt, invalid()}
      end
```

  - replace `valid_tls?/2` (all three clauses) with

```elixir
  defp tls(:tcp, []), do: :ok
  defp tls(:tcp, _tls), do: invalid()
  defp tls(:tls, tls), do: TLSOptions.validate(tls)
```

  - delete `path?/1` if nothing else uses it (`grep -n "path?" mqtt/config.ex`).
- [ ] **Step 4:** Run the same test. Expected: `0 failures`.
- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
P=(apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/config.ex apps/graviton_mq_runtime/test/mqtt/config_test.exs)
env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix format "${P[@]}"
git commit --only -m "feat(mqtt): validate TLS listener options by key" -- "${P[@]}"
```

---

## Task 4: AMQP configuration uses `TLSOptions` (shared-file protocol)

**Files:** modify `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/config.ex`
(`listener/2` :242-265, `certificate?/1`, `private_key?/1` :305-311), modify
`apps/graviton_mq_runtime/test/amqp10/config_test.exs` (:66-95)

- [ ] **Step 0:** `git status --porcelain -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/config.ex apps/graviton_mq_runtime/test/amqp10/config_test.exs` must print nothing (rule 3).
- [ ] **Step 1: Failing test.** Replace the test "TLS listener options cannot
replace passive binary framing or write limits" with this one. It keeps every
original option and tightens `{:error, _}` to the exact terms:

```elixir
  test "TLS listener options are allowlisted and report the offending key" do
    build = fn tls ->
      Config.build(
        auth: :anonymous,
        container_id: "broker",
        queues: ["q"],
        listeners: [[transport: :tls, tls_options: tls]]
      )
    end

    identity = [certfile: "server.pem", keyfile: "server.key"]

    for {option, key} <- [
          {[active: true], :active},
          {[mode: :list], :mode},
          {[packet: 4], :packet},
          {[header: 1], :header},
          {[deliver: :port], :deliver},
          {[packet_size: 0], :packet_size},
          {[send_timeout: :infinity], :send_timeout},
          {[send_timeout_close: false], :send_timeout_close},
          {[verify_fun: {fn _, _, s -> {:valid, s} end, nil}], :verify_fun},
          {[partial_chain: fn _ -> :unknown_ca end], :partial_chain},
          {[crl_check: true], :crl_check}
        ] do
      assert {:error, {:unsupported_tls_option, ^key}} = build.(identity ++ option)
    end

    assert {:error, {:unsupported_tls_option, :versions}} =
             build.(identity ++ [versions: [:"tlsv1.2"]])

    assert {:error, {:invalid_tls_option, :depth}} = build.(identity ++ [depth: 0])
    assert {:error, {:invalid_tls_option, :certfile}} = build.([])
    # WORKAROUND(upstream): gsmlg-dev/http_fetch#168
    assert {:error, {:unsupported_tls_option, :key}} =
             build.(cert: <<48>>, key: {:RSAPrivateKey, <<48>>})

    assert {:ok, %{listeners: [%{transport: :tls}]}} = build.(identity)

    # TCP listeners keep rejecting owned socket options.
    assert {:error, :invalid_listener} =
             Config.build(
               auth: :anonymous,
               container_id: "broker",
               queues: ["q"],
               listeners: [[transport: :tcp, tls_options: [active: true]]]
             )
  end
```

- [ ] **Step 2:** `cd .../apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/amqp10/config_test.exs`.
Expected: FAIL with `{:error, :invalid_listener}` / `{:ok, _}` instead of the TLS terms.
- [ ] **Step 3: Implement** (`alias GravitonMQ.Runtime.TLSOptions`). In `listener/2`
replace the `valid = ...` expression and the `if valid` line with

```elixir
      valid =
        listener.transport in [:tcp, :tls] and valid_ip?(listener.ip) and
          bounded?(listener.port, 0, 65_535) and bounded?(listener.backlog, 1, 1024) and
          is_list(tls) and Keyword.keyword?(tls) and
          (listener.transport == :tls or
             Enum.all?(tls, fn {key, _} -> key not in @owned_socket_options end))

      cond do
        not valid -> {:error, :invalid_listener}
        listener.transport == :tls -> with :ok <- TLSOptions.validate(tls), do: {:ok, listener}
        true -> {:ok, listener}
      end
```

and delete `certificate?/1` and `private_key?/1`. `build/1`'s `{:error, _} = error -> error` already passes the terms through.
- [ ] **Step 4:** Run the same file. Expected: `0 failures`.
- [ ] **Step 5: Format and commit** (paths: the two files) with message `feat(amqp): validate TLS listener options by key`.

---

## Task 5: `HellHandoff` serves both transports (spec §4.1, §4.8, D5)

**Files:** modify `apps/graviton_mq_runtime/lib/graviton_mq/runtime/hell_handoff.ex`
(:1-41 comment, :93-119 `start_link`), the two call sites
`mqtt/listener.ex:43` and `amqp10/listener.ex:57` (shared-file protocol for the
AMQP one), and replace `apps/graviton_mq_runtime/test/hell_handoff_test.exs`.

- [ ] **Step 0:** `git status --porcelain -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/listener.ex` prints nothing.
- [ ] **Step 1: Write the parametrized test** (replaces the whole file; every
S1a assertion is kept for TCP and also applied to TLS)

```elixir
Code.require_file("support/tls.exs", __DIR__)

defmodule GravitonMQ.Runtime.HellHandoffTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias GravitonMQ.Runtime.HellHandoff
  alias GravitonMQ.Runtime.MQTT.Listener
  alias GravitonMQ.Runtime.Test.TLS

  @compile {:no_warn_undefined, :ssl}
  @moduletag timeout: 20_000
  # Below the module timeout so a stalled stop fails the call, not the case.
  @stop_call 8_000

  defmodule Probe do
    use GenServer, restart: :temporary

    def start_link({observer, linger}), do: GenServer.start_link(__MODULE__, {observer, linger})

    @impl true
    def init({observer, linger}) do
      if linger > 0, do: Process.flag(:trap_exit, true)
      {:ok, %{observer: observer, linger: linger, socket: nil}}
    end

    @impl true
    def handle_call({:controlling_process, owner}, _from, state),
      do: {:reply, SSL.controlling_process(state.socket, owner), state}

    @impl true
    def handle_info({:accepted, socket} = message, state) do
      send(state.observer, {message, self()})
      :ok = arm(socket)
      {:noreply, %{state | socket: socket}}
    end

    def handle_info({kind, socket, data}, state) when kind in [:tcp, :ssl] do
      send(state.observer, {:data, socket, data})
      {:noreply, state}
    end

    def handle_info(_other, state), do: {:noreply, state}

    @impl true
    def terminate(_reason, state) do
      send(state.observer, {:terminating, self(), System.monotonic_time(:millisecond)})
      Process.sleep(state.linger)
    end

    defp arm(%SSL.Socket{} = socket), do: SSL.setopts(socket, active: :once)
    defp arm(socket), do: :inet.setopts(socket, active: :once)
  end

  # Stands in for a protocol listener: it owns the Hell server, publishes the
  # Connection start argument (or withholds it and answers late, or loses
  # its table, or never answers) like a listener before and after wiring.
  defmodule Owner do
    use GenServer

    alias GravitonMQ.Runtime.HellHandoff

    def start_link(options), do: GenServer.start_link(__MODULE__, options)

    @impl true
    def init(options) do
      Process.flag(:trap_exit, true)
      table = options.table || HellHandoff.table()

      listener = %{
        ip: {127, 0, 0, 1},
        port: 0,
        backlog: 16,
        transport: options.transport,
        tls_options: options.tls_options
      }

      settings = %{
        send_timeout: 4_321,
        handshake_timeout: options.handshake_timeout,
        max_connections: options.max_connections
      }

      {:ok, server} = HellHandoff.start_link(listener, settings, table, options.handoff)
      state = Map.merge(options, %{server: server, table: table, calls: 0})
      if options.publish, do: :ok = HellHandoff.publish(table, spec(state))
      {:ok, state}
    end

    @impl true
    def handle_call(:server, _from, state), do: {:reply, state.server, state}
    def handle_call(:calls, _from, state), do: {:reply, state.calls, state}

    def handle_call(:stop_server, _from, state),
      do: {:reply, HellHandoff.stop(state.server, state.table), state}

    def handle_call(:connection_spec, from, state) do
      if is_integer(state.reply_after),
        do: Process.send_after(self(), {:answer, from}, state.reply_after)

      {:noreply, %{state | calls: state.calls + 1}}
    end

    @impl true
    def handle_info({:answer, from}, state) do
      GenServer.reply(from, spec(state))
      {:noreply, state}
    end

    def handle_info(_message, state), do: {:noreply, state}

    defp spec(state),
      do: %{connections: state.connections, child: {Probe, {state.observer, state.linger}}}
  end

  setup_all do
    %{certificates: TLS.certificates!()}
  end

  for transport <- [:tcp, :tls] do
    describe "#{transport}" do
      @describetag transport: transport

      test "hands an accepted socket to a supervised child and lives exactly as long as it",
           context do
        %{owner: owner, server: server, port: port} = server(context)
        client = connect(context, port)

        assert_receive {{:accepted, raw}, probe}, 1_000
        assert_owner(context, raw, probe)
        # After publication, handlers read the table and never call the listener.
        assert GenServer.call(owner, :calls) == 0

        assert {:ok, [send_timeout: 4_321, send_timeout_close: true, nodelay: false]} =
                 getopts(context, raw, [:send_timeout, :send_timeout_close, :nodelay])

        :ok = client_send(context, client, "hello")
        assert_receive {:data, ^raw, "hello"}, 1_000

        :ok = GenServer.stop(probe)
        assert {:error, :closed} = client_recv(context, client, 1_000)
        assert eventually(fn -> Hell.connection_pids(server) == {:ok, []} end)
      end

      test "the handler ignores stray messages and redacts its status", context do
        %{server: server, port: port} = server(context)
        client = connect(context, port)
        assert_receive {{:accepted, raw}, _probe}, 1_000
        assert {:ok, [handler]} = Hell.connection_pids(server)
        send(handler, :stray)
        assert {:status, ^handler, _module, status} = :sys.get_status(handler)
        refute inspect(status, limit: :infinity) =~ "Hell.Socket"
        refute inspect(status, limit: :infinity) =~ inspect(raw)
        assert Process.alive?(handler)
        assert {:error, :timeout} = client_recv(context, client, 50)
      end

      test "closes the connection when the child supervisor refuses it", context do
        %{server: server, port: port} = server(context, max_children: 0)
        client = connect(context, port)

        assert {:error, :closed} = client_recv(context, client, 1_000)
        refute_receive {{:accepted, _raw}, _probe}, 50
        assert eventually(fn -> Hell.connection_pids(server) == {:ok, []} end)
      end

      test "a connection accepted before the listener publishes waits for its answer", context do
        %{owner: owner, port: port} = server(context, publish: false, reply_after: 800)
        started = System.monotonic_time(:millisecond)
        client = connect(context, port)

        assert_receive {{:accepted, raw}, _probe}, 2_000
        assert System.monotonic_time(:millisecond) - started >= 800
        assert GenServer.call(owner, :calls) == 1
        :ok = client_send(context, client, "late")
        assert_receive {:data, ^raw, "late"}, 1_000
      end

      test "a listener whose table is gone closes the connection and says so", context do
        table = Task.async(fn -> HellHandoff.table() end) |> Task.await()

        log =
          capture_log(fn ->
            %{server: server, port: port} = server(context, publish: false, table: table)
            client = connect(context, port)

            assert {:error, :closed} = client_recv(context, client, 1_000)
            assert eventually(fn -> Hell.connection_pids(server) == {:ok, []} end)
          end)

        assert log =~ "Hell handoff closed a connection: its listener is unavailable"
        refute_received {{:accepted, _raw}, _probe}
      end

      test "stopping the server closes accepted connections and stops their live children",
           context do
        %{owner: owner, port: port} = server(context)
        client = connect(context, port)
        assert_receive {{:accepted, _raw}, probe}, 1_000
        probe_monitor = monitor!(probe)

        assert :ok = GenServer.call(owner, :stop_server, @stop_call)
        assert {:error, :closed} = client_recv(context, client, 1_000)
        # A non-owner close sends the owner nothing; the handler stops the child itself.
        assert_receive {:DOWN, ^probe_monitor, :process, ^probe, :shutdown}, 1_000
      end

      test "stopping releases slow children in parallel across acceptors and leaves nothing behind",
           context do
        %{owner: owner, server: server, port: port} = server(context, linger: 1_500)
        {clients, handlers} = spread(context, port, server, [])
        probes = for _ <- clients, do: receive_probe()
        started = System.monotonic_time(:millisecond)

        assert :ok = GenServer.call(owner, :stop_server, @stop_call)

        elapsed = System.monotonic_time(:millisecond) - started
        starts = for _ <- probes, do: assert_receive({:terminating, _probe, at}, 1_000) && at
        assert Enum.max(starts) - Enum.min(starts) < 750
        assert elapsed < 4_000
        refute Process.alive?(server)
        refute Enum.any?(handlers ++ probes, &Process.alive?/1)
        for client <- clients, do: assert({:error, :closed} = client_recv(context, client, 1_000))
      end

      test "a child that outlives the release budget is killed", context do
        %{owner: owner, server: server, port: port} =
          server(context, linger: 2_000, handoff: [connection_stop: 200])

        client = connect(context, port)
        probe = receive_probe()
        probe_monitor = monitor!(probe)
        started = System.monotonic_time(:millisecond)

        assert :ok = GenServer.call(owner, :stop_server, @stop_call)

        assert System.monotonic_time(:millisecond) - started < 1_500
        assert_receive {:DOWN, ^probe_monitor, :process, ^probe, :killed}, 1_000
        refute Process.alive?(server)
        assert {:error, :closed} = client_recv(context, client, 1_000)
      end

      test "stopping before publication does not wait on handlers blocked in the listener call",
           context do
        %{owner: owner, server: server, port: port} =
          server(context, publish: false, reply_after: :never)

        client = connect(context, port)
        assert eventually(fn -> GenServer.call(owner, :calls) == 1 end)
        {:ok, handlers} = Hell.connection_pids(server)
        started = System.monotonic_time(:millisecond)

        assert :ok = GenServer.call(owner, :stop_server, @stop_call)
        assert System.monotonic_time(:millisecond) - started < 500

        log =
          capture_log(fn ->
            :ok = stop_supervised(Owner)
            assert eventually(fn -> not Enum.any?([server | handlers], &Process.alive?/1) end)
          end)

        assert log =~ "its listener is unavailable"
        assert {:error, :closed} = client_recv(context, client, 1_000)
        refute_received {{:accepted, _raw}, _probe}
      end

      # T8 (unit): the rebind error shape is the same for TLS (ex_ssl returns
      # the :gen_tcp.listen/2 error unchanged), so the retry applies.
      test "rebinding a fixed port fails explicitly once the retry budget is spent", context do
        {:ok, holder} =
          :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])

        {:ok, port} = :inet.port(holder)
        Process.flag(:trap_exit, true)
        started = System.monotonic_time(:millisecond)

        assert {:error, {:shutdown, {:failed_to_start_child, :listener, :eaddrinuse}}} =
                 HellHandoff.start_link(
                   %{
                     ip: {127, 0, 0, 1},
                     port: port,
                     backlog: 16,
                     transport: context.transport,
                     tls_options: tls_options(context)
                   },
                   %{send_timeout: 1_000, handshake_timeout: 1_000, max_connections: 16},
                   HellHandoff.table()
                 )

        elapsed = System.monotonic_time(:millisecond) - started
        assert elapsed >= 1_000 and elapsed < 2_000
        refute_receive {:EXIT, _pid, _reason}, 50
      end
    end
  end

  describe "tls only" do
    @describetag transport: :tls

    # T1
    test "after handoff the handler is no longer the owner, the Connection is, and its exit closes the peer",
         context do
      %{server: server, port: port} = server(context)
      client = connect(context, port)
      assert_receive {{:accepted, raw}, probe}, 1_000
      {:ok, [handler]} = Hell.connection_pids(server)
      test = self()

      # The function runs inside the handler process.
      :sys.replace_state(handler, fn state ->
        send(test, {:from_handler, SSL.controlling_process(raw, test)})
        state
      end)

      assert_receive {:from_handler, {:error, :not_owner}}
      assert :ok = GenServer.call(probe, {:controlling_process, probe})
      :ok = GenServer.stop(probe)
      assert {:error, :closed} = client_recv(context, client, 1_000)
    end

    # T2 (handoff level)
    test "a TLS 1.2-only client is refused with protocol_version before any child starts",
         context do
      %{port: port} = server(context)

      assert {:error, {:tls_alert, {:protocol_version, _description}}} =
               TLS.connect(port, context.certificates, versions: [:"tlsv1.2"])

      refute_receive {{:accepted, _raw}, _probe}, 100
    end

    # T4: handshake budget
    test "silent and stalled peers are closed by the handshake timeout before any child starts",
         context do
      %{server: server, port: port} = server(context, handshake_timeout: 300)

      # A bare TCP peer, and one stalled inside a ClientHello record.
      for prefix <- [<<>>, <<22, 3, 1, 0, 64, 1, 0, 0, 60>>] do
        {:ok, raw} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
        started = System.monotonic_time(:millisecond)
        :ok = :gen_tcp.send(raw, prefix)
        assert :closed = closed(raw, started + 2_000)
        elapsed = System.monotonic_time(:millisecond) - started
        assert elapsed >= 250 and elapsed < 1_000, "closed after #{elapsed} ms"
      end

      refute_received {{:accepted, _raw}, _probe}
      assert eventually(fn -> Hell.connection_pids(server) == {:ok, []} end)
    end

    # T4: num_connections = max_connections bounds handshakes too
    test "max_connections bounds concurrent TLS connections including handshakes", context do
      %{port: port} = server(context, max_connections: 1)
      first = connect(context, port)
      assert_receive {{:accepted, _raw}, probe}, 1_000

      assert {:error, _reason} = TLS.connect(port, context.certificates, [], 500)
      refute_received {{:accepted, _raw}, _probe}

      :ok = GenServer.stop(probe)
      assert {:error, :closed} = client_recv(context, first, 1_000)
      # Hell's acceptor rechecks the limit once per second (5 attempts).
      assert {:ok, _third} = TLS.connect(port, context.certificates, [], 4_000)
      assert_receive {{:accepted, _raw}, _probe}, 4_000
    end
  end

  test "a handler released before Hell hands it a socket stops at once" do
    {:ok, handler} = GenServer.start(HellHandoff, {self(), HellHandoff.table(), 5_000})
    monitor = Process.monitor(handler)
    send(handler, :hell_handoff_release)
    assert_receive {:DOWN, ^monitor, :process, ^handler, {:shutdown, :released}}, 1_000
  end

  test "shutdown budgets nest and the listener child spec uses the outermost one" do
    budgets = HellHandoff.budgets()

    assert budgets[:connection_stop] + budgets[:kill_wait] < budgets[:handler_shutdown]
    assert budgets[:handler_shutdown] < budgets[:stop_wait]
    assert budgets[:handler_shutdown] + budgets[:stop_wait] < budgets[:owner_shutdown]
    assert budgets[:owner_shutdown] == HellHandoff.owner_shutdown()

    for transport <- [:tcp, :tls] do
      assert %{shutdown: shutdown} = Listener.child_spec({%{}, %{transport: transport}, self()})
      assert shutdown == HellHandoff.owner_shutdown()
    end
  end

  defp server(context, options \\ []) do
    {supervisor_options, options} = Keyword.split(options, [:max_children])

    connections =
      start_supervised!({DynamicSupervisor, [strategy: :one_for_one] ++ supervisor_options})

    options =
      %{
        publish: true,
        reply_after: 0,
        table: nil,
        linger: 0,
        handoff: [],
        handshake_timeout: 1_000,
        max_connections: 64
      }
      |> Map.merge(Map.new(options))
      |> Map.merge(%{transport: context.transport, tls_options: tls_options(context)})

    owner =
      start_supervised!(
        {Owner, Map.merge(options, %{connections: connections, observer: self()})},
        restart: :temporary,
        id: Owner
      )

    server = GenServer.call(owner, :server)
    {:ok, {_ip, port}} = Hell.listener_info(server)
    %{owner: owner, server: server, port: port}
  end

  defp tls_options(%{transport: :tcp}), do: []
  defp tls_options(%{transport: :tls, certificates: certificates}), do: TLS.listener_options(certificates)

  defp connect(%{transport: :tcp}, port) do
    {:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    client
  end

  defp connect(%{transport: :tls, certificates: certificates}, port) do
    {:ok, client} = TLS.connect(port, certificates)
    client
  end

  defp client_send(%{transport: :tcp}, client, data), do: :gen_tcp.send(client, data)
  defp client_send(%{transport: :tls}, client, data), do: :ssl.send(client, data)
  defp client_recv(%{transport: :tcp}, client, timeout), do: :gen_tcp.recv(client, 0, timeout)
  defp client_recv(%{transport: :tls}, client, timeout), do: :ssl.recv(client, 0, timeout)
  defp getopts(%{transport: :tcp}, raw, keys), do: :inet.getopts(raw, keys)
  defp getopts(%{transport: :tls}, raw, keys), do: SSL.getopts(raw, keys)

  defp assert_owner(%{transport: :tcp}, raw, probe),
    do: assert(:erlang.port_info(raw, :connected) == {:connected, probe})

  defp assert_owner(%{transport: :tls}, raw, probe) do
    assert {:error, :not_owner} = SSL.controlling_process(raw, self())
    assert :ok = GenServer.call(probe, {:controlling_process, probe})
  end

  # Monitor, then a synchronous call so the monitor is in place before any
  # stimulus from another process.
  defp monitor!(pid) do
    monitor = Process.monitor(pid)
    _ = :sys.get_state(pid)
    monitor
  end

  # Reads (and drops) any alert bytes until the server closes.
  defp closed(raw, deadline) do
    case :gen_tcp.recv(raw, 0, max(deadline - System.monotonic_time(:millisecond), 0)) do
      {:ok, _bytes} -> closed(raw, deadline)
      {:error, reason} -> reason
    end
  end

  # Connects until at least 8 handlers run under at least 2 acceptors.
  defp spread(context, port, server, clients) do
    {:ok, handlers} = Hell.connection_pids(server)
    parents = handlers |> Enum.map(&Process.info(&1, :parent)) |> Enum.uniq()

    if length(clients) >= 8 and length(handlers) == length(clients) and length(parents) >= 2 do
      {clients, handlers}
    else
      assert length(clients) < 64
      client = connect(context, port)

      assert eventually(fn ->
               Hell.connection_pids(server) |> elem(1) |> length() > length(clients)
             end)

      spread(context, port, server, [client | clients])
    end
  end

  defp receive_probe do
    assert_receive {{:accepted, _raw}, probe}, 1_000
    probe
  end

  defp eventually(predicate, deadline \\ System.monotonic_time(:millisecond) + 2_000) do
    cond do
      predicate.() -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true -> eventually_after_yield(predicate, deadline)
    end
  end

  defp eventually_after_yield(predicate, deadline) do
    receive do
    after
      10 -> eventually(predicate, deadline)
    end
  end
end
```

Notes: `eventually` grows from 1 s to 2 s because TLS handshakes are slower;
that is a timing allowance, not a weaker assertion. `SSL.getopts/2` on a server
socket reads the carrier through `:inet.getopts/2` (`server_connection.ex:472`),
so the TCP keys apply unchanged.

- [ ] **Step 2: Run to verify it fails**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/hell_handoff_test.exs
```

Expected: the `tcp` cases fail with a `FunctionClauseError`/`BadMapError`
(`start_link/4` still takes an integer `send_timeout`), and the `tls` cases fail
the same way.

- [ ] **Step 3: Implement `start_link/4`** — replace `:92-119`:

```elixir
  # `settings`: %{send_timeout:, handshake_timeout:, max_connections:}.
  # `options` exist for tests only; production uses the defaults above.
  def start_link(listener, settings, table, options \\ []) do
    connection_stop = Keyword.get(options, :connection_stop, @connection_stop)

    options =
      [
        port: listener.port,
        transport_module: transport_module(listener.transport),
        transport_options: transport_options(listener, settings),
        handler_module: __MODULE__,
        handler_options: {self(), table, connection_stop},
        num_acceptors: @num_acceptors,
        read_timeout: :infinity,
        # The TLS handshake runs in the handler before handle_connection/2;
        # it also bounds the server flight write and sni_fun. A no-op for TCP.
        handshake_timeout: settings.handshake_timeout,
        shutdown_timeout: @handler_shutdown
      ] ++ connection_limit(listener.transport, settings)

    start(options, System.monotonic_time(:millisecond) + @rebind_budget)
  end

  defp transport_module(:tcp), do: Hell.Transports.TCP
  defp transport_module(:tls), do: Hell.Transports.SSL

  defp transport_options(listener, settings) do
    carrier = [
      ip: listener.ip,
      backlog: listener.backlog,
      reuseaddr: true,
      buffer: 16_384,
      recbuf: 32_768,
      send_timeout: settings.send_timeout,
      send_timeout_close: true,
      # Hell defaults nodelay to true; false preserves the pre-Hell
      # listener behaviour. Changing it is a separate decision.
      nodelay: false
    ]

    case listener.transport do
      :tcp -> carrier
      # Validated by TLSOptions in the protocol Config; passwords as closures.
      :tls -> carrier ++ TLSOptions.hell_options(listener.tls_options)
    end
  end

  # A TLS connection costs a handler and an ex_ssl server process from accept,
  # so the protocol's max_connections also bounds in-progress handshakes. TCP
  # keeps Hell's default (S1a).
  defp connection_limit(:tls, settings), do: [num_connections: settings.max_connections]
  defp connection_limit(:tcp, _settings), do: []
```

Add `alias GravitonMQ.Runtime.TLSOptions`. In the header comment change the
first line to "Hell owns plain-TCP and TLS accept only (TLS: ex_ssl through
`Hell.Transports.SSL`, handshaken in the handler before `handle_connection/2`;
a failed handshake starts no Connection)", and add to the contract the sentence
"A TLS socket handed over is a `%SSL.Socket{}` the Connection owns; its exit
closes the TLS process through the owner monitor." Also change "A socket closed
by a non-owner sends the owner no `{:tcp_closed, socket}`" to "... no
`{:tcp_closed, _}`/`{:ssl_closed, _}`".

- [ ] **Step 4: Call sites (TCP path only; TLS is unchanged until Tasks 6/7).**
  - `mqtt/listener.ex:43` becomes
    `:tcp -> HellHandoff.start_link(listener, handoff_settings(config), handoff)`,
    with

```elixir
  defp handoff_settings(config),
    do: %{
      send_timeout: config.send_timeout,
      handshake_timeout: config.connect_timeout,
      max_connections: Keyword.get(config.machine_options, :max_connections, 128)
    }
```

  - `amqp10/listener.ex:57` becomes
    `:tcp -> HellHandoff.start_link(listener, handoff_settings(config), handoff)`,
    with

```elixir
  defp handoff_settings(config),
    do: %{
      send_timeout: config.send_timeout,
      handshake_timeout: config.connect_timeout,
      max_connections: config.max_connections
    }
```

(`128` is the same default `mqtt/supervisor.ex:40` uses for `max_children`.)

- [ ] **Step 5: Verify**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/hell_handoff_test.exs
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/graviton_mq/mqtt_connection_test.exs test/graviton_mq/amqp10_runtime_test.exs
```

Expected: `0 failures`. If "max_connections bounds ..." fails because Hell
gives up after its 5 retries and its acceptor exits, record the observed
behaviour verbatim. Do not lengthen timeouts past 4 s; report it instead
(it changes the spec §4.8 statement).

- [ ] **Step 6: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
P=(apps/graviton_mq_runtime/lib/graviton_mq/runtime/hell_handoff.ex apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/listener.ex apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/listener.ex apps/graviton_mq_runtime/test/hell_handoff_test.exs)
env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix format "${P[@]}"
git commit --only -m "feat(runtime): hand off TLS sockets accepted by Hell over ex_ssl" -- "${P[@]}"
```

---

## Task 6: MQTT listener, Connection and Writer on the single Hell path

**Files:** modify `apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/{listener,connection,writer}.ex`;
create `apps/graviton_mq/test/graviton_mq/mqtt_tls_test.exs` (round trip only here; T-tests follow in Tasks 8–12)

- [ ] **Step 1: Failing round-trip test** — create `G/test/graviton_mq/mqtt_tls_test.exs`:

```elixir
Code.require_file("../../../graviton_mq_runtime/test/support/tls.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.TLSTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Connack, Disconnect, Suback, Unsuback}
  alias GravitonMQ.Runtime.MQTT.Listener
  alias GravitonMQ.Runtime.Test.TLS

  @compile {:no_warn_undefined, :ssl}
  @moduletag timeout: 30_000

  defmodule Policy do
    def authenticate(credentials, options) do
      send(options[:observer], {:peer, credentials.client_id, credentials.peer})
      {:ok, :client}
    end

    def authorize(_principal, _action, _resource, _options), do: :ok
  end

  setup_all do
    %{certificates: TLS.certificates!()}
  end

  test "a verified TLS 1.3 client completes CONNECT and PINGREQ; the server socket is TLS 1.3",
       %{certificates: certificates} do
    instance = broker(certificates)
    socket = tls_connected(port(instance), certificates, "tls-round-trip")
    assert_ping(socket)

    assert {:ok, information} = SSL.connection_information(server_socket(connection!(instance)))
    assert information[:protocol] == :"tlsv1.3"
    assert information[:peer_authenticated] == false
  end

  # Helpers (used by all MQTT TLS tests).

  defp broker(certificates, options \\ [], tls \\ []) do
    start_supervised!(
      {GravitonMQ,
       name: nil,
       runtime_supervisor_name: nil,
       mqtt:
         Keyword.merge(
           [
             auth: :anonymous,
             host_principal: :test_host,
             connect_timeout: 500,
             tick_interval: 10,
             listeners: [tls_listener(certificates, tls)]
           ],
           options
         )},
      id: make_ref()
    )
  end

  defp tls_listener(certificates, tls \\ [], id \\ :tls, port \\ 0),
    do: [
      id: id,
      transport: :tls,
      ip: {127, 0, 0, 1},
      port: port,
      tls_options: TLS.listener_options(certificates, tls)
    ]

  defp status(instance) do
    assert {:ok, status} = GravitonMQ.status(instance)
    status
  end

  defp port(instance, id \\ :tls) do
    %{port: port} = Enum.find(status(instance).listeners, &(&1.id == id))
    port
  end

  defp tls_connected(port, certificates, client_id, options \\ []) do
    assert {:ok, socket} = TLS.connect(port, certificates, options)
    on_exit(fn -> :ssl.close(socket) end)
    assert :ok = :ssl.send(socket, connect_packet(client_id))
    assert %Connack{reason_code: 0} = receive_packet(socket)
    socket
  end

  defp tcp_connected(port, client_id) do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    :ok = :gen_tcp.send(socket, connect_packet(client_id))
    {:ok, <<0x20, _rest::binary>>} = :gen_tcp.recv(socket, 0, 1_000)
    socket
  end

  defp connect_packet(client_id) do
    # MQTT 5, Clean Start, keep alive 0, no credentials/Will, empty properties.
    body = <<0, 4, "MQTT", 5, 2, 0::16, 0, byte_size(client_id)::16, client_id::binary>>
    <<0x10, byte_size(body), body::binary>>
  end

  defp assert_ping(socket) do
    assert :ok = :ssl.send(socket, <<0xC0, 0>>)
    assert {:ok, <<0xD0, 0>>} = :ssl.recv(socket, 2, 2_000)
  end

  defp receive_packet(socket, timeout \\ 2_000) do
    {[packet], _rest} = receive_packets(socket, 1, <<>>, System.monotonic_time(:millisecond) + timeout)
    packet
  end

  # Reads exactly `count` packets; returns them in wire order plus leftovers.
  defp receive_packets(socket, count, bytes, deadline, packets \\ [])

  defp receive_packets(_socket, 0, bytes, _deadline, packets), do: {Enum.reverse(packets), bytes}

  defp receive_packets(socket, count, bytes, deadline, packets) do
    case Packet.decode(bytes, :v5) do
      {:ok, packet, rest} ->
        receive_packets(socket, count - 1, rest, deadline, [packet | packets])

      {:more, _needed} ->
        remaining = max(deadline - System.monotonic_time(:millisecond), 0)
        assert {:ok, more} = :ssl.recv(socket, 0, remaining)
        receive_packets(socket, count, bytes <> more, deadline, packets)

      other ->
        flunk("unexpected MQTT bytes: #{inspect(other)}")
    end
  end

  defp mqtt_supervisor(instance) do
    instance
    |> child(GravitonMQ.Runtime.Supervisor)
    |> child(GravitonMQ.Runtime.MQTT.Supervisor)
  end

  defp connections(instance), do: child(mqtt_supervisor(instance), :connections)

  defp connection!(instance) do
    supervisor = connections(instance)

    assert eventually(fn -> match?([_], DynamicSupervisor.which_children(supervisor)) end)
    [{_id, connection, _type, _modules}] = DynamicSupervisor.which_children(supervisor)
    connection
  end

  # The MQTT Connection state is a plain map; :sys.get_state/1 bypasses the
  # redacting format_status/1 and also works while the process is suspended.
  defp server_socket(connection), do: :sys.get_state(connection).socket

  defp listener!(instance, id) do
    listeners = child(mqtt_supervisor(instance), :listeners)
    child(listeners, {Listener, id})
  end

  defp find_listener(instance, id) do
    case find_child(mqtt_supervisor(instance), :listeners) do
      nil -> nil
      listeners -> find_child(listeners, {Listener, id})
    end
  end

  defp child(supervisor, id) do
    pid = find_child(supervisor, id)
    assert is_pid(pid)
    pid
  end

  defp find_child(supervisor, id) do
    case List.keyfind(Supervisor.which_children(supervisor), id, 0) do
      {^id, pid, _type, _modules} when is_pid(pid) -> pid
      _ -> nil
    end
  end

  defp monitor!(pid) do
    monitor = Process.monitor(pid)
    _ = :sys.get_state(pid)
    monitor
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp eventually(predicate, timeout \\ 2_000),
    do: eventually_until(predicate, System.monotonic_time(:millisecond) + timeout)

  defp eventually_until(predicate, deadline) do
    cond do
      predicate.() -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true -> Process.sleep(5) && eventually_until(predicate, deadline)
    end
  end
end
```

(`Process.sleep(5)` returns `:ok`, which is truthy, so the `&&` recursion
works. Unused aliases such as `Disconnect` will warn until Tasks 8–12 use them.
If `--warnings-as-errors` matters for this checkpoint, drop them now and add
them back with their tests.)

- [ ] **Step 2: Run to verify it fails**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/graviton_mq/mqtt_tls_test.exs
```

Expected: FAIL. The OTP `:ssl` listener still serves TLS, so
`SSL.connection_information/1` gets an OTP `:sslsocket` record and raises a
`FunctionClauseError`, or the call returns `{:error, :badarg}`.

- [ ] **Step 3: MQTT Listener.** In `mqtt/listener.ex`:
  - `init/1`: delete the OTP `options` list and both `case listener.transport`
    blocks, and write

```elixir
  @impl true
  def init(configuration) do
    Process.flag(:trap_exit, true)
    {config, listener, parent} = configuration.()
    handoff = HellHandoff.table()

    case HellHandoff.start_link(listener, handoff_settings(config), handoff) do
      {:ok, server} ->
        {:ok, {ip, port}} = Hell.listener_info(server)

        config =
          config
          |> Map.take([
            :limits,
            :connect_timeout,
            :send_timeout,
            :max_writer_packets,
            :max_writer_bytes
          ])
          |> Map.put(:client_certificate_verified, TLSOptions.verify_peer?(listener.tls_options))

        {:ok,
         %{
           config: config,
           parent: parent,
           socket: server,
           handoff: handoff,
           transport: listener.transport,
           info: %{id: listener.id, transport: listener.transport, ip: ip, port: port},
           broker: nil,
           connections: nil,
           wiring: nil,
           waiting: []
         }, {:continue, :wire}}

      {:error, _reason} ->
        {:stop, {:listener_setup_failed, listener.transport}}
    end
  end
```

  - drop the `transport: :tcp` match from both `handle_call(:connection_spec, ...)`
    clauses (keep `broker: nil` on the first);
  - in `wire/2`, replace the `case state.transport do ... end` with its `:tcp` body only;
  - `info({:EXIT, server, _reason}, %{socket: server} = state)` (no transport match); delete `info(:accept, ...)`;
  - `connection_spec/1`: child `{Connection, {state.config, state.broker, state.transport}}`;
  - `terminate/2`: one clause `def terminate(_reason, %{socket: server, handoff: table}), do: HellHandoff.stop(server, table)`;
  - update the comments that say "Plain TCP accept runs in Hell" to "Accept (TCP and TLS) runs in Hell";
  - add `alias GravitonMQ.Runtime.TLSOptions`.
- [ ] **Step 4: MQTT Connection** (`mqtt/connection.ex`):
  - `info({:accepted, socket}, %{socket: nil} = state)`: delete the
    `case handshake(...)` wrapper and its `{:error, _}` branch, so the body
    starts with `state = %{state | socket: socket}` and keeps the existing
    Writer/`writer_ready` code unchanged;
  - `arm/1`: `:tls -> SSL.setopts(state.socket, active: :once)`;
  - `peer/1`: `:tls -> SSL.peername(state.socket)` and
    `case SSL.peercert(state.socket) do {:ok, der} -> der; _ -> nil end`;
  - delete both `handshake/3` clauses; `close_socket(:tls, socket), do: SSL.close(socket)`.
- [ ] **Step 5: MQTT Writer** (`mqtt/writer.ex:109`): `defp socket_send(:tls, socket, bytes), do: SSL.send(socket, bytes)`.
- [ ] **Step 6: Verify**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/graviton_mq/mqtt_tls_test.exs test/graviton_mq/mqtt_connection_test.exs test/graviton_mq/mqtt_privacy_test.exs test/graviton_mq/mqtt_host_test.exs
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/hell_handoff_test.exs test/mqtt
grep -n ":ssl\." /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/*.ex
```

Expected: `0 failures`; the grep prints nothing.

- [ ] **Step 7: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
P=(apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/listener.ex apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/connection.ex apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/writer.ex apps/graviton_mq/test/graviton_mq/mqtt_tls_test.exs)
env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix format "${P[@]}"
git add -- "${P[@]}"
git commit --only -m "feat(mqtt): serve broker TLS listeners through Hell and ex_ssl" -- "${P[@]}"
```

---

## Task 7: AMQP listener, Connection and Writer (shared-file protocol)

**Files:** modify `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/{listener,connection,writer}.ex`,
`apps/graviton_mq_runtime/test/amqp10/{connection_input_test,connection_test}.exs`;
create `apps/graviton_mq/test/graviton_mq/amqp10_tls_test.exs`

- [ ] **Step 0:** `git status --porcelain -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/listener.ex apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/connection.ex apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/writer.ex apps/graviton_mq_runtime/test/amqp10/connection_input_test.exs apps/graviton_mq_runtime/test/amqp10/connection_test.exs` prints nothing.
- [ ] **Step 1: Migrate the Connection-level TLS test** — in
`connection_input_test.exs`, add `Code.require_file("../support/tls.exs", __DIR__)`
above `defmodule`, plus `alias GravitonMQ.Runtime.Test.TLS` and
`@compile {:no_warn_undefined, :ssl}`. Delete `tls_listener!/0` and replace the
test "a TLS connection is driven by active-once input" with:

```elixir
  test "a TLS connection is driven by active-once input" do
    {socket, peer} = TLS.server_pair!(TLS.certificates!())
    on_exit(fn -> :ssl.close(peer) end)
    connection = start_supervised!({Connection, {config!([]), relay!(), :tls}}, id: make_ref())
    # The ex_ssl server socket is owned by this test process (its handshaker).
    :ok = SSL.controlling_process(socket, connection)
    send(connection, {:accepted, socket})
    :ok = :ssl.send(peer, @header)
    assert {:ok, @header} = :ssl.recv(peer, 8, 1_000)
    {:ok, <<size::32, _::binary>> = frame_header} = :ssl.recv(peer, 8, 1_000)
    {:ok, _} = :ssl.recv(peer, size - byte_size(frame_header), 1_000)

    :ok = :ssl.send(peer, wire(%Open{container_id: Value.string("tls-peer")}))

    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "tls-peer"}}},
                   1_000

    assert {:ok, [active: :once]} = SSL.getopts(data(connection).socket, [:active])
    assert idle_messages(connection, 200) == []

    monitor = Process.monitor(connection)
    :ssl.close(peer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, _}}}, 1_000
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 1_000
  end
```

(Here the `DOWN` comes after the connection has seen our own `close`, which is
causally after the monitor, so no sync call is needed.)

- [ ] **Step 2: Remove the Connection-level handshake test** — delete
"TLS handshake with a silent peer respects the finite connection timeout"
(`connection_test.exs:280-362`). Its behaviour moves to the listener level
(Step 3 and Task 9), because the Connection no longer handshakes. Record this
move in the execution log.
- [ ] **Step 3: Failing listener-level tests** — create `G/test/graviton_mq/amqp10_tls_test.exs`:

```elixir
Code.require_file("../../../graviton_mq_runtime/test/support/tls.exs", __DIR__)

defmodule GravitonMQ.AMQP10TLSTest.Auth do
  @moduledoc false
  @behaviour GravitonMQ.Runtime.AMQP10.Auth

  @impl true
  def authenticate(credentials, options) do
    send(Keyword.fetch!(options, :test), {:peer, credentials.peer})

    if credentials.username == "graviton-user" and credentials.password == "graviton-password",
      do: {:ok, {:user, "graviton-user"}},
      else: {:error, :denied}
  end
end

defmodule GravitonMQ.AMQP10TLSTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Codec.{Frame, Limits, Performative}
  alias GravitonMQ.AMQP10.Codec.SASL, as: SASLCodec
  alias GravitonMQ.AMQP10.Frame, as: Envelope
  alias GravitonMQ.AMQP10.Performative.Open
  alias GravitonMQ.AMQP10.SASL.{Init, Mechanisms, Outcome, Plain}
  alias GravitonMQ.AMQP10TLSTest.Auth
  alias GravitonMQ.Runtime.AMQP10.Listener
  alias GravitonMQ.Runtime.Test.TLS

  @compile {:no_warn_undefined, :ssl}
  @moduletag timeout: 30_000
  @amqp <<"AMQP", 0, 1, 0, 0>>
  @sasl <<"AMQP", 3, 1, 0, 0>>

  setup_all do
    %{certificates: TLS.certificates!()}
  end

  test "a verified TLS 1.3 client exchanges headers and Open; the server socket is TLS 1.3",
       %{certificates: certificates} do
    broker = broker!(certificates)
    socket = opened!(port(broker), certificates)
    assert {:ok, information} = SSL.connection_information(server_socket(connection!(broker)))
    assert information[:protocol] == :"tlsv1.3"
    assert :ok = :ssl.close(socket)
  end

  # Migrated from runtime/test/amqp10/connection_test.exs (spec §7.1).
  test "a silent TCP peer to a TLS listener is closed within connect_timeout and starts no Connection",
       %{certificates: certificates} do
    broker = broker!(certificates, connect_timeout: 300)
    {:ok, raw} = :gen_tcp.connect({127, 0, 0, 1}, port(broker), [:binary, active: false], 1_000)
    started = System.monotonic_time(:millisecond)
    assert {:error, :timeout} = :gen_tcp.recv(raw, 0, 150)
    assert %{active: 0} = DynamicSupervisor.count_children(connections(broker))
    assert :closed = closed(raw, started + 2_000)
    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed >= 250 and elapsed < 1_500, "closed after #{elapsed} ms"
    assert %{active: 0} = DynamicSupervisor.count_children(connections(broker))
  end

  # Helpers (used by all AMQP TLS tests).

  defp broker!(certificates, options \\ [], tls \\ []) do
    config =
      Keyword.merge(
        [
          container_id: "tls-#{System.unique_integer([:positive])}",
          auth: :anonymous,
          queues: ["q"],
          endpoint_options: [max_frame_size: 512],
          listeners: [tls_listener(certificates, tls)]
        ],
        options
      )

    start_supervised!({GravitonMQ, name: nil, runtime_supervisor_name: nil, amqp10: config},
      id: make_ref()
    )
  end

  defp tls_listener(certificates, tls \\ [], id \\ :tls, port \\ 0),
    do: [
      id: id,
      transport: :tls,
      ip: {127, 0, 0, 1},
      port: port,
      tls_options: TLS.listener_options(certificates, tls)
    ]

  defp port(broker, id \\ :tls) do
    assert {:ok, %{listeners: listeners}} = GravitonMQ.amqp10_status(broker)
    %{port: port} = Enum.find(listeners, &(&1.id == id))
    port
  end

  # Header exchange and the broker's Open, then the peer's Open.
  defp opened!(port, certificates, options \\ []) do
    assert {:ok, socket} = TLS.connect(port, certificates, options)
    on_exit(fn -> :ssl.close(socket) end)
    :ok = :ssl.send(socket, @amqp)
    assert {:ok, @amqp} = :ssl.recv(socket, 8, 2_000)
    assert %Open{} = read_pdu!(socket)
    :ok = :ssl.send(socket, wire(%Open{container_id: Value.string("tls-peer")}))
    socket
  end

  defp plain!(socket) do
    :ok = :ssl.send(socket, @sasl)
    assert {:ok, @sasl} = :ssl.recv(socket, 8, 2_000)
    assert %Mechanisms{} = read_sasl!(socket)
    {:ok, message} = Plain.encode(%Plain{username: "graviton-user", password: "graviton-password"})

    init = %Init{mechanism: Value.symbol("PLAIN"), initial_response: Value.binary(message)}
    {:ok, frame} = SASLCodec.encode_frame(init)
    :ok = :ssl.send(socket, frame)
    assert %Outcome{code: %Value{type: :ubyte, value: 0}} = read_sasl!(socket)
  end

  defp read_sasl!(socket) do
    {:ok, <<size::32, _::binary>> = header} = :ssl.recv(socket, 8, 2_000)
    {:ok, body} = :ssl.recv(socket, size - 8, 2_000)

    {:ok, %Envelope{type: :sasl} = frame, <<>>} =
      Frame.decode(header <> body, Limits.default(), types: [:sasl])

    {:ok, decoded} = SASLCodec.decode(frame.body)
    decoded
  end

  defp wire(pdu, channel \\ 0) do
    {:ok, body} = Performative.encode(pdu)

    {:ok, bytes} =
      Frame.encode(%Envelope{
        declared_size: 8 + byte_size(body),
        data_offset_words: 2,
        type: :amqp,
        channel: channel,
        extended_header: <<>>,
        body: body
      })

    bytes
  end

  defp read_pdu!(socket) do
    {:ok, <<size::32, _::binary>> = header} = :ssl.recv(socket, 8, 2_000)
    body = if size > 8, do: elem(:ssl.recv(socket, size - 8, 2_000), 1), else: <<>>
    {:ok, frame, <<>>} = Frame.decode(header <> body)
    {:ok, pdu, _payload} = Performative.decode(frame.body)
    pdu
  end

  defp amqp_supervisor(broker) do
    broker
    |> child(GravitonMQ.Runtime.Supervisor)
    |> child(GravitonMQ.Runtime.AMQP10.Supervisor)
  end

  defp connections(broker), do: child(amqp_supervisor(broker), :connections)

  defp connection!(broker) do
    supervisor = connections(broker)
    assert eventually(fn -> match?([_], DynamicSupervisor.which_children(supervisor)) end)
    [{_id, connection, _type, _modules}] = DynamicSupervisor.which_children(supervisor)
    connection
  end

  # The AMQP Connection keeps its state in a private ETS table; read it from
  # inside the Connection process.
  defp server_socket(connection) do
    test = self()
    reference = make_ref()

    :sys.replace_state(connection, fn shell ->
      send(test, {reference, :ets.lookup_element(shell.table, :state, 2).socket})
      shell
    end)

    receive do
      {^reference, socket} -> socket
    after
      1_000 -> flunk("no AMQP Connection state")
    end
  end

  defp listener!(broker, id), do: child(child(amqp_supervisor(broker), :listeners), {Listener, id})

  defp find_listener(broker, id) do
    case find_child(amqp_supervisor(broker), :listeners) do
      nil -> nil
      listeners -> find_child(listeners, {Listener, id})
    end
  end

  defp child(supervisor, id) do
    pid = find_child(supervisor, id)
    assert is_pid(pid)
    pid
  end

  defp find_child(supervisor, id) do
    case List.keyfind(Supervisor.which_children(supervisor), id, 0) do
      {^id, pid, _type, _modules} when is_pid(pid) -> pid
      _ -> nil
    end
  end

  defp monitor!(pid) do
    monitor = Process.monitor(pid)
    _ = :sys.get_state(pid)
    monitor
  end

  defp closed(raw, deadline) do
    case :gen_tcp.recv(raw, 0, max(deadline - System.monotonic_time(:millisecond), 0)) do
      {:ok, _bytes} -> closed(raw, deadline)
      {:error, reason} -> reason
    end
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp eventually(predicate, timeout \\ 2_000),
    do: eventually_until(predicate, System.monotonic_time(:millisecond) + timeout)

  defp eventually_until(predicate, deadline) do
    cond do
      predicate.() -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true -> Process.sleep(5) && eventually_until(predicate, deadline)
    end
  end
end
```

Run:

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/graviton_mq/amqp10_tls_test.exs
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/amqp10/connection_input_test.exs
```

Expected: FAIL. The round trip fails on `SSL.connection_information/1` against
an OTP socket. In the silent-peer test the OTP path starts a Connection, so the
count is `1`. The migrated input test fails because the Connection calls
`:ssl.handshake/2` on an `%SSL.Socket{}`.

- [ ] **Step 4: AMQP Listener** — the same transformation as Task 6 Step 3:
  - `init/1` keeps `Process.flag(:sensitive, true)` and the `rescue`; it calls
    `HellHandoff.start_link(listener, handoff_settings(config), handoff)` for
    both transports, stores `socket: server`, and computes `sasl.client_certificate_verified`
    as `listener.transport == :tls and TLSOptions.verify_peer?(listener.tls_options)`;
  - drop the transport guards from `handle_call(:connection_spec, ...)`, the
    `:tls` branch in `wire/2`, `info(:accept, ...)`, and the `:tls`
    `terminate/2`;
  - `connection_spec/1` child `{Connection, {state.config, state.broker, state.transport}}`.
- [ ] **Step 5: AMQP Connection**:
  - `info({:accepted, socket}, ...)`: remove the `case handshake(...)` wrapper
    and the `{:error, reason} -> ... {:handshake, ...}` branch. The body is now:

```elixir
  defp info({:accepted, socket}, %{socket: nil, writer: nil} = state) do
    Process.cancel_timer(state.accept_timer)
    state = %{state | accept_timer: nil, accept_token: nil, socket: socket}

    options = [
      owner: self(),
      generation: state.endpoint.generation,
      socket: socket,
      transport: state.transport,
      max_frames: state.endpoint.options.max_write_frames,
      max_bytes: state.endpoint.options.max_write_bytes
    ]

    with :ok <- configure_socket(state),
         {:ok, writer} <- Writer.start_link(options) do
      state = %{state | writer: writer, writer_handle: Writer.handle(writer)}
      # The broker writes nothing before the peer's protocol header.
      state |> sasl_step(:start) |> arm_socket() |> continue()
    else
      {:error, reason} ->
        continue(step(state, {:transport_down, {:transport, transport_reason(reason)}}))
    end
  end
```

  - delete `handshake/2`; replace `configure_socket/1` with

```elixir
  defp configure_socket(state),
    do:
      setopts(
        state.transport,
        state.socket,
        socket_options(state.transport, state.config.send_timeout)
      )

  defp socket_options(:tcp, send_timeout),
    do: [:binary, packet: :raw, active: false, send_timeout: send_timeout, send_timeout_close: true]

  # ex_ssl's carrier is always binary raw; its setopts rejects :binary/packet.
  defp socket_options(:tls, send_timeout),
    do: [active: false, send_timeout: send_timeout, send_timeout_close: true]
```

  - `setopts(:tls, socket, options), do: SSL.setopts(socket, options)`;
  - `peer/1`: `SSL.peername/1`, `SSL.peercert/1`; replace the comment
    "Broker TLS stays on OTP :ssl until S1b moves it to Hell.Socket." with
    "TLS peer facts come from ex_ssl; `peercert/1` only yields a chain-validated leaf.";
  - `close_socket/1`: `:tls -> SSL.close(state.socket)`;
  - keep the shared `[:tcp_passive, :ssl_passive]` clause (TCP still uses it);
    above it add `# ex_ssl sends no {:ssl_passive, _}; the :ssl_passive arm is unreachable for TLS.`
- [ ] **Step 6: AMQP Writer** (`amqp10/writer.ex:111`): `defp socket_send(:tls, socket, bytes), do: SSL.send(socket, bytes)`.
- [ ] **Step 7: Verify**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/amqp10 test/hell_handoff_test.exs
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/graviton_mq/amqp10_tls_test.exs test/graviton_mq/amqp10_runtime_test.exs test/graviton_mq/amqp10_sasl_test.exs test/graviton_mq/amqp10_boundaries_test.exs
grep -rn ":ssl\." /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime/lib
```

Expected: `0 failures`; the grep prints nothing. `amqp10_sasl_test.exs` includes
our AMQP client → broker over verified TLS. If it fails because of S3's client
work in progress, record it and do not edit S3 files.

- [ ] **Step 8: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
P=(apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/listener.ex apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/connection.ex apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/writer.ex apps/graviton_mq_runtime/test/amqp10/connection_input_test.exs apps/graviton_mq_runtime/test/amqp10/connection_test.exs apps/graviton_mq/test/graviton_mq/amqp10_tls_test.exs)
env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix format "${P[@]}"
git add -- "${P[@]}"
git commit --only -m "feat(amqp): serve broker TLS listeners through Hell and ex_ssl" -- "${P[@]}"
```

---

## Task 8: MQTT listener-level T2, T3, T4

**Files:** modify `apps/graviton_mq/test/graviton_mq/mqtt_tls_test.exs`

These are regression tests for behaviour Task 6 already delivers. Run each one
right after writing it. A pass is expected; any failure is a real defect to diagnose.

- [ ] **Step 1: Add the tests** (above the helper section)

```elixir
  # T2
  test "a TLS 1.2-only client is refused during the handshake and reaches no owner",
       %{certificates: certificates} do
    instance = broker(certificates, auth: {Policy, observer: self()})

    assert {:error, {:tls_alert, {:protocol_version, _description}}} =
             TLS.connect(port(instance), certificates, versions: [:"tlsv1.2"])

    refute_receive {:peer, _client_id, _peer}, 100
    assert %{active: 0} = DynamicSupervisor.count_children(connections(instance))
    assert %{connections: 0, sessions: 0} = status(instance)
  end

  # T3: optional client certificate under verify_peer
  test "optional client certificates give exact peer facts and agree with ex_ssl",
       %{certificates: certificates} do
    instance =
      broker(certificates, [auth: {Policy, observer: self()}],
        cacertfile: certificates.ca,
        verify: :verify_peer,
        fail_if_no_peer_cert: false
      )

    port = port(instance)
    anonymous = tls_connected(port, certificates, "no-certificate")

    assert_receive {:peer, "no-certificate",
                    %{certificate: nil, certificate_verified: false, transport: :tls}}

    assert {:ok, information} = SSL.connection_information(server_socket(connection!(instance)))
    assert information[:peer_authenticated] == false
    :ok = :ssl.close(anonymous)
    assert eventually(fn -> status(instance).connections == 0 end)

    _identified = tls_connected(port, certificates, "with-certificate", TLS.client_identity(certificates))
    client_der = certificates.client_der

    assert_receive {:peer, "with-certificate",
                    %{certificate: ^client_der, certificate_verified: true, transport: :tls}}

    assert {:ok, information} = SSL.connection_information(server_socket(connection!(instance)))
    assert information[:peer_authenticated] == :client

    refused!(TLS.connect(port, certificates, TLS.stranger_identity(certificates)))
    refute_receive {:peer, "refused", _peer}, 200
  end

  # T3: required client certificate
  test "a required client certificate refuses an anonymous client and admits a trusted one",
       %{certificates: certificates} do
    instance =
      broker(certificates, [auth: {Policy, observer: self()}],
        cacertfile: certificates.ca,
        verify: :verify_peer,
        fail_if_no_peer_cert: true
      )

    port = port(instance)
    refused!(TLS.connect(port, certificates))
    refute_receive {:peer, "refused", _peer}, 200

    _identified = tls_connected(port, certificates, "required", TLS.client_identity(certificates))
    client_der = certificates.client_der
    assert_receive {:peer, "required", %{certificate: ^client_der, certificate_verified: true}}
  end

  # T3: verify_none requests no certificate
  test "a listener without verify_peer requests no client certificate",
       %{certificates: certificates} do
    instance = broker(certificates, auth: {Policy, observer: self()})
    _socket = tls_connected(port(instance), certificates, "not-requested", TLS.client_identity(certificates))

    assert_receive {:peer, "not-requested",
                    %{certificate: nil, certificate_verified: false, transport: :tls}}
  end

  # T4 (listener level; replaces the old Connection-level handshake test)
  test "silent and stalled TLS peers are closed within connect_timeout before any Connection",
       %{certificates: certificates} do
    instance = broker(certificates, auth: {Policy, observer: self()}, connect_timeout: 300)

    for prefix <- [<<>>, <<22, 3, 1, 0, 64, 1, 0, 0, 60>>] do
      {:ok, raw} = :gen_tcp.connect({127, 0, 0, 1}, port(instance), [:binary, active: false], 1_000)
      started = System.monotonic_time(:millisecond)
      :ok = :gen_tcp.send(raw, prefix)
      assert {:error, :timeout} = :gen_tcp.recv(raw, 0, 150)
      assert %{active: 0} = DynamicSupervisor.count_children(connections(instance))
      assert %{connections: 0} = status(instance)
      assert :closed = closed(raw, started + 2_000)
      elapsed = System.monotonic_time(:millisecond) - started
      assert elapsed >= 250 and elapsed < 1_500, "closed after #{elapsed} ms"
    end

    refute_received {:peer, _client_id, _peer}
  end
```

Add these helpers:

```elixir
  # TLS 1.3: the client may finish before the server rejects its certificate;
  # then the server's alert (or close) is the first read result.
  defp refused!({:error, _reason}), do: :ok

  defp refused!({:ok, socket}) do
    _ = :ssl.send(socket, connect_packet("refused"))
    assert {:error, _reason} = :ssl.recv(socket, 0, 2_000)
  end

  defp closed(raw, deadline) do
    case :gen_tcp.recv(raw, 0, max(deadline - System.monotonic_time(:millisecond), 0)) do
      {:ok, _bytes} -> closed(raw, deadline)
      {:error, reason} -> reason
    end
  end
```

- [ ] **Step 2: Run**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/graviton_mq/mqtt_tls_test.exs
```

Expected: `0 failures`. If the TLS 1.2 refusal surfaces as a different OTP alert
shape, record the observed term. Assert that exact term (still requiring
`:protocol_version`) and note it in the log.

- [ ] **Step 3: Format and commit** — path `apps/graviton_mq/test/graviton_mq/mqtt_tls_test.exs`, message `test(mqtt): cover TLS 1.3-only refusal, mTLS facts and handshake budget`.

---

## Task 9: AMQP listener-level T2, T3, T4

**Files:** modify `apps/graviton_mq/test/graviton_mq/amqp10_tls_test.exs`

- [ ] **Step 1: Add the tests**

```elixir
  # T2
  test "a TLS 1.2-only client is refused during the handshake and starts no Connection",
       %{certificates: certificates} do
    broker = broker!(certificates)

    assert {:error, {:tls_alert, {:protocol_version, _description}}} =
             TLS.connect(port(broker), certificates, versions: [:"tlsv1.2"])

    assert %{active: 0} = DynamicSupervisor.count_children(connections(broker))
  end

  # T3 through SASL PLAIN over TLS: the Auth callback sees peer/1 facts.
  test "optional client certificates give exact SASL peer facts and agree with ex_ssl",
       %{certificates: certificates} do
    broker =
      broker!(certificates, [auth: {Auth, [test: self()]}],
        cacertfile: certificates.ca,
        verify: :verify_peer,
        fail_if_no_peer_cert: false
      )

    port = port(broker)
    {:ok, anonymous} = TLS.connect(port, certificates)
    plain!(anonymous)
    assert_receive {:peer, %{certificate: nil, certificate_verified: false, transport: :tls}}
    assert {:ok, information} = SSL.connection_information(server_socket(connection!(broker)))
    assert information[:peer_authenticated] == false
    :ok = :ssl.close(anonymous)
    assert eventually(fn -> DynamicSupervisor.count_children(connections(broker)).active == 0 end)

    {:ok, identified} = TLS.connect(port, certificates, TLS.client_identity(certificates))
    plain!(identified)
    client_der = certificates.client_der
    assert_receive {:peer, %{certificate: ^client_der, certificate_verified: true}}
    assert {:ok, information} = SSL.connection_information(server_socket(connection!(broker)))
    assert information[:peer_authenticated] == :client

    case TLS.connect(port, certificates, TLS.stranger_identity(certificates)) do
      {:error, _reason} -> :ok
      {:ok, stranger} -> assert {:error, _reason} = :ssl.recv(stranger, 0, 2_000)
    end

    refute_receive {:peer, _peer}, 200
  end

  # T4: stalled mid-handshake (the silent case is the migrated test above)
  test "a peer stalled inside its ClientHello is closed within connect_timeout",
       %{certificates: certificates} do
    broker = broker!(certificates, connect_timeout: 300)
    {:ok, raw} = :gen_tcp.connect({127, 0, 0, 1}, port(broker), [:binary, active: false], 1_000)
    started = System.monotonic_time(:millisecond)
    :ok = :gen_tcp.send(raw, <<22, 3, 1, 0, 64, 1, 0, 0, 60>>)
    assert :closed = closed(raw, started + 2_000)
    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed >= 250 and elapsed < 1_500, "closed after #{elapsed} ms"
    assert %{active: 0} = DynamicSupervisor.count_children(connections(broker))
  end
```

- [ ] **Step 2: Run** `.../apps/graviton_mq && env ... mix test test/graviton_mq/amqp10_tls_test.exs`. Expected: `0 failures`.
- [ ] **Step 3: Format and commit** — message `test(amqp): cover TLS 1.3-only refusal, mTLS SASL peer facts and handshake budget`.

---

## Task 10: T5 — #161 regression at product level

**Files:** modify `apps/graviton_mq/test/graviton_mq/mqtt_tls_test.exs`,
`apps/graviton_mq_runtime/test/amqp10/connection_input_test.exs` (shared-file protocol)

The peer uses a small receive buffer (`recbuf: 8_192`) and never reads, so the
broker Writer blocks inside `SSL.send/2`. The tests keep producing output until
the Writer is observed blocked: its stack contains an `SSL` frame
(`SSL.call/2` is not a tail call).

- [ ] **Step 1: MQTT tests**

```elixir
  # T5
  test "a Writer blocked by a non-reading TLS peer does not stop the peer's DISCONNECT",
       %{certificates: certificates} do
    instance =
      broker(certificates,
        send_timeout: 3_000,
        max_writer_bytes: 8_388_608,
        max_pending_bytes: 16_777_216
      )

    %{owner: owner} = status(instance)
    socket = tls_connected(port(instance), certificates, "slow-reader", recbuf: 8_192)
    subscribe!(socket)
    connection = connection!(instance)
    writer = :sys.get_state(connection).writer
    monitor = monitor!(connection)
    assert fill_until_blocked(instance, owner, writer)

    sent = System.monotonic_time(:millisecond)
    :ok = :ssl.send(socket, <<0xE0, 0>>)
    assert_receive {:DOWN, ^monitor, :process, ^connection, _reason}, 2_000
    assert System.monotonic_time(:millisecond) - sent < 2_000
    assert eventually(fn -> status(instance).connections == 0 end)
  end

  test "a Writer blocked by a non-reading TLS peer closes by send_timeout and never hangs",
       %{certificates: certificates} do
    instance =
      broker(certificates,
        send_timeout: 1_000,
        max_writer_bytes: 8_388_608,
        max_pending_bytes: 16_777_216
      )

    %{owner: owner} = status(instance)
    socket = tls_connected(port(instance), certificates, "stuck-reader", recbuf: 8_192)
    subscribe!(socket)
    connection = connection!(instance)
    writer = :sys.get_state(connection).writer
    monitor = monitor!(connection)
    assert fill_until_blocked(instance, owner, writer)
    assert_receive {:DOWN, ^monitor, :process, ^connection, _reason}, 4_000
    assert eventually(fn -> status(instance).connections == 0 end)
  end
```

Helpers:

```elixir
  defp subscribe!(socket) do
    # SUBSCRIBE id 1, no properties, "load/#" at QoS 0.
    :ok = :ssl.send(socket, <<0x82, 12, 0, 1, 0, 0, 6, "load/#", 0>>)
    assert %Suback{packet_id: 1, reason_codes: [0]} = receive_packet(socket)
  end

  # Publishes 64 KiB host messages until the Writer is blocked in SSL.send.
  defp fill_until_blocked(instance, owner, writer, remaining \\ 400) do
    cond do
      blocked_in_tls_send?(writer) ->
        true

      remaining == 0 ->
        false

      true ->
        {:ok, reference} = GravitonMQ.publish(instance, "load/x", :binary.copy(<<1>>, 65_536), qos: 0)
        assert_receive {:graviton_mq, ^owner, {:operation, ^reference, _result}}, 2_000
        fill_until_blocked(instance, owner, writer, remaining - 1)
    end
  end

  defp blocked_in_tls_send?(writer) do
    case Process.info(writer, :current_stacktrace) do
      {:current_stacktrace, stack} -> Enum.any?(stack, &match?({SSL, _, _, _}, &1))
      nil -> false
    end
  end
```

If `max_pending_bytes` or `max_writer_bytes` is rejected by `Config.build`,
read the bounds in `mqtt/config.ex` and use the largest accepted values. Record
the change; do not remove the blocked-Writer precondition.

- [ ] **Step 2: AMQP tests** (`connection_input_test.exs`, after the migrated TLS test)

```elixir
  # T5
  test "a TLS Writer blocked by a non-reading peer does not stop the peer's Close" do
    {connection, peer} = tls_connected!(send_timeout: 3_000)
    monitor = monitor!(connection)
    %{writer: writer, writer_handle: handle} = data(connection)
    assert fill_until_blocked(handle, writer)

    sent = System.monotonic_time(:millisecond)
    :ok = :ssl.send(peer, wire(%Close{}))
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, _}}}, 2_000
    assert System.monotonic_time(:millisecond) - sent < 2_000
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 4_000
  end

  test "a TLS Writer blocked by a non-reading peer closes by send_timeout" do
    {connection, _peer} = tls_connected!(send_timeout: 1_000)
    monitor = monitor!(connection)
    %{writer: writer, writer_handle: handle} = data(connection)
    assert fill_until_blocked(handle, writer)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, _}}}, 4_000
    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 4_000
  end
```

Helpers (add `Close` to the `GravitonMQ.AMQP10.Performative` alias and
`alias GravitonMQ.Runtime.AMQP10.Writer`):

```elixir
  # An ex_ssl server socket handed to a Connection; header and Open exchanged.
  defp tls_connected!(options) do
    {socket, peer} = TLS.server_pair!(TLS.certificates!(), recbuf: 8_192)
    on_exit(fn -> :ssl.close(peer) end)
    connection = start_supervised!({Connection, {config!(options), relay!(), :tls}}, id: make_ref())
    :ok = SSL.controlling_process(socket, connection)
    send(connection, {:accepted, socket})
    :ok = :ssl.send(peer, @header)
    assert {:ok, @header} = :ssl.recv(peer, 8, 1_000)
    {:ok, <<size::32, _::binary>> = frame_header} = :ssl.recv(peer, 8, 1_000)
    {:ok, _} = :ssl.recv(peer, size - byte_size(frame_header), 1_000)
    :ok = :ssl.send(peer, wire(%Open{container_id: Value.string("tls-peer")}))
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "tls-peer"}}}, 1_000
    {connection, peer}
  end

  # Unknown write ids are ignored by the Endpoint (write_result {nil, _}).
  defp fill_until_blocked(handle, writer, remaining \\ 2_000) do
    cond do
      blocked_in_tls_send?(writer) ->
        true

      remaining == 0 ->
        false

      true ->
        _ = Writer.enqueue(handle, {:t5_fill, remaining}, :binary.copy(<<0>>, 65_536))
        Process.sleep(2)
        fill_until_blocked(handle, writer, remaining - 1)
    end
  end

  defp blocked_in_tls_send?(writer) do
    case Process.info(writer, :current_stacktrace) do
      {:current_stacktrace, stack} -> Enum.any?(stack, &match?({SSL, _, _, _}, &1))
      nil -> false
    end
  end

  defp monitor!(pid) do
    monitor = Process.monitor(pid)
    _ = :sys.get_state(pid)
    monitor
  end
```

`TLS.server_pair!/2` passes `client_options` to `TLS.connect/3`, so `recbuf`
reaches the OTP client. If `connection_input_test.exs` already defines
`monitor!/1`, reuse it.

- [ ] **Step 3: Run both files.** Expected: `0 failures`. A failure in the
first test of either pair (DOWN only after `send_timeout`) is a #161 regression
in ex_ssl. In that case stop, load the `upstream-issue-routing` skill and report
it (it is a blocker); do not work around it.
- [ ] **Step 4: Format and commit** — paths: the two test files; message `test(runtime): prove TLS input survives a blocked Writer (http_fetch#161)`.

---

## Task 11: T6 — the #169 gate (expected FAIL on rc.14)

**Files:** modify `apps/graviton_mq_runtime/test/test_helper.exs`,
`apps/graviton_mq/test/test_helper.exs` (shared-file protocol),
`apps/graviton_mq/test/graviton_mq/mqtt_tls_test.exs`,
`apps/graviton_mq_runtime/test/amqp10/connection_input_test.exs` (shared-file protocol)

- [ ] **Step 1: Exclusions.** Runtime `test_helper.exs` becomes:

```elixir
# Under host load, asynchronous messages (such as a crash's DOWN) can exceed
# ExUnit's 100 ms default. This default applies to every assert_receive here;
# a missing message still fails, and refute_receive keeps its own default.
#
# :requires_http_fetch_169 — S1b T6 pipelining-while-slow regression. It fails
# until ex_ssl bounds active-once delivery (gsmlg-dev/http_fetch#169) and the
# pin is updated; run it with `mix test --include requires_http_fetch_169`.
ExUnit.start(assert_receive_timeout: 2_000, exclude: [:requires_http_fetch_169])
```

In `G/test/test_helper.exs`, prepend the tag to the exclude list and add the
same comment:

```elixir
ExUnit.start(
  exclude:
    [:requires_http_fetch_169] ++
      if(System.get_env("GRAVITON_MOSQUITTO_BIN"), do: [], else: [:interop]) ++
      if(System.get_env("GRAVITON_PAHO_PYTHON"), do: [], else: [:paho_interop]) ++
      if(System.get_env("GRAVITON_PROTON_PYTHON"), do: [], else: [:proton_interop])
)
```

(If S3 has meanwhile changed this file in a committed form, keep its entries
and only prepend the tag.)

- [ ] **Step 2: MQTT T6**

```elixir
  # T6 (spec §4.3, option B). Expected to FAIL until gsmlg-dev/http_fetch#169:
  # ex_ssl coalesces ~960 KiB into one active-once message while the
  # Connection is suspended, exceeding max_packet_size + 16 KiB (20_480 here).
  @tag :requires_http_fetch_169
  test "more than 1 MiB of small packets pipelined to a suspended TLS Connection are all processed in order",
       %{certificates: certificates} do
    instance = broker(certificates, max_packet_size: 4_096, max_writer_packets: 100_000)
    socket = tls_connected(port(instance), certificates, "pipeline")
    connection = connection!(instance)
    monitor = monitor!(connection)
    count = 12_000
    filter = "pipeline/" <> String.duplicate("f", 81)
    # UNSUBSCRIBE of an absent filter: 97 bytes, answered by UNSUBACK 0x11.
    packets = for id <- 1..count, into: <<>>, do: <<0xA2, 95, id::16, 0, 90::16, filter::binary>>
    assert byte_size(packets) > 1_048_576

    :ok = :sys.suspend(connection)

    sender =
      try do
        sender = Task.async(fn -> :ssl.send(socket, [packets, <<0xC0, 0>>]) end)
        # Let ex_ssl read ahead while the Connection cannot arm.
        Process.sleep(300)
        sender
      after
        :ok = :sys.resume(connection)
      end

    deadline = System.monotonic_time(:millisecond) + 15_000
    {acks, rest} = receive_packets(socket, count, <<>>, deadline)
    assert Enum.map(acks, &match?(%Unsuback{}, &1)) |> Enum.all?()
    assert Enum.map(acks, & &1.packet_id) == Enum.to_list(1..count)
    {[pingresp], <<>>} = receive_packets(socket, 1, rest, deadline)
    assert pingresp.__struct__ == GravitonMQ.MQTT.Packet.Pingresp
    assert :ok = Task.await(sender, 15_000)
    refute_received {:DOWN, ^monitor, :process, ^connection, _reason}

    # The per-packet limit still applies.
    :ok = :ssl.send(socket, <<0x30, 0xFF, 0x7F>>)
    assert %Disconnect{reason_code: 0x95} = receive_packet(socket)
  end
```

- [ ] **Step 3: AMQP T6** (in `connection_input_test.exs`)

```elixir
  # T6 (spec §4.3, option B). Expected to FAIL until gsmlg-dev/http_fetch#169.
  @tag :requires_http_fetch_169
  test "more than 1 MiB of frames pipelined to a suspended TLS Connection are processed without an input-limit close" do
    {socket, peer} = TLS.server_pair!(TLS.certificates!())
    on_exit(fn -> :ssl.close(peer) end)
    config = config!(max_input_bytes: 65_536)
    connection = start_supervised!({Connection, {config, relay!(), :tls}}, id: make_ref())
    :ok = SSL.controlling_process(socket, connection)
    send(connection, {:accepted, socket})
    :ok = :ssl.send(peer, @header)
    assert {:ok, @header} = :ssl.recv(peer, 8, 1_000)
    {:ok, <<size::32, _::binary>> = frame_header} = :ssl.recv(peer, 8, 1_000)
    {:ok, _} = :ssl.recv(peer, size - byte_size(frame_header), 1_000)
    heartbeats = :binary.copy(heartbeat(), 140_000)
    assert byte_size(heartbeats) > 1_048_576

    :ok = :sys.suspend(connection)

    sender =
      try do
        sender =
          Task.async(fn ->
            :ssl.send(peer, [heartbeats, wire(%Open{container_id: Value.string("pipelined")})])
          end)

        Process.sleep(300)
        sender
      after
        :ok = :sys.resume(connection)
      end

    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:connected, "pipelined"}}},
                   15_000

    assert :ok = Task.await(sender, 15_000)
    refute_received {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, _}}}

    # An oversize frame is still rejected.
    :ok = :ssl.send(peer, <<1_000_000::32, 2, 0, 0::16>>)
    assert_receive {:amqp10_endpoint_event, ^connection, {:event, {:disconnected, _}}}, 2_000
  end
```

- [ ] **Step 4: Prove the exclusion, then record the expected failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/amqp10/connection_input_test.exs
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/graviton_mq/mqtt_tls_test.exs
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test --include requires_http_fetch_169 test/amqp10/connection_input_test.exs
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test --include requires_http_fetch_169 test/graviton_mq/mqtt_tls_test.exs
```

Expected: the first two runs report `0 failures` with `1 excluded`. The two
`--include` runs FAIL in exactly the T6 test. The MQTT failure shows a
`%Disconnect{}` (reason `0x95`/protocol error after `:parser_limit`) in place of
UNSUBACKs. The AMQP failure shows `{:disconnected, {:decode_error, :input_buffer_limit}}`.
Copy both failure summaries into the execution log as the #169 baseline.
**If T6 passes on rc.14,** the coalescing did not happen in this run: increase
the suspension to 1 s once and rerun. If it still passes, record it and report
to the coordinator (the spec §4.3 finding would need re-verification). Do not
remove the tag.

- [ ] **Step 5: Format and commit** — paths: both test helpers and both test files; message `test(runtime): add the http_fetch#169 coalesced TLS input gate (excluded)`.

---

## Task 12: T7, T8, T9 (listener failure, rebind, privacy)

**Files:** modify `apps/graviton_mq/test/graviton_mq/mqtt_tls_test.exs`,
`apps/graviton_mq/test/graviton_mq/amqp10_tls_test.exs`

- [ ] **Step 1: MQTT T7/T8/T9**

```elixir
  # T7
  test "a TLS listener failure closes its own connections; the TCP sibling and owner state survive",
       %{certificates: certificates} do
    instance =
      broker(certificates,
        listeners: [
          tls_listener(certificates),
          [id: :tcp, transport: :tcp, ip: {127, 0, 0, 1}, port: 0]
        ]
      )

    %{owner: owner} = status(instance)
    old_port = port(instance, :tls)
    tls = tls_connected(old_port, certificates, "tls-casualty")
    tcp = tcp_connected(port(instance, :tcp), "tcp-survivor")
    listener = listener!(instance, :tls)
    listener_monitor = monitor!(listener)

    capture_log(fn ->
      Process.exit(listener, :kill)
      assert_receive {:DOWN, ^listener_monitor, :process, ^listener, :killed}, 1_000
      assert {:error, _closed} = :ssl.recv(tls, 0, 2_000)

      assert eventually(fn ->
               replacement = find_listener(instance, :tls)
               is_pid(replacement) and replacement != listener
             end)

      :ok = :gen_tcp.send(tcp, <<0xC0, 0>>)
      assert {:ok, <<0xD0, 0>>} = :gen_tcp.recv(tcp, 2, 1_000)
      assert eventually(fn -> match?(%{owner: ^owner, connections: 1}, status(instance)) end)

      replacement = tls_connected(port(instance, :tls), certificates, "tls-after")
      assert_ping(replacement)
    end)
  end

  # T8
  test "a TLS listener on a fixed port rebinds the same port after repeated kills",
       %{certificates: certificates} do
    port = free_port()
    instance = broker(certificates, listeners: [tls_listener(certificates, [], :tls, port)])
    %{owner: owner} = status(instance)

    capture_log(fn ->
      for round <- 1..3 do
        listener = listener!(instance, :tls)
        listener_monitor = monitor!(listener)
        Process.exit(listener, :kill)
        assert_receive {:DOWN, ^listener_monitor, :process, ^listener, :killed}, 1_000

        assert eventually(fn ->
                 replacement = find_listener(instance, :tls)
                 is_pid(replacement) and replacement != listener
               end)

        assert %{owner: ^owner} = status(instance)
        assert port(instance) == port
        assert_ping(tls_connected(port, certificates, "fixed-port-#{round}"))
      end
    end)
  end

  # T9 (D10). WORKAROUND(upstream): gsmlg-dev/http_fetch#168 — password closure.
  test "the key password never reaches Hell telemetry, listener state, status or logs",
       %{certificates: certificates} do
    password = TLS.password()
    handler = "s1b-privacy-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach_many(
        handler,
        [[:hell, :listener, :start], [:hell, :listener, :stop]],
        &__MODULE__.forward_telemetry/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    log =
      capture_log(fn ->
        instance =
          broker(certificates, [], keyfile: certificates.encrypted_key, password: password)

        assert_ping(tls_connected(port(instance), certificates, "privacy"))
        listener = listener!(instance, :tls)
        server = :sys.get_state(listener).socket
        hell_listener = child(server, :listener)

        for term <- [
              :sys.get_status(listener),
              :sys.get_state(hell_listener),
              :sys.get_status(hell_listener),
              Supervisor.which_children(server)
            ] do
          refute leaks?(term, password)
          refute leaks?(term, certificates.key_der)
        end

        listener_monitor = monitor!(listener)
        Process.exit(listener, :kill)
        assert_receive {:DOWN, ^listener_monitor, :process, ^listener, :killed}, 1_000
        assert eventually(fn -> is_pid(find_listener(instance, :tls)) end)
      end)

    refute log =~ password
    events = telemetry_events([])
    assert Enum.any?(events, &match?({[:hell, :listener, :start], _}, &1))
    for {_event, metadata} <- events, do: refute(leaks?(metadata, password))

    # WORKAROUND(upstream): gsmlg-dev/http_fetch#168
    assert {:error, {:unsupported_tls_option, :key}} =
             GravitonMQ.Runtime.Supervisor.start_link(
               name: nil,
               mqtt: [
                 auth: :anonymous,
                 listeners: [
                   [
                     transport: :tls,
                     ip: {127, 0, 0, 1},
                     port: 0,
                     tls_options: [
                       certfile: certificates.certificate,
                       key: {:PrivateKeyInfo, certificates.key_der}
                     ]
                   ]
                 ]
               ]
             )
  end

  def forward_telemetry(event, _measurements, metadata, test),
    do: send(test, {:telemetry, event, metadata})

  defp telemetry_events(events) do
    receive do
      {:telemetry, event, metadata} -> telemetry_events([{event, metadata} | events])
    after
      0 -> events
    end
  end

  defp leaks?(term, secret) do
    printed = inspect(term, limit: :infinity, printable_limit: :infinity)
    String.contains?(printed, secret) or String.contains?(printed, inspect(secret, limit: :infinity))
  end
```

`child(server, :listener)` assumes the Hell server supervisor names its
listener child `:listener` (the rebind error `{:failed_to_start_child, :listener, _}`
shows that it does). Telemetry events from other async tests' listeners are
checked too, which is stricter and harmless.

- [ ] **Step 2: AMQP T7/T8/T9** (in `amqp10_tls_test.exs`)

```elixir
  # T7
  test "a TLS listener failure closes its own connections; the TCP sibling and queues survive",
       %{certificates: certificates} do
    broker =
      broker!(certificates,
        listeners: [tls_listener(certificates), [id: :tcp, transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
      )

    tls = opened!(port(broker), certificates)
    {:ok, tcp} = :gen_tcp.connect({127, 0, 0, 1}, port(broker, :tcp), [:binary, active: false], 1_000)
    on_exit(fn -> :gen_tcp.close(tcp) end)
    :ok = :gen_tcp.send(tcp, @amqp)
    assert {:ok, @amqp} = :gen_tcp.recv(tcp, 8, 1_000)
    listener = listener!(broker, :tls)
    listener_monitor = monitor!(listener)

    capture_log(fn ->
      Process.exit(listener, :kill)
      assert_receive {:DOWN, ^listener_monitor, :process, ^listener, :killed}, 1_000
      assert {:error, _closed} = :ssl.recv(tls, 0, 2_000)

      assert eventually(fn ->
               replacement = find_listener(broker, :tls)
               is_pid(replacement) and replacement != listener
             end)

      # The TCP sibling's connection is still open (no close, no EOF).
      assert {:ok, _broker_open} = :gen_tcp.recv(tcp, 0, 1_000)
      assert {:error, :timeout} = :gen_tcp.recv(tcp, 0, 100)
      assert {:ok, %{queues: %{"q" => _}}} = GravitonMQ.amqp10_status(broker)
      opened!(port(broker), certificates)
    end)
  end

  # T8
  test "a TLS listener on a fixed port rebinds the same port after repeated kills",
       %{certificates: certificates} do
    port = free_port()
    broker = broker!(certificates, listeners: [tls_listener(certificates, [], :tls, port)])

    capture_log(fn ->
      for _round <- 1..3 do
        listener = listener!(broker, :tls)
        listener_monitor = monitor!(listener)
        Process.exit(listener, :kill)
        assert_receive {:DOWN, ^listener_monitor, :process, ^listener, :killed}, 1_000

        assert eventually(fn ->
                 replacement = find_listener(broker, :tls)
                 is_pid(replacement) and replacement != listener
               end)

        assert port(broker) == port
        opened!(port, certificates)
      end
    end)
  end

  # T9 (AMQP config path)
  test "an in-memory key is refused before any socket opens and errors carry no values",
       %{certificates: certificates} do
    # WORKAROUND(upstream): gsmlg-dev/http_fetch#168
    assert {:error, {:unsupported_tls_option, :key}} =
             GravitonMQ.Runtime.Supervisor.start_link(
               name: nil,
               amqp10: [
                 container_id: "key-refused",
                 auth: :anonymous,
                 queues: ["q"],
                 listeners: [
                   [
                     transport: :tls,
                     tls_options: [
                       certfile: certificates.certificate,
                       key: {:PrivateKeyInfo, certificates.key_der}
                     ]
                   ]
                 ]
               ]
             )

    assert {:error, {:invalid_tls_option, :password}} =
             GravitonMQ.Runtime.Supervisor.start_link(
               name: nil,
               amqp10: [
                 container_id: "password-refused",
                 auth: :anonymous,
                 queues: ["q"],
                 listeners: [[transport: :tls, tls_options: TLS.listener_options(certificates, password: 4_242)]]
               ]
             )

    broker = broker!(certificates, [], keyfile: certificates.encrypted_key, password: TLS.password())
    _socket = opened!(port(broker), certificates)
    listener = listener!(broker, :tls)
    refute inspect(:sys.get_status(listener), limit: :infinity) =~ TLS.password()
  end
```

- [ ] **Step 3: Run both files.** Expected: `0 failures`.
- [ ] **Step 4: Format and commit** — message `test(runtime): cover TLS listener failure, rebind and secret privacy`.

---

## Task 13: Architecture rule and runtime application list (spec §4.11, Q3)

**Files:** `apps/graviton_mq/lib/graviton_mq/architecture.ex:77-82`,
`apps/graviton_mq/test/graviton_mq/architecture_test.exs:657-672`,
`apps/graviton_mq_runtime/mix.exs:22`,
`apps/graviton_mq/test/graviton_mq/release_test.exs:95`

- [ ] **Step 1: Failing tests.** Replace the test "client may not reference OTP :ssl; runtime still may" with:

```elixir
  test "neither the client nor the runtime may reference OTP :ssl" do
    for {app, module} <- [
          {:graviton_mq_client, GravitonMQ.Client.Transport},
          {:graviton_mq_runtime, GravitonMQ.Runtime.MQTT.Listener}
        ] do
      assert {:error, [{:forbidden_otp_transport_dependency, %{target_module: :ssl}} = violation]} =
               Architecture.check([ref(app, module, nil, :ssl)])

      assert Architecture.format_violation(violation) =~
               "#{app} may not reference :ssl (use the owned SSL package)"
    end

    assert :ok =
             Architecture.check([
               ref(:graviton_mq_runtime, GravitonMQ.Runtime.MQTT.Listener, nil, SSL),
               ref(:graviton_mq_runtime, GravitonMQ.Runtime.HellHandoff, nil, Hell.Transports.SSL),
               ref(:graviton_mq_runtime, GravitonMQ.Runtime.MQTT.Broker, nil, :crypto),
               ref(:graviton_mq_client, GravitonMQ.Client.Transport, nil, SSL),
               ref(:graviton_mq_client, GravitonMQ.Client.Transport, nil, :gen_tcp)
             ])
  end
```

In `release_test.exs:95` change the expectation to
`:graviton_mq_runtime -> [extra_applications: [:crypto]]`.

- [ ] **Step 2:** `cd .../apps/graviton_mq && env ... mix test test/graviton_mq/architecture_test.exs test/graviton_mq/release_test.exs`. Expected: two failures (the runtime `:ssl` reference is allowed; the application list is `[:ssl]`).
- [ ] **Step 3: Implement**
  - `architecture.ex`: `@forbidden_otp_transport_by_app %{graviton_mq_client: [:ssl], graviton_mq_runtime: [:ssl]}`
    and the comment above it: "OTP modules an application must not reference
    even though other applications may: the runtime and the independent client
    use the owned `SSL` package (S1b). `release_test` pins both application
    lists, so `extra_applications: [:ssl]` cannot silently return.";
  - `apps/graviton_mq_runtime/mix.exs`: `def application, do: [extra_applications: [:crypto]]`
    (the runtime calls `:crypto` directly in `mqtt/broker.ex` and the AMQP
    queue/broker/persistence; ex_ssl brings `:public_key` itself).
- [ ] **Step 4: Verify**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix compile --warnings-as-errors
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix graviton_mq.check_architecture
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/graviton_mq/architecture_test.exs test/graviton_mq/release_test.exs test/graviton_mq/dependency_graph_test.exs
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test
```

Expected: zero warnings (this confirms that the compiler's undefined-module
check accepts `:crypto` with `[:crypto]`); the architecture check passes with
no runtime `:ssl` reference; `0 failures`. A compile warning about
`:public_key`/`:crypto` means the declaration is wrong. Fix the declaration;
never suppress the warning. The runtime app-wide `mix test` is allowed here
because only this app's files are involved.

- [ ] **Step 5: Format and commit** — paths: the four files; message `feat(runtime): forbid OTP :ssl in the broker runtime`.

---

## Task 14: Independent-peer TLS cases (CI-only; NOT RUN locally)

**Files:** create `apps/graviton_mq/test/support/mqtt_tls_peer.py`,
`apps/graviton_mq/test/graviton_mq/mqtt_tls_paho_test.exs`; modify
`apps/graviton_mq/test/graviton_mq/mqtt_mosquitto_test.exs` (verified-TLS test, :223-253),
`apps/graviton_mq/test/graviton_mq/amqp10_proton_test.exs` (:149-206, shared-file protocol)

- [ ] **Step 1: Paho peer**

```python
"""Fixed-version independent MQTT5 TLS peer (Paho 2.1.0); never imported by production code."""

import argparse
import ssl
import time
from threading import Event

import paho.mqtt.client as mqtt


def context(ca, cert=None, key=None, tls12_only=False):
    tls = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
    if tls12_only:
        tls.maximum_version = ssl.TLSVersion.TLSv1_2
    if cert:
        tls.load_cert_chain(cert, key)
    return tls


def round_trip(port, ca, cert, key, hold):
    connected, received = Event(), Event()
    results = {}
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="paho-tls", protocol=mqtt.MQTTv5)
    client.tls_context = None
    client.tls_set_context(context(ca, cert, key))

    def on_connect(_client, _userdata, _flags, reason, _properties):
        results["connect"] = reason.value
        connected.set()

    def on_message(_client, _userdata, message):
        results["payload"] = message.payload
        received.set()

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost", port, keepalive=5)
    client.loop_start()
    try:
        assert connected.wait(5), "missing CONNACK"
        assert results["connect"] == 0, results
        client.subscribe("tls/paho/#", qos=1)
        time.sleep(0.2)
        client.publish("tls/paho/value", b"over-tls", qos=1).wait_for_publish(5)
        assert received.wait(5), "missing round-trip PUBLISH"
        assert results["payload"] == b"over-tls", results
        print(f"Paho TLS round trip PASS cipher={client.socket().cipher()} openssl={ssl.OPENSSL_VERSION}", flush=True)
        time.sleep(hold)
    finally:
        client.disconnect()
        client.loop_stop()


def tls12_only(port, ca):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="paho-tls12", protocol=mqtt.MQTTv5,
                         reconnect_on_failure=False)
    client.tls_set_context(context(ca, tls12_only=True))
    try:
        client.connect("localhost", port, keepalive=5)
    except ssl.SSLError as error:
        print(f"TLS1.2-only refused: {error.reason} openssl={ssl.OPENSSL_VERSION}", flush=True)
        return
    raise AssertionError("a TLS1.2-only Paho client connected to a TLS1.3-only broker")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["round-trip", "tls12-only"])
    parser.add_argument("port", type=int)
    parser.add_argument("ca")
    parser.add_argument("--cert")
    parser.add_argument("--key")
    parser.add_argument("--hold", type=float, default=1.0)
    arguments = parser.parse_args()
    if arguments.mode == "round-trip":
        round_trip(arguments.port, arguments.ca, arguments.cert, arguments.key, arguments.hold)
    else:
        tls12_only(arguments.port, arguments.ca)
```

(Delete the stray `client.tls_context = None` line if Paho 2.1.0 rejects it;
`tls_set_context` alone is the documented API.)

- [ ] **Step 2: Paho test file**

```elixir
Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)
Code.require_file("../../../graviton_mq_runtime/test/support/tls.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.TLSPahoTest do
  use ExUnit.Case, async: false
  @moduletag :paho_interop
  @moduletag timeout: 60_000

  alias GravitonMQ.Client.Test.Mosquitto
  alias GravitonMQ.Runtime.Test.TLS

  defmodule Policy do
    def authenticate(credentials, options) do
      send(options[:observer], {:peer, credentials.peer})
      {:ok, :client}
    end

    def authorize(_principal, _action, _resource, _options), do: :ok
  end

  setup_all do
    %{certificates: TLS.certificates!()}
  end

  test "Paho completes a verified TLS 1.3 round trip; the broker records TLS 1.3",
       %{certificates: certificates} do
    instance = broker(certificates)
    peer = peer!(["round-trip", port(instance), certificates.ca, "--hold", "2"])
    assert eventually!(fn -> tls_protocol(instance) == :"tlsv1.3" end)
    IO.puts("broker protocol=tlsv1.3; host #{String.trim(TLS.openssl!(["version"]))}")
    assert completed!(peer) =~ "Paho TLS round trip PASS"
  end

  test "Paho presents a client certificate; the mTLS principal facts are exact",
       %{certificates: certificates} do
    instance =
      broker(certificates, [auth: {Policy, observer: self()}],
        cacertfile: certificates.ca,
        verify: :verify_peer,
        fail_if_no_peer_cert: true
      )

    peer =
      peer!([
        "round-trip",
        port(instance),
        certificates.ca,
        "--cert",
        certificates.client_certificate,
        "--key",
        certificates.client_key
      ])

    client_der = certificates.client_der
    assert_receive {:peer, %{certificate: ^client_der, certificate_verified: true}}, 5_000
    assert completed!(peer) =~ "Paho TLS round trip PASS"
  end

  test "a TLS1.2-only Paho client is refused", %{certificates: certificates} do
    instance = broker(certificates)
    peer = peer!(["tls12-only", port(instance), certificates.ca])
    assert completed!(peer) =~ "TLS1.2-only refused"
    assert {:ok, %{connections: 0, sessions: 0}} = GravitonMQ.status(instance)
  end

  defp broker(certificates, options \\ [], tls \\ []) do
    start_supervised!(
      {GravitonMQ,
       name: nil,
       runtime_supervisor_name: nil,
       mqtt:
         Keyword.merge(
           [
             auth: :anonymous,
             host_principal: :host,
             listeners: [
               [
                 transport: :tls,
                 ip: {127, 0, 0, 1},
                 port: 0,
                 tls_options: TLS.listener_options(certificates, tls)
               ]
             ]
           ],
           options
         )},
      id: make_ref()
    )
  end

  defp port(instance) do
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    Integer.to_string(port)
  end

  defp peer!(arguments),
    do:
      Mosquitto.process!(
        System.fetch_env!("GRAVITON_PAHO_PYTHON"),
        [Path.expand("../support/mqtt_tls_peer.py", __DIR__) | arguments]
      )

  defp completed!(peer) do
    Mosquitto.eventually!(
      fn -> Mosquitto.snapshot(peer).exit_status != nil end,
      fn -> Mosquitto.snapshot(peer) end
    )

    assert %{exit_status: 0, output: output} = Mosquitto.snapshot(peer)
    IO.puts(output)
    output
  end

  defp tls_protocol(instance) do
    connections =
      instance
      |> find(GravitonMQ.Runtime.Supervisor)
      |> find(GravitonMQ.Runtime.MQTT.Supervisor)
      |> find(:connections)

    with [{_, connection, _, _} | _] <- DynamicSupervisor.which_children(connections),
         {:ok, information} <-
           SSL.connection_information(:sys.get_state(connection).socket, [:protocol]) do
      information[:protocol]
    else
      _ -> nil
    end
  end

  defp find(supervisor, id) do
    {^id, pid, _, _} = List.keyfind(Supervisor.which_children(supervisor), id, 0)
    pid
  end

  defp eventually!(predicate, deadline \\ System.monotonic_time(:millisecond) + 5_000) do
    cond do
      predicate.() -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true -> Process.sleep(20) && eventually!(predicate, deadline)
    end
  end
end
```

- [ ] **Step 3: Mosquitto evidence** — in "independent clients exchange over
verified TLS" (`mqtt_mosquitto_test.exs`), right after
`peer = subscriber(...)` (the subscriber is connected and waiting), add:

```elixir
    assert tls_protocol(broker.instance) == :"tlsv1.3"
    {openssl, 0} = System.cmd(context.executables.openssl, ["version"])
    IO.puts("Mosquitto TLS: broker protocol=tlsv1.3; peer #{String.trim(openssl)}")
```

Copy `tls_protocol/1` and `find/2` from Step 2 into this file. The existing
untrusted-CA and optional-certificate cases are unchanged; they now exercise the
ex_ssl server.

- [ ] **Step 4: Proton evidence** (shared-file protocol: run `git status --porcelain -- apps/graviton_mq/test/graviton_mq/amqp10_proton_test.exs` first). In "Proton exchanges over verified TLS and rejects a different trust root", right after `receiver = receiver!(...)`, add:

```elixir
    assert eventually_protocol(broker) == :"tlsv1.3"
    {openssl, 0} = System.cmd("openssl", ["version"])
    {proton_ssl, 0} =
      System.cmd(System.fetch_env!("GRAVITON_PROTON_PYTHON"), [
        "-c",
        "import ssl, proton; assert proton.SSL.present(); print(ssl.OPENSSL_VERSION)"
      ])

    IO.puts("Proton TLS: broker protocol=tlsv1.3; host #{String.trim(openssl)}; peer #{String.trim(proton_ssl)}")
```

with this helper:

```elixir
  defp eventually_protocol(broker, deadline \\ System.monotonic_time(:millisecond) + 5_000) do
    connections =
      broker
      |> find(GravitonMQ.Runtime.Supervisor)
      |> find(GravitonMQ.Runtime.AMQP10.Supervisor)
      |> find(:connections)

    protocol =
      Enum.find_value(DynamicSupervisor.which_children(connections), fn {_, connection, _, _} ->
        test = self()
        reference = make_ref()

        :sys.replace_state(connection, fn shell ->
          socket = :ets.lookup_element(shell.table, :state, 2).socket
          send(test, {reference, socket && SSL.connection_information(socket, [:protocol])})
          shell
        end)

        receive do
          {^reference, {:ok, [protocol: protocol]}} -> protocol
          {^reference, _other} -> nil
        after
          1_000 -> nil
        end
      end)

    cond do
      protocol -> protocol
      System.monotonic_time(:millisecond) >= deadline -> nil
      true -> Process.sleep(20) && eventually_protocol(broker, deadline)
    end
  end

  defp find(supervisor, id) do
    {^id, pid, _, _} = List.keyfind(Supervisor.which_children(supervisor), id, 0)
    pid
  end
```

`amqp10_sasl_proton_test.exs` P3/P4 (PLAIN over verified TLS, wrong password
refused) need no source change.

- [ ] **Step 5: Local compile-only check**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp mix test test/graviton_mq/mqtt_tls_paho_test.exs test/graviton_mq/mqtt_mosquitto_test.exs test/graviton_mq/amqp10_proton_test.exs
python3 -I -m py_compile /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq/test/support/mqtt_tls_peer.py
```

Expected: every test excluded (`0 tests` run, no compile warnings), and the
Python file compiles. Record "independent-peer TLS: NOT RUN (requires CI)".

- [ ] **Step 6: Format and commit** — paths: the four Elixir test files (minus any not touched) and the Python peer; message `test(interop): add TLS peer cases for Paho, Mosquitto and Proton`.

---

## Task 15: Packaging — installed consumer TLS round trip, `:ssl` not started

**Files:** modify `scripts/build_hex.exs` (before `smoke` at :198; in `smoke`; in `embedding_smoke`)

- [ ] **Step 1: Certificates in the staging directory** — insert after the
`version = ...` line (:65):

```elixir
tls_directory = Path.join(staging, "tls")
File.mkdir!(tls_directory)
tls = fn name -> Path.join(tls_directory, name) end

openssl! = fn arguments ->
  {output, status} = System.cmd("openssl", arguments, stderr_to_stdout: true)
  if status != 0, do: raise("openssl #{Enum.join(arguments, " ")} failed: #{output}")
end

openssl!.(~w(req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=archive-ca
  -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign
  -keyout) ++ [tls.("ca.key"), "-out", tls.("ca.pem")])

openssl!.(~w(req -new -newkey rsa:2048 -nodes -subj /CN=localhost -keyout) ++
  [tls.("server.key"), "-out", tls.("server.csr")])

File.write!(tls.("server.ext"), """
basicConstraints=critical,CA:FALSE
subjectAltName=DNS:localhost
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
""")

openssl!.(["x509", "-req", "-in", tls.("server.csr"), "-CA", tls.("ca.pem"), "-CAkey",
  tls.("ca.key"), "-set_serial", "1", "-days", "1", "-extfile", tls.("server.ext"), "-out",
  tls.("server.pem")])

tls_smoke =
  quote do
    fn ->
      {:ok, instance} =
        GravitonMQ.start_link(
          name: :hex_tls_smoke,
          runtime_supervisor_name: :hex_tls_smoke_runtime,
          mqtt: [
            auth: :anonymous,
            host_principal: :archive_host,
            listeners: [
              [
                transport: :tls,
                ip: {127, 0, 0, 1},
                port: 0,
                tls_options: [
                  certfile: unquote(tls.("server.pem")),
                  keyfile: unquote(tls.("server.key"))
                ]
              ]
            ]
          ]
        )

      {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)

      {:ok, socket} =
        SSL.connect(
          ~c"localhost",
          port,
          [
            :binary,
            active: false,
            verify: :verify_peer,
            cacertfile: unquote(tls.("ca.pem")),
            server_name_indication: ~c"localhost",
            versions: [:"tlsv1.3"]
          ],
          5_000
        )

      :ok = SSL.send(socket, <<0x10, 0x0E, 0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c">>)
      {:ok, <<0x20, _connack::binary>>} = SSL.recv(socket, 0, 5_000)
      :ok = SSL.send(socket, <<0xC0, 0>>)
      {:ok, <<0xD0, 0>>} = SSL.recv(socket, 2, 5_000)
      :ok = SSL.close(socket)
      :ok = Supervisor.stop(instance)

      false =
        Enum.any?(Application.started_applications(), fn {app, _, _} -> app == :ssl end)

      IO.puts("Broker TLS 1.3 listener round trip PASS; OTP :ssl not started")
      :ok
    end
  end
```

- [ ] **Step 2: Call it** — at the end of the `smoke` quote add
`check_tls = unquote(tls_smoke)` and `:ok = check_tls.()`. In
`embedding_smoke` (no client package there; ex_ssl still arrives through
hell), after `:ok = Supervisor.stop(broker_host)` add
`false = Enum.any?(Application.started_applications(), fn {app, _, _} -> app == :ssl end)`.
The client-only consumer is unchanged.
- [ ] **Step 3: Run packaging** (needs network for Hex; run as CI does)

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && grep -n "stage_hex\|build_hex" .github/workflows/*.yml
```

Use the exact staging invocation found there, with the S1b env prefix, and a
fresh staging directory under `/tmp/gmq-s1b-tmp`. Expected: seven archives and
three consumers pass, and the output contains
`Broker TLS 1.3 listener round trip PASS; OTP :ssl not started`. If Hex is
unreachable, record packaging as NOT RUN with the error.
- [ ] **Step 4: Format and commit** — path `scripts/build_hex.exs`; message `test(release): prove an installed broker serves TLS without OTP :ssl`.

---

## C1 (coordinator-serialized): Full required verification

Run after S3 has stopped writing.

- [ ] **Step 1: AGENTS.md sequence**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
export MIX_BUILD_ROOT=/tmp/gmq-build-s1b TMPDIR=/tmp/gmq-s1b-tmp
mix deps.get
mix format
mix format --check-formatted
mix compile --warnings-as-errors
mix test
mix graviton_mq.check_architecture
mix help xref
mix xref graph --format cycles --fail-above 0
git diff --quiet mix.lock && echo "lock unchanged"
```

Expected: `mix format` makes no change (`git status --short` shows nothing
new), zero warnings, `0 failures` (peer-tagged and `:requires_http_fetch_169`
tests excluded), architecture check clean, `No cycles found`, `lock unchanged`.

- [ ] **Step 2: Gate evidence** —
`mix test --include requires_http_fetch_169 apps/graviton_mq_runtime/test/amqp10/connection_input_test.exs apps/graviton_mq/test/graviton_mq/mqtt_tls_test.exs`.
Expected: exactly the two T6 tests fail, as in the Task 11 log.
- [ ] **Step 3: Diff review**

```bash
git log --oneline BASE..HEAD
git log BASE..HEAD --format=%B | grep -c "Co-Authored-By" || true
grep -rn ":ssl\b" apps/graviton_mq_runtime/lib apps/graviton_mq/lib
grep -rn "WORKAROUND(upstream): gsmlg-dev/http_fetch#168" apps
```

Confirm: the `Co-Authored-By` count is `0`. No runtime lib references `:ssl`
(only `architecture.ex` names it as data). No S1b commit touches an S3-owned
file. Every #168 marker is in `tls_options.ex` and its tests.

## C2 (coordinator): Push and CI

- [ ] When authorized to push, push and wait for `ci.yml` and the Tests
  workflow on that HEAD. Read `gh run view RUN_ID --log` for the Paho, Mosquitto
  and Proton TLS lines (protocol and OpenSSL versions), the TLS1.2-only refusal
  and the installed-consumer TLS line. On failure, diagnose and fix in a new S1b
  task; never write PASS for a failing or skipped run.
- [ ] `scripts/interop/*` and `.github/workflows/*`: the new Paho file uses the
  existing `:paho_interop` tag, which `run.py -- mix test` already selects, so
  no harness change is expected. Confirm this with
  `grep -n "paho_interop\|GRAVITON_PAHO_PYTHON" scripts/interop/run.py .github/workflows/ci.yml`.
  Change the harness only if CI shows the peer cannot load
  `ssl.create_default_context` (for example, CPython built without OpenSSL).

## C3 (coordinator): AGENTS.md and ADR 0009 amendments

- [ ] **AGENTS.md** (owner decision Q3, 2026-11-13): replace
  "`Hell` only in runtime; ex_ssl `SSL` only in runtime and client; the client
  must not reference OTP `:ssl`; and" with
  "`Hell` only in runtime; ex_ssl `SSL` only in runtime and client; neither the
  runtime nor the client may reference OTP `:ssl` (owner decision 2026-11-13, S1b); and".
- [ ] **ADR 0009**: in the S1b bullet, record "Implemented on `main`
  (`<first>..<last>`); broker TLS listeners accept through
  `Hell.Transports.SSL`; TLS 1.3 only; the runtime no longer references OTP
  `:ssl` (`extra_applications: [:crypto]`)." Add **"Not qualified: blocked on
  gsmlg-dev/http_fetch#169 (coalesced active-once input; T6 fails on rc.14).
  In-memory TLS private keys are rejected and passwords reach Hell only as
  closures until gsmlg-dev/http_fetch#168."** Replace the Consequences
  sentence "TLS listeners, still on OTP `:ssl`, keep their previous behaviour"
  with the TLS listener-failure semantics (now the same as TCP). Record only
  executed evidence, with run IDs.

## C4 (coordinator): CAPABILITIES, broker docs, upgrade note, release block

- [ ] `docs/CAPABILITIES.md`, MQTT5 broker and AMQP1 broker rows: append
  "2026-11-13 S1b (unreleased, NOT QUALIFIED): TLS listeners through `hell`/`ex_ssl`
  1.0.0-rc.14, TLS 1.3 only, TLS options allowlisted (`{:unsupported_tls_option, key}` /
  `{:invalid_tls_option, key}`), in-memory keys refused (#168), TLS listener failure
  closes its own connections; local tests PASS (`hell_handoff_test.exs`,
  `tls_options_test.exs`, `mqtt_tls_test.exs`, `amqp10_tls_test.exs`,
  `connection_input_test.exs`); independent peers <PASS in run RUN_ID | NOT RUN>;
  qualification blocked on gsmlg-dev/http_fetch#169 (T6 fails on rc.14)."
  Change the TCP/TLS limits cells to say that broker TLS is TLS 1.3 only. Keep
  existing text.
- [ ] `docs/MQTT_BROKER.md:136-137`: delete "The host must start OTP `:ssl`
  when using TLS …" and state TLS 1.3 only, the allowlist, `keyfile` required
  (no in-memory key until #168), no session resumption, no client-certificate
  revocation checking, and the TLS budget (at most 2 × `connect_timeout` for the
  handshake plus CONNECT). `docs/AMQP10_BROKER.md:261`: replace "TLS options
  cannot replace transport-owned binary/active-once framing or write timeouts"
  with the allowlist statement and the same TLS facts.
- [ ] Upgrade note: create `docs/releases/UNRELEASED-s1b.md` (or the
  owner-chosen next-version file) with the spec §6 bullets. For "Per-connection
  TLS memory" write: "Unchanged input limits (option B); qualification waits on
  gsmlg-dev/http_fetch#169."
- [ ] `docs/ACTIVE_SCOPE.md`/programme status: "S1b implemented; not qualified;
  **no release or publication from `main` until C6 passes** (D11)."

## C5 (coordinator): Agent Note

- [ ] Use the `agent-note` skill to evaluate one note: "S1b broker TLS on
  ex_ssl: layout, #168 workaround, #169 gate, test lessons" (sources: this plan,
  the spec, ADR 0009). Write or update it only if the quality gate passes. If the
  MCP is unavailable, report that.

## C6 (coordinator; when upstream ships): #169 gate and #168 removal

- [ ] **#169 released.** Update `{:hell, "== X"}` (runtime) and
  `{:ex_ssl, "== X"}` (client) together with `mix.lock` and every pin assertion
  (`grep -rn "1.0.0-rc.14" apps scripts docs`). Run
  `mix test --include requires_http_fetch_169` on both T6 files: they must
  **pass**. Then remove `@tag :requires_http_fetch_169` from both tests and the
  exclusion from both `test_helper.exs` files. Run C1 and the hosted
  interop/soak/packaging jobs. Only then mark S1b QUALIFIED in CAPABILITIES and
  ADR 0009 and lift the release block.
- [ ] **#168 fixed and pinned.** Remove the `value(:key, _)` rejection and the
  `:key in keys` branch in `identity_entry/2` (both carry the marker); flip the
  "in-memory private keys are rejected" test to accept them and add T9's
  key-DER telemetry assertion for an in-memory `key`. The password closure may
  stay as defence in depth; remove its marker comment or keep it with a note.
  Update the docs and the upgrade note.

---

## Spec coverage checklist

| Spec item | Task |
| --- | --- |
| §4.1 single Hell path, handshake in handler, `{:accepted, %SSL.Socket{}}`, deleted accept loop | 5, 6, 7 |
| §4.2 Connection transport calls, AMQP `configure_socket` per transport | 6, 7 |
| §4.3 coalesced input, option B, T6 gate | 11, C6 |
| §4.4 output: single Writer, errors mapped, `send_timeout` | 6, 7, 10 |
| §4.5 peer facts, `connection_information` agreement | 8, 9 |
| §4.6 SNI/ALPN accepted, default identity required | 2 |
| §4.7 allowlist, error terms, MQTT drops 3 OTP keys, AMQP allowlist | 2, 3, 4 |
| §4.8 `handshake_timeout`, TLS `num_connections` | 5, 8, 9 |
| §4.9 listener failure, rebind error shape, budgets | 5, 12 |
| §4.10 privacy, password closure, Q1 key rejection | 2, 12, C6 |
| §4.11 architecture rule, `[:crypto]`, release pin | 13, C3 |
| §5 no Connection/owner event on handshake failure | 5, 7, 8, 9 |
| §6 upgrade note | C4 |
| §7.1 migrations (connection_test, connection_input_test, config tests, hell_handoff_test, architecture/release tests) | 3, 4, 5, 7, 13 |
| §7.2 T1–T9 | T1 5; T2 5/8/9; T3 8/9; T4 5/8/9; T5 10; T6 11; T7 12; T8 5/12; T9 2/12 |
| §7.3 independent peers incl. TLS 1.2-only, protocol and OpenSSL records | 14, C2 |
| §7.4 packaging, `:ssl` absent | 15 |
| §8 U1/U2 already filed (#168, #169) | 0, C6 |

---

## Execution log

(Record BASE, per-task commits, deviations, the T6 baseline failures, and every NOT RUN item here.)

### Tasks 0–2 (2026-11-12)

- **Task 0.** BASE = `e015263`. Upstream: gsmlg-dev/http_fetch#168 and #169
  are both `CLOSED` (2026-11-12T09:28Z); their fixes ship in hell/ex_ssl
  `1.0.0-rc.17`, already pinned on `main` (`e967c1d`). No upstream writes.
  Baseline: runtime 69 tests, 0 failures; `graviton_mq` 51 tests, 0 failures
  (interop tags excluded).
- **Task 1.** `72924ad` test(runtime): fixture as planned; smoke load printed
  the 13 keys and `true`. `:public_key` was reachable; no change needed.
- **Task 2.** `35116e2` feat(runtime): red (7 tests, 7 failures, module not
  available) → green (7 tests, 0 failures); runtime compiles with
  `--warnings-as-errors`. Deviations per the amendment: in-memory `key`
  (`{:RSAPrivateKey | :ECPrivateKey | :PrivateKeyInfo, der}`, ex_ssl's accepted
  form) is accepted at top level, in `certs_keys` and in `sni_hosts`; a
  malformed key reports `:key` (nested: the outer key); an identity needs
  `cert | certfile` plus `key | keyfile` (missing private key still reports
  `:keyfile`); no #168 WORKAROUND markers; `active_chunk_size` is rejected as
  `{:unsupported_tls_option, :active_chunk_size}` (not on the allowlist). The
  runtime-owned `active_chunk_size: 16_384` is **not** set by `hell_options/1`;
  Task 5 adds it to HellHandoff's TLS transport options next to `buffer: 16_384`.

### Tasks 3–5 (2026-11-12)

- **Task 3.** `56f08d3` feat(mqtt): red (7 tests, 1 failure: `tls.([])` gave
  `{:error, :invalid_mqtt_options}`) → green (7 tests, 0 failures). Deviation
  per the amendment: the `# WORKAROUND(upstream): gsmlg-dev/http_fetch#168`
  key-rejection assertion becomes an acceptance of an in-memory
  `certs_keys: [%{cert:, key:}]` identity; an added assertion rejects a user
  `active_chunk_size` with `{:unsupported_tls_option, :active_chunk_size}`.
  `path?/1` and `@tls_keys` deleted (no other users).
- **Task 4.** `cb69c01` feat(amqp): shared-file check clean; red (7 tests,
  1 failure: `{:error, :invalid_listener}` for `active: true`) → green
  (7 tests, 0 failures). Same amendment deviations: in-memory
  `cert` + `key` is accepted (no WORKAROUND marker), and `active_chunk_size`
  joins the rejected-key list. `certificate?/1` and `private_key?/1` deleted.
- **Task 5.** `2463070` feat(runtime): shared-file check of
  `amqp10/listener.ex` clean. Red (27 tests, 25 failures: Hell `:badarg`
  from the settings map passed as `send_timeout`) → green (27 tests,
  0 failures, three consecutive runs). Product check
  `mqtt_connection_test.exs` + `amqp10_runtime_test.exs`: 30 tests, 0 failures
  when run **from the umbrella root**; from `apps/graviton_mq` all 15 AMQP
  runtime cases fail with "`GravitonMQ.AMQP10Client` ... does not exist"
  because `graviton_mq` does not depend on the client app (environmental, not
  this change). Deviations:
  - Per the amendment, the TLS transport options carry the runtime-owned
    `active_chunk_size: 16_384` (next to `buffer: 16_384`, before the user
    options). An added `tls only` case, "every active-once delivery is
    bounded to 16 KiB", sends 64 KiB while the probe is unarmed and asserts
    four ≤ 16_384-byte deliveries; it fails without the option (verified).
  - **Spec §4.8 / T4 conflict.** Hell's `num_connections` is **per
    acceptor** (`hell.ex` docs: the server-wide maximum is
    `num_acceptors * num_connections`; `acceptor_supervisor.ex:51`). With
    HellHandoff's 4 acceptors and `num_connections: max_connections`, a
    throwaway probe with `max_connections: 1` completed 4 concurrent TLS
    handshakes and refused the 5th. The planned test (one connection, then a
    refused second) failed. Implementation kept as planned; the test now
    asserts the bound Hell enforces (4 × `max_connections`: four held, the
    fifth refused within 500 ms, a new one accepted within 4 s after all four
    close). Established Connections stay bounded by the `:connections`
    supervisor's `max_children`. **Coordinator decision needed** on the spec
    statement (accept 4 × `max_connections`, or e.g. a single TLS acceptor,
    which would conflict with the parallel-release case that needs ≥ 2).
- **Coordinator fix.** `fa45b0f` fix(runtime): each of HellHandoff's 4
  acceptors takes `num_connections: ceil(max_connections / 4)`; spec §4.8
  amended accordingly.

### Tasks 6–7 (2026-11-12)

- **Task 6.** `b423c6e` feat(mqtt): red (1 test, 1 failure:
  `SSL.connection_information/1` returned `{:error, :badarg}` on the OTP
  socket) → green. Verify: `mqtt_tls` + `mqtt_connection` + `mqtt_privacy`
  + `mqtt_host` 19 tests, 0 failures; runtime `hell_handoff_test.exs` +
  `test/mqtt` 54 tests, 0 failures; extra regressions `mqtt_feature`,
  `mqtt_persistence`, `mqtt_feature_persistence` + `mqtt_tls` 31 tests,
  0 failures; no `:ssl.` in `runtime/mqtt/*.ex`. Deviations: to keep the
  test file warning-free, `mqtt_tls_test.exs` omits the helpers and aliases
  only Tasks 8–12 use (`import ExUnit.CaptureLog`, aliases `Disconnect`,
  `Suback`, `Unsuback`, `Listener`; `tcp_connected/2`, `listener!/2`,
  `find_listener/2`, `monitor!/1`, `free_port/0`) and reduces `tls_listener`
  to `tls_listener(certificates, tls)` (id `:tls`, port 0); Tasks 8–12
  restore them from the plan text. The listener `child_spec` comment now
  says "never kills a listener mid-stop" (was "a TCP listener").
- **Task 7.** `77006a5` feat(amqp): shared-file check of the five files
  clean. Red: `amqp10_tls_test.exs` 2 tests, 2 failures (`{:error, :badarg}`;
  silent-peer count `active: 1`); `connection_input_test.exs` 3 tests,
  1 failure (`:ssl.recv` `{:error, :closed}`: the Connection crashed calling
  `:ssl.handshake/2` on an `%SSL.Socket{}`) → green. The Connection-level
  test "TLS handshake with a silent peer respects the finite connection
  timeout" was deleted from `connection_test.exs`; its behaviour moved to
  the listener-level case "a silent TCP peer to a TLS listener is closed
  within connect_timeout and starts no Connection" in `amqp10_tls_test.exs`
  (and Task 9). The `{:transport_down, {:handshake, _}}` reason no longer
  exists. Verify: runtime `test/amqp10` + `hell_handoff_test.exs` 131 tests,
  1 failure in `DurableQueueTest` "a small log limit compacts repeatedly…"
  (`{:error, :unknown_outcome}`). It drives the S3-owned `Queue` directly,
  touches no listener/Connection, and passed 3/3 isolated reruns (10 tests,
  0 failures each): load flake, not this change. Umbrella `amqp10_tls` +
  `amqp10_runtime` + `amqp10_sasl` + `amqp10_boundaries` 39 tests,
  0 failures; after S3's `e71e62d` landed, `amqp10_tls` + `mqtt_tls` +
  `amqp10_sasl` 17 tests, 0 failures. `grep -rn ":ssl\." runtime/lib` prints
  nothing. Same test-helper trimming as Task 6 (`CaptureLog`, the SASL
  aliases, `@sasl`, `plain!/1`, `read_sasl!/1`, `listener!/2`,
  `find_listener/2`, `monitor!/1`, `free_port/0`; `broker!/2`,
  `tls_listener/1`, `port/1`); the `Auth` module is kept. The AMQP listener
  comment gets the same "a listener" wording.

### Tasks 8–10 (2026-11-12)

- **Task 8.** `1503a68` test(mqtt): T2/T3/T4 regression tests passed on
  their first run (6 tests, 0 failures; three runs). The TLS 1.2 refusal is
  the planned `{:tls_alert, {:protocol_version, _}}` shape. Restored only the
  helpers these tests use (new `refused!/1`, `closed/2`); `broker/3` already
  takes `tls`.
- **Task 9.** `1001267` test(amqp): T2/T3/T4 passed on their first run
  (5 tests, 0 failures; three runs). Restored the SASL aliases, `Limits`,
  `Auth` alias, `@sasl`, `plain!/1`, `read_sasl!/1`, `broker!/3` and
  `tls_listener/2` (`certificates, tls`; id `:tls`, port 0); `port/1` kept.
- **Task 10.** `66f0f28` test(runtime): shared-file check of
  `connection_input_test.exs` clean. AMQP T5: 5 tests, 0 failures (three
  runs). MQTT T5: 8 tests, 0 failures (three runs) after two test
  deviations, both diagnosed with throwaway instrumentation:
  - **Blocked-Writer precondition strengthened.** A single stack sample
    with an `SSL` frame catches an ordinary in-progress `SSL.send` (a
    synchronous ex_ssl call): 500 ms after `fill_until_blocked` returned,
    the Writer was idle in `gen_server:loop`, so the send_timeout case
    failed (DOWN never arrived). `blocked_in_tls_send?/1` now requires the
    Writer to be inside `SSL` on two samples 200 ms apart with its mailbox
    not shrunk (deliveries keep arriving asynchronously, so it may grow).
    Same predicate in the AMQP helpers. With it the Writer is genuinely
    blocked (the server carrier reported `send_pend` > 0).
  - **MQTT DISCONNECT assertion.** With a genuinely blocked Writer, the
    planned "DOWN within 2 s (< send_timeout 3 s)" failed every run. This is
    **not** an ex_ssl #161 regression: the carrier read the record
    (`recv_oct` +24), ex_ssl decrypted it (read sequence 3) and delivered it,
    the MQTT Connection stayed responsive, and `status/1` showed
    `sessions: 0, connections: 1` 250 ms after the DISCONNECT. The broker
    defers `{:mqtt_close, _}` while `connection.pending` holds unsettled sends
    (`runtime/mqtt/broker.ex` `close_connection/5`, non-forced), on any
    transport, so the Connection closes when the blocked write settles at
    `send_timeout`. The test now asserts the #161 property: the session ends
    within 2 s while the Writer is still blocked in `SSL` and the connection
    is still counted, then DOWN within 4 s. No upstream routing (not a
    blocker). The AMQP Close case passes as planned (no such deferral).
  - Config accepted `max_writer_bytes: 8_388_608` and
    `max_pending_bytes: 16_777_216` unchanged.
- **Regressions.** Runtime `test/amqp10/{connection_input,connection,writer}_test.exs`
  + `hell_handoff_test.exs` + `test/mqtt`: 108 tests, 0 failures. Umbrella
  `mqtt_tls` + `amqp10_tls` + `mqtt_connection` + `amqp10_sasl`: 40 tests,
  0 failures. No compile warnings in the touched test files.
- **Hand-off to Tasks 11–12** (helpers still absent, add with their tests):
  MQTT `import ExUnit.CaptureLog`, aliases `Disconnect`, `Unsuback`,
  `Listener`; `tcp_connected/2`, `listener!/2`, `find_listener/2`,
  `free_port/0`, and `tls_listener/4` (id/port; currently
  `tls_listener(certificates, tls)`). AMQP `import ExUnit.CaptureLog`, alias
  `Listener`; `listener!/2`, `find_listener/2`, `monitor!/1`, `free_port/0`,
  `tls_listener` id/port and `port(broker, id)`. MQTT already has
  `monitor!/1`, `closed/2`, `refused!/1`, `subscribe!/1`, `in_tls_send/1`;
  T6 can reuse the strengthened blocked predicate.

### Tasks 11–12 (2026-11-12)

- **Task 11.** `18f59ff` test(runtime): shared-file check of
  `connection_input_test.exs` clean. Per the amendment T6 is a normal test:
  no `:requires_http_fetch_169` tag, and **neither `test_helper.exs` is
  touched** (Step 1 dropped). Red evidence, both produced with throwaway
  changes that were reverted: MQTT with `active_chunk_size: 16_384` removed
  from HellHandoff → 1 failure (`:ssl.recv` `{:error, :closed}` mid-UNSUBACKs,
  `:parser_limit`); AMQP without the option on the hand-made socket → 1
  failure (the pipelined Open is never processed: input-limit close). Green:
  MQTT 9 tests, AMQP input 6 tests, 0 failures (three runs each). Deviations:
  - **Chunk sizes are not traceable.** Both Connections set
    `Process.flag(:sensitive, true)`; `:erlang.trace(connection, true,
    [:receive, ...])` returns 1 and `trace_info` shows `[:receive]`, yet no
    trace message arrives. The amendment's "no delivered chunk exceeds
    16 KiB" is therefore observed through the Connection's own bound: MQTT
    T6 uses `max_packet_size: 128` (plan: 4_096), so any delivery above
    16_512 bytes closes with `:parser_limit`. The exact 16_384 bound stays
    proven by HellHandoff's "every active-once delivery is bounded to
    16 KiB" case. Side finding (not fixed, out of scope): `idle_messages/2`
    in `connection_input_test.exs` traces the same sensitive Connection, so
    its `== []` assertions are vacuous.
  - **AMQP T6 is Connection-level (D8),** so the test applies the runtime's
    option itself: `SSL.setopts(socket, active_chunk_size: 16_384)` on the
    `TLS.server_pair!` socket. The 15 s wait needs `@tag timeout: 30_000`
    (the module default is 10 s).
  - **The AMQP oversize-frame assertion was wrong.** An oversize declaration
    makes the endpoint send `Close{error}` and wait in `:closing` (default
    close timeout). It emits no `{:disconnected, _}` within 2 s: the endpoint
    stayed `:closing` 500 ms later, consistent with the existing
    `connection_test.exs` case. T6 now reads the broker's `%Close{error: %{}}`
    from the peer (`read_tls_pdu/1`).
  - MQTT UNSUBACKs are also asserted to carry `reason_codes: [0x11]`.
  - The blocked-Writer predicate was not needed: the peer reads concurrently
    and no deferred close was observed.
  - Regressions: runtime `hell_handoff` + `amqp10/{connection_input,
    connection,writer}` + `test/mqtt` 109 tests, 0 failures; umbrella
    `mqtt_tls` + `amqp10_tls` 14 tests, 0 failures (three runs).
- **Task 12.** `363d0dd` test(runtime): T7/T8 as planned (passed first run).
  The T9 changes follow the amendment, because #168 is fixed:
  - The in-memory-key **refusal assertions are replaced.** Each broker runs
    two TLS listeners, `:tls` (encrypted `keyfile` + `password`) and `:key`
    (`certfile` + `key: {:PrivateKeyInfo, der}`). Through each listener the
    test makes a round trip. Neither the password nor the key DER appears in:
    our Listener status, the MQTT/AMQP Connection status, the HellHandoff
    handler status (`Hell.connection_pids/1`, precedent in
    `amqp10_boundaries_test.exs`), Hell's listener state and status, or
    Hell's server state and children. Neither appears in the captured logs
    of killing both listeners, nor in any `[:hell, :listener, :start|:stop]`
    metadata (MQTT; at least 4 starts plus one stop).
  - `leaks?/2` also matches the inspected 24-byte prefix, which catches
    truncated binaries in logs. An in-test assertion checks that it detects
    an unredacted key. A throwaway control confirmed that Hell's
    `ServerConfig` inspection shows `password: :redacted` and
    `key: :redacted`.
  - The AMQP `password: 4_242` refusal is kept.
  - Restored helpers: MQTT `CaptureLog`, `Listener`, `tcp_connected/2`,
    `listener!/2`, `find_listener/2`, `free_port/0`, `tls_listener/4`;
    `Disconnect` and `Unsuback` were restored in Task 11. AMQP `CaptureLog`,
    `Listener`, `listener!/2`, `find_listener/2`, `monitor!/1`,
    `free_port/0`, `tls_listener/4`, `port/2`.
  - Green: `mqtt_tls` + `amqp10_tls` 20 tests, 0 failures (three runs).
    Umbrella `mqtt_connection` + `amqp10_sasl` + `amqp10_runtime` +
    `mqtt_privacy` 45 tests, 0 failures. No compile warnings.
    `DurableQueueTest` was not run in these sets.

### Tasks 13–15 (2026-11-12)

- **Task 13.** `31becde` feat(runtime): red (`architecture_test` +
  `release_test` 38 tests, 2 failures: the runtime `:ssl` reference was
  allowed; the application list was `[:ssl]`) → green (+
  `dependency_graph_test` 41 tests, 0 failures). Umbrella
  `compile --warnings-as-errors` clean; `mix graviton_mq.check_architecture`
  passed (7943 source references across 7 applications); runtime app-wide
  `mix test` 171 tests, 0 failures. **`extra_applications: [:ssl]` removed**
  (now `[:crypto]`). Evidence that nothing needs OTP `:ssl` started:
  runtime lib has no `:ssl` module reference (only the `{:ssl, socket, bytes}`
  message tags ex_ssl sends); ex_ssl and hell lib call no `:ssl.` function;
  their built `.app` files list `[kernel, stdlib, elixir, crypto, public_key,
  logger]` and `[kernel, stdlib, elixir, logger, ex_ssl, telemetry]` (each adds
  `:ssl` only under its own `Mix.env() == :test`). In a plain `erl` VM (no Mix)
  the runtime's dependency closure has no `:ssl` and
  `ensure_all_started(:graviton_mq_runtime)` leaves `:ssl` unstarted. Under
  `mix run` `:ssl` is always started beforehand by Hex (hex.app lists `ssl`,
  `inets`), so Mix-hosted observations cannot show this.
- **Task 12 hand-off (`idle_messages/2`).** `ac581c3` test(amqp): the trace
  probe is replaced by a `:sys.install/2` debug function that runs inside the
  sensitive Connection and reports each `{:in, message}`; `:sys.remove/2` is
  synchronous, so collection uses `after 0`. Production `:sensitive` is kept.
  Mutation proof (temporary, reverted): a 50 ms self-rescheduling
  `:mutation_poll` armed with `active: :once` in `amqp10/connection.ex` →
  6 tests, 2 failures (both idle tests list `:mutation_poll`); the old trace
  probe under the same mutation → 6 tests, 0 failures (vacuous, confirmed).
  Green 6 tests, 0 failures (three runs).
- **Task 14.** `5410ea3` test(interop): Paho peer + test, Mosquitto and Proton
  TLS evidence (shared-file check of `amqp10_proton_test.exs` clean). Local
  compile-only run: 26 tests, 0 failures, 26 excluded, no warnings
  (`--warnings-as-errors`); `py_compile` OK. **Independent-peer TLS: NOT RUN
  (requires CI).** Deviations: stray `client.tls_context = None` dropped (plan
  allowance); `completed!/1` waits 15 s (default 5 s is tight with `--hold 2`);
  `tls_protocol/1` skips a Connection whose socket is still `nil`; Proton
  reuses the file's existing `child/2` instead of a duplicate `find/2` and its
  existing `context.python`. The Mosquitto "peer" OpenSSL line is the host
  `openssl version` (as planned), not libmosquitto's linked library.
- **Task 15.** `a124d3f` test(release): certificates in the staging directory
  and the planned `tls_smoke`. **Deviation:** the `:ssl`-not-started assertion
  cannot run under `mix run` (Hex has already started `:ssl`), so the smoke
  runs in a separate `elixir -pa <consumer>/_build/lib/*/ebin` VM (no Mix/Hex)
  that starts `:graviton_mq_runtime` itself, for the main consumer and for the
  embedding consumer (no client package: ex_ssl through hell only); the
  planned `embedding_smoke` in-`mix run` assertion is replaced by that run.
  Local run (`stage_hex.exs` + `build_hex.exs`, Hex reachable): exit 0, seven
  archives, three consumers, `Broker TLS 1.3 listener round trip PASS; OTP :ssl
  not started` printed twice; Mosquitto/Proton/Artemis/client network parts
  NOT RUN (no peers). Mutation in the same VM (`ensure_all_started(:ssl)`
  first) fails the assertion with `MatchError`.
- Regressions: umbrella `mqtt_tls` + `amqp10_tls` 20 tests, 0 failures; final
  architecture check passed.

### C-tasks done (2026-11-13)

- **C1/C2 (hosted CI).** CI run 43223448120 for `375f677`: `interoperability`
  and `checks` PASS; Tests run 43223447801 PASS. Evidence lines:
  `Paho TLS round trip PASS cipher=('TLS_AES_128_GCM_SHA256', 'TLSv1.3', 128)
  openssl=OpenSSL 3.0.13`; `TLS1.2-only refused: TLSV1_ALERT_PROTOCOL_VERSION`;
  `Mosquitto TLS: broker protocol=tlsv1.3; peer OpenSSL 3.0.13`; `Proton TLS:
  broker protocol=tlsv1.3; host OpenSSL 3.0.13 ...; peer OpenSSL 3.0.13`;
  `Broker TLS 1.3 listener round trip PASS; OTP :ssl not started` twice (main
  and embedding consumer); all installed-package checks PASS. The earlier CI
  run 43214315813 at `83567a7` failed only S3's Artemis R2 (Artemis routing,
  fixed in `375f677`), not an S1b case. No harness change was needed.
- **C3.** AGENTS.md: "neither the runtime nor the client may reference OTP
  `:ssl` (owner decision 2026-11-13, S1b)". ADR 0009: S1b implemented and
  CI-qualified, rc.17 pins, #168/#169 fixed, in-memory keys accepted,
  runtime-owned `active_chunk_size: 16_384`, TLS listener failure semantics
  equal to TCP, the `max_connections + 3` handshake bound; the "TLS listeners
  still on OTP `:ssl`" consequence is replaced. The rc.14 "Not qualified:
  blocked on #169" text of C3 is obsolete (Amendment 2026-11-13) and was not
  used.
- **C4.** CAPABILITIES MQTT5/AMQP1 broker rows (S1b evidence, TLS 1.3 only in
  the limitation cells), MQTT_BROKER.md (OTP `:ssl` host sentence deleted;
  allowlist table, no resumption/revocation, handshake budget),
  AMQP10_BROKER.md (allowlist statement replaces the framing sentence),
  upgrade note `docs/releases/UNRELEASED-s1b.md`, ACTIVE_SCOPE S1b row
  Complete and the release block's lifting condition recorded as met at run
  43223448120; a release still requires an explicit owner request.
- **C5 (Agent Note):** done by the coordinator: note `df942606` updated (rc.17 status, per-acceptor `num_connections`); new notes `0cb5c5d8` (Artemis bare address is MULTICAST) and `9cec1ae6` (sensitive processes cannot be traced; use `:sys.install`).
- **C6:** none (the Amendment pinned rc.17 with #168/#169 fixed).
- Verification: `release_test.exs` + `architecture_test.exs` and
  `mix format --check-formatted` (see the commit report).
