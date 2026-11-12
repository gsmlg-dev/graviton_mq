# S1a owned transport (broker plain TCP on Hell, client TLS on ex_ssl) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Broker plain-TCP listeners accept through `Hell`; the independent
client's TLS uses `ex_ssl`; architecture rules, packaging and documentation
follow. Broker TLS listeners stay on OTP `:ssl` (S1b, blocked on
gsmlg-dev/http_fetch#161).

**Architecture (owner decisions 2026-11-12):** Hell owns accept only. A
generic Hell handler (`GravitonMQ.Runtime.HellAcceptor`) starts the protocol
Connection under the existing `:connections` DynamicSupervisor, transfers
socket ownership to it and sends the existing `{:accepted, socket}` message,
then only monitors the Connection (read timeout `:infinity`). Connection and
Writer code, input arming and backpressure are unchanged, so no drain-pause
workaround (#162) is needed. Because Hell closes a handler's socket when the
handler exits, **a listener failure now closes that listener's live
connections** (accepted by the owner); the owner, sessions with expiry and
retained state survive. The listener GenServer keeps child id
`{Listener, id}` and `info/1`.

**Tech Stack:** Elixir 1.18.4 / OTP 28.5.0.1 (`.tool-versions`); `hell`
`== 1.0.0-rc.13` (runtime), `ex_ssl` `== 1.0.0-rc.13` (client); first
third-party dependencies and first `mix.lock`.

**Rules for every task:** work on `main` in this checkout; no branch/worktree;
no commit until Task 8; never weaken an assertion; zero warnings; run
`mix format` on touched files.

---

## File map

| File | Change |
| --- | --- |
| `apps/graviton_mq_runtime/mix.exs` | add `hell` |
| `apps/graviton_mq_client/mix.exs` | add `ex_ssl`; `extra_applications: [:public_key]` |
| `mix.lock` | new, tracked |
| `scripts/stage_hex.exs:41-45` | pass third-party Hex requirements through |
| `apps/graviton_mq/lib/graviton_mq/architecture.ex` | Hell/SSL allow-lists; client forbids `:ssl` |
| `apps/graviton_mq/test/graviton_mq/architecture_test.exs` | rule tests |
| `apps/graviton_mq_runtime/lib/graviton_mq/runtime/hell_acceptor.ex` | new shared Hell handler |
| `apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/listener.ex` | `:tcp` via Hell |
| `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/listener.ex` | `:tcp` via Hell |
| `apps/graviton_mq/test/graviton_mq/mqtt_connection_test.exs:141-167` | new listener-failure semantics |
| `apps/graviton_mq_runtime/test/hell_acceptor_test.exs` | new |
| `apps/graviton_mq_client/lib/graviton_mq/client/transport.ex` | `:ssl` → `SSL` |
| `apps/graviton_mq_client/test/tls_transport_test.exs` | new local TLS test |
| `docs/adr/0009-owned-network-stack.md`, `AGENTS.md`, `docs/CAPABILITIES.md`, design spec | documentation |

---

### Task 1: Dependencies, lockfile and Hex staging

**Files:** `apps/graviton_mq_runtime/mix.exs`, `apps/graviton_mq_client/mix.exs`,
`mix.lock` (new), `scripts/stage_hex.exs:41-45`

- [ ] **Step 1:** In `apps/graviton_mq_runtime/mix.exs` deps, after the umbrella
deps, add `{:hell, "== 1.0.0-rc.13"}`. Keep `extra_applications: [:ssl]`
(broker TLS still uses OTP `:ssl`).
- [ ] **Step 2:** In `apps/graviton_mq_client/mix.exs` deps add
`{:ex_ssl, "== 1.0.0-rc.13"}` and change `application/0` to
`[extra_applications: [:public_key]]` (the client keeps calling
`:public_key.cacerts_get/0` and `pkix_verify_hostname_match_fun/1`; OTP `:ssl`
is no longer used by the client).
- [ ] **Step 3:** `mix deps.get` from the umbrella root. Expected: `hell`,
`ex_ssl`, `telemetry` fetched; `mix.lock` created. `mix compile --warnings-as-errors`
passes.
- [ ] **Step 4:** In `scripts/stage_hex.exs` replace the dependency rewrite at
lines 41-45 so umbrella deps are pinned and Hex deps pass through unchanged:

```elixir
      Enum.map(config[:deps], fn
        {dependency, [in_umbrella: true]} ->
          true = dependency in @apps
          {dependency, "== #{version}"}

        {dependency, requirement} when is_atom(dependency) and is_binary(requirement) ->
          false = dependency in @apps
          {dependency, requirement}
      end)
```

(Read the surrounding code first and keep its variable names.)
- [ ] **Step 5:** Run `mix run scripts/stage_hex.exs` as CI does (check the
exact invocation in `.github/workflows/test.yml`). Expected: success; the staged
`graviton_mq_runtime/mix.exs` lists `{:hell, "== 1.0.0-rc.13"}` and the staged
client lists `{:ex_ssl, "== 1.0.0-rc.13"}`. Do not run `build_hex.exs` yet (Task 8).
- [ ] **Step 6:** Checkpoint, no commit.

### Task 2: Architecture rules for owned transport modules

Rules (AGENTS.md "source-level rules", design §3.4):
- `Hell` / `Hell.*` may be referenced only from `graviton_mq_runtime`.
- `SSL` / `SSL.*` (ex_ssl) only from `graviton_mq_runtime` and `graviton_mq_client`.
- Protocol apps keep rejecting all transport modules; add `Hell` and `SSL` to
  that list.
- Core, storage and `graviton_mq` must not reference `Hell`/`SSL`.

**Files:** `apps/graviton_mq/lib/graviton_mq/architecture.ex`,
`apps/graviton_mq/test/graviton_mq/architecture_test.exs`

- [ ] **Step 1: Failing tests.** Read the existing tests at
`architecture_test.exs:157-180` and `:552-559` to reuse their reference-building
helpers, then add tests asserting:
  1. a reference from `graviton_mq_runtime` to `Hell.Transports.TCP` and to
     `SSL` has no violation;
  2. a reference from `graviton_mq_client` to `SSL` and `SSL.Socket` has no
     violation, while one to `Hell` is a violation naming the target module;
  3. references from `graviton_mq_core`, `graviton_mq_storage` and
     `graviton_mq` to `Hell.Socket` and to `SSL` are violations;
  4. the AMQP 1.0 and MQTT transport-module lists also reject `Hell` and `SSL`
     (extend the existing list-driven tests).
- [ ] **Step 2:** `mix test apps/graviton_mq/test/graviton_mq/architecture_test.exs`
from the umbrella root — expected: the new tests fail.
- [ ] **Step 3: Implement.** In `architecture.ex`:
  - add `"Elixir.Hell"` and `"Elixir.SSL"` to `@transport_namespace_prefixes`
    and `Hell`, `SSL` to `@transport_modules` (protocol apps);
  - add `@owned_transport_allowed %{"Elixir.Hell" => [:graviton_mq_runtime], "Elixir.SSL" => [:graviton_mq_runtime, :graviton_mq_client]}`;
  - add an `owned_transport_violation/1` beside `transport_violation/1`
    (`:194-205`) that matches a target module equal to a key or under
    `key <> "."` and returns a new violation tuple when the source app is not
    listed; wire it into `reference_violations/1` (`:177-180`), extend
    `@type violation` and `format_violation/1` (`:93-98`, `:152-175`) with a
    message like `"graviton_mq_client may not reference Hell (owned transport allowed only in graviton_mq_runtime)"`.
  - Avoid double-reporting protocol apps (their generic transport violation
    already fires): skip the owned-transport check for `@protocol_apps`.
- [ ] **Step 4:** tests pass; `mix graviton_mq.check_architecture` passes (no
lib code references Hell/SSL yet).
- [ ] **Step 5:** Checkpoint.

### Task 3: Shared Hell acceptor and MQTT plain-TCP listener

**Files:** create `apps/graviton_mq_runtime/lib/graviton_mq/runtime/hell_acceptor.ex`,
create `apps/graviton_mq_runtime/test/hell_acceptor_test.exs`, modify
`apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/listener.ex`, modify
`apps/graviton_mq/test/graviton_mq/mqtt_connection_test.exs:141-167`.

Hell facts (rc.13): `Hell.start_link(opts)` returns a linked Supervisor;
options `port`, `transport_module`, `transport_options`, `handler_module`,
`handler_options`, `num_acceptors` (default 100), `num_connections`,
`read_timeout`, `shutdown_timeout`; `Hell.listener_info(sup)` →
`{:ok, {ip, port}} | :error`; `Hell.stop(sup, wait)`. `%Hell.Socket{socket: raw, transport_module: mod}`;
`mod.controlling_process/2` exists on Hell transports. Every handler
termination closes the socket (`Hell.Handler.do_socket_close/2`). Hell calls
`setopts(active: :once)` after `handle_connection/2` returns `{:continue, …}`;
because ownership has moved, those messages go to the Connection.

- [ ] **Step 1: Write the acceptor**

```elixir
defmodule GravitonMQ.Runtime.HellAcceptor do
  @moduledoc false
  # Hell owns accept only. The protocol Connection receives socket ownership
  # and keeps input arming, backpressure and close handling. This handler
  # lives exactly as long as that Connection so Hell's connection accounting
  # and shutdown cover it; Hell closes the socket when the handler exits.
  use Hell.Handler

  @impl Hell.Handler
  def handle_connection(socket, listener) do
    Process.flag(:sensitive, true)
    %{connections: connections, child: child} = listener.()

    case DynamicSupervisor.start_child(connections, child) do
      {:ok, connection} -> hand_off(socket, connections, connection)
      _refused -> {:close, nil}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, {socket, monitor}),
    do: {:stop, :normal, {socket, monitor}}

  defp hand_off(socket, connections, connection) do
    case socket.transport_module.controlling_process(socket.socket, connection) do
      :ok ->
        monitor = Process.monitor(connection)
        send(connection, {:accepted, socket.socket})
        {:continue, monitor, {:persistent, :infinity}}

      {:error, _reason} ->
        _ = DynamicSupervisor.terminate_child(connections, connection)
        {:close, nil}
    end
  end
end
```

`listener` is a zero-arity function supplied by the listener (Step 3) that
returns the connection spec at accept time, so configuration never appears in
Hell's process arguments or crash reports. If `use Hell.Handler` already
defines a catch-all `handle_info/2` that conflicts, follow the compiler and
Hell docs (`hell/lib/hell/handler.ex` `add_handle_info_fallback`) and record
what was needed.

- [ ] **Step 2: Focused failing test** —
`apps/graviton_mq_runtime/test/hell_acceptor_test.exs`. It starts a
`DynamicSupervisor` and a `Hell` server whose `handler_options` returns
`%{connections: sup, child: {Probe, self()}}`, where `Probe` is a test
GenServer (`restart: :temporary`) that forwards `{:accepted, socket}` and any
`{:tcp, s, data}` / `{:tcp_closed, s}` to the test process and on
`{:accepted, socket}` calls `:inet.setopts(socket, active: :once)`. Assert:
  1. a `:gen_tcp` client connects; the test receives `{:accepted, raw}`;
     `:erlang.port_info(raw, :connected)` is `{:connected, probe_pid}`;
  2. bytes sent by the client arrive as `{:tcp, ^raw, "hello"}` from the probe
     (ownership moved, data not lost);
  3. stopping the probe (`GenServer.stop`) makes the client observe
     `{:error, :closed}` from `:gen_tcp.recv(client, 0, 1_000)` and Hell's
     `Hell.connection_pids/1` eventually returns `{:ok, []}`;
  4. with `max_children: 0` on the DynamicSupervisor the client connection is
     closed (`{:error, :closed}`) and no probe starts;
  5. `Hell.stop(server, 1_000)` closes an accepted connection whose probe is
     still alive.
Run it: expected failures until Step 1 compiles (module missing) — write the
test before the module if following TDD strictly, then add Step 1.

- [ ] **Step 3: MQTT listener `:tcp` path.** In `mqtt/listener.ex`:
  - keep `:tls` exactly as today;
  - for `:tcp`, instead of `:gen_tcp.listen`, start Hell in `init/1`:

```elixir
      :tcp ->
        listener_pid = self()

        Hell.start_link(
          port: listener.port,
          transport_module: Hell.Transports.TCP,
          transport_options: [
            ip: listener.ip,
            backlog: listener.backlog,
            reuseaddr: true,
            buffer: 16_384,
            recbuf: 32_768,
            send_timeout: config.send_timeout,
            send_timeout_close: true
          ],
          handler_module: GravitonMQ.Runtime.HellAcceptor,
          handler_options: fn -> GenServer.call(listener_pid, :connection_spec) end,
          num_acceptors: 4,
          read_timeout: :infinity
        )
```

    (`Hell.Transports.TCP` enforces `mode: :binary` and `active: false` and
    defaults to raw packets; these are the same socket options the current
    `:gen_tcp.listen` call uses. Accepted sockets inherit `send_timeout`, which
    the Writers rely on. Verify in Hell's `transports/tcp.ex` that none is
    rejected or overridden and record the result.)
  - port: `{:ok, {ip, port}} = Hell.listener_info(server)`; store the Hell pid
    in `state.socket` and keep `info = %{id, transport: :tcp, ip, port}`.
  - Hell start failure → `{:stop, {:listener_setup_failed, :tcp}}` as today
    (so `GravitonMQ.start_link/1` still fails explicitly on bind errors).
  - `handle_continue(:wire)` unchanged; for `:tcp` do not send `:accept`.
  - add `handle_call(:connection_spec, _from, state)` replying
    `%{connections: state.connections, child: {Connection, {state.config, state.broker, :tcp}}}`
    (use exactly the start argument the current accept path passes).
  - `handle_info({:EXIT, pid, _reason}, %{socket: pid} = state)` →
    `{:stop, :listener_closed, state}`.
  - `terminate/2` for `:tcp`: `Hell.stop(state.socket, 1_000)` when alive
    (wrap in `catch :exit, _ -> :ok`); `:tls` closes its listen socket as today.
  - Remove only the now-dead `:tcp` accept code; keep the `:tls` loop.

- [ ] **Step 4: Update the listener-failure test** — replace the test
"listener failure preserves the owner, retained state and existing connection"
(`mqtt_connection_test.exs:141-167`) with:

```elixir
  test "listener failure closes its connections and restarts on a new port with the owner and retained state" do
    instance = broker()
    %{owner: owner, port: port} = status(instance)
    socket = connected(port, "listener-casualty")
    publish_retained(instance, owner)
    mqtt = mqtt_supervisor(instance)
    connections = child(mqtt, :connections)
    [{_id, reader, _type, _modules}] = DynamicSupervisor.which_children(connections)
    reader_monitor = Process.monitor(reader)
    listener = child(mqtt, {Listener, :tcp})
    listener_monitor = Process.monitor(listener)

    capture_log(fn ->
      Process.exit(listener, :kill)
      assert_receive {:DOWN, ^listener_monitor, :process, ^listener, :killed}, 1_000
      # Hell closes the sockets of the listener's handlers (accepted 2026-11-12).
      assert_receive {:DOWN, ^reader_monitor, :process, ^reader, _reason}, 1_000
      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)

      assert eventually(fn ->
               replacement = find_child(mqtt, {Listener, :tcp})
               is_pid(replacement) and replacement != listener
             end)

      assert eventually(fn ->
               match?(%{owner: ^owner, connections: 0, retained: 1}, status(instance))
             end)

      %{port: new_port} = status(instance)
      replacement_socket = connected(new_port, "new-listener")
      assert_ping(replacement_socket)
      assert %{owner: ^owner, connections: 1, retained: 1} = status(instance)
    end)
  end
```

- [ ] **Step 5: Verify** — `mix test` in `apps/graviton_mq_runtime` and
`mix test apps/graviton_mq/test/graviton_mq/mqtt_connection_test.exs` plus all
`apps/graviton_mq/test/graviton_mq/mqtt_*` files from the root. Expected
0 failures (peer-tagged tests excluded locally). Any other failure: report
verbatim, do not edit unrelated tests.
- [ ] **Step 6:** `mix graviton_mq.check_architecture` passes. Checkpoint.

### Task 4: AMQP 1.0 plain-TCP listener on Hell

**Files:** `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/listener.ex`

- [ ] **Step 1:** Apply the same change as Task 3 Step 3 to the AMQP listener,
keeping its differences: `Process.flag(:sensitive, true)`, the rescue to
`{:stop, :listener_setup_failed}`, `Map.drop(config, [:authorize, :listeners])`,
and its existing Connection start argument in `:connection_spec`. `:tls`
unchanged.
- [ ] **Step 2:** Search the AMQP tests for a listener-failure test asserting
that connections survive (`grep -rn "Listener" apps/graviton_mq/test/graviton_mq/amqp10_*`).
If one exists, update it to the Task 3 semantics (closed connection, restart
on a new port, queues/owner state retained) and report the change; otherwise
add one modelled on Task 3 Step 4 to `amqp10_runtime_test.exs` using that
file's helpers.
- [ ] **Step 3: Verify** — `mix test` in `apps/graviton_mq_runtime`, then
`mix test` in `apps/graviton_mq`. Expected 0 failures (68 peer tests
excluded). `mix graviton_mq.check_architecture` passes. Checkpoint.

### Task 5: Client TLS on ex_ssl

**Files:** `apps/graviton_mq_client/lib/graviton_mq/client/transport.ex`,
create `apps/graviton_mq_client/test/tls_transport_test.exs`,
`apps/graviton_mq/lib/graviton_mq/architecture.ex` (+ test)

ex_ssl client supports every option this module passes:
`:binary`, `packet: 0`, `active: false | :once`, `nodelay`, `send_timeout`,
`send_timeout_close: true`, `verify: :verify_peer`, `cacerts`/`cacertfile`,
DNS `server_name_indication`, `customize_hostname_check: [match_fun: fun]`
(ex_ssl `docs/COMPATIBILITY.md` "Connection options"). Messages keep the
`{:ssl, socket, data}`, `{:ssl_closed, socket}`, `{:ssl_error, socket, reason}`
shapes; no `{:ssl_passive, _}` is emitted. Unsupported caller options return
`{:error, {:options, reason}}` at connect.

- [ ] **Step 1: Failing local test** —
`apps/graviton_mq_client/test/tls_transport_test.exs`:
  - `Code.require_file("support/mosquitto.exs", __DIR__)`; in `setup`, create a
    temp dir and call
    `GravitonMQ.Client.Test.Mosquitto.certificates!(%{openssl: System.find_executable("openssl")}, dir)`
    (returns `%{certificate, private_key, ca_certificate, untrusted}`);
  - test server: OTP `:ssl.listen(0, [:binary, active: false, ip: {127,0,0,1}, versions: [:"tlsv1.3"], certfile: …, keyfile: …])`
    with a task that `transport_accept` + `handshake`, echoes one `recv` and
    then waits; OTP `:ssl` is an independent TLS peer here (test code only);
  - config map: `%{host: "localhost", port: port, transport: :tls, tls_options: [cacertfile: ca], connect_timeout: 2_000, send_timeout: 1_000}`;
  - assertions:
    1. `{:ok, socket} = Transport.connect(config)`; `match?(%SSL.Socket{}, socket)`;
       `:ok = Transport.send(:tls, socket, "ping")`;
       `:ok = Transport.active_once(:tls, socket)`;
       `assert_receive {:ssl, ^socket, "ping"}` (echo);
       `:ok = Transport.close(:tls, socket)`;
    2. `tls_options: [cacertfile: untrusted]` → `{:error, _}` and the server
       task observes a failed handshake;
    3. `host: "127.0.0.1"` with the localhost-only certificate (SAN
       `DNS:localhost`) → `{:error, _}` (hostname verification);
    4. server closes after echo → client receives `{:ssl_closed, ^socket}` or
       `{:ssl_error, ^socket, _}` after `active_once`.
  Expected now: test 1 fails (`socket` is an OTP `:sslsocket` record, not
  `%SSL.Socket{}`).
- [ ] **Step 2: Implement** — in `transport.ex` replace `:ssl.connect/4`,
`:ssl.send/2`, `:ssl.setopts/2`, `:ssl.close/1` with `SSL.connect/4`,
`SSL.send/2`, `SSL.setopts/2`, `SSL.close/1`. Update the module comment
("outgoing OTP transport" → TCP via `:gen_tcp`, TLS via ex_ssl). Keep
`tls_options/2` and `valid_tls_options?/1` unchanged.
- [ ] **Step 3:** `mix test` in `apps/graviton_mq_client`. Expected 0 failures
(43 peer tests excluded). Check that `mqtt.ex` and `amqp10.ex` message clauses
need no change (they match `{:ssl, socket, data}` on the stored socket term);
report if any clause references OTP-only shapes.
- [ ] **Step 4: Rule** — add to `architecture.ex` a check that
`graviton_mq_client` may not reference `:ssl` (OTP TLS), with a test in
`architecture_test.exs`; `mix graviton_mq.check_architecture` passes.
- [ ] **Step 5:** Checkpoint.

### Task 6: Documentation

**Files:** create `docs/adr/0009-owned-network-stack.md`; modify `AGENTS.md`
(`:16`, `:207-208`), `docs/CAPABILITIES.md`, design spec §3.3/§3.5.1.

- [ ] **Step 1: ADR 0009** (follow `docs/adr/0008-local-durability-profiles.md`
format): status Accepted 2026-11-12 by the owner; context (owner's network
stack, spike, #161/#162); decision (Hell accept-only for broker plain TCP;
ex_ssl for client TLS; broker TLS stays on OTP `:ssl` until #161 is fixed;
TLS 1.3-only broker once S1b lands; first third-party deps pinned with `==`);
consequences (listener failure closes its connections; client unsupported TLS
options fail at connect with `{:options, _}`; architecture allow-lists;
packaging resolves Hex deps).
- [ ] **Step 2: AGENTS.md** — line 16: networking/TLS primitives sentence now
also names the owner-authorized `hell` and `ex_ssl` packages (ADR 0009);
lines 207-208: keep the Phoenix/Ra/Khepri/Ranch/Bandit prohibition and add
"except the owner-authorized network packages in ADR 0009". Add to the
source-level rules list: "`Hell` only in runtime; ex_ssl `SSL` only in runtime
and client; the client must not reference OTP `:ssl`."
- [ ] **Step 3: Design spec** — §3.3 and §3.5.1 decision 1: replace the
"byte pump" layout with the accepted accept-only hand-off, and record that
#162's workaround is not needed for this layout; note the listener-failure
semantics decision.
- [ ] **Step 4: CAPABILITIES** — in the MQTT broker and AMQP broker rows add
"2026-11-12 (unreleased): plain-TCP listeners accept through `hell`
1.0.0-rc.13; a listener failure closes its connections; independent-peer
requalification NOT RUN (requires CI)"; in both client rows add "TLS through
`ex_ssl` 1.0.0-rc.13; local TLS 1.3 transport test PASS; Artemis/Mosquitto TLS
requalification NOT RUN (requires CI)".
- [ ] **Step 5:** Checkpoint.

### Task 7: Coordinator review of the integrated change

- [ ] Inspect the full diff; confirm no lib module outside runtime references
`Hell` and none outside runtime/client references `SSL`; confirm broker TLS
code paths are byte-for-byte unchanged (`git diff` of the `:tls` branches).

### Task 8: Verification and commit

- [ ] **Step 1:** Full AGENTS.md sequence from the umbrella root (all eight
commands). Expected 0 failures, zero warnings, architecture pass, no cycles.
- [ ] **Step 2:** Packaging, as CI `test.yml`: `mix run scripts/stage_hex.exs`
then `mix run scripts/build_hex.exs` (needs network for Hex). Expected: seven
archives and three consumers pass; the client-only consumer resolves
`ex_ssl` but not `hell`.
- [ ] **Step 3:** Record independent-peer interop as NOT RUN locally (no
Mosquitto/Paho/Artemis/Proton on this host); it runs in CI after a push, which
is not authorized yet.
- [ ] **Step 4:** Commit on `main` (owner authorized commits 2026-11-12), no
Co-Authored-By trailer, conventional messages, e.g.
`feat(runtime): accept broker plain TCP through Hell` and
`feat(client): use ex_ssl for client TLS`, plus `docs:` for ADR/AGENTS. Do not push.
