# S3 — AMQP 1.0 Client Reconnect and Asynchronous Broker Manager Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** (A) The running `GravitonMQ.AMQP10Client` owner opens a new AMQP Connection after transport loss under a bounded MQTT-style retry policy, re-authenticates, and restores the Sessions and Links the host declared, reporting every result. (B) The broker Manager (`GravitonMQ.Runtime.AMQP10.Broker`) stops making blocking calls to queue owners, so an unresponsive queue delays only its own Links and status answers within 1 s.

**Architecture:** Part B adds a pure FIFO tracker `GravitonMQ.Runtime.AMQP10.QueueRequests`, `{:request, _}`/`{:result, _}` exchange wrappers and async helpers in `Queue`, and converts the Manager's queue calls to `:gen_server.send_request/2` with one deadline timer per queue. At a deadline, an atomic take-back classifies the request as not executed, known or uncertain. Part A adds two pure client modules, `GravitonMQ.Client.AMQP10.Reconnect` (backoff and classification) and `GravitonMQ.Client.AMQP10.Intent` (bounded declared topology and restoration progress). The shell `GravitonMQ.Client.AMQP10` gains owner phases, a token-fenced retry timer, a fresh generation-fenced `Endpoint`/`Stream`/SASL machine per attempt, and internal restoration operations. The pure `graviton_mq_amqp10` layer is unchanged.

**Tech Stack:** Elixir 1.18.4 / OTP 28 umbrella (`.tool-versions`), ExUnit, raw `:gen_tcp` and OTP `:ssl` local peers, Apache Artemis 2.41.0 + Qpid Proton 0.40.0 (CI only, through `scripts/interop/run.py`).

**Input spec:** [`docs/superpowers/specs/2026-11-13-s3-amqp-reconnect-manager-design.md`](../specs/2026-11-13-s3-amqp-reconnect-manager-design.md), approved 2026-11-13. Owner decisions: Q1, the client restores declared Sessions/Links from bounded in-memory intent. Q5, an enqueue that definitively never executed is settled `Released`. Q2–Q4, Q6 and Q7 take the §9 recommended defaults: credit is not auto-restored; the retry policy mirrors MQTT (`max_attempts` 0..100, default 0, exponential, no jitter); `amqp:connection:forced` and an unrequested error-less Close are retried; status gains a new `:unresponsive` value; Artemis reconnect evidence uses the proxy (R1–R6, F1–F3).

---

## Execution rules (read before Task 0)

### Parallel checkout with S1b

S3 runs **in parallel with S1b (broker TLS on `ex_ssl`) in the same checkout on `main`**.

1. **S3 write scope.** Tasks 0–11 may create or modify only:
   - `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex` (the Manager);
   - `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue.ex`;
   - new `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue_requests.ex`;
   - `apps/graviton_mq_runtime/test/amqp10/queue_test.exs` (additive), new `apps/graviton_mq_runtime/test/amqp10/queue_requests_test.exs`;
   - `apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs`, `apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs`, new `apps/graviton_mq/test/graviton_mq/amqp10_manager_requests_test.exs`, new `apps/graviton_mq/test/support/amqp10_stand_in_queue.exs`;
   - `apps/graviton_mq_client/lib/graviton_mq/client/amqp10.ex`, `apps/graviton_mq_client/lib/graviton_mq/client/amqp10/config.ex`, new `apps/graviton_mq_client/lib/graviton_mq/client/amqp10/{reconnect,intent}.ex`, `apps/graviton_mq_client/lib/graviton_mq/amqp10_client.ex` (moduledoc only);
   - new `apps/graviton_mq_client/test/{amqp10_reconnect_test,amqp10_intent_test,amqp10_artemis_reconnect_test}.exs`, new `apps/graviton_mq_client/test/support/amqp_cut_proxy.exs`, additive changes to `apps/graviton_mq_client/test/{privacy_test,tls_transport_test}.exs` and `apps/graviton_mq_client/test/support/amqp_peer.py` (new `drain` operation only);
   - this plan's execution log (bottom of this file).
2. **S1b-owned, never edited by S3:** `runtime/amqp10/{listener,connection,writer,config}.ex`, `runtime/hell_handoff*.ex`, `runtime/mqtt/**`, `client/transport.ex`, `mix.exs`, `mix.lock`, any `apps/graviton_mq_amqp10/**` module. This plan needs **no** change to an S1b-owned file (the status facade `runtime/amqp10.ex` also stays unchanged). If an executor finds such a change unavoidable, it stops and hands a description to the coordinator. It does not edit the file.
3. **Coordinator-serialized files**, edited only in Tasks C1–C4 after S1b has stopped writing them: `docs/CAPABILITIES.md`, `docs/ACTIVE_SCOPE.md`, `AGENTS.md`, `docs/AMQP10_CLIENT.md`, `docs/AMQP10_BROKER.md`, `docs/FAILURE_MODEL.md`, `docs/DELIVERY_SEMANTICS.md`, `docs/qualification/*`, `scripts/interop/*`, `.github/workflows/*`.
4. **Isolated build (verified in S2/S4).** `MIX_BUILD_PATH` does **not** work: it drops the env suffix, so dev and test recompile each other. Every mix command uses
   `env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp`.
   `TMPDIR` isolates `System.tmp_dir!/0` (several tests name directories with `System.unique_integer/1`). Never run a bare `mix`.
5. **Formatting.** Never run umbrella-wide `mix format` before Task C4. Format only the files the task touched:
   `cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format <paths>`.
6. **Commits.** One commit per task on `main` is authorized: conventional message, **no `Co-Authored-By` trailer, no "Generated with" line**. Never push; the coordinator pushes. Never `git add -A`/`.`. Commit only your paths:
   `git add -- <paths> && git commit --only -m "<message>" -- <paths>`.
   zsh does not word-split `$P`. Spell the paths out, or use an array (`paths=(a b); git commit --only -m m -- $paths`). If `.git/index.lock` exists, wait and retry. Never delete it.
7. **Out-of-scope breakage.** If a compile or test fails because of S1b's in-progress files, do not edit them. Record the failure in the execution log, wait, and rerun.
8. **Peers.** Proton/Java/Artemis are not installed locally. `:artemis_interop`/`:artemis_tls_interop`/`:proton_interop` tests are excluded locally by the existing `test_helper.exs` files and run only in CI. Never claim CI evidence before a passing run id is recorded.

### Test rules learned in S1a/S2 (apply to every new test)

- After `Process.monitor(pid)`, make a **synchronous call to `pid`** before any stimulus from another process or a port. For the client, use `GravitonMQ.AMQP10Client.status/1`. For the Manager, use `GenServer.call(manager, :sync)` (it replies `{:error, :unsupported_command}`). For a Queue, use `Queue.status/1`.
- `:sys.suspend/1` blocks only ordinary messages. System messages, `GenServer.stop/1` and a trapped parent `EXIT` still reach the process. A suspended queue therefore never *takes* a request, so suspension exercises only the **not executed** branch. The **taken but unanswered** branch uses the stand-in queue (Task 4). Never use `:erlang.suspend_process/1`. Always `:sys.resume/1` in `after`.
- Suspending a broker Connection also requires suspending its Hell handlers (see `amqp10_boundaries_test.exs` `handlers/1`).
- Do not inspect the Manager with `:sys.get_state/1` (sensitive and redacted); use `GravitonMQ.amqp10_status/1`, `Broker.status/1` and `Queue.status/1`. Client state may be inspected with `:sys.get_state/1` (S2 practice).
- At-least-once only. An uncertain outcome stays explicit (`{:unknown, _}`, a closed Connection, or a fence). Never assert or implement a fake success.
- Credentials, provider results, hosts and payloads never appear in state dumps, status, events, logs or crash reports.

### Command shorthands used below

`ROOT` = `/home/gao/Workspace/gsmlg-dev/graviton_mq`. Commands are always written in full, for example:

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10/queue_requests_test.exs
```

Umbrella-level `apps/graviton_mq` tests must run from the umbrella root, because an app-directory run cannot load the client app (S2 log):

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs
```

### Streams

- **Stream B (broker):** Tasks 1 → 2 → 3 → 4 → 5 → 6 (`graviton_mq_runtime` + umbrella AMQP runtime tests).
- **Stream A (client):** Tasks 7 → 8 → 9 → 10 → 11 (`graviton_mq_client`).
- The streams share no file and may run concurrently, one agent each. Task 0 runs once before both. Tasks C1–C4 are coordinator-serialized, after both streams and after S1b has finished its shared-file edits.

### Decisions taken while planning (spec deltas; also listed in the final report)

- **D1 Disconnect reason of a locally initiated close.** The Endpoint reports a Close that the client itself initiated (receive idle timeout, open timeout, protocol error, host `close/1`) as `{:disconnected, :normal}` (the peer answered), `:close_timeout` (the peer did not answer) or `:closed`. The reason is never `:idle_timeout`/`:open_timeout` (`endpoint.ex` `peer_close/2`, `expired/3`). The shell therefore records a **close cause** whenever the Endpoint enters `:closing`: `:host_close` for the `:close` command, `:idle_timeout` for the `:receive_idle` wake, `:open_timeout` for the `:open` wake, and `:protocol` for anything else. `Reconnect.classify(reason, cause)` decides from both, so §3.3's "`:idle_timeout` → retry" and "unrequested error-less Close → retry" can be implemented without touching the pure layer. The `{:disconnected, reason}` event keeps the Endpoint's reason unchanged.
- **D2 Status keys.** `status/1` already has `phase` (the Endpoint phase, asserted as `:closed` by existing tests and Artemis A3). It keeps that meaning. The owner phase is a new key, `owner: :connecting | :active | :restoring | :offline | :stopped`. The spec's F1 criterion "`phase: :stopped`" becomes `owner: :stopped`. The other §3.7 keys are added: `attempt`, `next_retry_ms`, `intent: %{sessions, links}` and `restoring`. `generation` already exists.
- **D3 Host commands per owner phase.** In `:offline`, and in `:connecting` with `generation > 0`, every command returns `{:error, :disconnected}`, except `close/1`. During the **initial** connection (`generation == 0`) commands keep today's Endpoint answers (`{:error, :not_connected}`), so that `max_attempts: 0` is byte-for-byte unchanged. In `:stopped` the closed Endpoint answers `{:error, :closed}`, as today. `close/1` in `:offline`/reconnecting replies `{:ok, ref}` and immediately sends `{:operation, ref, {:ok, :closed}}`; the owner becomes `:stopped`, the process stays alive (MQTT parity).
- **D4 Classification additions** (spec table gaps). `{:connect, r}` is retried only for `r in [:econnrefused, :econnreset, :econnaborted, :ehostunreach, :enetunreach, :nxdomain, :timeout, :etimedout, :closed]`. Any other connect failure (TLS alerts, `:transport_error`, option errors) is terminal. `{:write_failed, {:error, {:encode, _, _}}}` is terminal; any other `{:write_failed, _}` is retried, being transport-level. `:owner_stopped` and `{:decode_error, _}` are terminal.
- **D5 Queue DOWN** is classified like a deadline, by take-back (`{:request, _}` = not executed, `{:result, r}` = known, missing = uncertain). A queue crash restarts the Manager anyway (`rest_for_one`), so this path only has to stay explicit.
- **D6 Expired and abandoned requests stay counted.** After a deadline or a fence, an entry moves to a `waiting` set. It stays in the request id collection and counts against `max_queue_requests` until its late reply or its queue DOWN arrives, and that reply deletes its exchange slot. This keeps the per-queue exchange table and the reply collection bounded without undocumented `gen_server` internals.
- **D7 Cap-full behaviour, per kind** (spec: "refuses new Link admission … and defers pump work"):
  - `register` rejects the Link (`amqp:resource-limit-exceeded`);
  - `enqueue` settles `Released`, because the request was not executed;
  - pump `status` and `claim` are deferred, and a pump is scheduled when space frees;
  - `settle`, unfit `settle`, cleanup `disconnect` and `watch_ack` fence the queue, because they cannot be deferred without diverging;
  - restore requests re-probe.
- **D8 Pump status.** No status cache. One in-flight status request per queue collects waiting route keys. On its reply every waiter is continued with that status through `continue_route/3`. `pump_route/2` stays the wave entry point, so the boundaries test that traces `Broker.pump_route/2` keeps counting waves exactly.
- **D9 Claim reply.** A successful claim sets `pending_delivery` and schedules a pump. The next wave re-reads Connection capacity (the bounded 25 ms query) and sends. The capacity read before the claim may be stale, so it is not reused for the send.
- **D10 Fence ordering.** `fence/2` classifies abandoned `enqueue` entries (disposition or Connection close) **before** it detaches the queue's Links, so a `Released` disposition reaches the Connection before its Detach. `fence/2` rejects `:registering` routes; it does not detach them, because their attach was never accepted.
- **D11 B6 trigger.** A client cannot detach a Link whose attach is still pending. B6 therefore removes the route by **stopping the client** (Connection DOWN) during `:registering`, which is the same route-removal path.
- **D12 Stand-in queue.** Spec B4's stand-in **forwards** every request to a real `Queue` it owns, except the request kinds it is told to hold. A held request is taken and never answered, and the stand-in stops taking later requests until released. Because every request goes through the stand-in, the real queue's controller checks see one consistent caller. `Queue.request/2` becomes public (`@doc false`) for this.
- **D13 Restoration details.**
  - Links of a Session whose restoration failed are reported `{:restored, {:link, sid, id}, {:error, :session_not_restored}}` and dropped.
  - A restoration interrupted by another transport loss is not reported; its entries stay declared and are retried on the next connection.
  - `{:restore_complete, %{restored: n, failed: m}}` follows **every** reconnect (`generation > 0`), also with empty intent, so the host has one deterministic signal. It never appears on the first connection.
  - A Link's "admission order" is its completion order in Intent.
  - An entry is removed on **any** peer Detach/End/session error of that item, not only one carrying an error: the peer closed it, so restoring it would resurrect it.
- **D14 Status deadline.** `Broker.status/1` computes its deadline as `now + 900 ms` at entry, so the whole call (Manager round trip plus concurrent queue requests) returns within the spec's 1,000 ms.
- **D15 Evidence placement.** The TLS reconnect test is added to `tls_transport_test.exs`, which has the OTP `:ssl` peer and certificates. Spec R2 compares the redelivered **body section**, not raw bytes: Artemis may change the header's delivery count on redelivery, as in the existing release test. The proxy reports `{:proxy_connection, n, monotonic_ms}`, a 3-tuple with the proxy's own timestamp for R4 gap measurement, plus `{:proxy_cut, n, :disposition}`, `{:proxy_transfer, n}` and `{:proxy_closed, n}`. R5 and F2 (PLAIN over verified TLS) also carry `@tag :artemis_tls_interop`. Spec R3's "Proton drain" adds a `drain` operation to `amqp_peer.py`.
- **D16 Pure modules.** `Reconnect` (policy) is a second new pure client module beside `Intent`. It is pure and has no process or clock, so the architecture rules allow it in the client app.

---

## File map

| File | Responsibility | Task |
| --- | --- | --- |
| `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue_requests.ex` (new) | Pure per-queue FIFO, head deadline, three-way classification, abandon-all, cap | 1 |
| `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue.ex` | Exchange wrappers; `send_request/3`, `classify/2`, `response/3`; public `request/2` | 2 |
| `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex` | Async paths 2–13, deadlines, fence step, `queue_requests`/`queue_fences`, caller-side status | 3–5 |
| `apps/graviton_mq_runtime/test/amqp10/{queue_requests_test,queue_test}.exs` | B8, exchange wrappers | 1, 2 |
| `apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs` | B1, B2, B5, register/cleanup isolation | 3–5 |
| `apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs` | B3 | 5 |
| `apps/graviton_mq/test/support/amqp10_stand_in_queue.exs` (new) | Stand-in queue and test parent supervisor | 6 |
| `apps/graviton_mq/test/graviton_mq/amqp10_manager_requests_test.exs` (new) | B4, B6, B7 | 6 |
| `apps/graviton_mq_client/lib/graviton_mq/client/amqp10/config.ex` | `reconnect:` option | 7 |
| `apps/graviton_mq_client/lib/graviton_mq/client/amqp10/reconnect.ex` (new) | Pure backoff and classification | 7 |
| `apps/graviton_mq_client/lib/graviton_mq/client/amqp10.ex` | Owner phases, retry loop, close cause, stale deliveries, restoration | 8, 9 |
| `apps/graviton_mq_client/lib/graviton_mq/client/amqp10/intent.ex` (new) | Pure declared topology and restoration progress | 9 |
| `apps/graviton_mq_client/lib/graviton_mq/amqp10_client.ex` | Moduledoc | 9 |
| `apps/graviton_mq_client/test/amqp10_reconnect_test.exs` (new) | Config, policy, shell loop, restoration, stopped owner, provider per attempt | 7–10 |
| `apps/graviton_mq_client/test/amqp10_intent_test.exs` (new) | Pure Intent | 9 |
| `apps/graviton_mq_client/test/{privacy_test,tls_transport_test}.exs` | Reconnect privacy; TLS per-attempt handshake and terminal trust failure | 10 |
| `apps/graviton_mq_client/test/support/amqp_cut_proxy.exs` (new) | Multi-accept cut proxy | 11 |
| `apps/graviton_mq_client/test/support/amqp_peer.py` | New `drain` operation | 11 |
| `apps/graviton_mq_client/test/amqp10_artemis_reconnect_test.exs` (new) | R1–R6, F1–F3 (CI only) | 11 |
| Docs, ledgers, CI evidence | Coordinator | C1–C4 |

---

## Task 0: Preflight — isolated build and baseline

**Files:** none modified.

- [ ] **Step 1: Create the isolated temp dir and confirm the build root**

```bash
mkdir -p /tmp/gmq-s3-tmp && cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp MIX_ENV=test mix run --no-compile --no-deps-check --no-archives-check --no-start -e 'IO.puts Mix.Project.build_path()'
```
Expected: `/tmp/gmq-build-s3/test`

- [ ] **Step 2: Baseline the touched suites**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix deps.get \
 && cd apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10 \
 && cd ../graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10_client_test.exs test/amqp10_sasl_client_test.exs test/tls_transport_test.exs test/privacy_test.exs \
 && cd ../.. && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs apps/graviton_mq/test/graviton_mq/amqp10_runtime_test.exs
```
Expected: every run ends with `0 failures`; interop tags are excluded. Record the summary lines and `git rev-parse HEAD` (`S3_BASE`) in the execution log. If anything fails before an S3 change, stop and report it as pre-existing. Do not fix it.

---

## Task 1 (spec T1, B8): Pure `QueueRequests` tracker

**Files:**
- Create: `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue_requests.ex`
- Test: `apps/graviton_mq_runtime/test/amqp10/queue_requests_test.exs`

- [ ] **Step 1: Write the failing test**

```elixir
defmodule GravitonMQ.Runtime.AMQP10.QueueRequestsTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Runtime.AMQP10.QueueRequests

  defp add!(requests, queue, kind, deadline) do
    assert {:ok, entry, requests} =
             QueueRequests.add(requests, queue, %{token: make_ref(), kind: kind}, deadline)

    {entry, requests}
  end

  test "entries resolve per queue in FIFO order and only the head deadline is armed" do
    {first, requests} = add!(QueueRequests.new(), :a, :settle, 100)
    {second, requests} = add!(requests, :a, :claim, 150)
    {other, requests} = add!(requests, :b, :status, 120)

    assert %{key: nil, route_token: nil, data: %{}, queue: :a, kind: :settle, deadline: 100} =
             first

    assert first.id < second.id
    assert QueueRequests.size(requests) == 3
    assert QueueRequests.next_deadline(requests, :a) == 100
    assert QueueRequests.next_deadline(requests, :b) == 120
    assert QueueRequests.pending?(requests, :a, :claim)
    refute QueueRequests.pending?(requests, :a, :status)

    assert {:pending, ^first, requests} = QueueRequests.resolve(requests, :a, first.id)
    assert QueueRequests.next_deadline(requests, :a) == 150
    assert {:pending, ^second, requests} = QueueRequests.resolve(requests, :a, second.id)
    assert QueueRequests.next_deadline(requests, :a) == nil
    assert {:unknown, ^requests} = QueueRequests.resolve(requests, :a, second.id)
    assert {:pending, ^other, requests} = QueueRequests.resolve(requests, :b, other.id)
    assert QueueRequests.size(requests) == 0
  end

  test "expire returns only due head entries, which stay counted until their late reply" do
    {first, requests} = add!(QueueRequests.new(), :a, :settle, 100)
    {second, requests} = add!(requests, :a, :claim, 200)
    assert {[], ^requests} = QueueRequests.expire(requests, :a, 99)
    assert {[^first], requests} = QueueRequests.expire(requests, :a, 100)
    assert QueueRequests.next_deadline(requests, :a) == 200
    assert QueueRequests.size(requests) == 2
    assert {:waiting, ^first, requests} = QueueRequests.resolve(requests, :a, first.id)
    assert QueueRequests.size(requests) == 1
    assert {:pending, ^second, requests} = QueueRequests.resolve(requests, :a, second.id)
    assert QueueRequests.size(requests) == 0
  end

  test "abandon_all takes every pending entry of one queue, oldest first" do
    {first, requests} = add!(QueueRequests.new(), :a, :register, 10)
    {second, requests} = add!(requests, :a, :enqueue, 20)
    {_other, requests} = add!(requests, :b, :status, 30)
    assert {[^first, ^second], requests} = QueueRequests.abandon_all(requests, :a)
    assert QueueRequests.next_deadline(requests, :a) == nil
    assert QueueRequests.next_deadline(requests, :b) == 30
    assert QueueRequests.size(requests) == 3
    refute QueueRequests.pending?(requests, :a, :enqueue)
    assert QueueRequests.pending?(requests, :b, :status)
    assert {:waiting, ^second, requests} = QueueRequests.resolve(requests, :a, second.id)
    assert {[], _requests} = QueueRequests.abandon_all(requests, :a)
  end

  test "the cap counts pending and waiting entries" do
    {first, requests} = add!(QueueRequests.new(2), :a, :status, 10)
    {_second, requests} = add!(requests, :b, :status, 10)
    assert QueueRequests.full?(requests)

    assert {:error, :full} =
             QueueRequests.add(requests, :c, %{token: make_ref(), kind: :status}, 10)

    assert {[^first], requests} = QueueRequests.expire(requests, :a, 10)
    assert QueueRequests.full?(requests)
    assert {:waiting, ^first, requests} = QueueRequests.resolve(requests, :a, first.id)
    refute QueueRequests.full?(requests)
    assert {:ok, _entry, _} = QueueRequests.add(requests, :c, %{token: make_ref(), kind: :status}, 10)
  end

  test "an exchange take-back classifies into exactly three outcomes" do
    token = make_ref()
    assert QueueRequests.classify([{token, {:request, {:enqueue, "x"}}}]) == :not_executed
    assert QueueRequests.classify([{token, {:result, {:ok, 1}}}]) == {:known, {:ok, 1}}
    assert QueueRequests.classify([]) == :uncertain
  end
end
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10/queue_requests_test.exs
```
Expected: FAIL, `GravitonMQ.Runtime.AMQP10.QueueRequests.new/0 is undefined (module ... is not available)`.

- [ ] **Step 3: Implement**

```elixir
defmodule GravitonMQ.Runtime.AMQP10.QueueRequests do
  @moduledoc false

  # Pure bookkeeping for the Manager's asynchronous queue requests. It has no
  # process, clock or ETS access: callers pass `now` and the list returned by
  # `:ets.take/2`, and act on the entries this module returns.
  #
  # Each queue keeps a FIFO of pending entries. The Manager issues requests to
  # one queue in order and the queue serves its mailbox in order, so deadlines
  # within one queue never decrease and only the head needs a timer.
  #
  # An expired or abandoned entry moves to `waiting` and stays counted until
  # its late reply (or the queue's DOWN) arrives. A queue that never answers
  # therefore cannot make the Manager's bookkeeping grow without bound.
  #
  # Exchange wrappers (written by Queue): `{token, {:request, message}}` until
  # the queue takes it; `{token, {:result, result}}` once executed.

  @default_max 4_096

  defstruct pending: %{}, waiting: %{}, size: 0, next_id: 0, max: @default_max

  @type t :: %__MODULE__{}
  @type outcome :: :not_executed | :uncertain | {:known, term()}

  @spec new(pos_integer()) :: t()
  def new(max \\ @default_max) when is_integer(max) and max > 0, do: %__MODULE__{max: max}

  def size(%__MODULE__{size: size}), do: size
  def full?(%__MODULE__{size: size, max: max}), do: size >= max

  def add(%__MODULE__{} = requests, queue, %{token: _, kind: _} = fields, deadline)
      when is_integer(deadline) do
    if full?(requests) do
      {:error, :full}
    else
      entry =
        %{key: nil, route_token: nil, data: %{}}
        |> Map.merge(fields)
        |> Map.merge(%{id: requests.next_id, queue: queue, deadline: deadline})

      fifo = Map.get(requests.pending, queue, :queue.new())

      {:ok, entry,
       %{
         requests
         | pending: Map.put(requests.pending, queue, :queue.in(entry, fifo)),
           size: requests.size + 1,
           next_id: requests.next_id + 1
       }}
    end
  end

  # A reply or queue DOWN for request `id`.
  def resolve(%__MODULE__{} = requests, queue, id) do
    case Map.pop(requests.waiting, id) do
      {%{} = entry, waiting} ->
        {:waiting, entry, %{requests | waiting: waiting, size: requests.size - 1}}

      {nil, _waiting} ->
        case Enum.split_with(entries(requests, queue), &(&1.id == id)) do
          {[entry], rest} ->
            {:pending, entry, put(%{requests | size: requests.size - 1}, queue, rest)}

          {[], _rest} ->
            {:unknown, requests}
        end
    end
  end

  # Due head entries, oldest first. They wait for their late reply.
  def expire(%__MODULE__{} = requests, queue, now) when is_integer(now) do
    {expired, rest} = Enum.split_while(entries(requests, queue), &(&1.deadline <= now))
    {expired, requests |> put(queue, rest) |> wait(expired)}
  end

  # Every pending entry of `queue` (a fence), oldest first.
  def abandon_all(%__MODULE__{} = requests, queue) do
    abandoned = entries(requests, queue)
    {abandoned, requests |> put(queue, []) |> wait(abandoned)}
  end

  def next_deadline(%__MODULE__{} = requests, queue) do
    case :queue.peek(Map.get(requests.pending, queue, :queue.new())) do
      {:value, entry} -> entry.deadline
      :empty -> nil
    end
  end

  def pending?(%__MODULE__{} = requests, queue, kind),
    do: Enum.any?(entries(requests, queue), &(&1.kind == kind))

  @spec classify(list()) :: outcome()
  def classify([{_token, {:request, _message}}]), do: :not_executed
  def classify([{_token, {:result, result}}]), do: {:known, result}
  def classify([]), do: :uncertain

  defp entries(requests, queue),
    do: requests.pending |> Map.get(queue, :queue.new()) |> :queue.to_list()

  defp put(requests, queue, []), do: %{requests | pending: Map.delete(requests.pending, queue)}

  defp put(requests, queue, entries),
    do: %{requests | pending: Map.put(requests.pending, queue, :queue.from_list(entries))}

  defp wait(requests, entries),
    do: %{requests | waiting: Enum.reduce(entries, requests.waiting, &Map.put(&2, &1.id, &1))}
end
```

- [ ] **Step 4: Run it to verify it passes; check architecture and warnings**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10/queue_requests_test.exs && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix compile --warnings-as-errors
```
Expected: `5 tests, 0 failures`; compile clean.

- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue_requests.ex apps/graviton_mq_runtime/test/amqp10/queue_requests_test.exs
git add -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue_requests.ex apps/graviton_mq_runtime/test/amqp10/queue_requests_test.exs
git commit --only -m "feat(amqp10): pure queue request tracker for the async Manager" -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue_requests.ex apps/graviton_mq_runtime/test/amqp10/queue_requests_test.exs
```

---

## Task 2 (spec T2): Queue exchange wrappers and async helpers

**Files:**
- Modify: `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue.ex` (moduledoc, `request/2` l.477–506, `take_request/3` l.508–519, `put_result/3` l.521–525, new public helpers)
- Test: `apps/graviton_mq_runtime/test/amqp10/queue_test.exs` (append one test)

- [ ] **Step 1: Write the failing test** (append inside `GravitonMQ.Runtime.AMQP10.QueueTest`, before `defp config`)

```elixir
  test "asynchronous requests classify executed, taken-back and queue-down outcomes" do
    queue = queue!()
    table = :ets.new(:exchange, [:set, :public])
    message = fn raw -> {:enqueue, raw, format(), %Message.Index{}, false} end

    {request, token} = Queue.send_request(queue, table, message.("executed"))
    assert {:reply, :ok} = reply = :gen_server.receive_response(request, 1_000)
    assert {:known, {:ok, :accepted_in_memory}} = Queue.response(table, token, reply)
    assert :ets.tab2list(table) == []

    :ok = :sys.suspend(queue)

    {request, token} =
      try do
        {request, token} = Queue.send_request(queue, table, message.("taken-back"))
        # Exactly one of this take and the queue's take wins.
        assert :not_executed = Queue.classify(table, token)
        {request, token}
      after
        :ok = :sys.resume(queue)
      end

    assert {:reply, :ok} = :gen_server.receive_response(request, 1_000)
    assert Queue.classify(table, token) == :uncertain
    assert Queue.status(queue).messages == 1

    {status_request, token} = Queue.send_request(queue, table, :status)

    assert {:known, %{messages: 1}} =
             Queue.response(table, token, :gen_server.receive_response(status_request, 1_000))

    stopped = queue!()
    monitor = Process.monitor(stopped)
    assert Queue.status(stopped).messages == 0
    :ok = GenServer.stop(stopped)
    assert_receive {:DOWN, ^monitor, :process, ^stopped, :normal}
    {request, token} = Queue.send_request(stopped, table, message.("never"))
    assert {:error, {:noproc, _}} = down = :gen_server.receive_response(request, 1_000)
    assert :not_executed = Queue.response(table, token, down)
    assert :ets.tab2list(table) == []
  end
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10/queue_test.exs
```
Expected: FAIL, `function GravitonMQ.Runtime.AMQP10.Queue.send_request/3 is undefined or private`.

- [ ] **Step 3: Implement**

Add `QueueRequests` to the alias line: `alias GravitonMQ.Runtime.AMQP10.{Persistence, QueueRequests}`.

Replace the moduledoc sentence "A call timeout returns `{:error, :unknown_outcome}` because an admitted command may still execute." with:

```text
  A synchronous call timeout returns `{:error, :unknown_outcome}` because an
  admitted command may still execute. Asynchronous callers (`send_request/3`)
  classify a missed reply by taking the request back from their exchange:
  a request the queue never took is never executed.
```

Replace `request/2`, `take_request/3` and `put_result/3` with:

```elixir
  @doc false
  # Synchronous form used by boot wiring, the probe, the stand-in test queue
  # and tests. The exchange belongs to the caller and is destroyed on caller
  # death. GenServer debug events contain only an opaque handle and token,
  # never request content or claimed messages.
  def request(queue, message) do
    table = :ets.new(__MODULE__, [:set, :public])
    token = make_ref()

    try do
      true = :ets.insert(table, {token, {:request, message}})

      case GenServer.call(queue, {:execute, table, token}) do
        :ok ->
          case :ets.take(table, token) do
            [{^token, {:result, result}}] -> result
            _ -> {:error, :queue_call_failed}
          end

        {:error, _} = error ->
          error
      end
    rescue
      _exception -> {:error, :queue_call_failed}
    catch
      :exit, {:timeout, _} -> {:error, :unknown_outcome}
      :exit, {:noproc, _} -> {:error, :closed}
      :exit, {:normal, _} -> {:error, :closed}
      :exit, {:shutdown, _} -> {:error, :closed}
      :exit, _reason -> {:error, :queue_call_failed}
    after
      :ets.delete(table)
    end
  end

  @doc false
  # Asynchronous form for the Manager and the status caller. `table` is a
  # `:public` table owned by the caller; the reply is only `:ok`, the result
  # stays in the table.
  def send_request(queue, table, message) do
    token = make_ref()
    true = :ets.insert(table, {token, {:request, message}})
    {:gen_server.send_request(queue, {:execute, table, token}), token}
  end

  @doc false
  # Atomic take-back: `:not_executed`, `{:known, result}` or `:uncertain`.
  def classify(table, token), do: QueueRequests.classify(:ets.take(table, token))

  @doc false
  # Interprets a `check_response/3` or `receive_response/2` result.
  def response(table, token, {:reply, :ok}), do: classify(table, token)

  def response(table, token, {:reply, {:error, _} = error}) do
    :ets.delete(table, token)
    {:known, error}
  end

  def response(table, token, {:error, {_reason, _server}}), do: classify(table, token)
  def response(table, token, :timeout), do: classify(table, token)

  defp take_request(table, token, caller) do
    if :ets.info(table, :owner) == caller and :ets.info(table, :protection) == :public do
      case :ets.take(table, token) do
        [{^token, {:request, message}}] -> {:ok, message}
        _ -> :abandoned
      end
    else
      :abandoned
    end
  rescue
    ArgumentError -> :abandoned
  end

  defp put_result(table, token, result) do
    :ets.insert(table, {token, {:result, result}})
  rescue
    ArgumentError -> :ok
  end
```

`handle_call({:execute, table, token}, …)` is unchanged.

- [ ] **Step 4: Run the queue suites and the umbrella AMQP runtime tests (no behaviour change)**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10 && cd ../.. && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test apps/graviton_mq/test/graviton_mq/amqp10_runtime_test.exs apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs
```
Expected: all `0 failures`.

- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue.ex apps/graviton_mq_runtime/test/amqp10/queue_test.exs
git add -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue.ex apps/graviton_mq_runtime/test/amqp10/queue_test.exs
git commit --only -m "feat(amqp10): tagged queue exchange and asynchronous request helpers" -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/queue.ex apps/graviton_mq_runtime/test/amqp10/queue_test.exs
```

---

## Task 3 (spec T3a, B1 part): Manager request infrastructure; async settle, claim and pump status

**Files:**
- Modify: `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex`
- Test: `apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs` (replace the first test)

- [ ] **Step 1: Write the failing test.** Replace the test `"an unanswered queue fences only its own Links and keeps the Broker running"` (l.12–92) with the following, and add the `within!/2` helper next to `eventually!/2`. Also change `@moduletag timeout: 30_000` comment to `# A pending queue request is classified at its 5 s deadline.`

```elixir
  test "a suspended queue delays only its own Links and is fenced at the request deadline" do
    broker = broker!()
    supervisor = amqp_supervisor(broker)
    manager = child(supervisor, Broker)
    orders = child(supervisor, {Queue, "orders"})
    other = child(supervisor, {Queue, "other"})
    connections = child(supervisor, :connections)

    for body <- ["first", "second"], do: enqueue!(orders, "orders", message(body))

    consumer = client!(broker)
    link = attach!(consumer, "orders-consumer", :receiver, "orders")
    assert {:ok, 1} = operation(Client.request(consumer, 0, link, 1, 1_048_576), consumer)
    assert_receive {:graviton_mq, ^consumer, {:delivery, unsettled, 0, ^link, _, _}}, 2_000
    assert %{ready: 1, inflight: 1, consumers: 1} = Queue.status(orders)

    bystander = client!(broker)
    publisher = attach!(bystander, "other-publisher", :sender, "other")
    reader = attach!(bystander, "other-consumer", :receiver, "other")
    connection_count = DynamicSupervisor.count_children(connections).active
    :sys.suspend(orders)

    try do
      deadline = System.monotonic_time(:millisecond) + 5_000
      # The Manager sends the settle to the suspended queue; it is never taken.
      assert {:ok, _} =
               operation(Client.settle(consumer, unsettled, %Outcome.Accepted{}), consumer)

      within!(500, fn ->
        assert {:ok, %Outcome.Accepted{}} =
                 operation(
                   Client.send_message(bystander, 0, publisher, message("unaffected")),
                   bystander
                 )
      end)

      within!(500, fn -> attach!(bystander, "other-late", :receiver, "other") end)

      within!(500, fn ->
        assert {:ok, 1} =
                 operation(Client.request(bystander, 0, reader, 1, 1_048_576), bystander)

        assert_receive {:graviton_mq, ^bystander, {:delivery, reference, 0, ^reader, _, _}}, 500

        assert {:ok, _} =
                 operation(Client.settle(bystander, reference, %Outcome.Accepted{}), bystander)
      end)

      assert System.monotonic_time(:millisecond) < deadline,
             "bystander work must complete before the 5 s request deadline"

      assert child(supervisor, Broker) == manager
      refute_received {:graviton_mq, ^consumer, {:session, 0, {:detached, ^link, _}}}

      # The settle was not executed: the queue is fenced at its deadline.
      assert_receive {:graviton_mq, ^consumer, {:session, 0, {:detached, ^link, error}}}, 7_000
      assert {:peer_detach, %{condition: %Value{value: "amqp:internal-error"}}} = error
      assert child(supervisor, Broker) == manager
      assert DynamicSupervisor.count_children(connections).active == connection_count

      # A new Link on the fenced queue is refused explicitly, not hung.
      assert {:error, _} =
               operation(
                 Client.attach(consumer, 0, "refused", :receiver, address: Value.string("orders")),
                 consumer
               )
    after
      :sys.resume(orders)
    end

    # The fenced lease is released by the queue: no claimed message is lost.
    eventually!(fn -> match?(%{ready: 2, inflight: 0, consumers: 0}, Queue.status(orders)) end)
    eventually!(fn -> Queue.status(other).messages == 0 end)

    # The probe restores the queue and new Links work again.
    eventually!(
      fn ->
        match?({:ok, %{queues: %{"orders" => %{ready: 2}}}}, GravitonMQ.amqp10_status(broker))
      end,
      5_000
    )

    link = attach!(consumer, "orders-again", :receiver, "orders")
    assert {:ok, 1} = operation(Client.request(consumer, 0, link, 1, 1_048_576), consumer)
    assert_receive {:graviton_mq, ^consumer, {:delivery, _, 0, ^link, _, _}}, 2_000
    assert child(supervisor, Broker) == manager
  end
```

```elixir
  defp within!(milliseconds, callback) do
    started = System.monotonic_time(:millisecond)
    result = callback.()
    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed < milliseconds, "took #{elapsed} ms, limit #{milliseconds} ms"
    result
  end
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs
```
Expected: FAIL in the first `within!`, with "took ~5000 ms, limit 500 ms": the Manager blocks in `Queue.settle/5`.

- [ ] **Step 3: Implement the infrastructure in `broker.ex`**

3a. Aliases and attributes. Use `alias GravitonMQ.Runtime.AMQP10.{Connection, Queue, QueueRequests}`. Below `@probe_delay`, add:

```elixir
  # The Manager never waits for a queue. A request missing this deadline (the
  # former GenServer.call timeout) is classified by taking it back.
  @request_deadline 5_000
  @max_queue_requests 4_096
```

Keep `@uncertain` for now; Tasks 4 and 5 remove its last uses.

3b. `init/1`: add these keys to the state map:

```elixir
         exchanges: %{},
         requests: QueueRequests.new(@max_queue_requests),
         reqids: :gen_server.reqids_new(),
         request_timers: %{},
         status_waiters: %{},
         deferred: false,
         queue_fences: 0,
```

3c. `handle_continue(:wire, …)`: after `state = %{state | auth_supervisor: auth, queues: queues}`, insert:

```elixir
      # One caller-owned exchange per queue for the Manager's lifetime; the
      # boot `watch` below is the only remaining blocking call (path 1).
      exchanges = Map.new(queues, fn {_, queue} -> {queue, :ets.new(__MODULE__, [:set, :public])} end)
      state = %{state | exchanges: exchanges}
```

3d. `handle_info/2` routes request replies first:

```elixir
  @impl true
  def handle_info(message, shell),
    do: protect(fn -> save(shell, receive_message(message, data(shell))) end)

  # Replies and queue DOWNs for asynchronous requests are matched first; every
  # other message keeps its existing handler.
  defp receive_message(message, state) do
    case :gen_server.check_response(message, state.reqids, true) do
      {response, {queue, id}, reqids} -> queue_response(%{state | reqids: reqids}, queue, id, response)
      _no_request_or_no_reply -> info(message, state)
    end
  end
```

3e. Add the deadline clause to the `info/2` group, before `defp info(_, state), do: state`:

```elixir
  defp info({:queue_deadline, queue, token}, state) do
    case Map.get(state.request_timers, queue) do
      {^token, _timer, _deadline} ->
        state = %{state | request_timers: Map.delete(state.request_timers, queue)}
        {expired, requests} = QueueRequests.expire(state.requests, queue, now())
        table = Map.fetch!(state.exchanges, queue)

        expired
        |> Enum.reduce(%{state | requests: requests}, fn entry, state ->
          answered(state, entry, Queue.classify(table, entry.token))
        end)
        |> arm_requests(queue)

      _ ->
        state
    end
  end
```

3f. Add the request helpers (place them after `send_command/4`):

```elixir
  # Issues one asynchronous request. A route-scoped request (`key` not nil)
  # occupies that route (`queue_ref`) until its reply, deadline or fence.
  defp queue_request(state, queue, kind, message, key, data) do
    if QueueRequests.full?(state.requests) do
      {:error, :full, %{state | deferred: true}}
    else
      route = if key, do: Map.get(state.routes, key)
      {request, token} = Queue.send_request(queue, Map.fetch!(state.exchanges, queue), message)

      fields = %{
        token: token,
        kind: kind,
        key: key,
        route_token: route && route.token,
        data: data
      }

      {:ok, entry, requests} =
        QueueRequests.add(state.requests, queue, fields, now() + @request_deadline)

      reqids = :gen_server.reqids_add(request, {queue, entry.id}, state.reqids)
      state = %{state | requests: requests, reqids: reqids}
      state = if route, do: put_route(state, key, %{route | queue_ref: entry.id}), else: state
      {:ok, arm_requests(state, queue)}
    end
  end

  defp queue_response(state, queue, id, response) do
    case QueueRequests.resolve(state.requests, queue, id) do
      {:pending, entry, requests} ->
        state = arm_requests(%{state | requests: requests}, queue)
        outcome = Queue.response(Map.fetch!(state.exchanges, queue), entry.token, response)
        state |> answered(entry, outcome) |> resume_deferred()

      {:waiting, entry, requests} ->
        # Already classified at its deadline or fence; free its exchange slot.
        :ets.delete(Map.fetch!(state.exchanges, queue), entry.token)
        resume_deferred(%{state | requests: requests})

      {:unknown, _requests} ->
        state
    end
  end

  defp resume_deferred(%{deferred: true} = state) do
    if QueueRequests.full?(state.requests), do: state, else: schedule(%{state | deferred: false})
  end

  defp resume_deferred(state), do: state

  # One timer per queue, for its head deadline only.
  defp arm_requests(state, queue) do
    deadline = QueueRequests.next_deadline(state.requests, queue)

    case Map.get(state.request_timers, queue) do
      {_token, _timer, ^deadline} ->
        state

      current ->
        if current, do: Process.cancel_timer(elem(current, 1))
        timers = Map.delete(state.request_timers, queue)

        if deadline do
          token = make_ref()
          timer = Process.send_after(self(), {:queue_deadline, queue, token}, deadline, abs: true)
          %{state | request_timers: Map.put(timers, queue, {token, timer, deadline})}
        else
          %{state | request_timers: timers}
        end
    end
  end

  # The route that issued `entry`, with its request slot cleared, or nil when
  # that route was removed or replaced meanwhile.
  defp take_route(state, entry) do
    case Map.get(state.routes, entry.key) do
      %{token: token, queue_ref: id} = route when token == entry.route_token and id == entry.id ->
        route = %{route | queue_ref: nil}
        {route, put_route(state, entry.key, route)}

      _ ->
        {nil, state}
    end
  end
```

3g. In `event(... {:attach_requested, …})`, add `queue_ref: nil,` to the new `route` map (after `busy: nil,`). Change the receiver delivery match in `event(connection, {:delivery, …})` from `%{phase: :active, role: :receiver} = route` to `%{phase: :active, role: :receiver, queue_ref: nil} = route`.

3h. Path 6: replace the `complete(state, %{kind: :send} = operation, result)` clause with:

```elixir
  defp complete(state, %{kind: :send} = operation, result) do
    route = Map.fetch!(state.routes, operation.key)

    outcome =
      case result do
        {:ok, outcome} -> OutcomeMapper.to_core(outcome)
        {:error, {:outcome_known, outcome, _}} -> OutcomeMapper.to_core(outcome)
        _ -> %CoreOutcome.MakeAvailable{}
      end

    state = put_route(state, operation.key, %{route | busy: nil})
    message = {:settle, route.consumer, route.generation, operation.delivery, outcome}

    # Issued before any later cleanup disconnect of this route: one queue
    # serves the Manager's requests in order (final outcomes before DOWN).
    case queue_request(state, route.queue, :settle, message, operation.key, %{result: result}) do
      {:ok, state} -> state
      {:error, :full, state} -> fence(state, route.queue)
    end
  end
```

3i. Paths 7–11: replace `pump_route/2`, both `pump_sender/4` clauses and `claim/5` with:

```elixir
  defp pump_route(state, key), do: continue_route(state, key, nil)

  # `status` is the queue status this route waited for, or nil inside a wave.
  defp continue_route(state, key, status) do
    candidate = if closing?(state, elem(key, 0)), do: nil, else: Map.get(state.routes, key)

    case candidate do
      %{phase: :active, busy: nil, queue_ref: nil, role: :receiver, credit: false, receiving: false} =
          route ->
        maximum = session_maximum(state.config)

        case status do
          nil ->
            need_status(state, key, route.queue)

          %{free_messages: messages, free_bytes: bytes} when messages > 0 and bytes >= maximum ->
            {issue(state, key, :demand, {:demand, elem(key, 2), 1, maximum}), true}

          _ ->
            {state, false}
        end

      %{phase: :active, busy: nil, queue_ref: nil, role: :sender} = route ->
        case capacity(state, key) do
          {:ok, %{messages: messages, bytes: bytes} = capacity} when messages > 0 and bytes > 0 ->
            pump_sender(state, key, route, capacity, status)

          {:ok, %{drain_token: _} = capacity} ->
            case status do
              nil -> need_status(state, key, route.queue)
              %{ready: 0} when is_nil(route.pending_delivery) -> drain_exhausted(state, key, capacity)
              _ -> {state, false}
            end

          {:ok, _} ->
            {state, false}

          :busy ->
            {busy(state, elem(key, 0)), false}

          :closed ->
            {close_connection(state, elem(key, 0), :owner_not_responding), false}
        end

      _ ->
        {state, false}
    end
  end

  defp pump_sender(state, key, %{pending_delivery: {delivery, bytes}} = route, capacity, _status) do
    if byte_size(bytes) <= capacity.bytes do
      state = put_route(state, key, %{route | pending_delivery: nil})
      {issue(state, key, {:send, delivery}, {:send, elem(key, 2), bytes, []}), true}
    else
      {state, false}
    end
  end

  defp pump_sender(state, key, route, _capacity, nil), do: need_status(state, key, route.queue)

  defp pump_sender(state, key, route, capacity, status) do
    # Queue data charges include identity/index/mutations. Wire capacity
    # bounds only the outbound view; these are deliberately different units.
    charge_limit =
      min(
        status.limits.max_bytes,
        status.limits.max_message_bytes + status.limits.max_mutation_bytes
      )

    message = {:claim, route.consumer, route.generation, 1, charge_limit}

    case queue_request(state, route.queue, :claim, message, key, %{capacity: capacity}) do
      {:ok, state} -> {state, true}
      {:error, :full, state} -> {state, false}
    end
  end

  # One status request per queue serves every route waiting on it.
  defp need_status(state, key, queue) do
    case Map.fetch(state.status_waiters, queue) do
      {:ok, waiters} ->
        {%{state | status_waiters: Map.put(state.status_waiters, queue, MapSet.put(waiters, key))},
         true}

      :error ->
        case queue_request(state, queue, :status, :status, nil, %{}) do
          {:ok, state} ->
            {%{state | status_waiters: Map.put(state.status_waiters, queue, MapSet.new([key]))},
             true}

          {:error, :full, state} ->
            {state, false}
        end
    end
  end

  defp unfit(state, key, route, delivery) do
    message =
      {:settle, route.consumer, route.generation, delivery, %CoreOutcome.MakeAvailable{}}

    case queue_request(state, route.queue, :unfit, message, key, %{}) do
      {:ok, state} -> state
      {:error, :full, state} -> fence(state, route.queue)
    end
  end
```

3j. Add the `answered/3` group, kept contiguous (Tasks 4–5 add clauses to this same group). Place it after `complete/3`:

```elixir
  # Path 6. A lost peer outcome fences: claims return and the message is
  # redelivered (at-least-once).
  defp answered(state, %{kind: :settle} = entry, outcome) do
    {route, state} = take_route(state, entry)

    case {outcome, route} do
      {{:known, _}, nil} ->
        state

      {{:known, :ok}, _route} ->
        if match?({:ok, _}, entry.data.result),
          do: schedule(state),
          else: detach(state, entry.key, "delivery interrupted")

      {{:known, {:error, _}}, _route} ->
        detach(state, entry.key, "settlement resource limit")

      {_not_executed_or_uncertain, _route} ->
        fence(state, entry.queue)
    end
  end

  # Path 10.
  defp answered(state, %{kind: :claim} = entry, outcome) do
    {route, state} = take_route(state, entry)
    capacity = entry.data.capacity

    case {outcome, route} do
      {{:known, _}, nil} ->
        # The route's cleanup disconnect was issued after this claim and
        # returns anything it claimed.
        state

      {{:known, {:ok, [{delivery, message, metadata}]}}, route} ->
        case Forwarding.prepare(message.encoded_content, metadata, capacity.max_message_bytes) do
          {:ok, bytes} ->
            state
            |> put_route(entry.key, %{route | pending_delivery: {delivery.id, bytes}})
            |> schedule()

          {:error, _} ->
            unfit(state, entry.key, route, delivery.id)
        end

      {{:known, {:ok, []}}, _route} ->
        state |> drain_exhausted(entry.key, capacity) |> elem(0)

      {{:known, {:error, _}}, _route} ->
        detach(state, entry.key, "queue lease is unavailable")

      {_not_executed_or_uncertain, _route} ->
        # The claim may have executed; fencing releases it.
        fence(state, entry.queue)
    end
  end

  # Path 11.
  defp answered(state, %{kind: :unfit} = entry, outcome) do
    {route, state} = take_route(state, entry)

    cond do
      outcome in [:not_executed, :uncertain] -> fence(state, entry.queue)
      route -> detach(state, entry.key, "message cannot fit this link")
      true -> state
    end
  end

  # Paths 7–9.
  defp answered(state, %{kind: :status} = entry, outcome) do
    {waiters, status_waiters} = Map.pop(state.status_waiters, entry.queue, MapSet.new())
    state = %{state | status_waiters: status_waiters}

    case outcome do
      {:known, status} when is_map(status) ->
        Enum.reduce(waiters, state, fn key, state -> elem(continue_route(state, key, status), 0) end)

      _ ->
        fence(state, entry.queue)
    end
  end
```

3k. Replace `fence/2` with the version that counts fences, abandons pending requests and rejects registering routes:

```elixir
  # Fencing isolates one queue whose state may have diverged from this
  # coordinator. Killing its lease anchor makes Queue return every claim held
  # by this coordinator there when it next runs; only that queue's Links are
  # detached. Messages stay in the queue process. Pending requests to it are
  # taken back (never executed) or covered by the killed anchor; abandoned
  # enqueues are answered before any Detach below.
  defp fence(state, queue) do
    case Map.pop(state.anchors, queue) do
      {nil, _} ->
        state

      {anchor, anchors} ->
        Process.exit(anchor, :kill)

        state = %{
          state
          | anchors: anchors,
            queue_fences: state.queue_fences + 1,
            status_waiters: Map.delete(state.status_waiters, queue)
        }

        {abandoned, requests} = QueueRequests.abandon_all(state.requests, queue)
        state = arm_requests(%{state | requests: requests}, queue)
        table = Map.fetch!(state.exchanges, queue)

        state =
          Enum.reduce(abandoned, state, fn entry, state ->
            abandoned(state, entry, Queue.classify(table, entry.token))
          end)

        state =
          state.routes
          |> Enum.filter(fn {_, route} -> route.queue == queue and route.phase != :authorizing end)
          |> Enum.reduce(state, fn
            {key, %{phase: :registering}}, state -> reject(state, key, unavailable())
            {key, _route}, state -> detach(state, key, unavailable())
          end)

        state |> start_probe(queue, @probe_delay) |> schedule()
    end
  end

  # Lease requests need nothing here: the killed anchor releases them.
  defp abandoned(state, _entry, _outcome), do: state
```

3l. Status counters (the caller-side status arrives in Task 5). In `handle_call({:status, …})`, add `queue_requests: QueueRequests.size(state.requests),` and `queue_fences: state.queue_fences,` to the result map.

- [ ] **Step 4: Run the availability, boundaries and runtime suites**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix compile --warnings-as-errors && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs apps/graviton_mq/test/graviton_mq/amqp10_runtime_test.exs apps/graviton_mq/test/graviton_mq/amqp10_g5_flow_test.exs apps/graviton_mq/test/graviton_mq/amqp10_g5_annotations_test.exs apps/graviton_mq/test/graviton_mq/amqp10_sasl_test.exs
```
Expected: `0 failures` (the boundaries "uncertain detach cleanup" test still passes, with its existing 7 s wait). Run it three times with `--seed 1`, `--seed 2` and `--seed 3`. Also run the pump-wave test alone: `--only line:177` on the boundaries file. It must still see waves of ≤ 1 inspected route (D8).

- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs
git add -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs
git commit --only -m "feat(amqp10): asynchronous Manager settle, claim and pump status requests" -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs
```

---

## Task 4 (spec T3b): Async register and cleanup disconnect

**Files:**
- Modify: `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex` (`decide/3`, `lease/2`, `register/2`, `remove_route/2`, `answered/3` group)
- Test: `apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs` (append one test)

- [ ] **Step 1: Write the failing test** (append to `AvailabilityTest`)

```elixir
  test "a pending register and cleanup disconnect on a stalled queue do not stall other queues" do
    broker = broker!()
    supervisor = amqp_supervisor(broker)
    orders = child(supervisor, {Queue, "orders"})
    holder = client!(broker)
    held = attach!(holder, "orders-held", :receiver, "orders")
    assert %{consumers: 1} = Queue.status(orders)
    bystander = client!(broker)
    publisher = attach!(bystander, "other-publisher", :sender, "other")
    late = client!(broker)
    :sys.suspend(orders)

    try do
      # Register (path 4) and cleanup disconnect (path 12) wait for the queue.
      assert {:ok, attaching} =
               Client.attach(late, 0, "orders-late", :receiver, address: Value.string("orders"))

      assert {:ok, _} = Client.detach(holder, 0, held)

      within!(500, fn ->
        assert {:ok, %Outcome.Accepted{}} =
                 operation(
                   Client.send_message(bystander, 0, publisher, message("unaffected")),
                   bystander
                 )
      end)

      refute_received {:graviton_mq, ^late, {:operation, ^attaching, _}}
      # The register was never taken: at its deadline the Link is refused.
      assert {:error, _} = operation({:ok, attaching}, late)
    after
      :sys.resume(orders)
    end

    eventually!(fn -> match?(%{consumers: 0, inflight: 0}, Queue.status(orders)) end)
  end
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs
```
Expected: the new test FAILS in `within!`, with "took ~5000 ms" (the synchronous register).

- [ ] **Step 3: Implement**

Replace `decide(state, key, :allow)`, `lease/2` and `register/2` with:

```elixir
  defp decide(state, key, :allow) do
    route = Map.fetch!(state.routes, key)

    cond do
      fenced?(state, route.queue) -> reject(state, key, unavailable())
      route.role == :receiver -> accept(state, key, route)
      true -> register(state, key, route)
    end
  end
```

(keep `decide(state, key, :deny)` and `reject/3` as they are)

```elixir
  defp accept(state, key, route) do
    state = put_route(state, key, %{route | phase: :accepting})
    issue(state, key, :accept, {:accept_link, elem(key, 2), route.token})
  end

  # This coordinator consumes ordered Connection outcome events before DOWN.
  # Monitoring the socket owner in Queue would race that final settlement
  # against a DOWN delivered independently to Queue. Queue instead monitors a
  # per-queue anchor that lives exactly as long as this coordinator unless the
  # coordinator fences that queue; either death releases all its leases there.
  defp register(state, key, route) do
    consumer = :crypto.strong_rand_bytes(24)
    message = {:register, consumer, Map.fetch!(state.anchors, route.queue)}
    state = put_route(state, key, %{route | phase: :registering})

    case queue_request(state, route.queue, :register, message, key, %{consumer: consumer}) do
      {:ok, state} ->
        state

      {:error, :full, state} ->
        reject(state, key, error("amqp:resource-limit-exceeded", "queue request limit reached"))
    end
  end

  # Path 12. A fenced queue has already lost every lease with its killed anchor.
  defp cleanup(state, queue, consumer, generation) do
    if fenced?(state, queue) do
      state
    else
      case queue_request(state, queue, :disconnect, {:disconnect, consumer, generation}, nil, %{}) do
        {:ok, state} -> state
        {:error, :full, state} -> fence(state, queue)
      end
    end
  end
```

Replace `remove_route/2` with:

```elixir
  defp remove_route(state, key) do
    case Map.pop(state.routes, key) do
      {nil, _} ->
        state

      {route, routes} ->
        jobs =
          Enum.reduce(state.jobs, %{}, fn {ref, job}, jobs ->
            if job.key == key do
              Process.cancel_timer(job.timer)
              Process.demonitor(ref, [:flush])
              Process.exit(job.pid, :kill)
              jobs
            else
              Map.put(jobs, ref, job)
            end
          end)

        operations = Map.reject(state.operations, fn {_, op} -> op.key == key end)
        state = %{state | routes: routes, operations: operations, jobs: jobs}

        # A route still registering has no consumer yet; its late reply is
        # released by the register clause of answered/3.
        if route.consumer,
          do: cleanup(state, route.queue, route.consumer, route.generation),
          else: state
    end
  end
```

Add to the `answered/3` group:

```elixir
  # Path 4.
  defp answered(state, %{kind: :register} = entry, outcome) do
    {route, state} = take_route(state, entry)

    case {outcome, route} do
      {{:known, {:ok, generation}}, %{phase: :registering} = route} ->
        accept(state, entry.key, %{route | consumer: entry.data.consumer, generation: generation})

      {{:known, {:ok, generation}}, _removed_or_replaced} ->
        # Invariant: a lease registered for a route that no longer exists is
        # released at once, so no lease is orphaned.
        cleanup(state, entry.queue, entry.data.consumer, generation)

      {{:known, {:error, _}}, nil} ->
        state

      {{:known, {:error, _}}, _route} ->
        decide(state, entry.key, :deny)

      {_not_executed_or_uncertain, _route} ->
        # A registration may exist in the queue: fencing kills its anchor and
        # rejects this registering Link.
        fence(state, entry.queue)
    end
  end

  defp answered(state, %{kind: :disconnect}, {:known, :ok}), do: state
  defp answered(state, %{kind: :disconnect}, {:known, {:error, :stale_consumer}}), do: state
  # The queue may still hold this lease and its claims.
  defp answered(state, %{kind: :disconnect} = entry, _outcome), do: fence(state, entry.queue)
```

`@uncertain` is still used by `admit_message/4`. Leave it until Task 5.

- [ ] **Step 4: Run**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix compile --warnings-as-errors && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs apps/graviton_mq/test/graviton_mq/amqp10_runtime_test.exs apps/graviton_mq/test/graviton_mq/amqp10_sasl_test.exs apps/graviton_mq/test/graviton_mq/amqp10_g5_flow_test.exs
```
Expected: `0 failures` (three seeds).

- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs
git add -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs
git commit --only -m "feat(amqp10): asynchronous Manager register and cleanup disconnect" -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs
```

---

## Task 5 (spec T3c, B1 status, B2, B3, B5): Async enqueue, watch_ack, restore and public status

**Files:**
- Modify: `apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex`
- Test: `apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs`, `apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs`

- [ ] **Step 1: Write the failing tests**

1a. In the Task 3 test, insert the status checks right after the `assert System.monotonic_time(:millisecond) < deadline, …` line:

```elixir
      # The Manager answers status at once; the stalled queue is reported as
      # :unresponsive (not fenced yet) within the 1 s status deadline (Q6).
      status = within!(1_000, fn -> GravitonMQ.amqp10_status(broker) end)
      assert {:ok, %{queues: %{"orders" => :unresponsive, "other" => %{}}} = status_map} = status
      assert status_map.queue_requests >= 1
      assert status_map.queue_fences == 0
```

After the `{:peer_detach, …}` assertion, add:

```elixir
      assert {:ok, %{queues: %{"orders" => :unavailable}, queue_fences: 1}} =
               within!(1_000, fn -> GravitonMQ.amqp10_status(broker) end)
```

1b. Append B2 and B5. `broker!/0` becomes `broker!(queues \\ ["orders", "other"])` with `queues: queues`.

```elixir
  test "two stalled queues answer status within 1 s and fence independently (B2)" do
    broker = broker!(["orders", "other", "third"])
    supervisor = amqp_supervisor(broker)
    stalled = for address <- ["orders", "other"], do: {address, child(supervisor, {Queue, address})}

    held =
      for {address, queue} <- stalled do
        enqueue!(queue, address, message(address))
        client = client!(broker)
        link = attach!(client, "#{address}-consumer", :receiver, address)
        assert {:ok, 1} = operation(Client.request(client, 0, link, 1, 1_048_576), client)
        assert_receive {:graviton_mq, ^client, {:delivery, delivery, 0, ^link, _, _}}, 2_000
        {client, link, delivery}
      end

    bystander = client!(broker)
    third = attach!(bystander, "third-publisher", :sender, "third")
    Enum.each(stalled, fn {_, queue} -> :sys.suspend(queue) end)

    try do
      for {client, _link, delivery} <- held,
          do: assert({:ok, _} = operation(Client.settle(client, delivery, %Outcome.Accepted{}), client))

      assert {:ok, %{queues: %{"orders" => :unresponsive, "other" => :unresponsive, "third" => %{}}}} =
               within!(1_000, fn -> GravitonMQ.amqp10_status(broker) end)

      within!(500, fn ->
        assert {:ok, %Outcome.Accepted{}} =
                 operation(Client.send_message(bystander, 0, third, message("third")), bystander)
      end)

      for {client, link, _delivery} <- held do
        assert_receive {:graviton_mq, ^client, {:session, 0, {:detached, ^link, _}}}, 7_000
      end

      assert {:ok,
              %{queues: %{"orders" => :unavailable, "other" => :unavailable, "third" => %{}}} =
                status} = within!(1_000, fn -> GravitonMQ.amqp10_status(broker) end)

      assert status.queue_fences == 2
    after
      Enum.each(stalled, fn {_, queue} -> :sys.resume(queue) end)
    end

    for {_, queue} <- stalled do
      eventually!(fn -> match?(%{ready: 1, inflight: 0, consumers: 0}, Queue.status(queue)) end)
    end
  end

  test "an enqueue the queue never took is Released and leaves no copy (B5, owner Q5)" do
    broker = broker!()
    supervisor = amqp_supervisor(broker)
    orders = child(supervisor, {Queue, "orders"})
    publisher = client!(broker)
    sender = attach!(publisher, "orders-publisher", :sender, "orders")
    :sys.suspend(orders)

    try do
      assert {:ok, %Outcome.Released{}} =
               operation(Client.send_message(publisher, 0, sender, message("not-executed")), publisher)

      assert Client.status(publisher).phase == :active
      assert {:ok, %{queue_fences: 0}} = GravitonMQ.amqp10_status(broker)
    after
      :sys.resume(orders)
    end

    Process.sleep(200)
    assert %{messages: 0, ready: 0} = Queue.status(orders)
  end
```

1c. B3, in `amqp10_boundaries_test.exs` "uncertain detach cleanup fences only its queue…": replace the comment and `eventually!(…, 7_000)` block inside `try` with:

```elixir
      # The Manager is not blocked: status answers within 1 s and reports the
      # stalled, not yet fenced queue as :unresponsive.
      started = System.monotonic_time(:millisecond)
      assert {:ok, %{queues: %{"orders" => :unresponsive}}} = GravitonMQ.amqp10_status(instance)
      assert System.monotonic_time(:millisecond) - started < 1_000

      # The cleanup disconnect was never taken: the queue is fenced at the
      # 5 s request deadline.
      eventually!(
        fn ->
          match?(
            {:ok, %{queues: %{"orders" => :unavailable}, queue_fences: 1}},
            GravitonMQ.amqp10_status(instance)
          )
        end,
        7_000
      )
```

- [ ] **Step 2: Run them to verify they fail**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs
```
Expected failures:
- Task 3 test: status took ~5,000 ms, because the Manager calls `Queue.status/1` inside `handle_call`.
- B2: status took ~10,000 ms, or the caller exits.
- B5: the operation is `{:unknown, _}` or `{:disconnected, …}`, not `Released`; synchronous enqueue closes the Connection.
- B3: `:unresponsive` is not a status value.

- [ ] **Step 3: Implement**

3a. Path 5. Replace `event(connection, {:delivery, …})` and `admit_message/4` (both clauses) with:

```elixir
  defp event(connection, {:delivery, sid, identity, token, raw, metadata}, state) do
    key = {connection, sid, identity}

    case Map.get(state.routes, key) do
      %{phase: :active, role: :receiver, queue_ref: nil} = route ->
        state = put_route(state, key, %{route | credit: false, receiving: true})

        case prepare_message(route, raw, metadata, state.config) do
          {:enqueue, message} ->
            case queue_request(state, route.queue, :enqueue, message, key, %{delivery: token}) do
              {:ok, state} -> state
              # Not executed: the sender may resend (owner Q5).
              {:error, :full, state} -> settle_publication(state, key, token, %Outcome.Released{})
            end

          outcome ->
            settle_publication(state, key, token, outcome)
        end

      _ ->
        close_connection(state, connection, :unadmitted_delivery)
    end
  end
```

```elixir
  defp prepare_message(route, raw, %{message_format: 0}, config) do
    maximum = Keyword.get(config.queue_options, :max_message_bytes, 1_048_576)

    with {:ok, message, <<>>} <- Message.decode(raw, %Limits{max_value_bytes: maximum}),
         {:ok, durable?} <- profile_message(message.sections, config.profile) do
      {:enqueue,
       {:enqueue, raw, %CoreMessage.Format{family: "amqp-1.0", identifier: 0},
        %CoreMessage.Index{routing: %{"address" => route.address}}, durable?}}
    else
      {:error, reason} -> refusal(reason)
      _ -> refusal(:decode_error)
    end
  end

  defp prepare_message(_, _, _, _),
    do: %Outcome.Rejected{error: error("amqp:not-implemented", "unsupported message format")}

  # No disposition is invented for an unknown publication outcome. Enqueue
  # holds no lease, so only the publishing Connection is closed.
  defp settle_publication(state, key, _token, :unknown),
    do: close_connection(state, elem(key, 0), :queue_outcome_unknown)

  defp settle_publication(state, key, token, {:reject_durable, outcome}),
    do: issue(state, key, {:settle, outcome.error}, {:settle, token, outcome})

  defp settle_publication(state, key, token, outcome),
    do: issue(state, key, :settle, {:settle, token, outcome})

  defp publication(:not_executed, _profile), do: %Outcome.Released{}
  defp publication(:uncertain, _profile), do: :unknown

  defp publication({:known, {:ok, acceptance}}, profile),
    do: if(accepted(acceptance, profile) == :ok, do: %Outcome.Accepted{}, else: :unknown)

  defp publication({:known, {:error, reason}}, _profile), do: refusal(reason)

  defp refusal(:persistence_failed), do: :unknown

  defp refusal(:durability_unsupported),
    do:
      {:reject_durable,
       %Outcome.Rejected{
         error: error("amqp:precondition-failed", "memory queue cannot accept durable messages")
       }}

  defp refusal(:expiry_unsupported),
    do: %Outcome.Rejected{
      error: error("amqp:precondition-failed", "message requires another profile")
    }

  defp refusal(reason) when reason in [:queue_full, :message_too_large, :byte_limit],
    do: %Outcome.Rejected{error: error("amqp:resource-limit-exceeded", "queue capacity exceeded")}

  defp refusal(_reason),
    do: %Outcome.Rejected{error: error("amqp:decode-error", "message was not admitted")}
```

Delete `@uncertain`; it now has no uses. `accepted/2` and `profile_message/2` stay.

3b. Path 3. Replace the `info({:amqp10_queue_changed, queue}, state)` clause with:

```elixir
  defp info({:amqp10_queue_changed, queue}, state) do
    cond do
      queue not in Map.values(state.queues) ->
        state

      # A fenced queue is acknowledged again when its probe restores it.
      fenced?(state, queue) ->
        state

      QueueRequests.pending?(state.requests, queue, :watch_ack) ->
        state

      true ->
        case queue_request(state, queue, :watch_ack, {:watch_ack, self()}, nil, %{}) do
          {:ok, state} -> state
          {:error, :full, state} -> fence(state, queue)
        end
    end
  end
```

3c. Path 13. Replace both `restore/3` clauses with:

```elixir
  defp restore(state, queue, :available) do
    if fenced?(state, queue) do
      case queue_request(state, queue, :restore_watch, {:watch, self()}, nil, %{}) do
        {:ok, state} -> state
        {:error, :full, state} -> start_probe(state, queue, @probe_delay)
      end
    else
      state
    end
  end

  defp restore(state, queue, _reason), do: start_probe(state, queue, @probe_delay)
```

3d. Add to the `answered/3` group:

```elixir
  # Path 5. A gone Link or Connection means the publisher saw no disposition:
  # the documented unknown-outcome case.
  defp answered(state, %{kind: :enqueue} = entry, outcome) do
    case take_route(state, entry) do
      {nil, state} ->
        state

      {_route, state} ->
        settle_publication(
          state,
          entry.key,
          entry.data.delivery,
          publication(outcome, state.config.profile)
        )
    end
  end

  # Path 3.
  defp answered(state, %{kind: :watch_ack}, {:known, :ok}), do: schedule(state)
  defp answered(state, %{kind: :watch_ack} = entry, _outcome), do: fence(state, entry.queue)

  # Path 13: watch, then watch_ack, then a fresh anchor.
  defp answered(state, %{kind: :restore_watch} = entry, {:known, :ok}) do
    case queue_request(state, entry.queue, :restore_ack, {:watch_ack, self()}, nil, %{}) do
      {:ok, state} -> state
      {:error, :full, state} -> start_probe(state, entry.queue, @probe_delay)
    end
  end

  defp answered(state, %{kind: :restore_ack} = entry, {:known, :ok}) do
    if fenced?(state, entry.queue),
      do: schedule(%{state | anchors: Map.put(state.anchors, entry.queue, anchor())}),
      else: state
  end

  defp answered(state, %{kind: kind} = entry, _outcome) when kind in [:restore_watch, :restore_ack],
    do: start_probe(state, entry.queue, @probe_delay)
```

Replace the `abandoned/3` fallback with two clauses:

```elixir
  defp abandoned(state, %{kind: :enqueue} = entry, outcome), do: answered(state, entry, outcome)
  # Lease requests need nothing here: the killed anchor releases them.
  defp abandoned(state, _entry, _outcome), do: state
```

3e. Path 2. In `handle_call({:status, table, token}, …)`, replace the `queues:` entry with:

```elixir
          queues:
            Map.new(state.queues, fn {address, queue} ->
              {address, if(fenced?(state, queue), do: :unavailable, else: {:queue, queue})}
            end)
```

Replace `status/1` (the public caller function) with:

```elixir
  # Under the 1 s status bound (D14) the caller waits at most this long for
  # the queues.
  @status_deadline 900

  # The Manager answers from its own state. The caller then asks every
  # unfenced queue concurrently under one deadline, so a stuck queue is
  # reported as :unresponsive and delays neither the caller nor the Manager.
  # Queue pids never leave this function.
  def status(owner) do
    deadline = now() + @status_deadline
    table = :ets.new(__MODULE__, [:set, :public])
    token = make_ref()

    try do
      :ok = GenServer.call(owner, {:status, table, token}, 5_000)
      [{^token, result}] = :ets.take(table, token)

      requests =
        for {address, {:queue, queue}} <- result.queues,
            into: %{},
            do: {address, Queue.send_request(queue, table, :status)}

      queues =
        Map.new(result.queues, fn
          {address, :unavailable} -> {address, :unavailable}
          {address, {:queue, _}} -> {address, queue_status(table, Map.fetch!(requests, address), deadline)}
        end)

      %{result | queues: queues}
    after
      :ets.delete(table)
    end
  end

  defp queue_status(table, {request, token}, deadline) do
    case Queue.response(table, token, :gen_server.receive_response(request, {:abs, deadline})) do
      {:known, status} when is_map(status) -> status
      _ -> :unresponsive
    end
  end
```

`:gen_server.receive_response/2` deactivates the alias on timeout (OTP 28 `gen.erl`), so no late reply reaches the caller. A request the queue takes later finds the caller's table deleted (`:abandoned`).

- [ ] **Step 4: Run**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix compile --warnings-as-errors && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs apps/graviton_mq/test/graviton_mq/amqp10_runtime_test.exs apps/graviton_mq/test/graviton_mq/amqp10_sasl_test.exs apps/graviton_mq/test/graviton_mq/amqp10_g5_flow_test.exs apps/graviton_mq/test/graviton_mq/amqp10_g5_annotations_test.exs && cd apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10
```
Expected: `0 failures`; repeat the umbrella run with seeds 1–3.

- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs
git add -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs
git commit --only -m "feat(amqp10): asynchronous enqueue, watch, restore and bounded status" -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/amqp10/broker.ex apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs
```

---

## Task 6 (spec T4: B4, B6, B7): Stand-in queue fault injection and Manager death

**Files:**
- Create: `apps/graviton_mq/test/support/amqp10_stand_in_queue.exs`
- Create: `apps/graviton_mq/test/graviton_mq/amqp10_manager_requests_test.exs`

These tests verify invariants that Tasks 3–5 implemented. They are expected to pass on first run. Each has a **mutation check** that must make it fail.

- [ ] **Step 1: Write the stand-in support**

```elixir
defmodule GravitonMQ.Test.StandInQueue do
  @moduledoc false
  # Test-only `{Queue, address}` child (S3 B4). It forwards every request to a
  # real Queue it owns, except a request kind it is told to hold: it *takes*
  # that request from the caller's exchange and never answers it, and stops
  # taking later requests until released. This models "the queue accepted the
  # request but has not answered", which `:sys.suspend/1` cannot model.
  # Every request goes through this process, so the real queue sees one
  # consistent controller.
  use GenServer

  alias GravitonMQ.Runtime.AMQP10.Queue

  def start_link(argument), do: GenServer.start_link(__MODULE__, argument)
  def hold(stand_in, kind), do: GenServer.call(stand_in, {:stand_in_hold, kind})
  def release(stand_in), do: GenServer.call(stand_in, :stand_in_release, 10_000)
  def queue(stand_in), do: GenServer.call(stand_in, :stand_in_queue)

  @impl true
  def init({config, address, observer}) do
    {:ok, queue} = Queue.start_link(fn -> {config, address} end)
    {:ok, %{queue: queue, observer: observer, hold: nil, held: nil, backlog: :queue.new()}}
  end

  @impl true
  def handle_call({:stand_in_hold, kind}, _from, state), do: {:reply, :ok, %{state | hold: kind}}
  def handle_call(:stand_in_queue, _from, state), do: {:reply, state.queue, state}

  def handle_call(:stand_in_release, _from, state) do
    state =
      case state.held do
        nil -> state
        {from, message, table, token} -> forward(state, from, message, table, token)
      end

    {:reply, :ok, drain(%{state | hold: nil, held: nil})}
  end

  def handle_call({:execute, table, token}, from, %{held: nil} = state),
    do: execute(state, from, table, token)

  def handle_call({:execute, table, token}, from, state),
    do: {:noreply, %{state | backlog: :queue.in({from, table, token}, state.backlog)}}

  defp execute(state, from, table, token) do
    case take(table, token, elem(from, 0)) do
      {:ok, message} ->
        if kind(message) == state.hold do
          send(state.observer, {:stand_in_held, self(), kind(message)})
          {:noreply, %{state | held: {from, message, table, token}}}
        else
          {:noreply, forward(state, from, message, table, token)}
        end

      :abandoned ->
        {:reply, :ok, state}
    end
  end

  defp forward(state, from, message, table, token) do
    result = Queue.request(state.queue, message)

    try do
      :ets.insert(table, {token, {:result, result}})
    rescue
      ArgumentError -> :ok
    end

    GenServer.reply(from, :ok)
    state
  end

  defp drain(state) do
    case :queue.out(state.backlog) do
      {{:value, {from, table, token}}, backlog} ->
        case execute(%{state | backlog: backlog}, from, table, token) do
          {:reply, :ok, state} ->
            GenServer.reply(from, :ok)
            drain(state)

          {:noreply, state} ->
            drain(state)
        end

      {:empty, _} ->
        state
    end
  end

  defp take(table, token, caller) do
    if :ets.info(table, :owner) == caller do
      case :ets.take(table, token) do
        [{^token, {:request, message}}] -> {:ok, message}
        _ -> :abandoned
      end
    else
      :abandoned
    end
  rescue
    ArgumentError -> :abandoned
  end

  defp kind(message) when is_tuple(message), do: elem(message, 0)
  defp kind(message), do: message
end

defmodule GravitonMQ.Test.StandInAMQP do
  @moduledoc false
  # The production AMQP child list with one queue replaced by the stand-in.
  # The Broker reads `{Queue, address}` through `Supervisor.which_children/1`
  # in `handle_continue(:wire)`, so it talks to the stand-in.
  use Supervisor

  alias GravitonMQ.Runtime.AMQP10.{Config, Queue}

  def start_link(argument), do: Supervisor.start_link(__MODULE__, argument)

  @impl true
  def init({options, address, observer}) do
    {:ok, config} = Config.build(options)
    {:ok, {flags, children}} = GravitonMQ.Runtime.AMQP10.Supervisor.init(fn -> config end)
    queue_config = Map.delete(config, :auth)

    children =
      Enum.map(children, fn
        %{id: {Queue, ^address}} = child ->
          %{child | start: {GravitonMQ.Test.StandInQueue, :start_link, [{queue_config, address, observer}]}}

        child ->
          child
      end)

    {:ok, {flags, children}}
  end
end
```

- [ ] **Step 2: Write the tests**

```elixir
Code.require_file("../support/amqp10_stand_in_queue.exs", __DIR__)

defmodule GravitonMQ.AMQP10.ManagerRequestsTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message, as: MessageCodec
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Runtime.AMQP10.{Broker, Listener, Queue}
  alias GravitonMQ.Test.{StandInAMQP, StandInQueue}

  # Taken-but-unanswered requests are classified at the 5 s deadline.
  @moduletag timeout: 30_000

  test "B4: a taken but unanswered settle is uncertain and fences only its queue" do
    %{manager: manager, stand_in: stand_in, port: port} = stand_in!()
    real = StandInQueue.queue(stand_in)
    publish!(port, "orders", "held-settle")
    {consumer, link, delivery} = consume!(port, "orders")
    bystander = client!(port)
    other = attach!(bystander, "other-publisher", :sender, "other")
    :ok = StandInQueue.hold(stand_in, :settle)
    assert {:ok, _} = operation(Client.settle(consumer, delivery, %Outcome.Accepted{}), consumer)
    assert_receive {:stand_in_held, ^stand_in, :settle}, 2_000

    assert {:ok, %Outcome.Accepted{}} =
             operation(Client.send_message(bystander, 0, other, message("other")), bystander, 500)

    assert %{queues: %{"orders" => :unresponsive}} = Broker.status(manager)
    assert_receive {:graviton_mq, ^consumer, {:session, 0, {:detached, ^link, error}}}, 7_000
    assert {:peer_detach, %{condition: %Value{value: "amqp:internal-error"}}} = error
    assert %{queues: %{"orders" => :unavailable}, queue_fences: 1} = Broker.status(manager)
    :ok = StandInQueue.release(stand_in)
    # The killed anchor returned the claim; the late settle met a stale lease.
    eventually!(fn -> match?(%{messages: 1, ready: 1, inflight: 0, consumers: 0}, Queue.status(real)) end)
  end

  test "B4: a taken but unanswered claim is uncertain and fences its queue" do
    %{manager: manager, stand_in: stand_in, port: port} = stand_in!()
    real = StandInQueue.queue(stand_in)
    publish!(port, "orders", "held-claim")
    consumer = client!(port)
    link = attach!(consumer, "orders-consumer", :receiver, "orders")
    :ok = StandInQueue.hold(stand_in, :claim)
    assert {:ok, 1} = operation(Client.request(consumer, 0, link, 1, 1_048_576), consumer)
    assert_receive {:stand_in_held, ^stand_in, :claim}, 2_000
    assert_receive {:graviton_mq, ^consumer, {:session, 0, {:detached, ^link, _}}}, 7_000
    refute_received {:graviton_mq, ^consumer, {:delivery, _, _, _, _, _}}
    assert %{queue_fences: 1} = Broker.status(manager)
    :ok = StandInQueue.release(stand_in)
    eventually!(fn -> match?(%{messages: 1, ready: 1, inflight: 0, consumers: 0}, Queue.status(real)) end)
  end

  test "B4: a taken but unanswered enqueue closes only the publishing Connection" do
    %{manager: manager, stand_in: stand_in, port: port} = stand_in!()
    real = StandInQueue.queue(stand_in)
    watcher = client!(port)
    watching = attach!(watcher, "orders-watcher", :receiver, "orders")
    publisher = client!(port)
    sender = attach!(publisher, "orders-publisher", :sender, "orders")
    :ok = StandInQueue.hold(stand_in, :enqueue)
    assert {:ok, sending} = Client.send_message(publisher, 0, sender, message("uncertain"))
    assert_receive {:stand_in_held, ^stand_in, :enqueue}, 2_000
    assert_receive {:graviton_mq, ^publisher, {:operation, ^sending, {:unknown, _}}}, 7_000
    assert_receive {:graviton_mq, ^publisher, {:disconnected, _}}, 2_000
    refute_received {:graviton_mq, ^publisher, {:operation, ^sending, {:ok, _}}}
    # No fence: the other Connection's Link on the same queue stays attached.
    refute_received {:graviton_mq, ^watcher, {:session, 0, {:detached, ^watching, _}}}
    assert Client.status(watcher).phase == :active
    :ok = StandInQueue.release(stand_in)
    assert %{queue_fences: 0, queues: %{"orders" => %{}}} = Broker.status(manager)
    # The documented unknown-outcome case: the message is in the queue.
    eventually!(fn -> Queue.status(real).messages == 1 end)
  end

  test "B6: a late register reply after its route was removed leaves no consumer" do
    %{stand_in: stand_in, port: port} = stand_in!()
    real = StandInQueue.queue(stand_in)
    consumer = client!(port)
    :ok = StandInQueue.hold(stand_in, :register)

    assert {:ok, _attaching} =
             Client.attach(consumer, 0, "orders-late", :receiver, address: Value.string("orders"))

    assert_receive {:stand_in_held, ^stand_in, :register}, 2_000
    # D11: the route is removed by Connection DOWN while it is registering.
    :ok = GenServer.stop(consumer)
    Process.sleep(100)
    :ok = StandInQueue.release(stand_in)
    eventually!(fn -> Queue.status(real).consumers == 0 end)
    Process.sleep(200)
    assert %{consumers: 0} = Queue.status(real)
  end

  test "B7: Manager death abandons its pending requests and returns its claims" do
    instance =
      start_supervised!(
        {GravitonMQ,
         name: nil, runtime_supervisor_name: nil, amqp10: options(["orders"])},
        id: make_ref()
      )

    supervisor = instance |> child(GravitonMQ.Runtime.Supervisor) |> child(GravitonMQ.Runtime.AMQP10.Supervisor)
    manager = child(supervisor, Broker)
    queue = child(supervisor, {Queue, "orders"})
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(instance)
    publish!(port, "orders", "first")
    {consumer, _link, delivery} = consume!(port, "orders")
    publisher = client!(port)
    sender = attach!(publisher, "orders-publisher-2", :sender, "orders")
    assert %{messages: 1, inflight: 1, consumers: 1} = Queue.status(queue)
    :ok = :sys.suspend(queue)

    try do
      assert {:ok, _} = operation(Client.settle(consumer, delivery, %Outcome.Accepted{}), consumer)
      assert {:ok, _sending} = Client.send_message(publisher, 0, sender, message("abandoned"))
      Process.sleep(100)
      monitor = Process.monitor(manager)
      assert {:error, :unsupported_command} = GenServer.call(manager, :sync)

      capture_log(fn ->
        Process.exit(manager, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^manager, :killed}, 1_000
      end)
    after
      :sys.resume(queue)
    end

    # The exchange tables died with the Manager: neither request executed;
    # the dead anchor returned the claim.
    eventually!(fn -> match?(%{messages: 1, ready: 1, inflight: 0, consumers: 0}, Queue.status(queue)) end)
  end

  defp stand_in!(queues \\ ["orders", "other"]) do
    supervisor = start_supervised!({StandInAMQP, {options(queues), "orders", self()}}, id: make_ref())
    children = Supervisor.which_children(supervisor)
    {_, manager, _, _} = List.keyfind(children, Broker, 0)
    {_, stand_in, _, _} = List.keyfind(children, {Queue, "orders"}, 0)
    {_, listeners, _, _} = List.keyfind(children, :listeners, 0)
    [{{Listener, _}, listener, _, _}] = Supervisor.which_children(listeners)
    %{manager: manager, stand_in: stand_in, port: Listener.info(listener).port}
  end

  defp options(queues),
    do: [
      container_id: "requests-broker-#{System.unique_integer([:positive])}",
      auth: :anonymous,
      queues: queues,
      endpoint_options: [max_frame_size: 512],
      listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
    ]

  defp client!(port) do
    client =
      start_supervised!(
        {Client,
         host: {127, 0, 0, 1},
         port: port,
         container_id: "requests-client-#{System.unique_integer([:positive])}",
         max_frame_size: 512},
        id: make_ref()
      )

    assert_receive {:graviton_mq, ^client, {:connected, _}}, 2_000
    assert {:ok, 0} = operation(Client.open_session(client, 0), client)
    client
  end

  defp attach!(client, name, role, address) do
    assert {:ok, identity} =
             operation(Client.attach(client, 0, name, role, address: Value.string(address)), client)

    identity
  end

  defp publish!(port, address, body) do
    client = client!(port)
    link = attach!(client, "publisher-#{System.unique_integer([:positive])}", :sender, address)
    assert {:ok, %Outcome.Accepted{}} = operation(Client.send_message(client, 0, link, message(body)), client)
  end

  defp consume!(port, address) do
    client = client!(port)
    link = attach!(client, "#{address}-consumer", :receiver, address)
    assert {:ok, 1} = operation(Client.request(client, 0, link, 1, 1_048_576), client)
    assert_receive {:graviton_mq, ^client, {:delivery, delivery, 0, ^link, _, _}}, 2_000
    {client, link, delivery}
  end

  defp message(body) do
    {:ok, raw} = MessageCodec.encode_sections([{:data, Value.binary(body)}])
    raw
  end

  defp operation({:ok, reference}, client, timeout \\ 3_000) do
    assert_receive {:graviton_mq, ^client, {:operation, ^reference, result}}, timeout
    result
  end

  defp child(supervisor, identity) do
    {_, pid, _, _} = List.keyfind(Supervisor.which_children(supervisor), identity, 0)
    assert is_pid(pid)
    pid
  end

  defp eventually!(predicate, timeout \\ 3_000), do: until!(predicate, System.monotonic_time(:millisecond) + timeout)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline
      Process.sleep(10)
      until!(predicate, deadline)
    end
  end
end
```

- [ ] **Step 3: Run**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test apps/graviton_mq/test/graviton_mq/amqp10_manager_requests_test.exs
```
Expected: `5 tests, 0 failures`. If one fails, diagnose before changing anything; do not weaken the assertion. A real Manager bug is fixed in `broker.ex` in this task, test first, and recorded in the log.

- [ ] **Step 4: Mutation checks** (revert each mutation immediately after confirming the failure, and record the results in the log)
  1. In `answered(%{kind: :register}, …)`, replace the `_removed_or_replaced` body with `state` → B6 FAILS (`consumers` stays 1).
  2. In `fence/2`, drop the `abandoned/3` reduce → B4 enqueue still passes, B5 (Task 5) still passes. Then make `publication(:uncertain, _)` return `%Outcome.Released{}` → B4 enqueue FAILS (the publisher gets `Released`).
  3. In `answered(%{kind: :settle}, …)`, make `{_not_executed_or_uncertain, _}` return `state` → B4 settle FAILS (no detach).

- [ ] **Step 5: Stability run, then commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && for s in 1 2 3; do env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test apps/graviton_mq/test/graviton_mq/amqp10_manager_requests_test.exs apps/graviton_mq/test/graviton_mq/amqp10_availability_test.exs apps/graviton_mq/test/graviton_mq/amqp10_boundaries_test.exs --seed $s || break; done
env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format apps/graviton_mq/test/support/amqp10_stand_in_queue.exs apps/graviton_mq/test/graviton_mq/amqp10_manager_requests_test.exs
git add -- apps/graviton_mq/test/support/amqp10_stand_in_queue.exs apps/graviton_mq/test/graviton_mq/amqp10_manager_requests_test.exs
git commit --only -m "test(amqp10): stand-in queue fault injection and Manager death evidence" -- apps/graviton_mq/test/support/amqp10_stand_in_queue.exs apps/graviton_mq/test/graviton_mq/amqp10_manager_requests_test.exs
```
Expected: three runs, `0 failures` each.

---

## Task 7 (spec T5): Client `reconnect:` option and pure `Reconnect` policy

**Files:**
- Modify: `apps/graviton_mq_client/lib/graviton_mq/client/amqp10/config.ex`
- Create: `apps/graviton_mq_client/lib/graviton_mq/client/amqp10/reconnect.ex`
- Test: `apps/graviton_mq_client/test/amqp10_reconnect_test.exs` (new)

- [ ] **Step 1: Write the failing test**

```elixir
defmodule GravitonMQ.AMQP10Client.ReconnectTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.{Error, Value}
  alias GravitonMQ.Client.AMQP10.{Config, Reconnect}

  describe "configuration" do
    test "reconnect mirrors the MQTT policy bounds and defaults to one connection" do
      assert {:ok, %{reconnect: %{max_attempts: 0, delay: 250, max_delay: 5_000}}} =
               Config.build(container_id: "c", observer: self())

      assert {:ok, %{reconnect: %{max_attempts: 100, delay: 1, max_delay: 60_000}}} =
               Config.build(
                 container_id: "c",
                 observer: self(),
                 reconnect: [max_attempts: 100, delay: 1, max_delay: 60_000]
               )

      for reconnect <- [
            [max_attempts: -1],
            [max_attempts: 101],
            [max_attempts: :infinity],
            [delay: 0],
            [delay: 60_001],
            [delay: 500, max_delay: 499],
            [max_delay: 60_001],
            [jitter: true],
            :yes,
            [{"delay", 1}]
          ] do
        assert {:error, :invalid_options} =
                 Config.build(container_id: "c", observer: self(), reconnect: reconnect)
      end
    end
  end

  describe "policy" do
    test "the delay before attempt n is min(max_delay, delay * 2^(n-1)) without jitter" do
      policy = %{max_attempts: 10, delay: 250, max_delay: 5_000}
      assert Enum.map(1..7, &Reconnect.delay(policy, &1)) == [250, 500, 1_000, 2_000, 4_000, 5_000, 5_000]
      assert Reconnect.delay(%{policy | delay: 60_000, max_delay: 60_000}, 100) == 60_000
    end

    test "transient losses retry and terminal failures stop" do
      forced = %Error{condition: Value.symbol("amqp:connection:forced")}
      denied = %Error{condition: Value.symbol("amqp:unauthorized-access")}

      for reason <- [
            :closed,
            {:transport, :econnreset},
            {:write_failed, {:error, :timeout}},
            {:connect, :econnrefused},
            {:connect, :timeout},
            {:connect, :nxdomain},
            {:connect, :closed},
            :sasl_timeout,
            {:sasl, :sys_temp},
            :normal,
            {:peer_close, forced}
          ] do
        assert Reconnect.classify(reason, nil) == :retry, inspect(reason)
      end

      for reason <- [
            {:sasl, :auth},
            {:sasl, :sys},
            {:sasl, :sys_perm},
            {:sasl, {:unknown, 9}},
            :sasl_not_offered,
            :sasl_mechanism_not_offered,
            :sasl_protocol_error,
            :invalid_sasl_credentials,
            {:connect, {:tls_alert, :unknown_ca}},
            {:connect, :transport_error},
            {:write_failed, {:error, {:encode, :limit_exceeded, :frame_size_limit}}},
            {:decode_error, :input_buffer_limit},
            {:peer_close, denied},
            :owner_stopped,
            :close_timeout
          ] do
        assert Reconnect.classify(reason, nil) == :stop, inspect(reason)
      end

      # D1: a locally initiated Close is classified by its recorded cause.
      for reason <- [:normal, :close_timeout, :closed] do
        assert Reconnect.classify(reason, :idle_timeout) == :retry
        assert Reconnect.classify(reason, :open_timeout) == :retry
        assert Reconnect.classify(reason, :host_close) == :stop
        assert Reconnect.classify(reason, :protocol) == :stop
      end
    end
  end
end
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10_reconnect_test.exs
```
Expected: FAIL. The default-case match fails (the built config has no `:reconnect` key), `reconnect:` itself is rejected as an unknown option, and `Reconnect` is undefined.

- [ ] **Step 3: Implement**

`reconnect.ex`:

```elixir
defmodule GravitonMQ.Client.AMQP10.Reconnect do
  @moduledoc false

  # Pure reconnect policy of the AMQP 1.0 client shell (S3 §3.2–3.3). The
  # backoff is the MQTT client's formula, without jitter (owner Q3). `cause` is
  # the shell's record of why the Endpoint began a local Close (D1): the
  # Endpoint reports such a close as :normal, :close_timeout or :closed.

  alias GravitonMQ.AMQP10.{Error, Value}

  @transient_connect ~w(econnrefused econnreset econnaborted ehostunreach enetunreach nxdomain timeout etimedout closed)a

  def delay(%{delay: delay, max_delay: maximum}, attempt) when is_integer(attempt) and attempt >= 1,
    do: min(maximum, delay * Integer.pow(2, min(attempt - 1, 20)))

  def classify(_reason, :host_close), do: :stop
  def classify(_reason, cause) when cause in [:idle_timeout, :open_timeout], do: :retry
  def classify(_reason, :protocol), do: :stop
  def classify(reason, nil), do: transient(reason)

  defp transient(:closed), do: :retry
  defp transient({:transport, _reason}), do: :retry
  defp transient({:write_failed, {:error, {:encode, _class, _reason}}}), do: :stop
  defp transient({:write_failed, _result}), do: :retry
  defp transient({:connect, reason}) when reason in @transient_connect, do: :retry
  defp transient(reason) when reason in [:idle_timeout, :open_timeout, :sasl_timeout], do: :retry
  defp transient({:sasl, :sys_temp}), do: :retry
  # An unrequested peer Close without error: a broker shutdown or restart (Q4).
  defp transient(:normal), do: :retry

  defp transient(
         {:peer_close, %Error{condition: %Value{type: :symbol, value: "amqp:connection:forced"}}}
       ),
       do: :retry

  defp transient(_reason), do: :stop
end
```

`config.ex`: add `:reconnect` to `@keys` (after `:sasl_timeout`). In `assemble/1`, after `sasl = sasl(...)`, add `reconnect = reconnect(Keyword.get(options, :reconnect, []))`, append `and reconnect != :invalid` to `valid`, and add `|> Map.put(:reconnect, reconnect)` to the success pipeline. Add:

```elixir
  # Mirrors the MQTT client's reconnect policy (owner Q3): 0..100 attempts
  # (0 = one connection, the historical behaviour), exponential delay from
  # `delay` capped at `max_delay`, no jitter, no infinity.
  defp reconnect(options) when is_list(options) do
    if Keyword.keyword?(options) and
         Enum.all?(Keyword.keys(options), &(&1 in [:max_attempts, :delay, :max_delay])) do
      policy = %{
        max_attempts: Keyword.get(options, :max_attempts, 0),
        delay: Keyword.get(options, :delay, 250),
        max_delay: Keyword.get(options, :max_delay, 5_000)
      }

      if is_integer(policy.max_attempts) and policy.max_attempts in 0..100 and
           is_integer(policy.delay) and policy.delay in 1..60_000 and
           is_integer(policy.max_delay) and policy.max_delay in policy.delay..60_000,
         do: policy,
         else: :invalid
    else
      :invalid
    end
  end

  defp reconnect(_options), do: :invalid
```

- [ ] **Step 4: Run**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10_reconnect_test.exs test/amqp10_client_test.exs test/amqp10_sasl_client_test.exs && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix compile --warnings-as-errors
```
Expected: `0 failures`; compile clean.

- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format apps/graviton_mq_client/lib/graviton_mq/client/amqp10/config.ex apps/graviton_mq_client/lib/graviton_mq/client/amqp10/reconnect.ex apps/graviton_mq_client/test/amqp10_reconnect_test.exs
git add -- apps/graviton_mq_client/lib/graviton_mq/client/amqp10/config.ex apps/graviton_mq_client/lib/graviton_mq/client/amqp10/reconnect.ex apps/graviton_mq_client/test/amqp10_reconnect_test.exs
git commit --only -m "feat(amqp10-client): reconnect option and pure retry policy" -- apps/graviton_mq_client/lib/graviton_mq/client/amqp10/config.ex apps/graviton_mq_client/lib/graviton_mq/client/amqp10/reconnect.ex apps/graviton_mq_client/test/amqp10_reconnect_test.exs
```

---

## Task 8 (spec T6): Shell reconnect loop

**Files:**
- Modify: `apps/graviton_mq_client/lib/graviton_mq/client/amqp10.ex`
- Test: `apps/graviton_mq_client/test/amqp10_reconnect_test.exs` (append a `describe "shell"` block and helpers)

- [ ] **Step 1: Write the failing tests** (append inside the module)

```elixir
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.AMQP10.Codec.{Frame, Performative}
  alias GravitonMQ.AMQP10.Performative.{Close, Open}

  @header <<"AMQP", 0, 1, 0, 0>>

  describe "shell" do
    test "the default policy keeps one connection and a stopped, inspectable owner" do
      {listener, client} = client!([])
      socket = handshake!(listener, client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:disconnected, :closed}}, 1_000
      refute_receive {:graviton_mq, ^client, {:reconnecting, _}}, 100
      refute_received {:graviton_mq, ^client, {:reconnect_exhausted, _}}
      assert %{owner: :stopped, phase: :closed, attempt: 0, next_retry_ms: nil} = Client.status(client)
      assert {:error, :closed} = Client.open_session(client, 1)
      assert {:error, :timeout} = :gen_tcp.accept(listener, 200)
    end

    test "a transport loss reconnects on a fresh generation and fences old deliveries" do
      {listener, client} = client!(reconnect: [max_attempts: 3, delay: 300, max_delay: 300])
      socket = handshake!(listener, client)
      assert %{owner: :active, generation: 0} = Client.status(client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:disconnected, :closed}}, 1_000
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000

      assert %{owner: :offline, attempt: 1, next_retry_ms: remaining} = Client.status(client)
      assert remaining in 0..300
      # Nothing is queued and nothing claims success while offline.
      assert {:error, :disconnected} = Client.open_session(client, 1)
      assert {:error, :disconnected} = Client.request(client, 1, {Value.string("l"), :receiver}, 1, 1)

      socket = handshake!(listener, client)
      assert %{owner: :active, generation: 1, attempt: 0, next_retry_ms: nil} = Client.status(client)
      # A generation-0 delivery reference never reaches the new connection.
      assert {:error, :stale_delivery} =
               Client.settle(client, {client, 1, {0, 1, 0, 0, 0}}, %GravitonMQ.AMQP10.Outcome.Accepted{})

      assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 50)
    end

    test "peer Close is retried only for connection:forced or an unrequested error-less Close" do
      for {error, action} <- [
            {%Error{condition: Value.symbol("amqp:connection:forced")}, :retry},
            {nil, :retry},
            {%Error{condition: Value.symbol("amqp:unauthorized-access")}, :stop}
          ] do
        {listener, client} = client!(reconnect: [max_attempts: 1, delay: 20, max_delay: 20])
        socket = handshake!(listener, client)
        :ok = :gen_tcp.send(socket, frame(%Close{error: error}))
        assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 1_000

        case action do
          :retry ->
            assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
            handshake!(listener, client)

          :stop ->
            refute_receive {:graviton_mq, ^client, {:reconnecting, _}}, 100
            assert %{owner: :stopped} = Client.status(client)
        end
      end
    end

    test "a host close is final" do
      {listener, client} = client!(reconnect: [max_attempts: 3, delay: 20, max_delay: 20])
      socket = handshake!(listener, client)
      assert {:ok, closing} = Client.close(client)
      assert %Close{error: nil} = recv_pdu!(socket)
      :ok = :gen_tcp.send(socket, frame(%Close{}))
      assert_receive {:graviton_mq, ^client, {:operation, ^closing, {:ok, :closed}}}, 1_000
      assert_receive {:graviton_mq, ^client, {:disconnected, :normal}}, 1_000
      refute_receive {:graviton_mq, ^client, {:reconnecting, _}}, 100
      assert %{owner: :stopped} = Client.status(client)
    end

    test "the budget resets only when the peer's Open arrives and exhaustion is reported" do
      {listener, client} = client!(reconnect: [max_attempts: 2, delay: 20, max_delay: 20])
      socket = handshake!(listener, client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000

      # TCP is accepted but the peer never sends Open: no reset.
      for attempt <- [2] do
        {:ok, refused} = :gen_tcp.accept(listener, 1_000)
        :ok = :gen_tcp.close(refused)
        assert_receive {:graviton_mq, ^client, {:reconnecting, ^attempt}}, 1_000
      end

      {:ok, refused} = :gen_tcp.accept(listener, 1_000)
      :ok = :gen_tcp.close(refused)
      assert_receive {:graviton_mq, ^client, {:reconnect_exhausted, 2}}, 1_000
      assert %{owner: :stopped, next_retry_ms: nil} = Client.status(client)
      assert %{socket: nil, retry: nil, timers: timers} = :sys.get_state(client)
      assert timers == %{}
      assert {:error, :timeout} = :gen_tcp.accept(listener, 200)
    end

    test "backoff gaps follow the formula" do
      {listener, client} = client!(reconnect: [max_attempts: 4, delay: 40, max_delay: 100])
      socket = handshake!(listener, client)
      :ok = :gen_tcp.close(socket)

      accepted =
        for _ <- 1..4 do
          {:ok, refused} = :gen_tcp.accept(listener, 2_000)
          at = System.monotonic_time(:millisecond)
          :ok = :gen_tcp.close(refused)
          at
        end

      gaps = accepted |> Enum.chunk_every(2, 1, :discard) |> Enum.map(fn [a, b] -> b - a end)

      # Gap k separates attempts k+1 and k+2: delay(k+1) = 80, 100, 100.
      for {gap, minimum} <- Enum.zip(gaps, [80, 100, 100]) do
        assert gap >= minimum, "gap #{gap} ms < #{minimum} ms"
        assert gap <= minimum + 500, "gap #{gap} ms is far above #{minimum} ms"
      end

      assert_receive {:graviton_mq, ^client, {:reconnect_exhausted, 4}}, 1_000
    end

    test "close/1 while offline stops at once; observer death while offline stops the owner" do
      {listener, client} = client!(reconnect: [max_attempts: 3, delay: 5_000, max_delay: 5_000])
      socket = handshake!(listener, client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
      assert {:ok, closing} = Client.close(client)
      assert_receive {:graviton_mq, ^client, {:operation, ^closing, {:ok, :closed}}}, 100
      assert %{owner: :stopped, next_retry_ms: nil} = Client.status(client)
      assert {:error, :timeout} = :gen_tcp.accept(listener, 200)

      observer = spawn(fn -> receive(do: (:stop -> :ok)) end)

      {listener, client} =
        client!(observer: observer, reconnect: [max_attempts: 3, delay: 5_000, max_delay: 5_000])

      {:ok, socket} = :gen_tcp.accept(listener, 1_000)
      assert {:ok, @header} = :gen_tcp.recv(socket, 8, 1_000)
      assert %Open{} = recv_pdu!(socket)
      :ok = :gen_tcp.send(socket, @header <> frame(open()))
      eventually!(fn -> Client.status(client).owner == :active end)
      monitor = Process.monitor(client)
      assert %{owner: :active} = Client.status(client)
      :ok = :gen_tcp.close(socket)
      eventually!(fn -> Client.status(client).owner == :offline end)
      send(observer, :stop)
      assert_receive {:DOWN, ^monitor, :process, ^client, :normal}, 1_000
    end

    test "a receive idle timeout against a silent peer reconnects" do
      {listener, client} =
        client!(
          idle_timeout_ms: 200,
          close_timeout_ms: 50,
          reconnect: [max_attempts: 1, delay: 20, max_delay: 20]
        )

      socket = handshake!(listener, client)
      assert %Close{} = recv_pdu!(socket, 2_000)
      assert_receive {:graviton_mq, ^client, {:disconnected, :close_timeout}}, 1_000
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
      handshake!(listener, client)
    end
  end

  defp client!(options) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    options =
      Keyword.merge([container_id: "reconnect-client", host: "127.0.0.1", port: port], options)

    {listener, start_supervised!(Supervisor.child_spec({Client, options}, id: make_ref()))}
  end

  # Accepts one connection, completes header + Open and waits for :connected.
  defp handshake!(listener, client) do
    {:ok, socket} = :gen_tcp.accept(listener, 2_000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    assert {:ok, @header} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = recv_pdu!(socket)
    :ok = :gen_tcp.send(socket, @header <> frame(open()))
    assert_receive {:graviton_mq, ^client, {:connected, "peer"}}, 1_000
    socket
  end

  defp open, do: %Open{container_id: Value.string("peer"), max_frame_size: Value.uint(512)}

  defp frame(pdu, channel \\ 0) do
    {:ok, bytes} = Performative.encode(pdu)
    <<8 + byte_size(bytes)::32, 2, 0, channel::16, bytes::binary>>
  end

  defp recv_pdu!(socket, timeout \\ 1_000) do
    {_channel, pdu} = recv_frame!(socket, timeout)
    pdu
  end

  defp recv_frame!(socket, timeout \\ 1_000) do
    assert {:ok, <<size::32>>} = :gen_tcp.recv(socket, 4, timeout)
    assert {:ok, rest} = :gen_tcp.recv(socket, size - 4, timeout)
    assert {:ok, frame, <<>>} = Frame.decode(<<size::32, rest::binary>>)
    assert {:ok, pdu, _payload} = Performative.decode(frame.body)
    {frame.channel, pdu}
  end

  defp eventually!(predicate, timeout \\ 2_000),
    do: until!(predicate, System.monotonic_time(:millisecond) + timeout)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline
      Process.sleep(5)
      until!(predicate, deadline)
    end
  end
```

Make the module `async: false` at this point (it now starts listeners and timing-sensitive owners).

- [ ] **Step 2: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10_reconnect_test.exs
```
Expected: the shell tests FAIL with `KeyError`/match errors on `owner:` (absent from status), and no `{:reconnecting, 1}` arrives.

- [ ] **Step 3: Implement in `client/amqp10.ex`**

3a. Aliases: `alias GravitonMQ.Client.AMQP10.{Config, Reconnect}`. In `start_link/1`, replace the `Stream.new(...)` accept expression with `accept: accept(config)`, and add:

```elixir
  defp accept(%{sasl: :none}), do: [0]
  defp accept(_config), do: [0, 3]
```

3b. `initialize/1` state map: add

```elixir
       owner: :connecting,
       attempt: 0,
       retry: nil,
       close_cause: nil,
       lost: nil,
```

3c. Replace `continued(:connect, state)` with:

```elixir
  defp continued(:connect, state), do: {:noreply, connect(state)}

  defp connect(state) do
    case Transport.connect(state.config) do
      {:ok, socket} -> state |> Map.put(:socket, socket) |> begin() |> arm_socket()
      {:error, reason} -> step(state, {:transport_down, {:connect, transport_reason(reason)}})
    end
  end
```

3d. Replace the `call(:status, …)` clause with:

```elixir
  defp call(:status, _from, state) do
    {:reply,
     %{
       phase: state.endpoint.phase,
       owner: state.owner,
       generation: state.endpoint.generation,
       attempt: state.attempt,
       next_retry_ms: next_retry_ms(state),
       pending: map_size(state.operations),
       sessions: map_size(state.endpoint.connection.sessions_by_identity),
       writes: map_size(state.endpoint.writes),
       buffered_bytes: byte_size(state.input) + byte_size(state.stream.buffer)
     }, state}
  end
```

3e. Replace the first `call({:settle_delivery, …})` clause and `call({:command, command}, …)` with:

```elixir
  defp call({:settle_delivery, {owner, session, token}, outcome}, from, state)
       when owner == self() do
    if stale_delivery?(state, token),
      do: {:reply, {:error, :stale_delivery}, state},
      else: call({:command, {:session, session, {:settle, token, outcome}}}, from, state)
  end
```

(keep the `{:settle_delivery, _reference, _outcome}` fallback clause right after it)

```elixir
  defp call({:command, command}, {caller, _tag}, state) do
    if offline?(state) do
      offline_command(state, command, caller)
    else
      case admit(state, command, %{caller: caller}) do
        {:ok, reference, state} -> {:reply, {:ok, reference}, state}
        {:error, reason, state} -> {:reply, {:error, reason}, state}
      end
    end
  end
```

Add the helpers after the `call/3` group (not inside it):

```elixir
  # Between connections nothing is queued and nothing claims success (D3).
  defp offline?(%{owner: :offline}), do: true
  defp offline?(%{owner: :connecting, endpoint: %{generation: generation}}), do: generation > 0
  defp offline?(_state), do: false

  defp offline_command(state, :close, caller) do
    reference = make_ref()
    state = %{cancel_retry(state) | close_cause: :host_close}

    state =
      if state.owner == :connecting,
        do: route(state, {:transport_down, :owner_stopped}),
        else: state

    send(caller, {:graviton_mq, self(), {:operation, reference, {:ok, :closed}}})
    {:reply, {:ok, reference}, %{state | owner: :stopped}}
  end

  defp offline_command(state, _command, _caller), do: {:reply, {:error, :disconnected}, state}

  # Shared by host commands and (Task 9) restoration. `owner` is stored with
  # the operation: `%{caller: pid}` or `%{caller: :restore, item: key}`.
  defp admit(state, command, owner) do
    if map_size(state.operations) >= state.config.max_operations do
      {:error, :operation_limit, state}
    else
      reference = make_ref()
      before = state.endpoint.phase
      event = {:command, reference, command}
      {endpoint, effects, link_locks} = transition(state, event)

      rejected =
        Enum.find_value(effects, fn
          {:operation, ^reference, {:error, reason}} -> reason
          _ -> nil
        end)

      state = track_close(%{state | endpoint: endpoint, link_locks: link_locks}, before, event)

      if rejected do
        {:error, rejected, execute(state, effects)}
      else
        state = %{state | operations: Map.put(state.operations, reference, owner)}
        {:ok, reference, execute(state, effects)}
      end
    end
  end

  # An incoming delivery token is {generation, sid, incarnation, n, id}.
  defp stale_delivery?(state, token) when is_tuple(token) and tuple_size(token) == 5,
    do: is_integer(elem(token, 0)) and elem(token, 0) != state.endpoint.generation

  defp stale_delivery?(_state, _token), do: false

  defp next_retry_ms(%{retry: {_timer, _token, at}}), do: max(0, at - now())
  defp next_retry_ms(_state), do: nil
```

3f. `info/2`: add before the observer `{:DOWN, …}` clause:

```elixir
  defp info({:retry, token}, %{retry: {_timer, token, _at}} = state) do
    generation = state.endpoint.generation + 1
    options = Keyword.put(state.config.endpoint_options, :generation, generation)
    # Both were validated by the identical first construction in start_link/1.
    {:ok, endpoint} = Endpoint.new(options)
    {:ok, stream} = Stream.new(state.config.limits, accept: accept(state.config))

    state = %{
      state
      | retry: nil,
        owner: :connecting,
        endpoint: endpoint,
        stream: stream,
        sasl: nil,
        close_cause: nil,
        lost: nil,
        input: <<>>,
        drain_pending: false
    }

    {:noreply, connect(state)}
  end
```

3g. Replace `step/2`:

```elixir
  defp step(state, event) do
    before = state.endpoint.phase
    {endpoint, effects, link_locks} = transition(state, event)
    state = track_close(%{state | endpoint: endpoint, link_locks: link_locks}, before, event)
    execute(state, effects)
  end

  # D1: remember why this Endpoint began a local Close.
  defp track_close(state, before, event) do
    if before not in [:closing, :closed] and state.endpoint.phase == :closing,
      do: %{state | close_cause: close_cause(event)},
      else: state
  end

  defp close_cause({:command, _reference, :close}), do: :host_close
  defp close_cause({:timeout, :receive_idle, _token}), do: :idle_timeout
  defp close_cause({:timeout, :open, _token}), do: :open_timeout
  defp close_cause(_event), do: :protocol
```

3h. Effects. Replace `effect({:operation, …})`, `effect({:event, event})` and `effect({:close_transport, …})`. Keep each effect clause inside the existing `effect/2` group, with the specific `:event` clauses before the generic one.

```elixir
  defp effect({:operation, reference, result}, state) do
    case Map.pop(state.operations, reference) do
      {nil, _operations} ->
        state

      {%{caller: caller}, operations} ->
        send(caller, {:graviton_mq, self(), {:operation, reference, result}})
        %{state | operations: operations}
    end
  end
```

```elixir
  defp effect({:event, {:connected, _container} = event}, state) do
    notify(state, event)
    %{state | owner: :active, attempt: 0}
  end

  defp effect({:event, {:disconnected, reason} = event}, state) do
    notify(state, event)
    %{state | lost: reason}
  end

  defp effect({:event, event}, state) do
    notify(state, event)
    state
  end
```

```elixir
  # Release the transport first, then decide: a stopped owner holds no socket
  # or timer (MQTT parity, spec §3.7).
  defp effect({:close_transport, generation, _reason}, state) do
    if generation == state.endpoint.generation,
      do: state |> close_socket() |> after_loss(),
      else: state
  end
```

Add after `close_socket/1`:

```elixir
  defp after_loss(%{lost: nil} = state), do: state

  defp after_loss(state) do
    reason = state.lost
    policy = state.config.reconnect
    state = %{state | lost: nil}

    cond do
      Reconnect.classify(reason, state.close_cause) == :stop ->
        %{state | owner: :stopped}

      state.attempt >= policy.max_attempts ->
        if policy.max_attempts > 0, do: notify(state, {:reconnect_exhausted, state.attempt})
        %{state | owner: :stopped}

      true ->
        attempt = state.attempt + 1
        notify(state, {:reconnecting, attempt})
        schedule_retry(%{state | owner: :offline, attempt: attempt}, Reconnect.delay(policy, attempt))
    end
  end

  defp schedule_retry(state, delay) do
    token = make_ref()
    timer = Process.send_after(self(), {:retry, token}, delay)
    %{state | retry: {timer, token, now() + delay}}
  end

  defp cancel_retry(%{retry: {timer, _token, _at}} = state) do
    Process.cancel_timer(timer)
    %{state | retry: nil}
  end

  defp cancel_retry(state), do: state
```

3i. `terminate/2`: change its first line inside `protect` to `state = cancel_retry(state)`, followed by the existing logic (rebinding `state`).

- [ ] **Step 4: Run all client AMQP suites**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix compile --warnings-as-errors && for s in 1 2 3; do env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10_reconnect_test.exs test/amqp10_client_test.exs test/amqp10_sasl_client_test.exs test/tls_transport_test.exs test/privacy_test.exs --seed $s || break; done
```
Expected: `0 failures` for each seed. The existing tests prove that the `max_attempts: 0` default is unchanged: `phase == :closed`, `{:error, :closed}`, `{:error, :not_connected}` during the first connection, and SASL `assert_sasl_closed!` with `timers == %{}`.

- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format apps/graviton_mq_client/lib/graviton_mq/client/amqp10.ex apps/graviton_mq_client/test/amqp10_reconnect_test.exs
git add -- apps/graviton_mq_client/lib/graviton_mq/client/amqp10.ex apps/graviton_mq_client/test/amqp10_reconnect_test.exs
git commit --only -m "feat(amqp10-client): bounded reconnect loop with generation fencing" -- apps/graviton_mq_client/lib/graviton_mq/client/amqp10.ex apps/graviton_mq_client/test/amqp10_reconnect_test.exs
```

---

## Task 9 (spec T7): `Intent`, restoration and events

**Files:**
- Create: `apps/graviton_mq_client/lib/graviton_mq/client/amqp10/intent.ex`
- Create: `apps/graviton_mq_client/test/amqp10_intent_test.exs`
- Modify: `apps/graviton_mq_client/lib/graviton_mq/client/amqp10.ex`, `apps/graviton_mq_client/lib/graviton_mq/amqp10_client.ex` (moduledoc)
- Test: `apps/graviton_mq_client/test/amqp10_reconnect_test.exs` (append a `describe "restoration"` block)

- [ ] **Step 1: Write the failing pure test**

```elixir
defmodule GravitonMQ.Client.AMQP10.IntentTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Client.AMQP10.Intent

  @a {"a", :sender}
  @b {"b", :receiver}

  defp declared do
    Intent.new()
    |> Intent.put_session(2, [])
    |> Intent.put_session(1, incoming_window: 8)
    |> Intent.put_link(1, @a, address: "q")
    |> Intent.put_link(1, @b, address: "r")
    |> Intent.put_link(9, {"orphan", :sender}, [])
  end

  test "declarations are bounded identities and options, removable by the host or peer" do
    intent = declared()
    assert Intent.counts(intent) == %{sessions: 2, links: 2}
    assert Intent.counts(Intent.drop_link(intent, 1, @a)) == %{sessions: 2, links: 1}
    assert Intent.counts(Intent.drop_session(intent, 1)) == %{sessions: 1, links: 0}
    refute Intent.pending?(intent, 1, nil)
  end

  test "restoration opens Sessions in ascending order, then each Session's Links in order" do
    intent = Intent.start(declared())
    assert Intent.pending?(intent, 1, nil) and Intent.pending?(intent, 2, @a)
    assert Intent.restoring(intent) == 4
    assert {[{{:session, 1}, {:open_session, 1, [incoming_window: 8]}}], intent} = Intent.take(intent, 1)
    assert {[{{:session, 2}, {:open_session, 2, []}}], intent} = Intent.take(intent, 8)
    assert {[], intent} = Intent.take(intent, 8)
    assert {intent, [{{:session, 1}, {:ok, 1}}]} = Intent.complete(intent, {:session, 1}, {:ok, 1})
    refute Intent.pending?(intent, 1, nil)
    assert Intent.pending?(intent, 1, @a)

    assert {[
              {{:link, 1, @a}, {:session, 1, {:attach, @a, [address: "q"]}}},
              {{:link, 1, @b}, {:session, 1, {:attach, @b, [address: "r"]}}}
            ], intent} = Intent.take(intent, 8)

    {intent, _} = Intent.complete(intent, {:link, 1, @a}, {:ok, @a})
    {intent, [{{:link, 1, @b}, {:error, :refused}}]} = Intent.complete(intent, {:link, 1, @b}, {:error, :refused})
    refute Intent.finished?(intent)
    {intent, _} = Intent.complete(intent, {:session, 2}, {:ok, 2})
    assert Intent.finished?(intent)
    assert {%{restored: 3, failed: 1}, intent} = Intent.finish(intent)
    assert Intent.counts(intent) == %{sessions: 2, links: 1}
    refute Intent.finished?(intent)
  end

  test "a failed Session drops its Links with explicit reports; interruption keeps entries" do
    intent = Intent.start(declared())
    {_commands, intent} = Intent.take(intent, 8)

    assert {intent,
            [
              {{:session, 1}, {:unknown, :timeout}},
              {{:link, 1, @a}, {:error, :session_not_restored}},
              {{:link, 1, @b}, {:error, :session_not_restored}}
            ]} = Intent.complete(intent, {:session, 1}, {:unknown, :timeout})

    assert Intent.counts(intent) == %{sessions: 1, links: 0}
    intent = intent |> Intent.interrupt() |> Intent.stop()
    assert Intent.counts(intent) == %{sessions: 1, links: 0}
    refute Intent.waiting?(intent)
    assert {[{{:session, 2}, _}], _} = intent |> Intent.start() |> Intent.take(8)
  end
end
```

- [ ] **Step 2: Write the failing shell tests** (append to `amqp10_reconnect_test.exs`; add `alias GravitonMQ.AMQP10.Performative.{Attach, Begin, Detach}` and `alias GravitonMQ.AMQP10.Terminus.{Source, Target}`)

```elixir
  describe "restoration" do
    test "declared Sessions and Links are restored in order and reported; in-flight ones are not" do
      {listener, client} = client!(reconnect: [max_attempts: 2, delay: 20, max_delay: 20])
      socket = handshake!(listener, client)
      session!(client, socket, 2)
      session!(client, socket, 1)
      channel = 1
      kept = sender!(client, socket, channel, "kept", 9)
      refused = sender!(client, socket, channel, "refused-later", 10)

      assert {:ok, inflight} =
               Client.attach(client, 1, "in-flight", :sender, address: Value.string("q"))

      assert {^channel, %Attach{}} = recv_frame!(socket)
      assert %{intent: %{sessions: 2, links: 2}} = Client.status(client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:operation, ^inflight, {:unknown, :closed}}}, 1_000
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
      socket = handshake!(listener, client)

      # Ascending Session identity: Session 1 gets the first channel.
      assert {0, %Begin{}} = recv_frame!(socket)
      assert {1, %Begin{}} = recv_frame!(socket)
      assert %{owner: :restoring, restoring: 4} = Client.status(client)
      assert {:error, :restoring} = Client.send_message(client, 1, kept, <<0, 0x53, 0x77, 0x40>>)

      assert {:error, :restoring} =
               Client.attach(client, 1, "new", :sender, address: Value.string("q"))

      :ok = :gen_tcp.send(socket, begin_frame(0) <> begin_frame(1))
      assert_receive {:graviton_mq, ^client, {:restored, {:session, 1}, {:ok, 1}}}, 1_000
      assert_receive {:graviton_mq, ^client, {:restored, {:session, 2}, {:ok, 2}}}, 1_000
      assert {0, %Attach{name: %Value{value: "kept"}} = first} = recv_frame!(socket)
      assert {0, %Attach{name: %Value{value: "refused-later"}} = second} = recv_frame!(socket)
      :ok = :gen_tcp.send(socket, attach_reply(first, 0, 9) <> refusal(second, 0, 10))
      assert_receive {:graviton_mq, ^client, {:restored, {:link, 1, ^kept}, {:ok, ^kept}}}, 1_000
      assert_receive {:graviton_mq, ^client, {:restored, {:link, 1, ^refused}, {:error, _}}}, 1_000
      assert_receive {:graviton_mq, ^client, {:restore_complete, %{restored: 3, failed: 1}}}, 1_000
      assert %{owner: :active, restoring: 0, intent: %{sessions: 2, links: 1}} = Client.status(client)
      refute Enum.any?(drain_frames(socket), &match?({_, %Attach{name: %Value{value: "in-flight"}}}, &1))
    end

    test "host detach and End remove declarations at admission" do
      {listener, client} = client!(reconnect: [max_attempts: 1, delay: 20, max_delay: 20])
      socket = handshake!(listener, client)
      session!(client, socket, 1)
      session!(client, socket, 2)
      link = sender!(client, socket, 0, "detached", 9)
      assert {:ok, _} = Client.detach(client, 1, link)
      assert {:ok, _} = Client.end_session(client, 2)
      assert %{intent: %{sessions: 1, links: 0}} = Client.status(client)
      :ok = :gen_tcp.close(socket)
      assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
      socket = handshake!(listener, client)
      assert {0, %Begin{}} = recv_frame!(socket)
      :ok = :gen_tcp.send(socket, begin_frame(0))
      assert_receive {:graviton_mq, ^client, {:restore_complete, %{restored: 1, failed: 0}}}, 1_000
      assert drain_frames(socket) == []
    end

    test "an empty intent still reports completion after every reconnect, never on first connect" do
      {listener, client} = client!(reconnect: [max_attempts: 1, delay: 20, max_delay: 20])
      socket = handshake!(listener, client)
      refute_received {:graviton_mq, ^client, {:restore_complete, _}}
      :ok = :gen_tcp.close(socket)
      handshake!(listener, client)
      assert_receive {:graviton_mq, ^client, {:restore_complete, %{restored: 0, failed: 0}}}, 1_000
    end
  end

  defp session!(client, socket, identity) do
    assert {:ok, operation} = Client.open_session(client, identity)
    assert {channel, %Begin{}} = recv_frame!(socket)
    :ok = :gen_tcp.send(socket, begin_frame(channel))
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^identity}}}, 1_000
  end

  defp sender!(client, socket, channel, name, handle) do
    session = channel_session(client, channel)
    assert {:ok, operation} = Client.attach(client, session, name, :sender, address: Value.string("q"))
    assert {^channel, %Attach{} = local} = recv_frame!(socket)
    :ok = :gen_tcp.send(socket, attach_reply(local, channel, handle))
    identity = {Value.string(name), :sender}
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^identity}}}, 1_000
    identity
  end

  # Sessions opened as 2 then 1 hold channels 0 and 1; tests pass the channel.
  defp channel_session(_client, 0), do: 1
  defp channel_session(_client, 1), do: 1

  defp begin_frame(channel),
    do:
      frame(
        %Begin{
          remote_channel: Value.ushort(channel),
          next_outgoing_id: Value.uint(0),
          incoming_window: Value.uint(16),
          outgoing_window: Value.uint(16)
        },
        channel
      )

  defp attach_reply(local, channel, handle),
    do:
      frame(
        %{
          local
          | handle: Value.uint(handle),
            role: Value.boolean(true),
            source: %Source{address: Value.string("q")},
            target: %Target{address: Value.string("q")},
            initial_delivery_count: nil
        },
        channel
      )

  defp refusal(local, channel, handle),
    do:
      frame(
        %{
          local
          | handle: Value.uint(handle),
            role: Value.boolean(true),
            source: %Source{address: Value.string("q")},
            target: nil,
            initial_delivery_count: nil
        },
        channel
      ) <>
        frame(
          %Detach{
            handle: Value.uint(handle),
            closed: Value.boolean(true),
            error: %Error{condition: Value.symbol("amqp:not-found")}
          },
          channel
        )

  defp drain_frames(socket, frames \\ []) do
    case :gen_tcp.recv(socket, 4, 100) do
      {:ok, <<size::32>>} ->
        {:ok, rest} = :gen_tcp.recv(socket, size - 4, 1_000)
        {:ok, frame, <<>>} = Frame.decode(<<size::32, rest::binary>>)

        case Performative.decode(frame.body) do
          {:ok, pdu, _} -> drain_frames(socket, [{frame.channel, pdu} | frames])
          _heartbeat -> drain_frames(socket, frames)
        end

      {:error, :timeout} ->
        Enum.reverse(frames)
    end
  end
```

In the first test, `session!(client, socket, 2)` takes channel 0 and `session!(client, socket, 1)` takes channel 1. So `kept`/`refused` are attached on channel 1, which is session 1; `channel_session(_, 1)` returns 1. In the second test, session 1 is opened first and has channel 0, so `channel_session(_, 0)` returns 1. After reconnect, session 1 is restored first and gets channel 0.

- [ ] **Step 3: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10_intent_test.exs test/amqp10_reconnect_test.exs
```
Expected: `Intent` undefined; the shell restoration tests time out waiting for `{0, %Begin{}}` after reconnect.

- [ ] **Step 4: Implement `intent.ex`**

```elixir
defmodule GravitonMQ.Client.AMQP10.Intent do
  @moduledoc false

  # Pure, bounded record of the Sessions and Links the host successfully
  # declared (S3 owner Q1), and of the progress restoring them on a new
  # Connection. No process and no clock. It stores identities and declaration
  # options only: never message bytes, credit, deliveries or credentials.
  # An entry is added only for a declaration the live Endpoint completed with
  # {:ok, _}, so it is bounded by the Endpoint's max_sessions x max_links.

  defstruct sessions: %{}, queue: [], outstanding: 0, active: false, restored: 0, failed: 0

  def new, do: %__MODULE__{}

  def put_session(intent, sid, options),
    do: %{intent | sessions: Map.put(intent.sessions, sid, %{options: options, status: :live, links: []})}

  def put_link(intent, sid, identity, options) do
    case Map.fetch(intent.sessions, sid) do
      {:ok, session} ->
        links = List.keystore(session.links, identity, 0, {identity, options, :live})
        put(intent, sid, %{session | links: links})

      :error ->
        intent
    end
  end

  def drop_session(intent, sid), do: %{intent | sessions: Map.delete(intent.sessions, sid)}

  def drop_link(intent, sid, identity) do
    case Map.fetch(intent.sessions, sid) do
      {:ok, session} -> put(intent, sid, %{session | links: List.keydelete(session.links, identity, 0)})
      :error -> intent
    end
  end

  def counts(intent) do
    %{
      sessions: map_size(intent.sessions),
      links: intent.sessions |> Map.values() |> Enum.map(&length(&1.links)) |> Enum.sum()
    }
  end

  def restoring(intent) do
    Enum.reduce(intent.sessions, 0, fn {_sid, session}, total ->
      total + if(session.status == :pending, do: 1, else: 0) +
        Enum.count(session.links, &match?({_, _, :pending}, &1))
    end)
  end

  # After a new Open every entry waits again; Sessions go first, ascending.
  def start(intent) do
    sessions =
      Map.new(intent.sessions, fn {sid, session} ->
        links = Enum.map(session.links, fn {identity, options, _} -> {identity, options, :pending} end)
        {sid, %{session | status: :pending, links: links}}
      end)

    queue = sessions |> Map.keys() |> Enum.sort() |> Enum.map(&{:session, &1})

    %{intent | sessions: sessions, queue: queue, outstanding: 0, active: true, restored: 0, failed: 0}
  end

  # Another loss interrupted restoration; entries stay declared (D13).
  def stop(intent), do: %{intent | queue: [], outstanding: 0, active: false}

  def pending?(intent, sid, identity) do
    case Map.fetch(intent.sessions, sid) do
      {:ok, %{status: :pending}} -> true
      {:ok, session} -> identity != nil and match?({_, _, :pending}, List.keyfind(session.links, identity, 0))
      :error -> false
    end
  end

  def waiting?(intent), do: intent.active and intent.queue != []
  def finished?(intent), do: intent.active and intent.queue == [] and intent.outstanding == 0

  def finish(intent),
    do: {%{restored: intent.restored, failed: intent.failed}, %{intent | active: false}}

  def take(intent, count) when count <= 0, do: {[], intent}

  def take(intent, count) do
    {keys, rest} = Enum.split(intent.queue, count)
    commands = Enum.flat_map(keys, &command(intent, &1))
    {commands, %{intent | queue: rest, outstanding: intent.outstanding + length(commands)}}
  end

  def interrupt(intent), do: %{intent | outstanding: max(intent.outstanding - 1, 0)}

  # Returns the reports `{key, result}` the shell sends as `{:restored, key, result}`.
  def complete(intent, key, result) do
    intent = interrupt(intent)

    case {key, result} do
      {{:session, sid}, {:ok, _}} ->
        case Map.fetch(intent.sessions, sid) do
          {:ok, session} ->
            links = Enum.map(session.links, fn {identity, _, _} -> {:link, sid, identity} end)
            intent = put(intent, sid, %{session | status: :live})
            {%{intent | queue: intent.queue ++ links, restored: intent.restored + 1}, [{key, result}]}

          :error ->
            {%{intent | restored: intent.restored + 1}, [{key, result}]}
        end

      {{:session, sid}, _failure} ->
        links =
          case Map.fetch(intent.sessions, sid) do
            {:ok, session} -> session.links
            :error -> []
          end

        reports =
          [{key, result} | Enum.map(links, fn {identity, _, _} -> {{:link, sid, identity}, {:error, :session_not_restored}} end)]

        intent = drop_session(intent, sid)
        {%{intent | failed: intent.failed + length(reports)}, reports}

      {{:link, sid, identity}, {:ok, _}} ->
        intent =
          case Map.fetch(intent.sessions, sid) do
            {:ok, session} ->
              links =
                Enum.map(session.links, fn
                  {^identity, options, _} -> {identity, options, :live}
                  link -> link
                end)

              put(intent, sid, %{session | links: links})

            :error ->
              intent
          end

        {%{intent | restored: intent.restored + 1}, [{key, result}]}

      {{:link, sid, identity}, _failure} ->
        intent = drop_link(intent, sid, identity)
        {%{intent | failed: intent.failed + 1}, [{key, result}]}
    end
  end

  defp command(intent, {:session, sid} = key) do
    case Map.fetch(intent.sessions, sid) do
      {:ok, %{status: :pending, options: options}} -> [{key, {:open_session, sid, options}}]
      _ -> []
    end
  end

  defp command(intent, {:link, sid, identity} = key) do
    with {:ok, session} <- Map.fetch(intent.sessions, sid),
         {^identity, options, :pending} <- List.keyfind(session.links, identity, 0) do
      [{key, {:session, sid, {:attach, identity, options}}}]
    else
      _ -> []
    end
  end

  defp put(intent, sid, session), do: %{intent | sessions: Map.put(intent.sessions, sid, session)}
end
```

- [ ] **Step 5: Integrate into the shell (`client/amqp10.ex`)**

5a. `alias GravitonMQ.Client.AMQP10.{Config, Intent, Reconnect}`; add `@restore_window 8` under the `use GenServer` line. Add `intent: Intent.new(), restore_pending: false` to the initial state.

5b. Status: add `intent: Intent.counts(state.intent)` and `restoring: restoring(state)`, and change `owner: state.owner` to `owner: owner(state)`:

```elixir
  defp restoring(%{intent: %{active: true} = intent}), do: Intent.restoring(intent)
  defp restoring(_state), do: 0

  defp owner(%{owner: :active} = state),
    do: if(restoring(state) > 0, do: :restoring, else: :active)

  defp owner(state), do: state.owner
```

5c. Host commands. Replace the `else` branch of `call({:command, command}, …)` with:

```elixir
      cond do
        restoring_target?(state, command) ->
          {:reply, {:error, :restoring}, state}

        true ->
          case admit(state, command, %{caller: caller, declare: declaration(command)}) do
            {:ok, reference, state} -> {:reply, {:ok, reference}, retract(state, command)}
            {:error, reason, state} -> {:reply, {:error, reason}, state}
          end
      end
```

Add the helpers next to `offline?/1`:

```elixir
  defp restoring_target?(%{intent: %{active: true} = intent}, command) do
    case target(command) do
      {sid, identity} -> Intent.pending?(intent, sid, identity)
      nil -> false
    end
  end

  defp restoring_target?(_state, _command), do: false

  defp target({:open_session, sid, _options}), do: {sid, nil}
  defp target({:end_session, sid}), do: {sid, nil}
  defp target({:session, sid, command}), do: {sid, link_identity(command)}
  defp target(_command), do: nil

  defp link_identity({:attach, identity, _options}), do: identity
  defp link_identity({:send, identity, _raw, _options}), do: identity
  defp link_identity({:demand, identity, _slots, _bytes}), do: identity
  defp link_identity({:detach, identity}), do: identity
  defp link_identity(_command), do: nil

  # Only a successful completion declares (spec §3.5); an admitted detach or
  # End retracts at once, so it is never restored.
  defp declaration({:open_session, sid, options}), do: {:session, sid, options}
  defp declaration({:session, sid, {:attach, identity, options}}), do: {:link, sid, identity, options}
  defp declaration(_command), do: nil

  defp retract(state, {:session, sid, {:detach, identity}}),
    do: %{state | intent: Intent.drop_link(state.intent, sid, identity)}

  defp retract(state, {:end_session, sid}),
    do: %{state | intent: Intent.drop_session(state.intent, sid)}

  defp retract(state, _command), do: state
```

5d. Replace the `effect({:operation, …})` clause with:

```elixir
  defp effect({:operation, reference, result}, state) do
    case Map.pop(state.operations, reference) do
      {nil, _operations} ->
        state

      {%{caller: :restore, item: key}, operations} ->
        state = %{state | operations: operations}

        if state.endpoint.phase == :closed,
          do: %{state | intent: Intent.interrupt(state.intent)},
          else: restored(state, key, result)

      {%{caller: caller} = owner, operations} ->
        send(caller, {:graviton_mq, self(), {:operation, reference, result}})
        %{state | operations: operations} |> declare(Map.get(owner, :declare), result) |> schedule_restore()
    end
  end
```

5e. Events. Update the connected/disconnected clauses, and route the generic one through `observe/2`:

```elixir
  defp effect({:event, {:connected, _container} = event}, state) do
    notify(state, event)
    start_restore(%{state | owner: :active, attempt: 0})
  end

  defp effect({:event, {:disconnected, reason} = event}, state) do
    notify(state, event)
    %{state | lost: reason, intent: Intent.stop(state.intent), restore_pending: false}
  end

  defp effect({:event, event}, state) do
    notify(state, event)
    observe(state, event)
  end
```

5f. `info/2`: add before the observer DOWN clause:

```elixir
  defp info({:restore, generation}, %{restore_pending: true} = state) do
    state = %{state | restore_pending: false}

    if generation == state.endpoint.generation and state.endpoint.phase == :active,
      do: {:noreply, restore_next(state)},
      else: {:noreply, state}
  end
```

5g. Restoration helpers (after `after_loss/1`):

```elixir
  # The first connection has nothing to restore and reports nothing (D13).
  defp start_restore(%{endpoint: %{generation: 0}} = state), do: state

  defp start_restore(state),
    do: %{state | intent: Intent.start(state.intent)} |> schedule_restore() |> finish_restore()

  defp schedule_restore(%{restore_pending: false} = state) do
    if Intent.waiting?(state.intent) do
      send(self(), {:restore, state.endpoint.generation})
      %{state | restore_pending: true}
    else
      state
    end
  end

  defp schedule_restore(state), do: state

  # At most min(8, free operations) restoration operations are outstanding,
  # so host commands are never starved (spec §3.5).
  defp restore_next(state) do
    free = state.config.max_operations - map_size(state.operations)
    {commands, intent} = Intent.take(state.intent, min(@restore_window - state.intent.outstanding, free))

    Enum.reduce(commands, %{state | intent: intent}, fn {key, command}, state ->
      case admit(state, command, %{caller: :restore, item: key}) do
        {:ok, _reference, state} -> state
        {:error, reason, state} -> restored(state, key, {:error, reason})
      end
    end)
  end

  defp restored(state, key, result) do
    {intent, reports} = Intent.complete(state.intent, key, result)
    Enum.each(reports, fn {key, result} -> notify(state, {:restored, key, result}) end)
    %{state | intent: intent} |> schedule_restore() |> finish_restore()
  end

  defp finish_restore(state) do
    if Intent.finished?(state.intent) do
      {counts, intent} = Intent.finish(state.intent)
      notify(state, {:restore_complete, counts})
      %{state | intent: intent}
    else
      state
    end
  end

  defp declare(state, {:session, sid, options}, {:ok, _}),
    do: %{state | intent: Intent.put_session(state.intent, sid, options)}

  defp declare(state, {:link, sid, identity, options}, {:ok, _}),
    do: %{state | intent: Intent.put_link(state.intent, sid, identity, options)}

  defp declare(state, _declaration, _result), do: state

  # The peer ended or detached it: restoring it would resurrect it (D13).
  defp observe(state, {:session, sid, {kind, identity, _reason}}) when kind in [:detached, :refused],
    do: %{state | intent: Intent.drop_link(state.intent, sid, identity)}

  defp observe(state, {:session_ended, sid, _error}),
    do: %{state | intent: Intent.drop_session(state.intent, sid)}

  defp observe(state, {:session_error, sid, _reason}),
    do: %{state | intent: Intent.drop_session(state.intent, sid)}

  defp observe(state, _event), do: state
```

5h. `amqp10_client.ex` moduledoc: replace "Reconnect and session resumption are not implemented: a new owner starts a new connection without implicit replay." with:

```text
  `reconnect: [max_attempts: 0..100 (default 0), delay: 1..60_000 (250),
  max_delay: delay..60_000 (5_000)]` opens a **new** Connection after a
  transient loss (TCP close/error, receive idle or open timeout, SASL timeout
  or `sys-temp`, an unrequested peer Close without error or with
  `amqp:connection:forced`), waiting `min(max_delay, delay * 2^(n-1))` ms
  before attempt n (no jitter). The budget resets when the peer's Open
  arrives. Authentication, TLS-trust and protocol failures, host `close/1` and
  an exhausted budget stop the owner (`status/1` `owner: :stopped`) without
  exiting it. The observer receives `{:reconnecting, n}`,
  `{:reconnect_exhausted, n}` (only if `max_attempts > 0`), and after every
  reconnect `{:restored, {:session, sid} | {:link, sid, link}, result}` per
  Session/Link the host had successfully declared, then
  `{:restore_complete, %{restored: n, failed: m}}`. Between connections
  commands return `{:error, :disconnected}`; commands on an item still being
  restored return `{:error, :restoring}`. Nothing is resumed: in-flight
  operations already completed with `{:unknown, _}` or `{:error, _}` at loss
  and are never replayed; old delivery references return
  `{:error, :stale_delivery}`; receiver credit must be granted again with
  `request/5`. Resending an `{:unknown, _}` publication may duplicate it
  (at-least-once).
```

- [ ] **Step 6: Run**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix compile --warnings-as-errors && for s in 1 2 3; do env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10_intent_test.exs test/amqp10_reconnect_test.exs test/amqp10_client_test.exs test/amqp10_sasl_client_test.exs test/tls_transport_test.exs test/privacy_test.exs --seed $s || break; done && cd ../.. && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix graviton_mq.check_architecture
```
Expected: `0 failures` for each seed; the architecture check passes.

- [ ] **Step 7: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format apps/graviton_mq_client/lib/graviton_mq/client/amqp10/intent.ex apps/graviton_mq_client/lib/graviton_mq/client/amqp10.ex apps/graviton_mq_client/lib/graviton_mq/amqp10_client.ex apps/graviton_mq_client/test/amqp10_intent_test.exs apps/graviton_mq_client/test/amqp10_reconnect_test.exs
git add -- apps/graviton_mq_client/lib/graviton_mq/client/amqp10/intent.ex apps/graviton_mq_client/lib/graviton_mq/client/amqp10.ex apps/graviton_mq_client/lib/graviton_mq/amqp10_client.ex apps/graviton_mq_client/test/amqp10_intent_test.exs apps/graviton_mq_client/test/amqp10_reconnect_test.exs
git commit --only -m "feat(amqp10-client): restore declared Sessions and Links after reconnect" -- apps/graviton_mq_client/lib/graviton_mq/client/amqp10/intent.ex apps/graviton_mq_client/lib/graviton_mq/client/amqp10.ex apps/graviton_mq_client/lib/graviton_mq/amqp10_client.ex apps/graviton_mq_client/test/amqp10_intent_test.exs apps/graviton_mq_client/test/amqp10_reconnect_test.exs
```

---

## Task 10 (spec T8): Local evidence — SASL per attempt, privacy, TLS per attempt

**Files:**
- Test: `apps/graviton_mq_client/test/amqp10_reconnect_test.exs`, `apps/graviton_mq_client/test/privacy_test.exs`, `apps/graviton_mq_client/test/tls_transport_test.exs`

These tests verify Tasks 8–9. They should pass on first run; each has a mutation check.

- [ ] **Step 1: SASL provider once per attempt** (append to `amqp10_reconnect_test.exs`; add the aliases `alias GravitonMQ.AMQP10.Codec.Limits`, `alias GravitonMQ.AMQP10.Codec.SASL, as: SASLCodec`, `alias GravitonMQ.AMQP10.Frame, as: Envelope`, `alias GravitonMQ.AMQP10.SASL.{Init, Mechanisms, Outcome, Plain}`)

```elixir
  @sasl <<"AMQP", 3, 1, 0, 0>>

  test "the password provider runs once per attempt and no password outlives sasl-init" do
    test = self()
    suffix = Integer.to_string(System.unique_integer([:positive]))
    calls = :counters.new(1, [])

    provider = fn ->
      :counters.add(calls, 1, 1)
      send(test, :provided)
      "rotating-password-" <> suffix
    end

    {listener, client} =
      client!(
        sasl: {:plain, "user", provider},
        allow_insecure_plain: true,
        reconnect: [max_attempts: 1, delay: 20, max_delay: 20]
      )

    for attempt <- 1..2 do
      {:ok, socket} = :gen_tcp.accept(listener, 2_000)
      assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)
      assert_receive :provided, 1_000
      :ok = :gen_tcp.send(socket, @sasl <> sasl_frame(%Mechanisms{server_mechanisms: Value.symbol("PLAIN")}))
      assert %Init{initial_response: %Value{value: response}} = read_sasl!(socket)
      assert {:ok, %Plain{password: "rotating-password-" <> ^suffix}} = Plain.parse(response)
      :ok = :gen_tcp.send(socket, sasl_frame(%Outcome{code: Value.ubyte(0)}) <> @header <> frame(open()))
      assert {:ok, @header} = :gen_tcp.recv(socket, 8, 1_000)
      assert %Open{} = recv_pdu!(socket)
      assert_receive {:graviton_mq, ^client, {:connected, "peer"}}, 1_000
      assert :counters.get(calls, 1) == attempt
      refute_retained!(client, "rotating-password-" <> suffix)
      if attempt == 1, do: :gen_tcp.close(socket)
    end

    refute_received :provided
  end

  defp sasl_frame(body) do
    {:ok, bytes} = SASLCodec.encode_frame(body)
    bytes
  end

  defp read_sasl!(socket) do
    {:ok, <<size::32, _::binary>> = header} = :gen_tcp.recv(socket, 8, 1_000)
    {:ok, body} = :gen_tcp.recv(socket, size - 8, 1_000)
    {:ok, %Envelope{type: :sasl} = frame, <<>>} = Frame.decode(header <> body, Limits.default(), types: [:sasl])
    {:ok, decoded} = SASLCodec.decode(frame.body)
    decoded
  end

  defp refute_retained!(client, secret) do
    state = :sys.get_state(client)
    refute inspect(state, limit: :infinity, printable_limit: :infinity) =~ secret
    refute :binary.match(:erlang.term_to_binary(state), secret) != :nomatch
    refute inspect(Client.status(client)) =~ secret
    refute inspect(:sys.get_status(client), limit: :infinity) =~ secret
  end
```

Mutation check: in `info({:retry, …})`, make the shell reuse the first attempt's machine (skip `begin/1`'s provider call by caching `credentials`) → the counter assertion FAILS on attempt 2.

- [ ] **Step 2: Privacy under reconnect** (append to `GravitonMQ.Client.PrivacyTest`)

```elixir
  test "reconnect status, events and crash reports never contain the password or host" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    {:ok, port} = :inet.port(listener)
    :ok = :gen_tcp.close(listener)
    password = "reconnect-password-#{System.unique_integer([:positive])}"
    host = "127.0.0.1"

    client =
      start_supervised!(
        Supervisor.child_spec(
          {GravitonMQ.AMQP10Client,
           container_id: "privacy-reconnect",
           host: host,
           port: port,
           sasl: {:plain, "user", fn -> password end},
           allow_insecure_plain: true,
           reconnect: [max_attempts: 2, delay: 20, max_delay: 20]},
          id: make_ref(),
          restart: :temporary
        )
      )

    assert_receive {:graviton_mq, ^client, {:reconnect_exhausted, 2}}, 2_000
    events = collect_events(client, [])
    assert Enum.any?(events, &match?({:reconnecting, 2}, &1))

    for item <- [GravitonMQ.AMQP10Client.status(client), events, :sys.get_status(client)] do
      printed = inspect(item, limit: :infinity, printable_limit: :infinity)
      refute printed =~ password
      refute printed =~ host
    end

    monitor = Process.monitor(client)
    assert %{owner: :stopped} = GravitonMQ.AMQP10Client.status(client)

    log =
      capture_log(fn ->
        :sys.terminate(client, {:crash_test, :reason})
        assert_receive {:DOWN, ^monitor, :process, ^client, reason}, 1_000
        refute inspect(reason) =~ password
      end)

    assert log =~ "State: :redacted"
    refute log =~ password
  end

  defp collect_events(client, events) do
    receive do
      {:graviton_mq, ^client, event} -> collect_events(client, [event | events])
    after
      50 -> Enum.reverse(events)
    end
  end
```

The connection-refused events (`{:disconnected, {:connect, :econnrefused}}`) contain no host. Mutation check: make `transport_reason/1` return the raw reason including the address tuple → FAILS.

- [ ] **Step 3: TLS full handshake per attempt and terminal trust failure** (append to `GravitonMQ.Client.TLSTransportTest`; add the aliases `alias GravitonMQ.AMQP10Client, as: Client`, `alias GravitonMQ.AMQP10.Value`, `alias GravitonMQ.AMQP10.Codec.{Frame, Performative}`, `alias GravitonMQ.AMQP10.Performative.Open`)

```elixir
  test "every reconnect performs a verified handshake; an untrusted certificate is terminal",
       context do
    header = <<"AMQP", 0, 1, 0, 0>>

    client =
      start_supervised!(
        {Client,
         container_id: "tls-reconnect",
         host: "localhost",
         port: context.port,
         transport: :tls,
         tls_options: [cacertfile: context.certs.ca_certificate],
         reconnect: [max_attempts: 5, delay: 20, max_delay: 20]}
      )

    for _attempt <- 1..2 do
      {:ok, transport} = :ssl.transport_accept(context.listener, 5_000)
      {:ok, socket} = :ssl.handshake(transport, 5_000)
      assert {:ok, ^header} = :ssl.recv(socket, 8, 2_000)
      assert %Open{} = ssl_pdu!(socket)
      {:ok, open} = Performative.encode(%Open{container_id: Value.string("peer"), max_frame_size: Value.uint(512)})
      :ok = :ssl.send(socket, header <> <<8 + byte_size(open)::32, 2, 0, 0::16, open::binary>>)
      assert_receive {:graviton_mq, ^client, {:connected, "peer"}}, 2_000
      :ok = :ssl.close(socket)
      assert_receive {:graviton_mq, ^client, {:disconnected, _}}, 2_000
      assert_receive {:graviton_mq, ^client, {:reconnecting, _}}, 2_000
    end

    # The server certificate changes to one the client does not trust.
    {:ok, transport} = :ssl.transport_accept(context.listener, 5_000)
    untrusted_key = Path.join(Path.dirname(context.certs.untrusted), "untrusted.key")

    _ =
      :ssl.handshake(
        transport,
        [certfile: String.to_charlist(context.certs.untrusted), keyfile: String.to_charlist(untrusted_key)],
        5_000
      )

    assert_receive {:graviton_mq, ^client, {:disconnected, {:connect, {:tls_alert, _alert}}}}, 5_000
    refute_receive {:graviton_mq, ^client, {:reconnecting, _}}, 200
    assert %{owner: :stopped} = Client.status(client)
  end

  defp ssl_pdu!(socket) do
    {:ok, <<size::32>>} = :ssl.recv(socket, 4, 2_000)
    {:ok, rest} = :ssl.recv(socket, size - 4, 2_000)
    {:ok, frame, <<>>} = Frame.decode(<<size::32, rest::binary>>)
    {:ok, pdu, <<>>} = Performative.decode(frame.body)
    pdu
  end
```

Mutation check: in `Reconnect`, add `defp transient({:connect, {:tls_alert, _}}), do: :retry` before the catch-all → FAILS (a `{:reconnecting, _}` arrives).

- [ ] **Step 4: Run (three seeds)**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && for s in 1 2 3; do env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10_reconnect_test.exs test/privacy_test.exs test/tls_transport_test.exs test/amqp10_client_test.exs test/amqp10_sasl_client_test.exs --seed $s || break; done
```
Expected: `0 failures` each.

- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format apps/graviton_mq_client/test/amqp10_reconnect_test.exs apps/graviton_mq_client/test/privacy_test.exs apps/graviton_mq_client/test/tls_transport_test.exs
git add -- apps/graviton_mq_client/test/amqp10_reconnect_test.exs apps/graviton_mq_client/test/privacy_test.exs apps/graviton_mq_client/test/tls_transport_test.exs
git commit --only -m "test(amqp10-client): reconnect SASL, privacy and TLS evidence" -- apps/graviton_mq_client/test/amqp10_reconnect_test.exs apps/graviton_mq_client/test/privacy_test.exs apps/graviton_mq_client/test/tls_transport_test.exs
```

---

## Task 11 (spec T9): Multi-accept cut proxy and Artemis R1–R6, F1–F3 (CI only)

**Files:**
- Create: `apps/graviton_mq_client/test/support/amqp_cut_proxy.exs`
- Modify: `apps/graviton_mq_client/test/support/amqp_peer.py` (`drain` operation only)
- Create: `apps/graviton_mq_client/test/amqp10_artemis_reconnect_test.exs`

- [ ] **Step 1: The proxy**

```elixir
defmodule GravitonMQ.Client.Test.AMQPCutProxy do
  @moduledoc false
  # Multi-accept TCP relay for AMQP reconnect evidence (S3 R1–R6, F1–F3).
  # Every accepted client socket gets its own upstream socket. `cut/1`,
  # `blackhole/1` and `cut_on_disposition/1` act on the current pairs;
  # `refuse/2` acts on later accepts (accept, then close at once).
  # With `parse: true` (no-SASL plain TCP only) frame envelopes and
  # descriptors are parsed independently of the Graviton codec; TLS is relayed
  # opaquely. The observer receives `{:proxy_connection, n, monotonic_ms}` for
  # every accept, `{:proxy_cut, n, :disposition}`, `{:proxy_transfer, n}` for
  # each client Transfer and `{:proxy_closed, n}`.
  use GenServer

  def start_link(options), do: GenServer.start_link(__MODULE__, options)
  def port(proxy), do: GenServer.call(proxy, :port)
  def cut(proxy), do: GenServer.call(proxy, :cut)
  def blackhole(proxy), do: GenServer.call(proxy, {:mode, :blackhole})
  def cut_on_disposition(proxy), do: GenServer.call(proxy, {:mode, :cut_on_disposition})
  def refuse(proxy, refuse?) when is_boolean(refuse?), do: GenServer.call(proxy, {:refuse, refuse?})

  @impl true
  def init(options) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)
    owner = self()
    acceptor = spawn_link(fn -> accept_loop(listener, owner) end)
    {host, upstream_port} = Keyword.fetch!(options, :upstream)

    {:ok,
     %{
       listener: listener,
       port: port,
       acceptor: acceptor,
       upstream: {String.to_charlist(host), upstream_port},
       observer: Keyword.fetch!(options, :observer),
       parse: Keyword.get(options, :parse, true),
       refuse: false,
       count: 0,
       pairs: %{},
       sockets: %{}
     }}
  end

  defp accept_loop(listener, owner) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        :ok = :gen_tcp.controlling_process(socket, owner)
        send(owner, {:accepted, socket})
        accept_loop(listener, owner)

      {:error, _closed} ->
        :ok
    end
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}
  def handle_call({:refuse, refuse?}, _from, state), do: {:reply, :ok, %{state | refuse: refuse?}}

  def handle_call(:cut, _from, state),
    do: {:reply, :ok, Enum.reduce(Map.keys(state.pairs), state, &close_pair(&2, &1))}

  def handle_call({:mode, mode}, _from, state),
    do: {:reply, :ok, %{state | pairs: Map.new(state.pairs, fn {n, pair} -> {n, %{pair | mode: mode}} end)}}

  @impl true
  def handle_info({:accepted, socket}, state) do
    n = state.count + 1
    send(state.observer, {:proxy_connection, n, System.monotonic_time(:millisecond)})
    state = %{state | count: n}

    if state.refuse do
      :gen_tcp.close(socket)
      {:noreply, state}
    else
      {host, port} = state.upstream
      {:ok, broker} = :gen_tcp.connect(host, port, [:binary, active: false], 2_000)
      :ok = :inet.setopts(socket, active: :once)
      :ok = :inet.setopts(broker, active: :once)

      pair = %{
        client: socket,
        broker: broker,
        mode: :relay,
        client_buffer: <<>>,
        broker_buffer: <<>>,
        client_header: true,
        broker_header: true
      }

      sockets = state.sockets |> Map.put(socket, {n, :client}) |> Map.put(broker, {n, :broker})
      {:noreply, %{state | pairs: Map.put(state.pairs, n, pair), sockets: sockets}}
    end
  end

  def handle_info({:tcp, socket, bytes}, state) do
    case Map.fetch(state.sockets, socket) do
      {:ok, {n, direction}} ->
        state = relay(state, n, direction, bytes)
        if Map.has_key?(state.pairs, n), do: :inet.setopts(socket, active: :once)
        {:noreply, state}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({kind, socket}, state) when kind in [:tcp_closed],
    do: {:noreply, close_socket(state, socket)}

  def handle_info({:tcp_error, socket, _reason}, state), do: {:noreply, close_socket(state, socket)}
  def handle_info(_message, state), do: {:noreply, state}

  defp close_socket(state, socket) do
    case Map.fetch(state.sockets, socket) do
      {:ok, {n, _}} -> close_pair(state, n)
      :error -> state
    end
  end

  defp relay(state, n, direction, bytes) do
    pair = Map.fetch!(state.pairs, n)

    cond do
      pair.mode == :blackhole ->
        state

      not state.parse ->
        _ = :gen_tcp.send(target(pair, direction), bytes)
        state

      true ->
        {buffer, _header} = keys(direction)
        pair = Map.update!(pair, buffer, &(&1 <> bytes))
        if byte_size(Map.fetch!(pair, buffer)) > 1_048_576, do: raise("test proxy buffer limit")
        frames(%{state | pairs: Map.put(state.pairs, n, pair)}, n, direction)
    end
  end

  defp frames(state, n, direction) do
    pair = Map.fetch!(state.pairs, n)
    {buffer, header} = keys(direction)

    case envelope(Map.fetch!(pair, buffer), Map.fetch!(pair, header)) do
      :more ->
        state

      {:ok, wire, body, rest} ->
        pair = pair |> Map.put(buffer, rest) |> Map.put(header, false)
        state = %{state | pairs: Map.put(state.pairs, n, pair)}

        if direction == :broker and pair.mode == :cut_on_disposition and disposition?(body) do
          send(state.observer, {:proxy_cut, n, :disposition})
          close_pair(state, n)
        else
          if direction == :client and transfer?(body), do: send(state.observer, {:proxy_transfer, n})
          _ = :gen_tcp.send(target(pair, direction), wire)
          frames(state, n, direction)
        end
    end
  end

  defp keys(:client), do: {:client_buffer, :client_header}
  defp keys(:broker), do: {:broker_buffer, :broker_header}
  defp target(pair, :client), do: pair.broker
  defp target(pair, :broker), do: pair.client

  defp envelope(input, true) when byte_size(input) < 8, do: :more
  defp envelope(<<wire::binary-size(8), rest::binary>>, true), do: {:ok, wire, <<>>, rest}
  defp envelope(input, false) when byte_size(input) < 8, do: :more

  defp envelope(<<size::32, offset, _::binary>> = input, false) do
    if size < 8 or offset < 2 or offset * 4 > size or size > 1_048_576,
      do: raise("invalid test peer envelope")

    if byte_size(input) < size do
      :more
    else
      <<wire::binary-size(size), rest::binary>> = input
      {:ok, wire, binary_part(wire, offset * 4, size - offset * 4), rest}
    end
  end

  defp disposition?(<<0, 0x53, 0x15, _::binary>>), do: true
  defp disposition?(<<0, 0x80, 0x15::64, _::binary>>), do: true
  defp disposition?(_body), do: false
  defp transfer?(<<0, 0x53, 0x14, _::binary>>), do: true
  defp transfer?(<<0, 0x80, 0x14::64, _::binary>>), do: true
  defp transfer?(_body), do: false

  defp close_pair(state, n) do
    case Map.pop(state.pairs, n) do
      {nil, _pairs} ->
        state

      {pair, pairs} ->
        :gen_tcp.close(pair.client)
        :gen_tcp.close(pair.broker)
        send(state.observer, {:proxy_closed, n})
        %{state | pairs: pairs, sockets: Map.drop(state.sockets, [pair.client, pair.broker])}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(Map.keys(state.pairs), &close_pair(state, &1))
    :gen_tcp.close(state.listener)
    if Process.alive?(state.acceptor), do: Process.exit(state.acceptor, :shutdown)
    :ok
  end
end
```

- [ ] **Step 2: Proton `drain` operation.** In `amqp_peer.py`, add `"drain"` to the `choices` list. Change `if arguments.operation == "send":` to `elif arguments.operation == "send":` and insert this before it, as the first branch of the `try` block:

```python
        if arguments.operation == "drain":
            receiver = connection.create_receiver(arguments.address, credit=1, options=AtLeastOnce())
            digests = []
            while True:
                try:
                    message = receiver.receive(timeout=3)
                except Exception as error:  # proton.utils raises Timeout
                    if type(error).__name__ != "Timeout":
                        raise
                    break
                digests.append(hashlib.sha256(bytes(message.body)).hexdigest())
                receiver.accept()
            print(json.dumps({"drained": len(digests), "sha256": digests}), flush=True)
```

Check the syntax: `python3 -c "import ast,sys; ast.parse(open(sys.argv[1]).read())" /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client/test/support/amqp_peer.py`. Expected: no output, exit 0.

- [ ] **Step 3: The Artemis suite**

```elixir
Code.require_file("support/peer_process.exs", __DIR__)
Code.require_file("support/amqp_cut_proxy.exs", __DIR__)

defmodule GravitonMQ.AMQP10Client.ArtemisReconnectTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message
  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.Client.Test.{AMQPCutProxy, PeerProcess}

  @moduletag :artemis_interop
  @moduletag timeout: 60_000
  @moduletag :tmp_dir
  @peer Path.join(__DIR__, "support/amqp_peer.py")
  # Created by scripts/interop/run.py (`artemis create --user ... --password ...`).
  @user "graviton-test"
  @password "ephemeral-test-only"

  setup_all do
    host = System.fetch_env!("ARTEMIS_HOST")
    port = System.fetch_env!("ARTEMIS_PORT") |> String.to_integer()
    python = System.fetch_env!("GRAVITON_PROTON_PYTHON")
    %{host: host, port: port, python: python, url: "amqp://#{host}:#{port}"}
  end

  test "R1: a cut while active reconnects, restores and publishes again", context do
    {proxy, client} = connected!(context, reconnect: [max_attempts: 3, delay: 100, max_delay: 400])
    session!(client)
    address = address("r1")
    sender = attach!(client, "r1-sender", :sender, address)
    :ok = AMQPCutProxy.cut(proxy)
    assert_receive {:graviton_mq, ^client, {:disconnected, :closed}}, 5_000
    assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
    restored!(client, [{:session, 1}, {:link, 1, sender}])
    round_trip!(context, client, sender, address)
  end

  test "R2: an unsettled delivery is voided and redelivered after new credit", context do
    {proxy, client} =
      connected!(context,
        session_options: [max_message_bytes: 1024],
        reconnect: [max_attempts: 3, delay: 100, max_delay: 400]
      )

    session!(client)
    address = address("r2")
    receiver = attach!(client, "r2-receiver", :receiver, address, max_message_bytes: 1024)
    path = body!(context, "r2-redelivered")
    assert {_, 0} = proton(context, "send", address, path)
    request!(client, receiver, 1, 1024)
    assert_receive {:graviton_mq, ^client, {:delivery, old, 1, ^receiver, first, _}}, 10_000
    :ok = AMQPCutProxy.cut(proxy)
    assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 5_000
    restored!(client, [{:session, 1}, {:link, 1, receiver}])
    assert {:error, :stale_delivery} = Client.settle(client, old, %Outcome.Accepted{})
    # Credit is not restored automatically (owner Q2).
    refute_receive {:graviton_mq, ^client, {:delivery, _, _, _, _, _}}, 300
    request!(client, receiver, 1, 1024)
    assert_receive {:graviton_mq, ^client, {:delivery, fresh, 1, ^receiver, second, _}}, 10_000
    assert body(first) == body(second)
    assert {:ok, settling} = Client.settle(client, fresh, %Outcome.Accepted{})
    assert_receive {:graviton_mq, ^client, {:operation, ^settling, {:ok, %Outcome.Accepted{}}}}, 5_000
    assert drained(context, address) == 0
  end

  test "R3: an outcome lost with the connection is unknown and never resent", context do
    {proxy, client} = connected!(context, reconnect: [max_attempts: 3, delay: 100, max_delay: 400])
    session!(client)
    address = address("r3")
    sender = attach!(client, "r3-sender", :sender, address)
    :ok = AMQPCutProxy.cut_on_disposition(proxy)
    {:ok, raw} = Message.encode_sections([{:data, Value.binary("r3-lost-outcome")}])
    assert {:ok, sending} = Client.send_message(client, 1, sender, raw)
    assert_receive {:proxy_cut, 1, :disposition}, 10_000
    assert_receive {:graviton_mq, ^client, {:operation, ^sending, {:unknown, _}}}, 5_000
    restored!(client, [{:session, 1}, {:link, 1, sender}])
    refute_receive {:proxy_transfer, 2}, 500
    refute_received {:graviton_mq, ^client, {:operation, ^sending, {:ok, _}}}
    assert drained(context, address) <= 1
  end

  test "R4: refused attempts back off by the formula until the peer is reachable", context do
    {proxy, client} = connected!(context, reconnect: [max_attempts: 5, delay: 100, max_delay: 400])
    :ok = AMQPCutProxy.refuse(proxy, true)
    :ok = AMQPCutProxy.cut(proxy)

    for attempt <- 1..3, do: assert_receive({:graviton_mq, ^client, {:reconnecting, ^attempt}}, 5_000)

    :ok = AMQPCutProxy.refuse(proxy, false)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    accepts = for n <- 2..4, do: receive_accept!(n)
    gaps = accepts |> Enum.chunk_every(2, 1, :discard) |> Enum.map(fn [a, b] -> b - a end)

    # Accept n+1 follows the retry scheduled after accept n failed:
    # delay(2) = 200 ms, delay(3) = 400 ms. Tolerance: -5 ms (timer and clock
    # granularity), +1,500 ms (CI scheduling).
    for {gap, delay} <- Enum.zip(gaps, [200, 400]) do
      assert gap >= delay - 5, "gap #{gap} ms below #{delay} ms"
      assert gap <= delay + 1_500, "gap #{gap} ms far above #{delay} ms"
    end
  end

  @tag :artemis_tls_interop
  test "R5: SASL PLAIN over verified TLS re-authenticates once per attempt", context do
    calls = :counters.new(1, [])
    provider = fn -> :counters.add(calls, 1, 1) && @password end

    {proxy, client} =
      connected!(context,
        upstream: tls_port(),
        parse: false,
        host: "localhost",
        transport: :tls,
        tls_options: [cacertfile: System.fetch_env!("ARTEMIS_TLS_CA")],
        sasl: {:plain, @user, provider},
        reconnect: [max_attempts: 3, delay: 100, max_delay: 400]
      )

    session!(client)
    :ok = AMQPCutProxy.cut(proxy)
    restored!(client, [{:session, 1}])
    assert :counters.get(calls, 1) == 2
  end

  test "R6: a blackholed connection is detected by idle timeout and replaced", context do
    {proxy, client} =
      connected!(context,
        idle_timeout_ms: 2_000,
        close_timeout_ms: 500,
        reconnect: [max_attempts: 3, delay: 100, max_delay: 400]
      )

    session!(client)
    :ok = AMQPCutProxy.blackhole(proxy)
    assert_receive {:graviton_mq, ^client, {:disconnected, :close_timeout}}, 6_000
    assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 1_000
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    # Restoration may be refused while Artemis keeps the half-open
    # connection (spec §6); only completion is asserted.
    assert_receive {:graviton_mq, ^client, {:restore_complete, _counts}}, 10_000
    assert %{owner: owner, attempt: 0} = Client.status(client)
    assert owner in [:active, :restoring]
  end

  test "F1: a permanently refusing peer exhausts the budget and stops cleanly", context do
    {proxy, client} = connected!(context, reconnect: [max_attempts: 3, delay: 50, max_delay: 200])
    :ok = AMQPCutProxy.refuse(proxy, true)
    :ok = AMQPCutProxy.cut(proxy)
    for attempt <- 1..3, do: assert_receive({:graviton_mq, ^client, {:reconnecting, ^attempt}}, 5_000)
    assert_receive {:graviton_mq, ^client, {:reconnect_exhausted, 3}}, 5_000
    assert %{owner: :stopped, next_retry_ms: nil} = Client.status(client)
    assert %{socket: nil, retry: nil, timers: timers} = :sys.get_state(client)
    assert timers == %{}
    refute_receive {:proxy_connection, 5, _}, 600
  end

  @tag :artemis_tls_interop
  test "F2: a wrong password on the second attempt is terminal", context do
    calls = :counters.new(1, [])

    provider = fn ->
      :counters.add(calls, 1, 1)
      if :counters.get(calls, 1) == 1, do: @password, else: "wrong-password"
    end

    {proxy, client} =
      connected!(context,
        upstream: tls_port(),
        parse: false,
        host: "localhost",
        transport: :tls,
        tls_options: [cacertfile: System.fetch_env!("ARTEMIS_TLS_CA")],
        sasl: {:plain, @user, provider},
        reconnect: [max_attempts: 3, delay: 100, max_delay: 400]
      )

    :ok = AMQPCutProxy.cut(proxy)
    assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 5_000
    assert_receive {:graviton_mq, ^client, {:disconnected, {:sasl, :auth}}}, 10_000
    refute_receive {:graviton_mq, ^client, {:reconnecting, _}}, 500
    assert %{owner: :stopped} = Client.status(client)
  end

  test "F3: close/1 during the backoff wait prevents any further connection", context do
    {proxy, client} = connected!(context, reconnect: [max_attempts: 3, delay: 2_000, max_delay: 2_000])
    :ok = AMQPCutProxy.cut(proxy)
    assert_receive {:graviton_mq, ^client, {:reconnecting, 1}}, 5_000
    assert {:ok, closing} = Client.close(client)
    assert_receive {:graviton_mq, ^client, {:operation, ^closing, {:ok, :closed}}}, 100
    refute_receive {:proxy_connection, 2, _}, 2_500
    assert %{owner: :stopped} = Client.status(client)
  end

  defp connected!(context, options) do
    {upstream, options} = Keyword.pop(options, :upstream, context.port)
    {parse, options} = Keyword.pop(options, :parse, true)

    proxy =
      start_supervised!(
        {AMQPCutProxy, upstream: {context.host, upstream}, observer: self(), parse: parse}
      )

    options =
      Keyword.merge(
        [container_id: address("client"), host: "127.0.0.1", port: AMQPCutProxy.port(proxy)],
        options
      )

    client = start_supervised!({Client, options})
    assert_receive {:proxy_connection, 1, _}, 5_000
    assert_receive {:graviton_mq, ^client, {:connected, peer}}, 10_000
    assert is_binary(peer) and peer != ""
    {proxy, client}
  end

  defp restored!(client, items) do
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 10_000

    for item <- items do
      assert_receive {:graviton_mq, ^client, {:restored, ^item, {:ok, _}}}, 10_000
    end

    count = length(items)
    assert_receive {:graviton_mq, ^client, {:restore_complete, %{restored: ^count, failed: 0}}}, 10_000
  end

  defp round_trip!(context, client, sender, address) do
    body = "artemis-reconnect-#{System.unique_integer([:positive])}"
    path = body!(context, body)

    receiver =
      start_supervised!(
        {PeerProcess, executable: context.python, args: [@peer, "receive", context.url, address, path]}
      )

    eventually!(fn -> PeerProcess.snapshot(receiver).output =~ "\"ready\": true" end)
    {:ok, raw} = Message.encode_sections([{:data, Value.binary(body)}])
    assert {:ok, sending} = Client.send_message(client, 1, sender, raw)
    assert_receive {:graviton_mq, ^client, {:operation, ^sending, {:ok, %Outcome.Accepted{}}}}, 10_000
    eventually!(fn -> PeerProcess.snapshot(receiver).exit_status != nil end)
    assert %{exit_status: 0, output: output} = PeerProcess.snapshot(receiver)
    assert output =~ :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)
  end

  defp session!(client) do
    assert {:ok, operation} = Client.open_session(client, 1)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, 1}}}, 5_000
  end

  defp attach!(client, name, role, address, options \\ []) do
    assert {:ok, operation} =
             Client.attach(client, 1, name, role, Keyword.put(options, :address, Value.string(address)))

    identity = {Value.string(name), role}
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^identity}}}, 5_000
    identity
  end

  defp request!(client, identity, slots, bytes) do
    assert {:ok, operation} = Client.request(client, 1, identity, slots, bytes)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, ^slots}}}, 5_000
  end

  defp body(raw) do
    {:ok, decoded, <<>>} = Message.decode(raw)
    Enum.find(decoded.sections, &match?({kind, _} when kind in [:amqp_value, :data], &1))
  end

  defp drained(context, address) do
    {output, 0} = proton(context, "drain", address, body!(context, "unused"))
    [_, count] = Regex.run(~r/"drained": (\d+)/, output)
    String.to_integer(count)
  end

  defp receive_accept!(n) do
    receive do
      {:proxy_connection, ^n, at} -> at
    after
      10_000 -> flunk("proxy connection #{n} not observed")
    end
  end

  defp body!(context, body) do
    path = Path.join(context.tmp_dir, "body-#{System.unique_integer([:positive])}")
    File.write!(path, body)
    path
  end

  defp tls_port, do: System.fetch_env!("ARTEMIS_TLS_PORT") |> String.to_integer()

  defp proton(context, operation, address, path),
    do: System.cmd(context.python, [@peer, operation, context.url, address, path], stderr_to_stdout: true)

  defp address(prefix), do: "graviton-#{prefix}-#{System.unique_integer([:positive])}"
  defp eventually!(predicate), do: until!(predicate, System.monotonic_time(:millisecond) + 12_000)

  defp until!(predicate, deadline) do
    unless predicate.() do
      assert System.monotonic_time(:millisecond) < deadline, "independent peer condition timed out"
      Process.sleep(10)
      until!(predicate, deadline)
    end
  end
end
```

The `{:proxy_connection, n, _}` messages that R4 does not consume stay in the mailbox; that is harmless. R1's `restored!` also consumes `{:connected, _}`.

- [ ] **Step 4: Local checks (compile and exclusion only; the suite cannot run without Artemis)**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix test test/amqp10_artemis_reconnect_test.exs test/amqp10_artemis_test.exs
```
Expected: `0 failures`, all tests excluded (`9 excluded` for the new file plus the existing excluded count). Also verify that the proxy compiles and relays locally. In a scratch IEx session (do not commit), start our own broker and a TCP listener, start `AMQPCutProxy` with `upstream: {"127.0.0.1", broker_port}` and connect our client through it. Record in the log that `cut/1` produced `{:disconnected, :closed}` → `{:reconnecting, 1}` → `{:connected, _}`. This self-to-self check is extra evidence only.

- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp mix format apps/graviton_mq_client/test/support/amqp_cut_proxy.exs apps/graviton_mq_client/test/amqp10_artemis_reconnect_test.exs
git add -- apps/graviton_mq_client/test/support/amqp_cut_proxy.exs apps/graviton_mq_client/test/amqp10_artemis_reconnect_test.exs apps/graviton_mq_client/test/support/amqp_peer.py
git commit --only -m "test(amqp10-client): multi-accept cut proxy and Artemis reconnect suite" -- apps/graviton_mq_client/test/support/amqp_cut_proxy.exs apps/graviton_mq_client/test/amqp10_artemis_reconnect_test.exs apps/graviton_mq_client/test/support/amqp_peer.py
```

---

## Coordinator-serialized tasks (C1–C4)

Run these only after Tasks 1–11 are committed and S1b has stopped writing the shared files listed in Execution rule 3.

### Task C1: Independent-peer evidence gate (CI)

- [ ] Confirm that no workflow or `run.py` change is needed. `apps/graviton_mq_client/test/test_helper.exs` already includes `:artemis_interop` when `ARTEMIS_HOST` and `GRAVITON_PROTON_PYTHON` are set, and `:artemis_tls_interop` when `ARTEMIS_TLS_PORT`, `ARTEMIS_TLS_CA` and `ARTEMIS_TLS_UNTRUSTED_CA` are set. `run.py` sets them all and runs `mix test` (S2 harness facts).
- [ ] With the owner's push authorization, push and wait for the `interoperability` job. It must show, all passing:
  - R1–R6 and F1–F3 (`amqp10_artemis_reconnect_test.exs`);
  - the unchanged Artemis suites;
  - the Proton → broker suites (`amqp10_*proton*`, durable path), the broker requalification required by spec §5.1;
  - the 300 s soak and the package step.

  Record the run id, SHA and test totals in the execution log.
- [ ] Triage a failure first against spec §6: half-open stale Link under the same name (R6 only asserts completion), and Artemis redelivery timing (R2). Fix only within the S3 scope, test first. Never weaken an assertion.

### Task C2: Documentation and capabilities (after C1 passes)

- [ ] `docs/AMQP10_CLIENT.md`: replace "Client reconnect/resumption …" (l.~118) with a "Reconnect" section covering:
  - options and backoff;
  - the §3.3 classification, with the D1 close-cause note and the D4 additions;
  - owner phases and command answers (D3);
  - restoration events and order (D13);
  - what requires host action (credit, `{:unknown, _}` resend, new delivery references, failed restorations);
  - SASL/TLS per attempt;
  - status keys (D2);
  - the half-open limitation.
- [ ] `docs/AMQP10_BROKER.md` l.~142–165: remove "Remaining limit: the first unanswered call still blocks the Manager …" and describe:
  - async requests with the 5 s deadline;
  - the three-way classification and per-path behaviour (spec §4.4 table plus D7);
  - `Released` for a not-executed enqueue (Q5);
  - status `queue_requests`, `queue_fences`, `:unresponsive` (Q6) and the 1 s bound;
  - the boot `watch` as the only blocking call.

  Add an upgrade note: a new status value, and `Released` replaces a Connection close for definitively not-executed enqueues.
- [ ] `docs/FAILURE_MODEL.md` l.~25 and `docs/DELIVERY_SEMANTICS.md`: the queue fence now happens at the request deadline without blocking the Manager. Client reconnect never replays uncertain publications.
- [ ] `docs/CAPABILITIES.md`:
  - AMQP1 client row: replace "reconnect/resumption … UNIMPLEMENTED" with reconnect evidence (local tests, plus run id for R1–R6/F1–F3). Keep "client process/VM recovery UNIMPLEMENTED" and "no protocol resumption".
  - AMQP1 broker row: replace the 5-second Manager block limitation with the B1–B8 evidence and the run id.
- [ ] `docs/ACTIVE_SCOPE.md`: S3 row → `Complete`, with plan link and run id.
- [ ] Executed evidence only. Commit with `git commit --only -m "docs(amqp10): S3 reconnect and asynchronous Manager evidence" -- <the edited doc paths>`.

### Task C3: Requirement inventory and clause audit

- [ ] Append a dated "S3 maintenance amendment" to `docs/qualification/2026-10-30-amqp10-clause-audit.md`. Do not rewrite the 2026-10-30 ledger. List:
  - the clauses the reconnect behaviour touches, with evidence: new Connection/Open per attempt (Part 2 §2.4), volatile termini not resumed (§2.6.13 stays unimplemented), and unknown outcomes after connection loss;
  - the broker's unchanged wire behaviour except `Released` (Part 3 §3.4.4).
- [ ] Do not change requirement IDs or statuses without executed evidence. Commit the ledger path only.

### Task C4: Final integrated verification

- [ ] From the umbrella root, run the complete AGENTS.md sequence. Every command uses the `env MIX_BUILD_ROOT=/tmp/gmq-build-s3 TMPDIR=/tmp/gmq-s3-tmp` prefix, or the shared `_build` if S1b has finished and the coordinator chooses it.

```bash
mix deps.get
mix format
mix format --check-formatted
mix compile --warnings-as-errors
mix test
mix graviton_mq.check_architecture
mix help xref
mix xref graph --format cycles --fail-above 0
```

- [ ] Inspect `git diff S3_BASE..HEAD --stat`. Confirm that no S3 commit touches an S1b-owned file, `mix.exs`, `mix.lock` or a pure `graviton_mq_amqp10` module. Confirm that no commit has `Co-Authored-By` or a "Generated with" line. Record the totals in the execution log.

---

## Self-review against the spec

| Spec item | Plan |
| --- | --- |
| §1 A client reconnect, B Manager non-blocking; non-goals (no resumption, no replay, no client store, no S1b files, no telemetry) | Tasks 7–11 / 1–6; D3/D13; Execution rule 2 |
| §2.3 13 blocking sites | 1 boot `watch` stays sync (Task 3, 3c comment); 2 Task 5 (3e); 3 Task 5 (3b); 4 Task 4; 5 Task 5 (3a); 6 Task 3 (3h); 7–9 Task 3 (3i, D8); 10–11 Task 3; 12 Task 4; 13 Task 5 (3c) |
| §3.1 unknown outcomes, voided deliveries (`:stale_delivery`), credit released, pending ops not re-run | Task 8 (stale check), Task 9 (in-flight not restored), R2/R3 |
| §3.2 config and formula, reset at Open, invalid → `:invalid_options`, default unchanged | Task 7; Task 8 tests "default policy", "budget resets only when … Open" |
| §3.3 classification | `Reconnect` (Task 7) + D1/D4; shell tests Task 8; TLS terminal Task 10; F2 |
| §3.4 phases, `:disconnected`, `:restoring`, `close/1` | Task 8 (D3), Task 9 |
| §3.5 Intent: add on completion, remove on admission / peer close, bounded, order, ≤ min(8, free) ops, events | Task 9 (D13) |
| §3.6 SASL per attempt, TLS full handshake, changed cert terminal | Task 10 Steps 1, 3; R5, F2 |
| §3.7 one timer, release first, status keys, redaction, privacy | Task 8 (`after_loss`), Task 10 Step 2, D2 |
| §3.8 pure layer unchanged | Execution rule 2; C4 diff check |
| §4.1 option (a) `send_request` | Tasks 2–5 |
| §4.2 wrappers, take-back race, Manager death | Task 2; Task 6 B7 |
| §4.3 FIFO, pure tracker, head timer, deadline 5,000, bounds incl. 4,096 cap | Tasks 1, 3 (`arm_requests`), D6/D7 |
| §4.4 per-path table, fence gains abandon-all, late replies | Tasks 3–5; D10 |
| §4.5 status 1 s, `queue_requests`, `queue_fences`, `:unresponsive` | Task 5, D14 |
| §5.1 B1–B8, Proton requalification | B1 Tasks 3/5; B2, B3, B5 Task 5; B4, B6, B7 Task 6; B8 Task 1; Proton C1 |
| §5.2 local list and R1–R6/F1–F3 with proxy modes | Tasks 7–11; D15 |
| §6 risks | R6 assertion scope; R3; docs C2 |
| §7 file ownership, T1–T10 | Execution rules; Tasks 1–11 map T1–T9; C1–C4 = T10 |
| §8 compatibility | Task 8 Step 4 (existing suites), C2 upgrade note |

Placeholder scan: every code step has code, and every command has an expected result. Type consistency:
- `QueueRequests.add/4`, `resolve/3` (`:pending | :waiting | :unknown`), `expire/3`, `abandon_all/2`, `next_deadline/2`, `pending?/3` and `classify/1` are used with those names in Tasks 3–5.
- `Queue.send_request/3`, `classify/2`, `response/3` (including `:timeout`) and the public `request/2` are used in Tasks 3, 5 and 6.
- The client `admit/3` owner maps `%{caller: pid, declare: …}` and `%{caller: :restore, item: key}` match `effect({:operation, …})`.
- `Intent.start/stop/take/complete/interrupt/finished?/finish/waiting?/pending?/counts/restoring` match their uses in Task 9.

---

## Execution log

(Append dated entries: Task 0 baseline and `S3_BASE`, per-task red/green lines with commit ids, deviations with root cause, mutation-check results, out-of-scope failures, C1 run id/SHA/outcome, C4 totals.)

### 2026-11-12 — Stream A, Task 8 (shell reconnect loop)

- Build root `/tmp/gmq-build-s3a`, `TMPDIR=/tmp/gmq-s3a-tmp` (Stream B uses `/tmp/gmq-build-s3`).
- Red: `test/amqp10_reconnect_test.exs` 11 tests, 8 failures (every shell test: `owner:` absent from status, no `{:reconnecting, 1}`).
- Green: the same file 11 tests, 0 failures. `mix compile --warnings-as-errors` clean. Seeds 1, 2, 3 over `amqp10_reconnect_test`, `amqp10_client_test`, `amqp10_sasl_client_test`, `tls_transport_test`, `privacy_test` and the three Artemis files (interop tags excluded): 60 tests, 0 failures, 13 excluded each.
- Folded in the Task 7 review test: `Reconnect.delay(%{delay: 1, max_delay: 60_000}, 100) == 60_000` (the `min(attempt - 1, 20)` exponent cap).
- Deviations: none in `amqp10.ex` beyond the plan's code. The plan's `recv_frame!/2` helper default was never used and raised a test-compile warning, so the default was dropped. Task 9 may restore it if it calls `recv_frame!(socket)`.

### 2026-11-12 — Stream B, Tasks 0–3 (broker)

- Build root `/tmp/gmq-build-s3b`, `TMPDIR=/tmp/gmq-s3b-tmp`.
- Task 0: `S3_BASE` e015263. Baseline: runtime `test/amqp10` 105 tests, 0 failures; umbrella `amqp10_availability`, `amqp10_boundaries` and `amqp10_runtime` 25 tests, 0 failures.
- Task 1 (49e4c61): red, `QueueRequests` module undefined; green, 5 tests, 0 failures.
- Task 2 (0b0ea49): red, 14 tests, 1 failure (`send_request/3` undefined); green 14/0; regression 105/0 and 25/0.
- Task 3 (e29bccf): red, availability 2 tests, 1 failure. It failed in the **second** `within!` (bystander attach, "took 4968 ms, limit 500 ms"), not the first as expected: the bystander publish completed before the Manager blocked in `Queue.settle/5`. Green: `mix compile --warnings-as-errors` clean; availability, boundaries, runtime, g5_flow, g5_annotations and sasl together 41 tests, 0 failures, 2 excluded, with seeds 1, 2 and 3; boundaries `--only line:177` (pump wave) passes alone; regression runtime `test/amqp10` 105/0, three umbrella AMQP suites 25/0.
- Task 3 deviations: none in `broker.ex` beyond the plan's code. `fence/2` already rejects `:registering` routes, although that phase only appears in Task 4.

### 2026-11-12 — Stream A, Task 9 (`Intent`, restoration and events)

- Commit `43e7adb`. Build root `/tmp/gmq-build-s3a`, `TMPDIR=/tmp/gmq-s3a-tmp`.
- Red: `test/amqp10_intent_test.exs` + `test/amqp10_reconnect_test.exs` 18 tests, 6 failures (3 `Intent` undefined; 3 restoration shell tests: no `intent:` status key, no restored Begin, no `{:restore_complete, _}`).
- Green: the same files 18 tests, 0 failures. `mix compile --warnings-as-errors` clean. Seeds 1, 2, 3 over `amqp10_reconnect_test`, `amqp10_intent_test`, `amqp10_client_test`, `amqp10_sasl_client_test`, `tls_transport_test`, `privacy_test`: 54 tests, 0 failures each. `mix graviton_mq.check_architecture` passed (7950 references, 7 applications).
- Deviations: (1) step 5c: the command `cond` already had the Task 8 / coordinator host-close clauses, so `restoring_target?/2` became a clause before `true`, not a replacement `else` branch. The host-close behaviour from `0312b11` is unchanged. (2) step 5f: the `{:restore, generation}` wake matches the current generation in the clause head. A stale wake falls through to the catch-all and leaves `restore_pending` alone, so it cannot clear the current connection's pending wake. (3) `recv_frame!/2` regained its `timeout \\ 1_000` default, because the restoration tests call `recv_frame!(socket)`.

### 2026-11-12 — Stream A, Task 10 (local evidence: SASL per attempt, privacy, TLS per attempt)

- Commit `8d0a78e` (tests only; no `lib/` change). Build root `/tmp/gmq-build-s3a`, `TMPDIR=/tmp/gmq-s3a-tmp`.
- Evidence tests, green on first run as the plan expects (no red step): the three new tests plus their files, 27 tests, 0 failures. Local TLS peer is OTP `:ssl` in `tls_transport_test.exs`; no CI evidence is claimed.
- Mutation checks (each reverted): (1) caching the first attempt's SASL credentials fails the SASL test on attempt 2 at `assert_receive :provided` (before the counter assertion). (2) The plan's mutation, `transport_reason/1` returning a raw tuple, fails the privacy test for the wrong reason: the tuple reason is no longer retried, so `{:reconnect_exhausted, 2}` never arrives. Two substitute mutations that keep classification intact both FAIL at `refute printed =~ host`: the host appended to the `{:disconnected, _}` event, and a `peer: host` status key. Transport.connect returns no address tuple, so the plan's literal mutation has no privacy-relevant analogue. (3) `transient({:connect, {:tls_alert, _}}) -> :retry` fails the TLS test at `refute_receive {:reconnecting, _}`.
- Regression, seeds 1, 2, 3 over `amqp10_reconnect_test`, `amqp10_intent_test`, `amqp10_client_test`, `amqp10_sasl_client_test`, `tls_transport_test`, `privacy_test`: 57 tests, 0 failures each.
- Deviations: the SASL test registers `on_exit` to close each accepted socket (as `handshake!/2` does). The Step 4 seed command adds `amqp10_intent_test` and uses the Stream A build root.
- Task 9 concern 2 (restoration failed while the Endpoint is `:closing`): reproducible, not fixed, for the coordinator. `Endpoint.begin_close/2` cancels only `:receive_idle` and `:heartbeat`; `expired(state, {:operation, op}, _)` has no phase guard. A scratch test (not committed) with `idle_timeout_ms: 200, operation_timeout_ms: 300, close_timeout_ms: 500`: the peer is silent after the restored Begin, the client begins an idle Close, then the Begin's operation deadline fires during `:closing`. The observer receives `{:restored, {:session, 1}, {:unknown, :timeout}}` with `phase: :closing`, the Session is dropped from intent (`intent: %{sessions: 0}`), and the next connection restores nothing. The loss was then reported as `{:disconnected, :closed}`, not `:close_timeout`; not investigated.

### 2026-11-12 — Stream B, Task 4 (async register and cleanup disconnect)

- Task 4 (e71e62d): red, availability 3 tests, 1 failure ("took 5003 ms, limit 500 ms", the synchronous register), as expected. Green: `mix compile --warnings-as-errors` clean; availability, boundaries, runtime, g5_flow, g5_annotations and sasl together 42 tests, 0 failures, 2 excluded, with seeds 1, 2 and 3; runtime `test/amqp10` 104 tests, 0 failures (105 before; the count changed with S1b's in-progress `connection_test.exs`/`connection_input_test.exs` edits, not with S3).
- Deviation: the plan's code alone gave 42 tests, 1 failure with seeds 1 and 2. The failure was the boundaries test "uncertain detach cleanup fences only its queue…", with the Manager crashing on `ArgumentError` from `:ets.insert/2` in `handle_call({:status, …})`. Root cause: once the cleanup disconnect is async, the Manager answers a status call while the suspended queue is not fenced yet. It then blocks 5 s in the synchronous `Queue.status/1`, the caller's 5 s `GenServer.call` times out, the caller deletes its table, and the late insert raises. Minimal fix: the insert rescues `ArgumentError`, because a caller that timed out awaits nothing. Task 5 removes the in-handler `Queue.status/1`; the guard stays valid there.
- Out of scope: one seed 3 run failed `AMQP10SASLTest` "our PLAIN client reaches our broker over verified TLS…" while S1b was editing runtime `listener/connection/writer.ex`; both reruns of seed 3 passed (42/0). Not investigated further.
- No `abandoned/3` clause was added (the plan's Task 4 specifies none). The `:registering` branch of `fence/2` is now live, exercised by the new availability test (register expires as not executed, fence rejects the Link).

### 2026-11-12 — Stream B, Task 5 (async enqueue, watch_ack, restore and public status)

- Task 5 (9f1cf45): red, availability + boundaries 11 tests, 4 failures, as expected: Task 3 test and B2 "took 5001/5002 ms, limit 1000 ms"; B5 `{:unknown, :closed}` instead of `Released`; B3 `{:error, :not_running}` (the status call timed out) instead of `:unresponsive`. Green: the same files 11 tests, 0 failures; `mix compile --warnings-as-errors` clean; availability, boundaries, runtime, g5_flow, g5_annotations and sasl together 45 tests, 0 failures, 2 excluded, with seeds 1, 2 and 3; runtime `test/amqp10` 104 tests, 0 failures (3 reruns).
- Deviation (Task 4 hand-off): the `ArgumentError` rescue around the status insert is removed. `status/1` creates its exchange with `{:heir, manager, :amqp10_status}`, deletes it only after the Manager's `:ok` reply, and on a failed call hands it to the Manager with `:ets.give_away/3`, then re-exits with the call's reason (a timed-out status stays an exit; the facade maps it to `{:error, :not_running}`). The Manager deletes the table on `{:"ETS-TRANSFER", table, _, :amqp10_status}`. The handler's existing owner check skips a table its caller no longer owns, so the insert (`true = :ets.insert/2`) cannot meet a deleted table. The one remaining rescue is caller-side: `give_away/3` raises `ArgumentError` only if the Manager is no longer alive; the caller then deletes its table. `status/1` gained an `is_pid/1` guard (the facade passes a pid; `:heir` needs one).
- Added test (availability): "a timed-out status call stays an exit and its late handler does not fail the Manager" (suspends the Manager 5 s; asserts `{:timeout, _}`, Manager survives, no table leaked). Mutation checks: dropping the ETS-TRANSFER delete fails it (leaked table); making the caller delete instead of hand over does **not** fail it, because without a blocking call in the handler the check-to-insert window is too narrow to hit deterministically.
- D5 (queue DOWN): no `info/2` clause is needed. Every `:gen_server.send_request/2` monitors the queue, and `check_response/3` turns its DOWN into `{:error, {reason, queue}}`, which `queue_response/4` classifies by take-back (`Queue.response/3`), or frees as a `waiting` entry. This was already in place since Task 3.
- Out of scope: the first runtime `test/amqp10` run had 1 failure, `DurableQueueTest` "a queue whose log already reached its limit compacts on open and accepts work" (no Broker reference; `queue.ex` unchanged in this task). Three reruns passed 104/0. Not investigated further.

### 2026-11-12 — Stream A, Task 11 (multi-accept cut proxy and Artemis R1–R6, F1–F3)

- Commit `365d1b9` (tests only; no `lib/` change). Build root `/tmp/gmq-build-s3a`, `TMPDIR=/tmp/gmq-s3a-tmp`.
- Step 2: `amqp_peer.py` gains `drain` only; `ast.parse` check passed. Proton is not installed locally, so `drain` has not executed.
- Step 4: `mix test --warnings-as-errors test/amqp10_artemis_reconnect_test.exs test/amqp10_artemis_test.exs`: 17 tests, 0 failures, 17 excluded (9 new + 8 existing). Self-to-self proxy check (scratch ExUnit script run with `mix run` in test env, not committed): our broker (`auth: :anonymous`, TCP) behind `AMQPCutProxy`, our client through it. `cut/1` gave `{:proxy_closed, 1}`, `{:disconnected, :closed}`, `{:reconnecting, 1}`, accept 2, `{:connected, _}`, restored Session and Link, `{:restore_complete, %{restored: 2, failed: 0}}`, then an Accepted publish with `{:proxy_transfer, 2}`. `cut_on_disposition/1` gave `{:proxy_cut, 1, :disposition}`, `{:unknown, _}` and no Transfer on pair 2 within 500 ms. `refuse/2` gave accept gaps `[202, 402]` ms (R4 shape) and, with a permanent refusal, `{:reconnect_exhausted, 3}`, `owner: :stopped`, no timers, no accept 5. `blackhole/1` with `idle_timeout_ms: 2_000, close_timeout_ms: 500` gave `{:disconnected, :close_timeout}` then reconnection and `{:restore_complete, _}`. `close/1` during a 2 s backoff gave `{:ok, :closed}` within 100 ms and no accept 2 within 2.5 s. This is self-to-self extra evidence only.
- Regression, seed 1 over `amqp10_reconnect_test`, `amqp10_intent_test`, `amqp10_client_test`, `amqp10_sasl_client_test`, `tls_transport_test`, `privacy_test` and `amqp10_artemis_reconnect_test`: 68 tests, 0 failures, 9 excluded. `MIX_ENV=test mix compile --warnings-as-errors` clean.
- CI: R1–R6 and F1–F3 are **unverified** until an `interoperability` run (Task C1). `run.py` already exports `ARTEMIS_*`/`GRAVITON_PROTON_PYTHON` and `ci.yml` runs plain `mix test` under it, so no harness or workflow change is needed.
- Deviations: none in the committed code beyond `mix format` line wrapping.

### 2026-11-12 — Stream B, Task 6 (stand-in queue fault injection and Manager death)

- Task 6 (879cc55, tests only; no `lib/` change). Build root `/tmp/gmq-build-s3b`, `TMPDIR=/tmp/gmq-s3b-tmp`.
- Evidence tests, green on first run as the plan expects: the plan's five tests (B4 settle, B4 claim, B4 enqueue, B6, B7) 5 tests, 0 failures; with the two added tests 7 tests, 0 failures.
- Added tests (Task 5 hand-off): (1) "B4/D10: a fence Releases an enqueue it abandons before it detaches the publisher": a held settle makes the stand-in stop taking, so a later enqueue waits untaken; the settle's deadline fences the queue, and the first publisher event in mailbox order is `{:ok, %Outcome.Released{}}`, before the sender's Detach; the queue keeps only the returned claim. (2) "B4: a restore request to a fenced queue ends at its deadline and re-probes": after the fence, the stand-in holds the restore `watch`; status stays `:unavailable` with `queue_fences: 1` (the fenced queue's `fence/2` is a no-op, so nothing abandons the restore); no probe arrives within 4.5 s, then the re-probe's `status` queues behind the held watch; after release the queue is restored without a second fence, and the returned message is delivered again.
- Support deviation: the stand-in also sends `{:stand_in_queued, pid, kind}` when a request queues behind a held one (it peeks at the exchange and does not take the request), so the restore test can observe the re-probe.
- Mutation checks (each reverted, `broker.ex` diff empty): (1) register `_removed_or_replaced` → `state`: B6 FAILS (`consumers` stays 1). (2a) `fence/2` without the `abandoned/3` reduce: B4 enqueue and B5 still pass; the new D10 test FAILS (`{:unknown, {:peer_detach, _}}` instead of `Released`). (2b) `publication(:uncertain, _)` → `Released`: B4 enqueue FAILS. (3) settle `{_not_executed_or_uncertain, _}` → `state`: B4 settle FAILS (no detach), and so do both added tests, which depend on that fence. (4) extra: restore_watch failure without a re-probe: the restore test FAILS at the queued `status`.
- Regression: manager_requests, availability, boundaries, runtime, sasl, g5_flow and g5_annotations together 52 tests, 0 failures, 2 excluded, with seeds 1, 2 and 3. Runtime `test/amqp10`, seeds 1 and 2: 106 tests, 0 failures. `mix compile --warnings-as-errors` clean.
- Out of scope: the first seed 3 runtime run had 1 failure, `AuthTest` "the watcher exits as soon as its callback finishes" (no Broker/Queue reference; no `lib/` change in this task). Two seed 3 reruns passed 106/0. Not investigated further.
- Note for the coordinator: `Broker.status/1` keeps the 5 s Manager call timeout, so the 1 s bound (D14) holds only while the Manager itself is responsive (unchanged, as planned).

### 2026-11-13 — C-tasks done

- **C1 (CI gate).** CI run 43223448120 for `375f677`: `interoperability` and
  `checks` PASS; Tests run 43223447801 PASS. All nine Artemis reconnect tests
  R1–R6 and F1–F3 (`amqp10_artemis_reconnect_test.exs`) passed against
  Artemis 2.41.0. The earlier CI run 43214315813 at `83567a7` failed only R2:
  Artemis 2.41.0 auto-creates a bare address as MULTICAST when no
  `default-address-routing-type` is set, so the receiver's temporary queue
  was deleted with its connection, unsettled message included. `375f677`
  makes R2 use `anycast://` (acceptor `anycastPrefix` in `run.py`); the
  client sends no source capabilities. Test totals per job were not
  recorded in this pass.
- **C2 (docs).** AMQP10_CLIENT.md "Reconnect" section (options and backoff,
  classification with the D1 close-cause note and D4 additions, owner phases
  and command answers per D3, `:stale_delivery`, restoration events and order
  per D13 including `f96595c`, host `close/1` during an in-flight Close final
  per `0312b11`, host actions, SASL/TLS per attempt, D2 status keys, the
  half-open limitation, the Artemis routing note). AMQP10_BROKER.md
  "Asynchronous Manager" (5 s request deadline, three-way classification and
  per-path table with D7, `Released` for a not-executed enqueue, D10 order,
  `queue_requests`/`queue_fences`/`:unresponsive`, the 1 s bound with the
  900 ms queue part and the 5 s Manager-call caveat, the boot `watch`, an
  upgrade note); the 2026-11-08 "first unanswered call still blocks" limit is
  marked removed. FAILURE_MODEL.md and DELIVERY_SEMANTICS.md S3 amendments
  (at-least-once, no replay, no exactly-once). CAPABILITIES AMQP1 client and
  broker rows; ACTIVE_SCOPE S3 row Complete.
- **C3.** "S3 maintenance amendment (2026-11-13)" in
  `docs/qualification/2026-10-30-amqp10-clause-audit.md`; no requirement ID
  or inventory status changed.
- **C4.** Not rerun in this documentation pass (no code change); the hosted
  `checks` job of run 43223448120 ran `deps.get`, the format check,
  `compile --warnings-as-errors`, `mix test`, the architecture check and the
  cycle check on `375f677`.
  Documentation verification: `release_test.exs` + `architecture_test.exs`
  and `mix format --check-formatted`.
- Task 10 concern 2 (restoration failing while the Endpoint is `:closing`)
  was fixed by `f96595c` (D13 interruption).
