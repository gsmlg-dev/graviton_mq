# S0 hygiene and S1 transport spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete S0 (test and documentation hygiene) and run the blocking S1
spike from the [P0–P3 design](../specs/2026-11-12-http-fetch-transport-and-p0-p3-design.md) §3.5.

**Architecture:** S0 changes only tests and documentation. The S1 spike runs in a
throwaway Mix project outside the repository and changes no GravitonMQ source;
its result decides the S1 process layout, which a follow-up plan specifies.

**Tech Stack:** Elixir 1.18 / OTP 28 umbrella; ExUnit; `hell` and `ex_ssl`
`1.0.0-rc.13` (spike only); `openssl` CLI for spike certificates.

**Repository rules that apply to every task:** use `.tool-versions`; never weaken
an assertion; no commit, push or release without explicit owner authorization
(each task ends with a checkpoint instead of a commit); keep zero warnings.

---

## Evidence behind S0 (2026-11-11/12)

- Full `mix test`: runtime `92 tests, 2 failures` then `3 failures` on rerun.
- `GravitonMQ.Runtime.{AMQP10,MQTT}.WriterTest` "status and callback crash
  reports…" (`writer_test.exs:181`) failed at line 202 `assert_receive {:DOWN, …}`
  with the default 100 ms timeout under load average ≈ 10; 3/3 isolated runs pass.
- `DurableQueueTest` "a small log limit compacts repeatedly…"
  (`durable_queue_test.exs:237`) hit the 60 s ExUnit timeout while synchronous
  512-byte writes measured 63 ms each (`dd … oflag=dsync`: 50 writes in 3.15 s).
  On 2026-11-12 the same probe measured 0.16 ms and 15/15 runs passed in ≈3 s.
  The test is fsync-latency bound, not hung.

## File map

| File | Change |
| --- | --- |
| `apps/graviton_mq_runtime/test/test_helper.exs` | Raise default `assert_receive` wait |
| `apps/graviton_mq_runtime/test/amqp10/durable_queue_test.exs` | Assert repeated compaction explicitly; fsync-tolerant timeout |
| `docs/CAPABILITIES.md` | Current-status summary; remove stale PENDING statements |
| `docs/ACTIVE_SCOPE.md` | Record P0–P3 authorization; fix stale Agent Note statement |
| `/tmp/gmq-spike/**` | Throwaway spike project (not in repository) |
| `docs/superpowers/specs/2026-11-12-http-fetch-transport-and-p0-p3-design.md` | Append spike results (§3.5) |

---

### Task 1: Load-tolerant `assert_receive` in runtime tests

`assert_receive_timeout` only extends how long a *successful* receive may take;
a missing message still fails. `refute_receive` keeps its own 100 ms default, so
no negative assertion is weakened.

**Files:**
- Modify: `apps/graviton_mq_runtime/test/test_helper.exs:1`

- [ ] **Step 1: Reproduce under load**

Run (load generator in the background, then the two Writer files 5 times):

```bash
cd apps/graviton_mq_runtime
(for i in $(seq 1 $(nproc)); do timeout 120 sh -c 'while :; do :; done' & done; wait) &
for i in 1 2 3 4 5; do mix test test/amqp10/writer_test.exs test/mqtt/writer_test.exs 2>&1 | grep -E "tests,"; done
wait
```

Expected: at least one run reports `1 failure` or `2 failures` at
`writer_test.exs:202`. If no failure reproduces, record that and still apply
Step 2 (the full-suite failure is already recorded above).

- [ ] **Step 2: Change the default**

Replace the whole file content:

```elixir
# Under host load a crash and its DOWN delivery can exceed ExUnit's 100 ms
# default; assert_receive still fails when the message never arrives.
ExUnit.start(assert_receive_timeout: 2_000)
```

- [ ] **Step 3: Verify**

Repeat the Step 1 command. Expected: every run `18 tests, 0 failures`.
Then `mix test` in `apps/graviton_mq_runtime`. Expected: Writer tests pass;
total runtime duration increases by less than 10 s compared with
`Finished in 190.4 seconds` recorded on 2026-11-11.

- [ ] **Step 4: Checkpoint** — no commit; note results for Task 5.

### Task 2: Make the compaction test prove repetition and tolerate slow fsync

**Files:**
- Modify: `apps/graviton_mq_runtime/test/amqp10/durable_queue_test.exs:237-268`

- [ ] **Step 1: Measure the generation reached today**

Temporarily add after `assert [log] = …` (line 266):

```elixir
IO.inspect(log, label: "compacted log")
```

Run: `mix test test/amqp10/durable_queue_test.exs:237`
Expected: output `compacted log: "events.<G>.wal"`. Record `<G>` and remove the
`IO.inspect` line.

- [ ] **Step 2: Assert repetition explicitly (strengthens the test)**

Replace

```elixir
    assert log =~ ~r/\Aevents\.[1-9][0-9]*\.wal\z/
```

with

```elixir
    assert [_, generation] = Regex.run(~r/\Aevents\.([1-9][0-9]*)\.wal\z/, log)
    # "Repeatedly" means at least two completed compactions.
    assert String.to_integer(generation) >= 2
```

If `<G>` from Step 1 is `1`, this assertion fails: stop and report, because the
test name then overstates its coverage (do not lower the bound).

- [ ] **Step 3: Give the fsync-bound test an explicit budget**

Insert directly above the `test "a small log limit compacts repeatedly…"` line:

```elixir
  # Every iteration waits for durable-through; on a contended disk (63 ms per
  # synchronous write observed 2026-11-11) the default 60 s budget is too small.
  @tag timeout: 300_000
```

- [ ] **Step 3b: Same budget for the two other fsync-bound loops** (added
2026-11-12 after Task 1's full run timed out in them while synchronous writes
measured 82–418 ms under host swap/I/O pressure; each passes in < 7 s on an
idle disk). Insert the identical three-line comment and `@tag timeout: 300_000`
directly above:
- `test "a queue whose log already reached its limit compacts on open and accepts work"`
  in `apps/graviton_mq_runtime/test/amqp10/durable_queue_test.exs` (≈line 285);
- `test "repeated snapshots compact to the latest one instead of exhausting the log"`
  in `apps/graviton_mq_runtime/test/mqtt/persistence_test.exs` (≈line 15).
Do not change their assertions.

- [ ] **Step 4: Verify**

Run: `mix test test/amqp10/durable_queue_test.exs test/mqtt/persistence_test.exs`
in `apps/graviton_mq_runtime`.
Expected: `0 failures` (Linux-only tests included on Linux).

- [ ] **Step 5: Checkpoint** — no commit.

### Task 3: Documentation corrections and current-status summary

**Files:**
- Modify: `docs/CAPABILITIES.md:1-27`, `docs/CAPABILITIES.md:53`
- Modify: `docs/ACTIVE_SCOPE.md` (top and final paragraph)

- [ ] **Step 1: Insert a current-status section** directly below the
`# Capability and qualification matrix` heading:

```markdown
## Current status (2026-11-12)

Latest release: **0.3.0**, built from `438acf4`; hosted CI run 42501601934
passed 1,215 tests including all four independent peers (Mosquitto, Paho,
Artemis/Proton client paths and Proton broker paths), the four-role soak and
installed consumers. The four network roles are qualified for the selected
profiles in the table below; the per-role limitations column lists what is not
supported. Dated paragraphs further down are historical records.
```

- [ ] **Step 2: Remove the stale PENDING statement.** In the "Maintenance update,
2026-11-08" paragraph, replace

```text
A
follow-up stops fencing on unknown enqueue outcomes; independent-peer
requalification is PENDING its CI interoperability run.
```

with

```text
The
follow-up `438acf4` stops fencing on unknown enqueue outcomes; its hosted CI
run 42501601934 passed the independent-peer requalification.
```

and in the AMQP1 broker row replace `independent-peer requalification pending`
with `independent-peer requalification passed in CI run 42501601934`.

- [ ] **Step 3: Record the authorization in ACTIVE_SCOPE.** Insert at the top,
below `# Active implementation scope`:

```markdown
## P0–P3 implementation authorization — 2026-11-12

The owner authorized sub-projects S0–S7 of the
[P0–P3 design](superpowers/specs/2026-11-12-http-fetch-transport-and-p0-p3-design.md):
hygiene, owned transport on `hell`/`ex_ssl` (TLS 1.3-only broker), AMQP SASL
and client reconnect, MQTT topic aliases/subscription identifiers/shared
subscriptions, performance/telemetry/TTL, MQTT over WebSocket via
`http_web_socket` and `gaoxy_http`, and MQTT 3.1.1 runtime. UDP/QUIC,
clustering/Raft, management UI, bridges, transactions and AMQP 0-9-1 remain
excluded. Commit, push, version bump, release and publication are not
authorized. No sub-project is complete until its recorded exit evidence exists.
```

- [ ] **Step 4: Fix the Agent Note statement.** Replace the final paragraph
beginning `Agent Note has no explicitly configured project label` with:

```markdown
Agent Note project: `graviton_mq` (see AGENTS.md).
```

- [ ] **Step 5: Verify** — `grep -n "PENDING" docs/CAPABILITIES.md` returns no
line about the 2026-11-08 requalification; links resolve
(`ls docs/superpowers/specs/2026-11-12-http-fetch-transport-and-p0-p3-design.md`).

- [ ] **Step 6: Checkpoint** — no commit.

### Task 4: Track the open Low-severity review findings

The 0.3.0 release notes say Low findings "remain open", but no list exists in
the repository or Agent Note.

- [ ] **Step 1:** Obtain the 2026-11-08 review's Low findings from the owner.
- [ ] **Step 2:** For each finding, after `gh issue list --repo gsmlg-dev/graviton_mq --search "<keyword>"`
shows no duplicate, create one issue:

```bash
gh issue create --repo gsmlg-dev/graviton_mq \
  --title "<finding title>" \
  --body "Source: 2026-11-08 repository review (Low). <finding text, location, failure scenario>"
```

- [ ] **Step 3:** If the owner cannot supply the list, record in the S0 report
that the findings are untracked and continue; do not invent findings.

### Task 5: S0 required verification

- [ ] **Step 1:** From the umbrella root run the complete AGENTS.md sequence:

```bash
mix deps.get && mix format && mix format --check-formatted && \
mix compile --warnings-as-errors && mix test && \
mix graviton_mq.check_architecture && mix help xref >/dev/null && \
mix xref graph --format cycles --fail-above 0
```

Expected: every app `0 failures`; `Architecture check passed (…)`; `No cycles found`.
Record per-app counts and the host's toolchain (`elixir --version`).

- [ ] **Step 2:** `git diff --stat` shows only the four files of Tasks 1–3 plus
the plan/spec documents.

### Task 6: S1 blocking spike (throwaway, outside the repository)

Checks from design §3.5: (a) non-owner `SSL.send/2` on an accepted ex_ssl
socket while Hell receives, (b) `Hell.Socket.peercert/1` under mTLS,
(c) pausing input for backpressure. Hell re-arms `active: :once` after every
`handle_data/3` return (`hell/lib/hell/handler.ex` `handle_continuation/2`),
so (c) is expected to need either a re-arm-free path or an upstream feature.

**Files (all under `/tmp/gmq-spike`, never inside the repository):**
- Create: `mix.exs`, `test/test_helper.exs`, `test/spike_test.exs`, `certs/*`

- [ ] **Step 1: Project and certificates**

```bash
mkdir -p /tmp/gmq-spike/test /tmp/gmq-spike/certs && cd /tmp/gmq-spike/certs
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
  -subj "/CN=spike-ca" -keyout ca.key -out ca.pem
for n in server client; do
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "/CN=$n" -keyout $n.key -out $n.csr
  printf "subjectAltName=DNS:localhost\nextendedKeyUsage=%s\n" \
    $([ $n = server ] && echo serverAuth || echo clientAuth) > $n.ext
  openssl x509 -req -in $n.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days 2 -extfile $n.ext -out $n.pem
done
```

`/tmp/gmq-spike/mix.exs`:

```elixir
defmodule GmqSpike.MixProject do
  use Mix.Project

  def project,
    do: [app: :gmq_spike, version: "0.0.0", elixir: "~> 1.18", deps: deps()]

  def application, do: [extra_applications: [:logger]]

  defp deps,
    do: [{:hell, "== 1.0.0-rc.13"}, {:ex_ssl, "== 1.0.0-rc.13"}]
end
```

`/tmp/gmq-spike/test/test_helper.exs`:

```elixir
ExUnit.start(assert_receive_timeout: 2_000)
```

- [ ] **Step 2: Spike test** — `/tmp/gmq-spike/test/spike_test.exs`:

```elixir
defmodule GmqSpikeTest do
  use ExUnit.Case, async: false

  @certs Path.expand("../certs", __DIR__)

  defmodule Handler do
    use Hell.Handler

    @impl Hell.Handler
    def handle_connection(socket, test) do
      send(test, {:connected, self(), socket.socket, Hell.Socket.peercert(socket)})
      {:continue, test}
    end

    @impl Hell.Handler
    def handle_data(data, _socket, test) do
      send(test, {:server_received, data})
      {:continue, test}
    end

    # Pause candidate: Hell re-armed active: :once when handle_data returned,
    # so turning it off from a later message stops further delivery.
    @impl GenServer
    def handle_info(:pause, {socket, test}) do
      :ok = Hell.Socket.setopts(socket, active: false)
      send(test, :paused)
      {:noreply, {socket, test}}
    end

    def handle_info(:resume, {socket, test}) do
      :ok = Hell.Socket.setopts(socket, active: :once)
      {:noreply, {socket, test}}
    end
  end

  setup do
    {:ok, server} =
      Hell.start_link(
        port: 0,
        handler_module: Handler,
        handler_options: self(),
        transport_module: Hell.Transports.SSL,
        transport_options: [
          certfile: Path.join(@certs, "server.pem"),
          keyfile: Path.join(@certs, "server.key"),
          cacertfile: Path.join(@certs, "ca.pem"),
          verify: :verify_peer,
          fail_if_no_peer_cert: true
        ]
      )

    {:ok, {_ip, port}} = Hell.listener_info(server)

    {:ok, client} =
      SSL.connect(~c"localhost", port,
        cacertfile: Path.join(@certs, "ca.pem"),
        certfile: Path.join(@certs, "client.pem"),
        keyfile: Path.join(@certs, "client.key"),
        verify: :verify_peer,
        active: false
      )

    assert_receive {:connected, handler, raw, peercert}
    %{client: client, handler: handler, raw: raw, peercert: peercert}
  end

  test "(b) peercert returns the client leaf", %{peercert: peercert} do
    assert {:ok, der} = peercert
    assert is_binary(der)
  end

  test "(a) non-owner process sends while the handler receives", ctx do
    writer =
      Task.async(fn ->
        for i <- 1..200, do: :ok = SSL.send(ctx.raw, "w#{i};")
        :done
      end)

    for i <- 1..200, do: :ok = SSL.send(ctx.client, "c#{i};")
    assert :done = Task.await(writer)

    expected = Enum.map_join(1..200, &"w#{&1};")
    assert {:ok, ^expected} = recv_exact(ctx.client, byte_size(expected))
    assert_server_bytes(Enum.map_join(1..200, &"c#{&1};"))
  end

  test "(c) input can be paused and resumed without losing bytes", ctx do
    send(ctx.handler, :pause)
    assert_receive :paused

    :ok = SSL.send(ctx.client, "after-pause")
    refute_receive {:server_received, _}, 500

    send(ctx.handler, :resume)
    assert_server_bytes("after-pause")
  end

  defp recv_exact(socket, size, acc \\ "") do
    if byte_size(acc) >= size do
      {:ok, acc}
    else
      case SSL.recv(socket, 0, 5_000) do
        {:ok, data} -> recv_exact(socket, size, acc <> data)
        error -> error
      end
    end
  end

  defp assert_server_bytes(expected, acc \\ "") do
    if byte_size(acc) >= byte_size(expected) do
      assert acc == expected
    else
      assert_receive {:server_received, data}
      assert_server_bytes(expected, acc <> data)
    end
  end
end
```

Hell exposes no pause result, so test (c) checks the only candidate available
without upstream change: toggling `active` from the handler's own
`handle_info/2`. Pass criterion: while paused nothing arrives within 500 ms,
and after resuming every byte arrives exactly once, in order. If `setopts`
returns an error on the ex_ssl server socket, or bytes arrive while paused,
(c) fails and Task 6 Step 5 applies.

- [ ] **Step 3: Run**

```bash
cd /tmp/gmq-spike && mix deps.get && mix test
```

Record each result (PASS/FAIL with the exact error). If an API name used above
differs in rc.13, use the documented equivalent from the Hell/ex_ssl docs and
record the difference.

- [ ] **Step 4: Record results.** Append a `### 3.5.1 Spike results (date)`
subsection to the design spec with: package versions, the three outcomes,
the chosen S1 process layout (Connection inside the Hell handler vs separate
owner), and any upstream gap.

- [ ] **Step 5: Upstream gaps.** For any failed check, load the
`upstream-issue-routing` skill before filing an http_fetch issue. A confirmed
blocker stops S1 (no local workaround); report it with the issue link.

- [ ] **Step 6:** `rm -rf /tmp/gmq-spike` after results are recorded.

### Task 7: Hand-off

- [ ] Write the follow-up plan `docs/superpowers/plans/<date>-s1-owned-transport.md`
from the recorded spike results (listener replacement, transport adapter,
client TLS, configuration validation, architecture rules, ADR 0009, AGENTS.md
amendment, interop requalification per design §3.7).
