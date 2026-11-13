# S5a Performance Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reproducible two-VM throughput and latency benchmark (`scripts/bench/`) for five MQTT 5.0 / AMQP 1.0 scenarios, record a `full` baseline of this host in `docs/qualification/`, and run the `smoke` profile in the CI `checks` job, without changing product code.

**Architecture:** `mix run --no-start scripts/bench/run.exs` is the generator VM. For each scenario × N it spawns `elixir -pa <ebin…> scripts/bench/broker_vm.exs`, a Mix-free broker VM that starts one `GravitonMQ.start_link/1` instance, prints `READY <os_pid> <mqtt_port> <amqp_port>`, answers `STATUS` lines on stdin and stops on stdin EOF. N publisher processes (one connection each, one outstanding publication) and one subscriber use only `GravitonMQ.MQTTClient` and `GravitonMQ.AMQP10Client`. Exact percentiles and sequence accounting are pure functions in `scripts/bench/stats.exs`, tested by ExUnit. The generator writes `<out>/results.json` and exits non-zero if any scenario fails its correctness gate.

**Tech Stack:** Elixir 1.18.4 / OTP 28 umbrella (`.tool-versions`), ExUnit, OTP ports, `:json`, GitHub Actions.

**Input spec:** [`docs/superpowers/specs/2026-11-13-s5a-performance-baseline-design.md`](../specs/2026-11-13-s5a-performance-baseline-design.md), approved 2026-11-13. This plan implements exactly that spec.

---

## Execution rules (read before Task 0)

1. **Isolated build.** Every mix command is prefixed with
   `env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp`. Never run a bare `mix`. `ROOT` below means `/home/gao/Workspace/gsmlg-dev/graviton_mq`; commands are always written in full.
2. **Formatting.** Format only the files the task touched:
   `cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix format <paths>`. Umbrella-wide `mix format` runs only in Task 6.
3. **Commits.** One commit per task on `main`, conventional message, **no `Co-Authored-By` trailer and no "Generated with" line**. Commit only the task's paths:
   `git add -- <paths> && git commit --only -m "<message>" -- <paths>`. Never `git add -A`/`.`. Never push; the coordinator pushes (Task 7). If `.git/index.lock` exists, wait and retry; never delete it.
4. **No product change.** Nothing under `apps/*/lib` changes. If the benchmark seems to need a product change (an option, a status field, a bug fix), stop and report to the coordinator with the evidence. Do not work around broker behaviour in the scripts.
5. **No interop claim.** The generator uses our own clients. Results are local performance records, never interoperability evidence, and no threshold fails anything.
6. **Agent Note.** Contributors do not write shared notes; return candidate findings to the coordinator.
7. **zsh.** The interactive shell is zsh, which does not word-split `$var`. Multi-argument shell snippets below run under `bash -c '…'` or spell paths out.

## Decisions taken while planning (spec gaps; also listed in the final report)

- **D1 QoS 0 window.** For `mqtt_qos0_mem` acceptance waits for nothing, so "one outstanding publication" means: the publisher waits for the local `{:ok, :sent}` result and then for the subscriber's in-VM `{:bench_delivered, sequence}` echo before publishing again. Without that bound 64 publishers flood a single subscriber, whose client terminates with 0x97 once `max_deliveries` QoS 0 packets are buffered. QoS 0 has no acceptance latency (`null`); its throughput is delivered messages per second (spec §5).
- **D2 Sequence identity and payload.** Each payload starts with `<<publisher::16, sequence::32, sent_at_us::signed-64>>` (14 bytes) followed by zero padding to `--payload-bytes` (default 256, allowed 16..16_384; the upper bound keeps 64 AMQP credits within the client session's 4 MiB incoming budget). The sequence key is `{publisher, sequence}`; each publisher numbers 1..count.
- **D3 Gate details.** Besides spec §5, a consumed key that no publisher published ("unexpected") also fails the gate. Drained means: MQTT `storage == ready`, `pending_deliveries == 0`, `prepared == 0`; AMQP queue `messages == 0` and `inflight == 0`. The broker answering `STATUS` with the expected `profile` (`memory` or `local_durable`) is the "broker VM stays alive" check and proves the durable path is selected. A broker VM that had to be killed at stop fails the scenario.
- **D4 AMQP durable acceptance.** The client only observes `%Outcome.Accepted{}`; `{:accepted_durable, commit_ref}` is the broker's internal queue receipt (docs/AMQP10_BROKER.md). The durable scenario sends a Header with `durable = true` (`{:header, Value.list([Value.boolean(true)])}`), which the memory profile rejects, and checks `profile: local_durable`.
- **D5 Broker VM shape.** Only the scenario's protocol is enabled; the other port is printed as `0`. Besides EOF, the broker VM answers a `STATUS` stdin line with one `STATUS <json>` line. Capacity limits are raised so that capacity is never the failure cause (MQTT `max_connections`/`max_sessions` 256, `max_inflight` 1,024, pending limits 262,144 / 256 MiB, `max_session_bytes` 256 MiB; AMQP `max_connections` 256, `max_auth_jobs` 128 — the default 16 refused concurrent attaches at N = 64 with `:unsupported_link` — and queue `max_messages` 1,000,000 / `max_bytes` 1 GiB). Timing options stay at their defaults.
- **D6 Client timeouts.** The spec fixes only READY (20 s) and broker stop (10 s). Client operation and handshake timeouts are 60 s (the MQTT maximum), the generator waits 65 s per operation, 120 s for all clients to connect, 60 s of consumer silence after publishing ends and 60 s for drain. Durable acceptance waits for disk synchronization, and the prototype measured 0.4–600 ms per synchronous 4 KiB write on this host.
- **D7 Credit.** The MQTT subscriber uses `max_deliveries`/`receive_maximum` 1,024 with demand refilled every 256 deliveries. The AMQP receiver grants 64 credits (the session's default `max_incoming_deliveries`) and refills in batches of 16 after settlement completes, because settlement releases the reservation.
- **D8 `results.json` layout.** `{"spec", "profile", "environment", "results": [...]}`: the environment is recorded once. Each result has the spec §6 fields plus `reason`, `consume_rate` (spec §4 "consumption rate is recorded"), and on success `published` and `delivered`. A failed result has `null` figures.
- **D9 Options.** `--out` is required; `--data-dir` (default `System.tmp_dir!/0`) selects the filesystem whose type and device are recorded. There is no scenario filter: every run covers all scenarios of its profile.
- **D10 Names.** Script modules use the `GravitonMQBench` top-level namespace, which no umbrella app uses, so `Code.require_file/2` in tests cannot redefine an app module.
- **D11 Host noise.** The qualification record adds a synchronous-write probe (`dd oflag=dsync`) and the load average before and after the run, and Task 4 refuses to record a baseline when the probe exceeds 10 ms per write.

## Prototype observations (planning run on this host, 2026-11-13; expected, not to be "fixed" here)

The complete scripts below were run during planning from `/tmp` against commit `70e0a5e`. The `smoke` profile completed in about 90 s, every data directory was removed, and no broker VM survived. These broker behaviours showed up and are baseline facts for S5b and later work, not tool bugs:

- AMQP acceptance latency is about 42 ms per publication even in memory (`amqp_mem` N = 1: 23.7 msg/s, end-to-end ≈ 4 ms, so delivery arrives before the disposition). This matches Nagle plus delayed ACK. `runtime/hell_handoff.ex` sets `nodelay: false` on broker sockets.
- With N ≥ 8, delivery to the single consumer falls far behind publication (`mqtt_qos1_mem` N = 8: 449 msg/s accepted, 112 msg/s consumed, end-to-end p50 ≈ 4.4 s; `amqp_mem` N = 64: consume 0.9 msg/s while publishers run). Generator mailboxes stayed empty, and a 10 ms broker tick did not change it, so the broker is the bottleneck.
- At N = 64, `mqtt_qos1_durable` failed with `{:error, :unknown_outcome}` (PUBACK beyond 60 s) and `amqp_durable` with `{:unknown, :closed}` (the Manager's 5 s enqueue deadline closed the publisher). While the disk was slow, `mqtt_qos1_durable` N = 8 also failed (`{:connack, 136}` or `:unknown_outcome`).
- Synchronous 4 KiB writes on `/dev/mapper/root_vg-root` (ext4, PERC H730) ranged from 0.36 ms to about 460 ms per write within minutes, with load average about 11. Durable figures on this host depend heavily on concurrent disk activity (D11).

## File map

| File | Status | Single responsibility |
| --- | --- | --- |
| `scripts/bench/stats.exs` | create | Pure helpers: exact nearest-rank latency summary, sequence accounting, gate verdict, rate |
| `apps/graviton_mq/test/graviton_mq/bench_stats_test.exs` | create | ExUnit tests for `stats.exs`, loaded with `Code.require_file/2` |
| `scripts/bench/broker_vm.exs` | create | Mix-free broker VM: one instance per scenario, READY line, STATUS answers, stop and delete its data directory on stdin EOF |
| `scripts/bench/run.exs` | create | Generator VM: options, broker VM lifecycle, publishers and subscriber, correctness gate, `results.json`, summary, exit status |
| `docs/qualification/2026-11-13-s5a-baseline.md` | create | Committed summary of the `full` run on this host |
| `docs/qualification/2026-11-13-s5a-baseline.json` | create | Verbatim `results.json` of that run |
| `.github/workflows/ci.yml` | modify | One `checks` step running the `smoke` profile |
| `docs/superpowers/plans/2026-11-13-s5a-performance-baseline.md` | modify | This plan's execution log only |

---

## Task 0: Preflight

**Files:** none.

- [ ] **Step 1: Confirm a clean `main` at or after the spec commit**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && git status --short && git branch --show-current && git log --oneline -1 -- docs/superpowers/specs/2026-11-13-s5a-performance-baseline-design.md
```
Expected: no status lines, `main`, and `70e0a5e docs: add approved S5a performance baseline design`. If unrelated changes exist, leave them untouched and never include them in a commit.

- [ ] **Step 2: Fetch dependencies and compile the isolated dev build**

Run:
```bash
mkdir -p /tmp/gmq-s5a-tmp && cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix deps.get && env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix compile --warnings-as-errors
```
Expected: exit 0, no warnings. `ls /tmp/gmq-build-s5a/dev/lib` lists `ex_ssl graviton_mq graviton_mq_amqp10 graviton_mq_client graviton_mq_core graviton_mq_mqtt graviton_mq_runtime graviton_mq_storage hell telemetry`.

- [ ] **Step 3: Confirm the host tools the scripts call**

Run:
```bash
command -v elixir git df findmnt dd actionlint && findmnt -n -o SOURCE,FSTYPE -T /tmp/gmq-s5a-tmp
```
Expected: six paths, then a device and filesystem type (on this host `/dev/mapper/root_vg-root ext4`). If `/tmp/gmq-s5a-tmp` is `tmpfs`, stop and report: the baseline needs a real disk.

---

## Task 1: Pure statistics helpers (TDD)

**Files:**
- Create: `apps/graviton_mq/test/graviton_mq/bench_stats_test.exs`
- Create: `scripts/bench/stats.exs`

- [ ] **Step 1: Write the failing test**

Create `apps/graviton_mq/test/graviton_mq/bench_stats_test.exs`:

```elixir
Code.require_file("../../../../scripts/bench/stats.exs", __DIR__)

defmodule GravitonMQ.BenchStatsTest do
  use ExUnit.Case, async: true

  alias GravitonMQBench.Stats

  describe "latency_summary/1" do
    test "has no summary without samples" do
      assert Stats.latency_summary([]) == nil
    end

    test "a single sample is every percentile" do
      assert Stats.latency_summary([42]) == %{count: 1, p50: 42, p99: 42, p999: 42, max: 42}
    end

    test "uses exact nearest ranks on unsorted samples" do
      samples = Enum.shuffle(1..1000)

      assert Stats.latency_summary(samples) ==
               %{count: 1000, p50: 500, p99: 990, p999: 999, max: 1000}
    end

    test "rounds the rank up for small sample counts" do
      assert Stats.latency_summary(Enum.to_list(10..1//-1)) ==
               %{count: 10, p50: 5, p99: 10, p999: 10, max: 10}

      assert Stats.latency_summary([3, 1, 2, 2]) == %{count: 4, p50: 2, p99: 3, p999: 3, max: 3}
    end
  end

  describe "accounting/2" do
    test "every publication consumed once" do
      seen = %{{0, 1} => 1, {0, 2} => 1, {0, 3} => 1, {1, 1} => 1, {1, 2} => 1}
      accounting = Stats.accounting(%{0 => 3, 1 => 2}, seen)

      assert accounting == %{expected: 5, delivered: 5, missing: 0, duplicates: 0, unexpected: 0}
      assert Stats.verdict(accounting) == :ok
    end

    test "duplicates are counted but do not fail the gate" do
      accounting = Stats.accounting(%{0 => 2}, %{{0, 1} => 3, {0, 2} => 1})

      assert accounting == %{expected: 2, delivered: 2, missing: 0, duplicates: 2, unexpected: 0}
      assert Stats.verdict(accounting) == :ok
    end

    test "a missing sequence number fails the gate" do
      accounting = Stats.accounting(%{0 => 3}, %{{0, 1} => 1, {0, 3} => 2})

      assert accounting == %{expected: 3, delivered: 2, missing: 1, duplicates: 1, unexpected: 0}
      assert Stats.verdict(accounting) == {:error, "missing 1, unexpected 0"}
    end

    test "a sequence number nobody published fails the gate" do
      seen = %{{0, 1} => 1, {0, 2} => 1, {7, 1} => 1}
      accounting = Stats.accounting(%{0 => 1}, seen)

      assert accounting == %{expected: 1, delivered: 1, missing: 0, duplicates: 0, unexpected: 2}
      assert Stats.verdict(accounting) == {:error, "missing 0, unexpected 2"}
    end

    test "nothing published and nothing consumed passes" do
      accounting = Stats.accounting(%{0 => 0}, %{})

      assert accounting == %{expected: 0, delivered: 0, missing: 0, duplicates: 0, unexpected: 0}
      assert Stats.verdict(accounting) == :ok
    end
  end

  test "rate/2 is events per second over a millisecond window" do
    assert Stats.rate(500, 2_000) == 250.0
    assert Stats.rate(1, 3_000) == 0.3
    assert Stats.rate(0, 20_000) == 0.0
  end
end
```

- [ ] **Step 2: Run it and see it fail**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix test apps/graviton_mq/test/graviton_mq/bench_stats_test.exs
```
Expected: non-zero exit with `** (Code.LoadError) could not load /home/gao/Workspace/gsmlg-dev/graviton_mq/scripts/bench/stats.exs` (reason `enoent`).

- [ ] **Step 3: Write the implementation**

Create `scripts/bench/stats.exs`:

```elixir
defmodule GravitonMQBench.Stats do
  @moduledoc false
  # Pure helpers for scripts/bench/run.exs: no processes, clocks, files or sockets.
  # Tested by apps/graviton_mq/test/graviton_mq/bench_stats_test.exs.

  @doc """
  Exact nearest-rank summary of integer latency samples in microseconds.
  Returns nil when there are no samples.
  """
  def latency_summary([]), do: nil

  def latency_summary(samples) when is_list(samples) do
    sorted = samples |> Enum.sort() |> List.to_tuple()
    count = tuple_size(sorted)

    %{
      count: count,
      p50: nearest_rank(sorted, count, 500),
      p99: nearest_rank(sorted, count, 990),
      p999: nearest_rank(sorted, count, 999),
      max: elem(sorted, count - 1)
    }
  end

  # The smallest sample with at least per_mille/1000 of all samples at or
  # below it: rank = ceil(count * per_mille / 1000), in integer arithmetic.
  defp nearest_rank(sorted, count, per_mille),
    do: elem(sorted, max(div(count * per_mille + 999, 1000), 1) - 1)

  @doc """
  Accounts for consumed sequence numbers.

  `published` maps each publisher index to its publication count; that
  publisher's sequence numbers are 1..count. `seen` maps each consumed
  `{publisher, sequence}` key to the number of times it was consumed.
  """
  def accounting(published, seen) when is_map(published) and is_map(seen) do
    expected = published |> Map.values() |> Enum.sum()
    delivered = Enum.count(seen, fn {key, _times} -> published?(published, key) end)

    %{
      expected: expected,
      delivered: delivered,
      missing: expected - delivered,
      duplicates: Enum.sum(Map.values(seen)) - map_size(seen),
      unexpected: map_size(seen) - delivered
    }
  end

  defp published?(published, {publisher, sequence}) when is_integer(sequence),
    do: sequence >= 1 and sequence <= Map.get(published, publisher, 0)

  defp published?(_published, _key), do: false

  @doc "Duplicates are only reported; a missing or unexpected sequence fails the gate."
  def verdict(%{missing: 0, unexpected: 0}), do: :ok

  def verdict(%{missing: missing, unexpected: unexpected}),
    do: {:error, "missing #{missing}, unexpected #{unexpected}"}

  @doc "Events per second over a window in milliseconds, rounded to 0.1."
  def rate(count, window_ms)
      when is_integer(count) and count >= 0 and is_integer(window_ms) and window_ms > 0,
      do: Float.round(count * 1000 / window_ms, 1)
end
```

- [ ] **Step 4: Run the test and see it pass**

Run the Step 2 command again.
Expected: `10 tests, 0 failures` for `graviton_mq` and no warnings. Other umbrella apps report no matching tests.

- [ ] **Step 5: Format and commit**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix format scripts/bench/stats.exs apps/graviton_mq/test/graviton_mq/bench_stats_test.exs && git diff --stat -- scripts/bench/stats.exs apps/graviton_mq/test/graviton_mq/bench_stats_test.exs; git add -- scripts/bench/stats.exs apps/graviton_mq/test/graviton_mq/bench_stats_test.exs && git commit --only -m "test(bench): add S5a percentile and sequence accounting helpers" -- scripts/bench/stats.exs apps/graviton_mq/test/graviton_mq/bench_stats_test.exs
```
Expected: formatting changes nothing (the code above is already formatted) and one commit with two new files.

---

## Task 2: Mix-free broker VM

**Files:**
- Create: `scripts/bench/broker_vm.exs`

- [ ] **Step 1: Write the broker VM script**

Create `scripts/bench/broker_vm.exs`:

```elixir
# Broker VM for scripts/bench/run.exs (S5a). Runs without Mix:
#   elixir -pa <ebin>... scripts/bench/broker_vm.exs <mqtt|amqp> <memory|durable> <directory> <address>
# Prints "READY <os_pid> <mqtt_port> <amqp_port>" (0 = protocol not enabled),
# answers each "STATUS" stdin line with "STATUS <json>", and stops on stdin EOF.
[protocol, storage, directory, address] = System.argv()
true = protocol in ["mqtt", "amqp"] and storage in ["memory", "durable"]
{:ok, _} = Application.ensure_all_started(:graviton_mq_runtime)
:ok = Logger.configure(level: :warning)
listeners = [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]

mqtt =
  [
    auth: :anonymous,
    listeners: listeners,
    max_connections: 256,
    max_sessions: 256,
    max_inflight: 1_024,
    max_pending_deliveries: 262_144,
    max_pending_bytes: 268_435_456,
    max_session_bytes: 268_435_456
  ] ++
    if(storage == "durable",
      do: [persistence: [directory: Path.join(directory, "mqtt"), stream_id: "s5a-bench-mqtt"]],
      else: []
    )

amqp10 =
  [
    container_id: "s5a-bench",
    auth: :anonymous,
    queues: [address],
    listeners: listeners,
    max_connections: 256,
    max_auth_jobs: 128,
    queue_options: [max_messages: 1_000_000, max_bytes: 1_073_741_824]
  ] ++
    if(storage == "durable",
      do: [persistence: [directory: Path.join(directory, "amqp"), stream_id: "s5a-bench-amqp"]],
      else: []
    )

protocol_options = if protocol == "mqtt", do: [mqtt: mqtt], else: [amqp10: amqp10]

{:ok, instance} =
  GravitonMQ.start_link([name: nil, runtime_supervisor_name: nil] ++ protocol_options)

status = fn ->
  if protocol == "mqtt" do
    {:ok, status} = GravitonMQ.status(instance)

    Map.take(status, [:profile, :storage, :connections, :sessions, :pending_deliveries, :prepared])
  else
    {:ok, status} = GravitonMQ.amqp10_status(instance)

    queue =
      case Map.fetch!(status.queues, address) do
        %{} = queue -> Map.take(queue, [:messages, :ready, :inflight])
        state -> %{state: state}
      end

    %{profile: status.profile, connections: status.connections, queue: queue}
  end
end

port = fn
  "mqtt" ->
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    port

  "amqp" ->
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(instance)
    port
end

{mqtt_port, amqp_port} =
  if protocol == "mqtt", do: {port.("mqtt"), 0}, else: {0, port.("amqp")}

IO.puts("READY #{System.pid()} #{mqtt_port} #{amqp_port}")

Stream.repeatedly(fn -> IO.gets("") end)
|> Enum.reduce_while(:running, fn
  "STATUS\n", state ->
    IO.puts("STATUS " <> IO.iodata_to_binary(:json.encode(status.())))
    {:cont, state}

  line, state when is_binary(line) ->
    {:cont, state}

  _eof_or_error, state ->
    {:halt, state}
end)

:ok = Supervisor.stop(instance)
File.rm_rf!(directory)
```

- [ ] **Step 2: Run it once per protocol with STATUS then EOF**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && bash -c 'set -euo pipefail
args=()
for app in graviton_mq_core graviton_mq_amqp10 graviton_mq_mqtt graviton_mq_storage graviton_mq_runtime graviton_mq telemetry ex_ssl hell; do args+=(-pa "/tmp/gmq-build-s5a/dev/lib/$app/ebin"); done
for mode in "mqtt memory" "amqp durable"; do
  mkdir -p /tmp/gmq-s5a-tmp/vm-check
  printf "STATUS\n" | elixir "${args[@]}" scripts/bench/broker_vm.exs $mode /tmp/gmq-s5a-tmp/vm-check s5a-vm-check
  echo "exit=$?"
  test ! -e /tmp/gmq-s5a-tmp/vm-check && echo "data directory removed"
done'
```
Expected (pids and ports vary; JSON key order may differ):
```text
READY 2679859 41231 0
STATUS {"profile":"memory","storage":"ready","connections":0,"sessions":0,"pending_deliveries":0,"prepared":0}
exit=0
data directory removed
READY 2679902 0 38023
STATUS {"profile":"local_durable","queue":{"messages":0,"ready":0,"inflight":0},"connections":0}
exit=0
data directory removed
```

- [ ] **Step 3: Format and commit**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix format scripts/bench/broker_vm.exs && git add -- scripts/bench/broker_vm.exs && git commit --only -m "chore(bench): add Mix-free S5a broker VM" -- scripts/bench/broker_vm.exs
```
Expected: one commit with one new file.

---

## Task 3: Load generator

**Files:**
- Create: `scripts/bench/run.exs`

- [ ] **Step 1: Write the generator**

Create `scripts/bench/run.exs`:

```elixir
# S5a throughput and latency baseline, the load generator VM. From the umbrella root:
#   mix run --no-start scripts/bench/run.exs --profile full --out /tmp/s5a
# Spec: docs/superpowers/specs/2026-11-13-s5a-performance-baseline-design.md.
# Drives our own MQTT 5.0 and AMQP 1.0 clients against a broker in a second,
# Mix-free VM (scripts/bench/broker_vm.exs). The figures are not
# interoperability evidence, and no threshold is asserted.
Code.require_file("stats.exs", __DIR__)

defmodule GravitonMQBench.Run do
  alias GravitonMQ.AMQP10.{Outcome, Value}
  alias GravitonMQ.AMQP10.Codec.Message
  alias GravitonMQ.AMQP10Client, as: AMQP
  alias GravitonMQ.MQTTClient, as: MQTT
  alias GravitonMQBench.Stats

  @broker_script Path.join(__DIR__, "broker_vm.exs")
  @broker_apps [
    :graviton_mq_core,
    :graviton_mq_amqp10,
    :graviton_mq_mqtt,
    :graviton_mq_storage,
    :graviton_mq_runtime,
    :graviton_mq,
    :telemetry,
    :ex_ssl,
    :hell
  ]
  @profiles %{
    "full" => %{warmup_ms: 5_000, measure_ms: 20_000, ns: [1, 8, 64]},
    "smoke" => %{warmup_ms: 1_000, measure_ms: 3_000, ns: [1, 8]}
  }
  @scenarios [
    %{id: "mqtt_qos0_mem", protocol: :mqtt, storage: "memory", qos: 0},
    %{id: "mqtt_qos1_mem", protocol: :mqtt, storage: "memory", qos: 1},
    %{id: "mqtt_qos1_durable", protocol: :mqtt, storage: "durable", qos: 1},
    %{id: "amqp_mem", protocol: :amqp, storage: "memory", durable: false},
    %{id: "amqp_durable", protocol: :amqp, storage: "durable", durable: true}
  ]
  # <<publisher::16, sequence::32, sent_at_us::signed-64>> precedes the padding.
  @header_bytes 14
  @credit 64
  # Spec §8 fixes READY (20 s) and broker stop (10 s). The client bounds are
  # generous because durable acceptance waits for synchronization on a real disk.
  @ready_ms 20_000
  @stop_ms 10_000
  @client_timeout_ms 60_000
  @operation_ms 65_000
  @connect_all_ms 120_000
  @consume_idle_ms 60_000
  @drain_ms 60_000

  def main(arguments) do
    config = options(arguments)
    File.mkdir_p!(config.out)
    File.mkdir_p!(config.data_root)
    environment = environment(config)

    results =
      for scenario <- @scenarios, n <- config.profile.ns do
        IO.puts("running #{scenario.id} n=#{n}")
        scenario(config, scenario, n)
      end

    document = %{
      spec: "docs/superpowers/specs/2026-11-13-s5a-performance-baseline-design.md",
      profile: config.profile_name,
      environment: environment,
      results: results
    }

    path = Path.join(config.out, "results.json")
    File.write!(path, json(document) <> "\n")
    Enum.each(results, &IO.puts(summary_line(&1)))
    IO.puts("wrote #{path}")
    failed = Enum.count(results, &(&1.status == "failed"))
    IO.puts("#{length(results) - failed} passed, #{failed} failed")
    if failed > 0, do: System.halt(1)
  end

  defp options(arguments) do
    {options, rest, invalid} =
      OptionParser.parse(arguments,
        strict: [profile: :string, out: :string, payload_bytes: :integer, data_dir: :string]
      )

    if rest != [] or invalid != [],
      do: raise("unknown arguments: #{inspect({rest, invalid})}")

    name = Keyword.get(options, :profile, "full")
    profile = Map.get(@profiles, name) || raise("--profile must be full or smoke")
    out = Keyword.get(options, :out) || raise("--out <directory> is required")
    bytes = Keyword.get(options, :payload_bytes, 256)

    unless bytes in 16..16_384, do: raise("--payload-bytes must be in 16..16384")

    %{
      profile_name: name,
      profile: profile,
      out: Path.expand(out),
      payload_bytes: bytes,
      data_root: Path.expand(Keyword.get(options, :data_dir, System.tmp_dir!()))
    }
  end

  defp scenario(config, scenario, n) do
    unique = System.unique_integer([:positive])
    name = "#{scenario.id}-#{n}-#{unique}"
    directory = Path.join(config.data_root, "graviton-s5a-" <> name)
    File.mkdir_p!(directory)
    Process.put(:bench_workers, [])
    Process.put(:bench_log, [])
    Process.put(:bench_broker, nil)

    {broker, outcome} =
      try do
        case start_broker(scenario, directory, "s5a-" <> name) do
          {:ok, broker} -> {broker, measure(config, scenario, n, broker, "s5a-" <> name)}
          {:error, reason, broker} -> {broker, {:error, reason}}
        end
      rescue
        error -> {Process.get(:bench_broker), {:error, Exception.message(error)}}
      end

    stop_workers()
    stopped = stop_broker(broker)
    File.rm_rf!(directory)
    flush_mailbox()

    case {outcome, stopped} do
      {{:ok, measured}, :ok} -> Map.merge(base(config, scenario, n, "passed", nil), measured)
      {{:ok, _measured}, {:error, reason}} -> base(config, scenario, n, "failed", reason)
      {{:error, reason}, _} -> base(config, scenario, n, "failed", reason)
    end
  end

  defp base(config, scenario, n, status, reason) do
    %{
      id: scenario.id,
      n: n,
      payload_bytes: config.payload_bytes,
      status: status,
      reason: reason,
      throughput: nil,
      consume_rate: nil,
      accept_latency_us: nil,
      e2e_latency_us: nil,
      duplicates: nil
    }
  end

  defp start_broker(scenario, directory, address) do
    paths =
      Enum.flat_map(@broker_apps, fn app ->
        ["-pa", app |> :code.lib_dir() |> List.to_string() |> Path.join("ebin")]
      end)

    port =
      Port.open({:spawn_executable, System.find_executable("elixir")}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        {:line, 65_536},
        args:
          paths ++
            [
              @broker_script,
              Atom.to_string(scenario.protocol),
              scenario.storage,
              directory,
              address
            ]
      ])

    {:os_pid, wrapper} = Port.info(port, :os_pid)
    broker = %{port: port, os_pids: [wrapper], mqtt_port: 0, amqp_port: 0}
    Process.put(:bench_broker, broker)
    deadline = now_ms() + @ready_ms

    case await_ready(port, deadline) do
      {:ok, os_pid, mqtt_port, amqp_port} ->
        broker = %{
          broker
          | os_pids: Enum.uniq([os_pid, wrapper]),
            mqtt_port: mqtt_port,
            amqp_port: amqp_port
        }

        Process.put(:bench_broker, broker)
        {:ok, broker}

      {:error, reason} ->
        {:error, reason, broker}
    end
  end

  defp await_ready(port, deadline) do
    receive do
      {^port, {:data, {:eol, "READY " <> fields}}} ->
        [os_pid, mqtt_port, amqp_port] =
          fields |> String.split(" ") |> Enum.map(&String.to_integer/1)

        {:ok, os_pid, mqtt_port, amqp_port}

      {^port, {:data, {_, line}}} ->
        log(line)
        await_ready(port, deadline)

      {^port, {:exit_status, status}} ->
        {:error, "broker VM exited with status #{status} before READY: #{log_tail()}"}
    after
      max(deadline - now_ms(), 0) -> {:error, "broker VM printed no READY within 20 s"}
    end
  end

  defp measure(config, scenario, n, broker, address) do
    parent = self()
    window = %{warmup_ms: config.profile.warmup_ms, measure_ms: config.profile.measure_ms}

    subscriber =
      spawn_worker(:subscriber, fn ->
        subscriber(parent, scenario, broker, address, config.payload_bytes)
      end)

    with :ok <- await_ready_workers(broker, [subscriber], now_ms() + @connect_all_ms),
         publishers =
           for(
             index <- 0..(n - 1),
             do:
               spawn_worker({:publisher, index}, fn ->
                 publisher(parent, scenario, broker, address, index, config.payload_bytes)
               end)
           ),
         :ok <- await_ready_workers(broker, publishers, now_ms() + @connect_all_ms) do
      start = now_us()
      window_start = start + window.warmup_ms * 1000
      window_end = window_start + window.measure_ms * 1000
      send(subscriber, {:go, window_start, window_end, List.to_tuple(publishers)})
      for pid <- publishers, do: send(pid, {:go, window_start, window_end})

      deadline = div(window_end, 1000) + @operation_ms + 5_000

      with {:ok, published} <-
             collect_published(broker, Map.new(publishers, &{&1, nil}), deadline),
           counts = Map.new(published, fn {_pid, result} -> {result.index, result.count} end),
           expected = counts |> Map.values() |> Enum.sum(),
           _ = send(subscriber, {:finish, expected}),
           {:ok, consumed} <- await_consumed(broker, subscriber),
           accounting = Stats.accounting(counts, consumed.seen),
           :ok <- Stats.verdict(accounting),
           :ok <- await_drained(broker, scenario, now_ms() + @drain_ms) do
        measured = published |> Map.values() |> Enum.map(& &1.measured) |> Enum.sum()
        accept = published |> Map.values() |> Enum.flat_map(& &1.samples)

        throughput =
          if Map.get(scenario, :qos) == 0,
            do: Stats.rate(length(consumed.e2e), window.measure_ms),
            else: Stats.rate(measured, window.measure_ms)

        {:ok,
         %{
           throughput: throughput,
           consume_rate: Stats.rate(consumed.in_window, window.measure_ms),
           accept_latency_us: Stats.latency_summary(accept),
           e2e_latency_us: Stats.latency_summary(consumed.e2e),
           duplicates: accounting.duplicates,
           published: expected,
           delivered: accounting.delivered
         }}
      end
    end
  end

  defp spawn_worker(role, fun) do
    parent = self()

    {pid, _monitor} =
      spawn_monitor(fn ->
        try do
          fun.()
        rescue
          error ->
            send(parent, {:failed, self(), role, Exception.format(:error, error, __STACKTRACE__)})
        catch
          kind, reason -> send(parent, {:failed, self(), role, inspect({kind, reason})})
        end
      end)

    Process.put(:bench_workers, [pid | Process.get(:bench_workers)])
    pid
  end

  defp await_ready_workers(_broker, [], _deadline), do: :ok

  defp await_ready_workers(broker, pending, deadline) do
    port = broker.port

    receive do
      {:ready, pid} ->
        await_ready_workers(broker, List.delete(pending, pid), deadline)

      {:failed, _pid, role, reason} ->
        {:error, "#{inspect(role)} failed: #{reason}"}

      {:DOWN, _, :process, _, reason} when reason != :normal ->
        {:error, "worker exited: #{inspect(reason)}"}

      {^port, {:exit_status, status}} ->
        {:error, "broker VM exited with status #{status}"}

      {^port, {:data, {_, line}}} ->
        log(line)
        await_ready_workers(broker, pending, deadline)
    after
      max(deadline - now_ms(), 0) -> {:error, "clients did not connect within 120 s"}
    end
  end

  defp collect_published(broker, pending, deadline) do
    port = broker.port

    if Enum.all?(pending, fn {_pid, result} -> result != nil end) do
      {:ok, pending}
    else
      receive do
        {:published, pid, result} ->
          collect_published(broker, Map.put(pending, pid, result), deadline)

        {:failed, _pid, role, reason} ->
          {:error, "#{inspect(role)} failed: #{reason}"}

        {:DOWN, _, :process, _, reason} when reason != :normal ->
          {:error, "worker exited: #{inspect(reason)}"}

        {^port, {:exit_status, status}} ->
          {:error, "broker VM exited with status #{status}: #{log_tail()}"}

        {^port, {:data, {_, line}}} ->
          log(line)
          collect_published(broker, pending, deadline)
      after
        max(deadline - now_ms(), 0) -> {:error, "publishers did not finish"}
      end
    end
  end

  defp await_consumed(broker, subscriber) do
    port = broker.port

    receive do
      {:consumed, ^subscriber, consumed} ->
        {:ok, consumed}

      {:failed, _pid, role, reason} ->
        {:error, "#{inspect(role)} failed: #{reason}"}

      {:DOWN, _, :process, _, reason} when reason != :normal ->
        {:error, "worker exited: #{inspect(reason)}"}

      {^port, {:exit_status, status}} ->
        {:error, "broker VM exited with status #{status}: #{log_tail()}"}

      {^port, {:data, {_, line}}} ->
        log(line)
        await_consumed(broker, subscriber)
    end
  end

  defp await_drained(broker, scenario, deadline) do
    expected_profile = if scenario.storage == "durable", do: "local_durable", else: "memory"

    case broker_status(broker) do
      {:ok, %{"profile" => profile}} when profile != expected_profile ->
        {:error, "broker profile #{profile}, expected #{expected_profile}"}

      {:ok, status} ->
        cond do
          drained?(scenario, status) ->
            :ok

          now_ms() >= deadline ->
            {:error, "not drained: #{inspect(status)}"}

          true ->
            Process.sleep(50)
            await_drained(broker, scenario, deadline)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp drained?(%{protocol: :mqtt}, status),
    do:
      status["storage"] == "ready" and status["pending_deliveries"] == 0 and
        status["prepared"] == 0

  defp drained?(%{protocol: :amqp}, status),
    do: match?(%{"messages" => 0, "inflight" => 0}, status["queue"])

  defp broker_status(broker) do
    port = broker.port
    true = Port.command(port, "STATUS\n")
    await_status(port, now_ms() + @operation_ms)
  end

  defp await_status(port, deadline) do
    receive do
      {^port, {:data, {:eol, "STATUS " <> json}}} ->
        {:ok, :json.decode(json)}

      {^port, {:data, {_, line}}} ->
        log(line)
        await_status(port, deadline)

      {^port, {:exit_status, status}} ->
        {:error, "broker VM exited with status #{status}: #{log_tail()}"}
    after
      max(deadline - now_ms(), 0) -> {:error, "broker VM did not answer STATUS"}
    end
  end

  # Publisher: one connection, a window of one outstanding publication.
  defp publisher(parent, %{protocol: :mqtt} = scenario, broker, address, index, bytes) do
    {:ok, client} =
      MQTT.start_link(
        host: "127.0.0.1",
        port: broker.mqtt_port,
        client_id: "#{address}-publisher-#{index}",
        observer: self(),
        session_expiry_interval: 0,
        handshake_timeout: @client_timeout_ms,
        operation_timeout: @client_timeout_ms
      )

    connected(client)
    expected = if scenario.qos == 0, do: :sent, else: :puback

    publish = fn payload, sequence ->
      {:ok, ^expected} =
        operation(client, MQTT.publish(client, address, payload, qos: scenario.qos))

      if scenario.qos == 0 do
        receive do
          {:bench_delivered, ^sequence} -> :ok
        after
          @operation_ms -> raise("QoS 0 publication #{sequence} was not delivered within 65 s")
        end
      end
    end

    run_publisher(parent, index, bytes, scenario.qos != 0, publish)
    :ok = MQTT.disconnect(client)
    stop_client(client)
  end

  defp publisher(parent, %{protocol: :amqp} = scenario, broker, address, index, bytes) do
    client = amqp_connect(broker, "#{address}-publisher-#{index}", [])
    name = "publisher-#{index}"

    {:ok, sender} =
      operation(client, AMQP.attach(client, 1, name, :sender, address: Value.string(address)))

    header = if scenario.durable, do: [{:header, Value.list([Value.boolean(true)])}], else: []

    publish = fn payload, _sequence ->
      {:ok, raw} = Message.encode_sections(header ++ [{:data, Value.binary(payload)}])
      {:ok, %Outcome.Accepted{}} = operation(client, AMQP.send_message(client, 1, sender, raw))
    end

    run_publisher(parent, index, bytes, true, publish)
    {:ok, :closed} = operation(client, AMQP.close(client))
    stop_client(client)
  end

  defp run_publisher(parent, index, bytes, record_accept?, publish) do
    send(parent, {:ready, self()})

    receive do
      {:go, window_start, window_end} ->
        state = %{
          index: index,
          padding: :binary.copy(<<0>>, bytes - @header_bytes),
          publish: publish,
          record_accept?: record_accept?,
          window_start: window_start,
          window_end: window_end,
          count: 0,
          measured: 0,
          samples: []
        }

        result = publish_loop(state)

        send(
          parent,
          {:published, self(), Map.take(result, [:index, :count, :measured, :samples])}
        )
    after
      @connect_all_ms + @ready_ms -> raise("publisher start gate timeout")
    end
  end

  defp publish_loop(state) do
    started = now_us()

    if started >= state.window_end do
      state
    else
      sequence = state.count + 1
      payload = <<state.index::16, sequence::32, started::signed-64, state.padding::binary>>
      state.publish.(payload, sequence)
      finished = now_us()
      measured? = started >= state.window_start

      publish_loop(%{
        state
        | count: sequence,
          measured: state.measured + if(measured?, do: 1, else: 0),
          samples:
            if(measured? and state.record_accept?,
              do: [finished - started | state.samples],
              else: state.samples
            )
      })
    end
  end

  # Subscriber/receiver: one connection, consumes everything.
  defp subscriber(parent, %{protocol: :mqtt} = scenario, broker, address, _bytes) do
    {:ok, client} =
      MQTT.start_link(
        host: "127.0.0.1",
        port: broker.mqtt_port,
        client_id: "#{address}-subscriber",
        observer: self(),
        session_expiry_interval: 0,
        handshake_timeout: @client_timeout_ms,
        operation_timeout: @client_timeout_ms,
        max_deliveries: 1_024,
        receive_maximum: 1_024
      )

    connected(client)
    qos = scenario.qos
    {:ok, [^qos]} = operation(client, MQTT.subscribe(client, address, qos: qos))
    :ok = MQTT.request(client, 1_024)

    take = fn freed, state ->
      receive do
        {:graviton_mq, ^client, {:delivery, delivery, packet}} ->
          :ok = MQTT.ack(client, delivery)
          state = received(state, packet.payload)

          if freed + 1 >= 256 do
            :ok = MQTT.request(client, freed + 1)
            {0, state}
          else
            {freed + 1, state}
          end

        {:graviton_mq, ^client, {:disconnected, reason}} ->
          raise("subscriber disconnected: #{inspect(reason)}")
      after
        100 -> {freed, state}
      end
    end

    run_subscriber(parent, qos == 0, take)
    :ok = MQTT.disconnect(client)
    stop_client(client)
  end

  defp subscriber(parent, %{protocol: :amqp}, broker, address, bytes) do
    link_bytes = bytes + 1_024
    client = amqp_connect(broker, "#{address}-receiver", max_operations: 512)

    {:ok, receiver} =
      operation(
        client,
        AMQP.attach(client, 1, "receiver", :receiver,
          address: Value.string(address),
          max_message_bytes: link_bytes
        )
      )

    {:ok, @credit} =
      operation(client, AMQP.request(client, 1, receiver, @credit, @credit * link_bytes))

    take = fn freed, state ->
      receive do
        {:graviton_mq, ^client, {:delivery, delivery, 1, ^receiver, raw, %{message_format: 0}}} ->
          {:ok, _settle} = AMQP.settle(client, delivery, %Outcome.Accepted{})
          {freed, received(state, binary_part(raw, byte_size(raw) - bytes, bytes))}

        {:graviton_mq, ^client, {:operation, _settle, {:ok, %Outcome.Accepted{}}}} ->
          if freed + 1 >= 16 do
            {:ok, _request} =
              AMQP.request(client, 1, receiver, freed + 1, (freed + 1) * link_bytes)

            {0, state}
          else
            {freed + 1, state}
          end

        {:graviton_mq, ^client, {:operation, _request, {:ok, granted}}}
        when is_integer(granted) ->
          {freed, state}

        {:graviton_mq, ^client, {:operation, _reference, result}} ->
          raise("receiver operation failed: #{inspect(result)}")

        {:graviton_mq, ^client, {:disconnected, reason}} ->
          raise("receiver disconnected: #{inspect(reason)}")
      after
        100 -> {freed, state}
      end
    end

    run_subscriber(parent, false, take)
    {:ok, :closed} = operation(client, AMQP.close(client))
    stop_client(client)
  end

  defp run_subscriber(parent, notify?, take) do
    send(parent, {:ready, self()})

    receive do
      {:go, window_start, window_end, publishers} ->
        state = %{
          notify?: notify?,
          publishers: publishers,
          window_start: window_start,
          window_end: window_end,
          seen: %{},
          e2e: [],
          in_window: 0,
          expected: nil,
          quiet_since: nil
        }

        state = consume(take, 0, state)
        send(parent, {:consumed, self(), Map.take(state, [:seen, :e2e, :in_window])})

        receive do
          :stop -> :ok
        after
          @drain_ms + @operation_ms -> :ok
        end
    after
      @connect_all_ms + @ready_ms -> raise("subscriber start gate timeout")
    end
  end

  # After {:finish, expected}, consumption ends when every expected key was
  # seen, or when nothing arrived for @consume_idle_ms (the gate then fails).
  defp consume(take, freed, state) do
    state =
      receive do
        {:finish, expected} -> %{state | expected: expected, quiet_since: now_us()}
      after
        0 -> state
      end

    cond do
      state.expected != nil and map_size(state.seen) >= state.expected ->
        state

      state.expected != nil and now_us() - state.quiet_since >= @consume_idle_ms * 1000 ->
        state

      true ->
        {freed, state} = take.(freed, state)
        consume(take, freed, state)
    end
  end

  defp received(state, <<publisher::16, sequence::32, sent_at::signed-64, _::binary>>) do
    at = now_us()
    key = {publisher, sequence}
    first? = not Map.has_key?(state.seen, key)

    if state.notify? and publisher < tuple_size(state.publishers),
      do: send(elem(state.publishers, publisher), {:bench_delivered, sequence})

    %{
      state
      | quiet_since: if(state.quiet_since, do: at, else: nil),
        seen: Map.update(state.seen, key, 1, &(&1 + 1)),
        e2e:
          if(first? and sent_at >= state.window_start and sent_at < state.window_end,
            do: [at - sent_at | state.e2e],
            else: state.e2e
          ),
        in_window:
          state.in_window + if(at >= state.window_start and at < state.window_end, do: 1, else: 0)
    }
  end

  defp received(_state, payload),
    do: raise("malformed benchmark payload of #{byte_size(payload)} bytes")

  defp amqp_connect(broker, container_id, options) do
    {:ok, client} =
      AMQP.start_link(
        [
          host: {127, 0, 0, 1},
          port: broker.amqp_port,
          container_id: container_id,
          observer: self(),
          operation_timeout_ms: @client_timeout_ms
        ] ++
          options
      )

    connected(client)
    {:ok, 1} = operation(client, AMQP.open_session(client, 1))
    client
  end

  defp connected(client) do
    receive do
      {:graviton_mq, ^client, {:connected, _peer}} ->
        :ok

      {:graviton_mq, ^client, {:disconnected, reason}} ->
        raise("connect failed: #{inspect(reason)}")
    after
      @operation_ms -> raise("client connection timeout")
    end
  end

  defp operation(client, {:ok, reference}) do
    receive do
      {:graviton_mq, ^client, {:operation, ^reference, result}} ->
        result

      {:graviton_mq, ^client, {:disconnected, reason}} ->
        raise("operation interrupted: #{inspect(reason)}")
    after
      @operation_ms -> raise("client operation timeout")
    end
  end

  defp operation(_client, admission), do: raise("command admission failed: #{inspect(admission)}")

  defp stop_client(client) do
    if Process.alive?(client), do: GenServer.stop(client, :normal, 5_000)
  end

  defp stop_workers do
    workers = Process.get(:bench_workers, [])
    for pid <- workers, do: send(pid, :stop)
    deadline = now_ms() + 8_000
    Enum.each(workers, &wait_dead(&1, deadline))
    for pid <- workers, Process.alive?(pid), do: Process.exit(pid, :kill)
  end

  defp wait_dead(pid, deadline) do
    if Process.alive?(pid) and now_ms() < deadline do
      Process.sleep(20)
      wait_dead(pid, deadline)
    end
  end

  # Closes the broker VM's stdin (EOF), waits up to 10 s, then kills it.
  defp stop_broker(nil), do: :ok

  defp stop_broker(broker) do
    try do
      Port.close(broker.port)
    rescue
      ArgumentError -> :already_closed
    end

    deadline = now_ms() + @stop_ms
    Enum.each(broker.os_pids, &wait_exited(&1, deadline))
    survivors = Enum.filter(broker.os_pids, &os_alive?/1)
    for pid <- survivors, do: System.cmd("kill", ["-KILL", Integer.to_string(pid)])

    if survivors == [],
      do: :ok,
      else: {:error, "broker VM did not stop within 10 s and was killed"}
  end

  defp wait_exited(os_pid, deadline) do
    if os_alive?(os_pid) and now_ms() < deadline do
      Process.sleep(20)
      wait_exited(os_pid, deadline)
    end
  end

  defp os_alive?(os_pid) do
    case File.read("/proc/#{os_pid}/stat") do
      {:ok, stat} -> not String.contains?(stat, ") Z ")
      {:error, _} -> false
    end
  end

  defp flush_mailbox do
    receive do
      _message -> flush_mailbox()
    after
      0 -> :ok
    end
  end

  defp log(line), do: Process.put(:bench_log, Enum.take([line | Process.get(:bench_log, [])], 20))
  defp log_tail, do: Process.get(:bench_log, []) |> Enum.reverse() |> Enum.join(" | ")

  defp environment(config) do
    %{
      commit: command("git", ["rev-parse", "HEAD"]),
      dirty: command("git", ["status", "--porcelain"]) != "",
      cpu_model: cpu_model(),
      logical_cores: :erlang.system_info(:logical_processors),
      memory_bytes: memory_bytes(),
      otp: otp_version(),
      elixir: System.version(),
      schedulers_online: System.schedulers_online(),
      data_directory: config.data_root,
      filesystem: filesystem(config.data_root),
      block_device: command("findmnt", ["-n", "-o", "SOURCE", "-T", config.data_root]),
      warmup_ms: config.profile.warmup_ms,
      measure_ms: config.profile.measure_ms,
      payload_bytes: config.payload_bytes
    }
  end

  defp command(executable, arguments) do
    case System.find_executable(executable) do
      nil ->
        "unavailable"

      path ->
        case System.cmd(path, arguments, stderr_to_stdout: true) do
          {output, 0} -> String.trim(output)
          {output, status} -> "unavailable (#{status}: #{String.trim(output)})"
        end
    end
  end

  defp cpu_model do
    with {:ok, info} <- File.read("/proc/cpuinfo"),
         [_, model] <- Regex.run(~r/^model name\s*:\s*(.+)$/m, info) do
      String.trim(model)
    else
      _ -> "unavailable"
    end
  end

  defp memory_bytes do
    with {:ok, info} <- File.read("/proc/meminfo"),
         [_, kib] <- Regex.run(~r/^MemTotal:\s+(\d+) kB$/m, info) do
      String.to_integer(kib) * 1024
    else
      _ -> nil
    end
  end

  defp otp_version do
    path = Path.join([:code.root_dir(), "releases", System.otp_release(), "OTP_VERSION"])

    case File.read(path) do
      {:ok, version} -> String.trim(version)
      {:error, _} -> System.otp_release()
    end
  end

  defp filesystem(directory) do
    case command("df", ["-T", directory]) |> String.split("\n") do
      [_header, row | _] -> row |> String.split() |> Enum.at(1, "unavailable")
      _ -> "unavailable"
    end
  end

  defp summary_line(%{status: "failed"} = result),
    do: "FAIL #{result.id} n=#{result.n}: #{result.reason}"

  defp summary_line(result) do
    "PASS #{result.id} n=#{result.n}: #{result.throughput} msg/s, " <>
      "consume #{result.consume_rate} msg/s, " <>
      "accept p50/p99/p999/max #{latency(result.accept_latency_us)} us, " <>
      "e2e #{latency(result.e2e_latency_us)} us, duplicates #{result.duplicates}"
  end

  defp latency(nil), do: "-"
  defp latency(summary), do: "#{summary.p50}/#{summary.p99}/#{summary.p999}/#{summary.max}"

  defp now_ms, do: System.monotonic_time(:millisecond)
  defp now_us, do: System.monotonic_time(:microsecond)
  defp json(value), do: value |> portable() |> :json.encode() |> IO.iodata_to_binary()

  defp portable(value) when is_map(value),
    do: Map.new(value, fn {key, child} -> {to_string(key), portable(child)} end)

  defp portable(value) when is_list(value), do: Enum.map(value, &portable/1)
  defp portable(nil), do: :null
  defp portable(value) when is_boolean(value), do: value
  defp portable(value) when is_atom(value), do: Atom.to_string(value)
  defp portable(value), do: value
end

GravitonMQBench.Run.main(System.argv())
```

- [ ] **Step 2: Check compilation and option validation**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix run --no-start scripts/bench/run.exs --profile bogus; echo "exit=$?"
```
Expected: no `warning:` line, then `** (RuntimeError) --profile must be full or smoke` and `exit=1`.

- [ ] **Step 3: Run the smoke profile end to end**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix run --no-start scripts/bench/run.exs --profile smoke --data-dir /tmp/gmq-s5a-tmp/data --out /tmp/gmq-s5a-tmp/smoke; echo "exit=$?"
```
Expected after about 1–2 minutes: ten `running …` lines, ten `PASS <id> n=<1|8>: …` lines (N ∈ {1, 8} for each of the five scenarios), `wrote /tmp/gmq-s5a-tmp/smoke/results.json`, `10 passed, 0 failed`, `exit=0`. Then confirm the cleanup:
```bash
ls /tmp/gmq-s5a-tmp/data; pgrep -af 'broker_vm[.]exs' || echo "no broker VM left"
```
Expected: an empty listing and `no broker VM left`.

If only `mqtt_qos1_durable` or `amqp_durable` fails, with a timeout, `{:connack, 136}` or `{:unknown, :closed}` reason, measure the disk:
```bash
dd if=/dev/zero of=/tmp/gmq-s5a-tmp/sync-probe bs=4k count=200 oflag=dsync 2>&1 | tail -1; rm -f /tmp/gmq-s5a-tmp/sync-probe
```
More than 2 s for 200 writes (10 ms per write) is host disk contention (see Prototype observations). Record it in the execution log and rerun Step 3 once the probe is below 2 s. Any other failure, or a durable failure with a fast disk, is a defect: diagnose it with superpowers:systematic-debugging. Fix the script if the defect is in the script; stop and report if it is in product code (Execution rule 4).

- [ ] **Step 4: Check failure handling (spec §8) by killing a broker VM**

Run (about 2 minutes):
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && bash -c 'log=/tmp/gmq-s5a-tmp/kill-check.log; rm -f "$log"
(env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix run --no-start scripts/bench/run.exs --profile smoke --data-dir /tmp/gmq-s5a-tmp/data --out /tmp/gmq-s5a-tmp/kill-check > "$log" 2>&1; echo "exit=$?" >> "$log") &
until grep -q "running mqtt_qos1_mem n=1" "$log" 2>/dev/null; do python3 -c "import time; time.sleep(0.5)"; done
python3 -c "import time; time.sleep(3)"
pkill -KILL -f "broker_vm.exs mqtt memory"
wait
grep -E "^(PASS|FAIL)|passed|exit=" "$log" | cut -c1-120'
```
Expected: `FAIL mqtt_qos1_mem n=1: broker VM exited with status 137…` (or a publisher/subscriber failure such as `operation interrupted`, if a client noticed the loss first). Every later scenario still runs and the run ends with `9 passed, 1 failed` and `exit=1`. Durable host-contention failures (Step 3) may add to the failed count. Then confirm `ls /tmp/gmq-s5a-tmp/data` is empty and `pgrep -af 'broker_vm[.]exs'` prints nothing.

- [ ] **Step 5: Format and commit**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix format scripts/bench/run.exs && env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix format --check-formatted scripts/bench/run.exs scripts/bench/broker_vm.exs scripts/bench/stats.exs && git add -- scripts/bench/run.exs && git commit --only -m "chore(bench): add S5a two-VM load generator" -- scripts/bench/run.exs
```
Expected: the format check exits 0 and one commit with one new file.

---

## Task 4: Full baseline on this host and qualification record

**Files:**
- Create: `docs/qualification/2026-11-13-s5a-baseline.md`
- Create: `docs/qualification/2026-11-13-s5a-baseline.json`

- [ ] **Step 1: Record the pre-run disk probe and load**

Run:
```bash
mkdir -p /tmp/gmq-s5a-tmp/data && dd if=/dev/zero of=/tmp/gmq-s5a-tmp/data/sync-probe bs=4k count=200 oflag=dsync 2>&1 | tail -1; rm -f /tmp/gmq-s5a-tmp/data/sync-probe; cat /proc/loadavg
```
Expected: a line such as `819200 bytes (819 kB, 800 KiB) copied, 0.08 s, 10.2 MB/s` and a load line. Per-write milliseconds = seconds × 1000 / 200, rounded to two decimals (0.08 s → 0.40). Write the value and the first three load fields into the execution log. If the per-write value exceeds 10 ms, do not start the run; record the measurement and wait for a quieter disk. If it stays above 10 ms for 30 minutes, report to the coordinator and stop.

- [ ] **Step 2: Run the full profile**

Run it in the background (expected 10–30 minutes; N = 64 drains slowly):
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix run --no-start scripts/bench/run.exs --profile full --data-dir /tmp/gmq-s5a-tmp/data --out /tmp/gmq-s5a-tmp/full > /tmp/gmq-s5a-tmp/full.log 2>&1; echo "exit=$?" >> /tmp/gmq-s5a-tmp/full.log
```
Expected at the end of `/tmp/gmq-s5a-tmp/full.log`: fifteen `PASS`/`FAIL` lines (five scenarios × N ∈ {1, 8, 64}), `wrote /tmp/gmq-s5a-tmp/full/results.json`, a `<p> passed, <f> failed` line, and `exit=0` when f = 0 or `exit=1` otherwise. Failures at N = 64 in the durable scenarios are expected on this host (Prototype observations). For each `FAIL`, decide from its reason whether the defect is in the script or in the broker. A script defect is fixed in `scripts/bench/run.exs` with its own commit (`fix(bench): …`) and the run is repeated from Step 1. A broker behaviour (timeouts, `{:unknown, :closed}`, CONNACK refusal, a non-draining broker) stays in the record as `failed`, with no figures.

- [ ] **Step 3: Record the post-run disk probe and load**

Run the Step 1 command again and log its per-write value and load. If either probe exceeded 10 ms per write, stop before Step 4 and report both values to the coordinator, who decides whether to rerun.

- [ ] **Step 4: Copy the raw results**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && cp /tmp/gmq-s5a-tmp/full/results.json docs/qualification/2026-11-13-s5a-baseline.json && grep -o '"profile":"full"' docs/qualification/2026-11-13-s5a-baseline.json
```
Expected: `"profile":"full"`.

- [ ] **Step 5: Render the record**

Create the throwaway renderer `/tmp/gmq-s5a-tmp/render_s5a.exs` (it is not committed):

````elixir
# Throwaway renderer for docs/qualification/2026-11-13-s5a-baseline.md (S5a plan, Task 4).
[json_path, probe_before, probe_after, load_before, load_after, out] = System.argv()
document = json_path |> File.read!() |> :json.decode()
env = document["environment"]
gib = fn bytes -> :erlang.float_to_binary(bytes / 1_073_741_824, decimals: 1) end

latency = fn
  :null -> "-"
  s -> "#{s["p50"]} / #{s["p99"]} / #{s["p999"]} / #{s["max"]}"
end

value = fn
  :null -> "-"
  v -> to_string(v)
end

rows =
  for r <- document["results"] do
    "| `#{r["id"]}` | #{r["n"]} | #{r["status"]} | #{value.(r["throughput"])} | " <>
      "#{value.(r["consume_rate"])} | #{latency.(r["accept_latency_us"])} | " <>
      "#{latency.(r["e2e_latency_us"])} | #{value.(r["duplicates"])} |"
  end

failures =
  for %{"status" => "failed"} = r <- document["results"] do
    first = r["reason"] |> String.split("\n") |> hd() |> String.trim()
    "- `#{r["id"]}` n=#{r["n"]}: #{first}"
  end

failures = if failures == [], do: ["None."], else: failures

text = """
# S5a performance baseline (2026-11-13)

Recorded with `scripts/bench/run.exs --profile full` as specified in
[the S5a design](../superpowers/specs/2026-11-13-s5a-performance-baseline-design.md).
The generator drives our own MQTT 5.0 and AMQP 1.0 clients against a broker in a
second, Mix-free VM over loopback TCP. These figures are a local measurement on
the owner's development host. They are **not** interoperability evidence, and no
threshold is asserted. They are only comparable with runs of the same profile on
the same host and the same filesystem; S5b reruns this profile there and records
before and after.

Raw results: [2026-11-13-s5a-baseline.json](2026-11-13-s5a-baseline.json).

## Environment

| Field | Value |
| --- | --- |
| Commit | `#{env["commit"]}` (dirty tree: #{env["dirty"]}) |
| CPU | #{env["cpu_model"]} |
| Logical cores | #{env["logical_cores"]} |
| Memory | #{gib.(env["memory_bytes"])} GiB |
| OTP / Elixir | #{env["otp"]} / #{env["elixir"]} |
| Schedulers online (generator VM; the broker VM uses the same default) | #{env["schedulers_online"]} |
| Data directory | `#{env["data_directory"]}` |
| Filesystem / block device | #{env["filesystem"]} on `#{env["block_device"]}` |
| Profile | #{document["profile"]}: #{env["warmup_ms"]} ms warm-up + #{env["measure_ms"]} ms measurement, payload #{env["payload_bytes"]} B |
| Synchronous 4 KiB write probe, before / after (`dd oflag=dsync`, 200 writes) | #{probe_before} / #{probe_after} ms per write |
| Load average (1/5/15 min), before / after | #{load_before} / #{load_after} |

## Results

Throughput is accepted publications per second in the measurement window; for
`mqtt_qos0_mem` it is delivered messages per second. "Consume" is the
subscriber's receipts per second in the same window. Acceptance latency runs from
the publish call to PUBACK or `Accepted`; end-to-end latency from the publish call
to receipt by the subscriber. Both use `System.monotonic_time/1` in the generator
VM and are exact nearest-rank percentiles over every sample of the window. Each
publisher keeps one publication outstanding, so N is the offered concurrency. A
failed scenario has no figures.

| Scenario | N | Status | Throughput (msg/s) | Consume (msg/s) | Accept p50 / p99 / p999 / max (µs) | End-to-end p50 / p99 / p999 / max (µs) | Duplicates |
| --- | --- | --- | --- | --- | --- | --- | --- |
#{Enum.join(rows, "\n")}

## Failed scenarios

#{Enum.join(failures, "\n")}

## Reproduce

```bash
mix run --no-start scripts/bench/run.exs --profile full --data-dir #{env["data_directory"]} --out <directory>
```

## CI

The `checks` job runs the `smoke` profile (1 s + 3 s, N in {1, 8}, all
scenarios) and only checks that the tool runs and the correctness gate passes.
Its figures come from the runner's temporary disk and are not part of this
baseline. CI run: not yet recorded.
"""

File.write!(out, text)
IO.puts("wrote #{out}")
````

Run it, passing the two probe values (ms per write) and the two load triples from Steps 1 and 3:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && elixir /tmp/gmq-s5a-tmp/render_s5a.exs docs/qualification/2026-11-13-s5a-baseline.json <probe-before-ms> <probe-after-ms> "<load-before>" "<load-after>" docs/qualification/2026-11-13-s5a-baseline.md
```
The angle-bracket arguments are the values logged in Steps 1 and 3, for example `0.40 0.52 "11.20 10.80 10.10" "12.00 11.10 10.30"`.
Expected: `wrote docs/qualification/2026-11-13-s5a-baseline.md`. Open the file and check that the environment table has the 40-hex commit with `dirty tree: false`, the CPU `Intel(R) Xeon(R) CPU E5-2650 v4 @ 2.20GHz`, 48 logical cores, `ext4` on `/dev/mapper/root_vg-root`, and `full: 5000 ms warm-up + 20000 ms measurement, payload 256 B`. The results table must have fifteen rows. "Failed scenarios" lists every `failed` row with the first line of its reason, or `None.`. If `dirty tree` is `true`, the run measured uncommitted code: discard both files, commit or remove the change, and repeat from Step 1.

- [ ] **Step 6: Commit**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && git add -- docs/qualification/2026-11-13-s5a-baseline.md docs/qualification/2026-11-13-s5a-baseline.json && git commit --only -m "docs: record S5a full-profile performance baseline" -- docs/qualification/2026-11-13-s5a-baseline.md docs/qualification/2026-11-13-s5a-baseline.json
```
Expected: one commit with two new files.

---

## Task 5: CI smoke step

**Files:**
- Modify: `.github/workflows/ci.yml` (job `checks`, after the step `Check source cycles`)

- [ ] **Step 1: Add the step**

In `.github/workflows/ci.yml`, replace:
```yaml
      - name: Check source cycles
        run: mix xref graph --format cycles --fail-above 0

  interoperability:
```
with:
```yaml
      - name: Check source cycles
        run: mix xref graph --format cycles --fail-above 0

      - name: Performance baseline smoke (correctness gate only, no thresholds)
        run: mix run --no-start scripts/bench/run.exs --profile smoke --data-dir "$RUNNER_TEMP/s5a-data" --out "$RUNNER_TEMP/s5a-smoke"

  interoperability:
```

- [ ] **Step 2: Lint the workflow**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && actionlint .github/workflows/ci.yml && echo actionlint-ok && yq '.jobs.checks.steps[-1].run' .github/workflows/ci.yml
```
Expected: `actionlint-ok`, then `mix run --no-start scripts/bench/run.exs --profile smoke --data-dir "$RUNNER_TEMP/s5a-data" --out "$RUNNER_TEMP/s5a-smoke"`.

- [ ] **Step 3: Commit**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && git add -- .github/workflows/ci.yml && git commit --only -m "ci: run the S5a benchmark smoke profile in checks" -- .github/workflows/ci.yml
```
Expected: one commit changing one file (+3 lines).

---

## Task 6: Required verification sequence

**Files:** none (execution log only, committed in Task 7).

- [ ] **Step 1: Run the AGENTS.md sequence from the umbrella root**

Run each command and stop at the first failure:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix deps.get
env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix format
env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix format --check-formatted
env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix compile --warnings-as-errors
env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix test
env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix graviton_mq.check_architecture
env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix help xref
env MIX_BUILD_ROOT=/tmp/gmq-build-s5a TMPDIR=/tmp/gmq-s5a-tmp mix xref graph --format cycles --fail-above 0
```
Expected: every command exits 0. `mix test` reports 0 failures in every app (tagged interop tests are excluded without peers) and includes the 10 `bench_stats_test.exs` tests. `mix graviton_mq.check_architecture` passes unchanged, since no `lib` file changed. The cycles check reports no cycles.

- [ ] **Step 2: Inspect the integrated diff**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && git status --short && git diff --stat 70e0a5e..HEAD && git diff --name-only 70e0a5e..HEAD -- 'apps/*/lib'
```
Expected: `git status` shows nothing. If the umbrella `mix format` changed a file outside this plan, do not commit it: restore it with `git checkout -- <path>` and report it. The diff stat lists only the File map paths (and this plan, if Task 7 has run). The `apps/*/lib` listing is empty.

- [ ] **Step 3: Record the results in the execution log**

Append the Task 6 results (command, exit status, test counts) to the `## Execution log` of this plan. Task 7 commits them.

---

## Task 7 (coordinator): Push, CI run id, record

**Files:**
- Modify: `docs/qualification/2026-11-13-s5a-baseline.md` (last line of the `## CI` section)
- Modify: `docs/superpowers/plans/2026-11-13-s5a-performance-baseline.md` (execution log)

- [ ] **Step 1: Push and find the run**

Run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && git push origin main && gh run list --workflow CI --branch main --limit 1 --json databaseId,headSha,status
```
Expected: the push succeeds and a run for the pushed `HEAD` SHA is listed.

- [ ] **Step 2: Wait for the `checks` job**

Run `gh run watch <databaseId> --exit-status`, then `gh run view <databaseId> --log --job <checks-job-id> | grep -E "^.*(PASS|FAIL) |passed, .* failed"`, where `<databaseId>` comes from Step 1 and `<checks-job-id>` from `gh run view <databaseId> --json jobs`.
Expected: the `checks` job succeeds, and its smoke step prints ten `PASS` lines and `10 passed, 0 failed`. A failed smoke step is diagnosed like Task 3 Step 3; CI evidence is never claimed for a failed or pending run.

- [ ] **Step 3: Record the run id**

In `docs/qualification/2026-11-13-s5a-baseline.md`, replace:
```text
baseline. CI run: not yet recorded.
```
with:
```text
baseline. The `checks` job of CI run <databaseId> for `<headSha>` passed the smoke step.
```
Use the values from Step 1. Add the run id and the Task 6 results to the execution log, then run:
```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && git add -- docs/qualification/2026-11-13-s5a-baseline.md docs/superpowers/plans/2026-11-13-s5a-performance-baseline.md && git commit --only -m "docs: record S5a CI smoke run" -- docs/qualification/2026-11-13-s5a-baseline.md docs/superpowers/plans/2026-11-13-s5a-performance-baseline.md && git push origin main
```
Expected: one commit and a successful push.

---

## Self-review

**Spec coverage, section by section**

- §1 Goal: Tasks 2–4 measure the durable-through acceptance path (`mqtt_qos1_durable`, `amqp_durable`) with a fixed tool that S5b reruns.
- §2 Non-goals: no `apps/*/lib` change (rule 4, Task 6 Step 2); no optimisation or tuning (D5 changes capacity only); no soak; no interop claim (rule 5, record wording); no thresholds (CI step name, record).
- §3 Architecture: two VMs (Task 3 `start_broker/3`, Task 2); Mix-free `elixir -pa` with `:code.lib_dir/1` ebin paths, the same apps as `mqtt_persistence_paho_test.exs` and the `scripts/build_hex.exs` TLS smoke; `persistence: [directory:, stream_id:]` for MQTT and for AMQP (`profile: :local_durable` is selected by `persistence`); TCP loopback only; one broker VM per scenario × N; exit on EOF with data directory deletion (Task 2 Step 2); public client APIs only.
- §4 Scenarios: the `@scenarios` list in `run.exs` has the five ids; N from `@profiles`; one connection per publisher and one outstanding publication (`publish_loop/1`); 256 B default with `--payload-bytes`; one consumer with QoS equal to the publish QoS or settling `Accepted`, with ample credit (D7); consumption rate recorded (`consume_rate`).
- §5 Measurement: the `full` and `smoke` phases; the window rule (`measured? = started >= window_start`, loop ends at `window_end`); throughput (QoS 0 uses delivered messages); acceptance and end-to-end latency from the generator's monotonic clock; exact percentiles (`Stats.latency_summary/1`, Task 1 tests); all four gate checks (`Stats.verdict/1`, worker failure or timeout messages, `STATUS` answered with the right profile, `drained?/2`). A failed result has `null` figures (`base/5`).
- §6 Record: `results.json` fields (D8) and environment (commit and dirty state, CPU, cores, memory, OTP/Elixir, schedulers, `df -T` filesystem, `findmnt` device); qualification record with environment table, scenario table and the comparability statement (Task 4).
- §7 CI: one `checks` step, smoke profile with all scenarios, no threshold, runner temporary disk, figures not recorded as the baseline (Task 5, record wording).
- §8 Failure handling: READY within 20 s (`@ready_ms`), early exit (`{:exit_status, _}` clauses), connect failure without retry (`connected/1` raises), stdin close + 10 s + kill (`stop_broker/1`), data directory always deleted (`File.rm_rf!/1` in `scenario/3` and in the broker VM), non-zero exit (`System.halt(1)`). Task 3 Step 4 exercises an early broker exit.
- §9 Testing: pure functions in `scripts/bench/stats.exs`, ExUnit tests at the specified path; the CI smoke step is the end-to-end test.
- §10 Exit evidence: 1 is Task 4, 2 is Task 7, 3 is Task 6.

**Placeholder scan.** Every file's complete content is given. The only angle-bracket values are run-time outputs that the step names explicitly (probe values, load triples, run id, job id, SHA). `<out>`/`<directory>` appear only in usage text. No "TBD" or "similar to" remains.

**Name and signature consistency.** `GravitonMQBench.Stats.latency_summary/1`, `accounting/2`, `verdict/1` and `rate/2` are defined in Task 1 and used with these arities in `run.exs`. The `accounting/2` result keys `expected`, `delivered`, `missing`, `duplicates` and `unexpected` match the tests and `run.exs`. The broker VM argv order `<mqtt|amqp> <memory|durable> <directory> <address>` matches `start_broker/3`. The `READY` field order matches `await_ready/2`. The `STATUS` JSON keys (`profile`, `storage`, `pending_deliveries`, `prepared`, `queue.messages`, `queue.inflight`) match `await_drained/3` and `drained?/2`. The worker messages `{:ready, pid}`, `{:go, …}`, `{:published, pid, result}`, `{:finish, expected}`, `{:consumed, pid, consumed}`, `{:bench_delivered, sequence}`, `{:failed, pid, role, reason}` and `:stop` are each sent and received consistently. The renderer reads exactly the keys `run.exs` writes.

## Execution log


### 2026-11-13 — Tasks 0–3 (implementer)

- **Task 0.** Clean `main` at `f7636b1`; the spec's last commit is `70e0a5e`. `mix deps.get` was skipped on the coordinator's instruction (deps were already present). `mix compile --warnings-as-errors` exited 0, and the isolated build lists the ten expected apps. All six host tools are present, and `/tmp/gmq-s5a-tmp` is on `/dev/mapper/root_vg-root ext4`. The host `elixir` is 1.18.5 (OTP 28), while `.tool-versions` declares 1.18.4.
- **Task 1** (`a0117fe`). Red: `Code.LoadError … scripts/bench/stats.exs … enoent`, exit 1. Green: `10 tests, 0 failures`, no warnings. `mix format` changed nothing.
- **Task 2** (`4b0d3b2`). `mqtt memory` printed `READY <pid> 35559 0` and `STATUS {"profile":"memory","prepared":0,"storage":"ready","connections":0,"sessions":0,"pending_deliveries":0}`. `amqp durable` printed `READY <pid> 0 40067` and `STATUS {"profile":"local_durable","queue":{"messages":0,"ready":0,"inflight":0},"connections":0}`. Both exited 0, and both data directories were removed.
- **Task 3** (`e8aa305`). `--profile bogus` printed the `RuntimeError`, exit 1, with no warnings.
  - First smoke run, with a slow disk (200 dsync writes took 16.8 s, about 84 ms each, load about 8): 9 passed and 1 failed, `FAIL mqtt_qos1_durable n=8: {:publisher, 3} … {:error, :unknown_outcome}`. This is the documented host-contention case. The disk probe stayed at 14–17 s for about 5 minutes and then dropped to 0.03 s.
  - Smoke rerun with a fast disk (probe 0.03–0.04 s): `10 passed, 0 failed`, exit 0. The data directory was empty, and no broker VM was left. p99 figures: `mqtt_qos0_mem` 1/8 e2e 706/3272 µs; `mqtt_qos1_mem` 1/8 accept 913/26870 µs; `mqtt_qos1_durable` 1/8 accept 5936/218273 µs; `amqp_mem` 1/8 accept 44151/43938 µs; `amqp_durable` 1/8 accept 44094/44024 µs. The prototype observations were reproduced: AMQP acceptance takes about 42 ms (Nagle), and with N = 8 the single consumer lags (`mqtt_qos1_mem` 466 msg/s accepted vs 116.7 msg/s consumed).
  - Step 4 kill check: `FAIL mqtt_qos1_mem n=1: broker VM exited with status 137`, then every later scenario ran. Result: `8 passed, 2 failed` and `exit=1`. The second failure was a disk-contention `mqtt_qos1_durable n=8 :unknown_outcome`. The data directory was empty, and no broker VM was left.
  - Deviation in Step 4: as written, `pkill -KILL -f "broker_vm.exs mqtt memory"` also matched the `bash -c`/zsh wrapper, whose command line contains that string. That killed the check shell before it could record `exit=`. The broker VM was still killed, and the orphaned generator finished with the same FAIL line and no leftovers. The check was rerun with the pattern `broker_vm[.]exs mqtt memory`, which cannot match itself. Only the check command changed; `scripts/bench/` was not changed.

### 2026-11-13 — Tasks 4–6 (implementer)

- **Task 4: stopped at Step 3; no baseline committed.** Both full runs measured the committed tree (`409dcb1`, `dirty: false`). Each run's post-run disk probe exceeded the 10 ms/write gate.
  - Run 1. Pre-run probe at 07:29: 0.0335 s, 0.17 ms/write, load `3.47 5.68 5.91`. Post-run probe at 07:39: 16.07 s, 80.35 ms/write, load `7.08 6.89 6.45`. Result: `13 passed, 2 failed`, `exit=1`.
    - The failures were `mqtt_qos1_durable n=64` `{:error, :unknown_outcome}` and `amqp_durable n=64` `{:unknown, :closed}`.
    - The disk was slow during the AMQP durable scenarios: `amqp_durable n=1` had 1.6 msg/s and an acceptance p50 of 551 ms, against about 42 ms when the disk is fast.
  - Waiting. The disk was re-probed every 3 minutes from 07:40 to 07:56. It measured 15.6, 11.8, 7.8, 13.5 and 14.8 s per 200 writes, then 0.035 s at 07:56:19.
  - Run 2, a full repeat from Step 1. Pre-run probe at 07:56: 0.0327 s, 0.16 ms/write, load `4.82 4.72 5.22`. Post-run probe at 08:06: 13.73 s, 68.66 ms/write, load `5.67 6.08 5.75`. Result: `12 passed, 3 failed`, `exit=1`.
    - The failures were `mqtt_qos1_durable n=64` `{:error, :unknown_outcome}`, `amqp_durable n=8` `missing 1876, unexpected 0`, and `amqp_durable n=64` `{:unknown, :closed}`.
    - A probe at 08:07 still measured 10.85 s.
  - Cause. A postgres server outside this project (`postgres -D /tmp/secrethub-hk-pg/…`, about 30 GB written, plus its walwriter) and other host processes were writing to the same `/dev/mapper/root_vg-root`. This is host contention, not a fault in the benchmark tool.
  - Following Step 3, both values go to the coordinator. The raw files stay uncommitted at `/tmp/gmq-s5a-tmp/full-run1{,.log}` and `/tmp/gmq-s5a-tmp/full{,.log}`.
  - The memory-scenario figures matched across the two runs and reproduce the known broker findings:
    - AMQP acceptance is about 42 ms. Nagle is on: `nodelay: false` in `runtime/hell_handoff.ex`.
    - The single consumer lags at N ≥ 8. `mqtt_qos1_mem` n=8 accepted 218 msg/s but consumed 54.5 msg/s, with an end-to-end p50 of about 25 s. `amqp_mem` n=64 consumed 1.4 msg/s.
    - The durable N = 64 scenarios fail.
- **Task 5** (`84273ac`). One `checks` step was added after `Check source cycles`, adding 3 lines. `actionlint .github/workflows/ci.yml` passed (`actionlint-ok`), and `yq` shows the step's `run` as the smoke command.
- **Task 6.** The sequence ran on a clean `git archive HEAD` export at `84273ac`, in `/tmp/gmq-verify` with `MIX_BUILD_ROOT=/tmp/gmq-verify/_build`.
  - Every command exited 0: `mix deps.get`, `mix format --check-formatted`, `mix compile --warnings-as-errors`, `mix test`, `mix graviton_mq.check_architecture` (passed, 7943 source references across 7 applications), `mix help xref`, and `mix xref graph --format cycles --fail-above 0` (`No cycles found`).
  - `mix test` reported 0 failures in every app: core 38, amqp10 532, mqtt 381, storage 82, client 171 (61 excluded), runtime 171, graviton_mq 268 (89 excluded). The 10 `bench_stats_test.exs` tests pass.
  - The umbrella-wide `mix format` (no flag) was not run on the working tree. The `--check-formatted` run on the export passed.
  - `git diff --name-only 70e0a5e..HEAD -- 'apps/*/lib'` is empty. No broker VM is left, and the data directory is empty.

### 2026-11-13 — Task 4 rerun on `/data` (implementer)

- **Coordinator and owner decisions.** The data directory moves to the separate disk `/data` (`/dev/sdb1`, ext4), as `--data-dir /data/gmq-s5a-bench`. The 10 ms/write gate is unchanged and is probed against that directory. Broker behaviour is recorded as it is, with failed scenarios kept as failed.
- **Clean tree.** `run.exs` records `dirty` from `git status --porcelain` at start-up. The uncommitted Task 6 log entry was copied to `/tmp/gmq-s5a-tmp`, the plan was checked out for the run, and the copy was restored afterwards. The run recorded `84273ac`, `dirty: false`. Run 2's raw output moved to `/tmp/gmq-s5a-tmp/full-run2{,.log}`.
- **Step 1.** Pre-run probe at 08:22:56: 0.866 s, 4.33 ms/write, load `4.28 5.49 5.87`.
- **Step 2.** The full profile ran from 08:23:08 to 08:34. Result: `14 passed, 1 failed`, `exit=1`.
  - The one failure was `mqtt_qos1_durable n=64`: `{:publisher, 53} … {:error, :unknown_outcome}`. This is broker behaviour (PUBACK beyond 60 s), so it is recorded as failed.
  - `amqp_durable n=64` passed (70.0 msg/s, consume 0.7 msg/s).
  - `amqp_durable n=8` passed (59.0 msg/s, consume 6.8 msg/s, duplicates 0). Run 2's `missing 1876` did not reproduce.
  - The 27 `GenServer … terminating ** (stop) :redacted` log entries after `mqtt_qos1_durable n=64` also appeared in run 2. They are the client teardown of that failed scenario.
- **Step 3.** Post-run probe at 08:34:31: 0.773 s, 3.86 ms/write, load `5.58 6.34 6.23`. The data directory was empty, and no broker VM was left.
- **Steps 4–5.** The JSON was copied, and the record was rendered with the plan's renderer. The environment was `ext4` on `/dev/sdb1` instead of the plan's expected `/dev/mapper/root_vg-root`, which is the coordinator's decision. The record adds sections on the disk choice and its probe history, the toolchain (host Elixir 1.18.5 vs `.tool-versions` 1.18.4), and Known broker findings (Nagle, consumer lag, durable N = 64, and the unreproduced `amqp_durable` N = 8 missing messages). These are inputs to a follow-up hot-path fix sub-project and are not fixed in S5a.

### Task 7 (2026-11-13, coordinator)

- Pushed `2a9bbb3..023b132`. CI run 43338450615 passed: `checks` (including the new
  smoke step, `10 passed, 0 failed`) and `interoperability`. Tests run 43338450472 passed.
- The CI run id is recorded in `docs/qualification/2026-11-13-s5a-baseline.md`.
- Follow-up: the hot-path fix sub-project takes the Known broker findings as input.
