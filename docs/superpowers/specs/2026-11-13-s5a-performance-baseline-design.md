# S5a — Throughput and latency baseline (design)

Status: approved by the owner on 2026-11-13. This is part of programme S5
(`2026-11-12-http-fetch-transport-and-p0-p3-design.md` §2), which the owner
split into S5a (baseline), S5b (WAL group commit), S5c (`:telemetry` events)
and S5d (AMQP TTL/absolute-expiry). They are done in that order, and each
has its own spec, plan and evidence.

## 1. Goal

Record a reproducible baseline of broker throughput and latency, and give
S5b a fixed tool to compare against. The baseline must cover the path S5b
changes: publisher acceptance that waits for the durable-through boundary.

## 2. Non-goals

- Changing product code. S5a adds only scripts, a CI step and records.
- Optimisation of any kind, WAL changes (S5b), telemetry (S5c), TTL (S5d).
- Soak, long-duration or unbounded-load qualification. The existing
  `scripts/interop/soak.exs` remains the bounded stability check.
- Interoperability evidence. The load generator uses our own clients, so
  results say nothing about independent peers.
- Performance gates. No threshold fails CI.

## 3. Architecture

```text
mix run --no-start scripts/bench/run.exs          (generator VM)
  ├─ spawns: elixir -pa <ebin…> scripts/bench/broker_vm.exs <scenario args>
  │            (broker VM, no Mix; prints "READY <os_pid> <mqtt_port> <amqp_port>")
  ├─ N publisher processes  (GravitonMQ.MQTTClient / GravitonMQ.AMQP10Client)
  ├─ 1 subscriber/receiver process
  └─ writes <out>/results.json and prints a summary
```

- **Two VMs.** The broker and the generator run on separate BEAM schedulers,
  so the generator's work does not show up as broker cost.
- **The broker VM** is Mix-free, like the S1b installed-package TLS smoke in
  `scripts/build_hex.exs`. It starts one instance with `GravitonMQ.start_link/1`
  for the scenario's profile:
  - memory, or durable with `persistence: [directory:, stream_id:]` for MQTT
    and for `:amqp10` (`profile: :local_durable`);
  - plain-TCP loopback listeners only.
  It exits when its stdin reaches EOF, and deletes its data directory.
  One broker VM per scenario keeps the scenarios from interfering.
- **The generator** uses only public client APIs. It drives MQTT 5.0 through
  `GravitonMQ.MQTTClient` and AMQP 1.0 through `GravitonMQ.AMQP10Client`.

## 4. Scenarios

| id | protocol | profile | acceptance waits for |
| --- | --- | --- | --- |
| `mqtt_qos0_mem` | MQTT 5.0 QoS 0 | memory | nothing; end-to-end only |
| `mqtt_qos1_mem` | MQTT 5.0 QoS 1 | memory | PUBACK |
| `mqtt_qos1_durable` | MQTT 5.0 QoS 1 | persistence | PUBACK after durable-through |
| `amqp_mem` | AMQP 1.0, unsettled sender | memory queue | `Accepted` |
| `amqp_durable` | AMQP 1.0, durable message, unsettled sender | `:local_durable` | `Accepted` after durable-through (`{:accepted_durable, _}`) |

- Each scenario runs with N concurrent publishers, N ∈ {1, 8, 64}, each with
  its own connection. Each publisher has a fixed window of 1 outstanding
  publication, so N is the offered concurrency.
- Payload: 256 B by default. `--payload-bytes` allows another size. The
  recorded baseline uses 256 B only.
- One subscriber or receiver consumes everything, with QoS equal to the
  publish QoS (MQTT) or settling `Accepted` (AMQP), and with enough credit
  that delivery is never the bottleneck. Its consumption rate is recorded.

## 5. Measurement

- **Phases:** warm-up, then measurement. `full` is 5 s + 20 s; `smoke` is
  1 s + 3 s. Only publications started inside the measurement window count.
- **Throughput:** accepted publications per second in the measurement window.
  For QoS 0, delivered messages per second.
- **Acceptance latency:** from the publish call to the PUBACK/Accepted
  operation result.
- **End-to-end latency:** from the publish call to receipt by the subscriber.
  Each payload carries its sequence number and send timestamp.
- Both latencies come from `System.monotonic_time/1` in the generator VM, so
  they share one clock. They are reported as p50, p99, p999 and max, computed
  exactly from the recorded samples. The sample count is bounded by
  duration × rate, and the generator keeps them in memory.

**Correctness gate.** Every measured scenario checks four things:

- every accepted publication is delivered exactly once to the subscriber,
  by sequence number. QoS 1 / at-least-once duplicates are counted and
  reported, and they make the scenario fail only if a sequence number is
  missing;
- no publication fails or times out;
- the broker VM stays alive;
- the queue or session is drained at the end.

A scenario that fails the gate records `status: "failed"` with the reason and
**no** throughput or latency numbers. A broken run never produces a figure.

## 6. Record

- `<out>/results.json` contains one object per scenario × N with these fields:
  - `id`, `n`, `payload_bytes`, `status`, `throughput`, `accept_latency_us`,
    `e2e_latency_us` and `duplicates`.
  - `environment`: commit (`git rev-parse HEAD`, plus whether the tree is
    dirty), CPU model, logical cores, memory, OTP and Elixir versions,
    `System.schedulers_online/0`, and the data directory's filesystem type
    and block device (from `df -T` / `findmnt`).
- `docs/qualification/2026-11-13-s5a-baseline.md` is the committed summary
  of a `full` run on the owner's development host. It holds the environment
  table, the per-scenario table, and a statement that the figures are only
  comparable with runs on the same host and filesystem.
- S5b reruns the same profile on the same host and records before/after.

## 7. CI

- `.github/workflows/ci.yml`, job `checks`, gets one step: `smoke` profile
  with N ∈ {1, 8}, all scenarios, about 1 minute.
- It checks only that the tool runs and that the correctness gate passes.
  It reports numbers but asserts no threshold. It runs on the runner's
  temporary disk, and those numbers are not recorded as the baseline.

## 8. Failure handling

- **Broker VM ends early or never prints READY within 20 s:** the scenario
  fails and the generator moves on to the next one.
- **Client start or connect failure:** the scenario fails. There are no retries.
- **On exit, normal or abnormal:** the generator closes the broker VM's stdin
  and waits up to 10 s, then kills the OS process. It always deletes the data
  directory.
- **Exit status:** the overall exit code is non-zero if any scenario failed,
  so CI notices.

## 9. Testing the tool

- The latency summary (exact percentiles) and the correctness accounting
  (sequence numbers, duplicates, missing) are pure functions in
  `scripts/bench/stats.exs`. ExUnit tests for them live in
  `apps/graviton_mq/test/graviton_mq/bench_stats_test.exs`.
- The CI smoke step is the end-to-end test of the tool.

## 10. Exit evidence

1. A `full` run on the development host, recorded in
   `docs/qualification/2026-11-13-s5a-baseline.md` with `results.json`
   attached alongside it.
2. CI `checks` passing with the smoke step, with the run id recorded.
3. The required verification sequence passes.
