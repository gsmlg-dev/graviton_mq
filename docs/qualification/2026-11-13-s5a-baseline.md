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
| Commit | `84273ac6581394ad20cf3214ce8b983ae8d495b1` (dirty tree: false) |
| CPU | Intel(R) Xeon(R) CPU E5-2650 v4 @ 2.20GHz |
| Logical cores | 48 |
| Memory | 94.3 GiB |
| OTP / Elixir | 28.5.0.5 / 1.18.5 |
| Schedulers online (generator VM; the broker VM uses the same default) | 48 |
| Data directory | `/data/gmq-s5a-bench` |
| Filesystem / block device | ext4 on `/dev/sdb1` |
| Profile | full: 5000 ms warm-up + 20000 ms measurement, payload 256 B |
| Synchronous 4 KiB write probe, before / after (`dd oflag=dsync`, 200 writes) | 4.33 / 3.86 ms per write |
| Load average (1/5/15 min), before / after | 4.28 5.49 5.87 / 5.58 6.34 6.23 |

### Data directory and disk

The data directory is on `/data`, a separate physical disk (`/dev/sdb1`, ext4,
10.9 TB), not on the root disk `/dev/mapper/root_vg-root`. Two earlier full runs
used the root disk, and both were rejected by the plan's 10 ms/write probe gate.
An unrelated postgres server outside this project writes to that disk. Before
each run it probed at 0.17 and 0.16 ms/write. After the runs it probed at 80.35
and 68.66 ms/write, and between the runs at 39–78 ms/write (7.8–15.6 s per
200 writes); see the plan's execution log. Before this run the coordinator
probed `/data` at 0.73–0.92 s per 200 synchronous writes in four probes.

This run's probes against `/data/gmq-s5a-bench` were 4.33 ms/write before
(0.866 s) and 3.86 ms/write after (0.773 s). Each synchronous write on `/data`
costs about 4 ms, compared with about 0.2 ms on an idle root disk. The durable
figures (`mqtt_qos1_durable`, `amqp_durable`) include that cost and are only
comparable with runs on the same disk.

### Toolchain

The host Elixir is 1.18.5 (OTP 28.5.0.5), and `.tool-versions` declares 1.18.4.
The run used the host toolchain.

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
| `mqtt_qos0_mem` | 1 | passed | 2029.5 | 2029.5 | - | 473 / 776 / 1384 / 5248 | 0 |
| `mqtt_qos0_mem` | 8 | passed | 4052.7 | 4052.7 | - | 1907 / 2981 / 3795 / 41089 | 0 |
| `mqtt_qos0_mem` | 64 | passed | 2262.2 | 2262.2 | - | 27414 / 36995 / 39082 / 41303 | 0 |
| `mqtt_qos1_mem` | 1 | passed | 2052.4 | 2052.4 | 492 / 748 / 1262 / 2607 | 658 / 1044 / 2506 / 8644 | 0 |
| `mqtt_qos1_mem` | 8 | passed | 229.4 | 57.3 | 33512 / 57453 / 67176 / 68188 | 27966720 / 29430652 / 29436993 / 29439182 | 0 |
| `mqtt_qos1_mem` | 64 | passed | 243.8 | 7.7 | 257095 / 422977 / 433768 / 438813 | 40036522 / 40999479 / 41023318 / 41025867 | 0 |
| `mqtt_qos1_durable` | 1 | passed | 28.5 | 28.4 | 31971 / 110348 / 142209 / 142209 | 40538 / 127956 / 163209 / 163209 | 0 |
| `mqtt_qos1_durable` | 8 | passed | 16.6 | 4.2 | 473441 / 855598 / 862025 / 862025 | 23103707 / 24516640 / 24544748 / 24544748 | 0 |
| `mqtt_qos1_durable` | 64 | failed | - | - | - | - | - |
| `amqp_mem` | 1 | passed | 23.6 | 23.6 | 42037 / 44112 / 45135 / 45135 | 4057 / 5693 / 6577 / 6577 | 0 |
| `amqp_mem` | 8 | passed | 189.4 | 178.2 | 42026 / 44112 / 83468 / 84417 | 73483 / 1283523 / 1313785 / 1317123 | 0 |
| `amqp_mem` | 64 | passed | 102.8 | 1.5 | 604647 / 1345009 / 1580699 / 1621618 | 107419413 / 109272353 / 109536277 / 109728231 | 0 |
| `amqp_durable` | 1 | passed | 23.3 | 23.3 | 42038 / 63102 / 80924 / 80924 | 19327 / 74325 / 106745 / 106745 | 0 |
| `amqp_durable` | 8 | passed | 59.0 | 6.8 | 128707 / 269917 / 302189 / 306517 | 40865793 / 45762075 / 45795089 / 45817559 | 0 |
| `amqp_durable` | 64 | passed | 70.0 | 0.7 | 906981 / 1640237 / 1900143 / 1935369 | 61339939 / 69506709 / 69763869 / 69767287 | 0 |

## Failed scenarios

- `mqtt_qos1_durable` n=64: {:publisher, 53} failed: ** (MatchError) no match of right hand side value: {:error, :unknown_outcome}

## Known broker findings

The baseline records the broker's current behaviour as it is. Each item below
is an input to a follow-up hot-path fix sub-project. None of them is fixed in
S5a, and no `apps/*/lib` file changed.

- **Nagle on broker sockets.** AMQP acceptance takes about 42 ms per
  publication, even in memory. In `amqp_mem` N = 1, acceptance p50 is
  42037 µs, while end-to-end p50 is 4057 µs: the delivery arrives before the
  disposition. This matches Nagle plus delayed ACK.
  `apps/graviton_mq_runtime/lib/graviton_mq/runtime/hell_handoff.ex`
  (lines 133–135) sets `nodelay: false` on broker listener sockets.
- **Single-consumer lag at N ≥ 8.** Delivery to the single subscriber falls
  far behind acceptance (accepted vs consumed msg/s):

  | Scenario | N | Accepted | Consumed | End-to-end p50 |
  | --- | --- | --- | --- | --- |
  | `mqtt_qos1_mem` | 8 | 229.4 | 57.3 | about 28 s |
  | `mqtt_qos1_mem` | 64 | 243.8 | 7.7 | about 40 s |
  | `mqtt_qos1_durable` | 8 | 16.6 | 4.2 | about 23 s |
  | `amqp_mem` | 64 | 102.8 | 1.5 | about 107 s |
  | `amqp_durable` | 8 | 59.0 | 6.8 | about 41 s |
  | `amqp_durable` | 64 | 70.0 | 0.7 | about 61 s |

  These scenarios pass the correctness gate because the backlog drains after
  publishing stops.
- **Durable N = 64 timeouts.** In this run, `mqtt_qos1_durable` N = 64 failed
  with `{:error, :unknown_outcome}`: no PUBACK arrived within the 60 s client
  timeout. `amqp_durable` N = 64 passed on `/data`, with acceptance p99 of
  1.64 s. In both earlier root-disk runs it had failed with
  `{:unknown, :closed}`, when the Manager's 5 s enqueue deadline closed the
  publisher. It is therefore sensitive to the disk's sync latency.
- **`amqp_durable` N = 8 missing messages (not reproduced).** The second
  root-disk run failed `amqp_durable` N = 8 with `missing 1876, unexpected 0`:
  the subscriber never received 1876 accepted publications. That run was on the
  contended disk, and no cause was established. It did not reproduce on
  `/data`: that scenario passed, with every accepted publication consumed, no
  duplicates, and a drained queue. The finding stays open as unexplained.

## Reproduce

```bash
mix run --no-start scripts/bench/run.exs --profile full --data-dir /data/gmq-s5a-bench --out <directory>
```

## CI

The `checks` job runs the `smoke` profile (1 s + 3 s, N in {1, 8}, all
scenarios) and only checks that the tool runs and the correctness gate passes.
Its figures come from the runner's temporary disk and are not part of this
baseline. CI run: 43338450615 (`checks` and `interoperability` passed) and Tests run
43338450472 for `023b132`; the `checks` smoke step passed 10 of 10 scenarios
(runner figures are not a baseline).
