# S4 MQTT 5.0 Topic Alias, Subscription Identifiers and Shared Subscriptions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement MQTT 5.0 Topic Alias (both directions), Subscription Identifiers and Shared Subscriptions in the pure MQTT layer, the broker runtime and the independent client, with a v2 broker snapshot that upgrades v1 at read time, and qualify each feature against independent peers.

**Architecture:** All protocol behaviour lives in the pure `apps/graviton_mq_mqtt` machines (`GravitonMQ.MQTT.Topic`, a new `GravitonMQ.MQTT.TopicAlias`, `GravitonMQ.MQTT.Broker`, `GravitonMQ.MQTT.Broker.Checkpoint`, `GravitonMQ.MQTT.Client`). Runtime and client owners only add configuration switches, status fields, a subscribe option and checkpoint validation. Alias tables are transient per Network Connection; Subscription Identifiers and shared membership are session state persisted in broker snapshot v2.

**Tech Stack:** Elixir 1.18 / OTP 28 umbrella (`.tool-versions`), ExUnit, Paho MQTT Python 2.1.0 and Mosquitto 2.1.2 (CI only, through `scripts/interop/run.py`).

**Approved design:** [`docs/superpowers/specs/2026-11-12-s4-mqtt-alias-subid-shared-design.md`](../specs/2026-11-12-s4-mqtt-alias-subid-shared-design.md) (owner decisions of 2026-11-12: Q1 on-by-default with switches and broker outbound aliases off; Q9 snapshot v2 with read-time v1 upgrade, same WAL record type, no downgrade; all other §10 recommendations accepted).

---

## Execution rules (read before Task 0)

### Parallel checkout with S2

S2 (AMQP SASL) runs in the same checkout on `main` at the same time.

1. **Write scope.** Tasks 0–18 may create or modify only:
   - `apps/graviton_mq_mqtt/**` (pure MQTT library and its tests/fixtures);
   - `apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/**` and `apps/graviton_mq_runtime/test/mqtt/**`;
   - `apps/graviton_mq_client/lib/graviton_mq/client/mqtt.ex`, `apps/graviton_mq_client/lib/graviton_mq/client/mqtt/**`, `apps/graviton_mq_client/lib/graviton_mq/mqtt_client.ex` (moduledoc only) and MQTT tests/support under `apps/graviton_mq_client/test/`;
   - MQTT tests and Python peers under `apps/graviton_mq/test/graviton_mq/mqtt_*` and `apps/graviton_mq/test/support/mqtt_*`;
   - `docs/MQTT_BROKER.md`, `docs/MQTT_CLIENT.md` (MQTT-only documents).
   Never touch AMQP files, `mix.exs`, `mix.lock`, `deps/`, `build/`, `config/`, `apps/graviton_mq/lib/**`.
2. **Coordinator-serialized files.** `docs/CAPABILITIES.md`, `docs/qualification/2026-10-30-mqtt5-clause-audit.md`, `docs/qualification/2026-10-30-mqtt5-requirement-inventory.md` (and its `.json`), `scripts/interop/*` and `.github/workflows/*` are edited **only** in Tasks 19–20, which the coordinator runs after S2 has finished writing. S4 needs no change to `scripts/interop/*` or `.github/workflows/*` (new peer tests reuse the existing tags `:interop` / `:paho_interop`, which the CI `interoperability` job already runs through `run.py -- mix test`); Tasks 19–20 only confirm that.
3. **Isolated build directory (verified 2026-11-12 on Elixir 1.18.5).** `MIX_BUILD_PATH` *does* override each app's `build_path: "../../_build"`, but it is used verbatim without an environment suffix, so `mix compile` (dev) and `mix test` (test) would share one directory and keep recompiling each other's artifacts. `MIX_BUILD_ROOT` also overrides `build_path` **and** keeps per-environment subdirectories:

   ```text
   MIX_BUILD_PATH=/tmp/p mix eval --no-mix-exs 'IO.inspect Mix.Project.build_path(build_path: "../../_build")'
   "/tmp/p"
   MIX_BUILD_ROOT=/tmp/r MIX_ENV=test mix eval --no-mix-exs 'IO.inspect Mix.Project.build_path(build_path: "../../_build", build_per_environment: true)'
   "/tmp/r/test"
   ```

   Therefore every mix command in this plan is prefixed with
   `env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp`.
   `TMPDIR` isolates `System.tmp_dir!/0` directories, because several existing
   tests name directories with `System.unique_integer/1`, which can collide
   between two concurrently running VMs. `mix graviton_mq.check_architecture`
   reads manifests through `Mix.Project.build_path/0`, so it honours the same
   prefix.
4. **Never run umbrella-wide `mix format`, `mix deps.get` or the full `mix test` in Tasks 0–18.** Format only the files you touched; run only the touched apps' MQTT test files. The full AGENTS.md sequence runs once, in coordinator Task 19. If a compile in another app fails because of S2's in-progress files, do not edit them: record it, wait, and rerun.
5. **Commits.** Commits on `main` are authorized per task (conventional message, **no** `Co-Authored-By` trailer, no "Generated with" line). Never push (the coordinator pushes). Never `git add -A`/`git add .`. Stage new files explicitly, then commit with an explicit path list so S2's staged changes are never included:

   ```bash
   git add <new files>
   git commit -m "<message>" -- <every path of this task>
   ```

   If `git` reports `index.lock` exists, wait a few seconds and retry; never delete the lock.
6. **Peers.** Mosquitto and Paho are not installed locally. Tests tagged `:interop` / `:paho_interop` are excluded locally (`test_helper.exs`) and run only in CI. Local evidence is pure tests plus raw-socket runtime tests. Never claim CI evidence before the coordinator has a passing run.

### Command prefix used below

Every `mix` command is written in full, for example:

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/topic_test.exs
```

`M` = `apps/graviton_mq_mqtt`, `R` = `apps/graviton_mq_runtime`, `C` = `apps/graviton_mq_client`, `G` = `apps/graviton_mq`, `ROOT` = `/home/gao/Workspace/gsmlg-dev/graviton_mq`.

### Decisions taken while planning (spec deltas, see final report)

- **D1 alias table shape.** The spec names three connection fields (`inbound_aliases`, `outbound_aliases`, `outbound_alias_max`). Both roles instead hold one `aliases: %GravitonMQ.MQTT.TopicAlias{inbound, inbound_max, outbound, outbound_max}` field. Same data, one helper.
- **D2 outbound size rule.** A publication is deliverable iff its **full-topic** encoding fits the peer Maximum Packet Size (it must stay retransmittable after a reconnect, when the table is empty). The aliased form is then sent only if it also fits; otherwise the full topic is sent and no alias is recorded. This is stricter than spec §4.2 "size checks use the aliased bytes" and avoids a message that can be sent now but never re-sent.
- **D3 resolved-packet size.** Durable state stores the resolved packet re-encoded with the role's codec (no alias value reaches durable state). If the resolved encoding exceeds the receiver's Maximum Packet Size, the broker disconnects with 0x95 and the client terminates with 0x95 (`:resolved_packet_too_large`); the spec did not cover this case.
- **D4 `max_alias_bytes` default.** The spec default 16 MiB is smaller than its own default worst case (128 connections × 10 × 65,535 B = 83,884,800 B), so `Broker.new/1` would reject its defaults. Default is **134,217,728** (128 MiB, a static bound, not an allocation).
- **D5 redistribution dispatch.** Redistribution runs inside `end_session/3`, `down_existing/4` and UNSUBSCRIBE commit as pure state changes. UNSUBSCRIBE pumps immediately; session end and connection loss are dispatched by the next pump (every `{:tick, now}` pumps all sessions; runtime tick 100 ms default). This keeps `Checkpoint.restore/4`'s `{machine, []} = Broker.transition(...)` loss replay effect-free.
- **D6 Q14 reason.** Shared + No Local decodes to a dedicated reason `:shared_subscription_no_local` (class unchanged) that the broker maps to DISCONNECT 0x82; other subscription-option violations keep `:invalid_subscription_options` / 0x81.
- **D7 task order.** Spec T6 (checkpoint v2) is split: the record shape and v1 upgrade (Task 6) land before Sub-IDs (Task 7) so every commit keeps `Checkpoint.capture/3` valid; capability-mismatch clauses land with each feature (Tasks 7, 8). Each feature's runtime switch lands with the feature (Tasks 3, 7, 8). The v1 fixture is generated from the unchanged pre-S4 tree in Task 0.
- **D8 wire tap.** The tap parses MQTT envelopes and PUBLISH properties independently (like `puback_loss_proxy.exs`), instead of "our codec" (spec §7), so it is not an oracle derived from the code under test.
- **D9 Paho QoS2 shared cases.** QoS2-in-flight resume and expiry discard on shared groups are proven by pure and raw-socket runtime tests; Paho covers distribution, no-retained, QoS1 redistribution on member loss, No Local and `SHARED_OFF` (Paho's QoS2 manual-ack timing is not controllable enough for a deterministic peer test).
- **D10 ledgers.** The JSON inventory is not edited; only dated maintenance amendments are appended to the two Markdown ledgers (spec §3 "do not rewrite the 2026-10-30 ledger").

---

## File map

| File | Responsibility | Tasks |
| --- | --- | --- |
| `M/lib/graviton_mq/mqtt/topic.ex` | `parse_filter/1`, `valid_subscription_filter?/1`; `valid_filter?/1` / `matches?/2` stay ordinary-only | 1 |
| `M/lib/graviton_mq/mqtt/topic_alias.ex` (new) | Pure per-connection alias table: `new/2`, `resolve/2`, `assign/2` | 2 |
| `M/lib/graviton_mq/mqtt/broker.ex` | Limits, CONNACK flags, inbound/outbound aliases, Sub-IDs, shared routing, redistribution, Q14 mapping | 3–5, 7–10 |
| `M/lib/graviton_mq/mqtt/broker/checkpoint.ex` | v2 schema, v1 read-time upgrade, capability mismatch | 6–8 |
| `M/lib/graviton_mq/mqtt/codec/operational.ex` | Q14 dedicated decode reason | 10 |
| `M/lib/graviton_mq/mqtt/client.ex` | Client alias options/tables, inbound resolution, outbound assignment | 13 |
| `M/test/fixtures/broker_checkpoint_v1.etf` (new) | v1 snapshot bytes written by pre-S4 code | 0 |
| `M/test/{topic,topic_alias,broker_alias,broker_subscription_identifier,broker_shared,broker_shared_redistribution,client_alias}_test.exs` | Pure vectors | 1–13 |
| `R/lib/graviton_mq/runtime/mqtt/config.ex` | Switches, value mapping, `Broker.new/1` validation | 3, 7, 8 |
| `R/lib/graviton_mq/runtime/mqtt/broker.ex` | `status/1` additions | 11 |
| `R/test/mqtt/{config,persistence}_test.exs` | Switch validation; v1 WAL record recovery | 3, 6–8 |
| `C/lib/graviton_mq/client/mqtt.ex`, `C/lib/graviton_mq/client/mqtt/{config,checkpoint}.ex`, `C/lib/graviton_mq/mqtt_client.ex` | Options, `subscription_identifier:`, persistent shared subscriptions | 14 |
| `C/test/support/mqtt_wire_tap.exs` (new) | Independent byte relay recording PUBLISH topic/alias/Sub-IDs | 15 |
| `C/test/mqtt_peer_capabilities_test.exs` (new, `:interop`) | T0 Mosquitto capability probes | 15 |
| `C/test/mqtt_feature_mosquitto_test.exs` (new, `:interop`) | Our client ↔ Mosquitto | 17 |
| `G/test/graviton_mq/mqtt_feature_test.exs` (new) | Raw-socket broker evidence (local) | 12 |
| `G/test/graviton_mq/mqtt_feature_persistence_test.exs` (new) | Owner-restart recovery, capability mismatch at start | 12 |
| `G/test/support/mqtt_feature_peer.py` (new) | Paho 2.1.0 feature peer and T0 probe | 16 |
| `G/test/graviton_mq/mqtt_feature_paho_test.exs` (new, `:paho_interop`) | Paho → our broker | 16 |
| `G/test/graviton_mq/mqtt_feature_mosquitto_test.exs` (new, `:interop`) | mosquitto_pub/sub → our broker | 16 |
| `docs/MQTT_BROKER.md`, `docs/MQTT_CLIENT.md` | User docs, upgrade note | 18 |
| `docs/CAPABILITIES.md`, MQTT ledgers, spec §1 | Coordinator-serialized evidence | 20 |

---

## Task 0: Workspace isolation, baseline and the v1 fixture

**Files:**
- Create: `apps/graviton_mq_mqtt/test/fixtures/broker_checkpoint_v1.etf`
- Create (scratch, not committed): `/tmp/gmq-s4-tmp/gen_v1.exs`

- [ ] **Step 1: Create isolated directories and record the baseline commit**

```bash
mkdir -p /tmp/gmq-build-s4 /tmp/gmq-s4-tmp
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && git rev-parse --short HEAD && git status --short
```

Expected: `8a31f2a` (the commit that approved the S2/S4 designs) or a later S2 commit, and only untracked plan documents. Write the hash down as `BASE`; it is quoted in the fixture test and in Tasks 19–20.

- [ ] **Step 2: Baseline the MQTT test files in the isolated build**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test
```

Expected: `0 failures` and `ls /tmp/gmq-build-s4/test/lib` lists `graviton_mq_mqtt`. Nothing new appears under `ROOT/_build`.

- [ ] **Step 3: Write the fixture generator (pre-S4 code, unmodified tree)**

Create `/tmp/gmq-s4-tmp/gen_v1.exs`:

```elixir
alias GravitonMQ.Core.CheckpointCodec
alias GravitonMQ.MQTT.{Broker, Broker.Checkpoint}
alias GravitonMQ.MQTT.Codec.Packet
alias GravitonMQ.MQTT.Packet.{Connect, Publish, Subscribe}

allow = fn allow, machine, effects, now ->
  Enum.reduce(effects, machine, fn
    {:authorize, id, request, _}, machine ->
      decision = if is_list(request), do: Enum.map(request, fn _ -> :ok end), else: {:ok, :user}

      {machine, more} =
        Broker.transition(
          machine,
          {:auth_result, id, machine.operations[id].generation, decision, now}
        )

      allow.(allow, machine, more, now)

    _effect, machine ->
      machine
  end)
end

step = fn machine, id, packet, now ->
  {:ok, bytes} = Packet.encode(packet)
  {machine, effects} = Broker.transition(machine, {:packet, id, 1, packet, bytes, now})
  allow.(allow, machine, effects, now)
end

open = fn machine, id, now ->
  {machine, []} = Broker.transition(machine, {:connection_open, id, 1, now})

  step.(
    machine,
    id,
    %Connect{
      version: :v5,
      client_id: to_string(id),
      clean_start: true,
      properties: [session_expiry_interval: 600]
    },
    now
  )
end

{:ok, machine} = Broker.new()
machine = open.(machine, :reader, 0)
machine = step.(machine, :reader, %Subscribe{packet_id: 1, subscriptions: [{"fixture/#", 1}]}, 0)
machine = open.(machine, :writer, 0)

machine =
  step.(
    machine,
    :writer,
    %Publish{topic: "fixture/a", payload: "v1-content", qos: 1, packet_id: 9, retain: true},
    10
  )

{machine, _} = Broker.transition(machine, {:connection_down, :reader, 1, :abnormal, 20})
{:ok, snapshot} = Checkpoint.capture(machine, 30, 100_000)
1 = snapshot.version
[%{ever_sent: true}] = snapshot.state.sessions["reader"].deliveries
{:ok, bytes} = CheckpointCodec.encode(snapshot, max_bytes: 1_048_576, max_items: 500_000)
File.write!(System.fetch_env!("FIXTURE"), bytes)
IO.puts("wrote #{byte_size(bytes)} bytes")
```

- [ ] **Step 4: Generate the fixture with the unmodified pre-S4 code**

```bash
mkdir -p /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt/test/fixtures
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp FIXTURE=$PWD/test/fixtures/broker_checkpoint_v1.etf mix run --no-start /tmp/gmq-s4-tmp/gen_v1.exs
```

Expected: `wrote <n> bytes` with `n` below 4,000. Run `git diff --stat -- apps/graviton_mq_mqtt/lib` and confirm it is empty (the fixture must come from pre-S4 code).

- [ ] **Step 5: Commit the fixture**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
git add apps/graviton_mq_mqtt/test/fixtures/broker_checkpoint_v1.etf
git commit -m "test(mqtt): add pre-S4 v1 broker checkpoint fixture" -- apps/graviton_mq_mqtt/test/fixtures/broker_checkpoint_v1.etf
```

---

## Task 1 (spec T1): Shared filter parsing in `Topic`

**Files:**
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/topic.ex`
- Test: `apps/graviton_mq_mqtt/test/topic_test.exs`

- [ ] **Step 1: Write the failing tests** (append inside `GravitonMQ.MQTT.TopicTest`)

```elixir
  test "subscription filters parse shared and ordinary forms through one grammar" do
    assert Topic.parse_filter("a/+") == {:ordinary, "a/+"}
    assert Topic.parse_filter("$share") == {:ordinary, "$share"}
    assert Topic.parse_filter("$share/g/a") == {:shared, "g", "a"}
    assert Topic.parse_filter("$share/g/#") == {:shared, "g", "#"}
    assert Topic.parse_filter("$share/g/a/+/c") == {:shared, "g", "a/+/c"}
    assert Topic.parse_filter("$share/温度/x") == {:shared, "温度", "x"}

    for filter <- [
          "$share//a",
          "$share/g",
          "$share/g/",
          "$share/g+/a",
          "$share/g#/a",
          "$share/g/a#",
          "",
          nil,
          42,
          <<255>>
        ] do
      assert Topic.parse_filter(filter) == :error, inspect(filter)
      refute Topic.valid_subscription_filter?(filter), inspect(filter)
    end

    assert Topic.valid_subscription_filter?("$share/g/a")
    assert Topic.valid_subscription_filter?("a/#")
    refute Topic.valid_filter?("$share/g/a")
  end

  test "shared matching applies the inner filter, including MQTT-4.7.2-1" do
    {:shared, "g", inner} = Topic.parse_filter("$share/g/#")
    assert Topic.matches?(inner, "a/b")
    refute Topic.matches?(inner, "$SYS/x")
    refute Topic.matches?("$share/g/#", "a/b")
  end
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/topic_test.exs
```

Expected: FAIL with `UndefinedFunctionError ... Topic.parse_filter/1 is undefined`.

- [ ] **Step 3: Implement**

Replace the moduledoc and `valid_filter?/1` clauses of `topic.ex` with:

```elixir
  @moduledoc """
  Pure MQTT 5.0 topic validation, subscription filter parsing and ordinary
  subscription matching.

  Names and filters are nonempty UTF-8 strings of at most 65,535 encoded
  bytes, without NUL. Topic levels, including empty levels, are compared
  exactly without Unicode normalization. `parse_filter/1` separates a Shared
  Subscription `$share/{ShareName}/{filter}` (MQTT-4.8.2-1/2) from an ordinary
  filter using the codec's single filter grammar. `valid_filter?/1` and
  `matches?/2` accept ordinary filters only; shared matching applies
  `matches?/2` to the parsed inner filter.
  """

  alias GravitonMQ.MQTT.Codec.{Operational, Wire}

  @type parsed :: {:ordinary, String.t()} | {:shared, String.t(), String.t()} | :error
```

and

```elixir
  @spec parse_filter(term()) :: parsed()
  def parse_filter(filter) when is_binary(filter) and byte_size(filter) in 1..65_535 do
    cond do
      not Operational.topic_filter?(filter) ->
        :error

      String.starts_with?(filter, "$share/") ->
        [name, inner] =
          filter |> binary_part(7, byte_size(filter) - 7) |> String.split("/", parts: 2)

        {:shared, name, inner}

      true ->
        {:ordinary, filter}
    end
  end

  def parse_filter(_filter), do: :error

  @spec valid_subscription_filter?(term()) :: boolean()
  def valid_subscription_filter?(filter), do: parse_filter(filter) != :error

  @spec valid_filter?(term()) :: boolean()
  def valid_filter?(filter), do: match?({:ordinary, _}, parse_filter(filter))
```

Keep `valid_name?/1`, `matches?/2` and the private helpers unchanged.

- [ ] **Step 4: Run tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test
```

Expected: `0 failures` (existing "reject shared subscriptions" vectors still hold).

- [ ] **Step 5: Format, compile, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/topic.ex apps/graviton_mq_mqtt/test/topic_test.exs
cd apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && git commit -m "feat(mqtt): parse shared subscription filters" -- apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/topic.ex apps/graviton_mq_mqtt/test/topic_test.exs
```

---

## Task 2 (spec T2): Pure `TopicAlias` helper

**Files:**
- Create: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/topic_alias.ex`
- Test: `apps/graviton_mq_mqtt/test/topic_alias_test.exs`

- [ ] **Step 1: Write the failing tests**

```elixir
defmodule GravitonMQ.MQTT.TopicAliasTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Packet.Publish
  alias GravitonMQ.MQTT.TopicAlias

  defp publish(topic, properties \\ []),
    do: %Publish{topic: topic, payload: "x", properties: properties}

  test "an absent alias leaves the packet and table unchanged" do
    table = TopicAlias.new(10, 0)
    packet = publish("a", user_property: {"k", "v"})
    assert {:ok, ^table, ^packet} = TopicAlias.resolve(table, packet)
  end

  test "alias zero or above the advertised maximum is invalid (U-055)" do
    for {maximum, value} <- [{10, 0}, {10, 11}, {0, 1}, {1, 2}] do
      assert {:error, :topic_alias_invalid} =
               TopicAlias.resolve(TopicAlias.new(maximum, 0), publish("a", topic_alias: value))
    end
  end

  test "an empty topic with an unmapped alias is unknown (U-056)" do
    assert {:error, :topic_alias_unknown} =
             TopicAlias.resolve(TopicAlias.new(10, 0), publish("", topic_alias: 3))
  end

  test "a topic with an alias sets then replaces the mapping; the property is removed" do
    table = TopicAlias.new(10, 0)

    assert {:ok, table, %Publish{topic: "a", properties: [user_property: {"k", "v"}]}} =
             TopicAlias.resolve(table, publish("a", topic_alias: 1, user_property: {"k", "v"}))

    assert {:ok, table, %Publish{topic: "a", properties: []}} =
             TopicAlias.resolve(table, publish("", topic_alias: 1))

    assert {:ok, table, %Publish{topic: "b"}} =
             TopicAlias.resolve(table, publish("b", topic_alias: 1))

    assert {:ok, _table, %Publish{topic: "b", properties: []}} =
             TopicAlias.resolve(table, publish("", topic_alias: 1))
  end

  test "every alias from one to the advertised maximum is accepted (MQTT-3.3.2-10/12)" do
    table =
      Enum.reduce(1..5, TopicAlias.new(5, 0), fn value, table ->
        assert {:ok, table, _} = TopicAlias.resolve(table, publish("t/#{value}", topic_alias: value))
        table
      end)

    assert map_size(table.inbound) == 5

    assert {:ok, _, %Publish{topic: "z"}} =
             TopicAlias.resolve(TopicAlias.new(65_535, 0), publish("z", topic_alias: 65_535))
  end

  test "outbound assignment is stable first-N and never exceeds its maximum (MQTT-3.3.2-9/11)" do
    table = TopicAlias.new(0, 2)
    {table, first} = TopicAlias.assign(table, publish("a"))
    assert {first.topic, first.properties} == {"a", [topic_alias: 1]}
    {table, again} = TopicAlias.assign(table, publish("a"))
    assert {again.topic, again.properties} == {"", [topic_alias: 1]}
    {table, second} = TopicAlias.assign(table, publish("b"))
    assert {second.topic, second.properties} == {"b", [topic_alias: 2]}
    {table, full} = TopicAlias.assign(table, publish("c"))
    assert {full.topic, full.properties} == {"c", []}
    {_table, reused} = TopicAlias.assign(table, publish("b"))
    assert {reused.topic, reused.properties} == {"", [topic_alias: 2]}
  end

  test "a zero outbound maximum or an existing alias property is left untouched" do
    table = TopicAlias.new(10, 0)
    packet = publish("a")
    assert {^table, ^packet} = TopicAlias.assign(table, packet)

    table = TopicAlias.new(0, 5)
    aliased = publish("a", topic_alias: 4)
    assert {^table, ^aliased} = TopicAlias.assign(table, aliased)
  end
end
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/topic_alias_test.exs
```

Expected: FAIL with `module GravitonMQ.MQTT.TopicAlias is not available`.

- [ ] **Step 3: Implement `topic_alias.ex`**

```elixir
defmodule GravitonMQ.MQTT.TopicAlias do
  @moduledoc """
  Pure MQTT 5.0 Topic Alias tables for one Network Connection.

  A table lives exactly as long as its Network Connection and is never
  persisted or carried to another connection (MQTT-3.3.2-7). `resolve/2`
  applies §3.3.4 inbound rules and removes the hop-local `topic_alias`
  property. `assign/2` applies the "stable first-N" outbound policy: the first
  `outbound_max` distinct topics receive aliases 1..N, which are never
  replaced. The caller records an assigned mapping only when it emits the
  aliased packet.
  """

  alias GravitonMQ.MQTT.Packet.Publish

  defstruct inbound: %{}, inbound_max: 0, outbound: %{}, outbound_max: 0

  @type t :: %__MODULE__{
          inbound: %{pos_integer() => String.t()},
          inbound_max: 0..65_535,
          outbound: %{String.t() => pos_integer()},
          outbound_max: 0..65_535
        }

  @spec new(0..65_535, 0..65_535) :: t()
  def new(inbound_max, outbound_max)
      when inbound_max in 0..65_535 and outbound_max in 0..65_535,
      do: %__MODULE__{inbound_max: inbound_max, outbound_max: outbound_max}

  @spec resolve(t(), Publish.t()) ::
          {:ok, t(), Publish.t()} | {:error, :topic_alias_invalid | :topic_alias_unknown}
  def resolve(%__MODULE__{} = table, %Publish{properties: properties} = packet) do
    case Keyword.fetch(properties, :topic_alias) do
      :error ->
        {:ok, table, packet}

      {:ok, value} when not is_integer(value) or value < 1 or value > table.inbound_max ->
        {:error, :topic_alias_invalid}

      {:ok, value} ->
        properties = Keyword.delete(properties, :topic_alias)

        case packet.topic do
          "" ->
            case Map.fetch(table.inbound, value) do
              {:ok, topic} -> {:ok, table, %{packet | topic: topic, properties: properties}}
              :error -> {:error, :topic_alias_unknown}
            end

          topic ->
            {:ok, %{table | inbound: Map.put(table.inbound, value, topic)},
             %{packet | properties: properties}}
        end
    end
  end

  @spec assign(t(), Publish.t()) :: {t(), Publish.t()}
  def assign(%__MODULE__{} = table, %Publish{topic: topic, properties: properties} = packet) do
    cond do
      Keyword.has_key?(properties, :topic_alias) ->
        {table, packet}

      Map.has_key?(table.outbound, topic) ->
        {table, %{packet | topic: "", properties: properties ++ [topic_alias: table.outbound[topic]]}}

      map_size(table.outbound) < table.outbound_max ->
        value = map_size(table.outbound) + 1

        {%{table | outbound: Map.put(table.outbound, topic, value)},
         %{packet | properties: properties ++ [topic_alias: value]}}

      true ->
        {table, packet}
    end
  end
end
```

- [ ] **Step 4: Run tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/topic_alias_test.exs
```

Expected: `7 tests, 0 failures`.

- [ ] **Step 5: Format, compile, architecture check, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/topic_alias.ex apps/graviton_mq_mqtt/test/topic_alias_test.exs
cd apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix graviton_mq.check_architecture
git add apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/topic_alias.ex apps/graviton_mq_mqtt/test/topic_alias_test.exs
git commit -m "feat(mqtt): add pure per-connection topic alias tables" -- apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/topic_alias.ex apps/graviton_mq_mqtt/test/topic_alias_test.exs
```

Expected: architecture check prints no violation. (If it fails to compile an S2 file, follow Execution rule 4.)

---

## Task 3 (spec T3, part 1): Broker alias limits, CONNACK and runtime switches

**Files:**
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex` (`@defaults`, `new/1`, `connection_open`, `attach/4` CONNACK)
- Modify: `apps/graviton_mq_mqtt/test/broker_test.exs:87`
- Create: `apps/graviton_mq_mqtt/test/broker_alias_test.exs`
- Modify: `apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/config.ex`
- Test: `apps/graviton_mq_runtime/test/mqtt/config_test.exs`

- [ ] **Step 1: Create `broker_alias_test.exs` with helpers and failing limit tests**

The alias list already names modules first used in Tasks 4–5 (`Checkpoint`, `Packet`, `Disconnect`, `Publish`, `Puback`) and helpers used later (`packet/4`, `subscribe/3`, `publishes/2`, `disconnected?/3`); the resulting "unused" test warnings disappear in Task 5. `mix compile --warnings-as-errors` does not compile test files, so it is unaffected.

```elixir
defmodule GravitonMQ.MQTT.BrokerAliasTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.{Broker, Broker.Checkpoint}
  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Disconnect, Publish, Puback, Subscribe}

  defp new(options \\ []) do
    {:ok, state} = Broker.new(options)
    state
  end

  defp allowed(state, effects, now \\ 0) do
    Enum.reduce(effects, {state, []}, fn
      {:authorize, op, request, _deadline}, {state, effects} ->
        result = if is_list(request), do: Enum.map(request, fn _ -> :ok end), else: {:ok, :user}

        {state, more} =
          Broker.transition(
            state,
            {:auth_result, op, Map.fetch!(state.operations, op).generation, result, now}
          )

        {state, effects ++ more}

      effect, {state, effects} ->
        {state, effects ++ [effect]}
    end)
    |> then(fn {state, effects} ->
      if Enum.any?(effects, &match?({:authorize, _, _, _}, &1)),
        do: allowed(state, effects, now),
        else: {state, effects}
    end)
  end

  defp attach_as(state, id, client_id, options \\ []) do
    {state, []} = Broker.transition(state, {:connection_open, id, 1, 0})

    connect = %Connect{
      version: :v5,
      client_id: client_id,
      clean_start: Keyword.get(options, :clean_start, true),
      properties: Keyword.get(options, :properties, [])
    }

    {state, effects} = Broker.transition(state, {:packet, id, 1, connect, <<16, 0>>, 0})
    allowed(state, effects)
  end

  defp attach(state, id, options \\ []), do: attach_as(state, id, to_string(id), options)

  defp packet(state, id, packet, now \\ 1) do
    {state, effects} = Broker.transition(state, {:packet, id, 1, packet, <<1, 2>>, now})
    allowed(state, effects, now)
  end

  defp subscribe(state, id, filters) do
    packet(state, id, %Subscribe{packet_id: rem(state.next_id, 65_535) + 1, subscriptions: filters})
  end

  defp publishes(effects, id), do: for({:send, ^id, _, _, %Publish{} = p} <- effects, do: p)

  defp disconnected?(effects, id, code),
    do: Enum.any?(effects, &match?({:send, ^id, _, _, %Disconnect{reason_code: ^code}}, &1))

  test "CONNACK advertises the configured inbound Topic Alias Maximum" do
    for {options, expected} <- [
          {[], 10},
          {[topic_alias_maximum: 0], 0},
          {[topic_alias_maximum: 65_535, max_alias_bytes: 9_000_000_000_000], 65_535}
        ] do
      {_state, effects} = attach(new(options), :one)

      assert [{:send, :one, 1, _, %Connack{properties: properties}}] =
               Enum.filter(effects, &match?({:send, _, _, _, %Connack{}}, &1))

      assert properties[:topic_alias_maximum] == expected
    end
  end

  test "alias limits are validated and statically bounded by max_alias_bytes" do
    assert {:error, :invalid_options} = Broker.new(topic_alias_maximum: 65_536)
    assert {:error, :invalid_options} = Broker.new(max_outbound_topic_aliases: 65_536)
    assert {:error, :invalid_options} = Broker.new(max_alias_bytes: 1)
    assert {:ok, _} = Broker.new(topic_alias_maximum: 0, max_alias_bytes: 0)

    bounded = [max_connections: 1, topic_alias_maximum: 1, max_packet_size: 100]
    assert {:ok, _} = Broker.new(bounded ++ [max_alias_bytes: 100])
    assert {:error, :invalid_options} = Broker.new(bounded ++ [max_alias_bytes: 99])
  end
end
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/broker_alias_test.exs
```

Expected: FAIL (`topic_alias_maximum` is 0; unknown option keys return `{:error, :invalid_options}` for valid inputs).

- [ ] **Step 3: Implement in `broker.ex`**

1. Aliases: change line 15 to `alias GravitonMQ.MQTT.{Topic, TopicAlias}`.
2. Add to `@defaults` (after `dispatch_retry_interval: 1_000`):

```elixir
    dispatch_retry_interval: 1_000,
    topic_alias_maximum: 10,
    max_outbound_topic_aliases: 0,
    # Static worst case: max_connections x (inbound + outbound aliases) x
    # min(65_535, max_packet_size) bytes. Never enforced by refusing an alias
    # (MQTT-3.3.2-12). 128 MiB admits the defaults (83,884,800 bytes).
    max_alias_bytes: 134_217_728
```

3. In `new/1`, extend the inner condition:

```elixir
      if limits.max_packet_size in 1..268_435_460 and limits.receive_maximum in 1..65_535 and
           limits.max_inflight in 1..65_535 and limits.maximum_qos in 0..2 and
           limits.auth_timeout > 0 and
           limits.dispatch_retry_interval > 0 and
           limits.topic_alias_maximum in 0..65_535 and
           limits.max_outbound_topic_aliases in 0..65_535 and
           limits.max_connections *
             (limits.topic_alias_maximum + limits.max_outbound_topic_aliases) *
             min(65_535, limits.max_packet_size) <= limits.max_alias_bytes do
```

4. In `transition(state, {:connection_open, ...})`, add `aliases: TopicAlias.new(0, 0)` to the `conn` map (after `held_bytes: 0`).
5. In `attach/4`, extend the `conn` built before `put_conn`:

```elixir
          conn = %{
            state.connections[id]
            | phase: :connected,
              client_id: client_id,
              held: [],
              held_bytes: 0,
              aliases:
                TopicAlias.new(
                  state.limits.topic_alias_maximum,
                  min(
                    Keyword.get(packet.properties, :topic_alias_maximum, 0),
                    state.limits.max_outbound_topic_aliases
                  )
                )
          }
```

6. In the CONNACK `properties` list replace `topic_alias_maximum: 0,` with `topic_alias_maximum: state.limits.topic_alias_maximum,`.

- [ ] **Step 4: Update the historical CONNACK assertion**

In `apps/graviton_mq_mqtt/test/broker_test.exs` line 87 replace `assert properties[:topic_alias_maximum] == 0` with `assert properties[:topic_alias_maximum] == 10`.

- [ ] **Step 5: Run MQTT tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test
```

Expected: `0 failures`.

- [ ] **Step 6: Failing runtime config tests** (append to `R/test/mqtt/config_test.exs`)

```elixir
  test "Topic Alias limits pass through and are validated by the machine" do
    assert {:ok, config} =
             Config.build(
               auth: :anonymous,
               topic_alias_maximum: 0,
               max_outbound_topic_aliases: 4,
               max_alias_bytes: 134_217_728
             )

    assert config.machine_options[:topic_alias_maximum] == 0
    assert config.machine_options[:max_outbound_topic_aliases] == 4

    for options <- [
          [topic_alias_maximum: -1],
          [topic_alias_maximum: 65_536],
          [max_outbound_topic_aliases: 65_536],
          [max_alias_bytes: 0],
          [max_connections: 4_096, topic_alias_maximum: 65_535]
        ] do
      assert {:error, :invalid_mqtt_options} = Config.build([auth: :anonymous] ++ options),
             inspect(options)
    end
  end
```

Run:

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/mqtt/config_test.exs
```

Expected: FAIL (`typo`-style rejection of the new keys).

- [ ] **Step 7: Implement in `R/lib/graviton_mq/runtime/mqtt/config.ex`**

1. Append `topic_alias_maximum max_outbound_topic_aliases max_alias_bytes` to `@machine_keys` (before the closing `)a`).
2. Add the alias import: `alias GravitonMQ.MQTT.Broker, as: Machine` (below the existing `alias GravitonMQ.MQTT.Codec.Limits`).
3. In `valid_machine_limits?/1` add before the generic `{:ok, value}` clause:

```elixir
        {:ok, value} when key in [:topic_alias_maximum, :max_outbound_topic_aliases] ->
          is_integer(value) and value in 0..65_535
```

4. In `build/1`'s final validation, add a machine check as the last conjunct:

```elixir
           is_integer(config.max_writer_bytes) and config.max_writer_bytes > 0 and
           match?({:ok, _}, Machine.new(config.machine_options)),
```

- [ ] **Step 8: Run runtime MQTT tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/mqtt
```

Expected: `0 failures`.

- [ ] **Step 9: Format, compile, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/test/broker_alias_test.exs apps/graviton_mq_mqtt/test/broker_test.exs apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/config.ex apps/graviton_mq_runtime/test/mqtt/config_test.exs
for app in graviton_mq_mqtt graviton_mq_runtime; do (cd apps/$app && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors) || exit 1; done
git add apps/graviton_mq_mqtt/test/broker_alias_test.exs
git commit -m "feat(mqtt): advertise configurable broker topic alias maximum" -- apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/test/broker_alias_test.exs apps/graviton_mq_mqtt/test/broker_test.exs apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/config.ex apps/graviton_mq_runtime/test/mqtt/config_test.exs
```

---

## Task 4 (spec T3, part 2): Broker inbound alias resolution

**Files:**
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex` (`incoming_publish/5`)
- Test: `apps/graviton_mq_mqtt/test/broker_alias_test.exs`

- [ ] **Step 1: Append failing tests to `broker_alias_test.exs`**

```elixir
  test "inbound aliases resolve before every PUBLISH check and never reach subscribers" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :sub)
    {state, _} = subscribe(state, :sub, [{"a/#", 2}])

    {state, first} =
      packet(state, :pub, %Publish{topic: "a/b", payload: "0", properties: [topic_alias: 1]})

    {state, second} =
      packet(state, :pub, %Publish{
        topic: "",
        payload: "1",
        qos: 1,
        packet_id: 1,
        properties: [topic_alias: 1]
      })

    {state, third} =
      packet(state, :pub, %Publish{
        topic: "",
        payload: "2",
        qos: 2,
        packet_id: 2,
        properties: [topic_alias: 1]
      })

    received = publishes(first ++ second ++ third, :sub)
    assert Enum.map(received, &{&1.topic, &1.payload}) == [{"a/b", "0"}, {"a/b", "1"}, {"a/b", "2"}]
    refute Enum.any?(received, &Keyword.has_key?(&1.properties, :topic_alias))
    assert Enum.any?(second, &match?({:send, :pub, 1, _, %Puback{packet_id: 1, reason_code: 0}}, &1))
    assert state.connections[:pub].aliases.inbound == %{1 => "a/b"}
  end

  test "stored messages keep the resolved topic and re-encoded bytes without the alias" do
    {state, _} = attach(new(), :pub)

    {state, _} =
      packet(state, :pub, %Publish{topic: "r/t", payload: "x", retain: true, properties: [topic_alias: 2]})

    {state, _} =
      packet(state, :pub, %Publish{topic: "", payload: "y", retain: true, properties: [topic_alias: 2]})

    message = state.retained["r/t"]
    assert message.payload == "y"
    refute Keyword.has_key?(message.properties, :topic_alias)
    assert {:ok, %Publish{topic: "r/t", properties: []}, <<>>} = Packet.decode(message.encoded, :v5)
    assert {:ok, _snapshot} = Checkpoint.capture(state, 0, 100_000)
  end

  test "out-of-range aliases disconnect with 0x94 and unknown aliases with 0x82" do
    for {options, publication, code} <- [
          {[], %Publish{topic: "a", payload: "x", properties: [topic_alias: 11]}, 0x94},
          {[topic_alias_maximum: 0], %Publish{topic: "a", payload: "x", properties: [topic_alias: 1]},
           0x94},
          {[], %Publish{topic: "", payload: "x", properties: [topic_alias: 3]}, 0x82}
        ] do
      {state, _} = attach(new(options), :pub)
      {state, effects} = packet(state, :pub, publication)
      assert disconnected?(effects, :pub, code), inspect({options, publication})
      refute Map.has_key?(state.connections, :pub)
    end
  end

  test "alias mappings belong to one Network Connection (MQTT-3.3.2-7)" do
    {state, _} = attach(new(), :pub, properties: [session_expiry_interval: 60])
    {state, _} = packet(state, :pub, %Publish{topic: "a", payload: "x", properties: [topic_alias: 1]})
    {state, _} = Broker.transition(state, {:connection_down, :pub, 1, :abnormal, 2})

    {state, _} =
      attach_as(state, :pub_again, "pub",
        clean_start: false,
        properties: [session_expiry_interval: 60]
      )

    {_state, effects} =
      packet(state, :pub_again, %Publish{topic: "", payload: "y", properties: [topic_alias: 1]})

    assert disconnected?(effects, :pub_again, 0x82)
  end

  test "a DUP retransmission in alias form is compared after resolution" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :sub)
    {state, _} = subscribe(state, :sub, [{"d", 1}])
    first = %Publish{topic: "d", payload: "x", qos: 1, packet_id: 5, properties: [topic_alias: 1]}
    {state, pending} = Broker.transition(state, {:packet, :pub, 1, first, <<1, 2>>, 1})
    duplicate = %Publish{first | topic: "", dup: true}
    assert {state, []} = Broker.transition(state, {:packet, :pub, 1, duplicate, <<1, 2>>, 2})
    {_state, effects} = allowed(state, pending, 3)
    assert [%Publish{topic: "d", payload: "x"}] = publishes(effects, :sub)
  end

  test "packets held during CONNECT authentication resolve aliases in arrival order" do
    {state, _} = attach(new(), :sub)
    {state, _} = subscribe(state, :sub, [{"h", 0}])
    {state, []} = Broker.transition(state, {:connection_open, :pub, 1, 0})
    connect = %Connect{version: :v5, client_id: "pub", clean_start: true}

    {state, [{:authorize, op, _, _}]} =
      Broker.transition(state, {:packet, :pub, 1, connect, <<16, 0>>, 0})

    first = %Publish{topic: "h", payload: "1", properties: [topic_alias: 1]}
    second = %Publish{topic: "", payload: "2", properties: [topic_alias: 1]}
    {state, []} = Broker.transition(state, {:packet, :pub, 1, first, <<1, 2>>, 0})
    {state, []} = Broker.transition(state, {:packet, :pub, 1, second, <<1, 2>>, 0})

    {state, effects} = Broker.transition(state, {:auth_result, op, 1, {:ok, :user}, 1})
    {_state, effects} = allowed(state, effects, 1)
    assert Enum.map(publishes(effects, :sub), &{&1.topic, &1.payload}) == [{"h", "1"}, {"h", "2"}]
  end

  test "a resolved publication larger than Maximum Packet Size disconnects with 0x95" do
    {state, _} = attach(new(max_packet_size: 64), :pub)
    topic = String.duplicate("t", 40)
    {state, _} = packet(state, :pub, %Publish{topic: topic, payload: "", properties: [topic_alias: 1]})

    {_state, effects} =
      packet(state, :pub, %Publish{
        topic: "",
        payload: String.duplicate("p", 40),
        properties: [topic_alias: 1]
      })

    assert disconnected?(effects, :pub, 0x95)
  end
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/broker_alias_test.exs
```

Expected: the new tests FAIL (every alias currently disconnects with 0x94).

- [ ] **Step 3: Implement**

Rename the existing `defp incoming_publish(state, id, packet, bytes, now)` to `defp resolved_publish(state, id, packet, bytes, now)` and delete its first `cond` branch (`Keyword.has_key?(packet.properties, :topic_alias) -> disconnect(... 0x94 ...)`). Insert above it:

```elixir
  # §3.3.4: resolve the Topic Alias first, so DUP comparison, authorization,
  # routing, retained storage and checkpoints only see the full topic. The
  # stored encoding is the resolved packet; no alias value reaches durable state.
  defp incoming_publish(state, id, packet, bytes, now) do
    conn = state.connections[id]

    case TopicAlias.resolve(conn.aliases, packet) do
      {:error, :topic_alias_invalid} ->
        disconnect(state, id, :topic_alias_invalid, 0x94, now)

      {:error, :topic_alias_unknown} ->
        disconnect(state, id, :topic_alias_unknown, 0x82, now)

      {:ok, aliases, ^packet} ->
        state |> put_conn(id, %{conn | aliases: aliases}) |> resolved_publish(id, packet, bytes, now)

      {:ok, aliases, resolved} ->
        state = put_conn(state, id, %{conn | aliases: aliases})

        case Packet.encode(resolved, state.codec) do
          {:ok, encoded} -> resolved_publish(state, id, resolved, encoded, now)
          {:error, _} -> disconnect(state, id, :resolved_packet_too_large, 0x95, now)
        end
    end
  end
```

- [ ] **Step 4: Run all MQTT tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test
```

Expected: `0 failures`.

- [ ] **Step 5: Format, compile, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/test/broker_alias_test.exs
(cd apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors)
git commit -m "feat(mqtt): resolve inbound topic aliases in the broker" -- apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/test/broker_alias_test.exs
```

---

## Task 5 (spec T3, part 3): Broker outbound aliases

**Files:**
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex` (`deliver_authorized/4` network clause)
- Test: `apps/graviton_mq_mqtt/test/broker_alias_test.exs`

- [ ] **Step 1: Append failing tests**

```elixir
  defp publish_all(state, id, topics) do
    Enum.reduce(topics, {state, []}, fn topic, {state, acc} ->
      {state, effects} = packet(state, id, %Publish{topic: topic, payload: topic})
      {state, acc ++ effects}
    end)
  end

  test "outbound aliases are bounded by the client maximum and the broker cap (MQTT-3.1.2-26/27)" do
    {state, _} = attach(new(max_outbound_topic_aliases: 2), :pub)
    {state, _} = attach(state, :sub, properties: [topic_alias_maximum: 5])
    {state, _} = subscribe(state, :sub, [{"o/#", 0}])
    {_state, effects} = publish_all(state, :pub, ["o/a", "o/a", "o/b", "o/c", "o/c", "o/b"])

    assert Enum.map(publishes(effects, :sub), &{&1.topic, &1.properties[:topic_alias]}) ==
             [{"o/a", 1}, {"", 1}, {"o/b", 2}, {"o/c", nil}, {"o/c", nil}, {"", 2}]
  end

  test "the default broker cap and a client maximum of zero never send aliases" do
    for {options, properties} <- [
          {[], [topic_alias_maximum: 5]},
          {[max_outbound_topic_aliases: 5], []}
        ] do
      {state, _} = attach(new(options), :pub)
      {state, _} = attach(state, :sub, properties: properties)
      {state, _} = subscribe(state, :sub, [{"o/#", 0}])
      {_state, effects} = publish_all(state, :pub, ["o/a", "o/a"])

      assert Enum.map(publishes(effects, :sub), &{&1.topic, &1.properties[:topic_alias]}) ==
               [{"o/a", nil}, {"o/a", nil}]
    end
  end

  test "a reconnect starts a new outbound table and retransmits the full topic" do
    {state, _} = attach(new(max_outbound_topic_aliases: 2), :pub)

    {state, _} =
      attach(state, :sub, properties: [topic_alias_maximum: 2, session_expiry_interval: 60])

    {state, _} = subscribe(state, :sub, [{"r", 1}])
    {state, _} = packet(state, :pub, %Publish{topic: "r", payload: "1", qos: 1, packet_id: 1})
    {state, _} = packet(state, :pub, %Publish{topic: "r", payload: "2", qos: 1, packet_id: 2})
    {state, _} = Broker.transition(state, {:connection_down, :sub, 1, :abnormal, 2})

    {_state, effects} =
      attach_as(state, :sub_again, "sub",
        clean_start: false,
        properties: [topic_alias_maximum: 2, session_expiry_interval: 60]
      )

    assert [
             %Publish{topic: "r", dup: true, properties: first},
             %Publish{topic: "", dup: true, properties: second}
           ] = publishes(effects, :sub_again)

    assert first[:topic_alias] == 1 and second[:topic_alias] == 1
  end

  test "a new alias that would exceed the peer Maximum Packet Size is not assigned" do
    # The limit admits CONNACK/SUBACK and the full-topic PUBLISH, but not the
    # 3 extra bytes of a first alias assignment.
    payload = String.duplicate("a", 100)
    {:ok, bytes} = Packet.encode(%Publish{topic: "s", payload: payload})
    {state, _} = attach(new(max_outbound_topic_aliases: 2), :pub)

    {state, _} =
      attach(state, :sub,
        properties: [topic_alias_maximum: 2, maximum_packet_size: byte_size(bytes)]
      )

    {state, _} = subscribe(state, :sub, [{"s", 0}])
    {state, effects} = packet(state, :pub, %Publish{topic: "s", payload: payload})
    assert [%Publish{topic: "s", properties: []}] = publishes(effects, :sub)
    assert state.connections[:sub].aliases.outbound == %{}
  end
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/broker_alias_test.exs
```

Expected: FAIL on the first and third new tests (no alias is ever sent).

- [ ] **Step 3: Implement in `deliver_authorized/4` (network clause, `true ->` branch)**

Replace the block from `packet = recipient_packet(entry, packet_id, now)` to the end of the `if outbound_fits?` success branch with:

```elixir
        packet_id = if entry.qos > 0, do: entry.packet_id || free_packet_id(session), else: nil
        packet = recipient_packet(entry, packet_id, now)

        # D2: deliverable iff the full topic fits (a reconnect retransmits it
        # with an empty table); the alias form is used only if it also fits.
        {aliases, wire} = TopicAlias.assign(conn.aliases, packet)

        {aliases, wire} =
          if outbound_fits?(state, conn, wire), do: {aliases, wire}, else: {conn.aliases, packet}

        if outbound_fits?(state, conn, packet) do
          {state, effects} =
            send_packet(state, id, wire, {:delivery, op.recipient, entry.id, entry.qos}, now)

          if current?(state, id, generation) and Map.has_key?(state.sessions, op.recipient) do
            entry = %{
              entry
              | packet_id: packet_id,
                sending: true,
                ever_sent: true,
                dispatch: nil,
                retry_at: nil
            }

            state = put_delivery(state, :network, op.recipient, entry)
            session = state.sessions[op.recipient]
            conn = state.connections[id]
            quota = if entry.qos > 0, do: conn.send_quota - 1, else: conn.send_quota
            state = put_conn(state, id, %{conn | send_quota: quota, aliases: aliases})
```

and keep the following `state = put_session(state, op.recipient, %{session | next_packet_id: ...})`, `pump_recipient` and `else` branches unchanged. (The former `state = if entry.qos > 0, do: put_conn(... send_quota - 1), else: state` is replaced by the two lines above.)

- [ ] **Step 4: Run all MQTT tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test
```

Expected: `0 failures`.

- [ ] **Step 5: Format, compile, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/test/broker_alias_test.exs
(cd apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors)
git commit -m "feat(mqtt): assign bounded outbound topic aliases in the broker" -- apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/test/broker_alias_test.exs
```

---

## Task 6 (spec T6, part 1): Snapshot v2 record shape and v1 read-time upgrade

**Files:**
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex` (`commit_subscribe_snapshot/3` subscription record, `install_targets/2` entry)
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker/checkpoint.ex`
- Modify: `apps/graviton_mq_mqtt/test/broker_checkpoint_test.exs:210-211`
- Modify: `apps/graviton_mq_runtime/test/mqtt/persistence_test.exs`

- [ ] **Step 1: Write failing pure tests** (append to `broker_checkpoint_test.exs`; replace `BASE` with the hash from Task 0 Step 1)

```elixir
  # Written by Checkpoint v1 at commit BASE (Task 0 of the S4 plan).
  @v1_fixture Path.expand("fixtures/broker_checkpoint_v1.etf", __DIR__)

  test "a v1 snapshot written before S4 recovers under v2 and is rewritten as v2" do
    {:ok, snapshot} =
      CheckpointCodec.decode(File.read!(@v1_fixture), max_bytes: 1_048_576, max_items: 500_000)

    assert snapshot.version == 1
    assert {:ok, recovered} = Checkpoint.restore(new(), snapshot, 1_000, 100_100)

    assert %{subscription_identifier: nil, shared: nil, options: %{qos: 1}} =
             recovered.sessions["reader"].subscriptions["fixture/#"]

    assert [%{subscription_identifiers: [], shared: nil, message: %{topic: "fixture/a"}}] =
             recovered.sessions["reader"].deliveries

    assert recovered.retained["fixture/a"].payload == "v1-content"
    assert {:ok, again} = Checkpoint.capture(recovered, 1_000, 100_100)
    assert again.version == 2
    assert {:ok, _} = Checkpoint.restore(new(), again, 2_000, 100_200)
  end

  test "only exact v1 shapes are upgraded; mislabelled or unknown versions fail closed" do
    {machine, _} = connect(new(), :reader)
    {machine, _} = packet(machine, :reader, %Subscribe{packet_id: 1, subscriptions: [{"a", 1}]}, 0)
    assert {:ok, v2} = Checkpoint.capture(machine, 0, 100_000)
    assert v2.version == 2

    for forged <- [%{v2 | version: 1}, %{v2 | version: 3}] do
      assert {:error, :invalid_broker_checkpoint} =
               Checkpoint.restore(new(), forged, 1_000, 100_000)
    end
  end
```

In the existing test "rejects schema corruption ..." change `%{snapshot | version: 2}` to `%{snapshot | version: 3}`.

- [ ] **Step 2: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/broker_checkpoint_test.exs
```

Expected: FAIL (restored subscription lacks `:subscription_identifier`; capture version is 1).

- [ ] **Step 3: Machine record shape in `broker.ex`**

In `commit_subscribe_snapshot/3` replace `sub = %{id: state.next_id, options: %{opts | qos: code}}` with:

```elixir
              sub = %{
                id: state.next_id,
                options: %{opts | qos: code},
                subscription_identifier: nil,
                shared: nil
              }
```

In `install_targets/2` add to the `entry` map (after `retry_at: nil`):

```elixir
        retry_at: nil,
        subscription_identifiers: Map.get(target, :subscription_identifiers, []),
        shared: Map.get(target, :shared)
```

- [ ] **Step 4: Checkpoint v2 in `checkpoint.ex`**

1. Replace `@delivery` and add the new attributes:

```elixir
  @subscription ~w(id options subscription_identifier shared)a
  @delivery ~w(id message qos phase release_order retain packet_id sending ever_sent delivered dispatch retry_at subscription_identifiers shared)a
  @v1_subscription ~w(id options)a
  @v1_delivery ~w(id message qos phase release_order retain packet_id sending ever_sent delivered dispatch retry_at)a
```

(The literals `:subscription_identifier`, `:subscription_identifiers` and `:shared` must stay in this module: the runtime decodes ETF with `[:safe]` after `Code.ensure_loaded?(Checkpoint)`.)

2. In `capture/3` change `version: 1,` to `version: 2,`.
3. In `restore/4` make the first `with` clause upgrade before validating:

```elixir
    with snapshot = upgrade(snapshot),
         :ok <- validate(snapshot, template),
         true <- wall >= snapshot.at_wall do
```

4. In `validate/2` replace `snapshot.version == 1` with `snapshot.version == 2`.
5. Replace `subscription?/3` and extend `delivery?/3`:

```elixir
  defp subscription?(filter, sub, next) do
    Topic.valid_subscription_filter?(filter) and keys?(sub, @subscription) and positive?(sub.id) and
      sub.id < next and subscription_identifier?(sub.subscription_identifier) and
      sub.shared == shared_of(filter) and
      keys?(sub.options, [:qos, :no_local, :retain_as_published, :retain_handling]) and
      sub.options.qos in 0..2 and is_boolean(sub.options.no_local) and
      is_boolean(sub.options.retain_as_published) and sub.options.retain_handling in 0..2 and
      (sub.shared == nil or sub.options.no_local == false)
  end
```

and replace `delivery?/3` with:

```elixir
  defp delivery?(entry, next, template) do
    keys?(entry, @delivery) and positive?(entry.id) and entry.id < next and
      message?(entry.message, template) and entry.qos in [1, 2] and is_boolean(entry.retain) and
      entry.phase in [:publish, :pubrel] and
      ((entry.phase == :publish and entry.release_order == nil) or
         (entry.phase == :pubrel and entry.qos == 2 and packet_id?(entry.packet_id) and
            positive?(entry.release_order) and entry.release_order < next)) and
      (entry.packet_id == nil or packet_id?(entry.packet_id)) and
      is_boolean(entry.ever_sent) and (entry.packet_id == nil or entry.ever_sent) and
      entry.sending == false and entry.delivered == false and entry.dispatch == nil and
      entry.retry_at == nil and is_list(entry.subscription_identifiers) and
      Enum.all?(entry.subscription_identifiers, &(&1 != nil and subscription_identifier?(&1))) and
      (entry.shared == nil or match?({:shared, _, _}, Topic.parse_filter(entry.shared)))
  end
```

6. Add private helpers (above `defp rebase_message/2`):

```elixir
  # Owner decision Q9: v1 is upgraded in memory at read time; the next commit
  # writes v2. Only an exact v1 shape is upgraded, so mislabelled data keeps
  # version 1 and fails validation.
  defp upgrade(%{version: 1, state: %{sessions: sessions} = data} = snapshot)
       when is_map(sessions) do
    if Enum.all?(sessions, fn {_id, session} -> v1_session?(session) end) do
      sessions =
        Map.new(sessions, fn {id, session} ->
          subscriptions =
            Map.new(session.subscriptions, fn {filter, sub} ->
              {filter, Map.merge(sub, %{subscription_identifier: nil, shared: nil})}
            end)

          deliveries =
            Enum.map(session.deliveries, &Map.merge(&1, %{subscription_identifiers: [], shared: nil}))

          {id, %{session | subscriptions: subscriptions, deliveries: deliveries}}
        end)

      %{snapshot | version: 2, state: %{data | sessions: sessions}}
    else
      snapshot
    end
  end

  defp upgrade(snapshot), do: snapshot

  defp v1_session?(%{subscriptions: subscriptions, deliveries: deliveries})
       when is_map(subscriptions) and is_list(deliveries),
       do:
         Enum.all?(subscriptions, fn {_filter, sub} -> keys?(sub, @v1_subscription) end) and
           Enum.all?(deliveries, &keys?(&1, @v1_delivery))

  defp v1_session?(_session), do: false

  defp shared_of(filter) do
    case Topic.parse_filter(filter) do
      {:shared, name, inner} -> {name, inner}
      _ -> nil
    end
  end

  defp subscription_identifier?(nil), do: true
  defp subscription_identifier?(value), do: is_integer(value) and value in 1..268_435_455
```

- [ ] **Step 5: Run MQTT tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test
```

Expected: `0 failures`.

- [ ] **Step 6: Failing runtime v1 WAL-record recovery test** (append to `R/test/mqtt/persistence_test.exs`)

```elixir
  @v1_fixture Path.expand(
                "../../../graviton_mq_mqtt/test/fixtures/broker_checkpoint_v1.etf",
                __DIR__
              )

  test "a v1 snapshot record from before S4 recovers and is rewritten as v2", %{tmp_dir: dir} do
    config = %{config(dir, 1_048_576) | max_snapshot_bytes: 65_536}

    {:ok, wal} =
      WAL.start_link(
        path: dir,
        stream_id: config.stream_id,
        mode: :opaque,
        max_record_bytes: config.max_snapshot_bytes + 64,
        max_log_bytes: config.max_log_bytes,
        fault_injector: nil
      )

    {:ok, reference} = WAL.append_records(wal, [{"mqtt-broker-snapshot/v1", File.read!(@v1_fixture)}])
    {:ok, _durable} = WAL.sync(wal, reference)
    :ok = WAL.close(wal)

    {:ok, machine} = Machine.new()
    assert {:ok, persistence, recovered} = Persistence.open(config, machine, now(), wall())

    assert %{subscription_identifier: nil, shared: nil} =
             recovered.sessions["reader"].subscriptions["fixture/#"]

    assert {:ok, %{version: 2}} =
             GravitonMQ.Core.CheckpointCodec.decode(persistence.saved_bytes,
               max_bytes: 65_536,
               max_items: 500_000
             )

    assert :ok = Persistence.close(persistence)
  end
```

Run:

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/mqtt/persistence_test.exs
```

Expected: PASS already (the pure upgrade landed in Step 4). If it fails, the failure is a real regression: fix `checkpoint.ex`, not the test. The test is the explicit "v1 written by the current code recovers under v2" evidence at the WAL boundary (same record type `"mqtt-broker-snapshot/v1"`).

- [ ] **Step 7: Format, compile, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker/checkpoint.ex apps/graviton_mq_mqtt/test/broker_checkpoint_test.exs apps/graviton_mq_runtime/test/mqtt/persistence_test.exs
for app in graviton_mq_mqtt graviton_mq_runtime; do (cd apps/$app && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors) || exit 1; done
git commit -m "feat(mqtt): write broker snapshot v2 and upgrade v1 at read time" -- apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker/checkpoint.ex apps/graviton_mq_mqtt/test/broker_checkpoint_test.exs apps/graviton_mq_runtime/test/mqtt/persistence_test.exs
```

---

## Task 7 (spec T4 + T6 part 2): Broker Subscription Identifiers

**Files:**
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex`
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker/checkpoint.ex` (`capabilities/2`)
- Modify: `apps/graviton_mq_mqtt/test/broker_test.exs:86`
- Create: `apps/graviton_mq_mqtt/test/broker_subscription_identifier_test.exs`
- Modify: `apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/config.ex`, `apps/graviton_mq_runtime/test/mqtt/config_test.exs`

- [ ] **Step 1: Create the failing test file**

```elixir
defmodule GravitonMQ.MQTT.BrokerSubscriptionIdentifierTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.{Broker, Broker.Checkpoint}
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Disconnect, Publish, Subscribe}

  defp new(options \\ []) do
    {:ok, state} = Broker.new(options)
    state
  end

  defp allowed(state, effects, now \\ 0) do
    Enum.reduce(effects, {state, []}, fn
      {:authorize, op, request, _deadline}, {state, effects} ->
        result = if is_list(request), do: Enum.map(request, fn _ -> :ok end), else: {:ok, :user}

        {state, more} =
          Broker.transition(
            state,
            {:auth_result, op, Map.fetch!(state.operations, op).generation, result, now}
          )

        {state, effects ++ more}

      effect, {state, effects} ->
        {state, effects ++ [effect]}
    end)
    |> then(fn {state, effects} ->
      if Enum.any?(effects, &match?({:authorize, _, _, _}, &1)),
        do: allowed(state, effects, now),
        else: {state, effects}
    end)
  end

  defp attach_as(state, id, client_id, options \\ []) do
    {state, []} = Broker.transition(state, {:connection_open, id, 1, 0})

    connect = %Connect{
      version: :v5,
      client_id: client_id,
      clean_start: Keyword.get(options, :clean_start, true),
      properties: Keyword.get(options, :properties, [])
    }

    {state, effects} = Broker.transition(state, {:packet, id, 1, connect, <<16, 0>>, 0})
    allowed(state, effects)
  end

  defp attach(state, id, options \\ []), do: attach_as(state, id, to_string(id), options)

  defp packet(state, id, packet, now \\ 1) do
    {state, effects} = Broker.transition(state, {:packet, id, 1, packet, <<1, 2>>, now})
    allowed(state, effects, now)
  end

  defp subscribe(state, id, filters, identifier \\ nil) do
    properties = if identifier, do: [subscription_identifier: identifier], else: []

    packet(state, id, %Subscribe{
      packet_id: rem(state.next_id, 65_535) + 1,
      subscriptions: filters,
      properties: properties
    })
  end

  defp publishes(effects, id), do: for({:send, ^id, _, _, %Publish{} = p} <- effects, do: p)

  defp ids(%Publish{properties: properties}),
    do: for({:subscription_identifier, id} <- properties, do: id) |> Enum.sort()

  test "CONNACK advertises Subscription Identifiers and the switch restores 0xA1" do
    {_state, effects} = attach(new(), :one)
    [{:send, :one, 1, _, %Connack{properties: on}}] = Enum.filter(effects, &match?({:send, _, _, _, %Connack{}}, &1))
    assert on[:subscription_identifier_available] == 1

    {state, effects} = attach(new(subscription_identifiers: 0), :one)
    [{:send, :one, 1, _, %Connack{properties: off}}] = Enum.filter(effects, &match?({:send, _, _, _, %Connack{}}, &1))
    assert off[:subscription_identifier_available] == 0
    {state, effects} = subscribe(state, :one, [{"a", 1}], 7)
    assert Enum.any?(effects, &match?({:send, :one, 1, _, %Disconnect{reason_code: 0xA1}}, &1))
    refute Map.has_key?(state.connections, :one)
    assert {:error, :invalid_options} = Broker.new(subscription_identifiers: 2)
  end

  test "one copy carries the IDs of every matching subscription (MQTT-3.3.4-3/4)" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :sub)
    {state, _} = subscribe(state, :sub, [{"s/#", 1}], 7)
    {state, _} = subscribe(state, :sub, [{"s/+/x", 1}], 9)
    {state, effects} = packet(state, :pub, %Publish{topic: "s/a/x", payload: "1"})
    assert [copy] = publishes(effects, :sub)
    assert ids(copy) == [7, 9]

    {state, _} = subscribe(state, :sub, [{"s/+/x", 1}])
    {_state, effects} = packet(state, :pub, %Publish{topic: "s/a/x", payload: "2"})
    assert [copy] = publishes(effects, :sub)
    assert ids(copy) == [7]
  end

  test "identical identifiers are repeated and one SUBSCRIBE applies its ID to every filter" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :sub)
    {state, _} = subscribe(state, :sub, [{"i/#", 0}, {"i/+", 0}], 3)
    assert state.sessions["sub"].subscriptions["i/#"].subscription_identifier == 3
    assert state.sessions["sub"].subscriptions["i/+"].subscription_identifier == 3
    {_state, effects} = packet(state, :pub, %Publish{topic: "i/a", payload: "x"})
    assert [copy] = publishes(effects, :sub)
    assert ids(copy) == [3, 3]
  end

  test "retained-on-subscribe carries the subscribing filter's identifier" do
    {state, _} = attach(new(), :pub)
    {state, _} = packet(state, :pub, %Publish{topic: "r/x", payload: "kept", retain: true})
    {state, _} = attach(state, :sub)
    {_state, effects} = subscribe(state, :sub, [{"r/+", 1}], 11)
    assert [%Publish{retain: true} = copy] = publishes(effects, :sub)
    assert ids(copy) == [11]
  end

  test "inbound PUBLISH carrying an identifier is a protocol error (MQTT-3.3.4-6)" do
    {state, _} = attach(new(), :pub)

    {_state, effects} =
      packet(state, :pub, %Publish{topic: "a", payload: "x", properties: [subscription_identifier: 1]})

    assert Enum.any?(effects, &match?({:send, :pub, 1, _, %Disconnect{reason_code: 0x82}}, &1))
  end

  test "a retransmission after reconnect reuses the snapshotted identifiers" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :sub, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :sub, [{"q", 1}], 5)
    {state, _} = packet(state, :pub, %Publish{topic: "q", payload: "x", qos: 1, packet_id: 1})
    {state, _} = Broker.transition(state, {:connection_down, :sub, 1, :abnormal, 2})

    {_state, effects} =
      attach_as(state, :sub_again, "sub", clean_start: false, properties: [session_expiry_interval: 60])

    assert [%Publish{dup: true} = copy] = publishes(effects, :sub_again)
    assert ids(copy) == [5]
  end

  test "identifiers survive a checkpoint and fail closed when the feature is disabled" do
    {state, _} = attach(new(), :sub, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :sub, [{"c", 1}], 21)
    assert {:ok, snapshot} = Checkpoint.capture(state, 0, 100_000)
    assert {:ok, recovered} = Checkpoint.restore(new(), snapshot, 1_000, 100_100)
    assert recovered.sessions["sub"].subscriptions["c"].subscription_identifier == 21

    assert {:error, :checkpoint_capability_mismatch} =
             Checkpoint.restore(new(subscription_identifiers: 0), snapshot, 1_000, 100_100)
  end
end
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/broker_subscription_identifier_test.exs
```

Expected: FAIL (`subscription_identifiers` is an unknown option; SUBSCRIBE with ID disconnects 0xA1).

- [ ] **Step 3: Implement in `broker.ex`**

1. `@defaults`: add `subscription_identifiers: 1,`. In `new/1` add `and limits.subscription_identifiers in 0..1` to the inner condition.
2. CONNACK: `subscription_identifier_available: state.limits.subscription_identifiers,`.
3. `prepare_subscribe/4` first `cond` branch:

```elixir
      state.limits.subscription_identifiers == 0 and
          Keyword.has_key?(packet.properties, :subscription_identifier) ->
        disconnect(state, id, :subscription_identifiers_not_supported, 0xA1, now)
```

and add to the op data map passed to `prepare/4` (after `retained: retained,`):

```elixir
            subscription_identifier: Keyword.get(packet.properties, :subscription_identifier),
```

4. `commit_subscribe_snapshot/3`: `subscription_identifier: op.subscription_identifier,` in the `sub` map; in the retained `targets` map add

```elixir
                  subscription_identifiers:
                    subscription_ids(Enum.map(filters, &subscriptions[&1])),
```

5. `route/2` network target: add `subscription_identifiers: subscription_ids(Enum.map(matches, &elem(&1, 1))),`.
6. `refresh_target/2` network clause: add `subscription_identifiers: subscription_ids(surviving)` to the updated target.
7. `recipient_packet/3`: replace the final struct's `properties: properties` with
   `properties: properties ++ Enum.map(entry.subscription_identifiers, &{:subscription_identifier, &1})`.
8. Add the helper next to `options/1`:

```elixir
  defp subscription_ids(subscriptions),
    do: subscriptions |> Enum.map(& &1.subscription_identifier) |> Enum.reject(&is_nil/1)
```

- [ ] **Step 4: Capability mismatch in `checkpoint.ex`**

In `restore/4` add after `:ok <- validate(snapshot, template),`:

```elixir
         :ok <- capabilities(snapshot.state, template.limits),
```

and add:

```elixir
  # Owner decision Q10: persisted responsibility for a disabled feature fails
  # startup explicitly instead of being dropped.
  defp capabilities(data, limits) do
    subscriptions = for {_, session} <- data.sessions, {_, sub} <- session.subscriptions, do: sub
    entries = for {_, session} <- data.sessions, entry <- session.deliveries, do: entry

    if limits.subscription_identifiers == 0 and
         (Enum.any?(subscriptions, &(&1.subscription_identifier != nil)) or
            Enum.any?(entries, &(&1.subscription_identifiers != []))),
       do: {:error, :checkpoint_capability_mismatch},
       else: :ok
  end
```

- [ ] **Step 5: Update the historical CONNACK assertion**

`broker_test.exs` line 86: `assert properties[:subscription_identifier_available] == 1`.

- [ ] **Step 6: Run MQTT tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test
```

Expected: `0 failures`.

- [ ] **Step 7: Runtime switch — failing test** (append to `R/test/mqtt/config_test.exs`)

```elixir
  test "feature switches are booleans mapped to machine availability flags" do
    assert {:ok, config} = Config.build(auth: :anonymous, subscription_identifiers: false)
    assert config.machine_options[:subscription_identifiers] == 0
    assert {:ok, config} = Config.build(auth: :anonymous, subscription_identifiers: true)
    assert config.machine_options[:subscription_identifiers] == 1
    assert {:ok, config} = Config.build(auth: :anonymous)
    refute Keyword.has_key?(config.machine_options, :subscription_identifiers)

    assert {:error, :invalid_mqtt_options} =
             Config.build(auth: :anonymous, subscription_identifiers: 1)
  end
```

- [ ] **Step 8: Implement in `config.ex`**

1. Append `subscription_identifiers` to `@machine_keys`; add `@flag_keys ~w(subscription_identifiers)a` below `@machine_keys`.
2. In `valid_machine_limits?/1` add before the alias clause: `{:ok, value} when key in @flag_keys -> is_boolean(value)`.
3. Change `machine_options:` to

```elixir
        machine_options:
          options
          |> Keyword.take(@machine_keys)
          |> Keyword.put_new(:auth_timeout, 1_000)
          |> Enum.map(&machine_flag/1),
```

and add:

```elixir
  defp machine_flag({key, value}) when key in @flag_keys, do: {key, if(value, do: 1, else: 0)}
  defp machine_flag(option), do: option
```

- [ ] **Step 9: Run runtime MQTT tests, format, compile, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/mqtt
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker/checkpoint.ex apps/graviton_mq_mqtt/test/broker_subscription_identifier_test.exs apps/graviton_mq_mqtt/test/broker_test.exs apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/config.ex apps/graviton_mq_runtime/test/mqtt/config_test.exs
for app in graviton_mq_mqtt graviton_mq_runtime; do (cd apps/$app && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors) || exit 1; done
git add apps/graviton_mq_mqtt/test/broker_subscription_identifier_test.exs
git commit -m "feat(mqtt): support subscription identifiers in the broker" -- apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker/checkpoint.ex apps/graviton_mq_mqtt/test/broker_subscription_identifier_test.exs apps/graviton_mq_mqtt/test/broker_test.exs apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/config.ex apps/graviton_mq_runtime/test/mqtt/config_test.exs
```

Expected: `0 failures` before formatting.

---

## Task 8 (spec T5, part 1): Shared membership, routing tiers and the switch

**Files:**
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex`
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker/checkpoint.ex` (`capabilities/2`)
- Modify: `apps/graviton_mq_mqtt/test/broker_test.exs:85` and the test at line 598
- Create: `apps/graviton_mq_mqtt/test/broker_shared_test.exs`
- Modify: `apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/config.ex`, `apps/graviton_mq_runtime/test/mqtt/config_test.exs`
- Modify: `apps/graviton_mq/test/graviton_mq/mqtt_paho_test.exs` (SHARED_OFF keeps its meaning with the switch off)

- [ ] **Step 1: Create the failing test file**

```elixir
defmodule GravitonMQ.MQTT.BrokerSharedTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.{Broker, Broker.Checkpoint}
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Publish, Suback, Subscribe, Unsubscribe}

  defp new(options \\ []) do
    {:ok, state} = Broker.new(options)
    state
  end

  defp allowed(state, effects, now \\ 0) do
    Enum.reduce(effects, {state, []}, fn
      {:authorize, op, request, _deadline}, {state, effects} ->
        result = if is_list(request), do: Enum.map(request, fn _ -> :ok end), else: {:ok, :user}

        {state, more} =
          Broker.transition(
            state,
            {:auth_result, op, Map.fetch!(state.operations, op).generation, result, now}
          )

        {state, effects ++ more}

      effect, {state, effects} ->
        {state, effects ++ [effect]}
    end)
    |> then(fn {state, effects} ->
      if Enum.any?(effects, &match?({:authorize, _, _, _}, &1)),
        do: allowed(state, effects, now),
        else: {state, effects}
    end)
  end

  defp attach(state, id, options \\ []) do
    {state, []} = Broker.transition(state, {:connection_open, id, 1, 0})

    connect = %Connect{
      version: :v5,
      client_id: to_string(id),
      clean_start: true,
      properties: Keyword.get(options, :properties, [])
    }

    {state, effects} = Broker.transition(state, {:packet, id, 1, connect, <<16, 0>>, 0})
    allowed(state, effects)
  end

  defp packet(state, id, packet, now \\ 1) do
    {state, effects} = Broker.transition(state, {:packet, id, 1, packet, <<1, 2>>, now})
    allowed(state, effects, now)
  end

  defp subscribe(state, id, filters, properties \\ []) do
    packet(state, id, %Subscribe{
      packet_id: rem(state.next_id, 65_535) + 1,
      subscriptions: filters,
      properties: properties
    })
  end

  defp publishes(effects, id), do: for({:send, ^id, _, _, %Publish{} = p} <- effects, do: p)

  defp publish_all(state, payloads, qos \\ 0) do
    Enum.reduce(Enum.with_index(payloads, 1), {state, []}, fn {payload, index}, {state, acc} ->
      publication = %Publish{
        topic: "t",
        payload: payload,
        qos: qos,
        packet_id: if(qos == 0, do: nil, else: index)
      }

      {state, effects} = packet(state, :pub, publication)
      {state, acc ++ effects}
    end)
  end

  defp group(options \\ [], member \\ []) do
    {state, _} = attach(new(options), :pub)
    {state, _} = attach(state, :m1, member)
    {state, _} = attach(state, :m2, member)
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {state, _} = subscribe(state, :m2, [{"$share/g/t", 1}])
    state
  end

  test "CONNACK advertises shared subscriptions; the switch keeps DISCONNECT 0x9E" do
    {_state, effects} = attach(new(), :one)
    [{:send, :one, 1, _, %Connack{properties: on}}] = Enum.filter(effects, &match?({:send, _, _, _, %Connack{}}, &1))
    assert on[:shared_subscription_available] == 1
    assert {:error, :invalid_options} = Broker.new(shared_subscriptions: 2)
  end

  test "each message goes to exactly one member, round-robin over Client Identifiers" do
    {_state, effects} = publish_all(group(), ["1", "2", "3", "4"])
    assert Enum.map(publishes(effects, :m1), & &1.payload) == ["1", "3"]
    assert Enum.map(publishes(effects, :m2), & &1.payload) == ["2", "4"]
  end

  test "subscribe authorizes the full shared filter and sends no retained messages (§3.8.4)" do
    {state, _} = attach(new(), :pub)
    {state, _} = packet(state, :pub, %Publish{topic: "t", payload: "kept", retain: true})
    {state, _} = attach(state, :m1)
    subscribe = %Subscribe{packet_id: 1, subscriptions: [{"$share/g/t", %{qos: 1, retain_handling: 0}}]}
    {state, effects} = Broker.transition(state, {:packet, :m1, 1, subscribe, <<1, 2>>, 1})
    assert [{:authorize, _, [{:authorize, :user, :subscribe, "$share/g/t"}], _}] = effects
    {_state, effects} = allowed(state, effects, 1)
    assert Enum.any?(effects, &match?({:send, :m1, _, _, %Suback{reason_codes: [1]}}, &1))
    assert publishes(effects, :m1) == []
  end

  test "membership is keyed by session and full filter; UNSUBSCRIBE leaves the group" do
    state = group()
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 0}])
    assert map_size(state.sessions["m1"].subscriptions) == 1
    assert state.sessions["m1"].subscriptions["$share/g/t"].shared == {"g", "t"}
    {state, _} = packet(state, :m1, %Unsubscribe{packet_id: 77, filters: ["$share/g/t"]})
    {_state, effects} = publish_all(state, ["1", "2"])
    assert publishes(effects, :m1) == []
    assert Enum.map(publishes(effects, :m2), & &1.payload) == ["1", "2"]
  end

  test "a member's granted QoS and its own identifier apply; an overlapping ordinary subscription gets its own copy" do
    {state, _} = attach(new(maximum_qos: 1), :pub)
    {state, _} = attach(state, :m1)
    {state, effects} = subscribe(state, :m1, [{"$share/g/t", 2}], subscription_identifier: 4)
    assert Enum.any?(effects, &match?({:send, :m1, _, _, %Suback{reason_codes: [1]}}, &1))
    {state, _} = subscribe(state, :m1, [{"t", 0}], subscription_identifier: 8)
    {_state, effects} = packet(state, :pub, %Publish{topic: "t", payload: "x", qos: 1, packet_id: 1})
    copies = publishes(effects, :m1)
    assert Enum.sort(Enum.map(copies, &{&1.qos, &1.properties[:subscription_identifier]})) == [{0, 8}, {1, 4}]
  end

  test "a member without send quota is skipped while another can send" do
    state = group([], properties: [receive_maximum: 1])
    {_state, effects} = publish_all(state, ["1", "2", "3"], 1)
    assert Enum.map(publishes(effects, :m1), & &1.payload) == ["1"]
    assert Enum.map(publishes(effects, :m2), & &1.payload) == ["2"]
  end

  test "an oversized member is skipped and a QoS0 message nobody can receive is discarded" do
    # 64 bytes admits CONNACK and SUBACK but not a 100-byte payload.
    large = String.duplicate("p", 100)
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [maximum_packet_size: 64])
    {state, _} = attach(state, :m2)
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 0}])
    {state, _} = subscribe(state, :m2, [{"$share/g/t", 0}])
    {_state, effects} = publish_all(state, [large])
    assert publishes(effects, :m1) == []
    assert [%Publish{payload: ^large}] = publishes(effects, :m2)

    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [maximum_packet_size: 64])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 0}])
    {state, effects} = publish_all(state, [large])
    assert publishes(effects, :m1) == []
    assert state.sessions["m1"].deliveries == []
  end

  test "QoS1 binds to an offline persistent member; QoS0 is not queued offline" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    {state, _} = publish_all(state, ["q0"], 0)
    assert state.sessions["m1"].deliveries == []
    {state, _} = publish_all(state, ["q1"], 1)
    assert [%{shared: "$share/g/t", qos: 1, message: %{payload: "q1"}}] = state.sessions["m1"].deliveries
  end

  test "shared memberships and entries survive a checkpoint and fail closed when disabled" do
    {state, _} = attach(new(), :pub)
    {state, _} = attach(state, :m1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    {state, _} = publish_all(state, ["kept"], 1)
    assert {:ok, snapshot} = Checkpoint.capture(state, 0, 100_000)
    assert {:ok, recovered} = Checkpoint.restore(new(), snapshot, 1_000, 100_100)
    assert recovered.sessions["m1"].subscriptions["$share/g/t"].shared == {"g", "t"}
    assert [%{shared: "$share/g/t"}] = recovered.sessions["m1"].deliveries

    assert {:error, :checkpoint_capability_mismatch} =
             Checkpoint.restore(new(shared_subscriptions: 0), snapshot, 1_000, 100_100)
  end

  test "a forged shared field that disagrees with its filter is rejected" do
    {state, _} = attach(new(), :m1, properties: [session_expiry_interval: 60])
    {state, _} = subscribe(state, :m1, [{"$share/g/t", 1}])
    {:ok, snapshot} = Checkpoint.capture(state, 0, 100_000)
    forged = put_in(snapshot, [:state, :sessions, "m1", :subscriptions, "$share/g/t", :shared], nil)

    assert {:error, :invalid_broker_checkpoint} =
             Checkpoint.restore(new(), forged, 1_000, 100_100)
  end
end
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/broker_shared_test.exs
```

Expected: FAIL (`shared_subscriptions` unknown option; shared SUBSCRIBE disconnects 0x9E).

- [ ] **Step 3: Implement in `broker.ex`**

1. `@defaults`: add `shared_subscriptions: 1,`; `new/1`: add `and limits.shared_subscriptions in 0..1`.
2. `defstruct`: add `share_cursor: %{},` and `shared_discarded: 0,` (transient; never captured).
3. CONNACK: `shared_subscription_available: state.limits.shared_subscriptions,`.
4. `prepare_subscribe/4` shared branch becomes:

```elixir
      state.limits.shared_subscriptions == 0 and
          Enum.any?(packet.subscriptions, &match?({<<"$share/", _::binary>>, _}, &1)) ->
        disconnect(state, id, :shared_subscriptions_not_supported, 0x9E, now)
```

5. `commit_subscribe_snapshot/3`: replace `not Topic.valid_filter?(filter) -> 0x9E` with `not Topic.valid_subscription_filter?(filter) -> 0x9E`, and `shared: nil` in `sub` with `shared: shared_of(filter)`.
6. `publication/7`: replace `targets = route(state, message)` with `{state, targets} = route(state, message, now)`.
7. `route/2` becomes `route/3`: keep the `network` and `host` computations (host targets also gain `subscription_identifiers: [], shared: nil`), then end with

```elixir
    {state, shared} = shared_targets(state, message, now)
    {state, Enum.sort_by(network ++ shared ++ host, &{&1.kind, &1.recipient})}
```

8. Add the shared routing helpers after `route/3`:

```elixir
  # MQTT 5 §4.8.2: one copy per matching Shared Subscription group, bound to a
  # member at publication time (Q5). Members are ordered by Client Identifier
  # after a transient round-robin cursor (Q4); the cursor is fairness state,
  # not responsibility, and is never captured.
  defp shared_targets(state, message, now) do
    state.sessions
    |> Enum.flat_map(fn {client_id, session} ->
      for {filter, %{shared: {_name, inner}} = sub} <- session.subscriptions,
          Topic.matches?(inner, message.topic),
          do: {filter, {client_id, sub}}
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce({state, []}, fn {filter, members}, {state, targets} ->
      members = Enum.sort_by(members, &elem(&1, 0))

      case select_member(state, filter, members, message, now, :all) do
        nil ->
          {state, targets}

        {client_id, sub} ->
          state = %{state | share_cursor: Map.put(state.share_cursor, filter, client_id)}
          {state, targets ++ [shared_target(state, filter, client_id, sub, message)]}
      end
    end)
  end

  defp shared_target(state, filter, client_id, sub, message) do
    session = state.sessions[client_id]

    %{
      kind: :network,
      recipient: client_id,
      token: session.token,
      principal: session.principal,
      incarnations: [{filter, sub.id}],
      message: message,
      shared: filter,
      qos: min(message.qos, sub.options.qos),
      retain: message.retain and sub.options.retain_as_published,
      subscription_identifiers: List.wrap(sub.subscription_identifier)
    }
  end

  # Tiers (spec §4.4): connected members that fit and can send now; other
  # connected members that fit; for QoS > 0 and tiers == :all, offline
  # persistent members within the offline limits (Q7).
  defp select_member(state, filter, members, message, now, tiers) do
    cursor = Map.get(state.share_cursor, filter)

    {later, earlier} =
      Enum.split_with(members, fn {client_id, _} -> cursor != nil and client_id > cursor end)

    ordered = later ++ earlier

    connected =
      Enum.filter(ordered, fn {client_id, sub} -> member_fits?(state, client_id, sub, message, now) end)

    ready =
      Enum.filter(connected, fn {client_id, sub} ->
        min(message.qos, sub.options.qos) == 0 or member_connection(state, client_id).send_quota > 0
      end)

    offline =
      if tiers == :all,
        do:
          Enum.filter(ordered, fn {client_id, sub} ->
            min(message.qos, sub.options.qos) > 0 and offline_member?(state, client_id, message)
          end),
        else: []

    List.first(ready) || List.first(connected) || List.first(offline)
  end

  defp member_connection(state, client_id) do
    with %{connection: {id, generation}} <- Map.get(state.sessions, client_id),
         %{generation: ^generation, phase: :connected} = conn <- Map.get(state.connections, id) do
      conn
    else
      _ -> nil
    end
  end

  defp member_fits?(state, client_id, sub, message, now) do
    case member_connection(state, client_id) do
      nil ->
        false

      conn ->
        qos = min(message.qos, sub.options.qos)

        entry = %{
          message: message,
          qos: qos,
          retain: message.retain and sub.options.retain_as_published,
          ever_sent: false,
          subscription_identifiers: List.wrap(sub.subscription_identifier)
        }

        outbound_fits?(state, conn, recipient_packet(entry, if(qos > 0, do: 1, else: nil), now))
    end
  end

  defp offline_member?(state, client_id, message) do
    session = state.sessions[client_id]

    offline =
      Enum.flat_map(state.sessions, fn {_id, session} ->
        if session.connection == nil, do: session.deliveries, else: []
      end)

    session.connection == nil and session.expiry > 0 and
      length(offline) + 1 <= state.limits.max_offline_deliveries and
      pending_bytes(offline) + message_bytes(message) <= state.limits.max_offline_bytes
  end

  defp shared_of(filter) do
    case Topic.parse_filter(filter) do
      {:shared, name, inner} -> {name, inner}
      _ -> nil
    end
  end
```

9. Remove the now-unused `defp route(state, message)` head (it is replaced by `route/3`).

- [ ] **Step 4: Capability clause in `checkpoint.ex`**

Replace `capabilities/2`'s final `if` with:

```elixir
    cond do
      limits.subscription_identifiers == 0 and
          (Enum.any?(subscriptions, &(&1.subscription_identifier != nil)) or
             Enum.any?(entries, &(&1.subscription_identifiers != []))) ->
        {:error, :checkpoint_capability_mismatch}

      limits.shared_subscriptions == 0 and
          (Enum.any?(subscriptions, &(&1.shared != nil)) or Enum.any?(entries, &(&1.shared != nil))) ->
        {:error, :checkpoint_capability_mismatch}

      true ->
        :ok
    end
```

- [ ] **Step 5: Update historical broker tests**

`broker_test.exs` line 85: `assert properties[:shared_subscription_available] == 1`. In the test "a shared subscription after advertising unavailable disconnects before authorization" (line 598) change `attach(new(), :one, 1, ...)` to `attach(new(shared_subscriptions: 0), :one, 1, ...)`.

- [ ] **Step 6: Run MQTT tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test
```

Expected: `0 failures`.

- [ ] **Step 7: Runtime switch**

In `R/.../mqtt/config.ex` append `shared_subscriptions` to `@machine_keys` and change `@flag_keys` to `~w(subscription_identifiers shared_subscriptions)a`. Append to the config test from Task 7:

```elixir
    assert {:ok, config} = Config.build(auth: :anonymous, shared_subscriptions: false)
    assert config.machine_options[:shared_subscriptions] == 0
    assert {:error, :invalid_mqtt_options} = Config.build(auth: :anonymous, shared_subscriptions: 0)
```

- [ ] **Step 8: Keep the Paho SHARED_OFF test meaningful (CI only)**

In `apps/graviton_mq/test/graviton_mq/mqtt_paho_test.exs` change `defp broker do` to

```elixir
  defp broker(extra \\ []) do
    start_supervised!(
      {GravitonMQ,
       name: nil,
       runtime_supervisor_name: nil,
       mqtt:
         [
           auth: :anonymous,
           host_principal: :host,
           tick_interval: 10,
           listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
         ] ++ extra}
    )
  end
```

and in the 0x9E test use `instance = broker(shared_subscriptions: false)`.

- [ ] **Step 9: Run, format, compile, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/mqtt
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker/checkpoint.ex apps/graviton_mq_mqtt/test/broker_shared_test.exs apps/graviton_mq_mqtt/test/broker_test.exs apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/config.ex apps/graviton_mq_runtime/test/mqtt/config_test.exs apps/graviton_mq/test/graviton_mq/mqtt_paho_test.exs
for app in graviton_mq_mqtt graviton_mq_runtime graviton_mq; do (cd apps/$app && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors) || exit 1; done
git add apps/graviton_mq_mqtt/test/broker_shared_test.exs
git commit -m "feat(mqtt): route shared subscriptions to one member per group" -- apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker/checkpoint.ex apps/graviton_mq_mqtt/test/broker_shared_test.exs apps/graviton_mq_mqtt/test/broker_test.exs apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/config.ex apps/graviton_mq_runtime/test/mqtt/config_test.exs apps/graviton_mq/test/graviton_mq/mqtt_paho_test.exs
```

Expected: runtime tests `0 failures`; `apps/graviton_mq` compiles (its MQTT test compiles only under `mix test`; it is excluded locally by `:paho_interop`).

---

## Task 9 (spec T5, part 2): Shared redistribution

**Files:**
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex` (`end_session/3`, `down_existing/4`, `commit_unsubscribe/4`, new `redistribute/4`, `reroute/5`, `movable?/2`)
- Create: `apps/graviton_mq_mqtt/test/broker_shared_redistribution_test.exs`

- [ ] **Step 1: Create the failing test file**

```elixir
defmodule GravitonMQ.MQTT.BrokerSharedRedistributionTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Broker
  alias GravitonMQ.MQTT.Packet.{Connect, Puback, Publish, Subscribe, Unsubscribe}

  defp allowed(state, effects, now) do
    Enum.reduce(effects, {state, []}, fn
      {:authorize, op, request, _deadline}, {state, effects} ->
        result = if is_list(request), do: Enum.map(request, fn _ -> :ok end), else: {:ok, :user}

        {state, more} =
          Broker.transition(
            state,
            {:auth_result, op, Map.fetch!(state.operations, op).generation, result, now}
          )

        {state, effects ++ more}

      effect, {state, effects} ->
        {state, effects ++ [effect]}
    end)
    |> then(fn {state, effects} ->
      if Enum.any?(effects, &match?({:authorize, _, _, _}, &1)),
        do: allowed(state, effects, now),
        else: {state, effects}
    end)
  end

  defp attach_as(state, id, client_id, properties, clean_start \\ true, now \\ 0) do
    {state, []} = Broker.transition(state, {:connection_open, id, 1, now})

    connect = %Connect{
      version: :v5,
      client_id: client_id,
      clean_start: clean_start,
      properties: properties
    }

    {state, effects} = Broker.transition(state, {:packet, id, 1, connect, <<16, 0>>, now})
    allowed(state, effects, now)
  end

  defp packet(state, id, packet, now \\ 1) do
    {state, effects} = Broker.transition(state, {:packet, id, 1, packet, <<1, 2>>, now})
    allowed(state, effects, now)
  end

  defp payloads(state, client_id),
    do: Enum.map(state.sessions[client_id].deliveries, & &1.message.payload) |> Enum.sort()

  # m1: x1 sent and unacknowledged, x3 bound but unsent.
  # m2: x2 sent and unacknowledged, x4 bound but unsent.
  defp two_members(m1_expiry, qos \\ [1, 1, 1, 1]) do
    {:ok, state} = Broker.new()
    {state, _} = attach_as(state, :pub, "pub", [])

    {state, _} =
      attach_as(state, :m1, "m1", receive_maximum: 1, session_expiry_interval: m1_expiry)

    {state, _} = attach_as(state, :m2, "m2", receive_maximum: 1, session_expiry_interval: 60)

    {state, _} =
      packet(state, :m1, %Subscribe{packet_id: 1, subscriptions: [{"$share/g/t", 2}]})

    {state, _} =
      packet(state, :m2, %Subscribe{packet_id: 1, subscriptions: [{"$share/g/t", 2}]})

    Enum.zip(["x1", "x2", "x3", "x4"], qos)
    |> Enum.with_index(1)
    |> Enum.reduce(state, fn {{payload, qos}, index}, state ->
      publication = %Publish{topic: "t", payload: payload, qos: qos, packet_id: index}
      {state, _} = packet(state, :pub, publication)
      state
    end)
  end

  test "the fixture binds two sent and two unsent entries across the group" do
    state = two_members(60)
    assert payloads(state, "m1") == ["x1", "x3"]
    assert payloads(state, "m2") == ["x2", "x4"]
    assert Enum.find(state.sessions["m1"].deliveries, &(&1.message.payload == "x1")).ever_sent
    refute Enum.find(state.sessions["m1"].deliveries, &(&1.message.payload == "x3")).ever_sent
  end

  test "connection loss moves only never-sent entries to a connected member (Q6)" do
    {state, _} = Broker.transition(two_members(60), {:connection_down, :m1, 1, :abnormal, 2})
    assert payloads(state, "m1") == ["x1"]
    assert payloads(state, "m2") == ["x2", "x3", "x4"]

    moved = Enum.find(state.sessions["m2"].deliveries, &(&1.message.payload == "x3"))
    assert %{packet_id: nil, ever_sent: false, shared: "$share/g/t", phase: :publish} = moved
  end

  # m2 drops first: its never-sent x4 moves to the still-connected m1. When m1
  # then drops no member is connected, so its never-sent entries stay.
  test "with no connected member a never-sent entry stays with the disconnected member" do
    state = two_members(60)
    {state, _} = Broker.transition(state, {:connection_down, :m2, 1, :abnormal, 2})
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 3})
    assert payloads(state, "m1") == ["x1", "x3", "x4"]
    assert payloads(state, "m2") == ["x2"]
  end

  for {name, trigger} <- [
        {"expiry-0 connection loss", :expiry_zero_loss},
        {"session expiry on tick", :expiry_tick},
        {"clean-start reconnect", :clean_start},
        {"expiry-0 takeover", :takeover}
      ] do
    @trigger trigger
    test "session end by #{name} moves all QoS1 entries to another member (§4.8.2)" do
      state = end_m1(@trigger)
      assert payloads(state, "m2") == ["x1", "x2", "x3", "x4"]
    end
  end

  defp end_m1(:expiry_zero_loss) do
    {state, _} = Broker.transition(two_members(0), {:connection_down, :m1, 1, :abnormal, 2})
    state
  end

  defp end_m1(:expiry_tick) do
    {state, _} = Broker.transition(two_members(1), {:connection_down, :m1, 1, :abnormal, 2})
    {state, _} = Broker.transition(state, {:tick, 5_000})
    state
  end

  defp end_m1(:clean_start) do
    {state, _} = Broker.transition(two_members(60), {:connection_down, :m1, 1, :abnormal, 2})
    {state, _} = attach_as(state, :m1_again, "m1", [session_expiry_interval: 60], true, 3)
    state
  end

  defp end_m1(:takeover) do
    {state, _} = attach_as(two_members(0), :m1_again, "m1", [], false, 3)
    state
  end

  test "an already-sent QoS2 entry is never sent to another member (MQTT-4.8.2-5)" do
    state = two_members(0, [2, 2, 1, 2])
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    refute "x1" in payloads(state, "m2")
    assert payloads(state, "m2") == ["x2", "x3", "x4"]
  end

  test "an entry nobody can take is deleted and counted" do
    {:ok, state} = Broker.new()
    {state, _} = attach_as(state, :pub, "pub", [])
    {state, _} = attach_as(state, :m1, "m1", receive_maximum: 1, session_expiry_interval: 0)
    {state, _} = packet(state, :m1, %Subscribe{packet_id: 1, subscriptions: [{"$share/g/t", 1}]})
    {state, _} = packet(state, :pub, %Publish{topic: "t", payload: "a", qos: 1, packet_id: 1})
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 2})
    refute Map.has_key?(state.sessions, "m1")
    assert state.shared_discarded == 1
  end

  test "UNSUBSCRIBE moves never-sent entries and keeps sent ones with the member" do
    state = two_members(60)
    {state, effects} = packet(state, :m1, %Unsubscribe{packet_id: 9, filters: ["$share/g/t"]}, 2)
    assert payloads(state, "m1") == ["x1"]
    assert payloads(state, "m2") == ["x2", "x3", "x4"]
    assert is_list(effects)
  end

  test "a negative PUBACK discards the entry and never re-routes it (MQTT-4.8.2-6)" do
    state = two_members(60)
    [x1] = Enum.filter(state.sessions["m1"].deliveries, &(&1.message.payload == "x1"))
    {state, _} = packet(state, :m1, %Puback{packet_id: x1.packet_id, reason_code: 0x80}, 2)
    {state, _} = Broker.transition(state, {:connection_down, :m1, 1, :abnormal, 3})
    refute "x1" in payloads(state, "m2")
  end
end
```

Trigger walk-through (for reviewers): with m1 expiry 60, connection loss first moves the never-sent `x3`; the later session end (tick expiry or clean-start reconnect) moves the sent QoS1 `x1`. With expiry 0 (loss or takeover) both move at session end.

- [ ] **Step 2: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/broker_shared_redistribution_test.exs
```

Expected: the fixture test passes; redistribution tests FAIL (entries stay with or vanish with m1).

- [ ] **Step 3: Implement redistribution in `broker.ex`**

Add after `end_session/3`:

```elixir
  # MQTT 5 §4.8.2 responsibility for shared entries of a member whose session
  # ends, whose connection is lost (spec MAY, Q6) or which leaves the group.
  # Pure state change; moved entries are dispatched by the next pump (D5).
  defp redistribute(state, client_id, trigger, now) do
    case Map.get(state.sessions, client_id) do
      nil ->
        state

      session ->
        {moving, staying} = Enum.split_with(session.deliveries, &movable?(&1, trigger))
        state = put_session(state, client_id, %{session | deliveries: staying})
        Enum.reduce(moving, state, &reroute(&2, &1, client_id, trigger, now))
    end
  end

  defp movable?(%{shared: nil}, _trigger), do: false
  # QoS1 always moves; QoS2 moves only if never sent (MQTT-4.8.2-5).
  defp movable?(entry, :session_end), do: entry.qos == 1 or not entry.ever_sent
  # Sent entries stay to resume with the same member (MQTT-4.8.2-4).
  defp movable?(entry, :connection_loss), do: not entry.ever_sent
  defp movable?(entry, {:unsubscribe, filters}), do: entry.shared in filters and not entry.ever_sent

  defp reroute(state, entry, from, trigger, now) do
    members =
      state.sessions
      |> Enum.flat_map(fn {client_id, session} ->
        case Map.get(session.subscriptions, entry.shared) do
          %{shared: {_, _}} = sub when client_id != from -> [{client_id, sub}]
          _ -> []
        end
      end)
      |> Enum.sort_by(&elem(&1, 0))

    tiers = if trigger == :connection_loss, do: :connected, else: :all

    case select_member(state, entry.shared, members, entry.message, now, tiers) do
      {client_id, sub} ->
        moved = %{
          entry
          | id: state.next_id,
            qos: min(entry.message.qos, sub.options.qos),
            retain: entry.message.retain and sub.options.retain_as_published,
            subscription_identifiers: List.wrap(sub.subscription_identifier),
            phase: :publish,
            release_order: nil,
            packet_id: nil,
            sending: false,
            ever_sent: false,
            dispatch: nil,
            retry_at: nil
        }

        session = state.sessions[client_id]

        state = %{
          state
          | next_id: state.next_id + 1,
            share_cursor: Map.put(state.share_cursor, entry.shared, client_id)
        }

        put_session(state, client_id, %{session | deliveries: session.deliveries ++ [moved]})

      nil when trigger == :connection_loss ->
        session = state.sessions[from]
        put_session(state, from, %{session | deliveries: session.deliveries ++ [entry]})

      nil ->
        %{state | shared_discarded: state.shared_discarded + 1}
    end
  end
```

Wire it in:

1. `end_session/3`, first line of the `session ->` branch: `state = redistribute(state, client_id, :session_end, now)`.
2. `down_existing/4`, replace the last line of the matching branch with

```elixir
        if session.expiry == 0,
          do: end_session(state, conn.client_id, now),
          else: {redistribute(state, conn.client_id, :connection_loss, now), []}
```

3. `commit_unsubscribe/4` becomes:

```elixir
  defp commit_unsubscribe(state, id, packet, now) do
    conn = state.connections[id]
    session = state.sessions[conn.client_id]

    codes =
      Enum.map(packet.filters, fn filter ->
        if Map.has_key?(session.subscriptions, filter), do: 0, else: 0x11
      end)

    removed =
      Enum.filter(packet.filters, &match?(%{shared: {_, _}}, Map.get(session.subscriptions, &1)))

    session = %{session | subscriptions: Map.drop(session.subscriptions, packet.filters)}
    state = put_session(state, conn.client_id, session)

    state =
      if removed == [],
        do: state,
        else: redistribute(state, conn.client_id, {:unsubscribe, removed}, now)

    {state, effects} =
      send_packet(
        state,
        id,
        %Unsuback{packet_id: packet.packet_id, reason_codes: codes},
        {:request_ack, packet.packet_id},
        now
      )

    {state, more} = if removed == [], do: {state, []}, else: pump(state, now)
    {state, effects ++ more}
  end
```

- [ ] **Step 4: Run all MQTT tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test
```

Expected: `0 failures`, including `broker_checkpoint_test.exs` (restore's `{machine, []} = Broker.transition(...)` loss replay must remain effect-free) and `broker_connect_hold_test.exs` (takeover paths).

- [ ] **Step 5: Format, compile, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/test/broker_shared_redistribution_test.exs
(cd apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors)
git add apps/graviton_mq_mqtt/test/broker_shared_redistribution_test.exs
git commit -m "feat(mqtt): redistribute shared subscription responsibility" -- apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/test/broker_shared_redistribution_test.exs
```

---

## Task 10 (spec Q14): No Local on a shared filter → DISCONNECT 0x82

**Files:**
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/codec/operational.ex` (`decode_options/3`)
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex` (`transition/2` `:protocol_error` clause)
- Test: `apps/graviton_mq_mqtt/test/protocol_error_reason_test.exs`

- [ ] **Step 1: Failing test** (append to `protocol_error_reason_test.exs`)

```elixir
  test "No Local on a shared subscription is a Protocol Error (MQTT-3.8.3-4)" do
    # SUBSCRIBE id 1, no properties, filter "$share/g/t", options QoS1 + No Local.
    filter = "$share/g/t"
    body = <<0, 1, 0, byte_size(filter)::16, filter::binary, 0b0000_0101>>
    wire = <<0x82, byte_size(body)>> <> body

    assert {:error, %{class: :malformed, reason: :shared_subscription_no_local} = error} =
             Packet.decode(wire, :v5)

    {state, effects} =
      Broker.transition(broker(), {:protocol_error, :peer, 1, {error.class, error.reason}, 2})

    assert state.connections == %{}
    assert Enum.any?(effects, &match?({:send, :peer, 1, _, %Disconnect{reason_code: 0x82}}, &1))
  end
```

Run:

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/protocol_error_reason_test.exs
```

Expected: FAIL (`reason: :invalid_subscription_options`).

- [ ] **Step 2: Implement**

In `operational.ex` `decode_options/3` replace the `if ... do ... else` head with:

```elixir
    cond do
      flags >>> 6 != 0 or qos == 3 or retain_handling == 3 ->
        Wire.error(context, Wire.value_class(context), :invalid_subscription_options)

      no_local and shared?(filter) ->
        Wire.error(context, Wire.value_class(context), :shared_subscription_no_local)

      true ->
        {:ok,
         %{
           qos: qos,
           no_local: no_local,
           retain_as_published: (flags &&& 8) != 0,
           retain_handling: retain_handling
         }}
    end
```

In `broker.ex` `transition(state, {:protocol_error, ...})`:

```elixir
    code =
      case reason do
        {:limit_exceeded, :packet_too_large} -> 0x95
        {_class, :shared_subscription_no_local} -> 0x82
        _ -> 0x81
      end
```

- [ ] **Step 3: Run all MQTT tests (including codec suites)**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test
```

Expected: `0 failures` (the 0..255 option-octet vector uses filter `"t"` and is unchanged; the encode-side shared No Local rejection stays `:invalid_value`).

- [ ] **Step 4: Format, compile, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/codec/operational.ex apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/test/protocol_error_reason_test.exs
(cd apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors)
git commit -m "fix(mqtt): report No Local on shared filters as a protocol error" -- apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/codec/operational.ex apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/broker.ex apps/graviton_mq_mqtt/test/protocol_error_reason_test.exs
```

---

## Task 11 (spec T7, part 1): Runtime status

**Files:**
- Modify: `apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/broker.ex` (`call(:status, ...)`)
- Test: covered by Task 12 `mqtt_feature_test.exs` "status reports groups, discards and alias tables"

- [ ] **Step 1: Implement** (the failing test is written in Task 12 Step 1; write it first if executing tasks out of order)

In `call(:status, _from, state)` add to the `status` map (after `prepared:`):

```elixir
      shared_groups:
        state.machine.sessions
        |> Enum.flat_map(fn {_, session} ->
          for {filter, %{shared: {_, _}}} <- session.subscriptions, do: filter
        end)
        |> Enum.uniq()
        |> length(),
      shared_discarded: state.machine.shared_discarded,
      topic_aliases: %{
        inbound:
          Enum.sum(for {_, %{aliases: aliases}} <- state.machine.connections, do: map_size(aliases.inbound)),
        outbound:
          Enum.sum(for {_, %{aliases: aliases}} <- state.machine.connections, do: map_size(aliases.outbound))
      },
```

- [ ] **Step 2: Compile**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/broker.ex
(cd apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors)
```

Commit together with Task 12 (the test lives there).

---

## Task 12 (spec T7, part 2): Raw-socket runtime evidence and durable recovery

**Files:**
- Create: `apps/graviton_mq/test/graviton_mq/mqtt_feature_test.exs`
- Create: `apps/graviton_mq/test/graviton_mq/mqtt_feature_persistence_test.exs`

- [ ] **Step 1: Create `mqtt_feature_test.exs`** (untagged: runs locally)

```elixir
defmodule GravitonMQ.MQTTBroker.FeatureTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.MQTT.Codec.Packet

  alias GravitonMQ.MQTT.Packet.{
    Connack,
    Connect,
    Disconnect,
    Puback,
    Pubcomp,
    Publish,
    Pubrec,
    Pubrel,
    Suback,
    Subscribe
  }

  @moduletag timeout: 30_000

  defp broker(options \\ []) do
    mqtt =
      Keyword.merge(
        [
          auth: :anonymous,
          host_principal: :host,
          tick_interval: 10,
          listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
        ],
        options
      )

    instance =
      start_supervised!({GravitonMQ, name: nil, runtime_supervisor_name: nil, mqtt: mqtt},
        id: make_ref()
      )

    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    {instance, port}
  end

  defp connect(port, client_id, properties \\ []) do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    on_exit(fn -> :gen_tcp.close(socket) end)

    send_packet(socket, %Connect{
      version: :v5,
      client_id: client_id,
      clean_start: true,
      properties: properties
    })

    [%Connack{reason_code: 0} = connack] = recv(socket, 1)
    {socket, connack}
  end

  # A fresh packet identifier per SUBSCRIBE: the broker releases an identifier
  # only after the SUBACK write result, so reuse could race into 0x82.
  defp subscribe(socket, filters, properties \\ []) do
    packet_id = rem(System.unique_integer([:positive]), 65_535) + 1

    send_packet(socket, %Subscribe{
      packet_id: packet_id,
      subscriptions: filters,
      properties: properties
    })

    [%Suback{packet_id: ^packet_id}] = recv(socket, 1)
  end

  defp send_packet(socket, packet) do
    {:ok, encoded} = Packet.encode(packet)
    :ok = :gen_tcp.send(socket, encoded)
  end

  defp recv(socket, count),
    do: recv(socket, count, Process.get({:mqtt_buffer, socket}, <<>>), [])

  defp recv(socket, 0, buffer, acc) do
    Process.put({:mqtt_buffer, socket}, buffer)
    Enum.reverse(acc)
  end

  defp recv(socket, count, buffer, acc) do
    case Packet.decode(buffer, :v5) do
      {:ok, packet, rest} ->
        recv(socket, count - 1, rest, [packet | acc])

      {:more, _} ->
        {:ok, bytes} = :gen_tcp.recv(socket, 0, 2_000)
        recv(socket, count, buffer <> bytes, acc)
    end
  end

  test "inbound aliases resolve for QoS0/1/2 and subscribers receive full topics" do
    {_instance, port} = broker()
    {sub, _} = connect(port, "raw-alias-sub")
    subscribe(sub, [{"alias/#", 2}])
    {pub, %Connack{properties: properties}} = connect(port, "raw-alias-pub")
    assert properties[:topic_alias_maximum] == 10

    send_packet(pub, %Publish{topic: "alias/t", payload: "0", properties: [topic_alias: 1]})
    send_packet(pub, %Publish{topic: "", payload: "1", qos: 1, packet_id: 1, properties: [topic_alias: 1]})
    assert [%Puback{packet_id: 1, reason_code: 0}] = recv(pub, 1)
    send_packet(pub, %Publish{topic: "", payload: "2", qos: 2, packet_id: 2, properties: [topic_alias: 1]})
    assert [%Pubrec{packet_id: 2}] = recv(pub, 1)
    send_packet(pub, %Pubrel{packet_id: 2})
    assert [%Pubcomp{packet_id: 2}] = recv(pub, 1)

    received = recv(sub, 3)
    assert Enum.sort(Enum.map(received, &{&1.topic, &1.payload})) ==
             [{"alias/t", "0"}, {"alias/t", "1"}, {"alias/t", "2"}]

    refute Enum.any?(received, &Keyword.has_key?(&1.properties, :topic_alias))
  end

  test "an out-of-range alias receives 0x94 and an unknown alias 0x82" do
    {_instance, port} = broker()

    for {publication, code} <- [
          {%Publish{topic: "a", payload: "x", properties: [topic_alias: 11]}, 0x94},
          {%Publish{topic: "", payload: "x", properties: [topic_alias: 4]}, 0x82}
        ] do
      {socket, _} = connect(port, "raw-alias-bad-#{code}")
      send_packet(socket, publication)
      assert [%Disconnect{reason_code: ^code}] = recv(socket, 1)
    end
  end

  test "configured outbound aliases are bounded by the subscriber's maximum" do
    {_instance, port} = broker(max_outbound_topic_aliases: 4)
    {sub, _} = connect(port, "raw-out-sub", topic_alias_maximum: 1)
    subscribe(sub, [{"o/#", 0}])
    {pub, _} = connect(port, "raw-out-pub")
    for topic <- ["o/a", "o/a", "o/b"], do: send_packet(pub, %Publish{topic: topic, payload: topic})

    assert Enum.map(recv(sub, 3), &{&1.topic, &1.properties[:topic_alias]}) ==
             [{"o/a", 1}, {"", 1}, {"o/b", nil}]
  end

  test "Subscription Identifiers of overlapping subscriptions arrive on one copy" do
    {_instance, port} = broker()
    {sub, _} = connect(port, "raw-sid-sub")
    subscribe(sub, [{"sid/#", 1}], subscription_identifier: 7)
    subscribe(sub, [{"sid/+/x", 1}], subscription_identifier: 9)
    {pub, _} = connect(port, "raw-sid-pub")
    send_packet(pub, %Publish{topic: "sid/a/x", payload: "x"})
    [%Publish{properties: properties}] = recv(sub, 1)
    assert Enum.sort(for {:subscription_identifier, id} <- properties, do: id) == [7, 9]
  end

  test "a shared group splits messages and status reports groups and alias tables" do
    {instance, port} = broker()
    {m1, _} = connect(port, "raw-m1")
    {m2, _} = connect(port, "raw-m2")
    subscribe(m1, [{"$share/g/s", 0}])
    subscribe(m2, [{"$share/g/s", 0}])
    {pub, _} = connect(port, "raw-share-pub")
    send_packet(pub, %Publish{topic: "s", payload: "seed", properties: [topic_alias: 3]})
    for payload <- ["1", "2", "3"], do: send_packet(pub, %Publish{topic: "s", payload: payload})
    first = Enum.map(recv(m1, 2), & &1.payload)
    second = Enum.map(recv(m2, 2), & &1.payload)
    assert Enum.sort(first ++ second) == ["1", "2", "3", "seed"]

    assert {:ok, %{shared_groups: 1, shared_discarded: 0, topic_aliases: %{inbound: 1, outbound: 0}}} =
             GravitonMQ.status(instance)
  end

  test "the shared switch and host API keep their documented boundaries" do
    {instance, port} = broker(shared_subscriptions: false, subscription_identifiers: false)
    {socket, %Connack{properties: properties}} = connect(port, "raw-off")
    assert properties[:shared_subscription_available] == 0
    assert properties[:subscription_identifier_available] == 0
    send_packet(socket, %Subscribe{packet_id: 1, subscriptions: [{"$share/g/a", 1}]})
    assert [%Disconnect{reason_code: 0x9E}] = recv(socket, 1)
    assert {:error, :invalid_subscription} = GravitonMQ.subscribe(instance, "$share/g/a", qos: 1)
  end

  test "No Local on a shared filter receives DISCONNECT 0x82" do
    {_instance, port} = broker()
    {socket, _} = connect(port, "raw-nolocal")
    filter = "$share/g/t"
    body = <<0, 1, 0, byte_size(filter)::16, filter::binary, 0b0000_0101>>
    :ok = :gen_tcp.send(socket, <<0x82, byte_size(body)>> <> body)
    assert [%Disconnect{reason_code: 0x82}] = recv(socket, 1)
  end
end
```

- [ ] **Step 2: Create `mqtt_feature_persistence_test.exs`**

```elixir
defmodule GravitonMQ.MQTTBroker.FeaturePersistenceTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.Core.CommitRef
  alias GravitonMQ.MQTT.Codec.Packet
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Publish, Suback, Subscribe}

  @moduletag timeout: 120_000

  setup do
    parent = Path.join(System.tmp_dir!(), "graviton-mqtt-feature-#{System.unique_integer([:positive])}")
    File.mkdir!(parent)
    on_exit(fn -> File.rm_rf!(parent) end)
    %{path: Path.join(parent, "broker")}
  end

  defp start(context, extra \\ []) do
    mqtt =
      [
        auth: :anonymous,
        host_principal: :host,
        auth_timeout: 3_000,
        tick_interval: 25,
        persistence: [directory: context.path, stream_id: "mqtt-feature"],
        listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
      ] ++ extra

    start_supervised({GravitonMQ, name: nil, runtime_supervisor_name: nil, mqtt: mqtt}, id: :broker)
  end

  defp port(instance) do
    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    port
  end

  defp connect(port, clean_start) do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    on_exit(fn -> :gen_tcp.close(socket) end)

    send_packet(socket, %Connect{
      version: :v5,
      client_id: "durable-member",
      clean_start: clean_start,
      properties: [session_expiry_interval: 600]
    })

    {socket, receive_packet(socket)}
  end

  defp send_packet(socket, packet) do
    {:ok, encoded} = Packet.encode(packet)
    :ok = :gen_tcp.send(socket, encoded)
  end

  # CONNACK and a resumed PUBLISH can share one TCP read; keep the remainder.
  defp receive_packet(socket) do
    buffer = Process.get({:mqtt_buffer, socket}, <<>>)

    case Packet.decode(buffer, :v5) do
      {:ok, packet, rest} ->
        Process.put({:mqtt_buffer, socket}, rest)
        packet

      {:more, _} ->
        {:ok, received} = :gen_tcp.recv(socket, 0, 5_000)
        Process.put({:mqtt_buffer, socket}, buffer <> received)
        receive_packet(socket)
    end
  end

  defp wait(instance, predicate, deadline \\ 50) do
    {:ok, status} = GravitonMQ.status(instance)

    cond do
      predicate.(status) -> status
      deadline == 0 -> flunk("status condition timed out: #{inspect(status)}")
      true -> Process.sleep(50) && wait(instance, predicate, deadline - 1)
    end
  end

  test "shared membership, its Subscription Identifier and offline responsibility survive an owner restart",
       context do
    {:ok, instance} = start(context)
    {socket, %Connack{reason_code: 0}} = connect(port(instance), true)

    send_packet(socket, %Subscribe{
      packet_id: 1,
      subscriptions: [{"$share/g/d", 1}],
      properties: [subscription_identifier: 5]
    })

    assert %Suback{reason_codes: [1]} = receive_packet(socket)
    :ok = :gen_tcp.close(socket)
    wait(instance, &(&1.connections == 0))

    assert {:ok, reference} = GravitonMQ.publish(instance, "d", "kept", qos: 1)

    assert_receive {:graviton_mq, _,
                    {:operation, ^reference, {:ok, {:accepted_durable, %CommitRef{}}}}},
                   10_000

    :ok = stop_supervised(:broker)
    {:ok, restarted} = start(context)
    assert %{shared_groups: 1} = wait(restarted, &(&1.shared_groups == 1))
    {socket, %Connack{session_present: true}} = connect(port(restarted), false)

    assert %Publish{topic: "d", payload: "kept", qos: 1, properties: properties} =
             receive_packet(socket)

    assert properties[:subscription_identifier] == 5
    :ok = :gen_tcp.close(socket)
    :ok = stop_supervised(:broker)

    assert {:error, reason} = start(context, shared_subscriptions: false)
    assert inspect(reason) =~ "checkpoint_capability_mismatch"
  end
end
```

- [ ] **Step 3: Run**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/graviton_mq/mqtt_feature_test.exs test/graviton_mq/mqtt_feature_persistence_test.exs
```

Expected: `8 tests, 0 failures` (if run before Task 11, the status test fails with a `MatchError` on `shared_groups`). On a contended disk the persistence test can take tens of seconds (ADR 0009 notes 57–418 ms synchronous writes); the 120 s module timeout covers it.

- [ ] **Step 4: Format, compile, commit (Tasks 11 + 12)**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq/test/graviton_mq/mqtt_feature_test.exs apps/graviton_mq/test/graviton_mq/mqtt_feature_persistence_test.exs apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/broker.ex
(cd apps/graviton_mq_runtime && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors)
git add apps/graviton_mq/test/graviton_mq/mqtt_feature_test.exs apps/graviton_mq/test/graviton_mq/mqtt_feature_persistence_test.exs
git commit -m "feat(runtime): report MQTT shared groups and alias tables in status" -- apps/graviton_mq_runtime/lib/graviton_mq/runtime/mqtt/broker.ex apps/graviton_mq/test/graviton_mq/mqtt_feature_test.exs apps/graviton_mq/test/graviton_mq/mqtt_feature_persistence_test.exs
```

---

## Task 13 (spec T8): Pure client aliases and options

**Files:**
- Modify: `apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/client.ex`
- Modify: `apps/graviton_mq_mqtt/test/client_test.exs:364`
- Create: `apps/graviton_mq_mqtt/test/client_alias_test.exs`

- [ ] **Step 1: Create the failing test file**

```elixir
defmodule GravitonMQ.MQTT.ClientAliasTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.MQTT.Client
  alias GravitonMQ.MQTT.Packet.{Connack, Connect, Disconnect, Publish}

  defp connect(properties \\ []),
    do: %Connect{version: :v5, client_id: "client", clean_start: true, keep_alive: 2, properties: properties}

  defp online(connack_properties, options \\ []) do
    {:ok, state} = Client.new(connect(Keyword.get(options, :connect_properties, [])), options)
    {state, [{:open, 1}]} = Client.transition(state, :start)
    {state, [{:send, %Connect{} = sent, :connect}]} = Client.transition(state, {:transport_up, 1, 0})

    {state, _effects} =
      Client.transition(
        state,
        {:packet, 1,
         %Connack{version: :v5, session_present: false, reason_code: 0, properties: connack_properties},
         1}
      )

    {state, sent}
  end

  defp deliver(state, packet, now) do
    {state, effects} = Client.transition(state, {:packet, state.generation, packet, now})
    {state, for({:notify, {:delivery, _, %Publish{} = p}} <- effects, do: p), effects}
  end

  defp sends(effects), do: for({:send, %Publish{} = p, _} <- effects, do: p)

  test "the inbound maximum is an option advertised in CONNECT; the default is zero" do
    {_state, sent} = online([])
    assert sent.properties[:topic_alias_maximum] == 0
    {_state, sent} = online([], topic_alias_maximum: 3)
    assert sent.properties[:topic_alias_maximum] == 3
    assert {:ok, _} = Client.new(connect(topic_alias_maximum: 1))
    assert {:error, :invalid_options} = Client.new(connect(), topic_alias_maximum: 65_536)
    assert {:error, :invalid_options} = Client.new(connect(), max_outbound_topic_aliases: -1)
  end

  test "inbound aliases resolve before delivery and the property is stripped (Q15)" do
    {state, _} = online([], topic_alias_maximum: 2)
    {state, _} = Client.transition(state, {:demand, 5, 2})
    {state, [first], _} = deliver(state, %Publish{topic: "a", payload: "1", properties: [topic_alias: 1]}, 3)
    {_state, [second], _} = deliver(state, %Publish{topic: "", payload: "2", properties: [topic_alias: 1]}, 4)
    assert {first.topic, first.properties} == {"a", []}
    assert {second.topic, second.properties} == {"a", []}
  end

  test "out-of-range aliases terminate with 0x94 and unknown aliases with 0x82" do
    for {packet, code} <- [
          {%Publish{topic: "a", payload: "x", properties: [topic_alias: 3]}, 0x94},
          {%Publish{topic: "", payload: "x", properties: [topic_alias: 2]}, 0x82}
        ] do
      {state, _} = online([], topic_alias_maximum: 2)
      {state, _, effects} = deliver(state, packet, 3)
      assert state.phase == :stopped
      assert {:send, %Disconnect{reason_code: code}, :disconnect} in effects
    end
  end

  test "inbound mappings are reset with each Network Connection (MQTT-3.3.2-7)" do
    {state, _} =
      online([], topic_alias_maximum: 2, reconnect_max: 1, connect_properties: [session_expiry_interval: 60])

    {state, _, _} = deliver(state, %Publish{topic: "a", payload: "1", properties: [topic_alias: 1]}, 3)
    {state, _} = Client.transition(state, {:transport_down, 1, :closed, 4})
    {state, _} = Client.transition(state, {:retry, 5})
    {state, _} = Client.transition(state, {:transport_up, 2, 6})

    {state, _} =
      Client.transition(state, {:packet, 2, %Connack{version: :v5, session_present: true, reason_code: 0}, 7})

    {state, _, effects} = deliver(state, %Publish{topic: "", payload: "2", properties: [topic_alias: 1]}, 8)
    assert state.phase == :stopped
    assert {:send, %Disconnect{reason_code: 0x82}, :disconnect} in effects
  end

  test "outbound aliases are bounded by min(CONNACK maximum, client cap) (MQTT-3.2.2-17/18)" do
    {state, _} = online([topic_alias_maximum: 2], max_outbound_topic_aliases: 5)

    {_state, effects} =
      Enum.reduce(Enum.with_index(["a", "a", "b", "c", "c"]), {state, []}, fn {topic, index}, {state, acc} ->
        {state, effects} =
          Client.transition(state, {:publish, {:op, index}, %Publish{topic: topic, payload: "x"}, 10 + index})

        {state, acc ++ effects}
      end)

    assert Enum.map(sends(effects), &{&1.topic, &1.properties[:topic_alias]}) ==
             [{"a", 1}, {"", 1}, {"b", 2}, {"c", nil}, {"c", nil}]
  end

  test "a peer maximum of zero, the default cap and caller aliases never alias" do
    for {connack, options} <- [{[], [max_outbound_topic_aliases: 5]}, {[topic_alias_maximum: 5], []}] do
      {state, _} = online(connack, options)
      {state, first} = Client.transition(state, {:publish, :one, %Publish{topic: "a", payload: "x"}, 10})
      {_state, second} = Client.transition(state, {:publish, :two, %Publish{topic: "a", payload: "x"}, 11})
      assert Enum.map(sends(first ++ second), & &1.topic) == ["a", "a"]
    end

    {state, _} = online([topic_alias_maximum: 5], max_outbound_topic_aliases: 5)

    assert {_state, [{:notify, {:operation, :op, {:error, :topic_aliases_unsupported}}}]} =
             Client.transition(state, {:publish, :op, %Publish{topic: "a", payload: "x", properties: [topic_alias: 1]}, 10})
  end

  test "stored outbound publications keep the full topic and resend it after reconnect" do
    {state, _} =
      online([topic_alias_maximum: 2], max_outbound_topic_aliases: 2, reconnect_max: 1, connect_properties: [session_expiry_interval: 60])

    {state, _} = Client.transition(state, {:publish, :one, %Publish{topic: "a", payload: "1", qos: 1}, 10})
    {state, _} = Client.transition(state, {:sent, 1, {:operation, :one}, :ok, 10})
    {state, effects} = Client.transition(state, {:publish, :two, %Publish{topic: "a", payload: "2", qos: 1}, 11})
    assert [%Publish{topic: ""}] = sends(effects)
    assert state.outbound[:two].packet.topic == "a"
    refute Keyword.has_key?(state.outbound[:two].packet.properties, :topic_alias)
    {state, _} = Client.transition(state, {:sent, 1, {:operation, :two}, :ok, 11})
    {state, _} = Client.transition(state, {:transport_down, 1, :closed, 12})
    {state, _} = Client.transition(state, {:retry, 13})
    {state, _} = Client.transition(state, {:transport_up, 2, 14})

    {_state, effects} =
      Client.transition(
        state,
        {:packet, 2,
         %Connack{version: :v5, session_present: true, reason_code: 0, properties: [topic_alias_maximum: 2]}, 15}
      )

    assert [%Publish{topic: "a", dup: true} | _] = sends(effects)
  end
end
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/client_alias_test.exs
```

Expected: FAIL (`Client.new(connect(topic_alias_maximum: 1))` returns `:topic_aliases_unsupported`; no aliases are sent).

- [ ] **Step 3: Implement in `client.ex`**

1. Aliases: `alias GravitonMQ.MQTT.TopicAlias` (after the codec alias). Add struct fields: `topic_alias_maximum: 0, max_outbound_topic_aliases: 0, aliases: %TopicAlias{},`.
2. `new/2`: after `receive_maximum = ...` add

```elixir
      topic_alias_maximum =
        Keyword.get(
          options,
          :topic_alias_maximum,
          Keyword.get(connect.properties, :topic_alias_maximum, 0)
        )
```

replace `properties = Keyword.put_new(properties, :topic_alias_maximum, 0)` with `properties = Keyword.put(properties, :topic_alias_maximum, topic_alias_maximum)`, and add to the struct literal:

```elixir
               topic_alias_maximum: topic_alias_maximum,
               max_outbound_topic_aliases: Keyword.get(options, :max_outbound_topic_aliases, 0),
               aliases: TopicAlias.new(topic_alias_maximum, 0),
```

3. `connect_profile/1`: delete the `Keyword.get(connect.properties, :topic_alias_maximum, 0) != 0 -> {:error, :topic_aliases_unsupported}` branch.
4. `options_valid/1`: before `retries = ...` add

```elixir
    aliases_valid? =
      Enum.all?([:topic_alias_maximum, :max_outbound_topic_aliases], fn key ->
        value = Keyword.get(options, key, 0)
        is_integer(value) and value in 0..65_535
      end)
```

and change the condition to `if values_valid? and aliases_valid? and is_integer(retries) and retries >= 0,`.
5. `transition(state, {:transport_up, ...})`: add `aliases: TopicAlias.new(state.topic_alias_maximum, 0),` to the updated state map.
6. `transport_loss/4`: change the state update to
   `state = %{state | ping_at: nil, disconnected_at: state.disconnected_at || now, aliases: TopicAlias.new(state.topic_alias_maximum, 0)}`.
7. `connected/3`: add to the `phase: :connected` state map

```elixir
              aliases:
                TopicAlias.new(
                  state.topic_alias_maximum,
                  min(peer.topic_alias_maximum, state.max_outbound_topic_aliases)
                ),
```

8. Replace `receive_packet(state, %Publish{} = packet, now)` with

```elixir
  defp receive_packet(state, %Publish{} = packet, now) do
    if valid_packet?(packet, state) do
      case TopicAlias.resolve(state.aliases, packet) do
        {:error, :topic_alias_invalid} ->
          terminal(state, :topic_alias_invalid, 0x94)

        {:error, :topic_alias_unknown} ->
          terminal(state, :topic_alias_unknown, 0x82)

        {:ok, aliases, resolved} ->
          state = %{state | aliases: aliases}

          cond do
            not valid_packet?(resolved, state) ->
              terminal(state, :resolved_packet_too_large, 0x95)

            Keyword.get(resolved.properties, :payload_format_indicator, 0) == 1 and
                not String.valid?(resolved.payload) ->
              terminal(state, :payload_format_invalid, 0x99)

            true ->
              incoming_publication(state, resolved, now)
          end
      end
    else
      terminal(state, :malformed_publish, 0x81)
    end
  end
```

9. Add the outbound helper next to `publication_token/2`:

```elixir
  # D2: admission checked the full packet; the alias form is sent only if it
  # also fits. Stored outbound packets always keep the full topic.
  defp outbound_wire(state, %Publish{} = packet) do
    {aliases, wire} = TopicAlias.assign(state.aliases, packet)

    if wire != packet and peer_fits?(state, wire),
      do: {%{state | aliases: aliases}, wire},
      else: {state, packet}
  end

  defp outbound_wire(state, packet), do: {state, packet}
```

10. `allocate_operation/5` success branch: bind the updated state, then

```elixir
        state = %{
          state
          | outbound: Map.put(state.outbound, ref, entry),
            next_packet_id: next_packet_id,
            next_order: state.next_order + 1,
            send_quota: quota
        }

        {state, wire} = outbound_wire(state, packet)
        {state, [{:send, wire, publication_token(ref, entry)}]}
```

11. `send_resumptions/2` `state.send_quota > 0` branch:

```elixir
          state = %{
            state
            | outbound: Map.put(state.outbound, ref, entry),
              send_quota: state.send_quota - 1
          }

          {state, wire} = outbound_wire(state, packet)
          {state, effects ++ [{:send, wire, publication_token(ref, entry)}]}
```

12. Update `client_test.exs` line 364: `assert {:ok, _} = Client.new(connect(topic_alias_maximum: 1))`.

- [ ] **Step 4: Run all MQTT tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test
```

Expected: `0 failures` (the historical "inbound aliases ... disconnect explicitly" vector keeps 0x94 because the default inbound maximum is 0).

- [ ] **Step 5: Format, compile, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/client.ex apps/graviton_mq_mqtt/test/client_alias_test.exs apps/graviton_mq_mqtt/test/client_test.exs
(cd apps/graviton_mq_mqtt && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors)
git add apps/graviton_mq_mqtt/test/client_alias_test.exs
git commit -m "feat(mqtt): support topic aliases in the pure client" -- apps/graviton_mq_mqtt/lib/graviton_mq/mqtt/client.ex apps/graviton_mq_mqtt/test/client_alias_test.exs apps/graviton_mq_mqtt/test/client_test.exs
```

---

## Task 14 (spec T9): Client owner options, Sub-ID option, persistent shared subscriptions

**Files:**
- Modify: `apps/graviton_mq_client/lib/graviton_mq/client/mqtt/config.ex`
- Modify: `apps/graviton_mq_client/lib/graviton_mq/client/mqtt.ex`
- Modify: `apps/graviton_mq_client/lib/graviton_mq/client/mqtt/checkpoint.ex`
- Modify: `apps/graviton_mq_client/test/mqtt_shared_subscription_test.exs`
- Modify: `apps/graviton_mq_client/test/mqtt_checkpoint_test.exs`

- [ ] **Step 1: Replace the first test of `mqtt_shared_subscription_test.exs`** and add Sub-ID / alias option tests

```elixir
  test "persistent shared subscriptions are captured, recovered and reported on session loss",
       context do
    directory = Path.join(context.tmp_dir, "store")
    {client, socket} = connected(id: :first, persistence: [directory: directory])

    assert {:ok, subscription} = MQTTClient.subscribe(client, "$share/workers/events/#", qos: 1)

    assert %Subscribe{packet_id: id, subscriptions: [{"$share/workers/events/#", %{qos: 1}}]} =
             receive_packet(socket)

    send_packet(socket, %Suback{packet_id: id, reason_codes: [1]})
    assert_receive {:graviton_mq, ^client, {:operation, ^subscription, {:ok, [1]}}}, 15_000
    refute_receive {:graviton_mq, ^client, {:storage_error, _}}, 100
    assert MQTTClient.status(client).phase == :connected
    :ok = stop_supervised(:first)

    {restarted, _socket} = connected(id: :second, persistence: [directory: directory])

    assert_receive {:graviton_mq, ^restarted,
                    {:subscriptions_lost, %{"$share/workers/events/#" => 1}}},
                   15_000
  end

  test "subscribe accepts a Subscription Identifier option and rejects a duplicate property" do
    {client, socket} = connected([])

    assert {:ok, subscription} =
             MQTTClient.subscribe(client, "ids/#", qos: 1, subscription_identifier: 42)

    assert %Subscribe{packet_id: id, properties: [subscription_identifier: 42]} =
             receive_packet(socket)

    send_packet(socket, %Suback{packet_id: id, reason_codes: [1]})
    assert_receive {:graviton_mq, ^client, {:operation, ^subscription, {:ok, [1]}}}, 1_000

    assert {:error, :invalid_options} =
             MQTTClient.subscribe(client, "ids/#",
               subscription_identifier: 1,
               properties: [subscription_identifier: 2]
             )
  end

  test "alias options are advertised and the wire property stays forbidden in :properties" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    base = [client_id: "alias-options", host: "127.0.0.1", port: port, observer: self(), keep_alive: 0]

    assert {:error, :invalid_options} =
             GravitonMQ.Client.MQTT.Config.build(base ++ [properties: [topic_alias_maximum: 1]])

    assert {:error, :invalid_options} =
             GravitonMQ.Client.MQTT.Config.build(base ++ [topic_alias_maximum: 65_536])

    _client =
      start_supervised!({MQTTClient, base ++ [topic_alias_maximum: 4, max_outbound_topic_aliases: 2]})

    {:ok, socket} = :gen_tcp.accept(listener, 5_000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    assert %GravitonMQ.MQTT.Packet.Connect{properties: properties} = receive_packet(socket)
    assert properties[:topic_alias_maximum] == 4
  end
```

Delete the old first test ("persistent shared subscriptions reject before admission ..."). The `connected/1` helper already merges options, so `id:` reaches the child spec.

- [ ] **Step 2: Add checkpoint tests** (append to `mqtt_checkpoint_test.exs`)

```elixir
  test "shared subscription filters are valid checkpoint content" do
    {:ok, fresh} = Machine.new(%Connect{version: :v5, clean_start: true, client_id: "one"})
    machine = %{fresh | subscriptions: %{"$share/g/a/#" => 1}}
    meta = %{next_operation: 1, deadlines: %{}, results: %{}}
    assert {:ok, snapshot} = Checkpoint.capture(machine, meta, 0, 1_000)
    assert snapshot.version == 1
    assert {:ok, recovered, _} = Checkpoint.restore(snapshot, fresh, 0, 1_000)
    assert recovered.subscriptions == %{"$share/g/a/#" => 1}
  end

  test "persisted publications never carry a hop-local Topic Alias" do
    {:ok, fresh} = Machine.new(%Connect{version: :v5, clean_start: true, client_id: "one"})
    meta = %{next_operation: 2, deadlines: %{1 => 9_000}, results: %{}}
    packet = %Publish{qos: 1, packet_id: 3, topic: "a", payload: "x", properties: [topic_alias: 1]}
    entry = %{ref: 1, kind: :publish, packet: packet, order: 1, sent: true, expiry_deadline: nil}
    machine = %{fresh | outbound: %{1 => entry}, next_order: 2}
    assert {:error, :invalid_checkpoint_schema} = Checkpoint.capture(machine, meta, 0, 1_000)
  end
```

- [ ] **Step 3: Run to verify failure**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/mqtt_shared_subscription_test.exs test/mqtt_checkpoint_test.exs
```

Expected: FAIL (`:shared_subscription_persistence_unsupported`; `:subscription_identifier` is an invalid option; `topic_alias_maximum` is an unknown key; shared filters are invalid checkpoint content).

- [ ] **Step 4: Implement**

`config.ex`:
1. Append `topic_alias_maximum max_outbound_topic_aliases` to `@keys`.
2. In `assemble/3` add `aliases = Keyword.get(options, :topic_alias_maximum, 0)` and `outbound_aliases = Keyword.get(options, :max_outbound_topic_aliases, 0)`; add both to `machine_options` as `topic_alias_maximum: aliases, max_outbound_topic_aliases: outbound_aliases`; add `Enum.all?([aliases, outbound_aliases], &(is_integer(&1) and &1 in 0..65_535)) and` to `valid`; replace `|> Keyword.put(:topic_alias_maximum, 0)` with `|> Keyword.put(:topic_alias_maximum, aliases)`. Keep `:topic_alias_maximum` forbidden inside `:properties`.

`mqtt.ex`:
1. Delete the clause `defp call({:subscribe, <<"$share/", _::binary>>, _options}, _from, %{store: store} = state) when not is_nil(store)`.
2. Replace the `call({:subscribe, filter, options}, ...)` head and its property computation:

```elixir
  defp call({:subscribe, filter, options}, from, state) do
    properties = if is_list(options), do: Keyword.get(options, :properties, []), else: nil

    if valid_options?(options, [
         :qos,
         :no_local,
         :retain_as_published,
         :retain_handling,
         :properties,
         :subscription_identifier
       ]) and is_list(properties) and
         not (Keyword.has_key?(options, :subscription_identifier) and
                Keyword.keyword?(properties) and
                Keyword.has_key?(properties, :subscription_identifier)) do
      properties =
        case Keyword.fetch(options, :subscription_identifier) do
          {:ok, identifier} -> properties ++ [subscription_identifier: identifier]
          :error -> properties
        end

      subscription = %{
        qos: Keyword.get(options, :qos, 0),
        no_local: Keyword.get(options, :no_local, false),
        retain_as_published: Keyword.get(options, :retain_as_published, false),
        retain_handling: Keyword.get(options, :retain_handling, 0)
      }

      packet = %Subscribe{packet_id: 0, subscriptions: [{filter, subscription}], properties: properties}
      operation(state, from, :subscribe, packet)
    else
      {:reply, {:error, :invalid_options}, state}
    end
  end
```

(`valid_options?/2` already rejects non-keyword `options`, so the `is_list(options)` guard on `properties` only protects its evaluation order.)

`checkpoint.ex`:
1. In `retained?/3` replace `Topic.valid_filter?(filter) and qos in [0, 1, 2]` with `Topic.valid_subscription_filter?(filter) and qos in [0, 1, 2]`.
2. In `packet?/3` first clause use `defp packet?(%Publish{} = packet, :publish, fresh), do: valid_packet?(packet, fresh) and not Keyword.has_key?(packet.properties, :topic_alias)`.

The checkpoint version stays 1 (spec §6: schema unchanged, accepted values widen; a pre-S4 binary reading a shared filter fails closed with `:invalid_checkpoint_schema`).

- [ ] **Step 5: Run client MQTT tests**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/mqtt_shared_subscription_test.exs test/mqtt_checkpoint_test.exs test/mqtt_client_test.exs test/mqtt_store_test.exs test/privacy_test.exs test/mqtt_persistence_fault_test.exs
```

Expected: `0 failures`.

- [ ] **Step 6: Format, compile, commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_client/lib/graviton_mq/client/mqtt.ex apps/graviton_mq_client/lib/graviton_mq/client/mqtt/config.ex apps/graviton_mq_client/lib/graviton_mq/client/mqtt/checkpoint.ex apps/graviton_mq_client/test/mqtt_shared_subscription_test.exs apps/graviton_mq_client/test/mqtt_checkpoint_test.exs
(cd apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors)
git commit -m "feat(client): persist shared subscriptions and expose alias and Sub-ID options" -- apps/graviton_mq_client/lib/graviton_mq/client/mqtt.ex apps/graviton_mq_client/lib/graviton_mq/client/mqtt/config.ex apps/graviton_mq_client/lib/graviton_mq/client/mqtt/checkpoint.ex apps/graviton_mq_client/test/mqtt_shared_subscription_test.exs apps/graviton_mq_client/test/mqtt_checkpoint_test.exs
```

---

## Task 15 (spec T0 + T10 part 1): Wire tap and Mosquitto capability probes (CI-only)

**Files:**
- Create: `apps/graviton_mq_client/test/support/mqtt_wire_tap.exs`
- Create: `apps/graviton_mq_client/test/mqtt_peer_capabilities_test.exs`

- [ ] **Step 1: Create the wire tap**

```elixir
defmodule GravitonMQ.Client.Test.MQTTWireTap do
  @moduledoc false
  # Byte relay for interoperability evidence. It forwards every byte unchanged
  # and records PUBLISH topic, Topic Alias and Subscription Identifiers with an
  # independent parser (no GravitonMQ codec is used as an oracle). It is a
  # recorder, not an MQTT peer.
  use GenServer
  import Bitwise

  def start_link(options), do: GenServer.start_link(__MODULE__, options)
  def port(tap), do: GenServer.call(tap, :port)
  def observed(tap), do: GenServer.call(tap, :observed)
  def drop(tap), do: GenServer.call(tap, :drop)

  @impl true
  def init(options) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])

    {:ok, {_, port}} = :inet.sockname(listener)
    owner = self()
    acceptor = spawn_link(fn -> accept(listener, owner) end)

    {:ok,
     %{
       listener: listener,
       port: port,
       acceptor: acceptor,
       upstream_port: Keyword.fetch!(options, :upstream_port),
       observer: Keyword.fetch!(options, :observer),
       connections: 0,
       sockets: %{},
       observed: []
     }}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}
  def handle_call(:observed, _from, state), do: {:reply, Enum.reverse(state.observed), state}

  def handle_call(:drop, _from, state) do
    for {socket, _} <- state.sockets, do: :gen_tcp.close(socket)
    {:reply, :ok, %{state | sockets: %{}}}
  end

  @impl true
  def handle_info({:accepted, client}, state) do
    {:ok, server} =
      :gen_tcp.connect({127, 0, 0, 1}, state.upstream_port, [:binary, active: false], 1_000)

    connection = state.connections + 1

    sockets =
      state.sockets
      |> Map.put(client, %{peer: server, direction: :to_server, connection: connection, buffer: <<>>})
      |> Map.put(server, %{peer: client, direction: :to_client, connection: connection, buffer: <<>>})

    :ok = :inet.setopts(client, active: :once)
    :ok = :inet.setopts(server, active: :once)
    {:noreply, %{state | connections: connection, sockets: sockets}}
  end

  def handle_info({:tcp, socket, bytes}, state) do
    case Map.fetch(state.sockets, socket) do
      {:ok, entry} ->
        state = relay(state, socket, %{entry | buffer: entry.buffer <> bytes})
        if Map.has_key?(state.sockets, socket), do: :inet.setopts(socket, active: :once)
        {:noreply, state}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({closed, socket}, state) when closed in [:tcp_closed, :tcp_error] do
    {:noreply, close_pair(state, socket)}
  end

  def handle_info({:tcp_error, socket, _reason}, state), do: {:noreply, close_pair(state, socket)}

  @impl true
  def terminate(_reason, state) do
    for {socket, _} <- state.sockets, do: :gen_tcp.close(socket)
    :gen_tcp.close(state.listener)
    Process.exit(state.acceptor, :shutdown)
  end

  defp accept(listener, owner) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        :ok = :gen_tcp.controlling_process(socket, owner)
        send(owner, {:accepted, socket})
        accept(listener, owner)

      {:error, :closed} ->
        :ok
    end
  end

  defp close_pair(state, socket) do
    case Map.fetch(state.sockets, socket) do
      {:ok, %{peer: peer}} ->
        :gen_tcp.close(socket)
        :gen_tcp.close(peer)
        %{state | sockets: state.sockets |> Map.delete(socket) |> Map.delete(peer)}

      :error ->
        state
    end
  end

  defp relay(state, socket, entry) do
    case envelope(entry.buffer) do
      :more ->
        %{state | sockets: Map.put(state.sockets, socket, entry)}

      {:ok, header, body, wire, rest} ->
        :ok = :gen_tcp.send(entry.peer, wire)
        state = observe(state, entry, header, body)
        relay(state, socket, %{entry | buffer: rest})
    end
  end

  defp envelope(<<>>), do: :more

  defp envelope(<<header, rest::binary>> = wire) do
    case variable_integer(rest, 0, 0) do
      :more ->
        :more

      {:ok, length, body_and_rest} when byte_size(body_and_rest) < length ->
        :more

      {:ok, length, body_and_rest} ->
        <<body::binary-size(length), rest::binary>> = body_and_rest
        {:ok, header, body, binary_part(wire, 0, byte_size(wire) - byte_size(rest)), rest}
    end
  end

  defp variable_integer(<<>>, _value, _shift), do: :more

  defp variable_integer(<<byte, rest::binary>>, value, shift) when shift < 28 do
    value = value + ((byte &&& 0x7F) <<< shift)
    if (byte &&& 0x80) == 0, do: {:ok, value, rest}, else: variable_integer(rest, value, shift + 7)
  end

  defp observe(state, entry, header, body) when header >>> 4 == 3 do
    qos = header >>> 1 &&& 3
    <<length::16, topic::binary-size(length), rest::binary>> = body
    rest = if qos == 0, do: rest, else: binary_part(rest, 2, byte_size(rest) - 2)
    {:ok, property_length, rest} = variable_integer(rest, 0, 0)
    <<properties::binary-size(property_length), _payload::binary>> = rest

    fact =
      Map.merge(
        %{
          direction: entry.direction,
          connection: entry.connection,
          topic: topic,
          qos: qos,
          dup: (header &&& 8) != 0
        },
        properties(properties, %{topic_alias: nil, subscription_identifiers: []})
      )

    send(state.observer, {:mqtt_wire_tap, self(), fact})
    %{state | observed: [fact | state.observed]}
  end

  defp observe(state, _entry, _header, _body), do: state

  defp properties(<<>>, acc), do: acc
  defp properties(<<0x23, value::16, rest::binary>>, acc), do: properties(rest, %{acc | topic_alias: value})

  defp properties(<<0x0B, rest::binary>>, acc) do
    {:ok, value, rest} = variable_integer(rest, 0, 0)
    properties(rest, %{acc | subscription_identifiers: acc.subscription_identifiers ++ [value]})
  end

  defp properties(<<0x01, _, rest::binary>>, acc), do: properties(rest, acc)
  defp properties(<<0x02, _::32, rest::binary>>, acc), do: properties(rest, acc)

  defp properties(<<id, length::16, _::binary-size(length), rest::binary>>, acc)
       when id in [0x03, 0x08, 0x09],
       do: properties(rest, acc)

  defp properties(<<0x26, kl::16, _::binary-size(kl), vl::16, _::binary-size(vl), rest::binary>>, acc),
    do: properties(rest, acc)
end
```

- [ ] **Step 2: Create the T0 probe test (Mosquitto 2.1.2)**

```elixir
Code.require_file("support/mosquitto.exs", __DIR__)
Code.require_file("support/mqtt_wire_tap.exs", __DIR__)

defmodule GravitonMQ.MQTTClient.PeerCapabilitiesTest do
  # S4 T0: re-verify pinned peer capabilities before relying on them.
  use ExUnit.Case, async: false

  alias GravitonMQ.Client.Test.{Mosquitto, MQTTWireTap}

  @moduletag :interop
  @moduletag timeout: 30_000

  setup_all do
    %{executables: Mosquitto.executables!()}
  end

  setup do
    directory = Mosquitto.directory!()
    on_exit(fn -> File.rm_rf!(directory) end)
    %{directory: directory}
  end

  test "Mosquitto 2.1.2 accepts max_topic_alias_broker", context do
    broker =
      Mosquitto.broker!(context.executables, context.directory,
        configuration: "max_topic_alias_broker 10"
      )

    refute Mosquitto.snapshot(broker.owner).output =~ "Unknown configuration variable"
  end

  test "mosquitto_pub --repeat with topic-alias sends alias-only PUBLISH after the first",
       context do
    broker = Mosquitto.broker!(context.executables, context.directory)
    tap = start_supervised!({MQTTWireTap, upstream_port: broker.port, observer: self()})

    Mosquitto.command!(
      context.executables.publisher,
      Mosquitto.cli_args(%{port: MQTTWireTap.port(tap)}) ++
        ["-t", "probe/a", "-m", "x", "-q", "1", "--repeat", "3", "--repeat-delay", "0"] ++
        ["-D", "publish", "topic-alias", "1"]
    )

    sent = for %{direction: :to_server} = fact <- MQTTWireTap.observed(tap), do: {fact.topic, fact.topic_alias}
    assert sent == [{"probe/a", 1}, {"", 1}, {"", 1}]
  end
end
```

- [ ] **Step 3: Verify locally that the files compile and are excluded**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/mqtt_peer_capabilities_test.exs
```

Expected: `2 tests, 0 failures, 2 excluded` (no `GRAVITON_MOSQUITTO_BIN` locally). The real outcome is recorded from CI in Task 20.

- [ ] **Step 4: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_client/test/support/mqtt_wire_tap.exs apps/graviton_mq_client/test/mqtt_peer_capabilities_test.exs
git add apps/graviton_mq_client/test/support/mqtt_wire_tap.exs apps/graviton_mq_client/test/mqtt_peer_capabilities_test.exs
git commit -m "test(mqtt): add wire tap and pinned peer capability probes" -- apps/graviton_mq_client/test/support/mqtt_wire_tap.exs apps/graviton_mq_client/test/mqtt_peer_capabilities_test.exs
```

---

## Task 16 (spec T10): Independent peers → our broker (CI-only)

**Files:**
- Create: `apps/graviton_mq/test/support/mqtt_feature_peer.py`
- Create: `apps/graviton_mq/test/graviton_mq/mqtt_feature_paho_test.exs`
- Create: `apps/graviton_mq/test/graviton_mq/mqtt_feature_mosquitto_test.exs`

- [ ] **Step 1: Create the Paho peer**

```python
"""Fixed-version independent MQTT5 peer for S4 features; never imported by production code."""

import argparse
import time
from importlib.metadata import version
from threading import Event, Lock

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions


class ObservedClient(mqtt.Client):
    """Paho 2.1.0 parses a DISCONNECT reason only for Remaining Length > 2; keep the raw body."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wire_disconnect = None
        self.disconnected = Event()

    def _handle_disconnect(self):
        self.wire_disconnect = bytes(self._in_packet["packet"]).hex()
        self.disconnected.set()
        return super()._handle_disconnect()


class Inbox:
    def __init__(self, peer, auto_ack=False):
        self.lock = Lock()
        self.messages = []
        self.peer = peer
        self.auto_ack = auto_ack
        peer.on_message = self.on_message

    def on_message(self, _client, _userdata, message):
        properties = message.properties
        with self.lock:
            self.messages.append({
                "topic": message.topic,
                "payload": message.payload.decode(),
                "alias": getattr(properties, "TopicAlias", None) if properties else None,
                "ids": sorted(getattr(properties, "SubscriptionIdentifier", []) if properties else []),
                "retain": message.retain,
                "mid": message.mid,
                "qos": message.qos,
            })
        if self.auto_ack:
            self.peer.ack(message.mid, message.qos)

    def wait(self, count, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                if len(self.messages) >= count:
                    return list(self.messages)
            time.sleep(0.02)
        raise AssertionError(f"expected {count} messages, got {self.messages}")


def client(client_id, observed=False, manual_ack=False):
    kind = ObservedClient if observed else mqtt.Client
    return kind(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5,
                reconnect_on_failure=False, manual_ack=manual_ack)


def connect(peer, port, clean_start=True, **properties):
    connected, result = Event(), {}

    def on_connect(_client, _userdata, _flags, reason, props):
        result["reason"], result["properties"] = reason.value, props
        connected.set()

    peer.on_connect = on_connect
    props = Properties(PacketTypes.CONNECT)
    for name, value in properties.items():
        setattr(props, name, value)
    peer.connect("127.0.0.1", port, keepalive=5, clean_start=clean_start, properties=props)
    peer.loop_start()
    assert connected.wait(3), "missing CONNACK"
    assert result["reason"] == 0, result
    return result["properties"]


def subscribe(peer, topic, qos=1, identifier=None):
    done, granted = Event(), {}

    def on_subscribe(_client, _userdata, _mid, codes, _props):
        granted["codes"] = [code.value for code in codes]
        done.set()

    peer.on_subscribe = on_subscribe
    props = None
    if identifier is not None:
        props = Properties(PacketTypes.SUBSCRIBE)
        props.SubscriptionIdentifier = identifier
    peer.subscribe(topic, options=SubscribeOptions(qos=qos), properties=props)
    assert done.wait(3), "missing SUBACK"
    return granted["codes"]


def publish(peer, topic, payload, qos=1, alias=None, retain=False):
    props = Properties(PacketTypes.PUBLISH)
    if alias is not None:
        props.TopicAlias = alias
    info = peer.publish(topic, payload.encode(), qos=qos, retain=retain, properties=props)
    info.wait_for_publish(3)
    assert info.is_published(), f"publication {payload} not acknowledged"


def expect_disconnect(peer, code):
    assert peer.disconnected.wait(3), "server did not disconnect"
    assert peer.wire_disconnect == f"{code:02x}", peer.wire_disconnect


def stop(*peers):
    for peer in peers:
        peer.disconnect()
        peer.loop_stop()


def probe(_port):
    publisher = client("paho-probe")
    props = Properties(PacketTypes.PUBLISH)
    props.TopicAlias = 1
    publisher.publish("", b"x", qos=0, properties=props)
    subscribe_props = Properties(PacketTypes.SUBSCRIBE)
    subscribe_props.SubscriptionIdentifier = 7
    print("Paho 2.1.0 capability probe passed: manual TopicAlias with empty topic, SubscriptionIdentifier", flush=True)


def alias_inbound(port):
    subscriber = client("paho-alias-sub")
    connect(subscriber, port)
    inbox = Inbox(subscriber)
    subscribe(subscriber, "alias/#", qos=2)
    publisher = client("paho-alias-pub")
    assert connect(publisher, port).TopicAliasMaximum == 10
    for index, qos in enumerate([1, 0, 1, 2]):
        publish(publisher, "alias/t" if index == 0 else "", f"m{index}", qos=qos, alias=1)
    received = inbox.wait(4)
    assert sorted((m["topic"], m["payload"]) for m in received) == [("alias/t", f"m{i}") for i in range(4)], received
    assert all(m["alias"] is None for m in received), received
    stop(subscriber, publisher)
    print("Paho inbound Topic Alias resolved for QoS0/1/2", flush=True)


def alias_invalid(port):
    publisher = client("paho-alias-invalid", observed=True)
    connect(publisher, port)
    props = Properties(PacketTypes.PUBLISH)
    props.TopicAlias = 11
    publisher.publish("alias/x", b"x", qos=0, properties=props)
    expect_disconnect(publisher, 0x94)
    publisher.loop_stop()
    print("Paho alias above Topic Alias Maximum rejected by DISCONNECT 0x94", flush=True)


def alias_unknown(port):
    publisher = client("paho-alias-unknown", observed=True)
    connect(publisher, port)
    props = Properties(PacketTypes.PUBLISH)
    props.TopicAlias = 3
    publisher.publish("", b"x", qos=0, properties=props)
    expect_disconnect(publisher, 0x82)
    publisher.loop_stop()
    print("Paho unknown alias rejected by DISCONNECT 0x82", flush=True)


def alias_reconnect(port):
    first = client("paho-alias-reconnect")
    connect(first, port)
    publish(first, "alias/r", "set", qos=1, alias=1)
    stop(first)
    second = client("paho-alias-reconnect", observed=True)
    connect(second, port)
    props = Properties(PacketTypes.PUBLISH)
    props.TopicAlias = 1
    second.publish("", b"stale", qos=0, properties=props)
    expect_disconnect(second, 0x82)
    second.loop_stop()
    print("Paho alias mapping not carried across Network Connections", flush=True)


def alias_outbound(port, maximum):
    subscriber = client("paho-alias-out-sub")
    connect(subscriber, port, SessionExpiryInterval=60, TopicAliasMaximum=maximum)
    inbox = Inbox(subscriber)
    subscribe(subscriber, "out/#", qos=1)
    publisher = client("paho-alias-out-pub")
    connect(publisher, port)
    topics = [f"out/{index}" for index in range(maximum + 1)]
    for topic in topics:
        publish(publisher, topic, topic)
        publish(publisher, topic, topic)
    received = inbox.wait(2 * len(topics))
    table = {}
    resolved = []
    for message in received:
        if message["alias"] is not None:
            assert 1 <= message["alias"] <= maximum, message
            if message["topic"]:
                table[message["alias"]] = message["topic"]
        resolved.append(message["topic"] or table[message["alias"]])
    assert resolved == [topic for topic in topics for _ in range(2)], resolved
    assert [m["topic"] for m in received[1:2 * maximum:2]] == [""] * maximum, received
    assert all(m["alias"] is None for m in received[-2:]), received
    subscriber.loop_stop()
    subscriber._sock_close()
    again = client("paho-alias-out-sub")
    connect(again, port, clean_start=False, SessionExpiryInterval=60, TopicAliasMaximum=maximum)
    inbox = Inbox(again)
    publish(publisher, topics[0], "after-reconnect")
    message = inbox.wait(1)[0]
    assert message["topic"] == topics[0], message
    stop(again, publisher)
    print(f"Paho outbound aliases bounded by {maximum} and reset after reconnect", flush=True)


def subscription_ids(port):
    subscriber = client("paho-sid-sub")
    connect(subscriber, port)
    inbox = Inbox(subscriber)
    subscribe(subscriber, "sub/#", identifier=7)
    subscribe(subscriber, "sub/+/x", identifier=9)
    publisher = client("paho-sid-pub")
    connect(publisher, port)
    publish(publisher, "sub/a/x", "both")
    assert inbox.wait(1)[0]["ids"] == [7, 9], inbox.messages
    subscribe(subscriber, "sub/+/x")
    publish(publisher, "sub/a/x", "one")
    assert inbox.wait(2)[1]["ids"] == [7], inbox.messages
    publish(publisher, "sub/r/x", "kept", retain=True)
    inbox.wait(3)
    late = client("paho-sid-late")
    connect(late, port)
    late_inbox = Inbox(late)
    subscribe(late, "sub/+/x", identifier=11)
    retained = late_inbox.wait(1)[0]
    assert retained["retain"] and retained["ids"] == [11] and retained["payload"] == "kept", retained
    stop(subscriber, publisher, late)
    print("Paho Subscription Identifiers: overlap, removal and retained", flush=True)


def shared(port, count):
    publisher = client("paho-share-pub")
    connect(publisher, port)
    publish(publisher, "t/r", "retained", retain=True)
    members, inboxes = [], []
    for name in ["paho-share-m1", "paho-share-m2"]:
        member = client(name)
        connect(member, port)
        inboxes.append(Inbox(member))
        assert subscribe(member, "$share/g/t/#") == [1]
        members.append(member)
    time.sleep(0.5)
    assert all(not inbox.messages for inbox in inboxes), "retained message sent on shared subscribe"
    for index in range(count):
        publish(publisher, f"t/{index}", str(index))
    deadline = time.monotonic() + 5
    while sum(len(inbox.messages) for inbox in inboxes) < count and time.monotonic() < deadline:
        time.sleep(0.02)
    payloads = [m["payload"] for inbox in inboxes for m in inbox.messages]
    assert sorted(payloads, key=int) == [str(i) for i in range(count)], payloads
    assert all(inbox.messages for inbox in inboxes), "a member received nothing"
    stop(publisher, *members)
    print(f"Paho shared group delivered {count} messages exactly once across two members", flush=True)


def shared_redistribute(port):
    m1 = client("paho-share-r1", manual_ack=True)
    connect(m1, port, SessionExpiryInterval=60, ReceiveMaximum=1)
    inbox1 = Inbox(m1)
    subscribe(m1, "$share/r/w")
    m2 = client("paho-share-r2", manual_ack=True)
    connect(m2, port, SessionExpiryInterval=60, ReceiveMaximum=1)
    inbox2 = Inbox(m2)
    subscribe(m2, "$share/r/w")
    publisher = client("paho-share-rpub")
    connect(publisher, port)
    for payload in ["1", "2", "3", "4"]:
        publish(publisher, "w", payload)
    first = inbox1.wait(1)[0]
    inbox2.wait(1)
    m1.loop_stop()
    m1._sock_close()
    inbox2.auto_ack = True
    for message in list(inbox2.messages):
        m2.ack(message["mid"], message["qos"])
    received2 = inbox2.wait(3)
    again = client("paho-share-r1", manual_ack=True)
    connect(again, port, clean_start=False, SessionExpiryInterval=60, ReceiveMaximum=1)
    inbox_again = Inbox(again, auto_ack=True)
    resumed = inbox_again.wait(1)[0]
    assert resumed["payload"] == first["payload"], (first, resumed)
    union = sorted({m["payload"] for m in received2} | {resumed["payload"]})
    assert union == ["1", "2", "3", "4"], (received2, resumed)
    stop(again, m2, publisher)
    print("Paho shared QoS1 unsent entry moved to the connected member; sent entry resumed", flush=True)


def shared_no_local(port):
    member = client("paho-share-nolocal", observed=True)
    connect(member, port)
    member.subscribe("$share/g/t", options=SubscribeOptions(qos=1, noLocal=True))
    expect_disconnect(member, 0x82)
    member.loop_stop()
    print("Paho No Local on a shared subscription rejected by DISCONNECT 0x82", flush=True)


MODES = {
    "probe": probe,
    "alias-inbound": alias_inbound,
    "alias-invalid": alias_invalid,
    "alias-unknown": alias_unknown,
    "alias-reconnect": alias_reconnect,
    "subid": subscription_ids,
    "shared-redistribute": shared_redistribute,
    "shared-nolocal": shared_no_local,
}

if __name__ == "__main__":
    assert version("paho-mqtt") == "2.1.0", "peer requires paho-mqtt==2.1.0"
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=sorted(MODES) + ["alias-outbound", "shared"])
    parser.add_argument("port", type=int)
    parser.add_argument("count", type=int, nargs="?", default=0)
    arguments = parser.parse_args()
    if arguments.mode == "alias-outbound":
        alias_outbound(arguments.port, arguments.count)
    elif arguments.mode == "shared":
        shared(arguments.port, arguments.count)
    else:
        MODES[arguments.mode](arguments.port)
```

- [ ] **Step 2: Create the Paho ExUnit driver**

```elixir
Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.FeaturePahoTest do
  use ExUnit.Case, async: false
  @moduletag :paho_interop
  @moduletag timeout: 60_000
  alias GravitonMQ.Client.Test.Mosquitto

  test "T0: Paho 2.1.0 exposes manual Topic Alias and Subscription Identifier properties" do
    assert run!("probe", 0) =~ "capability probe passed"
  end

  for {mode, extra, broker_options, expected} <- [
        {"alias-inbound", [], [], "inbound Topic Alias resolved"},
        {"alias-invalid", [], [], "DISCONNECT 0x94"},
        {"alias-unknown", [], [], "DISCONNECT 0x82"},
        {"alias-reconnect", [], [], "not carried across"},
        {"alias-outbound", ["3"], [max_outbound_topic_aliases: 10], "bounded by 3"},
        {"subid", [], [], "overlap, removal and retained"},
        {"shared", ["10"], [], "exactly once across two members"},
        {"shared-redistribute", [], [], "moved to the connected member"},
        {"shared-nolocal", [], [], "DISCONNECT 0x82"}
      ] do
    @mode mode
    @extra extra
    @broker_options broker_options
    @expected expected
    test "Paho → broker: #{mode}" do
      port = broker(@broker_options)
      assert run!(@mode, port, @extra) =~ @expected
    end
  end

  defp broker(extra) do
    instance =
      start_supervised!(
        {GravitonMQ,
         name: nil,
         runtime_supervisor_name: nil,
         mqtt:
           [
             auth: :anonymous,
             host_principal: :host,
             tick_interval: 10,
             listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
           ] ++ extra}
      )

    {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    port
  end

  defp run!(mode, port, extra \\ []) do
    peer =
      Mosquitto.process!(
        System.fetch_env!("GRAVITON_PAHO_PYTHON"),
        [Path.expand("../support/mqtt_feature_peer.py", __DIR__), mode, to_string(port)] ++ extra
      )

    Mosquitto.eventually!(
      fn -> Mosquitto.snapshot(peer).exit_status != nil end,
      fn -> Mosquitto.snapshot(peer) end,
      30_000
    )

    assert %{exit_status: 0, output: output} = Mosquitto.snapshot(peer)
    output
  end
end
```

- [ ] **Step 3: Create the mosquitto_pub/sub → broker driver**

```elixir
Code.require_file("../../../graviton_mq_client/test/support/mosquitto.exs", __DIR__)
Code.require_file("../../../graviton_mq_client/test/support/mqtt_wire_tap.exs", __DIR__)

defmodule GravitonMQ.MQTTBroker.FeatureMosquittoTest do
  use ExUnit.Case, async: false
  @moduletag :interop
  @moduletag timeout: 60_000
  alias GravitonMQ.Client.Test.{Mosquitto, MQTTWireTap}

  setup_all do
    %{executables: Mosquitto.executables!()}
  end

  defp broker do
    instance =
      start_supervised!(
        {GravitonMQ,
         name: nil,
         runtime_supervisor_name: nil,
         mqtt: [
           auth: :anonymous,
           host_principal: :host,
           tick_interval: 10,
           listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
         ]}
      )

    {:ok, %{owner: owner, listeners: [%{port: port}]}} = GravitonMQ.status(instance)
    {instance, owner, port}
  end

  test "two mosquitto_sub members split a shared group evenly", context do
    {instance, _owner, port} = broker()

    members =
      for name <- ["mosq-share-1", "mosq-share-2"] do
        Mosquitto.process!(
          context.executables.subscriber,
          Mosquitto.cli_args(%{port: port}) ++
            ["-i", name, "-t", "$share/g/m/#", "-q", "1", "-C", "3", "-W", "10", "-F", "%t|%p"]
        )
      end

    Mosquitto.eventually!(fn -> match?({:ok, %{shared_groups: 1, sessions: 2}}, GravitonMQ.status(instance)) end)

    for index <- 1..6 do
      assert {:ok, _} = GravitonMQ.publish(instance, "m/#{index}", "p#{index}", qos: 1)
    end

    outputs =
      for member <- members do
        Mosquitto.eventually!(fn -> Mosquitto.snapshot(member).exit_status != nil end, fn -> Mosquitto.snapshot(member) end, 10_000)
        assert %{exit_status: 0, output: output} = Mosquitto.snapshot(member)
        output |> String.split("\n", trim: true) |> Enum.filter(&String.starts_with?(&1, "m/"))
      end

    assert Enum.map(outputs, &length/1) == [3, 3]
    assert Enum.sort(List.flatten(outputs)) == Enum.map(1..6, &"m/#{&1}|p#{&1}")
  end

  test "mosquitto_pub topic-alias publications reach a host subscriber with full topics",
       context do
    {instance, owner, port} = broker()
    tap = start_supervised!({MQTTWireTap, upstream_port: port, observer: self()})
    {:ok, operation} = GravitonMQ.subscribe(instance, "a/#", qos: 1)
    assert_receive {:graviton_mq, ^owner, {:operation, ^operation, {:ok, subscription}}}, 2_000
    :ok = GravitonMQ.request(instance, subscription, 3)

    Mosquitto.command!(
      context.executables.publisher,
      Mosquitto.cli_args(%{port: MQTTWireTap.port(tap)}) ++
        ["-t", "a/m", "-m", "x", "-q", "1", "--repeat", "3", "--repeat-delay", "0"] ++
        ["-D", "publish", "topic-alias", "1"]
    )

    for _ <- 1..3 do
      assert_receive {:graviton_mq, ^owner, {:delivery, ^subscription, delivery, %{topic: "a/m"}}}, 3_000
      :ok = GravitonMQ.ack(instance, delivery)
    end

    assert Enum.any?(MQTTWireTap.observed(tap), &(&1.direction == :to_server and &1.topic == "" and &1.topic_alias == 1))
  end
end
```

- [ ] **Step 4: Verify local compile and exclusion**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/graviton_mq/mqtt_feature_paho_test.exs test/graviton_mq/mqtt_feature_mosquitto_test.exs test/graviton_mq/mqtt_paho_test.exs
python3 -m py_compile apps/graviton_mq/test/support/mqtt_feature_peer.py 2>/dev/null || (cd /home/gao/Workspace/gsmlg-dev/graviton_mq && python3 -I -m py_compile apps/graviton_mq/test/support/mqtt_feature_peer.py)
```

Expected: all tests `excluded`, `0 failures`; `py_compile` prints nothing. Delete any generated `__pycache__` only if it did not exist before (`apps/graviton_mq/test/support/__pycache__` already exists and is untracked).

- [ ] **Step 5: Format and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq/test/graviton_mq/mqtt_feature_paho_test.exs apps/graviton_mq/test/graviton_mq/mqtt_feature_mosquitto_test.exs
git add apps/graviton_mq/test/support/mqtt_feature_peer.py apps/graviton_mq/test/graviton_mq/mqtt_feature_paho_test.exs apps/graviton_mq/test/graviton_mq/mqtt_feature_mosquitto_test.exs
git commit -m "test(mqtt): qualify broker aliases, Sub-IDs and shared groups with Paho and Mosquitto" -- apps/graviton_mq/test/support/mqtt_feature_peer.py apps/graviton_mq/test/graviton_mq/mqtt_feature_paho_test.exs apps/graviton_mq/test/graviton_mq/mqtt_feature_mosquitto_test.exs
```

---

## Task 17 (spec T11): Our client → Mosquitto (CI-only)

**Files:**
- Create: `apps/graviton_mq_client/test/mqtt_feature_mosquitto_test.exs`

- [ ] **Step 1: Create the test file**

```elixir
Code.require_file("support/mosquitto.exs", __DIR__)
Code.require_file("support/mqtt_wire_tap.exs", __DIR__)

defmodule GravitonMQ.MQTTClient.FeatureMosquittoTest do
  use ExUnit.Case, async: false

  alias GravitonMQ.Client.Test.{Mosquitto, MQTTWireTap}
  alias GravitonMQ.MQTT.Packet.Publish
  alias GravitonMQ.MQTTClient, as: Client

  @moduletag :interop
  @moduletag :tmp_dir
  @moduletag timeout: 60_000

  setup_all do
    %{executables: Mosquitto.executables!()}
  end

  setup do
    directory = Mosquitto.directory!()
    on_exit(fn -> File.rm_rf!(directory) end)
    %{directory: directory}
  end

  defp client!(port, overrides) do
    options =
      Keyword.merge(
        [
          host: "127.0.0.1",
          port: port,
          client_id: "graviton-s4-#{System.unique_integer([:positive])}",
          observer: self(),
          keep_alive: 5,
          session_expiry_interval: 60,
          reconnect: [max_attempts: 3, delay: 50, max_delay: 200],
          operation_timeout: 2_000
        ],
        overrides
      )

    client = start_supervised!({Client, options}, id: Keyword.get(overrides, :id, make_ref()))
    assert_receive {:graviton_mq, ^client, {:connected, _peer}}, 5_000
    client
  end

  defp subscribed!(client, filter, options) do
    {:ok, operation} = Client.subscribe(client, filter, options)
    assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, [_]}}}, 5_000
  end

  defp deliveries(client, count) do
    for _ <- 1..count do
      assert_receive {:graviton_mq, ^client, {:delivery, delivery, %Publish{} = packet}}, 5_000
      :ok = Client.ack(client, delivery)
      packet
    end
  end

  test "outbound aliases to Mosquitto respect its Topic Alias Maximum", context do
    for {configuration, bound} <- [{"", 3}, {"max_topic_alias 1", 1}] do
      broker = Mosquitto.broker!(context.executables, context.directory, configuration: configuration)
      tap = start_supervised!({MQTTWireTap, upstream_port: broker.port, observer: self()}, id: make_ref())
      subscriber = Mosquitto.process!(context.executables.subscriber, Mosquitto.cli_args(broker) ++ ["-t", "alias/#", "-q", "1", "-C", "8", "-W", "10", "-F", "%t|%p"])
      Mosquitto.eventually!(fn -> Mosquitto.snapshot(broker.owner).output =~ "Sending SUBACK" end)
      client = client!(MQTTWireTap.port(tap), max_outbound_topic_aliases: 3)

      for topic <- ["alias/a", "alias/a", "alias/b", "alias/b", "alias/c", "alias/c", "alias/d", "alias/d"] do
        {:ok, operation} = Client.publish(client, topic, topic, qos: 1)
        assert_receive {:graviton_mq, ^client, {:operation, ^operation, {:ok, :puback}}}, 5_000
      end

      Mosquitto.eventually!(fn -> Mosquitto.snapshot(subscriber).exit_status != nil end, fn -> Mosquitto.snapshot(subscriber) end, 10_000)
      assert %{exit_status: 0, output: output} = Mosquitto.snapshot(subscriber)
      assert length(Regex.scan(~r/^alias\/[a-d]\|alias\/[a-d]$/m, output)) == 8

      sent = for %{direction: :to_server} = fact <- MQTTWireTap.observed(tap), do: fact
      assert Enum.all?(sent, &(&1.topic_alias == nil or &1.topic_alias in 1..bound))
      assert Enum.count(sent, &(&1.topic == "")) == bound
    end
  end

  test "Mosquitto aliases to our client resolve to full topics and reset on reconnect", context do
    broker = Mosquitto.broker!(context.executables, context.directory, configuration: "max_topic_alias_broker 10")
    tap = start_supervised!({MQTTWireTap, upstream_port: broker.port, observer: self()})
    client = client!(MQTTWireTap.port(tap), topic_alias_maximum: 5)
    :ok = Client.request(client, 8)
    subscribed!(client, "in/#", qos: 1)
    for _ <- 1..3, do: Mosquitto.publish!(context.executables, broker, "in/a", "x", 1)
    received = deliveries(client, 3)
    assert Enum.all?(received, &(&1.topic == "in/a" and not Keyword.has_key?(&1.properties, :topic_alias)))
    first = for %{direction: :to_client, connection: 1} = fact <- MQTTWireTap.observed(tap), do: fact.topic
    assert first == ["in/a", "", ""]

    :ok = MQTTWireTap.drop(tap)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 5_000
    Mosquitto.publish!(context.executables, broker, "in/a", "y", 1)
    assert [%Publish{topic: "in/a"}] = deliveries(client, 1)
    second = for %{direction: :to_client, connection: 2} = fact <- MQTTWireTap.observed(tap), do: fact.topic
    assert hd(second) == "in/a"
  end

  test "Subscription Identifiers from Mosquitto reach the host", context do
    broker = Mosquitto.broker!(context.executables, context.directory)
    client = client!(broker.port, [])
    :ok = Client.request(client, 4)
    subscribed!(client, "sid/#", qos: 1, subscription_identifier: 7)
    subscribed!(client, "sid/+/x", qos: 1, subscription_identifier: 9)
    Mosquitto.publish!(context.executables, broker, "sid/a/x", "x", 1)
    Process.sleep(500)
    received = deliveries(client, 1) ++ for({:graviton_mq, ^client, {:delivery, d, %Publish{} = p}} <- flush(), do: (:ok = Client.ack(client, d)) && p)
    ids = received |> Enum.flat_map(&for({:subscription_identifier, id} <- &1.properties, do: id)) |> Enum.sort() |> Enum.uniq()
    assert ids == [7, 9]
    assert Enum.all?(received, &(&1.topic == "sid/a/x"))
  end

  test "memory and persistent clients share a Mosquitto group; the persistent member resumes it",
       context do
    broker = Mosquitto.broker!(context.executables, context.directory)
    memory = client!(broker.port, client_id: "s4-share-memory")
    directory = Path.join(context.tmp_dir, "store")
    persistent = client!(broker.port, client_id: "s4-share-durable", id: :durable, persistence: [directory: directory])

    for client <- [memory, persistent] do
      :ok = Client.request(client, 16)
      subscribed!(client, "$share/g/s/#", qos: 1)
    end

    for index <- 1..6, do: Mosquitto.publish!(context.executables, broker, "s/#{index}", "p#{index}", 1)
    Process.sleep(1_000)

    payloads =
      for {:graviton_mq, client, {:delivery, delivery, %Publish{payload: payload}}} <- flush() do
        :ok = Client.ack(client, delivery)
        {client, payload}
      end

    assert Enum.sort(Enum.map(payloads, &elem(&1, 1))) == Enum.map(1..6, &"p#{&1}")
    assert Enum.any?(payloads, &(elem(&1, 0) == memory)) and Enum.any?(payloads, &(elem(&1, 0) == persistent))

    :ok = stop_supervised(:durable)
    restarted = client!(broker.port, client_id: "s4-share-durable", id: :durable_again, persistence: [directory: directory])
    refute_receive {:graviton_mq, ^restarted, {:subscriptions_lost, _}}, 500
    :ok = Client.request(restarted, 8)
    for index <- 7..10, do: Mosquitto.publish!(context.executables, broker, "s/#{index}", "p#{index}", 1)
    Process.sleep(1_000)
    assert Enum.any?(flush(), &match?({:graviton_mq, ^restarted, {:delivery, _, %Publish{}}}, &1))
  end

  defp flush do
    receive do
      message -> [message | flush()]
    after
      0 -> []
    end
  end
end
```

- [ ] **Step 2: Verify local compile and exclusion; format; commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq/apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp mix test test/mqtt_feature_mosquitto_test.exs
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix format apps/graviton_mq_client/test/mqtt_feature_mosquitto_test.exs
git add apps/graviton_mq_client/test/mqtt_feature_mosquitto_test.exs
git commit -m "test(client): qualify MQTT aliases, Sub-IDs and shared groups against Mosquitto" -- apps/graviton_mq_client/test/mqtt_feature_mosquitto_test.exs
```

Expected: `4 tests, 0 failures, 4 excluded`.

---

## Task 18 (spec T12, MQTT-only docs)

**Files:**
- Modify: `docs/MQTT_BROKER.md` (§ "Negotiation, limits and failure" first paragraph; new "Topic Alias, Subscription Identifiers and shared subscriptions" section; § "Local durability" upgrade note)
- Modify: `docs/MQTT_CLIENT.md` (lines 95–100, 128–137, 140–145)
- Modify: `apps/graviton_mq_client/lib/graviton_mq/mqtt_client.ex` (moduledoc lines 33–35)

- [ ] **Step 1: `MQTT_BROKER.md`** — replace "Retain/wildcards are available, shared subscriptions and Subscription Identifiers unavailable, and inbound Topic Alias Maximum=0. Publications/Wills above the configured QoS, enhanced authentication, shared subscriptions and unsupported property use follow explicit rejection paths." with:

```markdown
Retain, wildcards, shared subscriptions and Subscription Identifiers are
available and inbound Topic Alias Maximum is 10 by default. Publications/Wills
above the configured QoS, enhanced authentication and unsupported property use
follow explicit rejection paths.
```

Insert a new section before "## Negotiation, limits and failure":

```markdown
## Topic Alias, Subscription Identifiers and shared subscriptions

| Option | Default | Meaning |
| --- | --- | --- |
| `topic_alias_maximum` | `10` | CONNACK Topic Alias Maximum (0..65,535); every alias 1..N is accepted |
| `max_outbound_topic_aliases` | `0` | Broker cap for aliases it sends, bounded by the client's CONNECT maximum |
| `max_alias_bytes` | `134_217_728` | Static bound `max_connections x (inbound + outbound) x min(65_535, max_packet_size)`; start fails if exceeded |
| `subscription_identifiers` | `true` | Subscription Identifier Available; `false` restores DISCONNECT 0xA1 |
| `shared_subscriptions` | `true` | Shared Subscription Available; `false` restores DISCONNECT 0x9E |

Alias tables belong to one Network Connection and are never persisted. Aliases
resolve before every other PUBLISH check; an alias of 0 or above the maximum
gets DISCONNECT 0x94, an empty topic with an unknown alias 0x82. Subscribers
receive full topics; outbound aliases use the first N distinct topics per
connection and never replace them. A publication is delivered only if its
full-topic form fits the client's Maximum Packet Size.

One SUBSCRIBE Subscription Identifier applies to every filter in it; a
re-SUBSCRIBE replaces it. A forwarded copy carries the identifiers of all
matching subscriptions (repeated identifiers stay repeated); a shared copy
carries only the receiving member's identifier. Retransmissions keep the
identifiers captured when the message was queued.

`$share/{group}/{filter}` groups are broker-wide and derived from session
memberships. Each matching message goes to one member: round-robin over Client
Identifiers, preferring connected members that fit and have send quota, then
connected members that fit, then (QoS>0) offline persistent members within the
offline limits. QoS0 messages nobody connected can take are discarded.
Shared subscriptions never receive retained messages. Authorization sees the
full `$share/...` filter on subscribe and the chosen member on delivery; a
denial drops that group copy. When a member's connection is lost its unsent
entries move to a connected member; when its session ends its QoS1 entries and
unsent QoS2 entries move, while already-sent QoS2 entries are discarded
(MQTT-4.8.2-5). Entries no member can take are counted in `status/1`
`shared_discarded`. No Local on a shared filter gets DISCONNECT 0x82. The host
API (`GravitonMQ.subscribe/3`) does not join shared groups and has no
Subscription Identifiers. `status/1` adds `shared_groups`, `shared_discarded`
and `topic_aliases: %{inbound: n, outbound: n}`.
```

Append to "## Local durability":

```markdown
Upgrade note (S4): the broker writes snapshot version 2 (subscription
identifiers, shared memberships and shared delivery entries) under the same WAL
record type. Version 1 data is upgraded in memory on recovery and the next
commit writes version 2; no offline migration is needed. A release before S4
cannot read version 2 and fails closed with `:invalid_broker_checkpoint`; keep
a copy of the data directory if a rollback may be needed. Restarting with
`shared_subscriptions: false` or `subscription_identifiers: false` while the
snapshot holds that state stops startup with `:checkpoint_capability_mismatch`.
```

- [ ] **Step 2: `MQTT_CLIENT.md`**

Replace "the client advertises Receive Maximum and Topic Alias Maximum=0." with "the client advertises Receive Maximum and its `:topic_alias_maximum` (default 0)." and replace "It rejects aliases because Topic Alias Maximum is zero." with "Inbound aliases above the advertised maximum terminate with 0x94 and unknown aliases with 0x82; deliveries carry the resolved topic without the `topic_alias` property."

After "`:receive_maximum`, and `:max_packet_size` adjust those bounds." insert: "`:topic_alias_maximum` (inbound, default 0) and `:max_outbound_topic_aliases` (default 0) enable Topic Aliases; outbound aliases are bounded by the peer's CONNACK maximum, assigned by the client to the first N distinct topics per connection, and a caller-supplied `topic_alias` property stays rejected. `subscribe/3` accepts `subscription_identifier: 1..268_435_455`; received identifiers arrive in the delivery properties."

Replace the paragraph "Shared subscriptions are outside the local persistence profile. ... shared-subscription capability." with:

```markdown
Shared subscriptions follow the peer's advertised capability in both the
memory and the persistent profile. A persistent client checkpoints a granted
`$share/...` filter like any other filter; after a restart with Session
Present 0 it is reported in `{:subscriptions_lost, filters}`. The checkpoint
version stays 1; a client release before S4 rejects such a checkpoint with
`:invalid_checkpoint_schema`.
```

- [ ] **Step 3: `mqtt_client.ex` moduledoc** — replace

```text
  Shared subscriptions are outside this persistence profile: `subscribe/3`
  returns `{:error, :shared_subscription_persistence_unsupported}` before
  admission or transmission when persistence is enabled.
```

with

```text
  Shared subscriptions are checkpointed like other filters when the peer
  advertises them; a lost session reports them in `{:subscriptions_lost, map}`.
```

- [ ] **Step 4: Verify nothing references the removed error and commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && grep -rn "shared_subscription_persistence_unsupported" apps docs/MQTT_CLIENT.md docs/MQTT_BROKER.md | grep -v "/tmp/" ; echo "exit=$?"
(cd apps/graviton_mq_client && env MIX_BUILD_ROOT=/tmp/gmq-build-s4 mix compile --warnings-as-errors)
git commit -m "docs(mqtt): document topic aliases, subscription identifiers and shared subscriptions" -- docs/MQTT_BROKER.md docs/MQTT_CLIENT.md apps/graviton_mq_client/lib/graviton_mq/mqtt_client.ex
```

Expected: `grep` prints nothing (`exit=1`). `docs/releases/0.2.1.md` keeps its historical mention; release notes are not authorized by the spec.

---

## Task 19 (coordinator-serialized): Full required verification

Run only after S2 has stopped writing (coordinator decision). This is the AGENTS.md sequence with build isolation; `mix format` here is umbrella-wide by design.

- [ ] **Step 1: Confirm no interop/workflow change is needed**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq && git diff --stat BASE -- scripts/interop .github/workflows
grep -n "mix test" .github/workflows/ci.yml
```

Expected: no **S4** change under `scripts/interop`/`.github/workflows` (S2 may legitimately change `run.py`; that diff belongs to S2); CI's interoperability job runs `run.py ... -- mix test`, which includes the new `:interop`/`:paho_interop` files because `run.py` exports `GRAVITON_MOSQUITTO_BIN` and `GRAVITON_PAHO_PYTHON`.

- [ ] **Step 2: Required sequence**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
export MIX_BUILD_ROOT=/tmp/gmq-build-s4 TMPDIR=/tmp/gmq-s4-tmp
mix deps.get
mix format
mix format --check-formatted
mix compile --warnings-as-errors
mix test
mix graviton_mq.check_architecture
mix help xref
mix xref graph --format cycles --fail-above 0
```

Expected: no formatting diff outside S2/S4 files (`git status --short` shows no new modification from `mix format`), zero warnings, `0 failures` (peer-tagged tests excluded locally), architecture check clean, `No cycles found`. `mix deps.get` must leave `mix.lock` unchanged (`git diff --quiet mix.lock`).

- [ ] **Step 3: Inspect the integrated S4 diff**

```bash
git log --oneline BASE..HEAD
git diff --stat BASE..HEAD -- apps docs
```

The range also contains S2 commits; inspect only S4 commits (conventional scopes `mqtt`, `runtime`, `client` with MQTT paths). Confirm every S4 commit touches only the files listed in its task; confirm no `Co-Authored-By` trailer (`git log BASE..HEAD --format=%B | grep -c Co-Authored-By` prints `0`).

---

## Task 20 (coordinator-serialized): Evidence documents after CI

Run after the coordinator pushes and the CI run for the pushed `HEAD` finishes.

- [ ] **Step 1: Identify the run**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
gh run list --workflow ci.yml --branch main --limit 1 --json databaseId,headSha,conclusion
```

Record `RUN_ID`, `HEAD_SHA` and `conclusion`. If `conclusion` is not `success`, open the logs (`gh run view RUN_ID --log-failed`), diagnose, and fix S4 failures in a new S4 task before any document claims them; do not write PASS.

- [ ] **Step 2: Apply the T0 outcome**

From the CI log, read `mqtt_peer_capabilities_test.exs` and the Paho probe:

- Both Mosquitto probes PASS → no change.
- "accepts max_topic_alias_broker" FAILS → in `C/test/mqtt_feature_mosquitto_test.exs` add `@tag :skip` to "Mosquitto aliases to our client ..." with the comment `# Mosquitto 2.1.2 lacks max_topic_alias_broker (CI run RUN_ID)`, invert the probe to `assert ... =~ "Unknown configuration variable"` (wait for the owner to exit instead of `broker!/3`), and record client inbound alias as qualified only by pure/raw-socket tests.
- "mosquitto_pub --repeat ..." FAILS → change the probe assertion to the observed list, and in `G/test/graviton_mq/mqtt_feature_mosquitto_test.exs` drop the final empty-topic `Enum.any?` assertion (Paho remains the alias-only inbound peer).

Commit any such change as `test(mqtt): record pinned peer capabilities from CI run RUN_ID` with its explicit paths, and have the coordinator rerun CI.

- [ ] **Step 3: Record T0 in the spec** — in `docs/superpowers/specs/2026-11-12-s4-mqtt-alias-subid-shared-design.md` §1 "Peer capabilities", append:

```markdown
T0 re-verification (CI run RUN_ID, HEAD_SHA): Mosquitto 2.1.2
`max_topic_alias_broker` <accepted|rejected>; `mosquitto_pub --repeat ... -D
publish topic-alias 1` sends <full topic then alias-only|observed list>; Paho
2.1.0 manual TopicAlias with empty topic and SubscriptionIdentifier: <pass>.
```

choosing each bracketed value from the CI log (no bracket may remain).

- [ ] **Step 4: `docs/CAPABILITIES.md`** — in the "Independently qualified network roles" table append to the MQTT5 client and broker rows' evidence column (keep existing text):

Broker row: `2026-11-12 S4 (RUN_ID, HEAD_SHA): Topic Alias inbound (TAM 10 default) and optional outbound, Subscription Identifiers, shared subscriptions with round-robin tiers and §4.8.2 redistribution; snapshot v2 with v1 read-time upgrade (`broker_checkpoint_test.exs`, `mqtt/persistence_test.exs`); raw-socket and owner-restart tests (`mqtt_feature_test.exs`, `mqtt_feature_persistence_test.exs`); Paho 2.1.0 and mosquitto_pub/sub peers (`mqtt_feature_paho_test.exs`, `mqtt_feature_mosquitto_test.exs`) PASS.` Change its limits cell "aliases, shared subscriptions and Subscription Identifiers negotiated off" to "Topic Alias inbound on (10), outbound off by default; shared subscriptions and Subscription Identifiers on, each with a switch".

Client row: `2026-11-12 S4 (RUN_ID, HEAD_SHA): optional inbound/outbound Topic Aliases, `subscription_identifier:` option, persistent shared subscriptions (`client_alias_test.exs`, `mqtt_shared_subscription_test.exs`); Mosquitto 2.1.2 both directions (`mqtt_feature_mosquitto_test.exs`) PASS.` Change "Topic Alias Maximum0" to "Topic Alias Maximum configurable (default 0)". Also update the "Current status" paragraph sentence "rejects shared subscriptions before persistent MQTT-client admission" to "checkpoints shared subscriptions in the persistent MQTT-client profile".

If Step 2 recorded a peer limitation, state it in the same cell (for example "Mosquitto→client aliases: peer lacks `max_topic_alias_broker`; raw-socket evidence only").

- [ ] **Step 5: Ledger amendments** — append to `docs/qualification/2026-10-30-mqtt5-requirement-inventory.md` after the 2026-11-08 amendment and to `docs/qualification/2026-10-30-mqtt5-clause-audit.md` before "## Evidence boundaries":

```markdown
## Maintenance amendment — 2026-11-12 (S4)

The ledger rows were not re-run. S4 (`BASE..HEAD_SHA`) changes the status of
`MQTT-3.1.2-26/27`, `MQTT-3.2.2-17/18`, `MQTT-3.3.2-7..12`, `MQTT-3.3.4-3..6`,
`MQTT-3.8.3-4`, `MQTT-4.8.2-1..6`, `U-012`, `U-034`, `U-038..041`, `U-048`,
`U-050`, `U-053`, `U-055`, `U-056` and `U-061`, and the `CPROFILE`, `BASIC`,
`CALIAS`, `OPER` and `SHARED_OFF` catalogs: Topic Alias, Subscription
Identifiers and shared subscriptions are implemented in both roles (spec
`docs/superpowers/specs/2026-11-12-s4-mqtt-alias-subid-shared-design.md`).
No Local on a shared filter now gets DISCONNECT 0x82 (previously 0x81).
`SHARED_OFF` holds with `shared_subscriptions: false`. Evidence: pure
`M/test/{topic,topic_alias,broker_alias,broker_subscription_identifier,broker_shared,broker_shared_redistribution,client_alias,broker_checkpoint,protocol_error_reason}_test.exs`;
runtime `G/test/graviton_mq/mqtt_feature_test.exs`,
`mqtt_feature_persistence_test.exs`, `B/test/mqtt/persistence_test.exs`;
independent peers in CI run RUN_ID (`mqtt_feature_paho_test.exs`,
`mqtt_feature_mosquitto_test.exs`, `C/test/mqtt_feature_mosquitto_test.exs`,
`C/test/mqtt_peer_capabilities_test.exs`). Status `COMMITTED_CI_INTEROP_PASS`.
```

(Use `COMMITTED_LOCAL_PASS_INTEROP_PENDING` and omit the CI clause if no passing run exists yet.) The JSON inventory is not edited (D10).

- [ ] **Step 6: Commit**

```bash
cd /home/gao/Workspace/gsmlg-dev/graviton_mq
git add docs/superpowers/plans/2026-11-12-s4-mqtt-alias-subid-shared.md
git commit -m "docs(mqtt): record S4 qualification evidence" -- docs/CAPABILITIES.md docs/qualification/2026-10-30-mqtt5-requirement-inventory.md docs/qualification/2026-10-30-mqtt5-clause-audit.md docs/superpowers/specs/2026-11-12-s4-mqtt-alias-subid-shared-design.md docs/superpowers/plans/2026-11-12-s4-mqtt-alias-subid-shared.md
```

(The spec is already tracked since `8a31f2a`. Committing this plan is the coordinator's choice; drop it from both commands if it is committed elsewhere.)

---

## Spec coverage checklist

| Spec item | Task |
| --- | --- |
| §4.1 `parse_filter/1`, `valid_subscription_filter?/1`, inner `$` rule | 1 |
| §4.2 tables, resolution order 1–5, outbound first-N, reset per connection | 2, 4, 5, 13 |
| §4.3 Sub-ID store/replace/route/refresh/retained/recipient, 0xA1 switch | 7 |
| §4.4 membership, authorization, no retained, routing tiers, two copies, redistribution table, `shared_discarded` | 8, 9 |
| §4.5 limits and `max_alias_bytes` (D4) | 3 |
| §4.6 v2 keys, filter validation, v1 upgrade, same WAL type, atoms, mismatch, no capture of tables/cursor | 6, 7, 8 |
| §4.7 client options, profile rule removed, inbound resolution, Q15, caller alias rejected | 13 |
| §5 runtime switches, status, persistence recovery test | 3, 7, 8, 11, 12 |
| §6 client options, `subscription_identifier:`, persistent shared, checkpoint alias invariant, docs | 14, 18 |
| §7 interop matrix both directions, wire tap | 15, 16, 17 |
| §8 T0 peer re-verification and spec record | 15, 16, 20 |
| Q14 0x82 | 10 |
| Durable compatibility: v1 written by pre-S4 code recovers; disabled feature → `:checkpoint_capability_mismatch` | 0, 6, 7, 8, 12 |
| CAPABILITIES / ledgers with executed evidence only | 20 |
