# Boolean and Detach Codec Implementation Plan

> **For agentic workers:** Use the subagent-driven-development workflow to
> implement the bounded tasks below with TDD and independent reviews.

**Goal:** Add tagged Boolean wire values and the immutable Detach schema to the
pure AMQP codec, including the approved recursive Boolean subset expansion.

**Architecture:** Extend the existing value codec and performative facade;
reuse private nested Error validation with a parameterized outer index. Keep
frames opaque and leave runtime ownership and protocol behavior outside scope.

**Tech Stack:** Elixir 1.18.4, Erlang/OTP 28.5.0.1, Mix umbrella, ExUnit; no
additional dependencies.

**Status:** completed; all tasks and independent reviews passed.
Implementation happened on `codex/boolean-detach-codec` in
`.trees/boolean-detach-codec` and was initially handed off uncommitted.
On 2026-10-10, the owner authorized committing, merging into `main`, cleaning
up the worktree, and pushing to the remote. The existing End/Close/Error commit
was verified separately and published in Task 1.

## Task 1: Verify and publish the existing End/Close/Error commit

- [x] Confirm the exact pinned toolchain and clean `main` at `9f86a4b`.
- [x] Run all eight required commands and inspect the entire commit diff.
- [x] Push `main` without force; verify the remote SHA and that commit's CI.

Evidence: 172 tests, zero warnings, architecture check passed (1322 references
across five apps), no xref cycles, and remote `main` equals
`9f86a4be25ea20837353b3f3e2d2ebb51a845f71`.
[Hosted CI](https://github.com/gsmlg-dev/graviton_mq/actions/runs/37945794170)
passed for the same SHA.

## Task 2: Extend Boolean scalar support with independent tests

**Files:**

- Modify `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/value.ex`.
- Modify the existing value primitive/compound tests.
- Create `apps/graviton_mq_amqp10/test/codec/value_boolean_test.exs`.
- Modify `apps/graviton_mq_amqp10/test/codec/performative_contract_test.exs`
  and `end_close_error_test.exs` for the approved subset expansion.

- [x] Add tests for these independent Boolean fixtures before production code:

  ```elixir
  for {wire, value} <- [
        {<<0x41>>, AMQPValue.boolean(true)},
        {<<0x42>>, AMQPValue.boolean(false)},
        {<<0x56, 0>>, AMQPValue.boolean(false)},
        {<<0x56, 1>>, AMQPValue.boolean(true)}
      ] do
    assert {:ok, ^value, <<0xFE>>} = Value.decode(wire <> <<0xFE>>)
  end
  assert {:more, 1} = Value.decode(<<0x56>>)
  assert {:ok, <<0x41>>} = Value.encode(AMQPValue.boolean(true))
  assert {:ok, <<0x42>>} = Value.encode(AMQPValue.boolean(false))
  ```

- [x] Test octets `2..255` as malformed Boolean payloads with exact error
  details; malformed outbound tagged values must return invalid-value errors.
  Add recursive Boolean list/map/described fixtures and canonical map tests.
- [x] Watch the new tests fail with unsupported-format/type results.
- [x] Implement the minimum Boolean decode/encode clauses and add `:boolean`
  to the supported semantic types. Keep arrays symbol-only.
- [x] Update primitive supported-format/type sets; use still-unsupported ubyte
  or binary fixtures where old tests asserted Boolean unsupported.
- [x] Prove Booleans in Open/Begin properties and End/Close Error info with
  independent wire bytes; preserve unsupported primitive propagation tests.
- [x] Run `mix test apps/graviton_mq_amqp10/test/codec` and pass specification
  and code-quality reviews for this task.

Evidence: the red run had 65 tests and 15 expected failures against the old
Boolean-unsupported codec. The completed codec suite passed 126 tests with
zero failures; independent specification and code-quality reviews approved it.

## Task 3: Add Detach and reuse nested Error

**Files:**

- Create `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative/detach.ex`.
- Modify `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative.ex`.
- Modify `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/performative.ex`.
- Create `apps/graviton_mq_amqp10/test/codec/detach_performative_test.exs`.
- Modify `apps/graviton_mq_amqp10/test/boundaries_test.exs`.

- [x] Before adding the module, test its schema contract using dynamic struct
  construction so the test can fail on a missing module rather than compilation:

  ```elixir
  module = GravitonMQ.AMQP10.Performative.Detach
  assert Code.ensure_loaded?(module)
  detach = struct(module, handle: AMQPValue.uint(0))
  assert {:ok, <<0x00, 0x53, 0x16, 0xC0, 2, 1, 0x43>>} =
           Performative.encode(detach)
  ```

- [x] Watch that contract fail, then add the immutable struct:

  ```elixir
  defstruct [:handle, :closed, :error]
  ```

  Its types are required tagged uint handle, optional tagged Boolean closed,
  and optional typed Error. Add no startup, process, or transition API.
- [x] Add independent decode/encode fixtures for the numeric and symbolic
  descriptors, full-width descriptors, list8/list32, defaults, null holes,
  nested Error, and exact suffixes. Watch the facade reject them before adding
  dispatch and schema validation.
- [x] Add only descriptor `0x16` / `amqp:detach:list`; validate at most three
  positions, required tagged uint, and tagged Boolean false default. Canonical
  fields are `[handle, omit_default(closed, :boolean, false), error_value]`.
- [x] Parameterize private nested Error field/value/descriptor/body helpers
  with the outer index. End and Close pass `0`, Detach passes `2`. Preserve
  nested Error's own field indexes and existing error result shape.
- [x] Prove wrong fields, invalid tagged payloads, missing handles, excess
  fields, malformed Error descriptors/bodies/fields, Boolean info, unsupported
  primitives, every strict prefix, and size/count/depth limits in both directions.
- [x] Include Detach in `Performative.t()`, facade specs, and boundary tests.
- [x] Run all AMQP codec and boundary tests; pass specification review followed
  by code-quality review.

Evidence: Detach's red run had 20 tests with 18 expected missing-feature
failures; the final codec and boundary suite passed 150 tests. Independent
specification and final code-quality reviews approved the implemented slice.

## Task 4: Document and verify the implemented boundary

**Files:** `AGENTS.md`, `README.md`, `docs/AMQP10_SCOPE.md`,
`docs/ARCHITECTURE.md`, `docs/ROADMAP.md`, `docs/RESEARCH_NOTES.md`, this plan,
and its design.

- [x] Record the Boolean/Detach slice separately from historical milestones;
  update the current supported surface and remove Boolean/Detach from current
  unsupported lists. Explain the recursive properties/Error-info expansion.
- [x] Preserve exclusions and the next bounded Flow/Attach guidance.
- [x] Run the exact required sequence from the worktree umbrella root:

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

- [x] Inspect the complete tracked diff and every untracked new file; run
  `git diff --check`. Confirm no changes outside the design's file scope.
- [x] Complete final independent reviews and mark design/plan completed.
- [x] Report exact verification evidence and preserve the initial uncommitted
  worktree for owner review, without creating a new commit, PR, or release
  at that stage.

Final verification: all eight commands exited zero under Elixir 1.18.4 and
Erlang/OTP 28.5.0.1. Full umbrella tests passed 208 tests (core 9, AMQP 172,
storage 3, runtime 2, public 22), warning-free compilation passed, architecture
checked 1359 references across five apps, and xref found no cycles. The scope
audit covered all 19 tracked/new files with no changes outside the approved
scope; `git diff --check` passed. The feature was uncommitted on
`codex/boolean-detach-codec` at the initial handoff.
