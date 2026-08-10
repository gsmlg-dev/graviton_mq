# Open and Begin Performative Codec Implementation Plan

> Status: Completed historical implementation record. The current
> authoritative scope is `AGENTS.md` and `README.md`; unchecked checklist
> markers are retained as authored and are not outstanding work. No commit was
> authorized when this plan was written; the later explicit first-release
> request superseded that historical constraint.

**Goal:** Add a pure, bounded AMQP 1.0 Open/Begin schema codec over the existing
semantic value codec without adding protocol behavior or transport.

**Architecture:** Dedicated immutable Open and Begin structs preserve exact
tagged AMQP field values. `Codec.Performative` delegates binary constructors to
`Codec.Value`, validates only the two composite schemas, and returns the exact
unconsumed suffix.

### Task 1: Establish the public model and codec contract

**Files:**

- Create `apps/graviton_mq_amqp10/test/codec/performative_contract_test.exs`
- Create `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative/open.ex`
- Create `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative/begin.ex`
- Create `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/performative.ex`
- Modify `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative.ex`
- Modify `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/error.ex`
- Modify `apps/graviton_mq_amqp10/test/boundaries_test.exs`

1. Write tests for the two structs, codec result API, new structured error
   operations, and exported pure functions.
2. Run the focused test and confirm it fails because the modules do not exist.
3. Add the minimal structs, types, error operations, and codec entry points.
4. Run the focused test until it passes.

### Task 2: Decode and validate Open

**Files:**

- Create `apps/graviton_mq_amqp10/test/codec/open_performative_test.exs`
- Modify `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/performative.ex`

1. Add exact binary fixtures for numeric and symbolic Open descriptors,
   defaults, multiple symbol forms, property maps, trailing input, and every
   strict prefix.
2. Add malformed fixtures for missing/null `container-id`, wrong exact field
   types, a frame size below 512, non-symbol property keys, non-list bodies,
   and extra fields.
3. Run the focused test and confirm the schema cases fail.
4. Implement Open descriptor dispatch, positional validation, and default
   materialization.
5. Run the focused test until it passes.

### Task 3: Decode and validate Begin

**Files:**

- Create `apps/graviton_mq_amqp10/test/codec/begin_performative_test.exs`
- Modify `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/performative.ex`

1. Add exact numeric and symbolic Begin fixtures, optional remote channel,
   default handle maximum, capabilities, properties, suffixes, and strict
   prefixes.
2. Add malformed fixtures for missing/null mandatory window fields, wrong
   exact field types, bad property keys, and extra fields.
3. Run the focused test and confirm the schema cases fail.
4. Implement Begin positional validation and default materialization without
   enforcing connection/session context.
5. Run the focused test until it passes.

### Task 4: Canonically encode both performatives

**Files:**

- Modify `apps/graviton_mq_amqp10/test/codec/open_performative_test.exs`
- Modify `apps/graviton_mq_amqp10/test/codec/begin_performative_test.exs`
- Modify `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/performative.ex`

1. Add round-trip and exact canonical-byte assertions.
2. Add invalid outbound struct assertions for mandatory fields, exact types,
   range constraints, multiple fields, maps, and unsupported structs.
3. Run both focused tests and confirm encoding cases fail.
4. Implement field-list construction, default-to-null normalization, trailing
   null trimming, numeric descriptors, and structured invalid-value errors.
5. Run all codec tests until they pass.

### Task 5: Publish the exact boundary

**Files:**

- Modify `README.md`
- Modify `docs/ARCHITECTURE.md`
- Modify `docs/AMQP10_SCOPE.md`
- Modify `docs/RESEARCH_NOTES.md`
- Modify `docs/ROADMAP.md`
- Modify `AGENTS.md`

1. Replace statements that Open and Begin are only generic described values
   with the new bounded schema-codec surface.
2. Keep explicit exclusions for protocol transitions, negotiation, other
   performatives, message sections, transport, queue behavior, and storage.
3. State that exact AMQP values and opaque frame/message bytes remain
   preserved and that this is not full AMQP compatibility.

### Task 6: Verify and review

1. Run all AMQP codec tests.
2. Run the complete repository verification sequence from `AGENTS.md`.
3. Inspect the complete Git diff and `git diff --check`.
4. Confirm no AMQP 0-9-1 or copied RabbitMQ source, process/network/storage
   behavior, message parsing, or later performative support was introduced.
