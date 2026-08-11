# End, Close, and Error Codec Implementation Plan

> Status: Completed historical implementation record. The current
> authoritative scope is `AGENTS.md` and `README.md`; unchecked checklist
> markers are retained as authored and are not outstanding work. No commit was
> authorized during implementation; the later explicit local-merge request
> superseded that historical constraint.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pure, bounded AMQP 1.0 schema codec for End, Close, and their nested Error composite without adding protocol behavior or widening the semantic value codec.

**Architecture:** Keep `Codec.Performative` as the only public performative binary facade. Add immutable End and Close structs, decode nested Error data into the existing `GravitonMQ.AMQP10.Error` struct, and build Error as a private tagged described value before delegating binary encoding to `Codec.Value`. Preserve the current process-free namespace, result shapes, limits, exact remainder handling, and Open/Begin behavior.

**Tech Stack:** Elixir 1.18.4, Erlang/OTP 28.5.0.1, Mix umbrella, ExUnit, the existing pure AMQP value codec, and `mix graviton_mq.check_architecture`.

---

## Execution constraints

- Work from the umbrella root. If execution uses a worktree, create it under
  `.trees/codex/end-close-error-codec` as required by `AGENTS.md`.
- The approved design is
  `docs/superpowers/specs/2026-08-11-end-close-error-codec-design.md`.
- When execution began, the approved design and this plan were uncommitted
  workspace files. They were preserved verbatim in the isolated worktree before
  code changes and updated only after verification to record completion.
- Do not commit unless the user separately authorizes commits. This plan uses
  verification checkpoints instead of commit steps.
- Modify only the AMQP 1.0 codec/model/tests, the AMQP boundary test, and the
  scope documents named by the approved design. Do not fix unrelated failures.
- Keep all code changes inside the exact post-Milestone-1 End/Close codec slice.

## File map

**Create:**

- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative/end.ex` — immutable End data.
- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative/close.ex` — immutable Close data.
- `apps/graviton_mq_amqp10/test/codec/end_close_performative_test.exs` — outer End/Close schema and canonical encoding tests.
- `apps/graviton_mq_amqp10/test/codec/end_close_error_test.exs` — nested Error schema, error propagation, and limit tests.

**Modify:**

- `AGENTS.md` — authorize this exact slice while preserving exclusions.
- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative.ex` — expand the performative union.
- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/performative.ex` — descriptor dispatch, validation, and canonical encoding.
- `apps/graviton_mq_amqp10/test/codec/performative_contract_test.exs` — public type and unchanged facade contract.
- `apps/graviton_mq_amqp10/test/boundaries_test.exs` — loadability and process-free public boundary.
- `README.md` — current supported codec surface and next boundary.
- `docs/AMQP10_SCOPE.md` — exact End/Close/Error schema and exclusions.
- `docs/ARCHITECTURE.md` — pure codec responsibilities and non-capabilities.
- `docs/RESEARCH_NOTES.md` — record the bounded schema decision.
- `docs/ROADMAP.md` — record the completed post-Milestone-1 slice without redefining Milestone 1.

No dependency declarations, runtime modules, storage modules, core modules,
frame decoding, or value constructors change.

### Task 1: Advance the governing scope before code

**Files:**

- Modify: `AGENTS.md:62-80`
- Modify: `AGENTS.md:134-144`

- [ ] **Step 1: Confirm the approved documents and clean baseline are present**

Run:

```bash
git status --short --branch
test -f docs/superpowers/specs/2026-08-11-end-close-error-codec-design.md
test -f docs/superpowers/plans/2026-08-11-end-close-error-codec.md
```

Expected: the selected checkout is based on `main`; the approved design and
plan are present; no unrelated tracked edits exist.

- [ ] **Step 2: Replace the Open/Begin-only data rule with the approved exact surface**

In `AGENTS.md`, replace the first bounded-performative bullet with:

```markdown
- The bounded performative codec supports Open, Begin, End, and Close. Decode
  their standard numeric and symbolic descriptors into dedicated immutable
  structs and encode the standard numeric descriptors. End and Close carry
  only an optional nested `GravitonMQ.AMQP10.Error`; Error is protocol data, not
  a top-level performative, and its condition, description, and info fields
  retain exact tagged AMQP values.
```

Immediately after the existing Open/Begin schema/default bullets, add:

```markdown
- End and Close each have one optional `error` field. A nested Error has
  mandatory tagged `symbol` condition, optional tagged `string` description,
  and optional ordered tagged `map` info with symbol keys. Accept numeric and
  symbolic Error descriptors only in that nested position; encode the numeric
  descriptor. Do not whitelist error-condition symbols or widen the bounded
  value subset for Error info.
```

- [ ] **Step 3: Replace the current milestone exclusion paragraph**

Use this exact governing text under `## Current milestone exclusions`:

```markdown
The approved post-Milestone-1 End/Close codec slice extends the process-free
codec only with the End and Close performative schemas and their nested Error
composite. Milestone 1 remains the completed Open/Begin foundation. Do not
extend the current slice to other performative schemas, message-section
parsing, protocol or SASL negotiation, Connection/Session/Link behavior, Flow,
Transfer, Disposition, queue transitions or scheduling, publisher or consumer
delivery, TCP/TLS/WebSocket, filesystem WAL/segments/fsync/recovery, Raft,
clustering, Phoenix, management APIs or UI, MQTT, or AMQP 0-9-1. Do not add
Phoenix, Ra, Khepri, Ranch, Bandit, or speculative runtime dependencies. Do not
claim full AMQP 1.0 compatibility.
```

- [ ] **Step 4: Verify the governing scope is exact**

Run:

```bash
rg -n "supports Open, Begin, End, and Close|nested Error|post-Milestone-1 End/Close|other performative schemas" AGENTS.md
git diff --check -- AGENTS.md
```

Expected: all four phrases are present and `git diff --check` prints nothing.

### Task 2: Add the immutable public structs and type contract

**Files:**

- Create: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative/end.ex`
- Create: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative/close.ex`
- Modify: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative.ex:10-24`
- Modify: `apps/graviton_mq_amqp10/test/codec/performative_contract_test.exs:4-54`
- Modify: `apps/graviton_mq_amqp10/test/boundaries_test.exs:4-68`

- [ ] **Step 1: Write failing public-contract tests**

Add these aliases to `performative_contract_test.exs`:

```elixir
alias GravitonMQ.AMQP10.Error, as: ProtocolError
alias GravitonMQ.AMQP10.Performative.Close
alias GravitonMQ.AMQP10.Performative.End
```

Add this test after the Begin struct test:

```elixir
test "End and Close retain an optional typed protocol error" do
  error = %ProtocolError{
    condition: AMQPValue.symbol("vendor:condition"),
    description: AMQPValue.string("diagnostic"),
    info: AMQPValue.map([{AMQPValue.symbol("code"), AMQPValue.uint(7)}])
  }

  assert %End{error: ^error} = %End{error: error}
  assert %Close{error: ^error} = %Close{error: error}
  assert %End{error: nil} = %End{}
  assert %Close{error: nil} = %Close{}
end
```

In `boundaries_test.exs`, add End and Close to `@modules`, to the bounded codec
module list, and to the list that must not export `start_link/1` or
`child_spec/1`.

- [ ] **Step 2: Run the tests and verify the new modules are missing**

Run:

```bash
mix test apps/graviton_mq_amqp10/test/codec/performative_contract_test.exs apps/graviton_mq_amqp10/test/boundaries_test.exs
```

Expected: compilation fails because `GravitonMQ.AMQP10.Performative.End` and
`GravitonMQ.AMQP10.Performative.Close` do not exist.

- [ ] **Step 3: Add the End struct**

Create `performative/end.ex` with:

```elixir
defmodule GravitonMQ.AMQP10.Performative.End do
  @moduledoc """
  Immutable AMQP 1.0 End performative data.

  This struct does not end a Session. It retains only the optional protocol
  error decoded by the pure schema codec.
  """

  defstruct error: nil

  @type t :: %__MODULE__{
          error: GravitonMQ.AMQP10.Error.t() | nil
        }
end
```

- [ ] **Step 4: Add the Close struct**

Create `performative/close.ex` with:

```elixir
defmodule GravitonMQ.AMQP10.Performative.Close do
  @moduledoc """
  Immutable AMQP 1.0 Close performative data.

  This struct does not close a Connection. It retains only the optional
  protocol error decoded by the pure schema codec.
  """

  defstruct error: nil

  @type t :: %__MODULE__{
          error: GravitonMQ.AMQP10.Error.t() | nil
        }
end
```

- [ ] **Step 5: Expand the performative union without adding behavior**

Add aliases for End and Close in `performative.ex`, and replace its `t()` type
with:

```elixir
@type t :: Open.t() | Begin.t() | End.t() | Close.t()
```

Update the module documentation to state that Open, Begin, End, and Close are
modeled, and that none of the structs executes protocol transitions.

- [ ] **Step 6: Run the contract tests**

Run:

```bash
mix test apps/graviton_mq_amqp10/test/codec/performative_contract_test.exs apps/graviton_mq_amqp10/test/boundaries_test.exs
```

Expected: all tests pass; no process or lifecycle functions are exported by the
new modules.

### Task 3: Decode the outer End and Close schemas

**Files:**

- Create: `apps/graviton_mq_amqp10/test/codec/end_close_performative_test.exs`
- Modify: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/performative.ex:11-115`

- [ ] **Step 1: Write failing outer-schema decode tests**

Create `end_close_performative_test.exs` with:

```elixir
defmodule GravitonMQ.AMQP10.Codec.EndClosePerformativeTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Performative
  alias GravitonMQ.AMQP10.Performative.Close
  alias GravitonMQ.AMQP10.Performative.End

  @empty_end <<0x00, 0x53, 0x17, 0x45>>
  @symbolic_end <<0x00, 0xA3, 13, "amqp:end:list", 0x45>>
  @null_end <<0x00, 0x53, 0x17, 0xC0, 2, 1, 0x40>>
  @empty_close <<0x00, 0x53, 0x18, 0x45>>
  @symbolic_close <<0x00, 0xA3, 15, "amqp:close:list", 0x45>>

  test "numeric and symbolic End descriptors normalize to End" do
    for fixture <- [@empty_end, @symbolic_end, @null_end] do
      assert {:ok, %End{error: nil}, <<>>} = Performative.decode(fixture)
    end
  end

  test "numeric and symbolic Close descriptors normalize to Close" do
    for fixture <- [@empty_close, @symbolic_close] do
      assert {:ok, %Close{error: nil}, <<>>} = Performative.decode(fixture)
    end
  end

  test "returns the exact suffix after one termination performative" do
    suffix = <<0xDE, 0xAD, 0xBE, 0xEF>>
    assert {:ok, %End{}, ^suffix} = Performative.decode(@empty_end <> suffix)
    assert {:ok, %Close{}, ^suffix} = Performative.decode(@empty_close <> suffix)
  end

  test "known descriptors require list bodies with at most one field" do
    for fixture <- [
          <<0x00, 0x53, 0x17, 0x40>>,
          <<0x00, 0x53, 0x18, 0x40>>,
          <<0x00, 0x53, 0x17, 0xC0, 3, 2, 0x40, 0x40>>,
          <<0x00, 0x53, 0x18, 0xC0, 3, 2, 0x40, 0x40>>
        ] do
      assert {:error,
              %Error{
                operation: :performative_decode,
                class: :malformed
              }} = Performative.decode(fixture)
    end
  end
end
```

- [ ] **Step 2: Run the new test and verify descriptor dispatch fails**

Run:

```bash
mix test apps/graviton_mq_amqp10/test/codec/end_close_performative_test.exs
```

Expected: valid End and Close fixtures fail with an unsupported unknown
descriptor because dispatch still recognizes only Open and Begin.

- [ ] **Step 3: Add aliases, constants, and type signatures**

In `codec/performative.ex`, add:

```elixir
alias GravitonMQ.AMQP10.Performative.Close
alias GravitonMQ.AMQP10.Performative.End

@end_descriptor 0x17
@end_symbol "amqp:end:list"
@end_field_count 1

@close_descriptor 0x18
@close_symbol "amqp:close:list"
@close_field_count 1

@protocol_error_descriptor 0x1D
@protocol_error_symbol "amqp:error:list"
@protocol_error_field_count 3
```

Change both decode specs to return:

```elixir
Codec.decode_result(Open.t() | Begin.t() | End.t() | Close.t())
```

Change both encode specs to accept:

```elixir
Open.t() | Begin.t() | End.t() | Close.t()
```

- [ ] **Step 4: Extend descriptor dispatch and body dispatch**

Extend the existing case in `decode_value/1` with:

```elixir
:end -> decode_body(:end, body)
:close -> decode_body(:close, body)
```

Add these descriptor clauses immediately before the existing
`descriptor_name(_descriptor)` catch-all:

```elixir
defp descriptor_name(%AMQPValue{type: :ulong, value: @end_descriptor}), do: :end
defp descriptor_name(%AMQPValue{type: :symbol, value: @end_symbol}), do: :end
defp descriptor_name(%AMQPValue{type: :ulong, value: @close_descriptor}), do: :close
defp descriptor_name(%AMQPValue{type: :symbol, value: @close_symbol}), do: :close
```

Add these body clauses immediately before the existing catch-all
`decode_body(_name, _body)` clause:

```elixir
defp decode_body(:end, %AMQPValue{type: :list, value: fields}) when is_list(fields),
  do: validate_end(fields, :performative_decode, :malformed)

defp decode_body(:close, %AMQPValue{type: :list, value: fields}) when is_list(fields),
  do: validate_close(fields, :performative_decode, :malformed)

defp decode_body(name, body) when name in [:end, :close] do
  outer_schema_error(name, :performative_decode, :malformed, :invalid_performative_body,
    expected: :list,
    actual: semantic_type(body)
  )
end
```

- [ ] **Step 5: Add minimal outer-schema validation**

Add these functions. The described Error clause is intentionally added in the
next task after its failing tests exist.

```elixir
defp validate_end(fields, operation, class) do
  with :ok <-
         validate_termination_field_count(
           fields,
           @end_field_count,
           :end,
           operation,
           class
         ),
       {:ok, error} <- termination_error_field(fields, :end, operation, class) do
    {:ok, %End{error: error}}
  end
end

defp validate_close(fields, operation, class) do
  with :ok <-
         validate_termination_field_count(
           fields,
           @close_field_count,
           :close,
           operation,
           class
         ),
       {:ok, error} <- termination_error_field(fields, :close, operation, class) do
    {:ok, %Close{error: error}}
  end
end

defp validate_termination_field_count(
       fields,
       maximum,
       _performative,
       _operation,
       _class
     )
     when length(fields) <= maximum,
     do: :ok

defp validate_termination_field_count(fields, maximum, performative, operation, class) do
  outer_schema_error(performative, operation, class, :too_many_fields,
    actual: length(fields),
    maximum: maximum
  )
end

defp termination_error_field(fields, performative, operation, class) do
  case Enum.at(fields, 0) do
    value when absent?(value) ->
      {:ok, nil}

    value ->
      outer_schema_error(performative, operation, class, :field_type_mismatch,
        field: :error,
        index: 0,
        expected: :error,
        actual: semantic_type(value)
      )
  end
end

defp outer_schema_error(performative, operation, class, reason, details \\ []) do
  schema_error(
    operation,
    class,
    reason,
    Keyword.put(details, :performative, performative)
  )
end
```

- [ ] **Step 6: Run the outer-schema tests and existing Open/Begin tests**

Run:

```bash
mix test apps/graviton_mq_amqp10/test/codec/end_close_performative_test.exs apps/graviton_mq_amqp10/test/codec/open_performative_test.exs apps/graviton_mq_amqp10/test/codec/begin_performative_test.exs
```

Expected: all tests pass. Open and Begin behavior remains unchanged.

### Task 4: Decode and validate nested Error data

**Files:**

- Create: `apps/graviton_mq_amqp10/test/codec/end_close_error_test.exs`
- Modify: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/performative.ex`

- [ ] **Step 1: Write failing valid nested-Error tests with independent fixtures**

Start `end_close_error_test.exs` with:

```elixir
defmodule GravitonMQ.AMQP10.Codec.EndCloseErrorTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Performative
  alias GravitonMQ.AMQP10.Error, as: ProtocolError
  alias GravitonMQ.AMQP10.Performative.Close
  alias GravitonMQ.AMQP10.Performative.End
  alias GravitonMQ.AMQP10.Value, as: AMQPValue

  test "decodes a numeric Error with an extension condition inside End" do
    fixture = end_with_error(numeric_error(<<0xA3, 16, "vendor:condition">>, 1))

    assert {:ok,
            %End{
              error: %ProtocolError{
                condition: %AMQPValue{type: :symbol, value: "vendor:condition"},
                description: nil,
                info: nil
              }
            }, <<>>} = Performative.decode(fixture)
  end

  test "decodes a symbolic Error descriptor inside Close" do
    fixture = close_with_error(symbolic_error(<<0xA3, 1, "x">>, 1))

    assert {:ok,
            %Close{
              error: %ProtocolError{
                condition: %AMQPValue{type: :symbol, value: "x"}
              }
            }, <<>>} = Performative.decode(fixture)
  end

  test "retains full Error fields and ordered info entries" do
    fields =
      <<
        0xA3,
        1,
        "x",
        0xA1,
        1,
        "d",
        0xC1,
        11,
        4,
        0xA3,
        1,
        "a",
        0x52,
        1,
        0xA3,
        1,
        "b",
        0x52,
        2
      >>

    assert {:ok, %End{error: %ProtocolError{} = error}, <<>>} =
             fields |> numeric_error(3) |> end_with_error() |> Performative.decode()

    assert error.condition == AMQPValue.symbol("x")
    assert error.description == AMQPValue.string("d")

    assert error.info ==
             AMQPValue.map([
               {AMQPValue.symbol("a"), AMQPValue.uint(1)},
               {AMQPValue.symbol("b"), AMQPValue.uint(2)}
             ])
  end

  defp numeric_error(fields, count) do
    size = byte_size(fields) + 1
    <<0x00, 0x53, 0x1D, 0xC0, size, count, fields::binary>>
  end

  defp symbolic_error(fields, count) do
    size = byte_size(fields) + 1
    <<0x00, 0xA3, 15, "amqp:error:list", 0xC0, size, count, fields::binary>>
  end

  defp end_with_error(error_value) do
    size = byte_size(error_value) + 1
    <<0x00, 0x53, 0x17, 0xC0, size, 1, error_value::binary>>
  end

  defp close_with_error(error_value) do
    size = byte_size(error_value) + 1
    <<0x00, 0x53, 0x18, 0xC0, size, 1, error_value::binary>>
  end
end
```

- [ ] **Step 2: Add failing malformed nested-Error tests**

Add these tests before the helper functions:

```elixir
test "rejects an unknown nested Error descriptor as malformed End data" do
  fixture = end_with_error(<<0x00, 0x53, 0x7F, 0x45>>)

  assert {:error,
          %Error{
            operation: :performative_decode,
            class: :malformed,
            reason: :invalid_error_descriptor,
            details: %{
              performative: :end,
              field: :error,
              index: 0,
              expected: :error,
              actual: :described,
              descriptor: %AMQPValue{}
            }
          }} = Performative.decode(fixture)
end

test "requires the nested Error condition" do
  for error_value <- [numeric_error(<<>>, 0), numeric_error(<<0x40>>, 1)] do
    assert {:error,
            %Error{
              operation: :performative_decode,
              class: :malformed,
              reason: :mandatory_field_missing,
              details: %{
                performative: :end,
                nested_schema: :error,
                field: :condition,
                index: 0
              }
            }} = error_value |> end_with_error() |> Performative.decode()
  end
end

test "validates exact Error field types and info keys" do
  wrong_condition = numeric_error(<<0xA1, 1, "x">>, 1)
  wrong_description = numeric_error(<<0xA3, 1, "x", 0x52, 1>>, 2)
  wrong_info = numeric_error(<<0xA3, 1, "x", 0x40, 0x52, 1>>, 3)

  bad_info_key =
    numeric_error(
      <<0xA3, 1, "x", 0x40, 0xC1, 6, 2, 0xA1, 1, "k", 0x52, 1>>,
      3
    )

  assert {:error,
          %Error{
            class: :malformed,
            reason: :field_type_mismatch,
            details: %{
              performative: :end,
              nested_schema: :error,
              field: :condition,
              index: 0,
              expected: :symbol,
              actual: :string
            }
          }} = wrong_condition |> end_with_error() |> Performative.decode()

  assert {:error,
          %Error{
            class: :malformed,
            reason: :field_type_mismatch,
            details: %{
              performative: :end,
              nested_schema: :error,
              field: :description,
              index: 1,
              expected: :string,
              actual: :uint
            }
          }} = wrong_description |> end_with_error() |> Performative.decode()

  assert {:error,
          %Error{
            class: :malformed,
            reason: :field_type_mismatch,
            details: %{
              performative: :end,
              nested_schema: :error,
              field: :info,
              index: 2,
              expected: :map,
              actual: :uint
            }
          }} = wrong_info |> end_with_error() |> Performative.decode()

  assert {:error,
          %Error{
            class: :malformed,
            reason: :invalid_property_key,
            details: %{
              performative: :end,
              nested_schema: :error,
              field: :info,
              index: 2
            }
          }} = bad_info_key |> end_with_error() |> Performative.decode()
end

test "rejects non-list and overlong Error bodies" do
  non_list = end_with_error(<<0x00, 0x53, 0x1D, 0x40>>)

  overlong =
    <<0xA3, 1, "x", 0x40, 0x40, 0x40>>
    |> numeric_error(4)
    |> end_with_error()

  for fixture <- [non_list, overlong] do
    assert {:error, %Error{operation: :performative_decode, class: :malformed}} =
             Performative.decode(fixture)
  end
end

test "does not dispatch Error as a top-level performative" do
  fixture = numeric_error(<<0xA3, 1, "x">>, 1)

  assert {:error,
          %Error{
            operation: :performative_decode,
            class: :unsupported,
            reason: :unknown_descriptor
          }} = Performative.decode(fixture)
end
```

- [ ] **Step 3: Run the nested tests and verify valid Error data fails**

Run:

```bash
mix test apps/graviton_mq_amqp10/test/codec/end_close_error_test.exs
```

Expected: valid nested Error fixtures fail because `termination_error_field/4`
still rejects every present value.

- [ ] **Step 4: Add the protocol Error alias and descriptor recognition**

In `codec/performative.ex`, add:

```elixir
alias GravitonMQ.AMQP10.Error, as: ProtocolError
```

Replace `termination_error_field/4` with:

```elixir
defp termination_error_field(fields, performative, operation, class) do
  case Enum.at(fields, 0) do
    value when absent?(value) ->
      {:ok, nil}

    %AMQPValue{type: :described} = value ->
      decode_protocol_error(value, performative, operation, class)

    value ->
      outer_schema_error(performative, operation, class, :field_type_mismatch,
        field: :error,
        index: 0,
        expected: :error,
        actual: semantic_type(value)
      )
  end
end
```

Add:

```elixir
defp decode_protocol_error(
       %AMQPValue{
         type: :described,
         value: %Described{descriptor: descriptor, value: body}
       },
       performative,
       operation,
       class
     ) do
  if protocol_error_descriptor?(descriptor) do
    case body do
      %AMQPValue{type: :list, value: fields} when is_list(fields) ->
        validate_protocol_error(fields, performative, operation, class)

      value ->
        nested_schema_error(performative, operation, class, :invalid_error_body,
          field: :error,
          index: 0,
          expected: :list,
          actual: semantic_type(value)
        )
    end
  else
    outer_schema_error(performative, operation, class, :invalid_error_descriptor,
      field: :error,
      index: 0,
      expected: :error,
      actual: :described,
      descriptor: descriptor
    )
  end
end

defp protocol_error_descriptor?(%AMQPValue{
       type: :ulong,
       value: @protocol_error_descriptor
     }),
     do: true

defp protocol_error_descriptor?(%AMQPValue{
       type: :symbol,
       value: @protocol_error_symbol
     }),
     do: true

defp protocol_error_descriptor?(_descriptor), do: false
```

- [ ] **Step 5: Add exact nested Error field validation**

Add:

```elixir
defp validate_protocol_error(fields, performative, operation, class) do
  with :ok <-
         validate_protocol_error_field_count(fields, performative, operation, class),
       {:ok, condition} <-
         required_protocol_error_field(
           fields,
           0,
           :condition,
           :symbol,
           performative,
           operation,
           class
         ),
       {:ok, description} <-
         optional_protocol_error_field(
           fields,
           1,
           :description,
           :string,
           performative,
           operation,
           class
         ),
       {:ok, info} <- protocol_error_info_field(fields, performative, operation, class) do
    {:ok, %ProtocolError{condition: condition, description: description, info: info}}
  end
end

defp validate_protocol_error_field_count(fields, _performative, _operation, _class)
     when length(fields) <= @protocol_error_field_count,
     do: :ok

defp validate_protocol_error_field_count(fields, performative, operation, class) do
  nested_schema_error(performative, operation, class, :too_many_fields,
    actual: length(fields),
    maximum: @protocol_error_field_count
  )
end

defp required_protocol_error_field(
       fields,
       index,
       name,
       expected_type,
       performative,
       operation,
       class
     ) do
  case Enum.at(fields, index) do
    value when absent?(value) ->
      nested_schema_error(performative, operation, class, :mandatory_field_missing,
        field: name,
        index: index
      )

    %AMQPValue{type: ^expected_type, value: value} = tagged when is_binary(value) ->
      {:ok, tagged}

    value ->
      protocol_error_type_mismatch(
        performative,
        operation,
        class,
        name,
        index,
        expected_type,
        value
      )
  end
end

defp optional_protocol_error_field(
       fields,
       index,
       name,
       expected_type,
       performative,
       operation,
       class
     ) do
  case Enum.at(fields, index) do
    value when absent?(value) ->
      {:ok, nil}

    %AMQPValue{type: ^expected_type, value: value} = tagged when is_binary(value) ->
      {:ok, tagged}

    value ->
      protocol_error_type_mismatch(
        performative,
        operation,
        class,
        name,
        index,
        expected_type,
        value
      )
  end
end

defp protocol_error_info_field(fields, performative, operation, class) do
  case Enum.at(fields, 2) do
    value when absent?(value) ->
      {:ok, nil}

    %AMQPValue{type: :map, value: entries} = value when is_list(entries) ->
      if Enum.all?(entries, &protocol_error_info_entry?/1) do
        {:ok, value}
      else
        nested_schema_error(performative, operation, class, :invalid_property_key,
          field: :info,
          index: 2
        )
      end

    value ->
      protocol_error_type_mismatch(
        performative,
        operation,
        class,
        :info,
        2,
        :map,
        value
      )
  end
end

defp protocol_error_info_entry?(
       {%AMQPValue{type: :symbol, value: key}, %AMQPValue{}}
     )
     when is_binary(key),
     do: true

defp protocol_error_info_entry?(_entry), do: false

defp protocol_error_type_mismatch(
       performative,
       operation,
       class,
       field,
       index,
       expected,
       actual
     ) do
  nested_schema_error(performative, operation, class, :field_type_mismatch,
    field: field,
    index: index,
    expected: expected,
    actual: semantic_type(actual)
  )
end

defp nested_schema_error(performative, operation, class, reason, details \\ []) do
  details =
    details
    |> Keyword.put(:nested_schema, :error)
    |> Keyword.put(:performative, performative)

  schema_error(operation, class, reason, details)
end
```

- [ ] **Step 6: Run all nested and outer decode tests**

Run:

```bash
mix test apps/graviton_mq_amqp10/test/codec/end_close_error_test.exs apps/graviton_mq_amqp10/test/codec/end_close_performative_test.exs
```

Expected: all tests pass, including extension conditions, exact ordered info,
top-level Error rejection, and exact error detail maps.

### Task 5: Canonically encode End, Close, and nested Error

**Files:**

- Modify: `apps/graviton_mq_amqp10/test/codec/end_close_performative_test.exs`
- Modify: `apps/graviton_mq_amqp10/test/codec/end_close_error_test.exs`
- Modify: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/performative.ex`

- [ ] **Step 1: Add failing exact-byte encoding tests**

Add to `end_close_performative_test.exs`:

```elixir
test "canonically encodes empty End and Close with numeric descriptors" do
  assert {:ok, @empty_end} = Performative.encode(%End{})
  assert {:ok, @empty_close} = Performative.encode(%Close{})
end

test "symbolic inputs re-encode with numeric descriptors" do
  assert {:ok, end_performative, <<>>} = Performative.decode(@symbolic_end)
  assert {:ok, close_performative, <<>>} = Performative.decode(@symbolic_close)
  assert {:ok, @empty_end} = Performative.encode(end_performative)
  assert {:ok, @empty_close} = Performative.encode(close_performative)
end
```

Add to `end_close_error_test.exs`:

```elixir
test "canonically encodes an extension condition with numeric descriptors" do
  value = %End{
    error: %ProtocolError{condition: AMQPValue.symbol("x")}
  }

  expected =
    <<
      0x00,
      0x53,
      0x17,
      0xC0,
      10,
      1,
      0x00,
      0x53,
      0x1D,
      0xC0,
      4,
      1,
      0xA3,
      1,
      "x"
    >>

  assert {:ok, ^expected} = Performative.encode(value)
  assert {:ok, ^value, <<>>} = Performative.decode(expected)
end

test "preserves the interior description null when info is present" do
  value = %Close{
    error: %ProtocolError{
      condition: AMQPValue.symbol("x"),
      info: AMQPValue.map([{AMQPValue.symbol("k"), AMQPValue.uint(1)}])
    }
  }

  expected =
    <<
      0x00,
      0x53,
      0x18,
      0xC0,
      19,
      1,
      0x00,
      0x53,
      0x1D,
      0xC0,
      13,
      3,
      0xA3,
      1,
      "x",
      0x40,
      0xC1,
      6,
      2,
      0xA3,
      1,
      "k",
      0x52,
      1
    >>

  assert {:ok, ^expected} = Performative.encode(value)
  assert {:ok, ^value, <<>>} = Performative.decode(expected)
end
```

- [ ] **Step 2: Add failing invalid outbound struct tests**

Add:

```elixir
test "rejects invalid outbound termination and Error structs at the schema boundary" do
  raw_error =
    AMQPValue.described(
      AMQPValue.ulong(0x1D),
      AMQPValue.list([AMQPValue.symbol("x")])
    )

  invalid_values = [
    {%End{error: raw_error}, :end, nil, :error},
    {%End{error: %ProtocolError{condition: nil}}, :end, :error, :condition},
    {%Close{
       error: %ProtocolError{
         condition: AMQPValue.symbol("x"),
         description: AMQPValue.uint(1)
       }
     }, :close, :error, :description},
    {%Close{
       error: %ProtocolError{
         condition: AMQPValue.symbol("x"),
         info: AMQPValue.map([{AMQPValue.string("k"), AMQPValue.uint(1)}])
       }
     }, :close, :error, :info}
  ]

  for {value, performative, nested_schema, field} <- invalid_values do
    assert {:error,
            %Error{
              operation: :performative_encode,
              class: :invalid_value,
              details: details
            }} = Performative.encode(value)

    assert details.performative == performative
    assert Map.get(details, :nested_schema) == nested_schema
    assert details.field == field
  end
end

test "propagates unsupported Error info values from the value encoder" do
  value = %End{
    error: %ProtocolError{
      condition: AMQPValue.symbol("x"),
      info: AMQPValue.map([{AMQPValue.symbol("flag"), AMQPValue.boolean(true)}])
    }
  }

  assert {:error,
          %Error{
            operation: :value_encode,
            class: :unsupported,
            reason: :semantic_type,
            details: %{type: :boolean}
          }} = Performative.encode(value)
end
```

- [ ] **Step 3: Run the encoding tests and verify End/Close are rejected**

Run:

```bash
mix test apps/graviton_mq_amqp10/test/codec/end_close_performative_test.exs apps/graviton_mq_amqp10/test/codec/end_close_error_test.exs
```

Expected: encode tests fail with `:invalid_performative` because encode clauses
for End and Close do not exist.

- [ ] **Step 4: Add End and Close encode clauses**

Insert before the fallback encode clause:

```elixir
def encode(%End{} = end_performative, limits) do
  with {:ok, error_value} <-
         termination_error_value(
           end_performative.error,
           :end,
           :performative_encode,
           :invalid_value
         ) do
    encode_fields([error_value], @end_descriptor, limits)
  end
end

def encode(%Close{} = close_performative, limits) do
  with {:ok, error_value} <-
         termination_error_value(
           close_performative.error,
           :close,
           :performative_encode,
           :invalid_value
         ) do
    encode_fields([error_value], @close_descriptor, limits)
  end
end
```

- [ ] **Step 5: Validate outbound Error structs and construct a nested value**

Add:

```elixir
defp termination_error_value(nil, _performative, _operation, _class), do: {:ok, nil}

defp termination_error_value(
       %ProtocolError{} = error,
       performative,
       operation,
       class
     ) do
  fields = [error.condition, error.description, error.info]

  with {:ok, validated} <-
         validate_protocol_error(fields, performative, operation, class) do
    {:ok, protocol_error_value(validated)}
  end
end

defp termination_error_value(value, performative, operation, class) do
  outer_schema_error(performative, operation, class, :field_type_mismatch,
    field: :error,
    index: 0,
    expected: :error,
    actual: semantic_type(value)
  )
end

defp protocol_error_value(%ProtocolError{} = error) do
  described_list_value(
    [error.condition, error.description, error.info],
    @protocol_error_descriptor
  )
end
```

- [ ] **Step 6: Extract tagged described-list construction from binary encoding**

Replace `encode_fields/3` with these functions:

```elixir
defp encode_fields(fields, descriptor, limits) do
  fields
  |> described_list_value(descriptor)
  |> Value.encode(limits)
end

defp described_list_value(fields, descriptor) do
  semantic_fields =
    fields
    |> trim_trailing_absent()
    |> Enum.map(fn
      nil -> AMQPValue.null()
      value -> value
    end)

  descriptor
  |> AMQPValue.ulong()
  |> AMQPValue.described(AMQPValue.list(semantic_fields))
end
```

Do not change `trim_trailing_absent/1`; Open and Begin must continue to use the
same canonical construction path.

- [ ] **Step 7: Run all performative tests**

Run:

```bash
mix test apps/graviton_mq_amqp10/test/codec/performative_contract_test.exs apps/graviton_mq_amqp10/test/codec/open_performative_test.exs apps/graviton_mq_amqp10/test/codec/begin_performative_test.exs apps/graviton_mq_amqp10/test/codec/end_close_performative_test.exs apps/graviton_mq_amqp10/test/codec/end_close_error_test.exs
```

Expected: all tests pass and exact canonical bytes match.

### Task 6: Prove incomplete, limit, and value-layer error behavior

**Files:**

- Modify: `apps/graviton_mq_amqp10/test/codec/end_close_error_test.exs`
- Modify: `apps/graviton_mq_amqp10/test/boundaries_test.exs`

- [ ] **Step 1: Add strict-prefix and exact-remainder tests**

Add:

```elixir
test "every strict prefix remains incomplete" do
  full_fields =
    <<0xA3, 1, "x", 0xA1, 1, "d", 0xC1, 6, 2, 0xA3, 1, "k", 0x52, 1>>

  fixtures = [
    <<0x00, 0x53, 0x17, 0x45>>,
    <<0x00, 0x53, 0x18, 0x45>>,
    end_with_error(numeric_error(<<0xA3, 1, "x">>, 1)),
    close_with_error(symbolic_error(<<0xA3, 1, "x">>, 1)),
    end_with_error(numeric_error(full_fields, 3))
  ]

  for fixture <- fixtures,
      length <- 0..(byte_size(fixture) - 1) do
    assert {:more, needed} =
             fixture
             |> binary_part(0, length)
             |> Performative.decode()

    assert needed > 0
  end
end

test "returns the exact suffix after a nested Error" do
  suffix = <<0xFE, 0xED, 0xFA, 0xCE>>
  fixture = end_with_error(numeric_error(<<0xA3, 1, "x">>, 1))

  assert {:ok, %End{}, ^suffix} = Performative.decode(fixture <> suffix)
end
```

- [ ] **Step 2: Add per-compound limit and value-error propagation tests**

Add aliases for `Codec.Limits`, then add:

```elixir
test "applies the existing item limit independently to nested compounds" do
  fields = <<0xA3, 1, "x", 0xA1, 1, "d", 0xC1, 1, 0>>
  fixture = fields |> numeric_error(3) |> end_with_error()
  limits = %{Limits.default() | max_compound_items: 2}

  assert {:error,
          %Error{
            operation: :value_decode,
            class: :limit_exceeded,
            reason: :compound_item_limit
          }} = Performative.decode(fixture, limits)
end

test "propagates malformed UTF-8 and unsupported nested constructors" do
  invalid_utf8 =
    <<0xA3, 1, "x", 0xA1, 1, 0xFF>>
    |> numeric_error(2)
    |> end_with_error()

  unsupported_boolean =
    <<0xA3, 1, "x", 0x40, 0xC1, 5, 2, 0xA3, 1, "k", 0x41>>
    |> numeric_error(3)
    |> end_with_error()

  assert {:error,
          %Error{
            operation: :value_decode,
            class: :malformed,
            reason: :invalid_utf8
          }} = Performative.decode(invalid_utf8)

  assert {:error,
          %Error{
            operation: :value_decode,
            class: :unsupported,
            reason: :format_code
          }} = Performative.decode(unsupported_boolean)
end

test "propagates duplicate ordered-map keys from the value decoder" do
  duplicate_info =
    <<
      0xA3,
      1,
      "x",
      0x40,
      0xC1,
      11,
      4,
      0xA3,
      1,
      "k",
      0x52,
      1,
      0xA3,
      1,
      "k",
      0x52,
      2
    >>
    |> numeric_error(3)
    |> end_with_error()

  assert {:error,
          %Error{
            operation: :value_decode,
            class: :malformed,
            reason: :duplicate_map_key
          }} = Performative.decode(duplicate_info)
end
```

- [ ] **Step 3: Strengthen the boundary test for the new modules**

Ensure the boundary test's no-process loop is exactly:

```elixir
for module <- [
      GravitonMQ.AMQP10.Codec.Performative,
      GravitonMQ.AMQP10.Performative.Open,
      GravitonMQ.AMQP10.Performative.Begin,
      GravitonMQ.AMQP10.Performative.End,
      GravitonMQ.AMQP10.Performative.Close
    ] do
  refute function_exported?(module, :start_link, 1)
  refute function_exported?(module, :child_spec, 1)
end
```

- [ ] **Step 4: Run the complete AMQP codec and boundary tests**

Run:

```bash
mix test apps/graviton_mq_amqp10/test/codec apps/graviton_mq_amqp10/test/boundaries_test.exs
```

Expected: all tests pass. The existing 88 codec tests remain green, with the
new End/Close/Error tests added to that count.

### Task 7: Publish the exact implemented boundary

**Files:**

- Modify: `README.md:15-26,103-148,325-335`
- Modify: `docs/AMQP10_SCOPE.md:5-9,64-69,161-242`
- Modify: `docs/ARCHITECTURE.md:75-114,302-316`
- Modify: `docs/RESEARCH_NOTES.md:108-118`
- Modify: `docs/ROADMAP.md:35-92`
- Verify: `docs/superpowers/specs/2026-08-11-end-close-error-codec-design.md`

- [ ] **Step 1: Update README current status and codec API wording**

State that the current post-Milestone-1 slice supports Open, Begin, End, and
Close; End and Close carry only an optional validated Error. Keep the existing
facade signatures and replace `open_or_begin` in the API example with:

```elixir
GravitonMQ.AMQP10.Codec.Performative.encode(open_or_begin_or_end_or_close, limits)
```

Add this exact boundary statement:

```markdown
Error is nested protocol data rather than a top-level performative. Its
mandatory condition is an exact tagged symbol; its optional description and
info retain tagged string and ordered symbol-keyed map values from the bounded
value subset. The codec accepts numeric and symbolic Error descriptors only
inside End or Close and canonically emits the numeric descriptor.
```

Replace the next-milestone paragraph so it recommends a separately designed
next performative slice and explicitly says Attach, Flow, Transfer,
Disposition, Detach, and message sections remain unimplemented.

- [ ] **Step 2: Update the AMQP scope document**

Preserve the historical `## Milestone 1 exclusions` section. Add a separate
section titled:

```markdown
## Post-Milestone-1 End/Close codec slice
```

Document descriptors `0x17`, `0x18`, and nested `0x1D`, their symbolic names,
the one-field outer schemas, the three-field Error schema, extension-condition
acceptance, ordered info maps, canonical numeric encoding, and the unchanged
bounded value subset. State explicitly that top-level Error is unsupported and
that no live Session or Connection transition is implemented.

- [ ] **Step 3: Update architecture, research notes, and roadmap**

In `docs/ARCHITECTURE.md`, advance the current codec paragraph to four
performatives and add:

```markdown
End and Close remain immutable protocol data. Decoding them does not end a
Session, close a Connection, dispatch a frame, or affect runtime ownership.
```

In `docs/RESEARCH_NOTES.md`, add a decision bullet recording that the first
post-Milestone-1 extension chose End/Close/Error because it needs no value-codec
widening and prepares later Connection/Session transition design without
implementing those transitions.

In `docs/ROADMAP.md`, leave the Milestone 1 section unchanged and add before
`## Later milestones`:

```markdown
## Post-Milestone-1: End, Close, and Error codec slice

- Encode and decode End and Close as dedicated immutable structs.
- Validate their optional nested Error composite without whitelisting condition
  symbols or widening the bounded value subset.
- Preserve the process-free codec, opaque-frame, structured-error, and exact
  remainder boundaries.
- Do not add protocol transitions, other performatives, message sections,
  transport, queues, or storage.
```

Keep the first later-milestone item for the remaining performative and
message-section slices.

- [ ] **Step 4: Verify documentation does not claim runtime behavior or full compatibility**

Run:

```bash
rg -n "End|Close|nested Error|top-level performative|post-Milestone-1" AGENTS.md README.md docs/AMQP10_SCOPE.md docs/ARCHITECTURE.md docs/RESEARCH_NOTES.md docs/ROADMAP.md
rg -n "full AMQP|complete AMQP|does not.*close|does not.*end|no.*transition" README.md docs/AMQP10_SCOPE.md docs/ARCHITECTURE.md docs/ROADMAP.md
git diff --check -- AGENTS.md README.md docs
```

Expected: the exact bounded surface and non-capabilities appear consistently;
`git diff --check` prints nothing.

### Task 8: Format, verify, and inspect the complete slice

**Files:**

- Verify all files listed above.

- [ ] **Step 1: Format all Elixir changes**

Run:

```bash
mix format
mix format --check-formatted
```

Expected: both commands exit zero; the second prints no formatting failures.

- [ ] **Step 2: Run warning-free compilation**

Run:

```bash
mix compile --warnings-as-errors
```

Expected: exit zero with no warnings.

- [ ] **Step 3: Run the full umbrella tests**

Run:

```bash
mix test
```

Expected: all tests pass with zero failures.

- [ ] **Step 4: Run architecture and xref gates**

Run:

```bash
mix graviton_mq.check_architecture
mix help xref
mix xref graph --format cycles --fail-above 0
```

Expected: architecture check passes across all five applications, `mix help
xref` exits zero, and xref reports no dependency cycles.

- [ ] **Step 5: Complete the repository-required sequence from the start**

Run:

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

Expected: every command exits zero. This exact sequence is the completion gate
from `AGENTS.md`.

- [ ] **Step 6: Inspect scope and whitespace**

Run:

```bash
git diff --stat
git diff --check
git status --short --branch
git diff -- AGENTS.md README.md apps/graviton_mq_amqp10 docs
rg -n '[ \t]+$' \
  docs/superpowers/specs/2026-08-11-end-close-error-codec-design.md \
  docs/superpowers/plans/2026-08-11-end-close-error-codec.md \
  apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative/end.ex \
  apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative/close.ex \
  apps/graviton_mq_amqp10/test/codec/end_close_performative_test.exs \
  apps/graviton_mq_amqp10/test/codec/end_close_error_test.exs || true
```

Expected: only the approved AMQP model/codec/tests, boundary test, design, plan,
and named scope documents changed; no whitespace errors or unrelated edits are
present.

- [ ] **Step 7: Confirm exclusions mechanically**

Run:

```bash
rg -n "GenServer|Supervisor|Agent|Task|Process|Registry|:gen_tcp|:ssl|Ranch|Bandit" apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/performative
rg -n "defmodule GravitonMQ.AMQP10.Performative.(Attach|Flow|Transfer|Disposition|Detach)" apps/graviton_mq_amqp10/lib apps/graviton_mq_amqp10/test || true
```

Expected: the first command finds no process or transport dependency in the
changed pure namespaces; the second finds no later performative implementation.

- [ ] **Step 8: Stop without committing**

Report focused and full verification results, the exact changed files, and the
uncommitted Git status. Do not create a commit, push, PR, release, or runtime
deployment without a new explicit user request.
