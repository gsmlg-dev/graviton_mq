# Bounded AMQP 1.0 Codec Foundation Implementation Plan

> Status: Completed historical implementation record. The current
> authoritative scope is `AGENTS.md` and `README.md`; unchecked checklist
> markers are retained as authored and are not outstanding work.

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bounded, process-free AMQP 1.0 protocol-header recognizer,
frame-envelope decoder, and semantic value encoder/decoder for the exact value
subset needed to represent Open and Begin.

**Architecture:** Pure modules below `GravitonMQ.AMQP10.Codec` return the
existing Milestone 0 `GravitonMQ.AMQP10.Value` structs and leave frame bodies
opaque. A shared structured error and limits contract distinguishes incomplete
input from malformed, unsupported, limit-exceeded, and invalid-value results.

**Tech Stack:** Elixir 1.18.4, Erlang/OTP 28.5.0.1, ExUnit, Mix xref, parsed
Elixir AST, and independently assembled OASIS AMQP 1.0 wire fixtures.

---

Commit steps were intentionally omitted when this plan was authored because no
commit had then been authorized. The later explicit first-release request
superseded that historical constraint.

## File map

Create these focused modules:

- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec.ex`: result types and
  bounded-scope documentation.
- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/error.ex`: structured
  expected-error data.
- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/limits.ex`: immutable
  default limits and validation.
- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/protocol_header.ex`:
  raw AMQP 1.0 header recognition.
- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/frame.ex`: raw AMQP
  frame-envelope validation.
- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/value.ex`: bounded
  semantic value encoding and decoding. Keep its recursive helpers private
  until a concrete maintenance need justifies more files.

Modify only directly related existing source and tests:

- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10.ex`
- `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/frame.ex`
- `apps/graviton_mq_amqp10/test/boundaries_test.exs`
- `apps/graviton_mq/lib/graviton_mq/architecture.ex`
- `apps/graviton_mq/lib/graviton_mq/architecture/source_analyzer.ex`
- `apps/graviton_mq/test/graviton_mq/architecture_test.exs`

Create focused tests below `apps/graviton_mq_amqp10/test/codec/`. Update only
`README.md`, `docs/ARCHITECTURE.md`, `docs/AMQP10_SCOPE.md`,
`docs/RESEARCH_NOTES.md`, and `docs/ROADMAP.md`. Leave historical Milestone 0
ADRs and unrelated architecture text unchanged.

### Task 1: Shared result, error, and limit contracts

**Files:**

- Create: `apps/graviton_mq_amqp10/test/codec/contracts_test.exs`
- Create: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec.ex`
- Create: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/error.ex`
- Create: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/limits.ex`

- [ ] **Step 1: Write the failing contract tests**

```elixir
defmodule GravitonMQ.AMQP10.Codec.ContractsTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Codec.Error
  alias GravitonMQ.AMQP10.Codec.Limits

  test "defaults bound frames, values, item counts, and nesting" do
    assert %Limits{
             max_frame_size: 16_777_216,
             max_value_bytes: 16_777_216,
             max_compound_items: 65_536,
             max_nesting_depth: 32
           } = Limits.default()
  end

  test "invalid limits return data instead of raising" do
    limits = %Limits{Limits.default() | max_nesting_depth: -1}

    assert {:error,
            %Error{
              operation: :value_decode,
              class: :invalid_value,
              reason: :invalid_limits
            }} = Limits.validate(limits, :value_decode)
  end
end
```

- [ ] **Step 2: Run the test and verify the modules are missing**

Run:

```bash
mix test apps/graviton_mq_amqp10/test/codec/contracts_test.exs
```

Expected: compilation fails because `Codec.Error` or `Codec.Limits` does not
exist.

- [ ] **Step 3: Implement the contracts**

`Codec.Error` is a plain struct, not an exception:

```elixir
@enforce_keys [:operation, :class, :reason]
defstruct [:operation, :class, :reason, :offset, details: %{}]

@type operation ::
        :protocol_header | :frame_decode | :value_decode | :value_encode
@type class :: :malformed | :unsupported | :limit_exceeded | :invalid_value
@type t :: %__MODULE__{
        operation: operation(),
        class: class(),
        reason: atom(),
        offset: non_neg_integer() | nil,
        details: map()
      }

@spec new(operation(), class(), atom(), keyword()) :: t()
def new(operation, class, reason, options \\ []) do
  %__MODULE__{
    operation: operation,
    class: class,
    reason: reason,
    offset: Keyword.get(options, :offset),
    details: Keyword.get(options, :details, %{})
  }
end
```

`Codec.Limits` defines the four approved defaults. `validate/2` returns
`{:ok, limits}` only when every field is a positive integer; otherwise it
returns `Error.new(operation, :invalid_value, :invalid_limits,
details: Map.from_struct(limits))`.

`Codec` documents the Open + Begin boundary and defines:

```elixir
@type decode_result(value) ::
        {:ok, value, binary()}
        | {:more, pos_integer()}
        | {:error, GravitonMQ.AMQP10.Codec.Error.t()}
@type encode_result ::
        {:ok, binary()} | {:error, GravitonMQ.AMQP10.Codec.Error.t()}
```

- [ ] **Step 4: Run the Task 1 test and verify 2 tests pass**

### Task 2: Raw AMQP 1.0 protocol-header recognition

**Files:**

- Create: `apps/graviton_mq_amqp10/test/codec/protocol_header_test.exs`
- Create: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/protocol_header.ex`

- [ ] **Step 1: Write failing specification-derived tests**

```elixir
alias GravitonMQ.AMQP10.Codec.Error
alias GravitonMQ.AMQP10.Codec.ProtocolHeader

header = <<"AMQP", 0, 1, 0, 0>>

assert {:ok,
        %ProtocolHeader{protocol_id: 0, major: 1, minor: 0, revision: 0},
        <<1, 2>>} = ProtocolHeader.recognize(header <> <<1, 2>>)

for length <- 0..7 do
  prefix = binary_part(header, 0, length)
  assert {:more, 8 - length} = ProtocolHeader.recognize(prefix)
end

assert {:error, %Error{class: :malformed, reason: :invalid_protocol_magic}} =
         ProtocolHeader.recognize(<<"AMQX", 0, 1, 0, 0>>)

assert {:error, %Error{class: :unsupported, reason: :sasl_not_supported}} =
         ProtocolHeader.recognize(<<"AMQP", 3, 1, 0, 0>>)

assert {:error, %Error{class: :unsupported, reason: :unsupported_protocol_id}} =
         ProtocolHeader.recognize(<<"AMQP", 7, 1, 0, 0>>)

assert {:error, %Error{class: :unsupported, reason: :unsupported_version}} =
         ProtocolHeader.recognize(<<"AMQP", 0, 1, 1, 0>>)
```

- [ ] **Step 2: Run the focused test and verify the module is missing**

Run:

```bash
mix test apps/graviton_mq_amqp10/test/codec/protocol_header_test.exs
```

- [ ] **Step 3: Implement recognition without negotiation**

Define an enforced struct with `protocol_id`, `major`, `minor`, and `revision`.
`recognize/1` first requests the missing bytes when input is shorter than eight,
then classifies bad magic, then accepts only protocol ID 0 and version 1.0.0.
Protocol ID 3 is `:sasl_not_supported`; other IDs and versions are unsupported
with numeric-only diagnostic details. Return every byte after the header as the
untouched remainder.

```elixir
@spec recognize(binary()) :: GravitonMQ.AMQP10.Codec.decode_result(t())
```

- [ ] **Step 4: Run the Task 2 test and verify it passes**

### Task 3: Frame-envelope validation with opaque body preservation

**Files:**

- Create: `apps/graviton_mq_amqp10/test/codec/frame_test.exs`
- Create: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/frame.ex`
- Modify: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/frame.ex`

- [ ] **Step 1: Write failing frame-envelope tests**

Use the exact heartbeat `<<0, 0, 0, 8, 2, 0, 0, 0>>`. Every strict prefix
must return the exact missing byte count; the complete frame must return:

```elixir
%GravitonMQ.AMQP10.Frame{
  declared_size: 8,
  data_offset_words: 2,
  type: :amqp,
  channel: 0,
  extended_header: <<>>,
  body: <<>>
}
```

Use a second frame with size 16, DOFF 3, channel 7, extended header
`<<1, 2, 3, 4>>`, body `<<9, 8, 7, 6>>`, and trailing bytes `<<5, 4>>`.
Assert all binary regions are exact.

Assert these errors:

- declared size 4: malformed `:frame_too_small`;
- DOFF 1: malformed `:data_offset_too_small`;
- size 8 with DOFF 3: malformed `:data_offset_beyond_frame`;
- size above a small caller limit: limit-exceeded `:frame_size_limit` before
  the decoder asks for the declared body;
- frame type 1: unsupported `:sasl_frame_not_supported`;
- frame type 9: unsupported `:unsupported_frame_type`.

- [ ] **Step 2: Run the focused test and verify the decoder is missing**

```bash
mix test apps/graviton_mq_amqp10/test/codec/frame_test.exs
```

- [ ] **Step 3: Expand the declarative frame envelope**

Make `GravitonMQ.AMQP10.Frame` enforce exactly these fields and type `body` as
a binary rather than a parsed performative:

```elixir
@enforce_keys [
  :declared_size,
  :data_offset_words,
  :type,
  :channel,
  :extended_header,
  :body
]
defstruct @enforce_keys
```

- [ ] **Step 4: Implement one-frame decoding in this order**

```text
size is at least 8
DOFF is at least 2
DOFF times 4 does not exceed size
size does not exceed the caller limit
frame type is 0
the input contains size bytes
```

Parse the fixed header as `size::32`, `doff::8`, `type::8`, and `channel::16`.
Split exactly `doff * 4 - 8` extended-header bytes and
`size - doff * 4` body bytes from the declared frame, then return all bytes
after `size` untouched. Do not inspect the body. Map type 0 to `:amqp`; reject
type 1 and unknown types with the tested unsupported errors.

- [ ] **Step 5: Run the focused test and existing AMQP tests**

```bash
mix test apps/graviton_mq_amqp10/test/codec/frame_test.exs
mix test apps/graviton_mq_amqp10/test
```

Expected: the focused test passes. The obsolete boundary assertion that the
codec is absent is the only expected AMQP-test failure and is replaced in
Task 8.

### Task 4: Primitive semantic value encoding and decoding

**Files:**

- Create: `apps/graviton_mq_amqp10/test/codec/value_primitive_test.exs`
- Create: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/value.ex`

- [ ] **Step 1: Write failing hand-built decode tests**

Use table-driven assertions for these exact constructor families and verify an
extra `<<0xAA>>` suffix remains untouched:

```elixir
[
  {<<0x40>>, Value.null()},
  {<<0x60, 1::16>>, Value.ushort(1)},
  {<<0x43>>, Value.uint(0)},
  {<<0x52, 1>>, Value.uint(1)},
  {<<0x70, 256::32>>, Value.uint(256)},
  {<<0x44>>, Value.ulong(0)},
  {<<0x53, 1>>, Value.ulong(1)},
  {<<0x80, 256::64>>, Value.ulong(256)},
  {<<0xA1, 1, "x">>, Value.string("x")},
  {<<0xB1, 1::32, "x">>, Value.string("x")},
  {<<0xA3, 1, "x">>, Value.symbol("x")},
  {<<0xB3, 1::32, "x">>, Value.symbol("x")}
]
```

Every strict prefix of each fixture must return `{:more, n}`. Add malformed
tests for invalid UTF-8 and non-ASCII symbols; unsupported tests for boolean,
ubyte, signed integers, binary, floating/decimal, char, timestamp, UUID, and an
unknown code; and a small `max_value_bytes` test that fails before waiting for
a declared string payload.

- [ ] **Step 2: Write failing canonical encoder tests**

Assert shortest uint/ulong constructors, `str8`/`sym8` through 255 bytes, and
`str32`/`sym32` at 256 bytes. Invalid UTF-8 and non-ASCII semantic values must
return class `:invalid_value`; unsupported Milestone 0 value types must return
class `:unsupported` and reason `:semantic_type`.

- [ ] **Step 3: Run the focused test and verify `Codec.Value` is missing**

```bash
mix test apps/graviton_mq_amqp10/test/codec/value_primitive_test.exs
```

- [ ] **Step 4: Implement the public API and primitive decoder**

Expose exactly:

```elixir
@spec decode(binary(), Limits.t()) :: Codec.decode_result(AMQPValue.t())
def decode(binary, limits \\ Limits.default())

@spec encode(AMQPValue.t(), Limits.t()) :: Codec.encode_result()
def encode(value, limits \\ Limits.default())
```

Validate limits first. Dispatch on format codes `40`, `60`, `43/52/70`,
`44/53/80`, `A1/B1`, and `A3/B3`. Fixed-width payloads return the exact
missing count. Variable-width payloads enforce the declared length before
requesting more bytes. Validate strings with `String.valid?/1`; validate
symbols using a tail-recursive binary matcher that rejects octets above
`0x7F` without converting the whole symbol to a list. All other codes return
unsupported `:format_code` with the numeric code and offset.

- [ ] **Step 5: Implement canonical primitive encoding**

```text
null                    -> 40
ushort                  -> 60 plus 2 bytes
uint 0                  -> 43
uint 1 through 255      -> 52 plus 1 byte
other uint              -> 70 plus 4 bytes
ulong 0                 -> 44
ulong 1 through 255     -> 53 plus 1 byte
other ulong             -> 80 plus 8 bytes
string length <= 255    -> A1 plus ubyte length and bytes
other string            -> B1 plus uint length and bytes
symbol length <= 255    -> A3 plus ubyte length and bytes
other symbol            -> B3 plus uint length and bytes
```

Validate manually constructed structs for payload type and numeric range so
bad semantic input returns structured data rather than raising.

- [ ] **Step 6: Run the Task 4 test and verify it passes**

### Task 5: Lists and ordered maps with isolated boundaries

**Files:**

- Create: `apps/graviton_mq_amqp10/test/codec/value_compound_test.exs`
- Modify: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/value.ex`

- [ ] **Step 1: Write failing list and map decode tests**

Use these independent fixtures:

```elixir
list0 = <<0x45>>
list8 = <<0xC0, 4, 1, 0xA1, 1, "x">>
list32 = <<0xD0, 7::32, 1::32, 0xA1, 1, "x">>
map8 = <<0xC1, 5, 2, 0xA3, 1, "k", 0x43>>
map32 = <<0xD1, 8::32, 2::32, 0xA3, 1, "k", 0x43>>
```

Assert exact tagged values and remainders. Add malformed fixtures for a size
smaller than its count field, odd map count, count underflow, count overflow,
bytes left after the declared count, a null map key, and an exact duplicate
typed key. Assert string key `"k"` and symbol key `"k"` remain distinct and
entry order is retained.

Construct a list whose declared sub-binary ends in an incomplete string and
append bytes after the list boundary. It must return malformed
`:compound_item_truncated`, proving the decoder cannot borrow top-level rest.

- [ ] **Step 2: Write failing canonical encoder tests**

Assert an empty list uses `list0`; small lists/maps use 8-bit compounds; and a
size or count above 255 uses the 32-bit form. Encoding must reject null map
keys, duplicate typed keys, non-Value entries, and configured item/depth/byte
limits with structured errors.

- [ ] **Step 3: Run the focused test and verify compounds are unsupported**

```bash
mix test apps/graviton_mq_amqp10/test/codec/value_compound_test.exs
```

- [ ] **Step 4: Implement isolated list and map decoding**

For list8/map8, size includes the one-byte count and item bytes. For
list32/map32, size includes the four-byte count and item bytes. First isolate
the complete declared sub-binary. Decode exactly `count` values only from that
sub-binary. Convert an internal `{:more, _}` into malformed
`:compound_item_truncated`; reject remaining sub-binary bytes as
`:compound_size_mismatch`.

Increment nesting depth before lists and maps, enforce count before recursion,
pair map items in encounter order, reject odd counts and null keys, and use
exact `%Value{}` equality to detect duplicates.

- [ ] **Step 5: Implement canonical list and map encoding**

Recursively encode members as iodata while enforcing limits and depth. Use
list0 for an empty list. Use list8/map8 only when both count and encoded size
fit in one byte; otherwise use list32/map32. For 8-bit compounds, size is
`1 + byte_size(items)`; for 32-bit compounds, size is
`4 + byte_size(items)`. Map count is twice the number of entries.

- [ ] **Step 6: Run primitive and compound value tests**

```bash
mix test apps/graviton_mq_amqp10/test/codec/value_primitive_test.exs \
  apps/graviton_mq_amqp10/test/codec/value_compound_test.exs
```

### Task 6: Symbol arrays, described values, and Open/Begin fixtures

**Files:**

- Create: `apps/graviton_mq_amqp10/test/codec/open_begin_value_test.exs`
- Modify: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10/codec/value.ex`

- [ ] **Step 1: Write failing symbol-array tests**

Use both outer widths:

```elixir
array8 = <<0xE0, 6, 2, 0xA3, 1, "a", 1, "b">>
array32 = <<0xF0, 9::32, 2::32, 0xA3, 1, "a", 1, "b">>
```

Both decode to `Value.array(:symbol, [Value.symbol("a"),
Value.symbol("b")])`. A zero-count array retains `:symbol`. Assert the shared
constructor appears once, non-symbol constructors are unsupported, malformed
element lengths cannot escape the array, and limits apply before recursion.

Encoder tests choose one shared sym8 constructor when every member is at most
255 bytes, one sym32 constructor when any member is longer, and array8 versus
array32 from the complete encoded size and count. General arrays return
unsupported `:array_element_type`.

- [ ] **Step 2: Write failing generic described-value tests**

Use the exact Open and Begin-shaped values:

```elixir
minimal_open = <<0x00, 0x53, 0x10, 0xC0, 4, 1, 0xA1, 1, "c">>

minimal_begin = <<
  0x00,
  0x53,
  0x11,
  0xC0,
  7,
  4,
  0x40,
  0x43,
  0x52,
  100,
  0x52,
  100
>>
```

They decode to generic described values with `Value.ulong(0x10)` and
`Value.ulong(0x11)` descriptors and list values. Encoding those semantic
values yields the exact fixtures. Every strict prefix returns `{:more, _}` and
an opaque suffix stays bit-for-bit unchanged.

Unknown ulong and symbol descriptors round-trip losslessly. A descriptor that
decodes as a supported string is unsupported `:descriptor_type`, not
malformed. Do not return `{:open, fields}` or `{:begin, fields}`.

- [ ] **Step 3: Run the focused test and verify array/described codes fail**

```bash
mix test apps/graviton_mq_amqp10/test/codec/open_begin_value_test.exs
```

- [ ] **Step 4: Implement bounded symbol arrays**

For array8, size includes the one-byte count, one shared constructor, and
untyped element payloads. For array32, size includes the four-byte count, the
constructor, and payloads. Accept only shared sym8 and sym32 constructors;
decode each element length and bytes without expecting another constructor.
Consume the declared boundary exactly and retain `element_type: :symbol` for
an empty array.

- [ ] **Step 5: Implement generic described values**

On code `0x00`, increment depth, decode one descriptor, require semantic type
`:ulong` or `:symbol`, and decode one supported value. Return
`Value.described(descriptor, value)` without interpreting the descriptor.
Encoding performs the same descriptor check and concatenates code `0x00`, the
canonical descriptor encoding, and the encoded value.

- [ ] **Step 6: Run all codec tests and verify they pass**

```bash
mix test apps/graviton_mq_amqp10/test/codec
```

### Task 7: Enforce process-free codec source

**Files:**

- Modify: `apps/graviton_mq/test/graviton_mq/architecture_test.exs`
- Modify: `apps/graviton_mq/lib/graviton_mq/architecture.ex`
- Modify: `apps/graviton_mq/lib/graviton_mq/architecture/source_analyzer.ex`

- [ ] **Step 1: Write failing architecture tests**

Create references from `GravitonMQ.AMQP10.Codec.Bad` to `GenServer`,
`Supervisor`, `DynamicSupervisor`, `PartitionSupervisor`, `Agent`, `Task`,
`Task.Supervisor`, `Process`, and `Registry`. Each must produce
`{:forbidden_codec_process_dependency, details}`. A runtime source module may
still reference these standard OTP modules.

Parse this source through `SourceAnalyzer` and assert its process sentinel
references are rejected:

```elixir
defmodule GravitonMQ.AMQP10.Codec.Bad do
  def start, do: spawn(fn -> :ok end)
  def deliver(pid), do: send(pid, :bytes)

  def wait do
    receive do
      :bytes -> :ok
    end
  end
end
```

- [ ] **Step 2: Run the architecture test and verify it fails**

```bash
mix test apps/graviton_mq/test/graviton_mq/architecture_test.exs
```

- [ ] **Step 3: Implement the namespace-scoped process rule**

Add `{:forbidden_codec_process_dependency, map()}` to the violation type and
formatter. Apply it only when `source_module` is within
`GravitonMQ.AMQP10.Codec` and the target is one of the tested process modules
or a child namespace. Preserve existing application, transport, and cycle
rules.

Extend parsed AST traversal so bare `spawn`, `spawn_link`, `spawn_monitor`,
`send`, and `receive` forms create a reference to the `Process` sentinel with
the original file and line. Keep source analysis syntax-based; do not add grep.

- [ ] **Step 4: Run architecture unit and command checks**

```bash
mix test apps/graviton_mq/test/graviton_mq/architecture_test.exs
mix graviton_mq.check_architecture
```

Expected: tests pass and the real source has no forbidden dependency.

### Task 8: Publish the exact Milestone 1 boundary

**Files:**

- Modify: `apps/graviton_mq_amqp10/test/boundaries_test.exs`
- Modify: `apps/graviton_mq_amqp10/lib/graviton_mq/amqp10.ex`
- Modify: `README.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `docs/AMQP10_SCOPE.md`
- Modify: `docs/RESEARCH_NOTES.md`
- Modify: `docs/ROADMAP.md`

- [ ] **Step 1: Replace the obsolete codec-absence assertion**

Add the codec namespace, Error, Limits, ProtocolHeader, Codec.Frame, and
Codec.Value modules to the load list. Assert:

```elixir
assert function_exported?(Codec.ProtocolHeader, :recognize, 1)
assert function_exported?(Codec.Frame, :decode, 1)
assert function_exported?(Codec.Value, :decode, 1)
assert function_exported?(Codec.Value, :encode, 1)
refute function_exported?(GravitonMQ.AMQP10.Performative, :parse, 1)
```

Retain every Milestone 0 value, state, outcome, message, lifecycle, and storage
test unchanged.

- [ ] **Step 2: Run all AMQP application tests**

```bash
mix test apps/graviton_mq_amqp10/test
```

- [ ] **Step 3: Update moduledocs and repository documentation surgically**

State consistently that:

- Milestone 1 recognizes only raw AMQP 1.0 headers, validates type-zero AMQP
  frame envelopes, and encodes/decodes only null, ushort, uint, ulong, string,
  symbol, list, map, symbol arrays, and described values;
- Open and Begin bound the subset, but remain generic described values because
  there is no performative-schema parser or protocol state machine;
- Open and Begin properties maps accept only values from this bounded subset;
  other legal AMQP property values are reported as unsupported;
- compact constructors are private encoder choices and preserve Milestone 0
  semantic identity;
- incomplete input uses `{:more, n}`, while malformed, unsupported, limit, and
  invalid semantic data use `Codec.Error`;
- frame extended headers, bodies, remainders, and authoritative opaque message
  bytes remain unchanged;
- SASL, Attach and later performatives, message sections, networking, OTP
  protocol processes, queues, and storage behavior remain excluded;
- architecture checks enforce process-free codec source;
- fixtures derive from OASIS AMQP 1.0 Types and Transport, not RabbitMQ source;
- this bounded foundation is not a full AMQP compatibility claim.

Leave historical Milestone 0 ADRs and exclusion sections intact. Change the
recommended next feature task to a separately reviewed pure Open/Begin
performative-schema codec built on this foundation, without Connection
behavior or transport.

- [ ] **Step 4: Format and check documentation whitespace**

```bash
mix format
mix format --check-formatted
git diff --check
```

### Task 9: Complete verification and final scope audit

**Files:**

- Inspect the complete shared worktree. Do not create a commit.

- [ ] **Step 1: Run the required repository sequence**

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

Every command must exit zero. Compilation must report no warnings; tests must
have zero failures; architecture checks must report no forbidden dependencies;
and Mix xref must report no cycles.

- [ ] **Step 2: Inspect the complete diff and status**

```bash
git diff --check
git diff --stat
git diff
git status --short
```

Every changed line must trace to preserved uncommitted Milestone 0 hardening or
this approved Milestone 1 task. Do not reset, overwrite, stage, or commit the
existing worktree.

- [ ] **Step 3: Perform the completion audit**

Confirm from source and tests:

- raw header recognition performs no negotiation;
- frame validation checks SIZE, DOFF, type, and limits while retaining bytes;
- only the Open + Begin value subset is encoded and decoded;
- compact forms collapse to stable semantic identities;
- declared compound boundaries and resource limits are enforced;
- malformed and unsupported inputs stay distinct and hostile bytes do not
  crash the caller;
- no performative parser, SASL, Attach, message parser, transport, OTP protocol
  process, queue, or storage behavior was added;
- no RabbitMQ source was copied or translated;
- no AMQP 0-9-1 concept was introduced;
- no project license was selected; and
- no Git commit was created.
