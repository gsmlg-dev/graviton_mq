# ADR 0006: Support standalone and embedded lifecycles

- Status: Accepted
- Date: 2026-07-14

## Context

GravitonMQ must work as a standalone OTP application and as a child of a host
application's supervision tree. Separate startup implementations would drift
in options, naming, child order, and failure behavior. Fixed global supervisor
names would prevent two instances from coexisting, while creating atoms from
arbitrary external instance strings would leak atoms permanently.

Only the public `graviton_mq` application may own the product Application
callback. The runtime child application remains a library and composition
boundary.

## Decision

Use one public lifecycle for both modes:

```elixir
GravitonMQ.start_link(options)
GravitonMQ.child_spec(options)
```

`GravitonMQ.Application.start/2` reads the keyword list in
`:graviton_mq, :default_instance` and calls `GravitonMQ.start_link/1`. It does
not build a separate standalone tree.

`GravitonMQ.Supervisor` is the per-instance top-level supervisor. Its
Milestone 0 child is `GravitonMQ.Runtime.Supervisor`, which remains empty. The
options are:

- `:name` — the exact OTP registration name for the top-level supervisor,
  defaulting to `GravitonMQ.Supervisor`; and
- `:runtime_supervisor_name` — the exact OTP registration name for the runtime
  supervisor, defaulting to `GravitonMQ.Runtime.Supervisor`.

The values are passed directly to OTP. The lifecycle does not derive atoms
from strings or construct new atoms dynamically. Two registered instances
must use distinct values for both options. A duplicate top-level name returns
the normal `{:error, {:already_started, pid}}` startup result. The public child
spec uses the top-level name in its child ID so differently named instances can
coexist under one host supervisor.

A host configures the dependency with `runtime: false`, then explicitly owns
the GravitonMQ child:

```elixir
# mix.exs
{:graviton_mq, "~> 0.1", runtime: false}

# host supervision tree
{GravitonMQ,
 name: :primary_graviton_mq,
 runtime_supervisor_name: :primary_graviton_mq_runtime}
```

The version requirement is illustrative until GravitonMQ is published; the
same `runtime: false` rule applies to the source selected by the host.

Starting an instance in Milestone 0 starts only the public and empty runtime
supervisors. It opens no TCP listener and starts no queue, storage engine,
connection worker, or broker operation.

## Consequences

Standalone and embedded operation exercise one tested startup path. Hosts
control restart policy and can run multiple explicitly named instances without
globally fixed module registrations. Duplicate naming fails predictably rather
than silently sharing a process.

Callers are responsible for choosing stable OTP names and for ensuring both
levels are unique. Adding future per-instance registries or workers will
require passing equally explicit names or non-atom naming mechanisms through
this lifecycle; it must not introduce arbitrary dynamic atom creation.
