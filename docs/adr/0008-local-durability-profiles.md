# ADR 0008: Local durability and role-owned recovery

- Status: Accepted implementation direction under the owner-authorized G6
- Date: 2026-10-30
- Implementation/acceptance: bounded local Linux G6 exit qualified in
  [the G6 record](../qualification/2026-10-30-g6-local-durability.md); subsequent
  clause review and final profile evidence are recorded in
  [G7 qualification](../qualification/2026-10-30-g7-local-qualification.md)

## Responsibility and layering

Preserve separate append and durable-through boundaries. A durable acceptance
effect may run only after its selected state/log boundary is synchronized.
Write, sync or checkpoint failure is an explicit failure or unknown result;
the running owner must stop using uncertain storage rather than acknowledge
success. Memory profile results remain explicitly memory-only.

`graviton_mq_storage` owns physical WAL records, checksums, positions, bounded
streaming reads and crash recovery. Its core storage behaviour retains logical
Queue.Event operations. MQTT protocol state is not disguised as Queue.Event:
an opaque stream has separate `append_records`/`fold_records` operations with
binary record type/payload. Each stream selects events or opaque records;
the logical event API rejects an opaque stream. Role-owned runtime adapters
own snapshot schemas, validation and transitions after recovery. Storage must
not import protocol or broker runtime modules.

Persistent MQTT broker data includes sessions, subscriptions, retained content,
Will jobs and incoming/outgoing QoS ownership. Host PIDs/references, callbacks,
credentials, sockets, timers, active authentication and local host subscription
ownership are transient. Recovery re-establishes runtime resources, treats
lost active connections explicitly and rebases elapsed-time deadlines using
a stored wall-clock boundary. It does not silently restart protocol timers
with their full original duration.

The independent client continues to exclude broker storage/runtime/composition
dependencies. Its local persistence adapter belongs to the client package;
the host may select a storage callback. Persist stable operation identities,
session and QoS phases before their wire/host effects; public call references
and caller PIDs remain transient. Recovered operations report their stable
identity to the new observer. A client process/VM restart is distinct from
transport reconnect and needs its own Mosquitto qualification.

## Filesystem profile

The first qualified filesystem profile is local Linux, with existing parent
directories and one serialized owner per WAL directory. Create only the leaf
directory and sync its parent; file creation and checkpoint replacement require
sync of the leaf directory. A checkpoint publishes a checksummed stream ID,
position and end offset only after the WAL, checkpoint file and directory
steps succeed. Failed/unknown publication poisons the open writer.

Use an abstract Unix socket bound to the directory device/inode to hold a
kernel-owned exclusivity lock. Alias paths must resolve to the same lock;
owner/VM death releases it without stale lockfile deletion. Test a second
independent BEAM, process/VM death and alias paths. Unsupported platforms or
directory synchronization fail explicitly. This local profile adds no
distributed filesystem or multi-node durability claim.

Recovery rejects stream/version/checksum/position corruption and damage within
a confirmed durable prefix. Only a demonstrably incomplete tail beyond that
boundary may be truncated. Complete unacknowledged appends can survive and
remain possible outcomes; they are not proof of a prior successful ACK.
Record/log limits and streaming early halt bound memory and disk admission.

G6 exits require actual crash/restart, partial-write and failed/uncertain-sync
evidence, including independent MQTT peers and AMQP queue recovery. G7 adds
CI, model/fuzz/soak/resources and clean installed-consumer qualification.

## Amendment — 2026-11-08: WAL compaction and recovery repair

Status: committed as `696dca0` on `main` (2026-11-08); Linux tests passed
locally; independent-peer requalification pending. The original decisions above are unchanged except where stated.

`WAL.compact/2` (event streams) and `WAL.compact_records/2` (opaque streams)
replace the whole durable log with caller-supplied base items; `WAL.usage/1`
reports log, base and limit bytes. Compaction requires every appended record to
be durable and is serialized with append/sync. The complete base is measured
before any file is created and must fit `max_log_bytes`. It is written to a new
generation file `events.<n>.wal` whose version 2 header names the generation,
base position and base end offset; the file and its directory are synchronized.
The single commit point is the atomic rename of a version 2 checkpoint naming
that generation. A crash before the rename recovers the old generation, after
it the new one. Base items take fresh positions after the current position, so
positions and durable-through references never go backwards. Success is
reported only after the checkpoint directory sync; removing the old generation
afterwards is garbage collection, repeated on every successful open, and its
failure does not fail the compaction. A failed compaction before the commit
poisons the owner. A never-compacted log keeps `events.wal` and the version 1
header/checkpoint byte-for-byte; both versions remain readable.

Owners compact when the log is at least half of `max_log_bytes` and at least
twice its base size, on open, and once (then retry) after an append rejected
by the log limit, which writes nothing. `max_log_bytes` is thus no longer a
lifetime cap, but the compacted live state must still fit. MQTT compacts to its
latest committed snapshot. An AMQP queue compacts its durable machine state to
the identity event followed by `queue_snapshot/v1` chunk events (deterministic
ETF, decoded with `:safe`, compressed ETF refused), which replay verifies
exactly.

Recovery is amended: an invalid tail, not only an incomplete one, at or beyond
the durable checkpoint is truncated, because it was never acknowledged; read
I/O errors are never treated as a tail. A log without a checkpoint that holds
at most a (partial) header of this stream is reinitialized. Any other log
without a checkpoint, or generation files without a checkpoint, remain an
error, as does a checkpoint naming a missing generation.

Evidence: `wal_codec_test.exs` and `queue_compaction_test.exs` ran on macOS.
`wal_compaction_test.exs`, the compaction crash matrix in `wal_crash_test.exs`,
the `wal_fault_test.exs` additions, `durable_queue_test.exs` and
`mqtt/persistence_test.exs` require Linux; before commit they were exercised
only through a non-repository scratch harness with the platform check stubbed.
On 2026-11-08 they passed for `696dca0` within the complete required sequence
on a local NixOS Linux x86_64 host (Elixir 1.18.5/OTP 28).
