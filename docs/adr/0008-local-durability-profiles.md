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
