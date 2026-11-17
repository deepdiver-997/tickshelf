# Delivery and durability contract

**Contract ID: `tickshelf-delivery-v1-20261117`**. This is the current boundary for
TTAP services, the optional Engine, and benchmark interpretation. It does not
change the TTAP wire version, startup defaults, or any final-Storage sync policy.
Where older prose implies a bounded loss window or unconditional recovery, this
contract supersedes that implication.

## 1. Storage is the final authority; earlier stages may be lost

Permanent loss is permitted **before the final Storage commit**. This includes
bytes in flight, partially received/parsed frames, accepted records, process-local
buckets/staging, retry buffers, queued/in-flight slices, and Redis pending data.
There is **no fixed RPO**, no promised maximum number of lost records, and no
promised bound such as one flush interval or one second of AOF. A prolonged
backlog can span many flush intervals. Backpressure limits admission; it does not
make admitted data durable.

Redis is a hot buffer/cache, **not the recovery authority**. Running a dedicated
Redis with `appendonly=no` and `save=""` (no AOF/no RDB) is allowed. AOF/RDB may be
present on an external Redis, but neither their requested configuration nor an
observed setting changes this end-to-end contract. Do not reconfigure an external
Redis to match a benchmark. Existing isolated Redis startup parameters remain
unchanged.

Once Storage reports success, the guarantee is only as strong as the **actual
backend, transaction boundary, and sync settings** used for that commit. This is
not a blanket guarantee against every subsequent storage/device failure.

## 2. Completion vocabulary

| Stage | Meaning | What it does **not** prove |
| --- | --- | --- |
| `sent` | Producer attempted/completed its local socket send, per that producer's metric | Server admission, Redis publication, or Storage commit |
| `accepted` | An in-process ingestion/Engine API accepted ownership of the record | Publication, persistence, or a wire-level acknowledgement |
| `published` | The relevant Redis publication completed successfully | Atomicity of an entire RESP pipeline, recoverability after Redis loss, or final Storage commit |
| `storage_committed` | Final backend completed its required write/transaction and sync path | A stronger guarantee than that backend and its observed settings provide |

**`accepted` ≠ `published` ≠ `storage_committed`.** These events can overlap in a
healthy write-through path but must not be relabeled as one another. Publish-only
completion is not Storage completion. Legacy counters containing `durable` in
their name must be interpreted by mode/backend, not by the counter's spelling.
An independently verified Storage result, not a stage counter alone, establishes
benchmark completion.

TTAP currently has **no ingestion ACK**: successful `sendall`, connection closure,
a heartbeat, or a query response is not a per-record Storage acknowledgement.
There is no declared upstream durable log, resume offset, or guaranteed replay.
Producer reconnection does not imply retransmission of lost frames. Mock
ring-buffer backfill and file/capture replay tools are separate, explicitly
bounded experimental facilities, not a guarantee of upstream recovery.

## 3. Conditional pending recovery and idempotence

Classic persister recovery and Engine/write-through fallback work **only while a
readable, sufficiently complete cache copy and its discovery/state metadata still
exist in Redis**. A lease TTL allows a surviving pending batch to be reclaimed; it
does not recreate missing segment bytes, repair a partially published batch, or
recover a lost Redis instance. If process-local copies also disappeared, the
record may be permanently lost before Storage commit.

Retries of surviving batches can be described as **at-least-once attempts within
that recovery domain**. Storage unique keys/idempotent writes can make repeated
attempts converge to one logical row. “Effectively exactly-once” refers only to
that Storage effect under the applicable unique-key/content rules, not to
end-to-end delivery from producer to Storage. Leases reduce duplicate work; they
do not turn a cache into a durable log. A `done` marker follows a successful
Storage write in the intended protocol; the marker itself is not durability
proof or a replacement for inspecting Storage.

## 4. Final-Storage baseline is not weakened

- **Service SQLite**: retain the existing service settings; do not silently
  alter them. `SqliteTickStore` enables WAL without explicitly setting
  `synchronous`; this contract does **not** assert a FULL default. Interpret each
  commit using actual connection readback of the SQLite PRAGMAs and backend
  behavior, not an inferred default or claimed physical power-cut validation.
  A deployment explicitly configured for FULL must not be downgraded.
- **Full-record benchmark SQLite / RocksDB**: retain the declared FULL and
  WAL-enabled synchronous-write baselines respectively; record effective backend
  settings. A stage-only/null sink is not a durable backend.
- **Append file plus index experiments**: retain data sync before the required
  synchronous index/high-water commit and final acknowledgement. File and index
  are not a cross-resource atomic transaction; recovery/allowed-tail rules remain
  those of the selected backend. Sparse-index and full-record layouts are not
  interchangeable correctness/performance claims.

Permitting pre-commit loss is **not** permission to count an asynchronous,
unsynced, or merely published write as final durable completion.

### 4.1 Raw journal (ADR-027, single-writer StorageOnly path)

With `--raw-journal` (Engine + `persist=storage` only), the per-shard raw file
becomes the local commit point: a batch frame is appended and synced
(`F_FULLFSYNC` when available, otherwise `fsync`) **before** the inner Storage
commit. There is no cross-resource atomic transaction between the raw file and
the index store; the ordering contract is:

- raw sync succeeds, index commit lost → replay on next open re-upserts
  idempotently (storage unique key);
- raw sync fails → the index commit is never started for that batch;
- an index commit with **no** durable raw frame is a contract violation of this
  mode and must not be introduced.

The persisted watermark (`<shard>.raw.wm`, temp-file + fsync + rename + directory
fsync, pinned at open and every 32 commits) is a **replay cursor, not a
durability promise**: it may lag committed state (harmless — replay is
idempotent) but must never claim more than the raw file durably contains. A torn
tail from a crash mid-append is truncated to the last complete frame at open;
corrupt frames beyond a parseable boundary are discarded, not silently served.
Watermark loss only costs replay work. Raw replay is a single-machine recovery
mechanism for the storage-only path; write-through keeps the Redis lease as its
recovery authority and does not stack this journal.

## 5. Fault policy is not the healthy benchmark PASS rule

A healthy/no-injected-fault benchmark still requires independently reconciled
input and final Storage: no unexpected missing records, no unintended duplicates,
and correct content under the declared deduplication/uniqueness rules. Drain,
backlog, final completion timing, source qualification, and rate/capacity gates
remain applicable. A healthy run that silently drops accepted or pending data
**fails**; “loss is allowed on failure” cannot turn it into PASS.

A fault drill must declare its fault, surviving copies, expected recovery/loss
scope, and observed missing/duplicate/content results. It may demonstrate allowed
pre-commit loss without proving lossless delivery. A detection counter or missing
range is evidence of loss, not evidence that the missing bytes can be replayed.

## 6. Runtime Redis evidence, not inferred defaults

`scripts/stack_runtime.py` provides
`redis_runtime_snapshot(host, port, *, ownership, requested=None, timeout=1.0,
connection_factory=None)`. Host/port and `ownership=owned_private|external` are
explicit caller declarations; ownership is not discovered or proven by this
read-only observation. The helper connects only to that endpoint, without probing
other ports, selecting a default Redis, or altering startup behavior.

Its fixed allowlist is one `CONFIG GET` each for `appendonly`, `save`,
`appendfsync`, `maxmemory`, and `maxmemory-policy`, plus `INFO server`,
`INFO persistence`, and `INFO memory`. It never sends CONFIG SET/REWRITE,
SAVE/BGSAVE, FLUSH commands, or SHUTDOWN. It is separate from `--print-config`,
which remains network-free.

Snapshot schema version 1 separates `requested` intent from `observed.config` and
`observed.info`; each observed item has `status=observed|unknown|denied|error` and a
value only when actually observed. `observed.redis_version` and
`observed.process_id` come from INFO server, not the launched binary/PID supplied
by a caller. UTC `observed_at`/`completed_at` bound sequential, **non-atomic**
reads. Top-level `status=observed|partial|denied|error` summarizes completeness,
not service health or durability. ACL/auth denial, missing fields, unsupported
commands and connection errors remain explicit; no Redis defaults or requested
values are substituted. `appendonly=no` makes
`observed.appendfsync_activity.value=inactive`, even when the observed configured
policy is `appendfsync=everysec`. “Active” with AOF enabled is not proof that a
sync succeeded.

## 7. Evidence version and source boundary

New evidence claiming this contract must identify
`tickshelf-delivery-v1-20261117` and separately record requested versus observed
runtime settings. The 2026-11-14/15 reports and machine evidence are historical:
they are **not rewritten or retroactively upgraded** with unobserved Redis
settings, this contract ID, or a claim that they were rerun under it.

## 8. Distributed topology (declared, not implemented)

A routing/dispatch layer that assigns work to multiple persisting instances may
be specified against this contract only under these boundaries:

- The **durability commitment is made by each instance's own Storage commit**
  (or raw-journal sync, §4.1). The router's watermark/completion tracking is an
  **aggregate recovery hint**, never the commit point and never a durability
  promise. Advancing a watermark must not ACK upstream records whose owning
  instance has not made its durable commitment.
- Raw-file ownership follows the topology: single-machine mode owns its raw
  file locally; a routed deployment must define which component records and
  replays each instance's journal (typically the router, keyed by instance) —
  undefined ownership is treated as **no raw journal**.
- Instance persistence success with a lost router acknowledgement produces
  redelivery, absorbed by storage idempotence; it does not require a new
  protocol. "Watermark above X is never lost" would be a two-generals promise
  and is out of scope.
- tickshelf ships **no implementation** of this topology; these boundaries
  exist so a production router cannot silently exceed the single-machine
  contract.

Default inputs remain **synthetic**, with `production_qualified=false` unless the
separate source/qualification requirements are actually satisfied. Public
field-shape references (including the publicly archived iQuant reference cited
in [pipeline-benchmark.md](pipeline-benchmark.md)) are not production captures,
source authorization, or deployment validation. Existing source labels and
provenance requirements remain in force. **DCRB: `not_adapted`**; this contract
does not claim a DCRB adapter or protocol compatibility. See
[full-record-benchmark.md](full-record-benchmark.md) for the distinct capture,
replay, and production-qualification gates.
