# Isolated pipeline benchmark

> **默认输入是合成数据，不是生产捕获。** “真实JSON解析/Redis/同步写入”指执行路径真实，不证明输入来自生产。新增全记录RocksDB、原文回放和来源分级见 [full-record-benchmark.md](full-record-benchmark.md)；没有获授权的实际捕获，不授予生产合格标签。

This is an **experimental clean-room ingestion pipeline**, not a measurement of a
live brokerage feed and not an official iQuant TCP implementation. It exercises
JSON or canonical binary input → parsing → record hashing/deduplication → an
isolated Redis stage → SQLite, segment, or null sink. It shares neither service
ports nor Redis state with the production four-service stack.

## Source boundary and fixtures

The field reference is the publicly archived Guosen iQuant manual:

- <https://www3.guosen.com.cn/guosen/newxwfiles/20190902/1567416998056.pdf>
- SHA-256 `e02cbd0804a2d42e8d1f4288bc7ccbadc49e79e4553dbb604b6cc9b0eeb93ffe`.
- That particular document identifies itself as V1.0 / 2019-08-07; physical PDF
  pages 46–47 describe the callback/quoter fields and five-level book. This does
  **not** establish which version or JSON representation any current account uses.
- See the separately maintained [source notes](../tests/fixtures/iquant/sources.md)
  and [coverage](../tests/fixtures/iquant/coverage.md) when present.

The archive documents the 12 scalar quoter names and four book arrays; it does
not supply an official JSON export, a framed TCP protocol, or production feed
captures. `examples/bench/five-level.json` uses those field names but **synthetic
values** from the deterministic benchmark formula. Its root is our new wrapper:

```json
{"ticks":[{"symbol":"600000.SH","time":1700000000000,"quoter":{"lastPrice":10.0042}}]}
```

The abbreviated example above is not the complete five-level fixture.
`optional-null.json` additionally demonstrates the experimental `transactionNum`
scalar and a null `openInt`; `transactionNum` is not claimed as one of the 12
scalars in that archived manual. The manual's spelling `pvolumn` is retained;
`pvolume` is a mutually exclusive convenience alias in this experiment. Missing,
null, and zero remain distinct through canonical encoding. Prices and amount use
integer 1e-4 units internally; volume/count fields are integers. Five-level JSON
is the primary correctness path; binary is a controlled ablation, not a substitute
for establishing that path.

## Build and quick runs

Normal service builds deliberately do **not** include this experiment. Enable
it explicitly with local simdjson >= 5 and OpenSSL Crypto development packages;
CMake does not download them. The measured Release binary retains `-O3` plus
`-g -fno-omit-frame-pointer` on benchmark targets for same-binary profiling.
The Python runner never compiles code automatically and needs only the standard
library plus a `redis-server` executable on `PATH` for full pipeline cases.

```sh
cmake -S . -B build-bench-release -DCMAKE_BUILD_TYPE=Release -DTICKSHELF_BUILD_BENCHMARKS=ON
cmake --build build-bench-release
./build-bench-release/tickshelf_bench_tests
python3 scripts/test_pipeline_bench_anomaly.py -v
python3 scripts/test_pipeline_bench.py --runtime-only -v
python3 scripts/test_pipeline_bench.py --build ./build-bench-release -v

# NEW output path: the runner refuses any existing directory, even an empty one.
python3 scripts/pipeline_bench.py --build ./build-bench-release --suite smoke \
  --output /tmp/tickshelf-pipeline-smoke-20261113

python3 scripts/pipeline_bench.py --build ./build-bench-release --suite diagnostic \
  --config examples/bench/runner-smoke.json \
  --records 54000 --repeat 3 --profile off \
  --output /tmp/tickshelf-pipeline-diagnostic-20261113

# 30k *wire records*/s, 10-second offered interval, no duplicates by default.
python3 scripts/pipeline_bench.py --build ./build-bench-release --suite target \
  --rate 30000 --duration 10 --repeat 3 \
  --output /tmp/tickshelf-pipeline-target-20261113
```

Do not reuse output names; use a timestamp or UUID for another run. The executable
itself accepts `--role receiver|source|export|generate|local --config file.json`.
`--check-config` and `--print-config` validate without creating a directory,
opening a store, or connecting to Redis. Its flat JSON rejects unknown/duplicate
keys and wrong types. Relative executable config paths resolve against that
config's directory. The runner's `--config` accepts a JSON filename or literal
object of **overrides**, not a complete standalone executable config. It owns
`directory`, loopback hosts, ports, `input_file`, `output_file`, and `failpoint`;
these keys are refused rather than silently redirected to shared resources.

The controlled file-replay inputs are generated independently in Python before
measurement. All file-replay JSON ablation cells with the same generator
parameters replay the **same raw file**; the file path, SHA-256, size, and canonical
checksums are retained. Binary frames carry the same logical records. The explicit
`generated_json_source` diagnostic cell instead leaves `input_file` empty so the
C++ source genuinely constructs records, serializes JSON, and frames/CRCs them
during measurement. Its canonical records use the same independent oracle, but
wire-byte identity with the Python file is not assumed. Its source CPU/work
changes, so subtracting it from file replay cannot isolate receiver parse cost. File generation/oracle computation is not timed
as receiver capacity. Source replay still uses one TCP connection, normal frames,
END, and the final ACK; it is not a direct storage shortcut.

## User-defined matrices: no code edits

Use `--matrix examples/bench/transaction-matrix.json` to replace the built-in
cells with an axes product, while retaining the baseline `--config`, repeats,
shuffled order, same-input fixtures, budgets, independent oracle and profiling:

```json
{"axes":{"transaction_rows":[16,256,1000,4000],"redis":["roundtrip","off"]}}
```

Alternatively name explicit combinations:

```json
{"cases":[{"name":"full","set":{}},{"name":"big_txn","set":{"transaction_rows":4000}},{"name":"files","set":{"backend":"segment"}}]}
```

```sh
python3 scripts/pipeline_bench.py --build build-bench-release --suite diagnostic \
  --matrix examples/bench/transaction-matrix.json --records 10000 --repeat 3 \
  --output /tmp/my-new-transaction-matrix
```

Matrices are bounded to 64 cells and 256 case/repeat combinations, with strict
keys/types, safe unique names, no runner-owned paths/ports, and C++ side-effect-free
validation of every combination **before fixture generation**. `axes` and
`cases` are mutually exclusive. Save configuration and actual rows/transaction:
changing a maximum does not guarantee every timed flush reaches that maximum.
`--compare-profiles` with a custom matrix requires a `full` entry with empty
per-case overrides. The matrix path/hash and expanded schedule are retained.

## Suites and bounded defaults

| Suite | Default input records | Repeats | Cells |
|---|---:|---:|---|
| `smoke` | 2,160 | 1 | full, optional+duplicates, duplicates+hash-off, null, segment, binary |
| `diagnostic` | 54,000 | **3** | full and all core ablations below |
| `target` | `ceil(rate * duration)` | 1 | full; offered rate 30,000/s and duration 10 s |

Diagnostic cells change one axis from the supplied baseline:

| Cell | Change | Interpretation |
|---|---|---|
| `full` | none | JSON, real parse, SHA-256 dedup, private Redis roundtrip, SQLite FULL by default |
| `parse_replay` | `parse=replay` | synthetic parse bypass of the identical controlled raw input |
| `hash_off` | `hash=off` | same final unique canonical set; duplicate physical attempts can increase |
| `redis_encode` | `redis=encode` | serialization without Redis network/server |
| `redis_off` | `redis=off` | bypass the complete Redis stage |
| `null` | `backend=null` | consume/checksum only, not durable throughput |
| `smalltxn` | `transaction_rows=16` | smaller SQLite transaction row limit |
| `heavy` | `transaction_model=heavy` | heavier benchmark SQL transaction work |
| `segment` | `backend=segment` | append/index/fsync backend, different physical representation |
| `binary` | `input=binary` | canonical binary transport replacing JSON decoding |
| `generated_json_source` | empty `input_file`, real generated JSON | includes C++ source record construction/serialization instead of file replay |

`--include-normal` adds a separately named, explicitly weaker SQLite
`synchronous=NORMAL` cell. It is never the default and is not a FULL-durability
target pass. `--compare-profiles` adds `full_profiled` with early sampling while
keeping `full` off; compare the two groups to quantify observer overhead rather
than merging their rates. Changes in a supplied `--config` affect the baseline;
inspect each saved resolved config instead of assuming the table still applies.

A separate, unscored full warm-up of at most 540 records runs first. It uses a new
store and is excluded from aggregate statistics; it warms code/OS caches, not the
subsequent case's empty database. `--no-warmup` records an explicit no-warmup policy.
Case/repeat order is shuffled with a recorded stable `--order-seed` (20261113 by
default); data uses `--seed` 42 by default. Aggregates give repeat samples and
median/min/max, not unsupported confidence bounds from three trials.

The source-frame `batch_records`, `hash_batch_records`, `redis_batch_records`,
and store transaction limits are independent. Hash batching still computes and
deduplicates **individual canonical records**, not an aggregate batch identity.
`transaction_rows`, `transaction_bytes`, and `transaction_ms` bound store work;
partial transport frames can delay receiver ingestion. A batch timer is not a
per-record latency percentile. SHA `hash_mode=batch` reuses an EVP context across
records, whereas `record` allocates a context per record: **both still perform
per-record Init/Update/Final**, and `hash_api_calls` remains a record count.
`hash_compute_batches` and `hash_context_allocations` distinguish dispatch/context
costs; there is no claim that many record identities use one SHA computation.
FNV has no EVP-context reuse distinction. `reuse_statement=false` prepares per
transaction, not per row.

For replay, the receiver decodes the controlled input file **before readiness**,
then checks frame sequence and exact raw payload during measurement, copies the
predecoded records, and performs canonical encoding. This is labelled
`synthetic_parse_bypass`, not a zero-cost parser. Setup is excluded from receiver
time but materialization is not. Preload is bounded to 128 MiB (raw, canonical,
and estimated record-object memory); oversized replay cells fail explicitly.
This is an **estimated preload data budget**, not a hard allocator/RSS cap:
vector capacities and library/container overhead can make RSS exceed 128 MiB.
The optimized implementation holds each cached canonical payload once, shared by
FIFO and digest lookup through stable entries. `dedup_payload_bytes` counts that
single owned payload; `dedup_cache_bytes` additionally charges conservative fixed
entry/key/link overhead. The 256 MiB limit applies to this accounting, **not RSS or
an exact allocator/container measurement**. Reaching the byte or entry budget
evicts old FIFO cache entries: this is a **bounded deduplication window**, not lifetime/global
deduplication and not a hard-failure threshold. Longer-delayed duplicates may be
forwarded again after eviction; exact backend content semantics remain separate.
The target suite does not run replay by default. Use a smaller diagnostic record
count to stay under that limit; do not interpret a rejected replay as capacity.

## Independent oracle and accounting

`pipeline_bench_support.py` implements the specification in `bench/record.hpp`
without calling the C++ generator for expected results:

1. Physical sequence `s` maps to `s - (s+1)//duplicate_every` for intervals ≥2;
   zero disables duplication and one is invalid.
2. All 13 scalar values, presence/null masks, four depth arrays, symbol, and time
   are independently generated from logical ID and seed. The archived 12 fields
   and the experimental 13th scalar are not conflated.
3. Python constructs full `RCD1` canonical bytes: magic, little-endian symbol
   length/symbol, time, masks, present non-null signed scalars, then counted book
   arrays. Binary batches are repeated little-endian u32 lengths plus records.
4. Every exported `hex` BLOB must byte-equal the independently expected record;
   exported symbol/time must also agree with decoded bytes. Every expected ID
   must appear. This proves **full unique BLOB-set equality**, not just a row
   count, shared digest, or C++ export-versus-C++-generator comparison.
5. SQLite must contain one row per unique canonical record. Segment can retain
   physical duplicate appends when hashing is bypassed; physical rows and
   duplicates are reported separately and checked against forwarded attempts.

The experimental transport has a 24-byte little-endian `<4sBBHQII` header:
`IQB1`, version 1, type (1 JSON, 2 binary, 3 END, 4 ACK), reserved 0, first physical
record index u64, payload length u32, IEEE CRC-32 u32. END sequence is the total
physical record count. After END the source half-closes its TCP write side; the
receiver requires EOF before emitting ACK, rejecting both coalesced and delayed
trailing frames. A reset peer must not leave successful receiver metrics. This
protocol is not TTAP or a claimed official wire.

Counts are strict: `source_generated = source_sent = received = parsed = records`;
`forwarded + dedup_dropped = received`. This generator has monotone logical IDs
and adjacent duplicates, so FIFO evictions do not change its expected forwarding
or unique-content oracle: old unique IDs never reappear later in this workload.
The report/CSV retain `dedup_evictions`, `dedup_entries`, and
`dedup_payload_bytes` plus conservative `dedup_cache_bytes`; eviction adds CPU work and shortens the cache window but
is not itself a correctness or durability failure. Do not extrapolate this
adjacent-duplicate result to arbitrarily delayed production replay.
Error/failure/rejected-frame counters must be zero in valid-input
runs. `received` means successfully decoded/accepted records, not an untrusted
record count claimed inside bad JSON (the wire header has no record-count field).
`rejected` counts rejected **frames** in the fail-fast error path, not an inferred
number of records. Negative tests require explicit failure/no success ACK, not
record conservation for an undecodable payload. SQLite/segment have
`committed_attempts = forwarded` and `sink_consumed = 0`; null has
`committed_attempts = 0` and `sink_consumed = forwarded`. Committed attempts are
**not** unique rows, especially with hash-off and SQLite ignored duplicates.

ACK counts and the FNV-1a64 checksum of **all forwarded canonical bytes in order**
are independently verified for every backend. Null has no export and must return
`durable=false`. FULL storage returns `durable=true` after finish/sync; NORMAL
returns false while still reporting completed transaction attempts. A successful
ACK proves the program's sync boundary, not a real power failure, controller
cache behavior, filesystem guarantees under hardware failure, or production
exactly-once delivery. Physical power-loss durability is **not proved** here.

Redis roundtrip is this isolated experiment's staging/readback stage. It is not
the production stack's leases, crash recovery, eviction, and full four-service
semantics. Run existing stack/crash/lease regressions separately; do not replace
them with these micro-pipeline results.

## Local-file preprocessing ceiling (separate track)

The additional executable role `local` requires a framed `input_file` and
`rate=0`. It reads the same controlled JSON/binary IQB1 file directly into the
same decoder/hash/Redis/store pipeline, without launching a source or using TCP.
This separates a file+parse+hash ceiling experiment (for example a 40k/s
preprocessing question) from competition with the TCP source. It writes
`local.json` (`role=local`, `transport=local_file`, counters/stages/store and
canonical `checksum_fnv1a64`) and `progress.jsonl`. It is not a default network
suite cell and its throughput is not network receiver capacity.

To replay an existing full JSON fixture in hash-only/null mode, create a **new**
local store directory and config from the saved run (replace paths):

```sh
RUN=/absolute/path/to/network-run
LOCAL=$(mktemp -d "${TMPDIR:-/tmp}/tickshelf-local.XXXXXX")
python3 -I -c '
import json, pathlib, sys
run, local = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
summary = json.loads((run / "summary.json").read_text())
full = next(r for r in summary["results"] if r["case"] == "full" and not r["warmup"])
c = dict(full["config"])
c.update(directory=str(local / "store"), output_file="", rate=0,
         parse="real", hash="only", redis="off", backend="null", failpoint="")
(local / "config.json").write_text(json.dumps(c))
' "$RUN" "$LOCAL"
./build-bench-release/pipeline_bench --role local --config "$LOCAL/config.json"
```

Use an unprofiled, repeated, isolated run before interpreting the number.
`hash=only` computes record hashes but does not deduplicate; null consumes every
forwarded canonical record and gives no durable-storage evidence. The Python
integration tests independently verify JSON and binary local hash-only/null
counters and the complete ordered canonical checksum, including duplicate input.

## Actual SQLite defaults and storage comparison limits

The existing production `SqliteTickStore` requests WAL but does not set
`synchronous`. Do **not** infer its connection setting from SQLite's generic
default or another connection. The optional read-only `connection_info()` method
and fresh-file probe inspect that exact connection without changing its settings:

```sh
PROBE=$(mktemp -d)
./build-bench-release/pipeline_sqlite_probe "$PROBE/new.db"
```

For this macOS system library the probe reports runtime SQLite **3.43.2**,
WAL, **synchronous=1 (NORMAL)**, fullfsync=0, checkpoint_fullfsync=1. This is not a
claim about a Linux server's library. The benchmark explicitly sets and verifies
FULL=2. CMake/header versions and the dynamically loaded runtime version can
also differ. Prior default-mode performance is not equal-durability evidence for
FULL. Production defaults are deliberately unchanged by this experiment.

The experimental SQLite full-record schema uses a **canonical payload BLOB
primary key WITHOUT ROWID**, plus a symbol/time index. Exact equality avoids
hash-collision data loss, but the large primary key is copied into secondary
indexes: this is not compact digest/row-ID indexing and is not claimed to match
an existing production schema. A file-backend speedup in this comparison cannot prove that
files beat a reasonably compact SQLite schema. Compare real schema, dirty pages,
write bytes and checkpoint behavior before migrating production storage.

The segment backend does not omit query-index work: in addition to segment
positions/lengths/CRC/counts it stores one min/max time row per segment and symbol,
with a symbol/time lookup index, in the same FULL index transaction. It first
syncs record data, then commits the index; recovery recomputes and verifies the
ranges, rejecting missing/wrong/orphan index rows and any corrupted indexed data.
Only unindexed tails may be reclaimed. Its index schema is version 2; older v1
experiment directories are explicitly refused, not silently migrated.
`Store::export_range(symbol, from, to, stream, error)` uses inclusive bounds;
segment lookup filters candidate segments and performs exact-content duplicate
elimination after decode. Query result order is not guaranteed across backends.
Fewer index rows exchange write cost for candidate-segment read/filter work and
recovery cost. SQLite still stores full records in its main-table comparison;
segment stores those same canonical bytes in the file, not just one book level.

### Optimized layouts and optional RocksDB comparison

`backend=sqlite_compact` keeps complete canonical records in a separate
`compact-record.sqlite3` schema2 database. An INTEGER PRIMARY KEY locates payloads;
the digest index is fixed-width **nonunique** SHA-256, and symbol/time secondary
indexes carry rowids rather than full payload keys. Digest candidates must match
full payload bytes before a record is considered duplicate. Prepared
`INSERT ... SELECT ... WHERE NOT EXISTS`, under `BEGIN IMMEDIATE`, also handles
same-batch replay and separate SQLite writers without treating digest collisions
as duplicate records. `UNIQUE(digest)` and `UNIQUE(digest,payload)` are deliberately
not the schema. Trusted internal SHA metadata can be reused; hash-off/FNV paths
must compute the backend's SHA and report that work separately. No old database
is migrated or overwritten.

Redis roundtrip persists bytes obtained from the real GET response. Whole-payload
equality and the same strict canonical validator are required before internal
SHA/ingress metadata can be transferred. The optimized decoder extracts
symbol/time and retains canonical bytes directly instead of reconstructing books
and re-encoding; malformed, truncated, or trailing data is still rejected.

`backend=segment_rocksdb` uses the same TSG1 complete data records and sparse
per-segment/per-symbol ranges as `segment`. Its independently stamped
`segment-rocksdb-v1` directory cannot be mistaken for a SQLite index. Data-file
sync precedes an atomic, WAL-enabled `WriteBatch` with `sync=true` and
`use_fsync=true`, containing metadata, ranges and the committed high-water mark.
Recovery validates indexed history and index completeness before reclaiming an
unindexed tail. Both segment layouts may retain physical duplicate records;
range reads exact-deduplicate them. Their index transaction is not a cross-file
atomic transaction: the ordering and recovery protocol are part of the contract.
Heavy RocksDB work uses extra metadata/CRC/count KVs, **not identical SQL heavy
index work**; inspect `info_json` logical write counts before comparing it.

RocksDB finish includes blocking flush, completion of pending compaction,
SyncWAL, Close and read-only reopen, with before/after background-debt properties.
Export does not silently finish writes. This makes necessary cleanup visible,
but a finite experiment is not a long-duration compaction/space-amplification
study. On the pinned Mac build `HAVE_FULLFSYNC=1` makes RocksDB's file sync use
`fcntl(F_FULLFSYNC)`. SQLite WAL+FULL with `fullfsync=0` has a different underlying
sync strength. Select `sqlite_fullfsync=on|off|default` and inspect the effective
PRAGMA for an explicitly aligned comparison; `default` preserves the baseline.
The common segment data file still uses POSIX fsync on Mac / fdatasync on Linux.
None of these settings proves hardware power-loss protection.

This dependency is opt-in, local and fixed at RocksDB 11.13.2:

```sh
# Build the pinned upstream library with USE_RTTI=ON, including on macOS.
cmake -S . -B build-opt-release -DCMAKE_BUILD_TYPE=Release \
  -DTICKSHELF_BUILD_BENCHMARKS=ON -DTICKSHELF_BENCH_ROCKSDB=ON \
  -DRocksDB_DIR=/absolute/path/to/install-rtti/lib/cmake/rocksdb
cmake --build build-opt-release
python3 scripts/test_pipeline_bench.py --build build-opt-release
python3 scripts/test_pipeline_bench_capacity.py --build build-opt-release
```

See [fixed-source build and design evidence](rocksdb-reference-20261114.md) for
upstream commit, runtime checks, native sync reference, actual sync capability and
the reproduced AppleClang mixed-RTTI exception problem. Do not use the older
no-RTTI archive for application tests. Normal production builds still need none
of simdjson/OpenSSL/RocksDB; optional benchmark builds need RocksDB only when its
specific option is enabled.

## Offered rate, backlog, and target verdicts

Receiver `elapsed_s` starts at the first frame header and ends after store finish,
excluding process startup and listener wait. Source `send_elapsed_s` covers its
input work through END; source `elapsed_s` additionally includes final ACK.
Source generation, transport backpressure, and queueing can limit the offered
rate. The report distinguishes wire offer, actual source rate, offered unique
rate, durable unique rate, physical rows, committed attempts, and ACK tail.
High source CPU with a missed offer is reported as possible **source-bound**
work, never silently relabelled as receiver capacity.

`progress.jsonl` and `source-progress.jsonl` retain approximately one-second
snapshots with monotonic timestamps. Postprocessing records:

- exact sampled receiver receive-minus-drop-minus-commit backlog;
- forwarded-minus-completed queue gap;
- monotonic-aligned, linearly interpolated source-sent-minus-received and
  source-sent-minus-drop-minus-commit gaps, with sampling/skew caveats;
- regression slopes over the interior samples, excluding initial and final
  points rather than using only drained final counts.

A target pass requires correctness, FULL storage, at least five seconds of offer,
at least three interior receiver **and source-aligned** samples, ≥95% actual
source offer, no growing backlog faster than max(100 records/s, 1% of target),
and ACK tail ≤max(1 s, 10% of duration). A short burst, insufficient telemetry,
or missed offer is **inconclusive**, not a performance PASS. Target fail or
inconclusive returns nonzero even if the canonical oracle passed. These are
explicit finite-observation acceptance criteria, not proof of indefinitely
sustainable 30k/s or a universal saturation capacity. With duplicates, 30k wire
records/s is not 30k unique durable records/s; baseline duplication is disabled.

### Hard full-chain capacity floor (separate from generic target)

`--min-durable-rps 100000` additionally requires the complete baseline contract:
raw JSON over TCP, real parsing, the twelve baseline scalar fields and five-level
book, per-record SHA-256 deduplication, real Redis roundtrip, FULL durable storage,
no generated duplicates or optional-field reduction, and at least 54 symbols.
Actual cardinality is always reported; 54-symbol acceptance does not imply
5,400-symbol acceptance. Parser/hash/Redis/null/NORMAL ablations cannot acquire
the same full-chain capacity label.

The acceptance numerator is the independent expected **unique** count, with
correctness/oracle success separately mandatory. Its denominator is the larger
of receiver completion time and source-through-final-ACK time, so final drain,
necessary backend finish and ACK wait cannot be hidden by reporting sender speed
or receiver time alone. It requires this actual end-to-end rate to meet the
absolute floor, **not 95% of the floor**; source offer, backlog and tail checks
still apply. Requested and actual sending intervals must both be at least ten
seconds. The default zero-rate warmup is explicitly unscored and exempt from the
formal positive-offer/floor requirement. JSON/CSV and grouped results retain
both old receiver-only and new end-to-end rates, plus contract and floor status.

For a new, bounded 33-second/3.96-million-record run, with the opt-in backend
already built (this is a test invocation, not a promised result):

```sh
python3 scripts/pipeline_bench.py --build build-opt-release --suite target \
  --config examples/bench/capacity-full.json \
  --rate 120000 --min-durable-rps 100000 --duration 33 \
  --record-limit 4000000 --max-disk-mib 16384 --reserve-free-mib 10240 \
  --timeout 180 --output /tmp/my-new-full-capacity-run
```

The example uses 5,400 symbols. Substitute the intended server/cardinality and
inspect the saved resolved config. For a target-server deployment, a lower offered
rate with an explicit 30k completion floor is more relevant than extrapolating
from a Mac (for example rate 36k, floor 30k, duration 60, record limit 3 million).
Do not equate synthetic records with an unmeasured production Record or promise
a storage device's fsync latency from sequential MB/s specifications.

`examples/bench/layout-screen-matrix.json` compares all four layouts with default
and larger batches; `layout-million-matrix.json` compares their layouts with a
shared supplied baseline. Diagnostic results select candidates, while the hard
finite target run decides capacity under the stated workload. Each phase uses a
new artifact directory. The measurement report retains misses and explicitly
skipped long-soak/physical-power-loss tests.

Each queue's oldest actual pending ingress timestamp controls its own timeout;
empty queues cannot retain a stale deadline or force newer upstream data to
flush. The poll timeout follows the nearest remaining deadline. Counters split
row/byte/age/tail flushes, actual transaction rows/bytes and queue ages. These
are observed batching ages, not a hard real-time guarantee: synchronous disk or
Redis calls cannot be preempted by `transaction_ms`.

## Artifacts, profiling, and cleanup

Each case uses a fresh UUID directory and independently allocated receiver/Redis
ports. Candidate ports are hints; successful child bind is required. Receiver
readiness is its exact `BENCH_READY pid=...` log marker, **never a TCP probe**,
because probing would consume the source's single connection. The runner reuses
`stack_runtime.OwnedProcesses` and `start_private_redis`: only child PIDs it owns
are stopped, and Redis INFO must identify the actual owned process. No shared
Redis reuse, FLUSHALL, process-name kills, raw-disk tests, sudo, or system tuning.

Artifacts include `environment.json`, shuffled `schedule.json`, independent raw
`inputs/*.iqb` plus metadata, `summary.json`, and per-case config/check/result,
source/receiver metrics, raw progress, process logs, `observations.jsonl`, export,
and `profiling.json`. `results.csv` distinguishes wire counters, committed
attempts, unique canonical rows, and physical rows/duplicates; per-case
`stage_summary` gives wall-time totals, invocation means, and amortized cost per
input record without interpreting them as CPU shares. Failures and signals
preserve artifacts before cleanup;
forced child termination is recorded. `--work-dir` is an **existing parent disk
directory**; it gets one new UUID subtree retained after the run, with its
absolute path in the summary. It is not an arbitrary SQLite/raw-device filename.
Before launching a sink, the harness fsyncs its newly created output/work/case
directories and their parents, recording `startup_directory_sync` evidence.
Failure to sync is a run/case failure, not a durable ACK claim. This is startup
work outside receiver timing. Raw input fixture files are reproducibility
artifacts, not durable feed journals, and are not individually fsynced.

Environment evidence includes actual executable SHA-256, git HEAD/dirty status,
available CMake cache and target-scoped `*bench*.dir/flags.make` flags, bench
`compile_commands.json` entries when present, linked libraries, Python SQLite version (explicitly not
proof of the executable's linked SQLite version), CPU/platform, filesystem, and
available `/proc` information. The receiver's SQLite store metadata additionally
reports the actual executable runtime version and compile options when available;
use that evidence instead of inferring a library version from Python. Periodic `ps` records exact PID, CPU time and RSS;
Linux adds per-process io/status and diskstats without interpreting unrelated
system-wide disk traffic as benchmark-only I/O.

`--profile always` starts one bounded, two-second sample of the owned receiver
**during measurement**, at the first available loop iteration; it is early
sampling, not continuous tracing of the entire run. `--profile after` starts
at `--profile-trigger-s` (default 2 s). `--profile trigger` instead observes live
snapshots and captures the same still-running receiver after an actual gate:

- at least `--profile-trigger-s` seconds of observation;
- completion rate below `--profile-min-rps` for `--profile-windows` distinct
  snapshots (default 2), or aligned source-to-commit backlog above
  `--profile-backlog-records` for that many snapshots;
- or a newly observed commit/file-sync maximum above `--profile-commit-ms`
  (default 100 ms). A historical maximum already seen during warmup is not a new event.

Zero rate/backlog thresholds choose conservative automatic bounds: 90% of the
configured unique/attempt offered rate, and max(4 transactions of rows, one
second of offered records, 4000 records). Gate evidence, thresholds and the last
observation are saved in `profiling.json`. Re-reading one snapshot does not count
as multiple windows. Source snapshots newer than the receiver point are excluded
from backlog triggers. With no configured offered rate, only backlog/latency
conditions apply unless an explicit minimum rate is supplied.

macOS uses a two-second `sample`; Linux uses a two-second `perf record` if
installed. A post-spike trigger cannot reconstruct a stack from the completed
spike: the continuous low-overhead counters preserve its timing, not its past
stack. Rare transient stacks outside the bounded sample require a separate
continuous/ring-buffer profiler, not a claim that these two-second samples cover everything. Exact argv/PIDs/start/end times, unavailable tools, and permission
errors are retained. A short case can finish before sampling; this is recorded,
not represented as a valid profile. Inspect `receiver.sample.txt` or
`receiver.perf.data` and profiler logs before attributing CPU hotspots. Wall-time
stage totals/histograms alone do not justify hotspot claims, and overlapping
stage totals must not be added as a CPU budget. In particular,
`json_parse_normalize` combines simdjson padding/scanning, extraction, exact
numeric conversion, and Record materialization: iterator scanning is interleaved,
not an independently measured scanner CPU cost. `record_encode` is separate.
Redis `redis_build` builds payload/commands; `redis_publish_wait` includes the
RESP client's one encoding plus socket/reply work; read/decode/cleanup waits
are separate. Encode-only bypass uses `redis_encode_only`, nested inside
`redis_build`, so those totals must not be added together. `consume_checksum`
is real correctness/accounting work and remains part of the measured pipeline.

Defaults bound each measurement/export to 120 s, total artifacts to 2048 MiB,
keep at least 256 MiB free, and cap one case at 2,000,000 records and repeats at
20. An explicit `--record-limit` may raise the case allowance up to 20,000,000
for long soaks, without changing the default or the independent full-content
oracle. The Python export oracle streams records and uses one byte per expected
ID, not a giant BLOB set. Segment recovery/export and exact duplicate scans still
need extra memory and may be expensive at large sizes. Conservative preflight
estimates and periodic actual-size/free-space checks abort rather than fill a disk. These are harness limits, not atomic filesystem
quotas: allow headroom for in-flight writes. Diagnostic repeats retain stores and
exports, so a long study may need an explicitly larger budget or smaller inputs.

## Target-server disk example and path checks

A typical entry-level SATA SSD's public datasheet lists SATA 6 Gb/s and up-to
sequential write figures of a few hundred MB/s varying by capacity, with up-to
500 MB/s reads. Its
footnotes refer to out-of-box ATTO performance and dependence on hardware,
software, and usage. Those figures are **not** fsync latency or a promise about
FULL small-transaction throughput. The actual server SKU/capacity is unknown;
the datasheet does not establish a hardware power-loss-protection guarantee.
Do not assert that the target disk definitely has or lacks PLP based on this experiment.
Measure actual canonical sizes, filesystem writes, transaction behavior, and
sustained workload rather than deriving capacity from sequential bandwidth.

Substitute the **actual** checkout, built executable, and dedicated mounted data
parent; do not assume a mount path is the intended disk without checking. Run as an ordinary
user. Check build type/dirty state in the resulting environment artifact.

```sh
ROOT=/srv/tickshelf
BUILD="$ROOT/build-bench-release"
DISK=/mnt/bench-data

test -d "$ROOT" && test -x "$BUILD/pipeline_bench" && test -d "$DISK" || exit 1
command -v redis-server || exit 1
df -Pk "$DISK" "$ROOT"
# Confirm this is the intended mounted filesystem and sufficient free space.
OUT="$DISK/pipeline-$(date +%Y%m%d-%H%M%S)-$$"
test ! -e "$OUT" || exit 1

python3 "$ROOT/scripts/test_pipeline_bench.py" --build "$BUILD" -v
python3 "$ROOT/scripts/pipeline_bench.py" --build "$BUILD" \
  --suite target --rate 30000 --duration 10 --repeat 3 \
  --work-dir "$DISK" --output "$OUT" --timeout 180 \
  --max-disk-mib 4096 --reserve-free-mib 4096 --profile off
```

The 10-second target is 300,000 records/case. Its three retained stores/exports
need a larger explicitly approved disk budget than smoke. Verify available disk
space first, inspect backlog/offer verdicts afterward, and do a separate sampled
run (or `--compare-profiles`) instead of calling a sampled number an unperturbed
baseline. A longer unique-record run can reach the receiver's 256 MiB estimated
dedup payload/digest budget and cause FIFO evictions even when
`dedup_max_entries` is large; raising the entry limit does not lift that byte
budget. Long unique runs and this generator's adjacent duplicates can still
pass, but report eviction CPU/window effects rather than claiming unchanged
cache behavior. Do not silently turn off dedup or weaken the oracle to extend
a FULL baseline. For parser diagnostics use
smaller inputs within the replay preload bound. No benchmark result is bundled
as evidence of performance on the target hardware until that hardware has actually executed it
and its artifacts have been inspected.

For an explicitly budgeted **continuous 10-minute soak** on the actual server,
use one repeat (not separate short runs that reset the database), `--duration 600
--record-limit 20000000 --timeout 1800`, and a suitable disk budget, e.g.
`--max-disk-mib 65536 --reserve-free-mib 8192`. At 30k/s this is 18 million input
records. **This is a command template, not a run performed here**: first verify
that the actual mounted disk has room for the conservative estimate, raw input,
retained store/WAL, full export, and reserve. Do not blindly copy a 64 GiB budget
onto a nearly full disk. Setup/verification may be lengthy; full-content
verification is not skipped. The 2-million default limit stays unchanged unless
explicitly raised. Actual write volume and latency over time, not elapsed time
alone, determine whether an SSD cache regime was exercised.
