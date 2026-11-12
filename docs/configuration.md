# Startup configuration

`tickshelfd`, `persister`, `evictor`, and `queryd` share one dependency-free startup
configuration parser and schema. The file format is a **small INI dialect, not
TOML**. The complete example is [`examples/tickshelf.ini`](../examples/tickshelf.ini).
Other tools such as `feedgen`, `floodgen`, and `capacity` are unchanged.

## Start the whole stack

```sh
# Redis must already be running at the configured endpoint.
python3 scripts/run_stack.py --build build-release --config examples/tickshelf.ini
```

The foreground supervisor validates all four applications using their own
`--print-config` output, then starts persister, queryd, tickshelfd, and evictor.
It checks child liveness and readiness, prints `STACK_READY` with child PIDs and
the log directory, and stops/waits for its own children on Ctrl-C or service
failure. It does not start, stop, or clear the external Redis. Repeated shutdown
signals do not bypass cleanup; a child requiring forced termination makes the
launcher fail rather than silently succeed.

`--log-dir NEW_DIRECTORY` selects the retained logs; existing log files are
never overwritten. `--ready-timeout SECONDS` adjusts the per-service startup
limit (default 10 seconds). A stack requires `persister.once=false`; use the
individual binary for one-shot persistence. No source/driver is launched.

For isolated synthetic validation, which creates its **own** Redis instead of
using the configured external one, see [real-source-sim.md](real-source-sim.md)
and the measured [three-trial results](ifind-sim-results.md).

## Loading and precedence

```sh
build-release/tickshelfd --config examples/tickshelf.ini
build-release/persister --config examples/tickshelf.ini --worker recovery
build-release/evictor --config examples/tickshelf.ini
build-release/queryd --config examples/tickshelf.ini --port 9911
```

Precedence is **CLI > file > existing defaults**, regardless of where `--config`
appears among CLI options. For example, these are equivalent:

```sh
build-release/queryd --port 9911 --config examples/tickshelf.ini --print-config
build-release/queryd --config examples/tickshelf.ini --port 9911 --print-config
```

- A file is loaded only with `--config PATH`; there is no automatic search or
  implicit default file. `--config` may appear only once.
- Only startup loading is supported: no hot reload, includes, environment-variable
  interpolation, or tilde expansion. Normal shell expansion occurs before the
  program receives CLI arguments; quote shell arguments to preserve literal `$`/`~`.
- Repeated ordinary CLI options use the last value. Each supplied value is still
  validated, so an invalid earlier value is not silently hidden.
- CLI options use separate arguments (`--port 9911`), not `--port=9911`.
  Unknown flags, positional arguments, flags belonging to a different component,
  missing values, and invalid values are errors. A following `--flag` is not
  consumed as the previous flag's value.
- All file sections and keys are schema-validated, including sections not consumed
  by the executable. Unknown sections/keys, invalid types/ranges, and duplicates
  fail even if an equivalent CLI setting would override them.
- All validation happens before opening a database, connecting to Redis, or
  creating a listener. Errors go to stderr and return exit status **2**.

### Database path origin

A relative `[storage] db` value is resolved against the **configuration file's
directory**, made absolute, and lexically normalized (without resolving symlinks).
The example's `db = "../ticks.db"` therefore names `ticks.db` in the repository
root when the example is used in place.

A relative CLI `--db` value, and the unchanged no-file default `ticks.db`, remain
relative to the **process working directory**. A file that omits `storage.db`
does not change that default's origin. Absolute file paths remain absolute.
Loading/checking configuration does not create database files or parent directories.

## INI syntax

Files contain named sections and one `key = value` assignment per line. Section
names and keys are case-sensitive. There are no top-level keys or nested sections.
Whitespace around section names, keys, `=`, and unquoted values is ignored.
LF and CRLF line endings and an optional initial UTF-8 BOM are accepted.

```ini
[redis]                  # A section comment
host = "127.0.0.1"        ; Either comment marker works
port = 6379

[persister]
worker = 'recovery #1; west'  # Comment markers inside quotes are literal
poll_ms = 1000
once = true
```

- `#` and `;` start comments anywhere **outside** quotes. Blank/comment-only lines
  are ignored.
- Values may be unquoted or surrounded by matching single or double quotes.
  Quotes must surround the entire value; concatenated quoted fragments or text
  following the closing quote are invalid.
- Both quote styles support exactly the same escapes: `\\`, `\"`, `\'`, `\n`,
  `\r`, and `\t`. Unknown escapes, unclosed quotes, and multiline values are
  rejected. Backslashes in unquoted values are literal.
- Quoted whitespace and quoted comment markers are preserved. Numeric and boolean
  values may also be quoted; their decoded text is validated in exactly the same
  way as an unquoted value. Strings must be nonempty UTF-8 without NUL.
- Booleans are exactly `true` or `false`, not `yes/no`, `on/off`, or `1/0`.
- Integers are decimal digits only (leading zeroes allowed): no signs, suffixes,
  fractions, separators, or hexadecimal. The percentage accepts nonnegative
  decimal fractions and decimal exponent notation, but not a leading sign,
  NaN/infinity, trailing text, overflow, or underflow to zero.
- Repeating a key or a section is an error, even with identical values. This is
  not a merging/include mechanism. Arrays, tables, and other TOML features are
  not implemented.

## Schema and defaults

Shared Redis/storage settings belong **only** in `[redis]` / `[storage]`, not in
component sections. For instance, `[tickshelfd] redis_host` and `[queryd] db` are
errors. Existing CLI flags for these shared settings continue to work.

All four applications consume `[redis]`:

| Key | CLI | Default | Validation |
| --- | --- | --- | --- |
| `host` | `--redis-host H` | `127.0.0.1` | Nonempty string |
| `port` | `--redis-port P` | `6379` | Integer 1–65535 |

`tickshelfd`, `persister`, and `queryd` consume `[storage]`:

| Key | CLI | Default | Validation |
| --- | --- | --- | --- |
| `db` | `--db PATH` | `ticks.db` | Nonempty path; origin rules above |
| `shards` | `--shards N` | `1` | Integer 1–2147483647 |

`evictor` validates storage entries in a shared file but does not consume or print
them and does not accept `--db`/`--shards`. Shard counts and database paths must
agree across writing and querying processes. File naming/routing behavior is
unchanged: one shard uses `db`, multiple shards use `db.0`, `db.1`, etc.

### `[tickshelfd]`

| Key | CLI | Default | Validation / meaning |
| --- | --- | --- | --- |
| `bind` | `--bind H` | `127.0.0.1` | Nonempty string |
| `port` | `--port P` | `9900` | Integer 1–65535 |
| `flush_interval_ms` | `--flush-interval-ms MS` | `500` | Positive millisecond interval |
| `persist` | `--persist none\|sqlite` | `none` | `none`: classic persister read-back; `sqlite`: write-through |

The example explicitly selects `sqlite`; omitting both file and CLI settings
still selects the original `none` mode. Storage settings are validated and
included in effective JSON even when `persist=none`.

### `[persister]`

| Key | CLI | Default | Validation / meaning |
| --- | --- | --- | --- |
| `worker` | `--worker ID` | `persister` | Nonempty prefix; runtime worker ID remains `<prefix>-<PID>` |
| `poll_ms` | `--poll-ms MS` | `1000` | Positive millisecond interval |
| `once` | `--once` / `--no-once` | `false` | Boolean; one poll instead of a continuous loop |

`--no-once` overrides `once=true` from the file. `--once` overrides a file value
of `false`; these flags take no value and the last one wins.

### `[evictor]`

| Key | CLI | Default | Validation / meaning |
| --- | --- | --- | --- |
| `soft_watermark_bytes` | `--soft-watermark-bytes N` | **Required** | Positive unsigned 64-bit integer |
| `emergency_watermark_bytes` | `--emergency-watermark-bytes N` | `0` | 0 disables tier3; otherwise greater than soft |
| `sample_ms` | `--sample-ms MS` | `1000` | Positive millisecond interval |
| `bytes_per_tick` | `--bytes-per-tick N` | `0` | Unsigned 64-bit integer; 0 skips growth reconciliation |
| `growth_tolerance_pct` | `--growth-tolerance-pct PCT` | `50.0` | Finite, nonnegative decimal number; percentages over 100 are allowed |
| `inflight` | `--inflight` / `--no-inflight` | `false` | Boolean; tier2 enablement |

The soft watermark is required for an evictor invocation. A shared file may omit
`[evictor]` entirely when used by other applications. If the section is present,
all applications validate its required soft watermark and emergency/soft
relationship. For an evictor invocation, its CLI overrides are applied before
checking these cross-field requirements; for example, its CLI may supply an
omitted soft watermark. An explicitly invalid file value such as `soft=0` (using
the full key above) is still an error and cannot be masked by CLI.

`--no-inflight` overrides a file value of `true`; `--inflight` overrides `false`.
Neither flag takes a value. The original safe defaults remain: tier2 off and
emergency tier3 off. Enabling these modes does not change their existing data-loss
semantics. The example soft watermark (256 MiB) is an illustration, not capacity
planning for your Redis instance.

### `[queryd]`

| Key | CLI | Default | Validation |
| --- | --- | --- | --- |
| `bind` | `--bind H` | `127.0.0.1` | Nonempty string |
| `port` | `--port P` | `9910` | Integer 1–65535 |

Millisecond intervals are limited to **1–9223372036854**, so conversion to signed
64-bit nanoseconds cannot overflow. Unsigned 64-bit byte values are at most
**18446744073709551615**. Shards/ports are range-checked before narrowing; negative
values and integer overflow never wrap into valid settings.

## Validation and machine-readable effective configuration

Every application provides:

```sh
build-release/queryd --config examples/tickshelf.ini --check-config
build-release/queryd --config examples/tickshelf.ini --print-config
build-release/evictor --help
```

- `--check-config`: run normal parsing/validation, then exit **0** with no stdout
  on success. It does not test Redis reachability, port availability, or database
  access permissions.
- `--print-config`: run the **same parsing/validation**, print **exactly one JSON
  object followed by a newline to stdout**, and exit **0**. No runtime messages,
  network connections, listener setup, or database opens occur. Invalid config
  prints no JSON and exits 2 with a stderr diagnostic.
- `--help`: print usage and exit 0 without loading the file or requiring an
  evictor watermark. Unknown flags/missing CLI values are still rejected. If
  `--check-config` or `--print-config` is also present, full validation wins over
  help; if both check and print are present, print wins.

Automation should consume `--print-config` rather than reimplementing the INI
dialect. Object keys are fixed snake_case names; strings are JSON-escaped, numbers
are JSON numbers, and booleans are JSON booleans. Key order is not an API contract.
No action flags or config filename are included.

Exact key sets, shown with no-file defaults (evictor supplied with
`--soft-watermark-bytes 1`):

**tickshelfd**

```json
{"redis_host":"127.0.0.1","redis_port":6379,"db":"ticks.db","shards":1,"bind":"127.0.0.1","port":9900,"flush_interval_ms":500,"persist":"none"}
```

**persister**

```json
{"redis_host":"127.0.0.1","redis_port":6379,"db":"ticks.db","shards":1,"worker":"persister","poll_ms":1000,"once":false}
```

**evictor**

```json
{"redis_host":"127.0.0.1","redis_port":6379,"soft_watermark_bytes":1,"emergency_watermark_bytes":0,"sample_ms":1000,"bytes_per_tick":0,"growth_tolerance_pct":50,"inflight":false}
```

**queryd**

```json
{"redis_host":"127.0.0.1","redis_port":6379,"db":"ticks.db","shards":1,"bind":"127.0.0.1","port":9910}
```

`worker` is the configured prefix, **not** a generated PID-suffixed runtime ID.
`db` is absolute for a file-sourced path and remains as supplied for a CLI/default
path. Thus a consumer of relative CLI/default paths must use the same working
directory as the eventual process. Parsing/printing does not rewrite configuration
files or alter runtime storage, ingestion, persistence, eviction, or query behavior.

## Offline tests

The startup configuration doctests all have the explicit `startup_config:` prefix:

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DTICKSHELF_BUILD_TESTS=ON
cmake --build build-release --target tickshelf_tests tickshelfd persister evictor queryd
build-release/tickshelf_tests --test-case='startup_config:*'
```

CMake also registers a filtered `startup_config` test with that same selector.
Do **not** run the unfiltered test executable/CTest merely to check configuration:
existing integration cases use a shared Redis endpoint and may call `FLUSHDB`.
The startup-prefixed tests use only uniquely owned temporary config directories,
with no network or SQLite operations.
