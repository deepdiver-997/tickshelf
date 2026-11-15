# Record adapter coverage and strictness

This ledger describes `bench/record.cpp` and `tests/test_bench_record.cpp`. Field provenance and the explicit non-wire-format boundary are in `sources.md`.

## Exact value and representation policy

- All 13 scalar slots and four book arrays distinguish absent, explicit null, and present non-null using bits 0–16. Null bits must be a subset of present bits. Unknown mask bits are rejected.
- Absent/null scalar storage must be zero; absent/null book storage must be empty. Encoders reject noncanonical hidden data instead of silently discarding it. Decoders and the generator construct canonical storage.
- Every price and `amount` is a signed 64-bit integer in units of **1e-4**. Other scalars and book volumes are signed 64-bit integers. No business-domain positivity or ordering constraint is invented. `time` must be a nonnegative unsigned 64-bit exact integer; negative zero is rejected for `time`.
- Numeric tokens are interpreted as exact decimal strings, never `double`. Ordinary decimals and scientific notation are accepted for all numeric fields when the mathematical result fits the destination exactly. Trailing decimal zeros beyond four places are accepted; **nonzero extra precision is rejected**, not rounded or truncated. Integer fields likewise reject fractional values. Signed 64-bit minimum/maximum and unsigned timestamp maximum are checked exactly, including scientific notation and values beyond IEEE-754's exact integer range.
- Non-null book depths are 0–10 inclusive. Every present non-null book must have the same depth, including depth zero; absent/null books do not participate. Null individual levels are unsupported and rejected. All five documented levels are retained; depths beyond five are explicitly experimental.
- Unknown keys and duplicate decoded keys are rejected at root, tick and quoter levels, even when a key is Unicode-escaped. `pvolume` and `pvolumn` share one canonical field identity and cannot coexist.
- `symbol`, `time` and object-valued `quoter` are required on each tick; `ticks` is the only required root key and must be an array. Empty batches and empty symbols are allowed (the frozen interface imposes no symbol naming syntax). Symbols must be valid UTF-8; NUL and control characters are preserved and escaped correctly in JSON. Invalid/overlong UTF-8 and Unicode surrogates are rejected, not replaced.
- JSON serialization emits only present keys, preserves nulls, and spells the alias canonically. It preserves record meaning, not the original key order, number lexeme, alias spelling, or escape style. Four fixed decimal places are emitted for scaled fields.

## Bounds and failure contracts

- `kMaxRecordBytes` (65,536) bounds each full binary record and each raw JSON tick span; binary symbol length also must fit `u16`. A valid maximum-size binary record may be too large after JSON escaping and is then rejected by `records_json`.
- `kMaxPayloadBytes` (4 MiB) bounds complete JSON payloads and framed binary payloads independently, and `kMaxBatchRecords` (8,192) bounds records per batch. JSON parsing/serialization additionally ensures its canonical binary batch will fit. Limits are inclusive.
- Binary decode validates magic/version, every prefix/value/count read, symbol UTF-8, masks, depth agreement, record size, batch size/count, and exact end-of-record/end-of-batch consumption. Extra bytes cannot conceal absent/null fields.
- `JsonDecoder` owns and reuses one simdjson **On-Demand** parser and padded input allocation. Schema fields are visited in input order, so neither lookup order nor duplicate keys can hide data. Every accepted field is consumed and validated, and trailing root content is rejected. A failed parse does not poison the next use of the same decoder. A decoder is not a shared concurrent object.
- JSON parse and binary batch decode clear the caller's output on entry and publish only a fully validated batch. Failure returns false with nonempty error and empty output. Single-record binary decode publishes only after full validation and leaves its old output unchanged on failure. Success clears stale error text.
- Invalid arguments to encode/serialize/generator functions throw `std::invalid_argument`; allocation failures may propagate from those throwing APIs. Parse/decode APIs convert `std::exception` failures to their error string.
- Generator options reject zero symbols and depth >10. Timestamp and `id + seed` overflow are rejected. Optional absent/null slots are canonically zeroed. `logical_id` rejects duplicate cadence 1 and implements the frozen mapping without overflow at `UINT64_MAX`.
- `record.cpp` itself depends on simdjson; it belongs only to the opt-in benchmark/test build. No default-build dependency or CMake change is introduced by these assigned files.

## Test matrix

| Area | Explicit checks |
| --- | --- |
| Documented field mapping | Original full fixture to explicit 13 scalar values and masks, every element of all four five-level books; optional fixture to explicit masks/values/symbol/time |
| Optionality | Every one of 17 fields independently absent, zero/empty, and null; omitted fields are not materialized by serialization |
| Alias and order | `pvolume` alone, canonical output, reordered root/tick/quoter keys, duplicate alias rejection in both orders including null |
| Number grammar | Ordinary/scientific/trailing-zero forms, signed extrema, timestamp maximum, exact values above 2^53, long mantissa cancellation, huge zero exponents |
| Numeric rejection | Precision below 1e-4, fractional integer, positive/negative overflow, huge exponent, invalid leading zeros/sign/fraction/exponent, nonnumeric types |
| UTF-8 and escaping | All 32 ASCII controls including NUL, quote/backslash, CJK, surrogate-pair input, maximum Unicode scalar, invalid continuation/overlong/surrogate/out-of-range/truncated UTF-8 |
| Structure and atomicity | Unknown and duplicate decoded keys at every level; null/wrong root, tick, wrapper and quoter types; missing wrappers; trailing root data; every nontrivial prefix of a fixture; valid first record followed by invalid/partial second record; parser reuse after rejection |
| Book fidelity | Every depth 0–10, all four arrays, partial absent/null books, disagreement including zero versus one, 11 levels, null/nonnumeric/fractional/overprecision levels |
| Binary protocol | Independently hand-written byte oracle for RCD1 with signed negative value, null mask and LE book data; framed batch without count prefix; all-field/optional/variable-depth round trips |
| Binary corruption | Every truncation of a full record; wrong magic/version, impossible symbol length, invalid masks, depth count >10, unequal counts, invalid UTF-8, trailing hidden byte, truncated/zero/oversized frame prefixes, partial second record |
| Bounds | Exact maximum binary record and exact 4 MiB payload accepted; one byte beyond rejected; 8,192 JSON records accepted and 8,193 rejected; excessive JSON raw record, escaped record and serialized batch rejected |
| Canonical encoder | Unknown/null-without-present masks, hidden absent/null values/levels, inconsistent/oversized depth, excessive symbol/record size rejected by both binary and JSON encoders |
| Generator | Independent explicit sample at id 123 plus default id 0; optional mask precedence across 120 ids; depth 10; invalid options and arithmetic overflow; fixed duplicate sequence and unsigned maximum mapping |

Independent local invocation (CMake is integrated by the coordinating agent) uses C++20, repository doctest, and `pkg-config simdjson`. Third-party headers are system includes for strict local `-Wall -Wextra -Wconversion -Werror` checks. Test run totals and sanitizer results are reported to the coordinator rather than encoded as a claim about unrun future builds.
