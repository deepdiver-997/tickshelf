# iQuant field provenance and experimental fixture boundary

## Verified primary source

- Publisher: 国信证券 (Guosen Securities).
- Title: 《国信iQuant策略交易系统 Python API 使用手册》.
- Cover metadata inspected: V1.0, completion date 2019-08-07.
- Official HTTPS URL: <https://www3.guosen.com.cn/guosen/newxwfiles/20190902/1567416998056.pdf>.
- Retrieved/verified: 2026-11-13. The date embedded in the URL is not used to infer the manual version.
- PDF SHA-256: `e02cbd0804a2d42e8d1f4288bc7ccbadc49e79e4553dbb604b6cc9b0eeb93ffe`.
- Section: “（17）获取历史行情数据 ContextInfo.get_market_data()”, beginning on physical PDF page 46 (one-based). The complete `quoter` dictionary field list is on physical PDF page 47.
- Verified against the downloaded PDF using read-only `pdftotext -f 46 -l 47`; the full extracted text has the relevant passage at lines 1848–1907. Page numbers refer to PDF physical pages, not inferred printed pagination.

Page 47 explicitly describes `'quoter'分笔数据结构：dict` and lists these **12 scalar fields** in order:

| Field in manual | Meaning in manual | Benchmark representation |
| --- | --- | --- |
| `lastPrice` | 最新价 | signed integer units of 1e-4 |
| `amount` | 成交额 | signed integer units of 1e-4 |
| `volume` | 成交量 | signed integer |
| `pvolumn` | 前成交量 | signed integer |
| `openInt` | 持仓量 | signed integer |
| `stockStatus` | 股票状态 | signed integer |
| `lastSettlementPrice` | 最新结算价 | signed integer units of 1e-4 |
| `open` | 开盘价 | signed integer units of 1e-4 |
| `high` | 最高价 | signed integer units of 1e-4 |
| `low` | 最低价 | signed integer units of 1e-4 |
| `settlementPrice` | 结算价 | signed integer units of 1e-4 |
| `lastClose` | 收盘价 | signed integer units of 1e-4 |

It also lists `askPrice` (列表，卖价五档), `bidPrice` (列表，买价五档), `askVol` (列表，卖量五档), and `bidVol` (列表，买量五档). The spelling **`pvolumn` is verbatim**, not a typo introduced by this benchmark.

## What this source does not establish

This is documentation of a Python API dictionary. It is **not** a published JSON packet, TCP capture, TTAP message, or binary transport specification. We have not found or claimed an official JSON/TCP wire protocol here. The source passage does not define missing/null semantics, fixed-point precision, alias rules, or this benchmark's binary layout.

Everything below is a clean-room benchmark decision, not a broker-protocol claim:

- The root `{"ticks":[...]}` and each tick's `symbol`, `time`, `quoter` wrapper.
- Integer millisecond `time`, UTF-8 `symbol`, and the generated sample values.
- The 13th scalar `transactionNum`: an **experimental extension**, absent from the cited manual field list; it is absent in default generator output and present with `optional_fields`.
- Exclusive alias `pvolume` for the source spelling `pvolumn`. Supplying both names, even both null, is an error. JSON serialization emits the canonical `pvolumn` key.
- The fixed-point and null/missing policies, variable depth 0–10, limits, and `RCD1`/batch little-endian binary defined in `bench/record.hpp`.

## Fixture inventory

All fixture values are **synthetic, hand-authored adaptations of the documented field names**, not market observations or official sample JSON.

- `full-five-level.json`: every scalar (including the labeled experimental `transactionNum`) plus all four five-level books. Nonuniform levels catch accidental level-1-only storage and price/volume field swaps.
- `optional-alias-unicode.json`: deliberately reordered keys, the experimental `pvolume` alias, decimal/scientific notation, UTF-8/Unicode escapes, an embedded NUL, a surrogate-pair character, missing and null fields, depth 2, empty books, and unsigned timestamp maximum. These cases exercise experimental adapter semantics, not claims about broker output.

The tests compare the original fixture JSON directly to explicit `Record` masks, integer values and every book level before testing binary/JSON round trips. A round trip alone would not detect a shared parser/serializer field-loss bug.
