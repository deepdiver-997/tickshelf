# 行情线格式（feedgen ↔ collector）— DRAFT v1.2

全小端。帧 = 帧头(20B) + N × Tick(64B)。**本文件是官方接入契约（ADR-009），实现中发现的坑直接改这里并升 v 号。**

## 帧头（20 字节）

| 偏移 | 字段 | 类型 | 说明 |
|---|---|---|---|
| 0  | magic | u8[4] | ASCII `"TTAP"`（按字节序写入，不做整数解释，回避端序歧义）|
| 4  | version | u8 | 1 |
| 5  | type | u8 | 0 = tick 批，1 = heartbeat，2 = 查询请求/响应（v1.1），3 = 最新价请求/响应（v1.2）|
| 6  | count | u16 | 本帧 tick 数（heartbeat/查询请求为 0；查询响应=N）|
| 8  | flags | u16 | bit0：混沌帧（含乱序/重复）；bit1：查询响应带缺失区间（v1.1）|
| 10 | reserved | u16 | 0 |
| 12 | payload_bytes | u32 | = count × 64 |
| 16 | crc32 | u32 | payload 的 CRC-32（IEEE 802.3），帧头本身不校验 |

## Tick 记录（64 字节，固定）

| 偏移 | 字段 | 类型 | 说明 |
|---|---|---|---|
| 0  | symbol | char[8] | ASCII，NUL 填充，如 `"600000\0\0"` |
| 8  | ts_exchange_ms | u64 | 交易所时间戳（模拟源生成；幂等唯一键的一部分）|
| 16 | ts_receive_ms | u64 | 采集端接收时间（链路延迟观测用；**不参与唯一键**）|
| 24 | bid_price | i64 | 固定点 1e-4（如 12345000 = 1234.5000）|
| 32 | ask_price | i64 | 同上 |
| 40 | last_price | i64 | 最近成交价 |
| 48 | bid_size | u32 | |
| 52 | ask_size | u32 | |
| 56 | last_size | u32 | |
| 60 | flags | u8 | bit0：last 无成交 |
| 61 | _pad | u8[3] | 0 |

`static_assert(sizeof(Tick) == 64)`。唯一自然键 = **(symbol, ts_exchange_ms)**——落库幂等与去重全部锚定它。

## 查询帧（v1.1，双向）

- **请求**（client → queryd）：type=2、count=0、payload 24B = `sym[8] + from_ms u64LE + to_ms u64LE`。
- **响应**（queryd → client）：type=2、count=N、payload = `N×Tick64B + u32LE missing_count + missing_count×(u64 first + u64 last)`，flags bit1 置位当且仅当 missing_count > 0。
- **防线 1 按 type 分型**：tick 批/心跳维持 `count×64 == payload_bytes` 互证；控制型帧（type ≥ 2）载荷不是 64B 整数倍（请求 24B、响应含缺失块），count 对其无意义——互证退化为 `payload_bytes ≤ 256KB` 上限防炸弹，结构合法性由消费方校验。
- 缺口语义：期望 ts 网格 1ms（生成器契约）；缺失区间为闭区间 `[first, last]`，含查询范围两端的半开缺口。
- 查询命中的缺口由 queryd 侧懒触发缺口回补（backfill 键，ADR-014）；客户端重查即见闭合。
- 老实现兼容：v1.0 解析器对未知 type 按"CRC 通过放行、不产出 tick"处理（防线不受影响）。

## 最新价帧（v1.2，双向）

- **请求**（client → queryd）：type=3、count=0、payload 8B = `sym[8]`。
- **响应**（queryd → client）：type=3、count=1（有投影）/0（无）、payload = `1×Tick64B` 或空。
- 数据源：全局 HASH `latest`（field=symbol，value=64B tick 字节），由 flush 路径每次冲刷
  以**单条 HSET** 聚合更新全部 symbol（进程内先聚合取最新——ADR-016，实测教训：
  逐 tick Lua 更新投影 = N tick/M symbol 触发 N 次调用而非 M 次）。回补路径不改投影。
- 语义：投影是最终一致的（滞后 ≤ flush 间隔），且 chaos 延迟发送可使其短暂回退到
  旧 tick——下一次实时 flush 自愈；"当前价"读不应该依赖它做精确断言。

## 心跳帧

type=1、count=0、payload_bytes=0、crc32=0。collector 超过 N 秒（默认 5s）未收到任何帧判定连接死亡并重连。

## 帧内容零信任与错帧处理策略

**原则：帧头也是从不可信字节里解析出来的。** 凡要驱动状态机移动的字段（长度、计数）必须先通过校验才能使用；描述内容的字段（载荷）校验失败只需丢弃，不会破坏边界。解析器是本协议的安全边界。

### 三道防线（按状态机移动顺序）

> 本节防线描述必须与「查询帧（v1.1）」节的分型规则一致；修订防线时两处同步改。

1. **长度校验（分型，防失步炸弹）**：tick 批/心跳：`count × 64 == payload_bytes` 且 `payload_bytes ≤ MAX_FRAME_PAYLOAD`（= 64 × 4096 = 256KB）——count 与 payload_bytes 冗余表达同一信息，单字段翻转无法同时骗过两者。控制型帧（type ≥ 2）：count 不适用（载荷非 64B 整数倍），互证退化为 `payload_bytes ≤ 256KB` 上限，结构合法性由消费方校验。**上限检查必须发生在"移动状态机/扩缓冲"之前**，防损坏的长度触发巨大分配。
2. **CRC32 载荷校验**（防内容损坏）：错帧 → 丢弃 + 计数 + **继续**——长度已过互证，边界仍对齐。**本防线对所有帧类型统一生效，含控制型帧**——分型弱化的只是长度校验，不是 CRC。
3. **失步重同步**（兜底）：magic / version / 长度校验任一失败 → 进入重同步：从下一字节起扫描 magic `"TTAP"`，**候选头必须再过一遍防线 1 的分型长度校验**才算重同步成功（防载荷里碰巧出现 magic 的假锚点），否则继续扫。计失步事件。

### 处理策略表

| 情形 | 动作 | 连接 |
|---|---|---|
| CRC 错 | 丢帧 + 计数 | 保持 |
| 长度互证失败 / magic 错 | 重同步扫描 | 保持 |
| version ≠ 1 | 协议不匹配，无法降级 | **断连** |
| 心跳超时（> 5s 无任何帧） | 判死 | 断连，重连由 driver 负责 |

**版本演进策略（ADR-018）**：上表是**单版本期**的行为。引入 v2 时切换为**双版本解析窗口**：服务端同时解析 1/2（帧头 version 字段即分派点，互证规则按版本各自声明），旧版本在整个弃用窗口内保持可解析——"旧解析器不马上下线"；窗口结束后该版本恢复按不匹配断连。协议只约束分派机制，窗口长度由部署方约定。

### 测试要求（M1）

- 随机字节流 fuzz：解析器永不错误分配、永不越界，错帧后能恢复后续正常帧
- 单字段翻转（count / payload_bytes / crc32 各翻转一位）：全部被对应防线拦截
- 重同步：载荷中埋假 magic + 不一致长度 → 扫描跳过假锚点，在真帧头处恢复

## 键空间与批 id 约定（v1.3 状态 HASH，ADR-019）

- 段 `seg:{批id}`（STRING，批内 tick 64B 打包，与批 1:1）、索引 `idx:{symbol}`（ZSET，
  score=批首 ts，member=`{date}:{首}-{尾}`）——v1 逐 tick 行情键 `ts:{symbol}:{yyyymmdd}`
  已退役；未来段格式升级的迁移配方（双读 + idx 当迁移账本）见 ADR-018
- 状态 `state`（HASH，field=批id → pending/done；ADR-019：S 批一条 HSET/HDEL，遍历走
  HSCAN 不走 SCAN；v0.x 的逐批 `state:{批id}` 键已退役）
- 租约键 `lease:{批id}`（SET NX EX 60，**保留逐批键形态**——需要 per-key TTL，
  HASH field 无独立过期除非绑 Redis ≥7.4 的 HEXPIRE）
- 回补键 `backfill:{sym}:{from}-{to}`（SETNX 去重，值 done/unrepairable——ADR-014）
- 最新价投影 `latest`（全局 HASH，field=symbol，value=64B tick 字节；flush 路径单条
  HSET 聚合更新，回补不改——ADR-016）
- **批 id = `{symbol}:{yyyymmdd}:{首ts}-{尾ts}`**，由 collector 每次冲刷生成；
  首尾为批内 min/max ts，邻批区间可重叠（幂等兜底）
- **symbol 字符集约束：不含 `:` 与 `-` 的 ASCII 短名**——批 id 按 `:`/`-` 切分，接入方保证
  （feedgen/python driver 均用 `6000NN` 风格）；如需放宽，批 id 要加转义（v0.x 未做）
- 日期段为 UTC 日界（`civil_from_days` 算法，`common/civil.hpp`）

## 有意设计的坑（M4 混沌模式相关）

- 乱序：ts_exchange_ms 允许非单调（flags bit0 标注）
- 重复：同 (symbol, ts_exchange_ms) 重发，下游必须幂等
- 半包/粘包：TCP 语义天然存在，collector 解析状态机必须处理任意切分
