# 设计决定记录（ADR）

> 纪律：每个决定写 背景 / 决定 / 代价。通用模式的自学实现，取舍自己做、自己记、可推翻（推翻时追加新 ADR 并标注旧 ADR 状态）。
> 当前交付/耐久合同为 [`tickshelf-delivery-v1-20261117`](delivery-contract.md)（ADR-024）。
> 旧 ADR 中“有界丢失/AOF ~1s/崩溃必补偿/at-least-once + 幂等”等描述不得推导为端到端保证：
> 最终 Storage 提交前均可能永久 loss、无固定 RPO；pending 恢复须缓存完整副本仍存在。
> 2026-11-14/15 历史机器证据不改写、不回填本合同或未观测配置。

## ADR-001 热缓冲用 Redis，不引入消息队列

- **背景**：采集速率与落盘速率解耦需要缓冲。候选：Redis / Kafka / 进程内自研队列。
- **决定**：Redis，初版 ZSET 承载（score = 行情时间戳，天然支持按时间范围读；现段式见 ADR-017）。Kafka 的价值在语义（持久化、回放、消费组），本项目不建立持久日志/上游 replay 契约，允许最终 Storage 提交前永久丢失，引入 broker 不是当前目标；进程内队列仍是易失缓冲，且没有“按时间读”的能力。
- **代价【ADR-024 修订】**：Redis 不是恢复权威，允许 no-AOF/no-RDB；进程内/Redis pending 丢失没有固定 RPO，AOF everysec 也不能把端到端窗口证明为 ~1s。对账能发现缺失但不保证有源可补。队列的价值在语义不在容量，Redis 此处只是缓冲队列，不是持久提交日志。

## ADR-002 租约协议：claim 是唯一仲裁点，正确性由幂等兜底

- **背景**：多 persister 实例消费同一缓冲，不能重复劳动，也不能因进程崩溃丢活。
- **决定**：认领 = `SET lease:{批id} {worker} NX EX {ttl}`（原子性来自 Redis 单线程命令语义——查候选批可以读到脏数据，claim 才是仲裁点）。落库幂等：UNIQUE(symbol, ts_exchange) + `INSERT OR IGNORE`。状态用**独立标记键** `state:{批id}`，不嵌进行情数据，写完无条件 SET，与淘汰无竞态。核心认知：**正确性不依赖租约**——at-least-once + 幂等 = 效果上的 exactly-once，租约只是减少重复劳动的效率优化，TTL 判错（过期双做、崩溃后重做）代价只是多写一次。批量场景用分块 + 续期（worker 周期性 EXPIRE），不做动态估时。
- **代价**：租约过期但进程还活着的双活窗口 → 幂等兜底；标记 SET 落空（数据已被淘汰）无害。
- **边界【ADR-024】**：上述 at-least-once 仅指完整缓存批仍存在时的重试尝试；effectively exactly-once 仅指唯一键下的 Storage 幂等效果。丢失段/发现状态或全部易失副本后，不保证重做，更不是 TTAP 到 Storage 的端到端 exactly-once。

## ADR-003 批次按固定时间间隔，不按固定条数

- **背景**：collector 攒批写 Redis 的时机。
- **决定**：固定冲刷间隔（默认 500ms），批大小随流速自漂。不做自适应控制器——给余量充足的系统加控制器是负资产。延迟上界 = 间隔。
- **代价**：流速剧烈波动时单批忽大忽小 → 可接受；夜间批量场景（若做）单独按吞吐调参，不与实时通道共用。

## ADR-004 淘汰：层间按丢失代价，层内按行情时间（M2 实现）【状态注记 2026-11-08：tier2 默认关——M2 演练实测 stale lease × tier2 = 真丢失窗口（worker 被杀后租约 TTL 内，tier2 误判"worker 内存有数据"而删其数据），见 roadmap M2 实测/教训；安全水位语义（连续 done 前缀）取代原"按批区间淘汰"的隐含设想，纯由 state:* 键名推导】

- **背景**：内存水位超阈值需要腾地方；Redis 自带 maxmemory 不认识"是否已持久化"，淘汰语义必须自定义。
- **决定**：层间优先级 = 已持久化（免费）> 租约在飞（几乎免费：worker 已把整批读进内存，Redis 那份没了照样落库）> 完全崭新（真丢，全系统只有 Redis 有）。层内按行情时间，老的先走（arrival order ≠ quote-time order，所以按行情时间不按入库时间）。两层正交。
- **代价**：需要淘汰进程遍历读状态键 → 键空间设计时就把状态键做成可扫描的（见数据模型节）。

## ADR-005 v1 落库用 SQLite，PG/ClickHouse 是后续里程碑

- **背景**：需要"真实唯一约束 + 幂等 upsert"的最小自包含载体。
- **决定**：SQLite（UNIQUE 索引 + `INSERT OR IGNORE`）。零运维、单文件、可进 CI；PG（`ON CONFLICT` / COPY 批量）与 ClickHouse（列存、ReplacingMergeTree 去重、异步 insert）作为 M5+，届时对比行为差异并记录。
- **代价**：SQLite 单写者 → v1 只跑一个 persister 实例，正好符合"架构留并行度、日常单实例"的口径；无网络故障面（也少了练 DB 重连的机会，接受）。

## ADR-006 只用模拟行情，永不接真实数据源

- **背景**：真实行情数据有源商授权问题；学习项目需要一键复现。
- **决定**：feedgen 生成带突发特性的随机游走 tick（线格式见 protocol.md），字段形状按真实行情常见形态设计（双档报价、固定点价格、交易所/接收双时间戳）。M4 加混沌模式主动制造脏数据（乱序、重复、断流、重连突发）——真实世界会自己脏，模拟世界要自己脏。
- **代价**：没有真实数据的意外 → 靠混沌注入器补。

## ADR-007 容量模型即代码（M2 实现）

- **背景**：管线容量应该是一笔静态可验证的账，而不是"跑了才发现"。
- **决定**：启动时与配置变更时计算五条不等式：①稳定性（落盘最坏速率 > 峰值流速×安全系数）；②瞬时队列深度（突发时长×净增速 < 缓冲上限−水位）；③出口带宽 < 网卡×余量；④单 symbol 一天的缓冲占用上限；⑤持续净增长（max(0, 峰值−持久化最慢速率) × 时段 × 每tick占用 < 预留内存，2026-11-11 补充——②管单次突发、排水记零，⑤管持续工况、按最慢排水抵扣；超账 = 保障失效 ≠ 必溢出，运行面由 evictor 增速对账看实测）。违反即拒启或告警。账面常数（单条编码大小、ZSET 放大）从运行时采样校准，不写死；预测内存增速 vs 实测增速偏差即告警——抓配置漂移也抓编码 bug。
- **代价**：要维护一个小的采样/校准模块，值得。

## ADR-008 v1 collector 用阻塞 socket + pipeline，事件循环推迟到 M3 【部分取代：M3 事件循环计划取消，见 ADR-009；"阻塞 + pipeline" 部分仍有效】

- **背景**：epoll/kqueue 是学习目标，但事件驱动应该落在已验证正确性的组件上，一次改一层。
- **决定**：M1 的 collector 用阻塞读写 + RESP pipeline（吞吐型负载不需要低延迟事件循环）；M3 引入 epoll/kqueue 抽象层改写 collector，并做背压（写积压 → 暂停读）。
- **代价**：M1 阶段单线程阻塞意味着 feedgen 断连会阻塞批定时器 → 用简单的时间片循环（非阻塞读 + 定时器轮询）绕开，仍不引入完整事件循环。

## ADR-009 接入契约 = 线协议 + 守护进程，跨语言走协议不走绑定（2026-11-09）【边界再确认 2026-11-10：真实数据源驱动（如 vendor 的 JSON 拉取/解析，simdjson 之类）属于"源特定"，由消费侧服务自研，不进本 repo——本项目保持 vendor 中立的 sink 契约；TTAP 二进制对高频友好，JSON 源在边缘适配后转 TTAP push。接真实源时需新 ADR + 真实 schema/凭据 + 离线测试方案】

- **背景**：真实生产环境里数据源驱动必然由接入方自研（各源协议、账号、限流都是源特定的），本项目只规范"数据进来之后"的部分。原 M3 的 epoll/kqueue 事件循环对纯 mock 数据流 ROI 有限（进程内异步网络已由其他项目覆盖，见 ADR-008 状态注记）。
- **决定**：三层结构——
  1. **`libtickshelf` 静态库**：攒批、RESP 客户端、租约落盘等核心组件，暴露进程内 ingest API（单元测试直连，不走网络；C++ 嵌入者可直接链库）；
  2. **`tickshelfd` 守护进程**：TCP server 收 TTAP 帧（`docs/protocol.md` 升格为**官方接入契约**）→ 调核心库 ingest；无状态，崩溃即重启；
  3. **接入方用任何语言按 TTAP 协议推数据**——协议就是跨语言接口。不做 C ABI 绑定（稳定性负担与版本化地狱，收益不成比例）；函数指针只留在核心库内部当测试缝。feedgen 降级为参考驱动，仅用于验证契约可跑通。
- **收益**：driver 崩溃被进程边界隔离（与租约/淘汰的故障隔离主题同构）；跨语言零绑定维护；M3 里程碑删除。
- **代价**：localhost 多一跳 TCP（demo 速率无碍）；未来若做超低延迟路径是共享内存环（M5+，可参考 journal 头指针 commit 模式）；feedgen 从 server 变 client。

## ADR-010 与 quantify 的关系：不复用代码，只引用模式（2026-11-09）

- **背景**：同目录已有 quantify demo（行情 → SPSC → 双副本订单簿 → mmap 恢复）。评估过直接复用其代码。
- **决定**：不搬代码——两者模型不同：quantify 的 Tick 是订单簿**事件流**（ADD/MODIFY/DELETE、整型价位下标、seq 全局递增），tickshelf 是**报价流**（双档+成交、64B 线格式、(symbol, ts) 唯一键）；quantify 的 mmap 持久化解决"内存状态崩溃快速恢复"，tickshelf 解决"租约 + 幂等入库"。只引用三处模式：
  1. M3 对账补拉的历史环形缓冲 → 参考 quantify journal 的 head 原子 commit 模式（不复制实现）；
  2. **崩溃一致性两种方案的对比叙事**：commit-point 恢复（内存状态大、事件重放便宜 → 恢复现场）vs 幂等重放（数据已入耐久库 → 允许重做，唯一键去重）——选型依据是恢复成本与重放成本之比；
  3. 简历定位互补：quantify = 进程内低延迟（无锁队列、双副本、mmap），tickshelf = 跨进程数据管线（协议契约、缓冲、租约、分层存储、容量账）。
- **代价**：无（引用模式不是依赖）。

## ADR-011 I/O 与线程模型：两层生命周期 + 单线程 poll 时间片循环（2026-11-09）

- **背景**：接入端不做事件驱动后端（ADR-009），但字节流仍需入口。以往 asio 风格的"每流一个 session + shared_ptr 回调链"习惯需要按本项目条件重估。
- **决定**：
  1. **两层生命周期分离，不搞"每 symbol 一个 session"**：传输层 `ConnSession`（每 TCP 连接一个：字节缓冲 + 帧状态机，**随连接生死**）× 数据层 `Ingestor`（symbol → 累积缓冲表，**随 symbol/交易日生死**）。TTAP 帧内 symbol 交织，字节流只有连接一条，半包状态只能挂连接；symbol 拆分发生在帧解析之后的结构化数据上。两层生命周期不同步（连接死了，symbol 状态还在），这正是不能合并成一个 session 对象的原因。
  2. **零 shared_ptr**：同步单线程流没有悬空异步操作；tick 是 64B POD，按值拷贝便宜于智能指针间接。asio 的 `shared_from_this` 习惯是"异步完成晚于发起"这一模型的产物，不迁移。symbol 8 字节打包成 u64 做 map key，零分配。
  3. **单线程 poll 时间片循环**：`poll(fds, timeout = 距下次 flush)` → 可读连接逐个 `on_bytes` → 到点 flush。poll(2) 只是多 fd 等待原语，不构成事件驱动后端，不违反 ADR-008/009 的禁令。【修订 2026-11-11，灌流基准抓到】"时间片"必须是真的：`read_session` 的 recv 循环原本没有读配额，持续洪泛下内核缓冲永远非空（每次 recv 满 64KB → 循环继续），单个高速客户端即可无限推迟 flush/stats 定时器——实测 42.9M ticks 只进内存不落 Redis、pending 膨胀 2.7GB、stats 键过期消失。修：每会话每轮 1MB 读配额，配额用尽即让出（数据留内核，下轮 poll 立即续读）。总吞吐不变，定时器节奏有界——ADR-007 的 flush 预算、ADR-013 的 stall 观测都以此为前提。
  4. **背压交给 TCP 窗口**：Redis 写慢 → flush 慢 → 接收放缓 → 内核反压 driver，不写应用层队列。二线程（网络线程 + SPSC）留作可选扩展，触发条件 = Redis 慢到阻塞主循环（mock 速率不会发生）；届时 quantify 的 SPSC 是唯一代码复用点（ADR-010 修订），交接用被 move 的 `vector<Tick>`，不是 shared_ptr。
  5. **多 driver 发同一 symbol 不在入口去重**：收敛点在 SQLite 唯一键——at-least-once 的既定语义，入口重复无害。
- **帧内容零信任**：解析器是安全边界——帧头本身也是不可信输入，长度字段必须交叉互证后才能驱动状态机，详见 protocol.md"帧内容零信任与错帧处理策略"。
- **代价**：单线程 = flush 期间 ingest 串行（TCP 反压是正确行为，不是故障）；poll 的 fd 扫描 O(n)（接入方是少量 driver，接受）；accumulator 无上限（容量账 M2 管）。

## ADR-012 持久化路径：异步接口与 daemon 写穿（2026-11-08，双实现共存，异步仍 deferred）

- **背景**：评审提出两项工程建议——①持久化提供异步接口（完成后执行函数指针回调，如自动改租约标记）；②持久化在 flush 到 Redis 之前就发起（daemon 侧写穿），省掉"写进 Redis 再被 persister 读回来"的字节往返。提问：异步有必要吗？
- **分析**：
  1. **异步接口的必要性绑定在写穿方案上，单独存在无收益**。当前 persister 是独立进程，天然与摄入路径异步：SQLite 批写 ~ms ≪ persister poll 周期 300ms ≫ 摄入路径 0 依赖——进程内再做异步回调，端到端时延一点不降，只多出线程/回调复杂度。真正需要异步的位置是 daemon 侧（见 2），那是写穿的前提而非独立特性。
  2. **写穿是一次架构分叉，不是局部优化**。收益真实：省掉 tick 字节 2×Redis I/O、ZRANGEBYSCORE 读回、SCAN 轮询时延（落库时延从 poll 周期级降到 flush 级）。但代价：SQLite 单写者（ADR-005）→ 落库所有权必须从 persister 进程迁进 daemon；daemon 无状态性被打破（ADR-009/011 的故障隔离叙事改变）；**崩溃窗口重构后读回路径仍要保留**——daemon 死于写库中途的批只在 Redis 里，重启后仍要从 Redis 补账，persister 降级为崩溃补偿器而不能删除；租约协议退化为 daemon 实例间仲裁（当前单 daemon，等于没有）。内存占用：M1 两种方案完全相同（无淘汰，Redis 全量持有）；收益要等 M2 evictor 才兑现（持久化更早 → 可淘汰更早）。
  3. **读回不是浪费，是消费契约**：任何语言/进程只凭 Redis 键空间就能落库，这是 at-least-once + 多实例消费 + 崩溃隔离的来源（ADR-001/002/009）。localhost 下实测代价：10min 演练 216 万 tick 双向 ≈ 460KB/s，RTT ~50µs——当前规模不构成瓶颈。
- **决定（2026-11-08 修订：双实现共存，A/B 实测后异步仍 deferred）**：评审意见指出单写者只是 SQLite 的限制（PG/ClickHouse 无此约束，ADR-005 本就排期后置）且两条路径应可对比。采纳为**同一租约协议下的双实现**，不是两套系统：
  1. **写穿（`--persist sqlite`）**：flush pipeline 顺序改为 **lease→state→ZADD**——认领先于 state 可见，persister 的既有仲裁（SETNX）天然挡下，零新机制；随后 daemon 内联 SQLite upsert → 批量 [SET done, DEL lease]。SQLite 故障 → release_claims 降级回读回路径。好路径一条 tick 字节只写 Redis **一次**（ZADD），状态收尾是每批一条 SET（比预想的"写两次"还省）。
  2. **经典读回（`--persist none`，默认）**：不变。persister 在两种模式下都是**条件式孤儿补偿器**：daemon 崩于写库中途 → 若完整段与可发现 pending 状态仍在 Redis → TTL 过期后可走读回路径接手。缓存副本丢失不保证恢复（ADR-024），租约本身不恢复字节。
  3. **WAL 化**：SQLite 开 WAL + busy_timeout，"单写者"= 同时单写者（文件锁串行），两进程写频次无争用；多进程同瞬间首开撞 WAL 初始化锁 → open 小退避重试。
  4. **异步/多线程仍 deferred，但这次有数据**：写穿同步内联的实测 stall = **3ms 均值 / 16ms 峰值**（负载 8×500/s），占 500ms flush 间隔的 0.6%/3.2%——触发条件（stall 逼近间隔）远未满足。线程化的两个候选形态记录在案：①SQLite 写移入工作线程 + 主循环收割完成队列（回调改租约标记）；②Redis 客户端并入 poll 循环做多路复用（Redis 慢=反压是正确行为，现在做是负资产，ADR-008 同款逻辑）。
- **实测（bench_compare.sh，8×500/s × 15s，无注入）**：两模式 SENT==rows 零丢失。Redis 命令 CPU 合计 A≈194ms / B≈169ms；**读回字节的差距才是大头**：A 的 ZRANGEBYSCORE 把 60816×64B ≈ 3.9MB 读回 persister，B 仅 21 批作废读（RTT 合并管道里 SETNX 失败但读已随行——认领失败路径的一次性浪费，约 10KB）。经典模式落库追平 248ms / 写穿 463ms（都在最终 flush+关停语义内，无实质差距）。
- **代价**：daemon 持有 SQLite 句柄，"无状态"严格来说破了一条口子（崩溃窗口由租约+幂等兜底，drill 语义不变）；persister 以 reaper 身份常驻时每 poll 的 SCAN/GET 是固定底噪（可调 poll 间隔换 laziness）。
- **决定修订（2026-11-11，摄入天花板实测驱动）：写穿落库事务边界从批原子改为 flush 原子**。256 符号 × 每批一事务 = 每 flush 256 次 COMMIT ≈ 256 次 fsync，实测 stall 地板 64ms——单是地板就越过 ADR-013 的 10% 预算触发线，"多符号 × 每批一事务"天然把写穿推进需要决策的工况。合并为 `upsert_flush`（全部已认领批一次 BEGIN…COMMIT）后地板消失（50k 步 stall_max 64→41ms，残量 = 线性斜率 + WAL checkpoint 尖峰）。**语义变化**：SQLite 故障的回滚半径从"当前批"扩大到"整个 flush 的全部已认领批"——恢复路径不变（release_claims → persister 读回，幂等 upsert 吃掉重做），单次故障的重做范围变大，故障罕见且幂等，接受。**实测钳位 700k → ~850k ticks/s**，未到天真的 1.3M 预测：预算里还有另外两个串行分项（Redis 管道收发 + 解析）与 upsert 共享单线程——下一个杠杆候选：①`PRAGMA synchronous=NORMAL`（WAL 下进程崩溃安全，仅断电风险，需独立 ADR 改永久层耐久契约）；②ADR-012 本来 deferred 的工作线程持久化（把 upsert 挪出主循环，两分项与解析重叠）。

## ADR-013 负载暴露 vs 在线模式切换：观测落地，控制备案（2026-11-08）

- **背景**：评审提议持久化组件暴露负载，Ingestor 每次 flush 自主决定首写是否带租约（写穿 vs 空白）；并确认最小决策单位不能是单 Tick。
- **分析**：
  1. **粒度：批级，确认**。租约/批id/state 键全是批级；单 Tick 级混搭被 ZSET member 幂等兜着"无害"但零收益。决策单位 = 一次 flush 的整轮批次（整轮统一带或不带租约），决策频率 = flush 周期。
  2. **"哪边负载低走哪边"的双向择优不成立**：SQLite 写吞吐两模式相同（DB 层串行化），差别是 stall 由谁承担（daemon ingest 关键路径 vs persister 隔离进程）+ 写穿省读回 I/O 与轮询时延。所以策略是**单边**的：写穿为常态优选，经典模式只作为"保 ingest"的降级手段。
  3. **两个信号的映射是反直觉的**：persister backlog 大 → 应该**更多**写穿（别再喂它，backlog 才能排干）；daemon stall 大 / flush 死线连续失约 → 应该**更少**写穿（挪回隔离进程）。一个真实的控制器需要权衡两者——但现在不建。
  4. **现在不建控制器**：实测 stall 3/16ms 对 500ms 预算有 30~160× 余量，控制器在"两模式行为无差异"的工况里是死代码（ADR-003 同款原则）；对冲 daemon 饱和的既定方案是 ADR-012 的工作线程持久化，而非运行时换拓扑（非平稳的崩溃窗口更难推理）。
- **决定**：①**负载暴露落地**——各组件发布 `stats:{component}` 自过期键（EX 10，k=v;... 文本），persister 带 `backlog`（SCAN 待处理批数）/claimed/skipped/rows，tickshelfd 带 persist 模式/stall_avg/stall_max/pending；发布失败静默（观测不反噬主流程）。②**在线切换不做**，策略草图备案：写穿常态；降级信号 = daemon flush 死线连续失约；重访触发 = stall p99 > flush 间隔 10%，或出现落库时延 SLO，或 M3 补拉场景出现持续 backlog。
- **代价**：stats 键 ~1 QPS 写放大（可忽略、自过期不进淘汰视野）；观测与控制分离——未来接控制器无需新管道，只换消费端。

## ADR-014 缺口回补：懒触发，客户端请求驱动（2026-11-08）

- **背景**：原 M3 计划"按 (symbol,ts) 连续性找缺口 → 从 feedgen 环形缓冲补拉 → 幂等入库"未指明触发方。评审提出：主动巡检对没人读的数据白做功，应由客户端请求触发。
- **分析**：
  1. 缺口检测本来就免费发生在查询点——SDK 契约就是返回"已知缺失区间"元数据（显式缺失 ≠ 静默空）。检测与回补天然同点挂钩：查询发现缺口 → 触发回补 → 重查。主动巡检是凭空多出的第三条路径。
  2. 回补请求去重（多客户端同缺口/重试）→ `backfill:{sym}:{range}` SETNX——租约仲裁第三次复用（批落盘、写穿认领、回补去重），一套模式三种仲裁场景。
  3. **保留窗边界必须显式**：feedgen 环形缓冲有限 + M2 按行情时间淘汰——缺口停留过久会落入"ring 已环绕且热数据已淘汰"的不可回补态。处理不是静默吞掉：回补执行器对窗外缺口写显式 `unrepairable` 标记，SDK 元数据如实上报。"缺失永远显式"的语义不破。
  4. 主动巡检的唯一正当价值是完整度**指标**（对账计数），那是观测不是回补——并入容量账/演练输出即可，不做后台回补循环。
- **决定**：M3 对账语义改为**缺口回补（懒触发）**——SDK 查询返回缺失区间时才发起回补；回补执行器以 backfill 键 SETNX 去重；窗外缺口显式标记不可回补。补拉数据仍直接落库不走热缓冲（防"最老先淘汰"的回补循环，原计划保留）。
- **决定修订（同日，评审确认）**：补拉传输路径**降级为实现侧自由**，不进契约——契约面只有 backfill 键状态机（requested→done/unrepairable）；feedgen 怎么交付 ring 数据（侧端口/快照文件/直接写 SQLite）是拉取进程的实现细节。回补数据不进热缓冲的理由补强：它是历史冷数据，写进热缓冲会被 M2"最老先淘汰"立即再删，且热缓冲语义就是新数据——正路是 feedgen → 直接 SQLite 幂等 upsert → backfill 键 done，SDK 重查即见闭合；"先回 SDK"的 inline 交付是可选优化。
- **代价**：冷数据缺口依赖 ring 保留窗，窗外永久缺失（显式上报，接受）；首个查询者承担回补时延（lease 去重后其余等待者轮询 backfill 键状态即可）。

## ADR-015 查询面：语义进核心库，跨语言走 TTAP 双向查询进程（2026-11-08）

- **背景**：M1 系统只有摄入面——tickshelfd 的 accept 循环只 recv，是生产者的被动接入点，无任何查询监听。M3 SDK（读穿缓存 + 缺失区间元数据）需要一个读侧形态。评审问：查询是不是"另一个后端自己查 Redis 然后 HTTP 返回"？
- **分析**：
  1. **查询不能搭在 tickshelfd 上**：单线程 poll 循环，查询 burst（大范围 ZRANGEBYSCORE）直接顶住摄入——违背故障隔离主题（查询负载 ≠ 摄入负载，与"driver 崩溃 ≠ daemon 崩溃"同构）。独立进程的判断成立。
  2. **接口形态按 ADR-009 语义修一处**：契约是 TTAP 双向化（查询请求/响应消息类型），不是 HTTP——协议即跨语言契约对读侧同样成立；HTTP 若要（运维面板/浏览器）只能是查询进程外挂的皮，不进契约。
  3. **语义进核心库**：Redis（热）+ SQLite（耐久）合并读、缺口检测、repair 触发——全放 libtickshelf，沿用"先核心库直连测试、再包网络前端"模式；C++ 嵌入者进程内直用（ADR-009 第一层），任意语言经薄 queryd 说 TTAP 双向。
  4. **读侧并发已免费**：SQLite 开 WAL 后任意进程并发读不阻塞写（双写改造的顺带红利），queryd 只读打开同一文件即可。
- **决定**：M3 落两个东西——①libtickshelf 查询组件（合并读/缺口/回补触发语义）；②`queryd` 薄进程（TTAP 查询消息类型，包核心库）。HTTP 不进契约。
- **代价**：TTAP 变双向（帧 type 空间要分上行/下行，protocol.md 升版）；多一个进程形态（测试直连核心库的成本没变）。

## ADR-016 最新价投影 + 聚合写（2026-11-10）

- **背景**：更新"最新行情投影"的常见实现是逐 tick 调 Lua 脚本——N tick / M 个 symbol 触发 N 次调用而非最多 M 次；写入路径也未按 key 批量。本项目同病：pipeline 已批量，但命令粒度未按 key 聚合。
- **决定**：
  1. **按 key 聚合 ZADD**：Batch 本就按 (symbol, 交易日) 分桶，同批 tick 共享同 key——一条多对 `ZADD key s1 m1 s2 m2 …` 取代逐 tick N 条。命令数与 tick 数解耦（N tick/M 股 = M 条）。
  2. **全局最新价投影 `latest`**：HASH（field=symbol，value=64B tick 字节），flush 路径**进程内先聚合取每 symbol 最新，再单条 HSET** 更新全部 symbol——一条命令、一次往返。查询面加 type=3 最新价帧（queryd 走 HGET 快速路径，不扫 ZSET 尾部）。
  3. **投影只由 flush 路径维护**：回补路径不碰投影（历史数据不该覆盖最新）；投影最终一致（滞后 ≤ flush 间隔；chaos 延迟发送可短暂回退到旧 tick，下一次实时 flush 自愈）——"当前价"读不应对它做精确断言。丢失可重建（SQLite 按 symbol 取 max ts 重放）。
  4. **刻意不用 Lua**：逐 tick Lua 方案的根因是没做进程内聚合；聚合后每 flush 只剩无条件写，SET/HSET 足够——不是"Lua 不行"，是"聚合之前不需要 Lua"。
- **代价**：投影滞后 ≤ flush 间隔且 chaos 下可能短暂回退（可接受，语义已文档化）；`latest` 键 O(symbols) 常驻不淘汰（体积极小，丢失可重建）。
- **实现注记**：flush 命令构建抽成纯函数 `build_flush_commands`（可无 Redis 单测命令结构）；写穿管道 per-batch 命令数从 2+N 降到 3，classic 从 1+N 降到 2。

## ADR-017 存储布局 v2：段式 + 索引（读取侧优化，2026-11-11）

- **背景**：读取侧优化的通用思路——"索引记录位置，只返回命中的几段原始数据"，避免整份行情复制与应用层过滤（定点价经浮点转换有精度损失）。tickshelf 原布局（逐 tick ZSET）同病：ZRANGEBYSCORE 整段返回后服务端逐 tick 解码再重编码（两次多余拷贝），每 tick ~198B 内存足迹，10s 窗口查询 53ms。
- **决定**：
  1. **段式布局**：每批一个原始字节段 `seg:{sym}:{date}:{首}-{尾}`（STRING，批内 tick 64B 打包）+ 每符号索引 `idx:{sym}`（ZSET，score=批首 ts，member 携带日期与区间）。段与批 1:1，段键 = "seg:" + 批id。
  2. **读路径 verbatim**：索引查段 → MGET 命中段 → 段字节原样进响应载荷（服务端零解码零重编码，逐 tick 只抽 8B ts 做缺口检测）——"只返回命中的几段原始数据"的字面实现，精度无损天然成立。
  3. **evictor 简化，水位机退役**：段与批 1:1 使批生命周期原子化——done 段整段删（数据必然已落库），不再需要水位机（ADR-004 的水位是逐 tick ZSET 下"批区间混住 done/pending 成员"的产物，那个正确性问题在段式下构造性消失）。tier2/tier3 语义保留，粒度粗化为整段。
  4. persister 读回 = GET 段键（批 id 直接推导，RTT 合并管道不变）；latest 投影不受影响。
- **实测（read_bench.sh，4×500/s 灌 45s，同负载 A/B）**：
  | 窗口 | ZSET 基线 | 段式 v2 | 变化 |
  |---|---|---|---|
  | 1s（1k ticks） | 2.66ms | 1.71ms | −36% |
  | 10s（10k ticks） | 52.95ms | 13.45ms | **−75%** |
  | 30s（16k ticks） | 119.38ms | 23.04ms | **−81%** |
  | 内存（66k ticks） | 13.09MB（198B/tick） | 7.48MB（113B/tick） | −43% |
  窗口越大收益越大——查询成本从 O(ticks) 变为 O(命中段数)。
- **代价**：pending 段不可部分淘汰（tier2/3 粗化为整段粒度）；段内 tick 乱序（chaos 延迟发送）由客户端排序；done 段待淘汰期间与 SQLite 双份（与原布局一致）。

## ADR-018 格式迁移与旁路策略：版本戳 + 双读配方 + 预算规则（2026-11-11）

- **背景**：生产期三类通用课题——①资源/额度配额分级与保底；②有计算成本的处理走旁路（与主管线隔离、独立监控资源占用）；③存储格式迁移"双读 + 旧解析器不马上下线"。本项目刚做过 v1→v2 存储迁移（ADR-017），是 dev 期 flag-day（清库重来），生产期不可用——正好补上"下次怎么迁"的空白。
- **决定**：
  1. **SQLite 格式版本戳（本次落地）**：`PRAGMA user_version` 盖 `kStoreFormatVersion=1`，打开校验——0（新库/遗留库）盖当前值，**不匹配拒绝打开**。永久层（SQLite 行）格式必须可检测：旧二进制遇新格式库要响亮失败，不能拿旧字段偏移静默误解码。迁移配方 = 经典双读：新旧 reader 并存、按版本分派，旧 reader 保留到旧格式数据清零才退役。
  2. **Redis 段格式的未来迁移（备案）**：idx ZSET 天然是**迁移账本**——每段必在索引中，`idx:{sym}` 扫描即精确枚举待迁移段，"迁移完成" = 账本扫不到旧格式成员。v3 段若变编码（如压缩），段前缀或 idx member 携带格式号，双读按段分派。热层 flag-day 是开发期接受未提交缓存丢失的动作，不是“最坏只丢 500ms 一窗”的保证（ADR-024 无固定 RPO）；永久层仍必须检测格式不匹配——所以版本戳落在 SQLite 而非 Redis。
  3. **TTAP 版本退役策略（备案，protocol.md 已注记）**：当前单版本期 `version≠1` 断连；引入 v2 时 = **双版本解析窗口**——服务端同时解析 1/2（帧头 version 字段即分派点），旧版本在整个弃用窗口内保持可解析（"旧解析器不马上下线"），窗口结束恢复按不匹配断连。
  4. **回补任务的预算/优先级（备案，真源接入期要求）**：当前回补源是自家 ring buffer，零外部成本，无预算可耗；接真实供应商后回补执行器需要——任务优先级（交互查询触发的补缺 > 批量历史拉取）、额度预算（每轮/每分钟令牌）、**保底线**（额度低于阈值只放行高优先级）。与 ADR-004 内存水位同构（保底层 + 余量层），且与渐进收敛天然兼容——回补本就容忍任意慢，降速只是收敛变缓不是失败。
  5. **派生数据走旁路，flush 只内联零成本聚合**：`latest` 投影是"顺手聚合"（零计算成本）所以内联在 flush；任何有计算成本的派生（K 线聚合、指标）必须走独立消费者进程——薄入口 + libtickshelf 纯函数，即 persister/evictor/queryd 同款形态，绝不进 tickshelfd 的时间片。旁路的故障隔离与资源监控复用现成机制（进程边界 + `stats:{component}` 观测面），不需要新增设计——"旁路处理与主管线隔离 + 独立监控"的通用实践，在本项目里的对应物就是组件模型本身。
  6. **拉取配置热更新（备案，适配层职责）**：盘中调整订阅集（改 symbol 列表/数据级别/频率）——**配置是数据不是语法**：不需要发明映射语言，版本化配置键（`pull:config`，值 = symbol 集合 + 级别 + 频率的 JSON/二进制 + 版本号），适配层轮询或订阅变更 → 重订阅拉取管道 → 新数据自然进 TTAP。与持久化管道零耦合（TTAP 契约不变，管道只认帧不认订阅）。一个 tick 的不同事件类型（快照/成交/委托）同样适配层内消化为 Tick 语义或 flags。真正要造"语法"的那天（用户可编程的拉取策略），先问 ADR-003：这工况出现的频率配得上一个 DSL 吗。
- **代价**：版本戳是 DB 头部一个整数（无表无行），每次 open 多读一次 pragma；v1 残留（quote_key 辅助、两处文档的 ts: 行情键条目）随本 ADR 一并退役。

## ADR-019 命令数与符号数解耦：状态键 HASH 化（2026-11-11）

- **背景**：SYMS=5400 生产基数对照（benchmarks.md）暴露"flush 固定成本随符号数线性涨"——classic 摄入 2.2M→1.5M（−30%）。逐批 `state:{批id}` 键是主要贡献者：每 flush 3S+1 条命令 + 收尾 2S 条，5,400 符号时每轮 ~27k 命令的构建/编码/回复解析全在 daemon 主线程。
- **分析**：段 SET 与 idx ZADD 按符号天然分键（查询按符号范围走 idx），无法合并；state 是纯标记（批id→pending/done），聚合进一个 HASH 后 flush 变一条 HSET（S 个 field）、收尾变一条 HSET + 一条多键 DEL、遍历走 HSCAN（SCAN 的键名前缀剥离坑随键形态消失）。租约**不能**跟进去：需要 per-key TTL（HSETNX 无 EX；HEXPIRE 要 Redis ≥7.4，不做版本耦合）。
- **决定**：`state:{批id}` 逐批键退役 → `state` HASH（field=批id → pending/done）；finish/release 改单条多键 DEL；写穿认领块前置（连续 S 条 SETNX，claimed 步长 1，管道序仍保证 lease 先于 state HSET——ADR-012 仲裁前提不变）。语义不变：HSET/HDEL 原子性与逐键 SET 等价，崩溃窗口表逐行照旧。
- **实测（SYMS=5400）**：classic 摄入 1.5M→**1.9M**（基数税 −30%→−14%）；写穿固定成本地板 70→51ms（50k 步 stall_max）。高步平台样本噪声大（2 采样窗/步），趋势不动结论。
- **代价**：dev 期 flag-day（键空间形态变更，公开仓库无存量数据）；单 HASH 内存 ≈ 原 S 个小键（省每键键名头开销）；未来若需按批 TTL，HEXPIRE 是唯一路径（备案）。剩余的基数税（−14%）在 seg/ZADD 每符号一对命令 + 回复解析，进一步解耦需聚合键设计（备案观察）。

## ADR-020 分片路由层：纯函数路由，无路由数据库（2026-11-12）

- **背景**：分片规模曲线（benchmarks.md）确认 N 个文件 + N 个独立写者的聚合上限与拐点；单股单库不可行（5,400 只 = 5,400 文件与句柄爆炸）。提出"中心路由库 + 互斥读写"的设计疑问。
- **分析**：路由的需求本质是"symbol → 分片"的确定性映射。若映射是**纯函数**（FNV-1a(symbol) % N），则路由是函数不是状态——零查询、零锁、零崩溃一致性面；中心路由库（一张表互斥读写）会把要并行化的写路径重新串行化到一把锁上，还引入"路由库挂了怎么办"的新故障域。代价：纯函数路由**改 N 需要重哈希迁移**（数据按旧函数分布）——rebalance 是运维动作不是常态路径，备案（迁移账本思路同 ADR-018：新分片组上线双读，idx/耐久层枚举迁移进度）。
- **决定**：`ShardedTickStore : TickStore`（第二存储形态落地，rule of three 到点，接口 `TickStore` 显式化提取）。①路由 = FNV-1a % N，规范化输入 = 去 NUL 的 symbol 字节（写侧 strnlen(8)/读侧 string::size()，两侧同规范否则读写分家）；②每分片独立文件（`db.i`）+ 独占写线程（SQLite 单写者由线程所有权保证）+ 有界队列（满则阻塞=背压，⑤运行时对应物）；③demux：批跨多股 → 按路由拆分片缓冲（一次 64B/tick 拷贝），upsert_flush 外部语义同步不变、内部并行等 future 聚合；④查询 = 单分片直查（纯函数保证一个 symbol 只在一个分片），无扇出归并；⑤`TickStore` 抽象基类 = open/upsert_batch/upsert_flush/query_range/query_failed/count，persister/daemon/queryd 按 `--shards N` 经工厂装配，默认 1 完全兼容旧行为。
- **代价**：demux 拷贝（分片化固有）；单进程内读写不同时（进程形态天然满足，跨进程靠 WAL）；分片文件数 = N（句柄/内存随之线性，N≤8 有曲线背书）。
- **实测**：4 分片端到端写穿 SENT 419,968 = ROWS 419,968、四文件均匀分布（104,992/片，FNV-1a 均匀性）、组内零重复零故障行；单测覆盖路由确定性/跨 flush 幂等/查询一致性/N=1 退化。
- **附录：分片队列的锁竞争分析（问题：要不要换 SPSC + unique_ptr 载荷）**：
  1. **访问模式恒为 SPSC**：upsert_flush 同步等待 ⇒ 恰好一个生产者（串行）+ 一个分片写线程消费者——mutex+cv 实现的就是 SPSC 语义，只是带锁。
  2. **队列深度结构性 ≤1**：生产者提交后立即阻塞等 future，锁步运行——任何时刻每分片在飞 Job ≤ 1，有界队列的槽位从未用到第二个。所以锁竞争在任何存储速度下都到不了可见程度，不是"很难出现"。
  3. **存储速度的角色（必要非充分）**：消费者处理快（SSD 快/批提交均摊后）只是缩短"消费者抱住 Job 的时长"，把碰撞窗口占比维持在 ~10⁻⁵（µs 级持锁 / 250ms 周期）。批提交均摊的是事务/fsync 成本——队列作业数 = flush 节奏 × 分片数，与批内 tick 数无关（写 1 千条和 1 万条对队列是同一件事），均摊让消费者变快但**不增加抢锁次数**。
  4. **冲突出现的两个前置条件**：①拆掉同步等待（生产者持续投递 = 工作线程持久化的异步形态）；②交接粒度变细到流式（ADR-010 备案的 SPSC 场景）。两者同时满足时，第一个可见症状也不是锁竞争，而是**队列填满 → 生产者阻塞在 cv**——背压（⑤）本来就是降压阀；届时换 SPSC 的理由是"无锁交接的 µs 级尾延迟成为产品指标"，不是"锁慢"。
  5. **载荷**：tick 数据不经过队列（Job.batches 指向 demux 缓冲，payload shared_ptr 保活），队列元素已是指针尺寸小对象；unique_ptr 包装在锁版下无收益（deque 节点独立分配，move 零拷贝）；promise/future 的每 flush 堆分配同理（N 分片 × 2 次/s）。
  结论：mutex+cv 保留；触发条件（千次/秒级 flush 或队列操作进 stall P99）钉死在 sharded_store.hpp 注释。

## ADR-021 性能诊断双轨：原始五档 JSON 实验不改变生产契约（2026-11-13）

- **背景**：目标服务器出现 3–5k 条/s 的落库吞吐，规范化文件读取加 hash/查重约 40k 条/s；既有 TTAP 64B 一档 Tick 的合成结果不足以代表五档变长 Record、前置内容去重和 SQLite FULL 的组合。仅采 CPU 栈也不足以解释提交、锁与磁盘等待。
- **决定**：保持 ADR-006/009 的生产边界，不接真实账号和业务数据；新增默认不构建的 `bench/` 双进程实验台。按公开 iQuant 手册的 quoter 字段构造原始 JSON，经 TCP、simdjson、变长 canonical Record、逐记录 hash/查重、Redis 到耐久存储；实验 JSON 包装和 IQB1/RCD1 格式不宣称为官方导出或网络协议。现有四服务完整链路另行回归，不用实验程序冒充原管线。短路、记录/字节/等待封顶、实际 PRAGMA、独立 oracle、同轮采样和版本/构建证据由实验配置驱动；仅消费模式不得计作持久化。批 hash 对照明确为逐记录同算法、SHA context 复用，并不减少实际摘要记录数。
- **存储对照**：SQLite FULL 全记录表与追加段文件加 SQLite FULL 的 symbol/time 段级稀疏索引保存同一 canonical 内容。文件先同步，再提交索引，最后确认；索引须具备范围读取能力，不能省略真实索引工作来美化结果。恢复验证已索引段及其范围索引，仅未索引尾部允许回收。该后端是实验原型，不替换生产 TickStore，也不承诺硬件 PLP 或已完成物理断电验证。
- **代价**：新增 simdjson/OpenSSL 仅为可选实验依赖；JSON 到 Record 使用严格已声明 schema，不支持的字段拒绝而非静默丢弃。实验 Redis 是隔离的暂存/读回阶段，不实现四服务的租约补偿；字段覆盖与协议兼容性必须分开描述。稀疏索引减少索引行，但范围查询需读候选段并过滤，追加文件还增加独立同步/恢复成本；性能优劣由同耐久条件实测决定。阶段 timer 存在嵌套，禁止直接求和；本机吞吐不能证明目标部署达标，需在目标服务器另跑 30k offered-rate 稳态与长跑。

## ADR-022 五档实验台的紧凑键、同步索引与硬容量验收（2026-11-14）

- **背景**：宽 canonical BLOB 主键的 SQLite FULL 基线不能代表紧凑数据库；短跑也没有覆盖库容、checkpoint 与 FIFO 驱逐。Mac 100k 条/s 是工程目标，不是目标环境 30k 的硬件换算系数。
- **决定**：保留宽表基线，新增独立 schema2 的 rowid 全记录表，固定32B非唯一 SHA-256 索引仅筛候选，完整内容决定相等；二级索引不再复制大 BLOB。可信内部摘要复用必须以业务字节未变为前提，Redis 下游仍来自真实 GET，并经过完整 canonical 验证。批中、重放、并发 writer 和强制摘要碰撞分别测试。
- **文件＋索引**：TSG1 数据文件先同步，SQLite FULL 或 RocksDB WAL+sync WriteBatch 再原子提交完整批索引及高水位，之后才 ACK。相同 symbol/time 稀疏范围与恢复/尾回收合同，不省查询索引；保持物理重复与逻辑去重的区别。RocksDB 为可选固定版本实验依赖，不替换生产 TickStore。必要 flush/compaction 等待/SyncWAL/close 在完成计时内；数据文件与索引没有跨资源原子事务。开源固定源码、Mac 底层同步差异与 RTTI 构建根因见 [rocksdb-reference-20261114.md](rocksdb-reference-20261114.md)。
- **验收**：完整原始五档 JSON、真实解析、SHA-256 查重、Redis roundtrip、FULL、零重复基线与声明证券基数缺一不可。硬 floor 使用包含最终 ACK 的端到端唯一完成率，实际发送至少10秒，并同时检查供给、积压与尾部；不能把 95% offered 或短跑当作100k通过。最大批量不是实际批量，按各阶段真实剩余队首记录记龄，独立记录 rows/bytes/age/tail 触发与实际事务大小。
- **代价与边界**：摘要缓存为有预算的 FIFO 窗口；单份 payload 和固定开销记账不等于 RSS 上限。稀疏索引节省写入，但增加候选段读取/过滤及启动恢复工作。全记录 SQL 布局和追加文件不是相同物理去重合同；宏观 fsync 名称也不代表相同硬件承诺。先以可关闭布局/批量对照取证；没有证据需要时不增加后台 checkpoint 或异步派生索引，更不降低 FULL 来换成绩。

## ADR-023 分片异步持久化引擎：把 flush 从收包线程解耦（2026-11-15）

- **背景**：旧路径在单线程 poll 循环里同步完成 Redis pipeline 与 `store_->upsert_flush()`；写穿时 `ShardedTickStore` 内部虽有多写线程，调用方仍等全部 future。ADR-012 把异步/多线程 deferred，ADR-020 附录把“异步化 + 流式交接”列为会改变瓶颈的前置条件。现在该条件被显式跨过，但以 opt-in 方式保留旧路径。
- **决定**：`tickshelfd --engine on` 使用 `components/engine/`。源线程按 `symbol` 路由到每分片一个 `Ingestor` staging；达到条数/字节/年龄后 `drain()` 封成不可变 `Slice`（内部仍是 `std::vector<Batch>`，批 id/段/索引/state/lease 协议不变，重试不重新分组）。每分片一个有界队列 + 单 worker；write-through 复用 `flush_batches`/`finish_written_batch_ids`/`release_claim_ids`，publish-only 只发布。Redis 每分片一条连接，SQLite 每分片一个 `SqliteTickStore`，文件与路由和 `ShardedTickStore` 完全一致（`shard_route.*` 共享，含历史非标准 FNV 常量）。
- **写穿策略**：v1 静态，`--persist sqlite` = 写穿，`none` = 仅发布；不做负载控制器（ADR-013 的信号映射仍成立）。去重可选、默认关闭：每分片 FNV-1a(64B) + 完整 64B 比较，FIFO 预算可配；Tick 定长，不需要 SHA/OpenSSL。唯一键仍是最终收敛点。
- **失败语义**：发布失败在同一 Slice 上退避重试；pipeline 部分生效后 `SETNX` 对自己租约返回 Null 时，补 `GET lease:{id}` 与 worker_id 比对，避免误判他人持有而漏写存储。存储失败只 `release` 自己的精确 id 集合，Slice 标 degraded，交 persister 补偿；finish 失败重试后计 `finish_pending`，数据已耐久不谎报丢失。`drain` 在整个 deadline 内允许重试，超时后停止并报告未完成条数；未完成不伪装成零。
- **背压**：每分片 `pending_records` 上限；`submit` 只接收前缀，被拒尾部进 daemon 的有界 retry buffer；buffer 满则不再 poll 会话（数据留内核，TCP 反压 driver），避免“可读但故意不读”的忙循环。队列满时症状是源端阻塞/拒绝，不是锁竞争——与 ADR-020 附录的预判一致。
- **代价与实测边界**：真实队列深度出现，`mutex+cv` 的 `notify_one`/谓词循环与 timed wait 需要被 TSan 覆盖；每分片慢存储会反压整个源（有意的背压）。在 Mac 合成 TTAP flood（4 分片、write-through、`floodgen` 4 进程）上，80k/400k/1M offered 两组路径都完整落库，稳态约 7.8万/37.8万/87.0万 行/s（按实际行数完成判定），**Engine 没有可归因的稳态吞吐提升**；旧同步 flush 在该负载下已远超 30k。结论：这次交付的是解耦、可配置与可观测性，不是免费的性能倍数；若目标服务器仍只有 3–5k/s，瓶颈在生产 schema/同步模式/单条 Record 处理，不能靠本引擎自动消除。
- **验证**：engine 单测覆盖发布/部分认领/存储降级/finish 失败/重试/背压/年龄封口/去重/两分片/drain 超时；私有 Redis 端到端覆盖写穿精确行数与 kill -9 后 persister 补偿；TSan 11 用例 + 端到端通过；ASan/UBSan 全离线 98/34964 通过（并修复了两处既有测试自身的越界：`test_frame_parser` 20B 后写 4B 锚点、`test_resp` 15B 字面量按 17B 构造）。旧路径默认不变，persister/queryd/evictor 不改。

## ADR-024 交付边界与只读运行态证据（2026-11-17）

- **背景**：进程接收、Redis 发布、最终 Storage 提交是不同完成点；旧“500ms 一窗/AOF ~1s/孤儿必补偿”叙述容易把易失缓存误当作恢复权威。不同实验 backend 的同步强度也不能由同一个 durable 计数证明。
- **决定**：以 [`tickshelf-delivery-v1-20261117`](delivery-contract.md) 版本化交付合同。`accepted` ≠ `published` ≠ `storage_committed`；最终 Storage 提交前在途、内存 staging、队列与 Redis pending 均可能永久丢失，没有固定 RPO。TTAP 当前无摄入 ACK，不虚构上游 durable log/offset/replay。Redis 可 no-AOF/no-RDB，不是恢复权威；经典 pending recovery/Engine 降级仅在完整缓存段与发现状态仍存活时成立。at-least-once 限于该重试域，effectively exactly-once 限于 Storage 唯一键/内容幂等效果。
- **耐久与验收不降级**：服务 SQLite 保持现有设置，不悄然改动；`SqliteTickStore` 启用 WAL 但未显式设置 synchronous，以实际连接 PRAGMA 回读解释，不断言默认 FULL；显式 FULL 部署不得降级。Benchmark 的 SQLite FULL、RocksDB WAL+sync 或追加文件先同步再同步索引基线不变；强度依实际 backend/sync，不以预提交允许 loss 为借口降级。健康 benchmark 仍须独立对账零意外 missing/重复/内容错误；故障 drill 的允许 loss 必须明确记录，不能冒充健康 PASS。
- **观测决定**：`scripts/stack_runtime.py` 提供 `redis_runtime_snapshot`，只连显式 host/port，记录 caller-declared `owned_private|external`、requested/observed、UTC 时间、server 版本/PID；逐项 CONFIG GET appendonly/save/appendfsync/maxmemory/maxmemory-policy 与 INFO server/persistence/memory。ACL/缺失/错误为 explicit denied/unknown/error 与 partial 汇总，不猜默认值；no-AOF 下 appendfsync 标 inactive。不改私有启动参数/外部 Redis，不执行 CONFIG SET/REWRITE、SAVE/BGSAVE、FLUSH、SHUTDOWN；不接入无网络的 `--print-config`。
- **代价与证据边界**：接受预提交永久缺失且不能给定固定损失预算；观测是分时读取而非原子持久性证明。2026-11-14/15 历史报告/机器证据不改写，不回填新合同或未观测设置。synthetic、`production_qualified=false` 和公开字段来源/授权要求保持原意；DCRB 为 `not_adapted`，不新增适配/生产兼容声明。

## ADR-025 RESP 客户端就绪驱动化：send/collect 解耦与可中断 I/O（2026-11-18）

- **背景**：旧 RespClient 是阻塞 socket + `SO_RCVTIMEO(5s)`：收回复占用整个调用线程，半死连接要等满超时；`Engine::drain` 的 deadline 只能唤醒重试退避，无法打断已进入适配器的 Redis I/O（旧文档如实承认 join 干等）。消融实验显示 Redis 阶段是真实热点之一（`R=0` 行完成率显著更高），客户端阻塞行为正是公司侧实验的第一个热点。
- **决定**：连接 fd 改非阻塞，一切等待 `poll` 就绪驱动（回复字节到达由内核唤醒，不轮询）；API 拆为 `send_pipeline`（字节交给内核即返回，不等回复）与 `collect_replies`（就绪时收，先收割解析缓冲残留、可跨调用续收）并显式携带 `RespDeadline`；`interrupt()` 以 `shutdown(SHUT_RDWR)` 从其他线程立即唤醒阻塞中的等待。`RedisShard` 端口新增 `interrupt()`，`Engine::drain` 超时放弃时对全部 Redis 端口调用——drain 的 deadline 对 Redis I/O 从此是硬上界。`exec()` 保留为 send+collect 的兼容封装：flush/finish/release、persister、reader、queryd、bench 全部零改动。
- **拒绝的替代**：完整 epoll/kqueue 事件循环 + 回调框架。单线程多路复用只省线程数，不改变任何已测瓶颈（热点在存储提交与发布重试）；每分片专职 worker 已把 Redis I/O 移出收包线程；且违背"RESP/网络手写、零外部框架"的学习目标。Engine 内"发布发送与上一片存储提交重叠"的两阶段 publish/finish 是独立的吞吐优化，评估后再做，不搭车。
- **边界**：被中断或超时的连接一律视为不可信，调用方 close 后重连（Engine 的懒重连语义不变）；Storage 侧（SQLite 提交）的阻塞 I/O 仍不可中断，drain 报告如实区分。`send/collect` 解耦当前无生产调用方（为后续流水线重叠预留），行为由离线假服务器测试覆盖：半包重组、跨调用留存、截止失败、中断唤醒，均不依赖 redis-server。

## ADR-026 StorageOnly 写策略：历史回填不经 Redis（2026-11-18）

- **背景**：写穿/仅发布都以 Redis 为热缓冲与租约仲裁。历史回填、单写者导入这类"只要落库"的负载不需要热缓冲，却要为每分片维持 Redis 连接并付出发布与 finish 往返——消融表里 `R=0` 行的差值就是这个成本的上界（54k 单轮观测：compact +20%、RocksDB +33%）。
- **决定**：`WriteMode::StorageOnly`（`--persist storage`，必须 `--engine on`）：Engine 不建 Redis 端口、不发布、不租约仲裁、无 finish；worker 把整片直接交给 `StorageShard`。`committed_input`/`inserted_unique`/`degraded`/`sink_consumed` 与写穿同义；没有 `published`/`skipped`/`finish_pending`。daemon 在此模式跳过 ADR-013 负载键发布（无意义重连也不做），退出检查 `storage_complete`。
- **边界**：没有租约仲裁——多写者同库只有存储唯一键幂等兜底；不写 `state` HASH，persister/淘汰对这批数据无可处理对象（数据已直接落库）。这不是"更耐久"或"更快"的一般承诺，只是少做了发布义务的口径；交付合同不变（最终提交前允许丢失，Storage 同步强度依实际配置）。

## ADR-027 单机 raw journal：本体先于索引，水位是恢复提示（2026-11-18）

- **背景**：StorageOnly 路径没有 Redis 副本，"最终提交前允许丢失"的窗口即每次 Storage 提交前的全部在途数据；索引提交本身也缺一个可重放的本地权威。
- **决定**：`RawJournalShard`（StorageShard 装饰器）：每批先追加 raw 帧（`u32 count + u64 seq_first + 64B ticks`，版本戳文件头）并 `F_FULLFSYNC`/`fsync`，**之后**才调用内层 Storage 提交；raw 追加失败则索引绝不提交。索引提交失败 → raw 已留痕，下次 open 重放（幂等 upsert）补齐。水位文件 `<shard>.raw.wm` 经临时文件+fsync+rename+目录 fsync 原子落盘，open 时钉住、此后每 32 次提交摊销——它是**恢复游标不是耐久承诺**：落后无害（多重放一段），超前即违约。撕裂尾部（崩溃半帧）在 open 时按最后完整帧边界截断并计数；水位之后不足整帧的尾部按幂等重放。
- **拒绝的替代**：查询时 digest 校验兜底（把静默损坏升级为可检测丢失，但违反"提交后不允许错误"）；先提交索引再补 raw（顺序颠倒使第三象限成立）；每批一次水位落盘（多一次 fsync，收益只是重放更短）。
- **边界**：仅限 `--engine --persist storage --raw-journal` 单写者路径；写穿以 Redis 租约为恢复权威不叠加。raw 同步不是跨资源事务，故障窗口语义见 [delivery-contract §4.1](delivery-contract.md)。macOS fsync 的 barrier 语义不做断言，能探测到 F_FULLFSYNC 时使用之。分布式拓扑的 raw 所有权与水位语义在合同 §8 声明为边界，不实现。

## ADR-028 写穿两级流水线：发布 RTT 与存储提交重叠（2026-11-18）

- **背景**：写穿 worker 串行执行 publish_and_claim（Redis RTT）→ upsert_flush（SQLite 提交）→ finish（Redis RTT）；SQLite 提交期间网络空闲、等 RTT 期间存储空闲。消融表显示两段都是真实成本（54k 单轮：Redis 段 wall ≈9%、存储段更大）。ADR-025 的 send/collect 解耦为此预留了接缝。
- **决定**：`EngineConfig::pipeline`（`--engine-pipeline`，仅 `--engine --persist sqlite`）：每分片两级线程——发布线程 `ready` 队列 pop → publish_and_claim（重试/退避不变）→ 有界 `handoff` 队列（容量=inflight）→ 存储线程 mine 过滤/upsert/finish。存储线程用**第二条 Redis 连接**做 finish/release（RESP 客户端非线程安全，不共享；租约状态在 Redis 服务端，跨连接无碍）。切片是共享指针，tick 数据不过队列。发布线程或存储线程放弃（停止/交接队列关闭）时切片保持 pending 计数，与单级模式同语义；`queue_depth`/`queue_peak` 覆盖两级。
- **拒绝的替代**：单线程合并 finish+下一片 publish 的 collect（只省 RTT 不重叠 SQLite）；finish 经命令队列绕回发布连接（跨线程确认通道比第二条连接更复杂）。不默认开启：先以 native 套件同口径实测，确认收益与稳定性后再考虑改默认。
- **边界**：流水线不减少任何工作，只重叠等待；吞吐上界仍是两段中较慢的一段（Little 定律）。`inflight` 翻倍内存语义（ready+handoff 各 inflight 个切片）。finish 晚于提交的合同语义不变（finish_pending 单独计数）。NullRedis/NullStorage 组合在流水线下行为与单级一致。
- **首轮实测（2026-11-18，Mac、10k 条/64 证券/2 分片/批 256、每格独立 warmup、单轮）**：native 十六格全 PASS，但方向分化——I/O 等待主导的格 pipeline=on 明显更快（real+null：558k→728k 条/s；null+sqlite FIFO on：679k→729k），而 **real+sqlite 关键格反而慢约 8–30%**（397k→309k、331k→282k）。原因：该规模下 SQLite 提交每批仅亚毫秒，两级交接的锁/唤醒开销与第二条连接成本占比超过重叠收益。结论与决定一致：**保持默认关**；当存储提交变重（更大批量、FULL 同步、数毫秒级）时重叠收益才可能显现，需在目标负载同口径重测后再评估。单轮小样本不构成吞吐结论。

## 数据模型（Redis）

- ~~行情键 `ts:{symbol}:{yyyymmdd}`（v1 逐 tick ZSET，member=64B Tick 原始字节）~~ — ADR-017 起退役（dev 期 flag-day 换段式；生产期格式迁移走 ADR-018 双读配方）
- 租约键：`lease:{批id}` — SET NX EX，TTL 60s，worker 每 20s 续期（**保留逐批键形态**——需要 per-key TTL，HASH field 独立过期需 Redis ≥7.4 HEXPIRE，不做版本耦合）
- 状态：`state`（HASH，field=批id → pending/done，ADR-019）——S 批一条 HSET/HDEL、遍历走 HSCAN；批id = `{symbol}:{yyyymmdd}:{首ts}-{尾ts}`
- 最新价投影：`latest`（全局 HASH，field=symbol，value=64B tick 字节；flush 单条 HSET 聚合更新，回补不改——ADR-016）
- 段（v2，ADR-017）：`seg:{批id}`（STRING，批内 tick 64B 打包）、索引 `idx:{symbol}`（ZSET，score=批首 ts，member=`{date}:{首}-{尾}`）
- 回补键：`backfill:{sym}:{from}-{to}` — M3 懒触发回补的请求去重（SETNX，ADR-014），值 done/unrepairable
- 设计约束：状态必须可增量遍历（HSCAN，淘汰/落库进程都要遍历）、行情键必须可按 symbol 分片（未来扩展：全操作 per-symbol，天然可哈希路由）
