#pragma once
// SQLite 落库（ADR-005：UNIQUE + INSERT OR IGNORE 即幂等）。
// 唯一自然键 (symbol, ts_exchange_ms) —— 重复 upsert 不产生第二行，这是
// "at-least-once + 幂等 = 效果上 exactly-once" 的收敛点。
#include <cstdint>
#include <string>
#include <vector>

#include "common/tick.hpp"

struct sqlite3;

namespace tickshelf {

// 落库格式版本（PRAGMA user_version 戳，ADR-018）：Tick 字段布局或表结构变更时递增。
// 打开时校验——0（新库/遗留库）盖当前值，不匹配拒绝打开：永久层的格式必须可检测，
// 旧二进制遇新格式库要响亮失败，不能拿旧字段偏移静默误解码。
constexpr int kStoreFormatVersion = 1;

class SqliteTickStore {
public:
    SqliteTickStore() = default;
    ~SqliteTickStore();
    SqliteTickStore(const SqliteTickStore&) = delete;
    SqliteTickStore& operator=(const SqliteTickStore&) = delete;

    // path = ":memory:" 或文件路径；false = 打开/建表失败（含格式版本不匹配）。
    // 打开即切 WAL：多进程并发（daemon 写穿 + persister 补偿）靠文件锁串行化写，
    // 读写不互斥——"单写者"是同时单写者（ADR-005 注记），本项目的写频次无争用。
    bool open(const std::string& path);

    // 批量幂等 upsert（单事务）。返回新插入行数（被唯一键忽略的不计）；-1 = SQLite 故障。
    std::int64_t upsert_batch(const std::vector<Tick>& ticks);

    // flush 级合并事务（ADR-012 修订）：多批一次 BEGIN…COMMIT——fsync 从每批一次降到
    // 每 flush 一次（256 符号时 = 256×fsync → 1×，写穿 stall 地板 64ms → 亚毫秒）。
    // 批指针列表（不拷贝 tick 数据）；任一步失败回滚整个 flush（回滚半径 = 全部批，
    // 恢复语义不变：release_claims → persister 读回，幂等 upsert 吃掉重做）。
    // 返回新插入行数合计；-1 = SQLite 故障（已回滚）。
    std::int64_t upsert_flush(const std::vector<const std::vector<Tick>*>& batches);

    // 耐久侧区间读（symbol 精确匹配，ts 升序）。查询失败返回空并置 query_failed()。
    std::vector<Tick> query_range(const std::string& symbol, std::uint64_t from_ms,
                                  std::uint64_t to_ms);
    bool query_failed() const { return query_failed_; }

    std::uint64_t count() const;

private:
    sqlite3* db_ = nullptr;
    mutable bool query_failed_ = false;
};

}  // namespace tickshelf
