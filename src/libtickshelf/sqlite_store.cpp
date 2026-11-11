#include "sqlite_store.hpp"

#include <sqlite3.h>

#include <chrono>
#include <cstdio>
#include <thread>

namespace tickshelf {
namespace {

constexpr const char* kSchema = R"(
CREATE TABLE IF NOT EXISTS ticks(
  symbol         TEXT    NOT NULL,
  ts_exchange_ms INTEGER NOT NULL,
  ts_receive_ms  INTEGER NOT NULL,
  bid_price      INTEGER NOT NULL,
  ask_price      INTEGER NOT NULL,
  last_price     INTEGER NOT NULL,
  bid_size       INTEGER NOT NULL,
  ask_size       INTEGER NOT NULL,
  last_size      INTEGER NOT NULL,
  flags          INTEGER NOT NULL,
  PRIMARY KEY(symbol, ts_exchange_ms)
) WITHOUT ROWID)";

// 格式版本戳（ADR-018）：0=新库/遗留库 → 盖当前值；匹配 → 放行；不匹配 → 拒绝。
// 返回 0=ok，1=瞬时失败（首开并发撞 user_version 写锁，上层重试兜），-1=致命不匹配。
int check_or_stamp_format(sqlite3* db, char** err) {
    sqlite3_stmt* stmt = nullptr;
    int ver = -1;
    if (sqlite3_prepare_v2(db, "PRAGMA user_version", -1, &stmt, nullptr) == SQLITE_OK &&
        sqlite3_step(stmt) == SQLITE_ROW)
        ver = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    if (ver < 0) return 1;
    if (ver == kStoreFormatVersion) return 0;
    if (ver != 0) {
        std::fprintf(stderr,
                     "sqlite: 落库格式版本不匹配（库=%d 期望=%d）——拒绝打开，"
                     "迁移按 ADR-018 双读配方（新旧 reader 并存至账本清零）\n",
                     ver, kStoreFormatVersion);
        return -1;
    }
    const std::string stamp = "PRAGMA user_version = " + std::to_string(kStoreFormatVersion);
    if (sqlite3_exec(db, stamp.c_str(), nullptr, nullptr, err) != SQLITE_OK) return 1;
    return 0;
}

}  // namespace

SqliteTickStore::~SqliteTickStore() {
    if (db_ != nullptr) sqlite3_close(db_);
}

bool SqliteTickStore::open(const std::string& path) {
    if (sqlite3_open_v2(path.c_str(), &db_,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr) != SQLITE_OK) {
        return false;
    }
    sqlite3_busy_timeout(db_, 3000);
    char* err = nullptr;
    // journal_mode 切换需要短暂独占锁且不吃 busy_timeout——多进程同瞬间首开同一新库
    // （daemon 写穿 + persister）会撞 "database is locked"，小退避重试即可
    for (int attempt = 0; attempt < 5; ++attempt) {
        if (sqlite3_exec(db_, "PRAGMA journal_mode=WAL", nullptr, nullptr, &err) == SQLITE_OK &&
            sqlite3_exec(db_, kSchema, nullptr, nullptr, &err) == SQLITE_OK) {
            const int v = check_or_stamp_format(db_, &err);
            if (v == 0) return true;
            if (v < 0) {  // 版本不匹配：永久层格式不可降级解码，响亮失败优于静默误解码
                sqlite3_close(db_);
                db_ = nullptr;
                return false;
            }
            // v > 0：瞬时失败（多进程首开并发撞 user_version 写锁）→ 走重试
        }
        std::fprintf(stderr, "sqlite open retry %d: %s\n", attempt, err ? err : "?");
        sqlite3_free(err);
        err = nullptr;
        std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
    }
    sqlite3_close(db_);
    db_ = nullptr;
    return false;
}

std::int64_t SqliteTickStore::upsert_flush(const std::vector<const std::vector<Tick>*>& batches) {
    if (db_ == nullptr) return -1;
    std::size_t total = 0;
    for (const auto* b : batches) total += b->size();
    if (total == 0) return 0;
    constexpr const char* kInsert =
        "INSERT OR IGNORE INTO ticks VALUES(?,?,?,?,?,?,?,?,?,?)";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db_, kInsert, -1, &stmt, nullptr) != SQLITE_OK) return -1;
    if (sqlite3_exec(db_, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return -1;  // busy_timeout 3s 后仍拿不到写锁
    }

    std::int64_t inserted = 0;
    for (const auto* b : batches) {
        for (const Tick& t : *b) {
            const std::string sym(tick_symbol(t));
            sqlite3_bind_text(stmt, 1, sym.data(), static_cast<int>(sym.size()), SQLITE_TRANSIENT);
            sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(t.ts_exchange_ms));
            sqlite3_bind_int64(stmt, 3, static_cast<sqlite3_int64>(t.ts_receive_ms));
            sqlite3_bind_int64(stmt, 4, t.bid_price);
            sqlite3_bind_int64(stmt, 5, t.ask_price);
            sqlite3_bind_int64(stmt, 6, t.last_price);
            sqlite3_bind_int64(stmt, 7, t.bid_size);
            sqlite3_bind_int64(stmt, 8, t.ask_size);
            sqlite3_bind_int64(stmt, 9, t.last_size);
            sqlite3_bind_int64(stmt, 10, t.flags);
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
                sqlite3_finalize(stmt);
                return -1;
            }
            inserted += sqlite3_changes(db_);
            sqlite3_reset(stmt);
            sqlite3_clear_bindings(stmt);
        }
    }
    if (sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr) != SQLITE_OK) {
        sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
        sqlite3_finalize(stmt);
        return -1;
    }
    sqlite3_finalize(stmt);
    return inserted;
}

std::int64_t SqliteTickStore::upsert_batch(const std::vector<Tick>& ticks) {
    if (ticks.empty()) return 0;
    return upsert_flush({&ticks});
}

std::vector<Tick> SqliteTickStore::query_range(const std::string& symbol, std::uint64_t from_ms,
                                               std::uint64_t to_ms) {
    query_failed_ = false;
    std::vector<Tick> out;
    if (db_ == nullptr) {
        query_failed_ = true;
        return out;
    }
    constexpr const char* kSql =
        "SELECT symbol, ts_exchange_ms, ts_receive_ms, bid_price, ask_price, last_price,"
        " bid_size, ask_size, last_size, flags FROM ticks"
        " WHERE symbol = ?1 AND ts_exchange_ms BETWEEN ?2 AND ?3 ORDER BY ts_exchange_ms";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db_, kSql, -1, &stmt, nullptr) != SQLITE_OK) {
        query_failed_ = true;
        return out;
    }
    sqlite3_bind_text(stmt, 1, symbol.data(), static_cast<int>(symbol.size()), SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(from_ms));
    sqlite3_bind_int64(stmt, 3, static_cast<sqlite3_int64>(to_ms));
    for (;;) {
        const int rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW) {
            Tick t{};
            const auto* sym = sqlite3_column_text(stmt, 0);
            if (sym != nullptr)
                std::memcpy(t.symbol, sym,
                            std::min<std::size_t>(8, static_cast<std::size_t>(
                                                         sqlite3_column_bytes(stmt, 0))));
            t.ts_exchange_ms = static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 1));
            t.ts_receive_ms = static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 2));
            t.bid_price = sqlite3_column_int64(stmt, 3);
            t.ask_price = sqlite3_column_int64(stmt, 4);
            t.last_price = sqlite3_column_int64(stmt, 5);
            t.bid_size = static_cast<std::uint32_t>(sqlite3_column_int64(stmt, 6));
            t.ask_size = static_cast<std::uint32_t>(sqlite3_column_int64(stmt, 7));
            t.last_size = static_cast<std::uint32_t>(sqlite3_column_int64(stmt, 8));
            t.flags = static_cast<std::uint8_t>(sqlite3_column_int64(stmt, 9));
            out.push_back(t);
            continue;
        }
        if (rc != SQLITE_DONE) query_failed_ = true;
        break;
    }
    sqlite3_finalize(stmt);
    return out;
}

std::uint64_t SqliteTickStore::count() const {
    if (db_ == nullptr) return 0;
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db_, "SELECT COUNT(*) FROM ticks", -1, &stmt, nullptr) != SQLITE_OK)
        return 0;
    std::uint64_t n = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) n = static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 0));
    sqlite3_finalize(stmt);
    return n;
}

}  // namespace tickshelf
