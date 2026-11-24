#include <doctest/doctest.h>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/tick.hpp"
#include "components/storage/raw_journal.hpp"

using namespace tickshelf;

namespace {

struct Directory {
    std::filesystem::path path;
    Directory() {
        const auto pattern =
            (std::filesystem::temp_directory_path() / "tickshelf_raw_journal_XXXXXX").string();
        std::vector<char> name(pattern.begin(), pattern.end());
        name.push_back('\0');
        const auto* made = ::mkdtemp(name.data());
        if (!made) throw std::runtime_error("mkdtemp failed");
        path = made;
    }
    ~Directory() {
        std::error_code error;
        std::filesystem::remove_all(path, error);
    }
};

// 计数型假存储：按 64B 内容去重的集合（模拟存储唯一键幂等）。
// succeed_first > 0 时先放行 N 次成功调用，随后 fail_times > 0 的下一次提交
// 返回 -1（模拟 SQLite 故障，可精确落在第 N+1 批上）。
class SetStorage final : public StorageShard {
public:
    std::int64_t upsert_flush(const std::vector<const std::vector<Tick>*>& batches) override {
        if (succeed_first > 0) {
            --succeed_first;
        } else if (fail_times > 0) {
            --fail_times;
            return -1;
        }
        std::int64_t inserted = 0;
        for (const auto* batch : batches) {
            for (const Tick& t : *batch) {
                std::uint8_t encoded[kTickBytes];
                encode_tick(t, encoded);
                const std::string key(reinterpret_cast<const char*>(encoded), kTickBytes);
                if (rows.insert(key).second) ++inserted;
            }
        }
        ++calls;
        return inserted;
    }
    bool persists_records() const override { return persistent; }

    bool persistent = true;
    int succeed_first = 0;
    int fail_times = 0;
    std::uint64_t calls = 0;
    std::set<std::string> rows;
};

// 非拥有视图：重开时复用同一存储实例（模拟真实重启——SQLite 文件仍在原位）。
class SetStorageView final : public StorageShard {
public:
    explicit SetStorageView(SetStorage& target) : target_(target) {}
    std::int64_t upsert_flush(const std::vector<const std::vector<Tick>*>& batches) override {
        return target_.upsert_flush(batches);
    }
    bool persists_records() const override { return target_.persists_records(); }

private:
    SetStorage& target_;
};

Tick make_tick(const char* symbol, std::uint64_t ts) {
    Tick tick{};
    std::memcpy(tick.symbol, symbol, 8);
    tick.ts_exchange_ms = ts;
    tick.ts_receive_ms = ts + 1;
    tick.bid_price = 10005000;
    return tick;
}

std::vector<Tick> span_ticks(std::uint64_t from, std::uint64_t to,
                             const char* sym = "RAWSHRD1") {
    std::vector<Tick> ticks;
    for (std::uint64_t i = from; i < to; ++i) ticks.push_back(make_tick(sym, 1700000000000 + i));
    return ticks;
}

// inner 的所有权随构造移入装饰器；测试用观察指针断言内部状态。
// reuse 非空时用视图包装既有实例（重启后同一存储），否则创建新实例。
std::unique_ptr<RawJournalShard> make_shard(const std::filesystem::path& base,
                                            SetStorage** observer, SetStorage* reuse = nullptr,
                                            int fail_times = 0) {
    std::unique_ptr<StorageShard> inner;
    if (reuse) {
        inner = std::make_unique<SetStorageView>(*reuse);
        if (observer) *observer = reuse;
    } else {
        auto fresh = std::make_unique<SetStorage>();
        fresh->fail_times = fail_times;
        if (observer) *observer = fresh.get();
        inner = std::move(fresh);
    }
    auto shard = std::make_unique<RawJournalShard>(
        base.string() + ".raw", base.string() + ".raw.wm", std::move(inner), 1);
    std::string error;
    REQUIRE(shard->open(&error));
    return shard;
}

}  // namespace

TEST_CASE("RawJournal: 追加+提交推进 raw/水位，重开零重放") {
    Directory dir;
    const auto base = dir.path / "basic";
    SetStorage* rows = nullptr;
    auto shard = make_shard(base, &rows);
    const std::vector<Tick> a = span_ticks(0, 4);
    const std::vector<Tick> b = span_ticks(4, 9);
    REQUIRE(shard->upsert_flush({&a}) == 4);
    REQUIRE(shard->upsert_flush({&b}) == 5);
    CHECK(shard->raw_records() == 9);
    CHECK(shard->watermark() == 9);
    CHECK(rows->rows.size() == 9);

    // 重开（同一存储，模拟重启后 SQLite 文件仍在）：水位已钉住 → 零重放。
    SetStorage* reopened_rows = nullptr;
    auto reopened = make_shard(base, &reopened_rows, rows);
    CHECK(reopened->raw_records() == 9);
    CHECK(reopened->watermark() == 9);
    CHECK(reopened->recovery_stats().replayed_records == 0);
    CHECK(reopened_rows->rows.size() == 9);
}

TEST_CASE("RawJournal: 内层提交失败——raw 已留痕，重开重放补齐") {
    Directory dir;
    const auto base = dir.path / "fail";
    SetStorage* rows = nullptr;
    auto shard = make_shard(base, &rows, nullptr, /*fail_times=*/1);
    const std::vector<Tick> a = span_ticks(0, 4);
    CHECK(shard->upsert_flush({&a}) < 0);       // raw 已追加，索引失败
    CHECK(shard->raw_records() == 4);
    CHECK(shard->watermark() == 0);             // 水位没动
    CHECK(rows->rows.empty());

    // 重开（换新存储实例模拟 SQLite 丢失后重建的最小重放场景）：
    // 重放把 raw 里的帧补进索引。
    SetStorage* recovered_rows = nullptr;
    auto reopened = make_shard(base, &recovered_rows);
    CHECK(reopened->recovery_stats().replayed_records == 4);
    CHECK(reopened->recovery_stats().replayed_inserted == 4);
    CHECK(recovered_rows->rows.size() == 4);
    CHECK(reopened->watermark() == 4);
    // 再次重开（同一存储）：零重放、无重复。
    SetStorage* again_rows = nullptr;
    auto third = make_shard(base, &again_rows, recovered_rows);
    CHECK(third->recovery_stats().replayed_records == 0);
    CHECK(again_rows->rows.size() == 4);
    CHECK(again_rows == recovered_rows);
}

TEST_CASE("RawJournal: 索引失败后继续写——水位冻结在缺口起点，重开重放补齐") {
    Directory dir;
    const auto base = dir.path / "gap";
    SetStorage* rows = nullptr;
    auto shard = make_shard(base, &rows, nullptr, /*fail_times=*/1);
    rows->succeed_first = 1;  // 让 W 先成功，故障恰好落在 A（第二次提交）上
    const std::vector<Tick> w = span_ticks(0, 2);
    REQUIRE(shard->upsert_flush({&w}) == 2);    // 缺口前的连续前缀
    CHECK(shard->watermark() == 2);

    // 失败顺序：A 索引失败（raw 已留痕）→ B 紧接着提交成功。
    const std::vector<Tick> a = span_ticks(2, 6);
    CHECK(shard->upsert_flush({&a}) < 0);       // raw 4 条留痕，索引失败 → 缺口
    CHECK(shard->raw_records() == 6);
    CHECK(shard->watermark() == 2);             // 冻结在缺口起点（A 的 seq_first）
    const std::vector<Tick> b = span_ticks(6, 11);
    REQUIRE(shard->upsert_flush({&b}) == 5);    // 失败后继续写成功
    CHECK(shard->raw_records() == 11);
    CHECK(shard->watermark() == 2);             // 不许跳过缺口推进到 11
    CHECK(rows->rows.size() == 7);              // 索引只有 W∪B，缺 A

    // 重开：从水位幂等重放 A∪B，按内容查齐；再重开零重放无重复。
    SetStorage* recovered_rows = nullptr;
    auto reopened = make_shard(base, &recovered_rows, rows);
    CHECK(reopened->recovery_stats().replayed_records == 9);   // A 的 4 + B 的 5
    CHECK(reopened->recovery_stats().replayed_inserted == 4);  // 只 A 新增，B 幂等忽略
    CHECK(recovered_rows->rows.size() == 11);
    CHECK(reopened->watermark() == 11);
    CHECK(reopened->raw_records() == 11);

    SetStorage* again_rows = nullptr;
    auto third = make_shard(base, &again_rows, recovered_rows);
    CHECK(third->recovery_stats().replayed_records == 0);
    CHECK(again_rows->rows.size() == 11);
}

TEST_CASE("RawJournal: raw 同步失败后继续写——截回帧边界，重开扫描自洽") {
    Directory dir;
    const auto base = dir.path / "rawfail";
    SetStorage* rows = nullptr;
    auto shard = make_shard(base, &rows);
    const std::vector<Tick> a = span_ticks(0, 4);
    shard->inject_raw_sync_fault(1);
    CHECK(shard->upsert_flush({&a}) < 0);       // 帧字节已写入但按同步失败处理 → 截回
    CHECK(shard->raw_records() == 0);
    CHECK(shard->watermark() == 0);

    // 失败后继续写：必须落在截回后的帧对齐位置（而不是半帧之后）。
    const std::vector<Tick> b = span_ticks(0, 5);  // seq 仍从 0 起：失败批未占号
    REQUIRE(shard->upsert_flush({&b}) == 5);
    CHECK(shard->raw_records() == 5);
    const auto fsize = std::filesystem::file_size(base.string() + ".raw");
    CHECK(fsize == (sizeof("TICKSHELF-RAW-JOURNAL v1\n") - 1) + 12 + 5 * kTickBytes);

    // 拨回水位强制重放：验证截回后重写的帧内容完整可扫、无撕裂尾被截。
    {
        FILE* f = std::fopen((base.string() + ".raw.wm").c_str(), "w");
        REQUIRE(f != nullptr);
        std::fputs("0\n", f);
        std::fclose(f);
    }
    SetStorage* recovered_rows = nullptr;
    auto reopened = make_shard(base, &recovered_rows);
    CHECK(reopened->recovery_stats().torn_tail_bytes == 0);
    CHECK(reopened->recovery_stats().replayed_records == 5);
    CHECK(reopened->recovery_stats().replayed_inserted == 5);
    CHECK(recovered_rows->rows.size() == 5);
    CHECK(reopened->raw_records() == 5);
    CHECK(reopened->watermark() == 5);
}

TEST_CASE("RawJournal: 水位落后时重放幂等，不产生重复行") {
    Directory dir;
    const auto base = dir.path / "idem";
    SetStorage* rows = nullptr;
    auto shard = make_shard(base, &rows);
    const std::vector<Tick> a = span_ticks(0, 6);
    REQUIRE(shard->upsert_flush({&a}) == 6);
    // 手工把水位文件拨回 2：模拟"索引已提交但水位没来得及落盘"的崩溃窗口。
    {
        FILE* f = std::fopen((base.string() + ".raw.wm").c_str(), "w");
        REQUIRE(f != nullptr);
        std::fputs("2\n", f);
        std::fclose(f);
    }
    SetStorage* replayed_rows = nullptr;
    auto reopened = make_shard(base, &replayed_rows, rows);
    CHECK(reopened->recovery_stats().replayed_records == 4);  // 只重放水位之后的尾部
    CHECK(reopened->recovery_stats().replayed_inserted == 0); // 幂等：全部被唯一键忽略
    CHECK(replayed_rows->rows.size() == 6);                  // 无重复
    CHECK(reopened->watermark() == 6);
}

TEST_CASE("RawJournal: 撕裂尾部按最后完整帧截断，重放不受影响") {
    Directory dir;
    const auto base = dir.path / "torn";
    SetStorage* rows = nullptr;
    auto shard = make_shard(base, &rows);
    const std::vector<Tick> a = span_ticks(0, 4);
    REQUIRE(shard->upsert_flush({&a}) == 4);

    // 模拟崩溃时写了一半的尾帧：合法帧头（count=2）+ 只有 40B 的不完整帧体。
    {
        FILE* f = std::fopen((base.string() + ".raw").c_str(), "ab");
        REQUIRE(f != nullptr);
        for (int i = 0; i < 4; ++i) std::fputc(0x02 >> (8 * i), f);
        for (int i = 0; i < 8; ++i) std::fputc(0x00, f);   // seq_first=0
        for (int i = 0; i < 40; ++i) std::fputc(0xAA, f);  // 40B/128B 帧体
        std::fclose(f);
    }
    SetStorage* reopened_rows = nullptr;
    auto reopened = make_shard(base, &reopened_rows, rows);
    CHECK(reopened->recovery_stats().torn_tail_bytes == 52);  // 12B 头 + 40B 体
    CHECK(reopened->raw_records() == 4);
    CHECK(reopened_rows->rows.size() == 4);
    // 截断后可继续追加且文件自洽。
    const std::vector<Tick> b = span_ticks(100, 103, "RAWSHRD2");
    REQUIRE(reopened->upsert_flush({&b}) == 3);
    CHECK(reopened->raw_records() == 7);
}

TEST_CASE("RawJournal: 外来文件头拒绝打开") {
    Directory dir;
    const auto raw = dir.path / "foreign.raw";
    {
        FILE* f = std::fopen(raw.string().c_str(), "w");
        REQUIRE(f != nullptr);
        std::fputs("not a tickshelf journal", f);
        std::fclose(f);
    }
    RawJournalShard shard(raw.string(), raw.string() + ".wm", std::make_unique<SetStorage>(), 1);
    std::string error;
    CHECK_FALSE(shard.open(&error));
    CHECK(error.find("header mismatch") != std::string::npos);
}

TEST_CASE("RawJournal: 水位文件缺失按全量重放，persists_records 透传") {
    Directory dir;
    const auto base = dir.path / "nowm";
    SetStorage* rows = nullptr;
    auto shard = make_shard(base, &rows);
    const std::vector<Tick> a = span_ticks(0, 5);
    REQUIRE(shard->upsert_flush({&a}) == 5);
    std::remove((base.string() + ".raw.wm").c_str());

    SetStorage* replayed_rows = nullptr;
    auto reopened = make_shard(base, &replayed_rows, rows);
    CHECK(reopened->recovery_stats().replayed_records == 5);
    CHECK(reopened->recovery_stats().replayed_inserted == 0);  // 同一存储：幂等忽略
    CHECK(replayed_rows->rows.size() == 5);
    CHECK(reopened->persists_records());
    replayed_rows->persistent = false;
    CHECK_FALSE(reopened->persists_records());
}
