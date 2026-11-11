#include "sharded_store.hpp"

#include <condition_variable>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>

#include "ingestor.hpp"  // tick_symbol

namespace tickshelf {
namespace {

// FNV-1a（规范化输入 = 去 NUL 后的实际 symbol 字节，见头注释）
std::uint64_t fnv1a(const char* bytes, std::size_t n) {
    std::uint64_t h = 1469598103934665603ull;
    for (std::size_t i = 0; i < n; ++i) {
        h ^= static_cast<unsigned char>(bytes[i]);
        h *= 1099511628211ull;
    }
    return h;
}

}  // namespace

ShardedTickStore::~ShardedTickStore() {
    for (auto& s : shards_) {
        if (!s->worker.joinable()) continue;
        {
            std::lock_guard<std::mutex> lk(s->mu);
            s->stop = true;
        }
        s->cv.notify_all();
    }
    for (auto& s : shards_)
        if (s->worker.joinable()) s->worker.join();
}

std::size_t ShardedTickStore::shard_of(const char* symbol, std::size_t len) const {
    return static_cast<std::size_t>(fnv1a(symbol, len) %
                                    static_cast<std::uint64_t>(shards_.size()));
}

void ShardedTickStore::worker_loop(Shard& s) {
    for (;;) {
        Shard::Job job;
        {
            std::unique_lock<std::mutex> lk(s.mu);
            s.cv.wait(lk, [&] { return s.stop || !s.jobs.empty(); });
            if (s.jobs.empty()) return;  // stop && 排空
            job = std::move(s.jobs.front());
            s.jobs.pop_front();
        }
        // 独占写线程：本分片文件唯一写者（SQLite 单写者约束由线程所有权保证）
        job.done.set_value(s.store.upsert_flush(job.batches));
    }
}

bool ShardedTickStore::open(const std::string& path) {
    if (shard_count_ < 1) shard_count_ = 1;
    for (int i = 0; i < shard_count_; ++i) {
        auto s = std::make_unique<Shard>();
        const std::string p = shard_count_ == 1 ? path : path + "." + std::to_string(i);
        if (!s->store.open(p)) return false;
        shards_.push_back(std::move(s));
    }
    for (auto& s : shards_) s->worker = std::thread([this, ptr = s.get()] { worker_loop(*ptr); });
    return true;
}

std::int64_t ShardedTickStore::upsert_flush(
    const std::vector<const std::vector<Tick>*>& batches) {
    if (shards_.empty()) return -1;
    std::size_t total = 0;
    for (const auto* b : batches) total += b->size();
    if (total == 0) return 0;

    // demux：每批跨多股 → 按路由拆到分片缓冲（一次 64B/tick 拷贝——分片化的固有代价）
    auto payload = std::make_shared<std::vector<std::vector<Tick>>>(shards_.size());
    for (const auto* b : batches)
        for (const Tick& t : *b)
            (*payload)[shard_of(t.symbol, strnlen(t.symbol, sizeof(t.symbol)))].push_back(t);

    // 提交 + 等全部聚合（外部语义保持同步；内部并行度 = 非空分片数）
    std::vector<std::future<std::int64_t>> futs;
    futs.reserve(shards_.size());
    for (std::size_t i = 0; i < shards_.size(); ++i) {
        if ((*payload)[i].empty()) continue;
        Shard::Job job;
        job.payload = payload;
        job.batches = {&(*payload)[i]};
        futs.push_back(job.done.get_future());
        {
            std::unique_lock<std::mutex> lk(shards_[i]->mu);
            // 有界队列：积压即背压（⑤），调用方阻塞等于把压力传给上游
            shards_[i]->cv.wait(lk, [&] { return shards_[i]->jobs.size() < 16; });
            shards_[i]->jobs.push_back(std::move(job));
        }
        shards_[i]->cv.notify_all();
    }

    std::int64_t inserted = 0;
    for (auto& f : futs) {
        const std::int64_t r = f.get();
        if (r < 0) return -1;  // 某分片故障：其余分片事务各自原子，重做由上层幂等兜底
        inserted += r;
    }
    return inserted;
}

std::int64_t ShardedTickStore::upsert_batch(const std::vector<Tick>& ticks) {
    if (ticks.empty()) return 0;
    return upsert_flush({&ticks});
}

std::vector<Tick> ShardedTickStore::query_range(const std::string& symbol, std::uint64_t from_ms,
                                                std::uint64_t to_ms) {
    query_failed_ = false;
    // 路由是纯函数：一个 symbol 只可能在一个分片——单分片查询，无需扇出归并
    const std::size_t i = shard_of(symbol.data(), symbol.size());
    auto out = shards_[i]->store.query_range(symbol, from_ms, to_ms);
    if (shards_[i]->store.query_failed()) query_failed_ = true;
    return out;
}

std::uint64_t ShardedTickStore::count() const {
    std::uint64_t n = 0;
    for (const auto& s : shards_) n += s->store.count();
    return n;
}

std::unique_ptr<TickStore> make_tick_store(int shards) {
    if (shards > 1) {
        auto s = std::make_unique<ShardedTickStore>();
        s->set_shards(shards);
        return s;
    }
    return std::make_unique<SqliteTickStore>();
}

}  // namespace tickshelf
