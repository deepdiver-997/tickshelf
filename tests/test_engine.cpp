#include <doctest/doctest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "components/engine/dedupe.hpp"
#include "components/engine/engine.hpp"

using namespace tickshelf;
using namespace std::chrono_literals;

namespace {

class FakeRedis : public RedisShard {
public:
    bool publish_and_claim(const std::vector<Batch>& batches, const FlushOptions&,
                           std::vector<char>& claimed) override {
        publish_calls.fetch_add(1, std::memory_order_relaxed);
        if (block.load(std::memory_order_acquire)) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [&] { return !block.load(std::memory_order_acquire); });
        }
        const int delay = publish_delay_ms.load(std::memory_order_relaxed);
        if (delay > 0) std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        if (fail_publish.load(std::memory_order_relaxed) > 0) {
            fail_publish.fetch_sub(1, std::memory_order_relaxed);
            return false;
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            published.push_back(batches);
        }
        claimed.assign(batches.size(), 1);
        if (claim_first_only) {
            claimed.assign(batches.size(), 0);
            if (!claimed.empty()) claimed[0] = 1;
        }
        return true;
    }

    bool finish(const std::vector<std::string>& batch_ids) override {
        finish_calls.fetch_add(1, std::memory_order_relaxed);
        if (fail_finish.load(std::memory_order_relaxed) > 0) {
            fail_finish.fetch_sub(1, std::memory_order_relaxed);
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        finished.insert(finished.end(), batch_ids.begin(), batch_ids.end());
        return true;
    }

    bool release(const std::vector<std::string>& batch_ids) override {
        release_calls.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(mutex_);
        released.insert(released.end(), batch_ids.begin(), batch_ids.end());
        return true;
    }

    std::size_t published_records() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::size_t n = 0;
        for (const auto& batches : published)
            for (const auto& batch : batches) n += batch.ticks.size();
        return n;
    }

    std::size_t finished_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return finished.size();
    }

    std::size_t released_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return released.size();
    }

    void unblock() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            block.store(false, std::memory_order_release);
        }
        cv_.notify_all();
    }

    std::atomic<std::uint64_t> publish_calls{0};
    std::atomic<std::uint64_t> finish_calls{0};
    std::atomic<std::uint64_t> release_calls{0};
    std::atomic<int> fail_publish{0};
    std::atomic<int> fail_finish{0};
    std::atomic<int> publish_delay_ms{0};
    std::atomic<bool> block{false};
    bool claim_first_only = false;

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::vector<Batch>> published;
    std::vector<std::string> finished;
    std::vector<std::string> released;
};

class FakeStorage : public StorageShard {
public:
    std::int64_t upsert_flush(const std::vector<const std::vector<Tick>*>& batches) override {
        calls.fetch_add(1, std::memory_order_relaxed);
        if (fail_times.load(std::memory_order_relaxed) > 0) {
            fail_times.fetch_sub(1, std::memory_order_relaxed);
            return -1;
        }
        std::vector<Tick> flat;
        for (const auto* batch : batches) flat.insert(flat.end(), batch->begin(), batch->end());
        const auto n = flat.size();
        std::lock_guard<std::mutex> lock(mutex_);
        stored.push_back(std::move(flat));
        return static_cast<std::int64_t>(n);
    }

    std::size_t stored_records() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::size_t n = 0;
        for (const auto& batch : stored) n += batch.size();
        return n;
    }

    std::atomic<std::uint64_t> calls{0};
    std::atomic<int> fail_times{0};

private:
    mutable std::mutex mutex_;
    std::vector<std::vector<Tick>> stored;
};

Tick make_tick(const char* symbol, std::uint64_t ts) {
    Tick tick{};
    const std::size_t n = std::strlen(symbol);
    std::memcpy(tick.symbol, symbol, n < sizeof(tick.symbol) ? n : sizeof(tick.symbol));
    tick.ts_exchange_ms = ts;
    tick.ts_receive_ms = ts + 1;
    tick.bid_price = 10005000;
    return tick;
}

EngineConfig config_for(WriteMode mode) {
    EngineConfig config;
    config.shards = 1;
    config.write_mode = mode;
    config.batch_records = 2;
    config.batch_bytes = 0;
    config.batch_ms = 10;
    config.inflight = 4;
    config.max_pending_records = 100;
    config.retry_ms = 1;
    config.max_retry_ms = 2;
    config.finish_attempts = 2;
    config.shutdown_ms = 2000;
    return config;
}

struct Harness {
    std::unique_ptr<Engine> engine;
    FakeRedis* redis = nullptr;
    FakeStorage* storage = nullptr;
};

Harness make_harness(EngineConfig config) {
    auto redis = std::make_unique<FakeRedis>();
    auto storage = std::make_unique<FakeStorage>();
    Harness harness;
    harness.redis = redis.get();
    harness.storage = storage.get();
    std::vector<std::unique_ptr<RedisShard>> redis_shards;
    redis_shards.push_back(std::move(redis));
    std::vector<std::unique_ptr<StorageShard>> storage_shards;
    if (config.write_mode == WriteMode::WriteThrough)
        storage_shards.push_back(std::move(storage));
    harness.engine = std::make_unique<Engine>(std::move(config), std::move(redis_shards),
                                              std::move(storage_shards));
    return harness;
}

}  // namespace

TEST_CASE("Engine: 仅发布模式封口/发布/完成") {
    auto harness = make_harness(config_for(WriteMode::PublishOnly));
    std::vector<Tick> ticks = {make_tick("A", 1), make_tick("B", 2), make_tick("C", 3)};
    CHECK(harness.engine->submit(ticks, 0) == 3);
    harness.engine->tick(100);
    CHECK(harness.engine->wait_idle(2000ms));

    const auto stats = harness.engine->stats();
    CHECK(stats.accepted_records == 3);
    CHECK(stats.publish_only_records == 3);
    CHECK(stats.durable_records == 0);
    CHECK(stats.sealed_slices == 2);
    CHECK(stats.pending_records == 0);
    CHECK(harness.redis->published_records() == 3);

    const auto report = harness.engine->drain(2000ms);
    CHECK(report.complete);
    CHECK(report.publish_only_records == 3);
}

TEST_CASE("Engine: 写穿模式认领/落库/finish 精确集合") {
    auto harness = make_harness(config_for(WriteMode::WriteThrough));
    std::vector<Tick> ticks = {make_tick("600000", 1000), make_tick("600000", 1001),
                               make_tick("600000", 1002)};
    CHECK(harness.engine->submit(ticks, 0) == 3);
    harness.engine->tick(100);
    REQUIRE(harness.engine->wait_idle(2000ms));

    const auto stats = harness.engine->stats();
    CHECK(stats.durable_records == 3);
    CHECK(stats.publish_only_records == 0);
    CHECK(stats.skipped_records == 0);
    CHECK(stats.degraded_records == 0);
    CHECK(stats.sealed_slices == 2);
    CHECK(harness.storage->calls.load() == 2);
    CHECK(harness.storage->stored_records() == 3);
    CHECK(harness.redis->finished_count() == 2);
    CHECK(harness.redis->released_count() == 0);

    const auto report = harness.engine->drain(2000ms);
    CHECK(report.complete);
    CHECK(report.durable_records == 3);
}

TEST_CASE("Engine: 部分认领只落库 claimed 批并计 skipped") {
    auto harness = make_harness(config_for(WriteMode::WriteThrough));
    harness.redis->claim_first_only = true;
    std::vector<Tick> ticks = {make_tick("A", 1), make_tick("B", 2)};
    CHECK(harness.engine->submit(ticks, 0) == 2);
    harness.engine->tick(100);
    REQUIRE(harness.engine->wait_idle(2000ms));

    const auto stats = harness.engine->stats();
    CHECK(stats.durable_records == 1);
    CHECK(stats.skipped_records == 1);
    CHECK(stats.degraded_records == 0);
    CHECK(harness.storage->stored_records() == 1);
    CHECK(harness.redis->finished_count() == 1);
    CHECK(harness.redis->released_count() == 0);
    CHECK(harness.engine->drain(2000ms).complete);
}

TEST_CASE("Engine: 存储失败释放租约并降级") {
    auto harness = make_harness(config_for(WriteMode::WriteThrough));
    harness.storage->fail_times.store(1);
    std::vector<Tick> ticks = {make_tick("600000", 1000), make_tick("600000", 1001)};
    CHECK(harness.engine->submit(ticks, 0) == 2);
    harness.engine->tick(100);
    REQUIRE(harness.engine->wait_idle(2000ms));

    const auto stats = harness.engine->stats();
    CHECK(stats.durable_records == 0);
    CHECK(stats.degraded_records == 2);
    CHECK(stats.skipped_records == 0);
    CHECK(harness.storage->calls.load() == 1);
    CHECK(harness.redis->release_calls.load() == 1);
    CHECK(harness.redis->released_count() == 1);
    CHECK(harness.redis->finished_count() == 0);
    CHECK(harness.engine->drain(2000ms).complete);
}

TEST_CASE("Engine: finish 失败计数 finish_pending 但不丢耐久") {
    auto harness = make_harness(config_for(WriteMode::WriteThrough));
    harness.redis->fail_finish.store(2);
    std::vector<Tick> ticks = {make_tick("600000", 1000), make_tick("600000", 1001)};
    CHECK(harness.engine->submit(ticks, 0) == 2);
    harness.engine->tick(100);
    REQUIRE(harness.engine->wait_idle(2000ms));

    const auto stats = harness.engine->stats();
    CHECK(stats.durable_records == 2);
    CHECK(stats.finish_pending_records == 2);
    CHECK(harness.redis->finish_calls.load() == 2);
    CHECK(harness.storage->stored_records() == 2);
    CHECK(harness.engine->drain(2000ms).complete);
}

TEST_CASE("Engine: 发布失败按退避重试") {
    auto harness = make_harness(config_for(WriteMode::PublishOnly));
    harness.redis->fail_publish.store(1);
    std::vector<Tick> ticks = {make_tick("600000", 1000), make_tick("600000", 1001)};
    CHECK(harness.engine->submit(ticks, 0) == 2);
    harness.engine->tick(100);
    REQUIRE(harness.engine->wait_idle(2000ms));

    const auto stats = harness.engine->stats();
    CHECK(stats.retries == 1);
    CHECK(stats.publish_only_records == 2);
    CHECK(harness.redis->publish_calls.load() == 2);
    CHECK(harness.engine->drain(2000ms).complete);
}

TEST_CASE("Engine: 队列满时拒绝并背压，恢复后可继续接收") {
    auto config = config_for(WriteMode::PublishOnly);
    config.batch_records = 1;
    config.inflight = 1;
    config.max_pending_records = 1;
    auto harness = make_harness(config);
    harness.redis->block.store(true);

    std::vector<Tick> first = {make_tick("A", 1)};
    CHECK(harness.engine->submit(first, 0) == 1);
    std::vector<Tick> second = {make_tick("B", 2)};
    CHECK(harness.engine->submit(second, 0) == 0);
    CHECK(harness.engine->stats().rejected_records == 1);

    harness.redis->unblock();
    REQUIRE(harness.engine->wait_idle(2000ms));
    CHECK(harness.engine->submit(second, 0) == 1);
    harness.engine->tick(100);
    REQUIRE(harness.engine->wait_idle(2000ms));

    const auto stats = harness.engine->stats();
    CHECK(stats.accepted_records == 2);
    CHECK(stats.rejected_records == 1);
    CHECK(stats.publish_only_records == 2);
    CHECK(harness.engine->drain(2000ms).complete);
}

TEST_CASE("Engine: 按年龄封口（低于批量上限）") {
    auto config = config_for(WriteMode::PublishOnly);
    config.batch_records = 100;
    config.batch_ms = 10;
    auto harness = make_harness(config);

    std::vector<Tick> ticks = {make_tick("A", 1)};
    CHECK(harness.engine->submit(ticks, 0) == 1);
    CHECK(harness.engine->stats().sealed_slices == 0);
    harness.engine->tick(9);
    CHECK(harness.engine->stats().sealed_slices == 0);
    harness.engine->tick(10);
    CHECK(harness.engine->stats().sealed_slices == 1);
    REQUIRE(harness.engine->wait_idle(2000ms));
    CHECK(harness.engine->stats().publish_only_records == 1);
    CHECK(harness.engine->drain(2000ms).complete);
}

TEST_CASE("Engine: 去重开启时重复不进 staging，默认关闭") {
    auto enabled = config_for(WriteMode::PublishOnly);
    enabled.batch_records = 100;
    enabled.dedup_bytes = 4 * DedupeCache::entry_size();
    auto harness = make_harness(enabled);
    const Tick a = make_tick("A", 1);
    std::vector<Tick> ticks = {a, a};
    CHECK(harness.engine->submit(ticks, 0) == 2);
    CHECK(harness.engine->stats().dedup_dropped == 1);
    harness.engine->tick(10);
    REQUIRE(harness.engine->wait_idle(2000ms));
    CHECK(harness.engine->stats().publish_only_records == 1);
    CHECK(harness.redis->published_records() == 1);
    CHECK(harness.engine->drain(2000ms).complete);

    auto disabled = config_for(WriteMode::PublishOnly);
    disabled.batch_records = 100;
    auto harness2 = make_harness(disabled);
    CHECK(harness2.engine->submit(ticks, 0) == 2);
    CHECK(harness2.engine->stats().dedup_dropped == 0);
    harness2.engine->tick(10);
    REQUIRE(harness2.engine->wait_idle(2000ms));
    CHECK(harness2.engine->stats().publish_only_records == 2);
    CHECK(harness2.engine->drain(2000ms).complete);
}

TEST_CASE("Engine: drain 超时报告未完成而不是谎称耐久") {
    auto config = config_for(WriteMode::PublishOnly);
    config.batch_records = 1;
    config.inflight = 1;
    auto harness = make_harness(config);
    harness.redis->publish_delay_ms.store(50);
    harness.redis->fail_publish.store(1);

    std::vector<Tick> ticks = {make_tick("A", 1)};
    CHECK(harness.engine->submit(ticks, 0) == 1);
    const auto report = harness.engine->drain(5ms);
    CHECK_FALSE(report.complete);
    CHECK(report.pending_records == 1);
    CHECK(report.publish_only_records == 0);
}

TEST_CASE("Engine: 两分片各自路由与独立 worker") {
    EngineConfig config = config_for(WriteMode::PublishOnly);
    config.shards = 2;
    config.batch_records = 1;

    auto redis0 = std::make_unique<FakeRedis>();
    auto redis1 = std::make_unique<FakeRedis>();
    FakeRedis* first = redis0.get();
    FakeRedis* second = redis1.get();
    std::vector<std::unique_ptr<RedisShard>> redis_shards;
    redis_shards.push_back(std::move(redis0));
    redis_shards.push_back(std::move(redis1));
    std::vector<std::unique_ptr<StorageShard>> storage_shards;
    Engine engine(std::move(config), std::move(redis_shards), std::move(storage_shards));

    std::vector<Tick> ticks = {make_tick("600000", 1), make_tick("000001", 2)};
    CHECK(engine.submit(ticks, 0) == 2);
    engine.tick(100);
    REQUIRE(engine.wait_idle(2000ms));

    CHECK(first->publish_calls.load() == 1);
    CHECK(second->publish_calls.load() == 1);
    CHECK(first->published_records() == 1);
    CHECK(second->published_records() == 1);
    CHECK(engine.drain(2000ms).complete);
}
