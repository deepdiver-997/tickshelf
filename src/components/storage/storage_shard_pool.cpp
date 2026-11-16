#include "storage_shard_pool.hpp"

#include <utility>

#include "shard_route.hpp"

namespace tickshelf {

SqliteStorageShard::SqliteStorageShard(std::unique_ptr<SqliteTickStore> store)
    : store_(std::move(store)) {}

std::int64_t SqliteStorageShard::upsert_flush(
    const std::vector<const std::vector<Tick>*>& batches) {
    return store_->upsert_flush(batches);
}

std::vector<std::unique_ptr<StorageShard>> make_sqlite_storage_shards(const std::string& path,
                                                                      std::size_t shards) {
    std::vector<std::unique_ptr<StorageShard>> out;
    out.reserve(shards);
    for (std::size_t i = 0; i < shards; ++i) {
        auto store = std::make_unique<SqliteTickStore>();
        if (!store->open(shard_path(path, i, shards))) return {};
        out.push_back(std::make_unique<SqliteStorageShard>(std::move(store)));
    }
    return out;
}

}  // namespace tickshelf
