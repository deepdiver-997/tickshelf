#pragma once
// 每分片一个 SQLite 文件的存储端口（Engine 的 StorageShard 实现）。
//
// 文件布局与 ShardedTickStore 完全一致（shard_path: N=1 原路径，N>1 path.i），
// 因此 persister/queryd 继续用 ShardedTickStore 读写同一批文件。Engine 自己
// 持有每分片连接与 worker，不再套一层 ShardedTickStore 的线程/队列。
//
// 未来 clickhouse/parquet 适配器实现同一个 StorageShard 接口即可，Engine 不感知。
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "components/engine/engine.hpp"
#include "components/storage/sqlite_store.hpp"

namespace tickshelf {

class SqliteStorageShard : public StorageShard {
public:
    explicit SqliteStorageShard(std::unique_ptr<SqliteTickStore> store);
    std::int64_t upsert_flush(const std::vector<const std::vector<Tick>*>& batches) override;

private:
    std::unique_ptr<SqliteTickStore> store_;
};

// 打开 N 个分片文件；任一失败返回空 vector（调用方负责报错/退出）。
std::vector<std::unique_ptr<StorageShard>> make_sqlite_storage_shards(const std::string& path,
                                                                      std::size_t shards);

}  // namespace tickshelf
