#pragma once
// 持久化切片：引擎封口后的不可变批次组，是队列/worker/重试之间的唯一载荷单位。
//
// 为什么不是 flat vector<Tick>：Redis 协议仍以 (symbol, 交易日) 为一个段/批 id，
// 所以 Slice 内部保留 std::vector<Batch>，批 id 在重试间稳定（重试不重新分组），
// 段/索引/state 语义与旧 flush 完全一致。records/bytes 只是容量与观测。
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "components/ingest/ingestor.hpp"

namespace tickshelf {

struct Slice {
    std::uint32_t shard = 0;
    std::uint64_t sequence = 0;
    std::vector<Batch> batches;
    std::uint64_t records = 0;
    std::uint64_t bytes = 0;  // 规范载荷字节（records × 64），不含 RESP/键开销
    std::uint32_t attempts = 0;
    std::vector<char> claimed;               // 每批 SETNX/GET 校验后的认领结果
    std::vector<std::string> claimed_ids;    // 本进程真正认领的批 id（精确集合）
};

using SlicePtr = std::shared_ptr<Slice>;

inline std::vector<const std::vector<Tick>*> slice_payload(const Slice& slice) {
    std::vector<const std::vector<Tick>*> out;
    out.reserve(slice.batches.size());
    for (const auto& batch : slice.batches) out.push_back(&batch.ticks);
    return out;
}

inline SlicePtr make_slice(std::uint32_t shard, std::uint64_t sequence,
                           std::vector<Batch> batches) {
    auto slice = std::make_shared<Slice>();
    slice->shard = shard;
    slice->sequence = sequence;
    slice->records = 0;
    for (const auto& batch : batches) slice->records += batch.ticks.size();
    slice->bytes = slice->records * kTickBytes;
    slice->batches = std::move(batches);
    return slice;
}

}  // namespace tickshelf
