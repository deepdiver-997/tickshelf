#pragma once
// 单机 raw journal（ADR-027）：StorageShard 装饰器，本体先于索引落盘。
//
// 顺序合同（唯一硬约束）：
//   追加原始帧到 raw 文件并同步 → 才调用内层 StorageShard 提交索引。
//   raw 同步成功后、索引提交前的任何崩溃，重放可以补齐（幂等 upsert）；
//   raw 同步失败则索引绝不提交，不会出现"索引成功但本体丢失"。
//
// 水位是恢复提示，不是耐久承诺：committed_records = raw 中已确认提交到索引的
// 累计 tick 数；wm 文件每 wm_interval 次提交经 临时文件+fsync+rename+目录fsync
// 原子落盘。崩溃后水位落后 → 从水位重放，重放是幂等的（存储唯一键）。
// 撕裂尾部（崩溃时未同步的半帧）在 open 时按最后完整帧边界截断。
//
// 面向 StorageOnly 单写者路径（--persist storage --engine --raw-journal）。
// 写穿模式以 Redis 租约为恢复权威，不叠 raw journal；多实例同库只有存储唯一键
// 幂等兜底，raw 不提供跨实例仲裁。
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "components/engine/engine.hpp"

namespace tickshelf {

class RawJournalShard : public StorageShard {
public:
    struct RecoveryStats {
        std::uint64_t replayed_records = 0;  // 从水位之后重放进索引的 tick 数
        std::uint64_t replayed_inserted = 0; // 重放实际新增行（其余为幂等忽略）
        std::uint64_t torn_tail_bytes = 0;   // open 时按最后完整帧截断的字节数
    };

    // inner 必须已打开且属于同一分片。wm_interval = 水位落盘间隔（提交次数）。
    RawJournalShard(std::string raw_path, std::string wm_path,
                    std::unique_ptr<StorageShard> inner, unsigned wm_interval = 32);
    ~RawJournalShard() override;
    RawJournalShard(const RawJournalShard&) = delete;
    RawJournalShard& operator=(const RawJournalShard&) = delete;

    // 打开/创建 raw 与水位文件，重放水位之后的帧。false = 致命错误（文件头不符、
    // 撕裂截断失败或重放提交失败），*error 带原因；调用方必须放弃启动。
    bool open(std::string* error);
    RecoveryStats recovery_stats() const { return recovery_; }

    // upsert_flush 语义与内层一致；负值 = raw 追加或内层提交失败。raw 追加成功
    // 而内层失败时，数据只存在于 raw，由下次 open 的重放补齐。
    std::int64_t upsert_flush(const std::vector<const std::vector<Tick>*>& batches) override;
    bool persists_records() const override { return inner_->persists_records(); }

    std::uint64_t watermark() const { return committed_records_; }  // 索引确认数
    std::uint64_t raw_records() const { return raw_records_; }      // raw 文件累计数

private:
    void persist_watermark();
    bool scan_and_replay(std::string* error);

    const std::string raw_path_;
    const std::string wm_path_;
    const unsigned wm_interval_;
    std::unique_ptr<StorageShard> inner_;
    int raw_fd_ = -1;
    std::uint64_t raw_records_ = 0;      // raw 文件中的累计 tick 数（追加后即推进）
    std::uint64_t committed_records_ = 0; // 已确认提交到索引的累计 tick 数（水位）
    unsigned commits_since_wm_ = 0;
    RecoveryStats recovery_;
};

// 打开 N 个分片并包装 inner_shards（顺序即 shard index）；任一分片失败返回空
// vector 并回滚已打开分片。replay 填每分片的 RecoveryStats（顺序同 index）。
std::vector<std::unique_ptr<StorageShard>> make_raw_journal_shards(
    const std::string& db_path, std::size_t shards, unsigned wm_interval,
    std::vector<std::unique_ptr<StorageShard>> inner_shards,
    std::vector<RawJournalShard::RecoveryStats>* replay);

}  // namespace tickshelf
