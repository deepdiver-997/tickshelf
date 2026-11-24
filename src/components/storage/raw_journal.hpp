#pragma once
// 单机 raw journal（ADR-027）：StorageShard 装饰器，本体先于索引落盘。
//
// 顺序合同（唯一硬约束）：
//   追加原始帧到 raw 文件并同步 → 才调用内层 StorageShard 提交索引。
//   raw 同步成功后、索引提交前的任何崩溃，重放可以补齐（幂等 upsert）；
//   raw 同步失败则索引绝不提交，不会出现"索引成功但本体丢失"。
//
// 水位是恢复提示，不是耐久承诺：committed_records = raw 中已确认提交到索引的
// 连续前缀长度；wm 文件每 wm_interval 次提交经 临时文件+fsync+rename+目录fsync
// 原子落盘。崩溃后水位落后 → 从水位重放，重放是幂等的（存储唯一键）。
// 撕裂尾部（崩溃时未同步的半帧）在 open 时按最后完整帧边界截断。
//
// 缺口合同：raw 同步成功、索引提交失败时，水位冻结在本帧起点（连续前缀端），
// 后续批继续提交但不再推进水位——否则重开时水位会把缺口当已提交跳过，缺口帧
// 永远不进索引。重开从水位幂等重放 [wm, raw 末尾)，一次补齐缺口与后续帧。
// 追加失败合同：失败时帧字节可能已部分/全部落盘但不可信，立即截回最后完整帧
// 边界，保证后续追加仍帧对齐；截不回则毒化分片拒绝后续写。撕裂截断只处理
// 进程崩溃的产物，不承担"失败后又继续写"的修复。
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

    // upsert_flush 语义与内层一致；负值 = raw 追加或内层提交失败。
    // 追加失败：文件已截回帧边界（截不回则毒化），本批由上游按 degraded 处理。
    // 内层失败：数据只在 raw 且水位冻结在缺口起点，重开重放幂等补齐。
    std::int64_t upsert_flush(const std::vector<const std::vector<Tick>*>& batches) override;
    bool persists_records() const override { return inner_->persists_records(); }

    std::uint64_t watermark() const { return committed_records_; }  // 索引确认数（连续前缀）
    std::uint64_t raw_records() const { return raw_records_; }      // raw 文件累计数

    // 测试注入：>0 时下一次 raw 同步按失败处理（帧字节已写入但按不可信处理，
    // 走截回失败路径）。仅测试用。
    void inject_raw_sync_fault(int times) { raw_fault_times_ = times; }

private:
    void persist_watermark();
    bool scan_and_replay(std::string* error);

    const std::string raw_path_;
    const std::string wm_path_;
    const unsigned wm_interval_;
    std::unique_ptr<StorageShard> inner_;
    int raw_fd_ = -1;
    std::uint64_t raw_bytes_ = 0;        // 最后一完整帧的字节末端（追加与截回的帧对齐边界；open 时由扫描校准）
    std::uint64_t raw_records_ = 0;      // raw 文件中的累计 tick 数（追加后即推进）
    std::uint64_t committed_records_ = 0; // 已确认提交到索引的连续前缀长度（水位）
    bool index_gap_ = false;             // raw 已留痕但索引有缺口：水位冻结至重开重放补齐
    bool poisoned_ = false;              // 追加失败且截不回帧边界：拒绝后续写直至重开
    int raw_fault_times_ = 0;            // 测试注入倒计时
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
