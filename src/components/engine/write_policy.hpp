#pragma once
// 写穿策略（v1 静态）：--persist sqlite = 写穿，storage = 直落存储，其余 = 仅发布。
// 负载自适应控制器不在 v1（ADR-013 明确信号映射反直觉，先保留策略接缝）。
//
// StorageOnly（ADR-026）：不建 Redis 连接、不发布、不租约仲裁，worker 直接把
// 分片切片交给 StorageShard。面向历史回填/单写者导入这类"只要落库"的负载；
// 多写者场景没有租约仲裁，靠 Storage 唯一键幂等兜底，不提供完成标记。
#include <string>

namespace tickshelf {

enum class WriteMode { PublishOnly, WriteThrough, StorageOnly };

inline WriteMode write_mode_from_persist(const std::string& persist) {
    if (persist == "sqlite") return WriteMode::WriteThrough;
    if (persist == "storage") return WriteMode::StorageOnly;
    return WriteMode::PublishOnly;
}

inline bool persists_final_records(WriteMode mode) {
    return mode != WriteMode::PublishOnly;
}

}  // namespace tickshelf
