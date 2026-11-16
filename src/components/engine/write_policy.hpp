#pragma once
// 写穿策略（v1 静态）：--persist sqlite = 写穿，其余 = 仅发布。
// 负载自适应控制器不在 v1（ADR-013 明确信号映射反直觉，先保留策略接缝）。
#include <string>

namespace tickshelf {

enum class WriteMode { PublishOnly, WriteThrough };

inline WriteMode write_mode_from_persist(const std::string& persist) {
    return persist == "sqlite" ? WriteMode::WriteThrough : WriteMode::PublishOnly;
}

}  // namespace tickshelf
