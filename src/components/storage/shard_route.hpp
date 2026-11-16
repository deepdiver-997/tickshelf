#pragma once
// 分片路由纯函数（ADR-020）：daemon、persister、queryd 与 engine 必须得到逐字节
// 相同的 symbol→分片结果，否则同一 symbol 会写进不同分片文件，读写分家。
//
// 历史实现使用的是一个非标准 FNV-1a 64 变体：offset basis = 1469598103934665603
// （标准值少一位，14695981039346656037）。这个常数已经是磁盘上分片文件的布局契约，
// 不能"顺手修正"；改动等于重哈希迁移（ADR-020 备案不做）。
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "common/tick.hpp"

namespace tickshelf {

std::uint64_t symbol_hash(std::string_view symbol);
std::size_t shard_of(std::string_view symbol, std::size_t shards);
std::size_t shard_of(const Tick& tick, std::size_t shards);

// N<=1：路径原样（单文件兼容）；N>1：path + "." + shard。
std::string shard_path(const std::string& path, std::size_t shard, std::size_t shards);

}  // namespace tickshelf
