#include "shard_route.hpp"

#include <string>

namespace tickshelf {

std::uint64_t symbol_hash(std::string_view symbol) {
    // 非标准 offset basis，见头注释：这是既有分片文件布局的契约。
    std::uint64_t h = 1469598103934665603ull;
    for (unsigned char byte : symbol) {
        h ^= byte;
        h *= 1099511628211ull;
    }
    return h;
}

std::size_t shard_of(std::string_view symbol, std::size_t shards) {
    if (shards <= 1) return 0;
    return static_cast<std::size_t>(symbol_hash(symbol) % static_cast<std::uint64_t>(shards));
}

std::size_t shard_of(const Tick& tick, std::size_t shards) {
    return shard_of(tick_symbol(tick), shards);
}

std::string shard_path(const std::string& path, std::size_t shard, std::size_t shards) {
    if (shards <= 1) return path;
    return path + "." + std::to_string(shard);
}

}  // namespace tickshelf
