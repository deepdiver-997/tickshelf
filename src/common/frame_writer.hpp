#pragma once
// 帧打包辅助（发送侧）：Tick 批 → 完整 TTAP 帧字节流（帧头 + 载荷 + 正确 CRC）。
// 解析侧的镜像逻辑在 FrameParser（libtickshelf）。
#include <cstdint>
#include <string>
#include <vector>

#include "crc32.hpp"
#include "tick.hpp"

namespace tickshelf {

// 任意载荷的帧打包（M3 查询响应：tick 载荷 + 缺失区间块）。
// count_override：控制型帧的 count 语义由协议自定义（响应=N tick 数，缺失块不计），
// 默认按载荷/64 推导仅对纯 tick 载荷成立。
inline std::string build_raw_frame(std::uint8_t type, std::uint16_t flags,
                                   const std::string& payload,
                                   std::uint16_t count_override = 0xFFFF) {
    FrameHeader h{};
    h.version = kFrameVersion;
    h.type = type;
    h.count = count_override != 0xFFFF
                  ? count_override
                  : static_cast<std::uint16_t>(payload.size() / kTickBytes);
    h.flags = flags;
    h.payload_bytes = static_cast<std::uint32_t>(payload.size());
    h.crc32 = crc32(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
    std::string frame;
    frame.resize(kHeaderBytes + payload.size());
    encode_frame_header(h, reinterpret_cast<std::uint8_t*>(frame.data()));
    payload.copy(frame.data() + kHeaderBytes, payload.size());
    return frame;
}

inline std::string build_frame_bytes(std::uint8_t type, const std::vector<Tick>& ticks,
                                     std::uint16_t flags = 0) {
    std::string payload(ticks.size() * kTickBytes, '\0');
    for (std::size_t i = 0; i < ticks.size(); ++i)
        encode_tick(ticks[i], reinterpret_cast<std::uint8_t*>(payload.data()) + i * kTickBytes);
    return build_raw_frame(type, flags, payload);
}

}  // namespace tickshelf
