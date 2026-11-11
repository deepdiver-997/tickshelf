#pragma once
// TTAP 帧解析状态机——本协议的安全边界（docs/protocol.md「帧内容零信任」）。
//
// 字节流可按任意切分喂入（半包/粘包）。错帧三道防线，按状态机移动顺序：
//   1. 长度交叉互证（count×64 == payload_bytes 且 ≤ 256KB）——先于一切状态机移动/扩缓冲；
//   2. CRC32 载荷校验——错帧丢弃 + 计数 + 继续（边界仍对齐）；
//   3. 失步重同步——扫描 magic，候选头必须再过一遍互证才算重同步成功（防假锚点）。
// version ≠ 1 在互证通过后判定：协议不匹配无法降级，返回 false，调用方必须断连。
//
// 实现注记：逐字节状态机，demo 速率下足够；拒绝候选头后从首个 magic 之后重扫，
// 重扫片段不足 20B，不可能在重扫内完成第二个候选头，无嵌套递归。
#include <cstdint>
#include <vector>

#include "common/crc32.hpp"
#include "common/tick.hpp"

namespace tickshelf {

// 非 tick 批的帧出口（M3：type 2 查询请求等）。CRC 通过的非 0 型帧在这里交付原载荷。
struct ControlFrame {
    std::uint8_t type;
    std::uint16_t flags;
    std::vector<std::uint8_t> payload;
};

class FrameParser {
public:
    struct Stats {
        std::uint64_t frames_ok = 0;       // 通过全部校验的帧（含心跳）
        std::uint64_t ticks_ok = 0;        // 解出的 tick 总数
        std::uint64_t heartbeats = 0;
        std::uint64_t crc_errors = 0;      // 防线 2：丢帧继续
        std::uint64_t resync_events = 0;   // 防线 3：候选头被互证拒绝的次数
        std::uint64_t resync_bytes = 0;    // 重同步扫描中丢弃的垃圾字节数
        std::uint64_t protocol_errors = 0; // version 不匹配（致命，前接断连）
    };

    // 喂入任意长度字节流；解析出的 tick 追加到 out；非 tick 型帧（type≠0/1）交付到
    // controls（可空）。返回 false = 协议级致命错误（version ≠ 1），调用方必须断连。
    bool on_bytes(const std::uint8_t* data, std::size_t n, std::vector<Tick>& out,
                  std::vector<ControlFrame>* controls = nullptr);

    const Stats& stats() const { return stats_; }

private:
    enum class State : std::uint8_t { Scan, Header, Payload };

    void feed(std::uint8_t b, std::vector<Tick>& out, std::vector<ControlFrame>* controls);
    void enter_scan();
    void finish_frame(std::vector<Tick>& out, std::vector<ControlFrame>* controls);
    void ensure_payload_buf();

    State state_ = State::Scan;
    std::uint8_t magic_win_[4] = {};  // Scan 态滑动 magic 窗口
    std::uint32_t magic_fill_ = 0;    // 窗口内新字节计数（4 后开始比对）
    std::uint32_t scan_bytes_ = 0;    // 本次扫描喂入的总字节（算垃圾字节数用）
    std::uint8_t header_buf_[kHeaderBytes];
    std::uint32_t header_pos_ = 0;
    std::vector<std::uint8_t> payload_buf_;  // 惰性分配到 kMaxFramePayloadBytes，复用
    std::uint32_t payload_left_ = 0;
    std::uint32_t payload_pos_ = 0;
    std::uint32_t expected_crc_ = 0;
    std::uint32_t crc_run_ = crc32_init();
    std::uint8_t frame_type_ = 0;
    std::uint16_t frame_flags_ = 0;
    std::uint16_t frame_count_ = 0;
    bool fatal_ = false;
    Stats stats_;
};

}  // namespace tickshelf
