#include "frame_parser.hpp"

#include <cstring>

namespace tickshelf {

bool FrameParser::on_bytes(const std::uint8_t* data, std::size_t n, std::vector<Tick>& out,
                           std::vector<ControlFrame>* controls) {
    if (fatal_) return false;
    for (std::size_t i = 0; i < n; ++i) {
        feed(data[i], out, controls);
        if (fatal_) return false;
    }
    return true;
}

void FrameParser::enter_scan() {
    state_ = State::Scan;
    std::memset(magic_win_, 0, sizeof(magic_win_));
    magic_fill_ = 0;
    scan_bytes_ = 0;
}

void FrameParser::ensure_payload_buf() {
    if (payload_buf_.size() < kMaxFramePayloadBytes) payload_buf_.resize(kMaxFramePayloadBytes);
}

void FrameParser::feed(std::uint8_t b, std::vector<Tick>& out,
                       std::vector<ControlFrame>* controls) {
    switch (state_) {
    case State::Scan: {
        magic_win_[0] = magic_win_[1];
        magic_win_[1] = magic_win_[2];
        magic_win_[2] = magic_win_[3];
        magic_win_[3] = b;
        if (magic_fill_ < 4) ++magic_fill_;
        ++scan_bytes_;
        if (magic_fill_ == 4 && magic_matches(magic_win_)) {
            stats_.resync_bytes += scan_bytes_ - 4;  // magic 本身 4 字节不算垃圾
            std::memcpy(header_buf_, magic_win_, 4);
            header_pos_ = 4;
            state_ = State::Header;
        }
        return;
    }

    case State::Header: {
        header_buf_[header_pos_++] = b;
        if (header_pos_ < kHeaderBytes) return;

        // 攒满 20B：先互证长度（防线 1），互证通过才谈得上"这是一个帧"。
        const FrameHeader h = decode_frame_header(header_buf_);
        if (!header_lengths_valid(h)) {
            ++stats_.resync_events;
            enter_scan();
            // 从首个 magic 之后重扫这段头部：载荷里碰巧出现的 magic（假锚点）
            // 会在后续互证中被再次挡下；重扫片段 < 20B，不会在循环内完成候选头。
            for (std::uint32_t i = 1; i < kHeaderBytes; ++i) feed(header_buf_[i], out, controls);
            return;
        }
        // 互证通过后才有资格判 version：≠1 协议不匹配，无法降级。
        if (h.version != kFrameVersion) {
            ++stats_.protocol_errors;
            fatal_ = true;
            return;
        }

        frame_type_ = h.type;
        frame_flags_ = h.flags;
        frame_count_ = h.count;
        expected_crc_ = h.crc32;
        crc_run_ = crc32_init();
        payload_left_ = h.payload_bytes;
        payload_pos_ = 0;
        if (payload_left_ > 0) {
            ensure_payload_buf();  // 互证已保证 ≤ 256KB，分配上限受控
            state_ = State::Payload;
        } else {
            finish_frame(out, controls);  // 心跳：0 载荷立即完成（CRC(空) == 0 与协议约定一致）
        }
        return;
    }

    case State::Payload: {
        payload_buf_[payload_pos_++] = b;
        crc_run_ = crc32_extend(crc_run_, &b, 1);
        if (payload_pos_ == payload_left_) finish_frame(out, controls);
        return;
    }
    }
}

void FrameParser::finish_frame(std::vector<Tick>& out, std::vector<ControlFrame>* controls) {
    if (crc32_finish(crc_run_) != expected_crc_) {
        ++stats_.crc_errors;  // 防线 2：长度已互证、边界仍对齐 → 丢帧继续
    } else if (frame_type_ == kFrameTypeTickBatch) {
        const std::uint8_t* p = payload_buf_.data();
        for (std::uint16_t i = 0; i < frame_count_; ++i)
            out.push_back(decode_tick(p + static_cast<std::size_t>(i) * kTickBytes));
        ++stats_.frames_ok;
        stats_.ticks_ok += frame_count_;
    } else {
        ++stats_.frames_ok;
        if (frame_type_ == kFrameTypeHeartbeat) {
            ++stats_.heartbeats;
        } else if (controls != nullptr) {  // 未知/控制型帧：原载荷交付（M3 查询请求等）
            controls->push_back(ControlFrame{
                frame_type_, frame_flags_,
                std::vector<std::uint8_t>(payload_buf_.begin(),
                                          payload_buf_.begin() + payload_left_)});
        }
    }
    enter_scan();
}

}  // namespace tickshelf
