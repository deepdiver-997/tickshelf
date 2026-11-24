#include "raw_journal.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <utility>

#include "common/tick.hpp"
#include "components/storage/shard_route.hpp"

namespace tickshelf {
namespace {

// 文件头：版本戳，防误把别的文件当 raw 重放。
constexpr char kRawMagic[] = "TICKSHELF-RAW-JOURNAL v1\n";
constexpr std::size_t kRawMagicBytes = sizeof(kRawMagic) - 1;
// 帧头：u32 count + u64 seq_first（LE）。上限防损坏文件把读循环撑爆。
constexpr std::uint32_t kMaxRawFrameTicks = 1u << 20;

// macOS fsync 不保证 barrier 语义；有 F_FULLFSYNC 用之（与 bench RocksDB use_fsync
// 同一思路：能力探测，不推断）。
bool durably_sync(int fd) {
#if defined(F_FULLFSYNC)
    if (::fcntl(fd, F_FULLFSYNC) == 0) return true;
    if (errno != EINVAL) return false;  // 不支持该 ioctl 的文件系统退回 fsync
#endif
    return ::fsync(fd) == 0;
}

bool sync_parent_dir(const std::string& path) {
    const std::size_t slash = path.find_last_of('/');
    const std::string dir = slash == std::string::npos ? "." : path.substr(0, slash);
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) return false;
    const bool ok = durably_sync(fd);
    ::close(fd);
    return ok;
}

bool write_all(int fd, const char* data, std::size_t size, std::string* error) {
    std::size_t done = 0;
    while (done < size) {
        const ssize_t n = ::write(fd, data + done, size - done);
        if (n > 0) {
            done += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (error) *error = std::string("raw write failed: ") + std::strerror(errno);
        return false;
    }
    return true;
}

void put_u32le(std::string& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

void put_u64le(std::string& out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

std::uint32_t get_u32le(const std::uint8_t* p) {
    return static_cast<std::uint32_t>(p[0]) | (static_cast<std::uint32_t>(p[1]) << 8) |
           (static_cast<std::uint32_t>(p[2]) << 16) | (static_cast<std::uint32_t>(p[3]) << 24);
}

std::uint64_t get_u64le(const std::uint8_t* p) {
    std::uint64_t v = 0;
    for (int i = 7; i >= 0; --i) v = (v << 8) | p[i];
    return v;
}

std::string shard_raw_path(const std::string& db_path, std::size_t index, std::size_t shards) {
    return shard_path(db_path, index, shards) + ".raw";
}

}  // namespace

RawJournalShard::RawJournalShard(std::string raw_path, std::string wm_path,
                                 std::unique_ptr<StorageShard> inner, unsigned wm_interval)
    : raw_path_(std::move(raw_path)),
      wm_path_(std::move(wm_path)),
      wm_interval_(wm_interval ? wm_interval : 1),
      inner_(std::move(inner)) {}

RawJournalShard::~RawJournalShard() {
    if (raw_fd_ >= 0) ::close(raw_fd_);
}

bool RawJournalShard::open(std::string* error) {
    // O_RDWR|O_APPEND：追加写；文件头校验用 pread 需要读权限。扫描用独立只读 fd。
    raw_fd_ = ::open(raw_path_.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
    if (raw_fd_ < 0) {
        if (error) *error = std::string("raw open failed: ") + std::strerror(errno);
        return false;
    }
    struct stat st{};
    if (::fstat(raw_fd_, &st) != 0) {
        if (error) *error = std::string("raw fstat failed: ") + std::strerror(errno);
        return false;
    }
    bool created = false;
    if (st.st_size == 0) {
        if (!write_all(raw_fd_, kRawMagic, kRawMagicBytes, error) || !durably_sync(raw_fd_)) {
            if (error && error->empty()) *error = "raw header sync failed";
            return false;
        }
        (void)sync_parent_dir(raw_path_);
        created = true;
    } else {
        char head[kRawMagicBytes];
        if (::pread(raw_fd_, head, kRawMagicBytes, 0) != static_cast<ssize_t>(kRawMagicBytes) ||
            std::memcmp(head, kRawMagic, kRawMagicBytes) != 0) {
            if (error) *error = "raw header mismatch: not a tickshelf raw journal file";
            return false;
        }
    }
    (void)created;

    // 水位：缺失/损坏一律按 0（全量重放）；它是提示，损坏不抬高、只降低。
    committed_records_ = 0;
    if (int wm_fd = ::open(wm_path_.c_str(), O_RDONLY); wm_fd >= 0) {
        char buf[32] = {};
        const ssize_t n = ::read(wm_fd, buf, sizeof(buf) - 1);
        ::close(wm_fd);
        if (n > 0) {
            buf[n] = '\0';
            const std::uint64_t parsed = std::strtoull(buf, nullptr, 10);
            committed_records_ = parsed;
        }
    }

    if (!scan_and_replay(error)) return false;
    persist_watermark();  // 重放后钉住恢复点；失败不致命（下次多重放一段）
    return true;
}

bool RawJournalShard::scan_and_replay(std::string* error) {
    const int scan_fd = ::open(raw_path_.c_str(), O_RDONLY);
    if (scan_fd < 0) {
        if (error) *error = std::string("raw scan open failed: ") + std::strerror(errno);
        return false;
    }
    struct stat st{};
    if (::fstat(scan_fd, &st) != 0) {
        ::close(scan_fd);
        if (error) *error = std::string("raw scan fstat failed: ") + std::strerror(errno);
        return false;
    }
    const off_t file_size = st.st_size;
    off_t offset = static_cast<off_t>(kRawMagicBytes);
    off_t last_good_end = offset;
    std::uint64_t last_frame_records = 0;  // 最后完整帧的 seq 末端（追加链的连续边界）
    std::vector<const std::vector<Tick>*> replay_batch;  // 单帧指针容器
    bool scan_ok = true;

    while (offset < file_size) {
        std::uint8_t header[12];
        if (::pread(scan_fd, header, sizeof(header), offset) != static_cast<ssize_t>(sizeof(header))) {
            scan_ok = false;  // 撕裂帧头
            break;
        }
        const std::uint32_t count = get_u32le(header);
        const std::uint64_t seq_first = get_u64le(header + 4);
        if (count == 0 || count > kMaxRawFrameTicks) {
            scan_ok = false;  // 结构损坏：不可安全跳过
            break;
        }
        // seq 必须与前一完整帧首尾相接：追加链只由本类维护，出现断链即结构损坏
        // （外部破坏或历史缺陷数据），截断到上一帧为止，不可安全跳过。
        if (seq_first != last_frame_records) {
            scan_ok = false;
            break;
        }
        const off_t body_bytes = static_cast<off_t>(count) * static_cast<off_t>(kTickBytes);
        if (offset + static_cast<off_t>(sizeof(header)) + body_bytes > file_size) {
            scan_ok = false;  // 撕裂帧体：崩溃时未同步完成的尾帧
            break;
        }

        // 帧整体已提交（seq_first+count <= 水位）→ 只推进扫描位置。
        if (seq_first + count <= committed_records_) {
            offset += static_cast<off_t>(sizeof(header)) + body_bytes;
            last_good_end = offset;
            last_frame_records = seq_first + count;
            continue;
        }

        std::vector<Tick> ticks;
        ticks.resize(count);
        std::vector<std::uint8_t> body(body_bytes);
        if (::pread(scan_fd, body.data(), body_bytes,
                    offset + static_cast<off_t>(sizeof(header))) !=
            static_cast<ssize_t>(body_bytes)) {
            scan_ok = false;
            break;
        }
        for (std::uint32_t i = 0; i < count; ++i)
            ticks[i] = decode_tick(body.data() + static_cast<std::size_t>(i) * kTickBytes);
        offset += static_cast<off_t>(sizeof(header)) + body_bytes;

        // 水位落在帧中间 → 只重放水位之后的尾部（重放是幂等的，宁可多重放）。
        std::uint64_t from = seq_first;
        if (from < committed_records_) from = committed_records_;
        if (from < seq_first + count) {
            const std::size_t begin = static_cast<std::size_t>(from - seq_first);
            ticks.erase(ticks.begin(), ticks.begin() + static_cast<std::ptrdiff_t>(begin));
            replay_batch.clear();
            replay_batch.push_back(&ticks);
            const std::int64_t inserted = inner_->upsert_flush(replay_batch);
            if (inserted < 0) {
                ::close(scan_fd);
                if (error) *error = "raw replay commit failed; refusing inconsistent start";
                return false;
            }
            recovery_.replayed_records += ticks.size();
            recovery_.replayed_inserted += static_cast<std::uint64_t>(inserted);
        }
        committed_records_ = seq_first + count;
        last_good_end = offset;
        last_frame_records = seq_first + count;
    }
    ::close(scan_fd);

    if (!scan_ok && last_good_end < file_size) {
        // 截断撕裂尾部，保证后续追加落在完整帧边界上。
        if (::truncate(raw_path_.c_str(), last_good_end) != 0) {
            if (error) *error = std::string("raw torn-tail truncate failed: ") + std::strerror(errno);
            return false;
        }
        recovery_.torn_tail_bytes = static_cast<std::uint64_t>(file_size - last_good_end);
    }

    // 字节偏移校准到实际文件内容：后续追加与同步失败截回都以它为帧对齐边界。
    raw_bytes_ = static_cast<std::uint64_t>(last_good_end);
    // raw_records_ 以文件实际内容为准（最后完整帧的 seq 末端），不是虚高的水位；
    // 水位若越过它（wm 文件陈旧或被外部抬高），钳回——宁可后续多重放，不可让
    // 追加从错误的 seq 起写把恢复链路写断。注意：被钳掉的水位之下若真有未提交
    // 帧，跳过决策已按虚高水位做出，属 wm 外部损坏，本类自身不产生这种水位。
    if (committed_records_ > last_frame_records) committed_records_ = last_frame_records;
    raw_records_ = last_frame_records;
    return true;
}

void RawJournalShard::persist_watermark() {
    const std::string tmp = wm_path_ + ".tmp";
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return;
    char buf[32];
    const int n = std::snprintf(buf, sizeof(buf), "%llu\n",
                                static_cast<unsigned long long>(committed_records_));
    if (!write_all(fd, buf, static_cast<std::size_t>(n), nullptr)) {
        ::close(fd);
        return;
    }
    if (!durably_sync(fd)) {
        ::close(fd);
        return;
    }
    ::close(fd);
    if (::rename(tmp.c_str(), wm_path_.c_str()) != 0) return;
    (void)sync_parent_dir(wm_path_);
}

std::int64_t RawJournalShard::upsert_flush(
    const std::vector<const std::vector<Tick>*>& batches) {
    if (raw_fd_ < 0 || poisoned_) return -1;
    std::uint64_t total = 0;
    for (const auto* batch : batches) total += batch->size();
    if (total == 0) return 0;

    // 1) 原始帧 + 同步：这是索引提交的先决条件，也是本装饰器存在的意义。
    std::string frame;
    put_u32le(frame, static_cast<std::uint32_t>(total));
    put_u64le(frame, raw_records_);
    std::vector<Tick> flat;
    flat.reserve(total);
    for (const auto* batch : batches) flat.insert(flat.end(), batch->begin(), batch->end());
    std::uint8_t encoded[kTickBytes];
    for (const Tick& t : flat) {
        encode_tick(t, encoded);
        frame.append(reinterpret_cast<const char*>(encoded), kTickBytes);
    }
    std::string append_error;
    bool append_ok = write_all(raw_fd_, frame.data(), frame.size(), &append_error);
    if (append_ok && raw_fault_times_ > 0) {
        --raw_fault_times_;  // 测试注入：帧字节已写入但按同步失败处理（不可信）
        append_ok = false;
    }
    if (append_ok) append_ok = durably_sync(raw_fd_);
    if (!append_ok) {
        // 追加/同步失败：帧字节可能已部分/全部落在文件里但不可信，索引绝不提交。
        // 立即截回最后完整帧边界，让后续追加仍落在帧对齐位置——否则下一帧会追加
        // 在半帧之后，重开时撕裂截断会把已成功提交的帧连同截掉，追加 seq 链断裂。
        // 截不回去（EIO 等）则毒化分片拒绝后续写，撕裂留给 open 扫描处理；
        // Engine 记 degraded。
        if (::ftruncate(raw_fd_, static_cast<off_t>(raw_bytes_)) != 0) poisoned_ = true;
        return -1;
    }
    raw_bytes_ += frame.size();
    const std::uint64_t seq_end = raw_records_ + total;
    raw_records_ = seq_end;

    // 2) 索引提交。失败 → 数据只在 raw，重放补齐；Engine 记 degraded。
    const std::int64_t inserted = inner_->upsert_flush(batches);
    if (inserted < 0) {
        // 缺口出现：水位冻结在本帧起点（上一个连续完成前缀端）。后续批即使提交
        // 成功也不许推进水位——水位一旦越过缺口，重开时缺口帧会被当已提交跳过，
        // 永远不进索引。重开从水位幂等重放 [wm, raw 末尾) 一次补齐缺口与后续帧。
        index_gap_ = true;
        return -1;
    }

    // 3) 水位推进（内存即时；落盘按间隔摊销）。缺口期间冻结：水位是连续前缀，
    // 冻结到重开重放补齐为止。
    if (!index_gap_) {
        committed_records_ = seq_end;
        if (++commits_since_wm_ >= wm_interval_) {
            commits_since_wm_ = 0;
            persist_watermark();
        }
    }
    return inserted;
}

std::vector<std::unique_ptr<StorageShard>> make_raw_journal_shards(
    const std::string& db_path, std::size_t shards, unsigned wm_interval,
    std::vector<std::unique_ptr<StorageShard>> inner_shards,
    std::vector<RawJournalShard::RecoveryStats>* replay) {
    if (inner_shards.size() != shards) return {};
    std::vector<std::unique_ptr<StorageShard>> out;
    out.reserve(shards);
    if (replay) replay->clear();
    for (std::size_t i = 0; i < shards; ++i) {
        auto shard = std::make_unique<RawJournalShard>(
            shard_raw_path(db_path, i, shards),
            shard_raw_path(db_path, i, shards) + ".wm",
            std::move(inner_shards[i]), wm_interval);
        std::string error;
        if (!shard->open(&error)) {
            std::fprintf(stderr, "tickshelfd: raw journal shard %zu open failed: %s\n", i,
                         error.c_str());
            return {};
        }
        if (replay) replay->push_back(shard->recovery_stats());
        out.push_back(std::move(shard));
    }
    return out;
}

}  // namespace tickshelf
