#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <sys/types.h>

namespace tickshelf::storage_io {

enum class ReadResult { complete, end_of_file, error };
enum class SyncMode { file, data, full };

struct Counts {
    std::uint64_t calls = 0;
    std::uint64_t bytes = 0;
};

// On system errors, errno contains the failing call's error. These operations
// retry EINTR and finish partial transfers. The caller owns the descriptor.
ReadResult read_at(int fd, void* data, std::size_t size, off_t offset);
// Write at the current position. Open with O_APPEND to enforce end-of-file writes.
bool append(int fd, const void* data, std::size_t size);
bool write_at(int fd, const void* data, std::size_t size, off_t offset,
              Counts* counts = nullptr);
bool sync(int fd, SyncMode mode, Counts* counts = nullptr);
bool sync_directory(const std::filesystem::path& path, SyncMode mode);

}  // namespace tickshelf::storage_io
