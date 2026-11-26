#include "file_io.hpp"

#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <unistd.h>

namespace tickshelf::storage_io {
namespace {

bool valid_range(off_t offset, std::size_t size) {
    if (offset >= 0 && size <= static_cast<std::uintmax_t>(std::numeric_limits<off_t>::max() - offset))
        return true;
    errno = EOVERFLOW;
    return false;
}

bool write_all(int fd, const void* data, std::size_t size, off_t* offset, Counts* counts) {
    const auto* buffer = static_cast<const char*>(data);
    std::size_t done = 0;
    while (done < size) {
        if (counts) ++counts->calls;
        const auto n = offset ? ::pwrite(fd, buffer + done, size - done, *offset + static_cast<off_t>(done))
                              : ::write(fd, buffer + done, size - done);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            if (n == 0) errno = EIO;
            return false;
        }
        done += static_cast<std::size_t>(n);
        if (counts) counts->bytes += static_cast<std::uint64_t>(n);
    }
    return true;
}

}  // namespace

ReadResult read_at(int fd, void* data, std::size_t size, off_t offset) {
    if (!valid_range(offset, size)) return ReadResult::error;
    auto* buffer = static_cast<char*>(data);
    std::size_t done = 0;
    while (done < size) {
        const auto n = ::pread(fd, buffer + done, size - done, offset + static_cast<off_t>(done));
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) return ReadResult::error;
        if (n == 0) return ReadResult::end_of_file;
        done += static_cast<std::size_t>(n);
    }
    return ReadResult::complete;
}

bool append(int fd, const void* data, std::size_t size) {
    return write_all(fd, data, size, nullptr, nullptr);
}

bool write_at(int fd, const void* data, std::size_t size, off_t offset, Counts* counts) {
    return valid_range(offset, size) && write_all(fd, data, size, &offset, counts);
}

bool sync(int fd, SyncMode mode, Counts* counts) {
    int rc;
#if defined(F_FULLFSYNC)
    if (mode == SyncMode::full) {
        do {
            if (counts) ++counts->calls;
            rc = ::fcntl(fd, F_FULLFSYNC);
        } while (rc < 0 && errno == EINTR);
        if (rc == 0) return true;
        if (errno != EINVAL) return false;
    }
#endif
    do {
        if (counts) ++counts->calls;
#if defined(__linux__)
        rc = mode == SyncMode::data ? ::fdatasync(fd) : ::fsync(fd);
#else
        (void)mode;
        rc = ::fsync(fd);
#endif
    } while (rc < 0 && errno == EINTR);
    return rc == 0;
}

bool sync_directory(const std::filesystem::path& path, SyncMode mode) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return false;
    const bool ok = sync(fd, mode);
    const int error = errno;
    ::close(fd);
    if (!ok) errno = error;
    return ok;
}

}  // namespace tickshelf::storage_io
