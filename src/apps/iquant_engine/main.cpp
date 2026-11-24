#include <sqlite3.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace {

constexpr std::size_t kMaxPayload = 4 * 1024 * 1024;
constexpr int kApplicationId = 0x54534951; // TSIQ

// The child can outlive a force-killed receiver until its pipe closes.
class WorkerLock {
public:
    explicit WorkerLock(const std::filesystem::path& path) {
#ifdef _WIN32
        handle_ = CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr,
                              OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (handle_ == INVALID_HANDLE_VALUE) throw std::runtime_error("cannot lock iQuant worker");
#else
        fd_ = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
        if (fd_ < 0) throw std::runtime_error("cannot open iQuant worker lock");
        if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
            ::close(fd_);
            throw std::runtime_error("cannot lock iQuant worker");
        }
#endif
    }
    ~WorkerLock() {
#ifdef _WIN32
        CloseHandle(handle_);
#else
        ::close(fd_);
#endif
    }
    WorkerLock(const WorkerLock&) = delete;
    WorkerLock& operator=(const WorkerLock&) = delete;
private:
#ifdef _WIN32
    HANDLE handle_;
#else
    int fd_;
#endif
};

class Database {
public:
    explicit Database(const char* path) {
        if (sqlite3_open(path, &db_) != SQLITE_OK) {
            const std::string error = sqlite3_errmsg(db_);
            sqlite3_close(db_);
            throw std::runtime_error(error);
        }
        sqlite3_busy_timeout(db_, 3000);
    }
    ~Database() { sqlite3_close(db_); }
    sqlite3* get() const { return db_; }
    void exec(const char* sql) {
        if (sqlite3_exec(db_, sql, nullptr, nullptr, nullptr) != SQLITE_OK)
            throw std::runtime_error(sqlite3_errmsg(db_));
    }
private:
    sqlite3* db_ = nullptr;
};

class Statement {
public:
    Statement(Database& db, const char* sql) : db_(db.get()) {
        check(sqlite3_prepare_v2(db_, sql, -1, &stmt_, nullptr));
    }
    ~Statement() { sqlite3_finalize(stmt_); }
    void reset() {
        check(sqlite3_reset(stmt_));
        check(sqlite3_clear_bindings(stmt_));
    }
    void integer(int i, std::int64_t value) { check(sqlite3_bind_int64(stmt_, i, value)); }
    void text(int i, const std::string& value) {
        check(sqlite3_bind_text(stmt_, i, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT));
    }
    void blob(int i, const std::string& value) {
        check(sqlite3_bind_blob(stmt_, i, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT));
    }
    void null(int i) { check(sqlite3_bind_null(stmt_, i)); }
    bool row() {
        const int rc = sqlite3_step(stmt_);
        if (rc == SQLITE_ROW) return true;
        if (rc == SQLITE_DONE) return false;
        check(rc);
        return false;
    }
    std::int64_t number(int i) const { return sqlite3_column_int64(stmt_, i); }
    std::string string(int i) const {
        const auto* data = sqlite3_column_text(stmt_, i);
        return data ? reinterpret_cast<const char*>(data) : "";
    }
private:
    void check(int rc) const {
        if (rc != SQLITE_OK) throw std::runtime_error(sqlite3_errmsg(db_));
    }
    sqlite3* db_;
    sqlite3_stmt* stmt_ = nullptr;
};

void require(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}

std::int64_t scalar(Database& db, const char* sql) {
    Statement stmt(db, sql);
    require(stmt.row(), "missing SQLite scalar");
    return stmt.number(0);
}

void initialize(Database& db) {
    const auto app = scalar(db, "PRAGMA application_id");
    const auto ver = scalar(db, "PRAGMA user_version");
    const bool fresh = app == 0 && ver == 0 &&
        scalar(db, "SELECT count(*) FROM sqlite_master WHERE substr(name,1,7) <> 'sqlite_'") == 0;
    require(fresh || (app == kApplicationId && ver == 1), "unknown iQuant database schema");
    {
        Statement wal(db, "PRAGMA journal_mode=WAL");
        require(wal.row() && wal.string(0) == "wal", "cannot enable iQuant WAL");
    }
    db.exec("PRAGMA synchronous=FULL");
#ifdef __APPLE__
    db.exec("PRAGMA fullfsync=ON");
#endif
    db.exec("PRAGMA foreign_keys=ON");
    if (fresh) db.exec(R"SQL(
        BEGIN IMMEDIATE;
        CREATE TABLE record_schema(name TEXT NOT NULL, version INTEGER NOT NULL,
                                   PRIMARY KEY(name, version)) WITHOUT ROWID;
        INSERT INTO record_schema VALUES('iqts-snapshot', 1);
        CREATE TABLE raw_frames(
            stream TEXT NOT NULL, first_seq TEXT NOT NULL, last_seq TEXT NOT NULL,
            byte_start INTEGER NOT NULL, byte_end INTEGER NOT NULL, crc32 INTEGER NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('data','gap','quarantine')),
            record_count INTEGER NOT NULL, detail TEXT,
            PRIMARY KEY(stream, first_seq), UNIQUE(stream, byte_start)
        ) WITHOUT ROWID;
        CREATE TABLE quotes(
            stream TEXT NOT NULL, batch_seq TEXT NOT NULL, ordinal INTEGER NOT NULL,
            symbol TEXT NOT NULL, source_time_ms INTEGER, snapshot BLOB NOT NULL,
            PRIMARY KEY(stream, batch_seq, ordinal),
            FOREIGN KEY(stream, batch_seq) REFERENCES raw_frames(stream, first_seq)
        ) WITHOUT ROWID;
        CREATE INDEX quotes_symbol_time ON quotes(symbol,source_time_ms);
        CREATE TABLE replay_cursor(stream TEXT PRIMARY KEY, raw_end INTEGER NOT NULL,
                                   last_seq TEXT NOT NULL) WITHOUT ROWID;
        PRAGMA application_id=1414744401;
        PRAGMA user_version=1;
        COMMIT;
    )SQL");
    require(scalar(db, "SELECT count(*) FROM record_schema WHERE name='iqts-snapshot' AND version=1") == 1,
            "missing iQuant snapshot schema");
}

std::string sequence_key(std::uint64_t value) {
    const auto digits = std::to_string(value);
    return std::string(20 - digits.size(), '0') + digits;
}

bool valid_stream(const std::string& stream) {
    return stream.size() == 32 && std::all_of(stream.begin(), stream.end(), [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    });
}

template<class... T>
void fields(std::istringstream& line, T&... values) {
    require(static_cast<bool>((line >> ... >> values)), "invalid iQuant engine request");
    std::string trailing;
    require(!(line >> trailing), "extra iQuant engine request fields");
}

std::istringstream input_line() {
    std::string line;
    require(static_cast<bool>(std::getline(std::cin, line)), "truncated iQuant engine request");
    return std::istringstream(line);
}

std::string input_bytes(std::size_t size) {
    require(size <= kMaxPayload, "iQuant engine field exceeds payload limit");
    std::string bytes(size, '\0');
    require(static_cast<bool>(std::cin.read(bytes.data(), static_cast<std::streamsize>(size))),
            "truncated iQuant engine payload");
    return bytes;
}

class Store {
public:
    explicit Store(Database& db)
        : db_(db),
          cursor_(db, "SELECT raw_end,last_seq FROM replay_cursor WHERE stream=?1"),
          advance_(db, "INSERT INTO replay_cursor VALUES(?1,?2,?3) ON CONFLICT(stream) "
                       "DO UPDATE SET raw_end=excluded.raw_end,last_seq=excluded.last_seq"),
          frame_(db, "INSERT INTO raw_frames VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9) "
                     "ON CONFLICT(stream, first_seq) DO NOTHING"),
          existing_(db, "SELECT last_seq,byte_start,byte_end,crc32,status,record_count "
                        "FROM raw_frames WHERE stream=?1 AND first_seq=?2"),
          quote_(db, "INSERT INTO quotes VALUES(?1,?2,?3,?4,?5,?6)") {}

    std::pair<std::int64_t, std::string> cursor(const std::string& stream) {
        cursor_.text(1, stream);
        auto result = std::make_pair(std::int64_t{0}, sequence_key(0));
        if (cursor_.row()) result = {cursor_.number(0), cursor_.string(1)};
        cursor_.reset();
        return result;
    }

    void write(const std::string& stream, int sweep, int frames) {
        require((sweep == 0 || sweep == 1) && frames > 0 && frames <= 32, "invalid iQuant write group");
        db_.exec("BEGIN IMMEDIATE");
        auto [offset, seq] = cursor(stream);
        bool check_continuity = sweep != 0;
        while (frames--) {
            auto line = input_line();
            std::uint64_t first, last;
            std::int64_t begin, end, crc;
            std::string status;
            std::size_t rows, detail_size;
            fields(line, first, last, begin, end, crc, status, rows, detail_size);
            require(first > 0 && last >= first && begin >= 0 && end > begin &&
                        end - begin <= static_cast<std::int64_t>(kMaxPayload + 32) &&
                        crc >= 0 && crc <= 0xffffffff &&
                        detail_size <= 1024 && rows <= kMaxPayload / 7 &&
                        ((status == "data" && first == last && detail_size == 0) ||
                         (status == "gap" && rows == 0 && detail_size == 0) ||
                         (status == "quarantine" && first == last && rows == 0 && detail_size > 0)),
                    "invalid iQuant frame metadata");
            if (check_continuity)
                require(begin == offset && sequence_key(first - 1) == seq, "iQuant replay cursor is discontinuous");
            check_continuity = true;
            const auto key = sequence_key(first);
            const auto last_key = sequence_key(last);
            frame_.text(1, stream);
            frame_.text(2, key);
            frame_.text(3, last_key);
            frame_.integer(4, begin);
            frame_.integer(5, end);
            frame_.integer(6, crc);
            frame_.text(7, status);
            frame_.integer(8, static_cast<std::int64_t>(rows));
            if (detail_size) frame_.text(9, input_bytes(detail_size));
            else frame_.null(9);
            frame_.row();
            const bool inserted = sqlite3_changes(db_.get()) == 1;
            frame_.reset();
            if (!inserted) {
                existing_.text(1, stream);
                existing_.text(2, key);
                require(existing_.row() && existing_.string(0) == last_key &&
                            existing_.number(1) == begin && existing_.number(2) == end &&
                            existing_.number(3) == crc && existing_.string(4) == status &&
                            existing_.number(5) == static_cast<std::int64_t>(rows),
                        "stored iQuant frame differs from raw");
                existing_.reset();
            }
            for (std::size_t ordinal = 0; ordinal < rows; ++ordinal) {
                auto record = input_line();
                std::size_t symbol_size, payload_size;
                std::int64_t timestamp;
                fields(record, symbol_size, timestamp, payload_size);
                require(symbol_size > 0 && payload_size > 0 && symbol_size <= kMaxPayload &&
                            payload_size <= kMaxPayload - symbol_size && timestamp >= -1,
                        "invalid iQuant record bounds");
                auto symbol = input_bytes(symbol_size);
                auto snapshot = input_bytes(payload_size);
                if (!inserted) continue;
                quote_.text(1, stream);
                quote_.text(2, key);
                quote_.integer(3, static_cast<std::int64_t>(ordinal));
                quote_.text(4, symbol);
                if (timestamp < 0) quote_.null(5);
                else quote_.integer(5, timestamp);
                quote_.blob(6, snapshot);
                quote_.row();
                quote_.reset();
            }
            offset = end;
            seq = last_key;
        }
        if (sweep) {
            advance_.text(1, stream);
            advance_.integer(2, offset);
            advance_.text(3, seq);
            advance_.row();
            advance_.reset();
        }
        db_.exec("COMMIT");
    }
private:
    Database& db_;
    Statement cursor_, advance_, frame_, existing_, quote_;
};

} // namespace

int main(int argc, char** argv) {
    if (argc != 3 || std::string_view(argv[1]) != "--worker") {
        std::cerr << "usage: iquant_engine --worker WORKSPACE\n";
        return 2;
    }
#ifdef _WIN32
    _setmode(_fileno(stdin), _O_BINARY);
    _setmode(_fileno(stdout), _O_BINARY);
#endif
    std::cin.tie(nullptr);
    try {
        const auto root = std::filesystem::path(argv[2]);
        WorkerLock lock(root / ".engine.lock");
        const auto path = (root / "history.sqlite3").u8string();
        Database db(reinterpret_cast<const char*>(path.c_str()));
        initialize(db);
        Statement streams(db, "SELECT DISTINCT stream FROM raw_frames UNION SELECT stream FROM replay_cursor");
        Statement history_end(db, "SELECT byte_end FROM raw_frames WHERE stream=?1 ORDER BY first_seq DESC LIMIT 1");
        while (streams.row()) {
            const auto id = streams.string(0);
            const auto raw = root / "raw" / (id + ".raw");
            require(valid_stream(id) && std::filesystem::is_regular_file(raw),
                    "iQuant history references a missing raw stream");
            history_end.text(1, id);
            require(history_end.row() && history_end.number(0) > 0 &&
                        static_cast<std::uint64_t>(history_end.number(0)) <= std::filesystem::file_size(raw),
                    "iQuant history is ahead of the raw stream");
            history_end.reset();
        }
        Store store(db);
        std::cout << "READY\n" << std::flush;
        std::string request;
        while (std::getline(std::cin, request)) {
            std::istringstream line(request);
            std::string operation, stream;
            require(static_cast<bool>(line >> operation >> stream) && valid_stream(stream),
                    "invalid iQuant engine stream");
            if (operation == "P") {
                auto [offset, sequence] = store.cursor(stream);
                std::cout << offset << ' ' << sequence << '\n' << std::flush;
            } else if (operation == "W") {
                int sweep, frames;
                fields(line, sweep, frames);
                store.write(stream, sweep, frames);
                std::cout << "OK\n" << std::flush;
            } else {
                throw std::runtime_error("unknown iQuant engine request");
            }
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "iquant_engine: " << error.what() << '\n';
        return 1;
    }
}
