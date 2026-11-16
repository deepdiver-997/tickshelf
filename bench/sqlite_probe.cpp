// Read the actual production-store connection, without changing its defaults.
#include "components/storage/sqlite_store.hpp"
#include <filesystem>
#include <iostream>

int main(int argc, char** argv) {
    if (argc != 2 || std::filesystem::exists(argv[1]) || !std::filesystem::is_directory(std::filesystem::path(argv[1]).parent_path())) {
        std::cerr << "usage: pipeline_sqlite_probe /existing/work/NEW.db (refuses existing database)\n";
        return 1;
    }
    tickshelf::SqliteTickStore store;
    tickshelf::SqliteConnectionInfo info;
    if (!store.open(argv[1]) || !store.connection_info(info)) return 1;
    std::cout << "{\"method\":\"actual SqliteTickStore connection after open\",\"sqlite_version\":\"" << info.version
              << "\",\"journal_mode\":\"" << info.journal_mode << "\",\"synchronous\":" << info.synchronous
              << ",\"fullfsync\":" << info.fullfsync << ",\"checkpoint_fullfsync\":" << info.checkpoint_fullfsync
              << ",\"wal_autocheckpoint\":" << info.wal_autocheckpoint << "}\n";
    return 0;
}
