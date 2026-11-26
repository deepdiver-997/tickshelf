cmake_minimum_required(VERSION 3.24)
project(tickshelf VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

option(TICKSHELF_BUILD_TESTS "Build tickshelf tests" ON)
option(TICKSHELF_BUILD_BENCHMARKS "Build isolated JSON/Record pipeline experiments" OFF)
option(TICKSHELF_BENCH_ROCKSDB "Build pinned RocksDB benchmark/index comparison" OFF)
if(TICKSHELF_BENCH_ROCKSDB AND NOT TICKSHELF_BUILD_BENCHMARKS)
  message(FATAL_ERROR "TICKSHELF_BENCH_ROCKSDB requires TICKSHELF_BUILD_BENCHMARKS")
endif()

if(NOT MSVC)
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(SQLite3 REQUIRED)

add_executable(iquant_engine src/apps/iquant_engine/main.cpp)
if(TARGET SQLite3::SQLite3)
  target_link_libraries(iquant_engine PRIVATE SQLite3::SQLite3)
else()
  target_link_libraries(iquant_engine PRIVATE SQLite::SQLite3)
endif()
option(TICKSHELF_IQUANT_ENGINE_ONLY "Build just the portable IQTS storage worker" OFF)
if(TICKSHELF_IQUANT_ENGINE_ONLY)
  return()
endif()

# 核心库（ADR-009 第一层）：common + libtickshelf 组件。
# 单元测试直连核心库，不走网络；进程嵌入者链这个库。
add_library(tickshelf_lib STATIC
  src/common/crc32.cpp
  src/common/civil.cpp
  src/common/net.cpp
  src/common/startup_config.cpp
  src/common/tick.cpp
  src/components/evict/evictor.cpp
  src/components/ingest/frame_parser.cpp
  src/components/ingest/ingestor.cpp
  src/components/lease/lease_persister.cpp
  src/components/query/reader.cpp
  src/components/redis/resp_client.cpp
  src/components/redis/redis_shard_pool.cpp
  src/components/storage/shard_route.cpp
  src/components/storage/sharded_store.cpp
  src/components/storage/sqlite_store.cpp
  src/components/storage/raw_journal.cpp
  src/components/storage/file_io.cpp
  src/components/storage/segment_frame.cpp
  src/components/storage/storage_shard_pool.cpp
  src/components/engine/dedupe.cpp
  src/components/engine/engine.cpp)
target_include_directories(tickshelf_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
if(TARGET SQLite3::SQLite3)
  target_link_libraries(tickshelf_lib PUBLIC SQLite3::SQLite3)
else()
  target_link_libraries(tickshelf_lib PUBLIC SQLite::SQLite3)
endif()

# 守护进程（ADR-009 第二层）、租约落盘进程与参考驱动（第三层示例）
add_executable(tickshelfd src/apps/tickshelfd/main.cpp)
target_link_libraries(tickshelfd PRIVATE tickshelf_lib)

add_executable(persister src/apps/persister/main.cpp)
target_link_libraries(persister PRIVATE tickshelf_lib)

add_executable(evictor src/apps/evictor/main.cpp)
target_link_libraries(evictor PRIVATE tickshelf_lib)

add_executable(capacity src/apps/capacity/main.cpp)
target_link_libraries(capacity PRIVATE tickshelf_lib)

add_executable(queryd src/apps/queryd/main.cpp)
target_link_libraries(queryd PRIVATE tickshelf_lib)

add_executable(feedgen src/apps/feedgen/main.cpp)
target_link_libraries(feedgen PRIVATE tickshelf_lib)

# 灌流驱动（benchmarks.md 未测清单 #1 的工具）：极限摄入基准源，不受 1ms ts 网格限制
add_executable(floodgen src/apps/floodgen/main.cpp)
target_link_libraries(floodgen PRIVATE tickshelf_lib)

if(TICKSHELF_BUILD_BENCHMARKS)
  # Optional, local dependencies only. Normal service builds do not need either.
  find_package(simdjson 5 CONFIG REQUIRED)
  find_package(OpenSSL REQUIRED COMPONENTS Crypto)
  add_library(tickshelf_bench_lib STATIC
    bench/config.cpp
    bench/hash.cpp
    bench/protocol.cpp
    bench/pipeline.cpp
    bench/record.cpp
    bench/store.cpp
    bench/rocks_record_store.cpp)
  target_include_directories(tickshelf_bench_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/bench)
  target_link_libraries(tickshelf_bench_lib PUBLIC tickshelf_lib simdjson::simdjson OpenSSL::Crypto)
  add_executable(pipeline_bench bench/main.cpp)
  target_link_libraries(pipeline_bench PRIVATE tickshelf_bench_lib)
  # Diagnostic adapters are never reachable from production service CLI flags.
  add_executable(engine_ablation bench/engine_ablation.cpp)
  target_link_libraries(engine_ablation PRIVATE tickshelf_lib)
  add_executable(pipeline_sqlite_probe bench/sqlite_probe.cpp)
  target_link_libraries(pipeline_sqlite_probe PRIVATE tickshelf_lib)
  if(TICKSHELF_BENCH_ROCKSDB)
    find_package(RocksDB 11.13.2 EXACT CONFIG REQUIRED)
    target_link_libraries(tickshelf_bench_lib PUBLIC RocksDB::rocksdb)
    target_compile_definitions(tickshelf_bench_lib PUBLIC TICKSHELF_HAVE_ROCKSDB=1)
    add_executable(rocks_reference bench/rocks_reference.cpp)
    target_link_libraries(rocks_reference PRIVATE tickshelf_bench_lib RocksDB::rocksdb)
  endif()
  if(NOT MSVC)
    # Keep the measured optimized binary usable for same-run stack sampling.
    target_compile_options(tickshelf_bench_lib PRIVATE -g -fno-omit-frame-pointer)
    target_compile_options(pipeline_bench PRIVATE -g -fno-omit-frame-pointer)
  endif()
endif()

if(TICKSHELF_BUILD_TESTS)
  enable_testing()
  add_executable(tickshelf_tests
    tests/main.cpp
    tests/test_smoke.cpp
    tests/test_startup_config.cpp
    tests/test_crc32.cpp
    tests/test_tick.cpp
    tests/test_civil.cpp
    tests/test_interval_set.cpp
    tests/test_coverage_ledger.cpp
    tests/test_frame_parser.cpp
    tests/test_resp.cpp
    tests/test_sharded_store.cpp
    tests/test_shard_route.cpp
    tests/test_raw_journal.cpp
    tests/test_identity.cpp
    tests/test_bounded_queue.cpp
    tests/test_engine_dedupe.cpp
    tests/test_engine.cpp
    tests/test_ingestor.cpp
    tests/test_lease_persister.cpp
    tests/test_evictor.cpp
    tests/test_reader.cpp
    tests/test_write_through.cpp)
  target_include_directories(tickshelf_tests PRIVATE third_party)
  target_compile_definitions(tickshelf_tests PRIVATE TICKSHELF_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
  target_link_libraries(tickshelf_tests PRIVATE tickshelf_lib)
  add_test(NAME tickshelf_tests COMMAND tickshelf_tests)
  # 配置测试完全离线；不要为验证配置而运行含共享 Redis 集成测试的完整套件。
  add_test(NAME startup_config COMMAND tickshelf_tests "--test-case=startup_config:*")
  set_tests_properties(startup_config PROPERTIES LABELS "unit;startup_config")
  if(TICKSHELF_BUILD_BENCHMARKS)
    add_executable(tickshelf_bench_tests tests/main.cpp
      tests/test_bench_record.cpp tests/test_bench_store.cpp tests/test_bench_pipeline.cpp
      tests/test_bench_runtime.cpp tests/test_bench_rocks_record.cpp tests/test_bench_sqlite_lookup.cpp
      tests/test_bench_store_lookup.cpp)
    target_include_directories(tickshelf_bench_tests PRIVATE third_party)
    target_compile_definitions(tickshelf_bench_tests PRIVATE TICKSHELF_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
    target_link_libraries(tickshelf_bench_tests PRIVATE tickshelf_bench_lib)
    add_test(NAME pipeline_bench_unit COMMAND tickshelf_bench_tests)
    set_tests_properties(pipeline_bench_unit PROPERTIES LABELS "unit;benchmark")
  endif()
endif()
