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

if(NOT MSVC)
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(SQLite3 REQUIRED)

# 核心库（ADR-009 第一层）：common + libtickshelf 组件。
# 单元测试直连核心库，不走网络；进程嵌入者链这个库。
add_library(tickshelf_lib STATIC
  src/common/crc32.cpp
  src/common/civil.cpp
  src/common/net.cpp
  src/common/startup_config.cpp
  src/common/tick.cpp
  src/libtickshelf/evictor.cpp
  src/libtickshelf/frame_parser.cpp
  src/libtickshelf/ingestor.cpp
  src/libtickshelf/lease_persister.cpp
  src/libtickshelf/reader.cpp
  src/libtickshelf/resp_client.cpp
  src/libtickshelf/sharded_store.cpp
  src/libtickshelf/sqlite_store.cpp)
target_include_directories(tickshelf_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(tickshelf_lib PUBLIC SQLite::SQLite3)

# 守护进程（ADR-009 第二层）、租约落盘进程与参考驱动（第三层示例）
add_executable(tickshelfd src/tickshelfd/main.cpp)
target_link_libraries(tickshelfd PRIVATE tickshelf_lib)

add_executable(persister src/persister/main.cpp)
target_link_libraries(persister PRIVATE tickshelf_lib)

add_executable(evictor src/evictor/main.cpp)
target_link_libraries(evictor PRIVATE tickshelf_lib)

add_executable(capacity src/capacity/main.cpp)
target_link_libraries(capacity PRIVATE tickshelf_lib)

add_executable(queryd src/queryd/main.cpp)
target_link_libraries(queryd PRIVATE tickshelf_lib)

add_executable(feedgen src/feedgen/main.cpp)
target_link_libraries(feedgen PRIVATE tickshelf_lib)

# 灌流驱动（benchmarks.md 未测清单 #1 的工具）：极限摄入基准源，不受 1ms ts 网格限制
add_executable(floodgen src/floodgen/main.cpp)
target_link_libraries(floodgen PRIVATE tickshelf_lib)

if(TICKSHELF_BUILD_TESTS)
  enable_testing()
  add_executable(tickshelf_tests
    tests/main.cpp
    tests/test_smoke.cpp
    tests/test_startup_config.cpp
    tests/test_crc32.cpp
    tests/test_tick.cpp
    tests/test_civil.cpp
    tests/test_frame_parser.cpp
    tests/test_resp.cpp
    tests/test_sharded_store.cpp
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
endif()
