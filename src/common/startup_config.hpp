#pragma once
// 四个服务共用的启动配置：显式 INI 文件 + CLI；不依赖网络、数据库或第三方解析器。
#include <cstdint>
#include <string>
#include <vector>

namespace tickshelf::startup {

enum class Component { Tickshelfd, Persister, Evictor, Queryd };
enum class Action { Run, CheckConfig, PrintConfig, Help };

struct Options {
    std::string redis_host = "127.0.0.1";
    std::uint16_t redis_port = 6379;
    std::string db = "ticks.db";
    int shards = 1;
    std::string bind_host = "127.0.0.1";
    std::uint16_t port = 9900;  // queryd 的默认端口由 parse 改为 9910
    std::uint64_t flush_interval_ms = 500;
    std::string persist = "none";
    std::string worker = "persister";  // 运行时仍由 persister 追加 -PID
    std::uint64_t poll_ms = 1000;
    bool once = false;
    std::uint64_t soft_watermark_bytes = 0;  // evictor 必填，无隐式水位
    std::uint64_t emergency_watermark_bytes = 0;
    std::uint64_t sample_ms = 1000;
    std::uint64_t bytes_per_tick = 0;
    double growth_tolerance_pct = 50.0;
    bool inflight = false;
};

struct ParsedConfig {
    Options options;
    Action action = Action::Run;
};

// args 不含 argv[0]。失败抛 std::runtime_error；只读配置文件，无运行时副作用。
// CLI > 文件 > 默认值，与 --config 的位置无关；整个文件均按统一 schema 校验。
ParsedConfig parse(Component component, const std::vector<std::string>& args);
std::string to_json(Component component, const Options& options);
std::string usage(Component component);

// 薄入口适配：负责诊断/帮助/检查/JSON 输出；Run 才能开始连接或打开数据库。
enum class Status { Run, ExitSuccess, ExitFailure };
Status configure(Component component, int argc, const char* const* argv, Options& options);

}  // namespace tickshelf::startup
