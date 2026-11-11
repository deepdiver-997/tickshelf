#!/usr/bin/env bash
# 组件极限基准（docs/benchmarks.md 的复现脚本）：各组件隔离测天花板，
# 与演练/集成测试互补——组件账回答"余量倍数"，集成账回答"哪个环节拖累"。
#
#   Redis   ：临时实例（端口 6399，maxmemory 固定）上 redis-benchmark，
#             tick 大小（64B）与段大小（16KB ≈ 256 tick/段）两档，pipeline 1/16 两档
#   SQLite  ：capacity 内置微基准——写（rows/s）+ --read-bench（queries/s、rows/s）
#
# 环境注记：本机 loopback、容器/跨机结果不可比；数字是"当下配置的相对值"。
set -euo pipefail
cd "$(dirname "$0")/.."

# 基准必须 Release 构建（Debug 数字虚高数倍，进不了账本）；BIN_DIR 可覆盖
BIN=${BIN:-build-release}
[ -x "$BIN/capacity" ] || BIN=build

PORT=6399
MEM=512mb
DATA=$(mktemp -d)
REDIS_CLI=${REDIS_CLI:-redis-cli}
BENCH=${REDIS_BENCH:-redis-benchmark}

cleanup() {
  "$REDIS_CLI" -p "$PORT" shutdown nosave >/dev/null 2>&1 || true
  rm -rf "$DATA"
}
trap cleanup EXIT

echo "== Redis 组件基准（临时实例 :$PORT，maxmemory=$MEM，无持久化）=="
redis-server --port "$PORT" --save '' --appendonly no \
  --maxmemory "$MEM" --maxmemory-policy noeviction --dir "$DATA" >/dev/null 2>&1 &
for i in $(seq 1 50); do
  "$REDIS_CLI" -p "$PORT" ping >/dev/null 2>&1 && break
  sleep 0.1
done
"$REDIS_CLI" -p "$PORT" ping >/dev/null

bench() { # bench <名> <redis-benchmark 参数...>
  local name="$1"; shift
  echo "--- $name"
  "$BENCH" -p "$PORT" -q --threads 3 "$@" || true
}

# tick 粒度（64B = 1 tick/命令）与段粒度（16KB ≈ 256 tick/命令，v2 常态 500ms×500/s/symbol）
bench "SET tick(64B)  pipeline=1 " -t SET -d 64    -n 200000 -P 1 -c 1
bench "SET tick(64B)  pipeline=16" -t SET -d 64    -n 200000 -P 16 -c 1
bench "SET seg(16KB)  pipeline=1 " -t SET -d 16384 -n 20000  -P 1 -c 1
bench "SET seg(16KB)  pipeline=16" -t SET -d 16384 -n 20000  -P 16 -c 1
bench "GET seg(16KB)  pipeline=1 " -t GET -d 16384 -n 20000  -P 1 -c 1
bench "GET seg(16KB)  pipeline=16" -t GET -d 16384 -n 20000  -P 16 -c 1
"$REDIS_CLI" -p "$PORT" info memory | grep -E "used_memory_human|maxmemory_human" || true

echo
echo "== SQLite 组件基准（capacity 微基准）=="
"$BIN/capacity" --read-bench 2>&1 | grep -vE "^  \[|容量账|结论|校准|建议" || true
echo "（写 = 微基准 persist_rate 行；读 = SQLite 读微基准行；容量不等式输出被过滤，只留微基准）"
