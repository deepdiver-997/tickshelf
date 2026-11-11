#!/usr/bin/env bash
# A/B 基准（ADR-012 修订）：经典读回路径（--persist none）vs daemon 写穿（--persist sqlite）
# 同负载各跑一轮，对比：
#   - Redis I/O 量：INFO commandstats 前后快照差（各命令 calls/usec）
#   - 落库追平时延：feedgen 停发 → SQLite rows == SENT 的耗时
#   - 写穿 stall：daemon 内联落库阻塞主循环的时长（线程化触发条件的观测）
# 无 kill 注入：两模式都应 MISSING==0（SENT==rows）。
# 用法: scripts/bench_compare.sh [每轮秒数=20]
set -euo pipefail

DUR="${1:-20}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"
WORK="/tmp/tickshelf_bench"
PORT="${PORT:-9922}"
RATE="${RATE:-500}"
SYMBOLS="${SYMBOLS:-8}"

REDIS_CLI="${REDIS_CLI:-/opt/homebrew/bin/redis-cli}"
command -v "$REDIS_CLI" >/dev/null || REDIS_CLI=redis-cli
"$REDIS_CLI" PING >/dev/null || { echo "redis-server 不可达"; exit 1; }
for bin in tickshelfd persister feedgen; do
  [ -x "$BUILD/$bin" ] || { echo "missing $BUILD/$bin"; exit 1; }
done

mkdir -p "$WORK"

snap() { "$REDIS_CLI" INFO commandstats | grep -E '^cmdstat_' | sed 's/\r$//' |
  awk -F'[:,=]' '{print substr($1, 9), $3, $5}' | sort; }   # "cmd calls usec"

diff_stats() {  # before after → 增量行（snap 产物 3 列：cmd calls usec）
  awk 'NR==FNR { bc[$1]=$2; bu[$1]=$3; next }
       { c=$2-bc[$1]; u=$3-bu[$1]; if (c > 0) printf "  %-16s calls=%-9d usec=%d\n", $1, c, u }' "$1" "$2"
}

run_mode() {
  local mode=$1 label=$2
  local db="$WORK/bench_$mode.db"
  "$REDIS_CLI" FLUSHALL >/dev/null
  rm -f "$db" "$db-wal" "$db-shm" "$WORK/$mode.feedgen.out" "$WORK/$mode.tickshelfd.log"
  snap > "$WORK/$mode.cmd.before"

  "$BUILD/tickshelfd" --port "$PORT" --flush-interval-ms 300 --persist "$mode" --db "$db" \
    >>"$WORK/$mode.tickshelfd.log" 2>&1 & local d_shelf=$!
  sleep 0.3   # daemon/persister 同瞬间开同一新库会撞 WAL 初始化锁（open 已带重试，双保险）
  "$BUILD/persister" --db "$db" --poll-ms 100 >>"$WORK/$mode.persister.log" 2>&1 & local d_pers=$!
  sleep 0.3

  # daemon 必须活着（B 轮曾因 open 失败静默退场 → feedgen 空发）
  if ! grep -q "listening" "$WORK/$mode.tickshelfd.log" 2>/dev/null; then
    echo "[$label] daemon failed to start:"; tail -3 "$WORK/$mode.tickshelfd.log"
    kill "$d_shelf" "$d_pers" 2>/dev/null || true
    return 1
  fi

  "$BUILD/feedgen" --port "$PORT" --duration-s "$DUR" --symbols "$SYMBOLS" --rate "$RATE" \
    >"$WORK/$mode.feedgen.out" 2>/dev/null
  local t_end=$(date +%s%N)
  local sent
  sent=$(awk '/^SENT /{print $2}' "$WORK/$mode.feedgen.out" | tail -1)

  # 追平：rows == SENT（无 kill 场景两模式都必须精确追平）
  local rows=0 tries=0
  while :; do
    rows=$(sqlite3 "$db" 'SELECT COUNT(*) FROM ticks' 2>/dev/null || echo 0)
    [ "$rows" -ge "$sent" ] && break
    tries=$((tries + 1))
    [ "$tries" -gt 600 ] && break   # 120s 超时
    sleep 0.2
  done
  local t_drain=$(( ($(date +%s%N) - t_end) / 1000000 ))

  kill -INT "$d_shelf" "$d_pers" 2>/dev/null || true
  wait "$d_shelf" "$d_pers" 2>/dev/null || true
  snap > "$WORK/$mode.cmd.after"

  local stall="n/a"
  local m
  m=$(grep -o 'stall=[0-9]*/[0-9]*ms' "$WORK/$mode.tickshelfd.log" | tail -1 || true)
  [ -n "$m" ] && stall="$m"

  echo "[$label] persist=$mode"
  echo "  SENT=$sent rows=$rows drain=${t_drain}ms stall=$stall"
  echo "  redis 命令增量（calls / 累计usec）:"
  diff_stats "$WORK/$mode.cmd.before" "$WORK/$mode.cmd.after"
  echo ""
}

echo "== A/B 基准：${SYMBOLS} symbols × ${RATE}/s，各 ${DUR}s =="
run_mode none "A 经典读回"
run_mode sqlite "B 写穿"
