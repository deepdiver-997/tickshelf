#!/usr/bin/env bash
# M2 故障演练（roadmap M2 #4）：淘汰进行中 kill -9 persister，验证"已持久化先淘汰"不变式。
#
# 压力设计：软水位压到 4MB 强制 evictor 全程工作；classic 模式（读回路径依赖 pending
# 数据，语义最严苛）。kill persister 窗口内的预期行为：
#   pending 批堆积 → 保护集非空 → 水位停住 → 内存上涨（绝不越 pending 删数据）；
#   persister 回来后 backlog 排干 → 水位恢复推进。
# 不变式的外部判据：
#   ①终态 SQLite rows == SENT（tickshelfd 零 kill，无在途丢失；若 evictor 误删 pending
#     数据，persister 把空批标 done → rows < SENT 立刻暴露）
#   ②evictor 全程 emergency=0（tier3 真丢层未触发）
#   ③kill 窗口内存曲线只涨不跌（水位停住的证据）+ 淘汰确实发生过（members_deleted > 0）
#
# 用法: scripts/evict_drill.sh [时长秒=60] [kill时刻秒=25] [kill窗口秒=15]
set -euo pipefail

DUR="${1:-60}"
KILL_AT="${2:-25}"
KILL_LEN="${3:-15}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"
WORK="/tmp/tickshelf_evict_drill"
DB="$WORK/drill.db"
PORT="${PORT:-9933}"

REDIS_CLI="${REDIS_CLI:-/opt/homebrew/bin/redis-cli}"
command -v "$REDIS_CLI" >/dev/null || REDIS_CLI=redis-cli
"$REDIS_CLI" PING >/dev/null || { echo "redis-server 不可达"; exit 1; }
for bin in tickshelfd persister feedgen evictor; do
  [ -x "$BUILD/$bin" ] || { echo "missing $BUILD/$bin"; exit 1; }
done

mkdir -p "$WORK"
rm -f "$DB" "$DB-wal" "$DB-shm" "$WORK"/*.log "$WORK"/*.out
"$REDIS_CLI" FLUSHALL >/dev/null

RATE="${RATE:-500}"; SYMBOLS="${SYMBOLS:-8}"; SOFT="${SOFT:-4000000}"

D_SHELF=""; D_PERS=""; D_FEED=""; D_EV=""
cleanup() {
  for p in "$D_FEED" "$D_SHELF" "$D_PERS" "$D_EV"; do
    [ -n "$p" ] && kill "$p" 2>/dev/null
  done
  wait 2>/dev/null
}
trap cleanup EXIT

log() { printf '[%03ds] %s\n' "$SECONDS" "$*" | tee -a "$WORK/drill.log"; }

start_persister() {
  "$BUILD/persister" --db "$DB" --poll-ms 300 >>"$WORK/persister.log" 2>&1 & D_PERS=$!
}

"$BUILD/tickshelfd" --port "$PORT" --flush-interval-ms 300 >>"$WORK/tickshelfd.log" 2>&1 & D_SHELF=$!
"$BUILD/evictor" --soft-watermark-bytes "$SOFT" --sample-ms 500 >>"$WORK/evictor.log" 2>&1 & D_EV=$!
start_persister
sleep 0.5

log "drill 开始: ${DUR}s, ${SYMBOLS}×${RATE}/s 突发5x, 软水位 ${SOFT}B, kill persister @${KILL_AT}s 持续 ${KILL_LEN}s"
"$BUILD/feedgen" --port "$PORT" --duration-s "$DUR" --symbols "$SYMBOLS" --rate "$RATE" \
  >"$WORK/feedgen.out" 2>"$WORK/feedgen.log" & D_FEED=$!

# kill 窗口：杀 persister → 采样内存曲线（水位停住的证据）
sleep "$KILL_AT"
kill -9 "$D_PERS" 2>/dev/null || true
log "kill -9 persister，窗口 ${KILL_LEN}s（预期：水位停住，内存只涨不跌）"
mem_win_start=$("$REDIS_CLI" INFO memory | grep '^used_memory:' | tr -d '\r' | cut -d: -f2)
sleep "$KILL_LEN"
mem_win_end=$("$REDIS_CLI" INFO memory | grep '^used_memory:' | tr -d '\r' | cut -d: -f2)
log "kill 窗口内存: ${mem_win_start} → ${mem_win_end}"
start_persister

wait "$D_FEED" || true; D_FEED=""

# 追平：persister 排干 backlog，evictor 恢复推进
log "等待追平 rows==SENT"
tries=0
SENT=$(awk '/^SENT /{print $2}' "$WORK/feedgen.out" | tail -1)
while :; do
  rows=$(sqlite3 "$DB" 'SELECT COUNT(*) FROM ticks' 2>/dev/null || echo 0)
  [ "$rows" -ge "$SENT" ] && break
  tries=$((tries + 1)); [ "$tries" -gt 300 ] && break
  sleep 0.5
done

kill -INT "$D_SHELF" "$D_PERS" "$D_EV" 2>/dev/null || true
wait "$D_SHELF" "$D_PERS" "$D_EV" 2>/dev/null || true; D_SHELF=""; D_PERS=""; D_EV=""

EV_FINAL="$(tail -1 "$WORK/evictor.log")"
EV_MEMBERS="${EV_FINAL##*segs=}"; EV_MEMBERS="${EV_MEMBERS%% *}"
EV_EMERGENCY="${EV_FINAL##*emergency=}"; EV_EMERGENCY="${EV_EMERGENCY%% *}"

echo "=================================="
echo "feedgen SENT        : $SENT"
echo "SQLite ROWS         : $rows"
echo "MISSING             : $((SENT - rows))"
echo "kill 窗口内存       : ${mem_win_start} → ${mem_win_end}（涨=水位停住 ✓）"
echo "evictor 淘汰段数    : $EV_MEMBERS members"
echo "tier3 触发数        : $EV_EMERGENCY（应 0）"
echo "=================================="

FAIL=0
[ "$rows" -eq "$SENT" ] || { echo "FAIL: rows != SENT（疑似误删 pending 数据）"; FAIL=1; }
[ "$mem_win_end" -gt "$mem_win_start" ] || { echo "FAIL: kill 窗口内存未涨（水位语义异常）"; FAIL=1; }
[ "$EV_EMERGENCY" -eq 0 ] || { echo "FAIL: tier3 在非紧急水位触发"; FAIL=1; }
[ "$EV_MEMBERS" -ge 1 ] 2>/dev/null || { echo "FAIL: 淘汰从未发生（水位/负载配置失效）"; FAIL=1; }
[ "$FAIL" -eq 0 ] && echo "PASS ✓  已持久化先淘汰不变式保持"
exit "$FAIL"
