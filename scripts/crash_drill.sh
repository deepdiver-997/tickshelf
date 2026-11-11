#!/usr/bin/env bash
# M1 崩溃注入验收（docs/roadmap.md M1 #4）：
#   运行中随机 kill -9 tickshelfd / persister 并重启，跑满时长后三层对账：
#     feedgen SENT（发送上界） vs SQLite 行数（唯一键数）
#   断言：①SQLite 零重复（PK 兜底，显式验证）②ROWS ≤ SENT
#         ③MISSING ≤ kill 次数 × 单次丢失预算（在途口径，见下）
#   口径说明：kill tickshelfd 丢的是"内核 socket 缓冲在途帧 + 未 flush 的攒批"，
#   属 TCP 断连窗口（roadmap 允许，单独标注）；kill persister 不丢数据只延迟
#   （数据留在 Redis 等下一轮认领）。Redis 自身崩溃窗口不在 M1 演练内。
#
# 用法: scripts/crash_drill.sh [时长_分钟]   # 默认 10
# 前置: 本机 redis-server 已就绪（脚本不代管 redis 生命周期），build/ 里已有四个二进制。
set -euo pipefail

DURATION_MIN="${1:-10}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"
WORK="/tmp/tickshelf_drill"
DB="$WORK/drill.db"
LOG="$WORK/drill.log"
LOSS_BUDGET_PER_KILL=200000   # 单次 kill tickshelfd 的丢失预算（tick 数，宽松上界）

REDIS_CLI="${REDIS_CLI:-/opt/homebrew/bin/redis-cli}"
command -v "$REDIS_CLI" >/dev/null || REDIS_CLI=redis-cli

for bin in tickshelfd persister feedgen; do
  [ -x "$BUILD/$bin" ] || { echo "missing $BUILD/$bin — 先 cmake --build build"; exit 1; }
done
"$REDIS_CLI" PING >/dev/null || { echo "redis-server 不可达"; exit 1; }

TICKSHELF_PORT="${TICKSHELF_PORT:-9911}"
RATE="${RATE:-500}"          # 每 symbol 每秒 tick 数
SYMBOLS="${SYMBOLS:-8}"
BURST_MULT="${BURST_MULT:-5}"

mkdir -p "$WORK"
rm -f "$DB" "$LOG" "$WORK/feedgen.out" "$WORK/tickshelfd.log" "$WORK/persister.log"
"$REDIS_CLI" FLUSHALL >/dev/null

D_SHELF="" D_PERS="" D_FEED=""
cleanup() {
  [ -n "$D_FEED" ]  && kill "$D_FEED" 2>/dev/null
  [ -n "$D_SHELF" ] && kill "$D_SHELF" 2>/dev/null
  [ -n "$D_PERS" ]  && kill "$D_PERS" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT

log() { printf '[%03ds] %s\n' "$SECONDS" "$*" | tee -a "$LOG"; }

start_tickshelfd() {
  "$BUILD/tickshelfd" --port "$TICKSHELF_PORT" --flush-interval-ms 300 \
    --persist "${TICKSHELF_PERSIST:-none}" --db "$DB" \
    >>"$WORK/tickshelfd.log" 2>&1 & D_SHELF=$!
}
start_persister() {
  "$BUILD/persister" --redis-port 6379 --db "$DB" --poll-ms 300 \
    >>"$WORK/persister.log" 2>&1 & D_PERS=$!
}

# ---- 总时长 = 演练时长 + 收尾余量（flush + persister 追完） ----
TOTAL_S=$((DURATION_MIN * 60 + 10))
log "drill 开始: ${DURATION_MIN}min, ${SYMBOLS} symbols × ${RATE}/s, 突发 ${BURST_MULT}x"

start_tickshelfd
start_persister
"$BUILD/feedgen" --port "$TICKSHELF_PORT" --duration-s "$TOTAL_S" \
  --symbols "$SYMBOLS" --rate "$RATE" --burst-mult "$BURST_MULT" \
  >"$WORK/feedgen.out" 2>"$WORK/feedgen.log" & D_FEED=$!

# ---- 随机 kill -9 循环 ----
KILLS_SHELF=0; KILLS_PERS=0
LAST=$((SECONDS + DURATION_MIN * 60 - 10))   # 最后 10s 停止注入，让管线追平
while (( SECONDS < LAST )); do
  sleep $((15 + RANDOM % 30))
  (( SECONDS >= LAST )) && break
  if (( RANDOM % 2 == 0 )); then
    kill -9 "$D_SHELF" 2>/dev/null || true
    KILLS_SHELF=$((KILLS_SHELF + 1))
    log "kill -9 tickshelfd（第 $KILLS_SHELF 次），2s 后重启"
    sleep 2; start_tickshelfd
  else
    kill -9 "$D_PERS" 2>/dev/null || true
    KILLS_PERS=$((KILLS_PERS + 1))
    log "kill -9 persister（第 $KILLS_PERS 次），2s 后重启"
    sleep 2; start_persister
  fi
done

# ---- 收尾：等 feedgen 自然退出（发满 TOTAL_S），再给 flush/追账余量 ----
log "注入结束，等待 feedgen 收尾"
wait "$D_FEED" || true; D_FEED=""
# 写穿模式下被杀 daemon 的在途批要等租约 TTL（60s）过期才由 persister 接手——等追平再收
tail_tries=0
while :; do
  PENDING_NOW=0
  while read -r k; do
    v=$("$REDIS_CLI" GET "$k")
    [ "$v" = "pending" ] && PENDING_NOW=$((PENDING_NOW + 1))
  done < <("$REDIS_CLI" --scan --pattern 'state:*')
  [ "$PENDING_NOW" -eq 0 ] && break
  tail_tries=$((tail_tries + 1))
  [ "$tail_tries" -gt 90 ] && break   # 180s 上限（>2×TTL）
  sleep 2
done
sleep 3
kill -INT "$D_SHELF" "$D_PERS" 2>/dev/null || true
wait "$D_SHELF" "$D_PERS" 2>/dev/null || true; D_SHELF=""; D_PERS=""

# ---- 三层对账 ----
SENT=$(awk '/^SENT /{print $2}' "$WORK/feedgen.out" | tail -1)
ROWS=$(sqlite3 "$DB" 'SELECT COUNT(*) FROM ticks')
DUP_SQL="SELECT COUNT(*) - COUNT(DISTINCT symbol || ':' || ts_exchange_ms) FROM ticks"
DUP=$(sqlite3 "$DB" "$DUP_SQL")
PENDING=0
while read -r k; do
  v=$("$REDIS_CLI" GET "$k")
  [ "$v" = "pending" ] && PENDING=$((PENDING + 1))
done < <("$REDIS_CLI" --scan --pattern 'state:*')

BUDGET=$(( KILLS_SHELF * LOSS_BUDGET_PER_KILL ))
MISSING=$(( SENT - ROWS ))

echo "=================================="
echo "feedgen SENT      : $SENT"
echo "SQLite ROWS       : $ROWS（唯一键数）"
echo "SQLite 重复行     : $DUP"
echo "MISSING           : $MISSING（kill tickshelfd $KILLS_SHELF 次 × 预算 $LOSS_BUDGET_PER_KILL）"
echo "state 残留 pending: $PENDING（>0 = persister 未追完）"
echo "=================================="

FAIL=0
[ "$DUP" -eq 0 ]              || { echo "FAIL: SQLite 出现重复"; FAIL=1; }
[ "$ROWS" -le "$SENT" ]       || { echo "FAIL: ROWS > SENT（违反上界口径）"; FAIL=1; }
[ "$MISSING" -le "$BUDGET" ]  || { echo "FAIL: MISSING 超出 kill 预算"; FAIL=1; }
[ "$PENDING" -eq 0 ]          || { echo "FAIL: 有批未落库"; FAIL=1; }
[ "$FAIL" -eq 0 ] && echo "PASS ✓  M1 崩溃注入验收通过"
exit "$FAIL"
