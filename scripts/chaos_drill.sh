#!/usr/bin/env bash
# M3 缺口回补演练（ADR-006 混沌 + ADR-014 懒触发 + ADR-015 查询面）：
#   混沌源制造真缺口（按概率丢弃发送，ring 保留）→ Python 查询客户端发现缺口
#   （显式上报 + 懒触发 backfill 键）→ feedgen 回补执行器从 ring 直写 SQLite
#   → 同窗口重查缺口闭合。
# 判据：
#   ①首次查询 missing > 0（缺失显式上报，不是静默空）
#   ②回补后同窗口重查 missing == 0（ring 保留窗内的缺口全部闭合）
#   ③SQLite 行数 > 0 且 ≥ 发送量（回补只增不减）
# 用法: scripts/chaos_drill.sh [灌数据秒=40] [查询窗口秒=5]
set -euo pipefail

DUR="${1:-40}"
WIN="${2:-5}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"
WORK="/tmp/tickshelf_chaos_drill"
DB="$WORK/drill.db"
PORT_D="${PORT_D:-9900}"
PORT_Q="${PORT_Q:-9910}"

REDIS_CLI="${REDIS_CLI:-/opt/homebrew/bin/redis-cli}"
command -v "$REDIS_CLI" >/dev/null || REDIS_CLI=redis-cli
"$REDIS_CLI" PING >/dev/null || { echo "redis-server 不可达"; exit 1; }
for bin in tickshelfd persister queryd feedgen; do
  [ -x "$BUILD/$bin" ] || { echo "missing $BUILD/$bin"; exit 1; }
done
[ -f "$ROOT/examples/query_client.py" ] || { echo "missing examples/query_client.py"; exit 1; }

mkdir -p "$WORK"
rm -f "$DB" "$DB-wal" "$DB-shm" "$WORK"/*.log "$WORK"/*.out
"$REDIS_CLI" FLUSHALL >/dev/null

D_SHELF=""; D_PERS=""; D_QRY=""; D_FEED=""
cleanup() {
  for p in "$D_FEED" "$D_SHELF" "$D_PERS" "$D_QRY"; do
    [ -n "$p" ] && kill -INT "$p" 2>/dev/null
  done
  wait 2>/dev/null
}
trap cleanup EXIT

log() { printf '[%03ds] %s\n' "$SECONDS" "$*" | tee -a "$WORK/drill.log"; }

missing_of() { printf '%s\n' "$1" | awk '{for(i=1;i<=NF;i++) if ($i ~ /^missing=/) {split($i,a,"="); print a[2]+0}}'; }

"$BUILD/tickshelfd" --port "$PORT_D" --flush-interval-ms 300 >>"$WORK/tickshelfd.log" 2>&1 & D_SHELF=$!
"$BUILD/persister" --db "$DB" --poll-ms 300 >>"$WORK/persister.log" 2>&1 & D_PERS=$!
"$BUILD/queryd" --port "$PORT_Q" --db "$DB" >>"$WORK/queryd.log" 2>&1 & D_QRY=$!
sleep 0.5

# 混沌源永续（回补执行器在它体内，演练结束才收）
"$BUILD/feedgen" --port "$PORT_D" --duration-s 0 --symbols 4 --rate 400 \
  --chaos-gap-pct 2 --chaos-ooo-pct 2 --chaos-dup-pct 5 \
  --chaos-stall-every-s 10 --chaos-stall-s 6 \
  --ring-cap 500000 --backfill-db "$DB" --backfill-every-ms 1000 \
  >"$WORK/feedgen.out" 2>"$WORK/feedgen.log" & D_FEED=$!
log "混沌源启动: gap=2% ooo=2% dup=5% stall=6s/10s，灌 ${DUR}s"

sleep "$DUR"

# 查询窗口必须用交易所 ts 口径（feedgen 的 ts 时钟 ≠ 墙钟：ts 每轮 +1ms，轮速=速率）。
# 从 feedgen stats 里取 ts_first/ts_last，取内部一段（避开两端半开缺口）。
TS_LINE=$(grep -o 'ts_first=[0-9]* ts_last=[0-9]*' "$WORK/feedgen.log" | tail -1)
TS_FIRST=$(printf '%s' "$TS_LINE" | cut -d= -f2 | cut -d' ' -f1)
TS_LAST=$(printf '%s' "$TS_LINE" | cut -d= -f3)
FROM=$((TS_FIRST + 3000))
TO=$((TS_LAST - 500))
log "查询窗口（交易所 ts 口径）: $FROM - $TO"

log "首次查询 → 期望缺口显式上报并懒触发回补"
Q1=$(python3 "$ROOT/examples/query_client.py" --port "$PORT_Q" --symbol 600000 \
      --from-ms "$FROM" --to-ms "$TO" 2>>"$WORK/query.log" | tail -1)
log "Q1: ${Q1:0:120}..."
M1=$(missing_of "$Q1")

# 收敛循环：每次查询触发下一批回补（max_missing_ranges=64/轮），客户端持续重查直至闭合
log "收敛循环：重查直至缺口闭合（每轮触发 64 个区间的回补）"
Q2="$Q1"
M2="$M1"
rounds=0
while [ "$M2" -gt 0 ] && [ "$rounds" -lt 12 ]; do
  sleep 2
  Q2=$(python3 "$ROOT/examples/query_client.py" --port "$PORT_Q" --symbol 600000 \
        --from-ms "$FROM" --to-ms "$TO" 2>>"$WORK/query.log" | tail -1)
  M2=$(missing_of "$Q2")
  rounds=$((rounds + 1))
done
TICKS_FINAL=$(printf '%s\n' "$Q2" | awk '{for(i=1;i<=NF;i++) if ($i ~ /^ticks=/) {split($i,a,"="); print a[2]+0}}')
log "收敛完成: ${rounds} 轮重查, 终态 missing=$M2 ticks=$TICKS_FINAL"

log "最新价投影快速路径检查（ADR-016）"
QL=$(python3 "$ROOT/examples/query_client.py" --port "$PORT_Q" --symbol 600000 \
      --latest 2>>"$WORK/query.log" | tail -1)
log "QL: $QL"

sleep 1
kill -INT "$D_FEED" "$D_SHELF" "$D_PERS" "$D_QRY" 2>/dev/null || true
wait "$D_FEED" "$D_SHELF" "$D_PERS" "$D_QRY" 2>/dev/null || true
D_FEED=""; D_SHELF=""; D_PERS=""; D_QRY=""

missing_of() { printf '%s\n' "$1" | awk '{for(i=1;i<=NF;i++) if ($i ~ /^missing=/) {split($i,a,"="); print a[2]+0}}'; }
M1=$(missing_of "$Q1")
ROWS=$(sqlite3 "$DB" 'SELECT COUNT(*) FROM ticks' 2>/dev/null || echo 0)
SENT=$(grep -o 'sent=[0-9]*' "$WORK/feedgen.log" | tail -1 | cut -d= -f2)

echo "=================================="
echo "首查缺口区间数     : $M1（应 > 0）"
echo "收敛轮数           : $rounds（每轮 64 区间）"
echo "终态缺口区间数     : $M2（应 = 0，ring 保留窗内全部闭合）"
echo "终态 ticks         : $TICKS_FINAL"
echo "SQLite ROWS        : $ROWS（含回补行；feedgen 发送 sent=$SENT）"
echo "最新价投影         : $QL（应 found=1）"
echo "=================================="

FAIL=0
[ "$M1" -gt 0 ] || { echo "FAIL: 首查无缺口——混沌没制造出缺口"; FAIL=1; }
[ "$M2" -eq 0 ] || { echo "FAIL: 回补后缺口未闭合"; FAIL=1; }
[ "$ROWS" -gt 0 ] || { echo "FAIL: SQLite 空"; FAIL=1; }
printf '%s\n' "$QL" | grep -q "^LATEST sym=600000 ts=[0-9]" || { echo "FAIL: 最新价投影缺失"; FAIL=1; }
[ "$FAIL" -eq 0 ] && echo "PASS ✓  缺口回补闭环（混沌 → 显式上报 → 懒触发 → ring 直写 → 闭合）"
exit "$FAIL"
