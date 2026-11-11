#!/usr/bin/env bash
# 读取侧基准（存储布局 A/B 用）：灌数据 → 多窗口连发查询 → 时延/响应体量/内存/命令量。
# 输出一段自包含报告；改动存储布局前后各跑一次即可对比。
# 用法: scripts/read_bench.sh [标签] [灌数据秒=45]
set -euo pipefail

TAG="${1:-base}"
SEED_S="${2:-45}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"
WORK="/tmp/tickshelf_readbench_$TAG"
DB="$WORK/bench.db"
PORT_D="${PORT_D:-9944}"
PORT_Q="${PORT_Q:-9945}"

REDIS_CLI="${REDIS_CLI:-/opt/homebrew/bin/redis-cli}"
command -v "$REDIS_CLI" >/dev/null || REDIS_CLI=redis-cli
"$REDIS_CLI" PING >/dev/null || { echo "redis-server 不可达"; exit 1; }

mkdir -p "$WORK"
rm -f "$DB" "$DB-wal" "$DB-shm" "$WORK"/*.log "$WORK"/*.out "$WORK"/q*.txt
"$REDIS_CLI" FLUSHALL >/dev/null

D_SHELF=""; D_PERS=""; D_QRY=""; D_FEED=""
cleanup() {
  for p in "$D_FEED" "$D_SHELF" "$D_PERS" "$D_QRY"; do
    [ -n "$p" ] && kill -INT "$p" 2>/dev/null
  done
  wait 2>/dev/null
}
trap cleanup EXIT

snap() { "$REDIS_CLI" INFO commandstats | grep -E '^cmdstat_' | sed 's/\r$//' |
  awk -F'[:,=]' '{print substr($1, 9), $3, $5}' | sort; }
mem() { "$REDIS_CLI" INFO memory | grep '^used_memory:' | tr -d '\r' | cut -d: -f2; }

"$BUILD/tickshelfd" --port "$PORT_D" --flush-interval-ms 200 >>"$WORK/tickshelfd.log" 2>&1 & D_SHELF=$!
"$BUILD/persister" --db "$DB" --poll-ms 300 >>"$WORK/persister.log" 2>&1 & D_PERS=$!
"$BUILD/queryd" --port "$PORT_Q" --db "$DB" >>"$WORK/queryd.log" 2>&1 & D_QRY=$!
sleep 0.5
MEM_EMPTY=$(mem)
snap > "$WORK/cmd.before"

echo "== [$TAG] 灌数据 ${SEED_S}s：4 symbols × 500/s = 2000 ticks/s = 720 万 tick/h 口径 =="
"$BUILD/feedgen" --port "$PORT_D" --duration-s "$SEED_S" --symbols 4 --rate 500 \
  >"$WORK/feedgen.out" 2>"$WORK/feedgen.log"
SENT=$(grep -o 'sent=[0-9]*' "$WORK/feedgen.log" | tail -1 | cut -d= -f2)
TS_LINE=$(grep -o 'ts_first=[0-9]* ts_last=[0-9]*' "$WORK/feedgen.log" | tail -1)
TS_FIRST=$(printf '%s' "$TS_LINE" | cut -d= -f2 | cut -d' ' -f1)
TS_LAST=$(printf '%s' "$TS_LINE" | cut -d= -f3)
MEM_SEEDED=$(mem)
echo "   SENT=$SENT ts=$TS_FIRST..$TS_LAST used_memory=$MEM_SEEDED（空库 $MEM_EMPTY）"

sleep 1   # persister 追平尾部

# 多窗口基准：窗口越大命中段越多（128B/tick × 2000/s，1s≈0.26MB 10s≈2.6MB 30s≈7.9MB）
for W in 1 10 30; do
  FROM=$((TS_LAST - W * 1000 - 500))
  TO=$((TS_LAST - 500))
  R=$(python3 "$ROOT/examples/query_client.py" --port "$PORT_Q" --symbol 600000 \
       --from-ms "$FROM" --to-ms "$TO" --bench 30 2>>"$WORK/query.log" | tail -1)
  echo "   $R"
done

snap > "$WORK/cmd.after"
echo "   redis 命令增量（读路径相关）:"
awk 'NR==FNR {bc[$1]=$2; bu[$1]=$3; next}
     { c=$2-bc[$1]; u=$3-bu[$1]; if (c > 0) printf "     %-16s calls=%-9d usec=%d\n", $1, c, u }' \
    "$WORK/cmd.before" "$WORK/cmd.after" | grep -E "zrange|mget|get|hset|zadd|set |scan" || true
echo "   used_memory=${MEM_SEEDED}（灌 $SENT ticks 后）"
echo "== [$TAG] 完 =="
