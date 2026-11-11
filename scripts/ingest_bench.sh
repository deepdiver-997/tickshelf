#!/usr/bin/env bash
# ingest_bench.sh —— daemon 纯摄入天花板 + 写穿 stall 线性验证（benchmarks.md 未测清单 #1）。
#
# 阶段A（纯摄入，classic --persist none）: 临时 Redis :6399 + evictor(tier3 紧急清 pending
#   段——基准数据不要了) + 4×floodgen 并行不限速灌 15s → daemon+Redis 联合摄入天花板。
#   4 发端进程各占一核绕开单进程表驱动 CRC 的发端上限；symbol 4×64=256 个。
# 阶段B（写穿 --persist sqlite）: 分步速率 50k/100k/200k/400k/800k 各 8s + 2M/3M 各 6s，
#   验证 stall ≈ R×flush_s/persist_rate 线性（benchmarks.md 推导的 ~11万 10%-预算线）
#   与高步的自节流平台（ingested 涨不动 = inline upsert 吃满 flush 预算）。
#
# 全程独立实例不碰主 Redis；Release 构建（BIN_DIR 覆盖，默认 build-release）。
# bash 纪律：无作业号（%N 在非交互 shell 不可用）、全部显式 PID、无 grep/pipe 退出码陷阱。
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-build-release}
[ -x "$BIN/tickshelfd" ] || BIN=build
SYMS=${SYMS:-256}   # 符号总数（阶段A 4 发端均分；生产基数对照用 SYMS=5400）
PORT=6399
DPORT_A=9101
DPORT_B=9102
DUR_A=15
DATA=$(mktemp -d)
DB=/tmp/tickshelf_ingest_bench.db
SOFW=536870912    # 512MB
EMGW=1073741824   # 1GB

rc() { redis-cli -p "$PORT" --raw "$@"; }

# 预检：清掉上次异常退出留下的僵尸 daemon/floodgen（真实教训：僵尸占用同一 stats 键
# 与监听端口，样本里混入冻结计数器，且新 daemon bind 失败后整个阶段跑在僵尸上）
pkill -f "tickshelfd --bind 127.0.0.1 --port 910" 2>/dev/null || true
pkill -f "floodgen --port 910" 2>/dev/null || true
sleep 0.5

# 从 stats:tickshelfd 文本体取字段（无管道、无 grep 退出码陷阱）
getf() {  # getf <body> <field>
  local b="$1" k="$2" v
  case "$b" in *"$k="*) v="${b#*"$k="}"; printf '%s' "${v%%;*}";; *) printf '0';; esac
}

PIDS=()
cleanup() {
  local p
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  rc SHUTDOWN NOSAVE >/dev/null 2>&1 || true
  rm -rf "$DATA" "$DB" "$DB-wal" "$DB-shm"
}
trap cleanup EXIT

bg() {  # bg <日志路径> <命令...> —— 后台起进程并记 PID
  local log="$1"; shift
  "$@" >"$log" 2>&1 &
  PIDS+=("$!")
}

redis-server --port "$PORT" --save '' --appendonly no --maxmemory 6gb \
  --maxmemory-policy noeviction --dir "$DATA" >/dev/null 2>&1 &
for _ in $(seq 1 50); do rc PING >/dev/null 2>&1 && break; sleep 0.1; done
rc PING >/dev/null

echo "== 预备：persist_rate 微基准（Release capacity，预测 stall 用）=="
"$BIN/capacity" --member-overhead 49 >"$DATA/cap.txt" 2>&1 || true
PERSIST_RATE=$(command awk '/persist_rate =/ {print int($4); exit}' "$DATA/cap.txt")
if [ -z "$PERSIST_RATE" ] || [ "$PERSIST_RATE" = "0" ]; then PERSIST_RATE=1000000; fi
echo "persist_rate = $PERSIST_RATE rows/s"

# 采样：每 2s 一次（对齐 stats 发布周期 2s，避免 1s 采样读同一发布值的混叠）
prev_ing=0
PEAK=0
sample() {
  local n="$1" i body ing pend rate used
  for i in $(seq 1 "$n"); do
    sleep 2
    body="$(rc GET stats:tickshelfd || true)"
    ing=$(getf "$body" ingested)
    pend=$(getf "$body" pending)
    rc INFO memory >"$DATA/info.txt" || true
    used=$(command awk '/^used_memory:/ {print substr($0, 13); exit}' "$DATA/info.txt")
    rate=$(( (ing - prev_ing) / 2 ))
    prev_ing=$ing
    (( rate > PEAK )) && PEAK=$rate
    printf '  t=%ss ingested_rate=%s/s pending=%s used_mem=%s\n' "$((i * 2))" "$rate" "$pend" "$used"
  done
}

echo
echo "== 阶段A：纯摄入天花板（classic --persist none，4×floodgen 不限速 ${DUR_A}s）=="
bg "$DATA/daemonA.log" "$BIN/tickshelfd" --bind 127.0.0.1 --port "$DPORT_A" \
  --redis-host 127.0.0.1 --redis-port "$PORT" --persist none
DAEMON_A=${PIDS[-1]}
bg "$DATA/evictorA.log" "$BIN/evictor" --redis-port "$PORT" \
  --soft-watermark-bytes "$SOFW" --emergency-watermark-bytes "$EMGW" --sample-ms 500
sleep 1
prev_ing=0
PEAK=0
for s in 0 1 2 3; do
  bg "$DATA/floodA$s.log" "$BIN/floodgen" --port "$DPORT_A" --symbols $((SYMS / 4)) \
    --sym-base $((s * SYMS / 4)) --ticks-per-frame 4096 --duration "$DUR_A"
done
sample $(( (DUR_A + 3) / 2 ))
echo "PHASE_A_CEILING(peak 2s-avg ingested rate) = $PEAK ticks/s"
command grep -h '^SENT' "$DATA"/floodA*.log >"$DATA/sentA.txt" || true
cat "$DATA/sentA.txt"
for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done   # redis 不在 PIDS，全杀安全
for p in "${PIDS[@]}"; do wait "$p" 2>/dev/null || true; done
PIDS=()

echo
echo "== 阶段B：写穿分步速率（--persist sqlite）=="
rc FLUSHALL >/dev/null
rm -f "$DB" "$DB-wal" "$DB-shm"
bg "$DATA/daemonB.log" "$BIN/tickshelfd" --bind 127.0.0.1 --port "$DPORT_B" \
  --redis-host 127.0.0.1 --redis-port "$PORT" --persist sqlite --db "$DB"
sleep 1

run_step() {  # run_step <速率> <秒>
  local R="$1" secs="$2" i body ing pend sa sm rows rate sum=0 n=0
  "$BIN/floodgen" --port "$DPORT_B" --symbols "$SYMS" --ticks-per-frame 4096 \
    --ts-base "$((R + 1000))" \
    --rate "$R" --duration "$secs" >"$DATA/floodB.log" 2>&1 &
  FGPID=$!
  for i in $(seq 1 $(( secs / 2 ))); do
    sleep 2
    body="$(rc GET stats:tickshelfd || true)"
    ing=$(getf "$body" ingested)
    pend=$(getf "$body" pending)
    sa=$(getf "$body" stall_avg)
    sm=$(getf "$body" stall_max)
    rows=$(getf "$body" rows)
    rate=$(( (ing - prev_ing) / 2 )); prev_ing=$ing; sum=$(( sum + rate )); n=$(( n + 1 ))
    printf '  target=%-8s t=%ss ing=%-8s/s pend=%-8s stall_avg=%-5s stall_max=%-5s rows=%s\n' \
      "$R" "$((i * 2))" "$rate" "$pend" "$sa" "$sm" "$rows"
  done
  wait "$FGPID" 2>/dev/null || true
  local avg=$(( sum / (n > 0 ? n : 1) ))
  local pred=$(( R * 500 / (PERSIST_RATE > 0 ? PERSIST_RATE : 1) ))
  printf 'STEP target=%-8s avg_ing=%-8s/s stall_max=%-6sms 预测stall=%sms\n' \
    "$R" "$avg" "$sm" "$pred"
}

prev_ing=0   # daemon B 新进程计数器从零起——不重置会把阶段A的累计值当成上一采样

run_step 50000 8
run_step 100000 8
run_step 200000 8
run_step 400000 8
run_step 800000 8
run_step 2000000 6
run_step 3000000 6
kill "${PIDS[-1]}" 2>/dev/null || true   # daemon B

echo
echo "== 完成（临时实例/DB 已清理）=="
