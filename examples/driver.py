#!/usr/bin/env python3
"""TTAP 参考驱动（Python 版）——"协议即跨语言接口"的最小证明（ADR-009）。

按 docs/protocol.md 的线格式裸手打包：帧头 20B + N × Tick 64B，小端，CRC32 载荷校验。
任何能开 TCP 的语言照本文件的 struct 格式即可喂数据，不需要任何客户端库。

用法：先起 tickshelfd，再：
    python3 examples/driver.py --port 9900 --symbols 2 --rate 100 --duration-s 3
"""
import argparse
import random
import socket
import struct
import sys
import time
import zlib

# 帧头（20B）：magic[4] version type count flags reserved payload_bytes crc32
HEADER = struct.Struct("<4sBBHHHII")
# Tick（64B）：symbol[8] ts_exchange ts_receive bid ask last bid_sz ask_sz last_sz flags pad[3]
TICK = struct.Struct("<8sQQqqqIIIB3x")
MAGIC = b"TTAP"
VERSION = 1
TYPE_TICK_BATCH = 0
TYPE_HEARTBEAT = 1
MAX_SYMBOLS = 1000  # 唯一键 (symbol, ts) 要求 ts 步长 1ms → 单 symbol 速率上限 1000/s


def build_frame(ftype: int, ticks: list[bytes]) -> bytes:
    payload = b"".join(ticks)
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    head = HEADER.pack(MAGIC, VERSION, ftype, len(ticks), 0, 0, len(payload), crc)
    return head + payload


def encode_tick(sym: bytes, ts: int, recv_ms: int, bid: int, ask: int, last: int,
                bid_sz: int, ask_sz: int, last_sz: int, flags: int) -> bytes:
    return TICK.pack(sym.ljust(8, b"\0")[:8], ts, recv_ms, bid, ask, last,
                     bid_sz, ask_sz, last_sz, flags)


def main() -> int:
    p = argparse.ArgumentParser(description="TTAP python driver")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9900)
    p.add_argument("--symbols", type=int, default=2)
    p.add_argument("--rate", type=float, default=100.0, help="每 symbol 每秒 tick 数 (≤1000)")
    p.add_argument("--duration-s", type=float, default=3.0)
    p.add_argument("--seed", type=int, default=7)
    args = p.parse_args()
    if not (1 <= args.symbols <= MAX_SYMBOLS and 0 < args.rate <= 1000):
        print("need 1<=symbols<=1000, 0<rate<=1000", file=sys.stderr)
        return 2

    rng = random.Random(args.seed)
    # symbol 命名与 C++ feedgen 同款（600000 起），中点价 ~100 元随机游走
    syms = [(f"600{i:03d}").encode() for i in range(args.symbols)]
    mids = [10_000_000 + rng.randrange(4_000_000) for _ in syms]

    sock = socket.create_connection((args.host, args.port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    sent = frames = 0
    start = time.monotonic()
    start_ms = int(time.time() * 1000)  # 交易所 ts 基点：每轮 +1ms，唯一键由生成器保证
    interval = 1.0 / min(args.rate, MAX_SYMBOLS)
    next_round = start
    round_ = 0
    try:
        while time.monotonic() - start < args.duration_s:
            now = time.monotonic()
            if now < next_round:
                time.sleep(next_round - now)
            next_round = time.monotonic() + interval

            ticks = []
            for i, sym in enumerate(syms):
                step = rng.randint(-100, 100)
                mids[i] += step + (10_000_000 - mids[i]) // 10000  # 游走 + 均值回复
                spread = rng.randint(100, 300)
                bid, ask = mids[i] - spread // 2, mids[i] + spread // 2
                if rng.random() < 0.8:
                    last, last_sz, flags = bid + rng.randint(0, spread), rng.randint(1, 500), 0
                else:
                    last, last_sz, flags = 0, 0, 1  # flags bit0：last 无成交
                ticks.append(encode_tick(sym, start_ms + round_, int(time.time() * 1000),
                                         bid, ask, last,
                                         rng.randint(10, 1000), rng.randint(10, 1000),
                                         last_sz, flags))
            sock.sendall(build_frame(TYPE_TICK_BATCH, ticks))
            sent += len(ticks)
            frames += 1
            round_ += 1
    except (BrokenPipeError, ConnectionResetError):
        print("driver: connection lost (tickshelfd down?)", file=sys.stderr)
        return 1
    finally:
        sock.close()

    print(f"SENT {sent} {frames}")  # 对账锚点，与 C++ feedgen 同格式
    return 0


if __name__ == "__main__":
    sys.exit(main())
