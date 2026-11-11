#!/usr/bin/env python3
"""TTAP 查询客户端（Python 版）——读侧跨语言契约的最小证明（ADR-015）。

查询请求（type=2）打给 queryd，解析响应帧（ticks + 缺失区间块）。
查询命中缺口时 queryd 侧会懒触发缺口回补（backfill 键），等回补执行后重查即闭合。

用法：
    python3 examples/query_client.py --port 9910 --symbol 600000 --seconds-back 60
输出最后一行：QUERY sym=... ticks=N missing=K ranges=lo-hi,lo-hi
"""
import argparse
import socket
import struct
import sys
import time
import zlib

HEADER = struct.Struct("<4sBBHHHII")
TICK = struct.Struct("<8sQQqqqIIIB3x")
MAGIC = b"TTAP"
VERSION = 1
TYPE_QUERY = 2
TYPE_LATEST = 3
FLAG_HAS_MISSING = 0x0002
QUERY_SPEC = struct.Struct("<8sQQ")       # sym[8] + from_ms + to_ms
LATEST_SPEC = struct.Struct("<8s")        # sym[8]
U32 = struct.Struct("<I")
RANGE = struct.Struct("<QQ")


def build_raw_frame(ftype: int, count: int, flags: int, payload: bytes) -> bytes:
    head = HEADER.pack(MAGIC, VERSION, ftype, count, flags, 0, len(payload),
                       zlib.crc32(payload) & 0xFFFFFFFF)
    return head + payload


def build_query_frame(sym: bytes, from_ms: int, to_ms: int) -> bytes:
    payload = QUERY_SPEC.pack(sym.ljust(8, b"\0")[:8], from_ms, to_ms)
    return build_raw_frame(TYPE_QUERY, 0, 0x0000, payload)


def build_latest_frame(sym: bytes) -> bytes:
    return build_raw_frame(TYPE_LATEST, 0, 0x0000, LATEST_SPEC.pack(sym.ljust(8, b"\0")[:8]))


def read_exact(sock, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("queryd closed connection")
        buf += chunk
    return buf


def main() -> int:
    p = argparse.ArgumentParser(description="TTAP query client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9910)
    p.add_argument("--symbol", default="600000")
    p.add_argument("--seconds-back", type=float, default=60.0,
                   help="查询 [now-seconds_back, now]（墙钟口径）")
    p.add_argument("--from-ms", type=int, default=0, help="显式交易所 ts 起点（优先于 seconds-back）")
    p.add_argument("--to-ms", type=int, default=0, help="显式交易所 ts 终点")
    p.add_argument("--latest", action="store_true", help="查最新价投影（type=3，不走区间）")
    p.add_argument("--bench", type=int, default=0, help="基准模式：同窗口连发 N 次统计时延")
    args = p.parse_args()

    sock = socket.create_connection((args.host, args.port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    sym = args.symbol.encode()

    if args.latest:
        sock.sendall(build_latest_frame(sym))
        magic, ver, ftype, count, flags, _res, payload_len, _crc = HEADER.unpack(
            read_exact(sock, HEADER.size))
        if magic != MAGIC or ftype != TYPE_LATEST:
            print("bad response frame", file=sys.stderr)
            return 1
        payload = read_exact(sock, payload_len)
        if count == 0:
            print(f"LATEST sym={args.symbol} found=0")
            return 0
        (lsym, ts, _recv, bid, ask, last, _bs, _as, _ls, fl) = TICK.unpack(payload)
        print(f"LATEST sym={lsym.rstrip(chr(0).encode()).decode()} ts={ts} "
              f"bid={bid / 10000:.4f} ask={ask / 10000:.4f} last={last / 10000:.4f} "
              f"no_last={bool(fl & 1)}")
        return 0

    now = int(time.time() * 1000)
    if args.from_ms and args.to_ms:
        from_ms, to_ms = args.from_ms, args.to_ms
    else:
        from_ms, to_ms = now - int(args.seconds_back * 1000), now

    if args.bench > 0:
        # 基准模式：同窗口连发 N 次，统计端到端时延与响应体量
        import statistics
        sock.sendall(build_query_frame(sym, from_ms, to_ms))
        head = read_exact(sock, HEADER.size)
        magic, ver, ftype, count, flags, _res, payload_len, _crc = HEADER.unpack(head)
        read_exact(sock, payload_len)
        total = payload_len + HEADER.size
        lat = []
        for _ in range(args.bench):
            t0 = time.perf_counter()
            sock.sendall(build_query_frame(sym, from_ms, to_ms))
            head = read_exact(sock, HEADER.size)
            magic, ver, ftype, count, flags, _res, payload_len, _crc = HEADER.unpack(head)
            read_exact(sock, payload_len)
            lat.append((time.perf_counter() - t0) * 1000.0)
        lat.sort()
        p95 = lat[min(len(lat) - 1, int(len(lat) * 0.95))]
        print(f"BENCH window={from_ms}-{to_ms} n={args.bench} "
              f"avg={statistics.fmean(lat):.2f}ms p95={p95:.2f}ms "
              f"max={lat[-1]:.2f}ms resp={total}B ticks={count}")
        return 0

    sock.sendall(build_query_frame(sym, from_ms, to_ms))

    head = read_exact(sock, HEADER.size)
    magic, ver, ftype, count, flags, _res, payload_len, _crc = HEADER.unpack(head)
    if magic != MAGIC or ftype != TYPE_QUERY:
        print("bad response frame", file=sys.stderr)
        return 1
    payload = read_exact(sock, payload_len)

    ticks = []
    for i in range(count):
        (sym, ts, _recv, bid, ask, last, _bs, _as, _ls, fl, ) = TICK.unpack_from(
            payload, i * TICK.size)
        ticks.append((sym.rstrip(b"\0").decode(), ts, bid, ask, last, fl))
    off = count * TICK.size
    (missing_count,) = U32.unpack_from(payload, off)
    missing = []
    for i in range(missing_count):
        lo, hi = RANGE.unpack_from(payload, off + 4 + i * RANGE.size)
        missing.append((lo, hi))

    sym0 = ticks[0][0] if ticks else args.symbol
    print(f"first_ts={ticks[0][1] if ticks else '-'} last_ts={ticks[-1][1] if ticks else '-'}")
    print(f"QUERY sym={sym0} ticks={len(ticks)} missing={len(missing)} "
          f"ranges={','.join(f'{lo}-{hi}' for lo, hi in missing)} has_missing_flag="
          f"{bool(flags & FLAG_HAS_MISSING)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
