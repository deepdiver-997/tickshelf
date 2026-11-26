"""Check page bounds and cold fallback for the IQTS query workload."""

from contextlib import closing
import sqlite3
from types import SimpleNamespace
import unittest

from iquant_query_bench import KEY, latency, query_window, read_hot, source_time

STREAM = "1234"
SYMBOL = b"600000.SH"


class Stream:
    def __init__(self, sequences, rows):
        self.sequences = list(sequences)
        self.rows = rows
        self.calls = []

    def command(self, command, key, end, start, count_command, count):
        if (command, key, start, count_command) != ("XREVRANGE", KEY, "-", "COUNT"):
            raise AssertionError("unexpected Redis query")
        self.calls.append((end, count))
        eligible = sorted(self.sequences, reverse=True)
        if end != "+":
            if not end.startswith("("):
                raise AssertionError("cursor must exclude the previous page edge")
            eligible = [seq for seq in eligible if seq < int(end[1:].split("-")[0])]
        page = []
        for seq in eligible[:count]:
            fields = [b"source_stream", STREAM.encode(), b"batch", str(seq).encode()]
            for ordinal in range(self.rows):
                index = str(ordinal).encode()
                fields += [b"symbol:" + index, SYMBOL, b"snapshot:" + index, b"{}"]
            page.append((("{}-0".format(seq)).encode(), fields))
        if len(self.calls) == 1:
            self.sequences.append(max(self.sequences) + 1)
        return page


class QueryTests(unittest.TestCase):
    def test_reports_each_requested_percentile(self):
        self.assertEqual(latency(range(1, 101)), {"p50": 50, "p95": 95, "p99": 99})

    def test_pagination_bounds_quotes_and_excludes_new_writes(self):
        redis = Stream([1, 2, 3, 4, 5], 128)
        result = read_hot(redis, STREAM.encode(), SYMBOL, 1, 6, 128, 256, 64)
        self.assertEqual(set(result.frames), {1, 2, 3, 4, 5})
        self.assertEqual(len(result.rows), 640)
        self.assertEqual(result.max_page_quotes, 256)
        self.assertEqual(redis.calls, [("+", 2), ("(4-0", 2), ("(2-0", 2)])

    def test_hybrid_fills_hot_holes_and_cold_runs_without_redis(self):
        redis = Stream([3, 5], 1)
        args = SimpleNamespace(frame_rows=1, redis_page_quotes=2, max_redis_pages=8, rate=1000)
        with closing(sqlite3.connect(":memory:")) as db:
            db.execute("CREATE TABLE quotes (stream, symbol, source_time_ms, batch_seq, ordinal, snapshot)")
            db.executemany("INSERT INTO quotes VALUES (?,?,?,?,?,?)",
                           [(STREAM, SYMBOL.decode(), source_time(i - 1, args.rate),
                             "{:020}".format(i), 0, b"{}") for i in range(1, 7)])
            db.commit()
            hot, metrics, cold_queries = query_window("hot", redis, None, STREAM, SYMBOL, 1, 5, args)
            self.assertEqual(sorted(hot), [(3, 0), (5, 0)])
            self.assertEqual(cold_queries, 0)
            cold, metrics, cold_queries = query_window("cold", None, db, STREAM, SYMBOL, 1, 5, args)
            self.assertIsNone(metrics)
            self.assertEqual(cold_queries, 1)
            mixed, metrics, cold_queries = query_window("hybrid", Stream([3, 5], 1), db,
                                                        STREAM, SYMBOL, 1, 5, args)
            self.assertEqual(sorted(mixed), [(seq, 0) for seq in range(1, 6)])
            self.assertEqual(mixed, cold)
            self.assertEqual(cold_queries, 2)

    def test_returns_partial_when_page_budget_is_exhausted(self):
        result = read_hot(Stream([1, 2, 3], 32), STREAM.encode(), SYMBOL, 1, 3, 32, 32, 1)
        self.assertTrue(result.hit_page_budget)
        self.assertEqual(result.frames, {3})


if __name__ == "__main__":
    unittest.main()
