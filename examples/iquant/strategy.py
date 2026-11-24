# coding: utf-8
"""iQuant live strategy entry point. It captures quotes; it never places orders."""

import json
import os
import sys
import threading
import time

# The strategy editor may exec this file without a real __file__ or a package.
if not globals().get("__package__"):
    module_dir = os.environ.get("TS_IQUANT_MODULE_DIR")
    if not module_dir and globals().get("__file__"):
        module_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if module_dir:
        sys.path.insert(0, module_dir)

from iquant import protocol as wire
from iquant.sender import Sender

MARKETS = (
    ("SSE_EQUITIES", "SH"),
    ("SZSE_EQUITIES", "SZ"),
    ("SSE_OPTIONS", "SHO"),
    ("SZSE_OPTIONS", "SZO"),
    ("SHFE_DERIVATIVES", "SF"),
    ("INE_DERIVATIVES", "INE"),
    ("CFFEX_DERIVATIVES", "IF"),
    ("DCE_DERIVATIVES", "DF"),
    ("CZCE_DERIVATIVES", "ZF"),
    ("GFEX_DERIVATIVES", "GF"),
)
_CAPTURE = None


def load_routes(path=None):
    routes = {name: {"markets": [market]} for name, market in MARKETS}
    if path:
        with open(path, encoding="utf-8-sig") as source:
            overrides = json.load(source)
        if not isinstance(overrides, dict) or overrides.keys() - routes.keys():
            raise ValueError("subscription config must use known IQTS coverage group names")
        routes.update(overrides)
    for name, route in routes.items():
        if not isinstance(route, dict) or len(route) != 1:
            raise ValueError("{} must specify one of markets, symbols or sectors".format(name))
        mode, values = next(iter(route.items()))
        if (mode not in ("markets", "symbols", "sectors") or not isinstance(values, list)
                or not values or any(not isinstance(v, str) or not v.strip() for v in values)):
            raise ValueError("{} needs a nonempty list of markets, symbols or sectors".format(name))
    return routes


def subscription_codes(context, route):
    mode, values = next(iter(route.items()))
    if mode == "sectors":
        codes = []
        for name in values:
            try:
                members = context.get_stock_list_in_sector(name)
            except TypeError:
                members = context.get_stock_list_in_sector(name, int(time.time() * 1000))
            if not members or isinstance(members, str):
                raise ValueError("empty or invalid sector: {}".format(name))
            codes.extend(members)
    else:
        codes = values
    if not codes:
        raise ValueError("no subscription codes resolved")
    for code in codes:
        if not isinstance(code, str) or not code or any(c.isspace() for c in code):
            raise ValueError("invalid subscription code: {!r}".format(code))
        symbol, dot, market = code.rpartition(".")
        if (mode == "markets" and dot) or (mode != "markets" and not (symbol and market)):
            raise ValueError("invalid {} subscription code: {!r}".format(mode, code))
    return list(dict.fromkeys(codes))


def _sdk_json(value):
    if type(value).__module__.split(".", 1)[0] == "numpy" and hasattr(value, "tolist"):
        return value.tolist()
    raise TypeError("unsupported SDK value type: {}".format(type(value).__name__))


class Capture:
    def __init__(self, sender, batch_bytes, batch_ticks, report_seconds=30):
        if not 3 <= batch_bytes <= min(wire.MAX_DATA, sender.buffer_bytes - wire.HEADER.size):
            raise ValueError("batch byte limit must fit one DATA frame and the ring")
        if batch_ticks < 1:
            raise ValueError("batch tick limit must be positive")
        self.sender = sender
        self.batch_bytes = batch_bytes
        self.batch_ticks = batch_ticks
        self.report_seconds = report_seconds
        self.context = None
        self.ids = []
        self.observed = {}
        self.rejected = 0
        self._last_report = time.monotonic()
        self._stats_lock = threading.Lock()
        self._closed = False

    def subscribe(self, context, routes, allow_partial=False):
        self.context = context
        method = getattr(context, "subscribe_whole_quote", None)
        if not callable(method):
            raise RuntimeError("iQuant client lacks ContextInfo.subscribe_whole_quote; "
                               "a live whole-quote callback API is required")
        failed = []
        for group, route in routes.items():
            ids = []
            try:
                codes = subscription_codes(context, route)
                callback = self._callback(group)
                for offset in range(0, len(codes), 500):
                    sub_id = method(codes[offset:offset + 500], callback=callback)
                    if type(sub_id) is not int or sub_id < 0:
                        raise RuntimeError("invalid subscription ID: {!r}".format(sub_id))
                    ids.append(sub_id)
            except Exception as exc:
                self._unsubscribe(context, ids)
                failed.append(group)
                print("[IQTS] FAILED {}: {}".format(group, exc))
                continue
            self.ids.extend(ids)
            print("[IQTS] REGISTERED {}: {} targets ({}), coverage unverified".format(
                group, len(codes), next(iter(route))))
        if not self.ids or (failed and not allow_partial):
            raise RuntimeError("IQTS subscription setup incomplete: {}".format(", ".join(failed)))
        if failed:
            print("[IQTS] PARTIAL CAPTURE: {}".format(", ".join(failed)))

    def _callback(self, group):
        def on_quote(data):
            if self._closed:
                return
            if not isinstance(data, dict):
                self._reject(group, "expected a code -> tick dictionary")
                return
            batch = []
            size = 2
            captured = 0
            for symbol, tick in data.items():
                try:
                    if not isinstance(symbol, str) or not symbol or not isinstance(tick, dict):
                        raise TypeError("expected a full instrument code and a tick dictionary")
                    item = json.dumps([symbol, tick], separators=(",", ":"),
                                      default=_sdk_json).encode("utf-8")
                    if len(item) + 2 > self.batch_bytes:
                        raise ValueError("one quote exceeds the configured batch byte limit")
                except (TypeError, ValueError, RecursionError) as exc:
                    self._reject(group, str(exc))
                    continue
                if batch and (size + len(item) + 1 > self.batch_bytes or len(batch) == self.batch_ticks):
                    if not self._publish(batch):
                        return
                    batch, size = [], 2
                size += len(item) + bool(batch)
                batch.append(item)
                captured += 1
            if batch and not self._publish(batch):
                return
            if captured:
                with self._stats_lock:
                    initial = group not in self.observed
                    self.observed[group] = self.observed.get(group, 0) + captured
                if initial:
                    print("[IQTS] FIRST PUSH {}: {} quotes (market signal, not proof of full coverage)".format(
                        group, captured))
        return on_quote

    def _publish(self, batch):
        try:
            self.sender.publish(b"[" + b",".join(batch) + b"]")
        except RuntimeError:
            if not self._closed:
                raise
            return False
        return True

    def _reject(self, group, message):
        with self._stats_lock:
            self.rejected += 1
            count = self.rejected
        if count == 1 or count % 1000 == 0:
            print("[IQTS] rejected SDK quotes={}: {}: {}".format(count, group, message))

    def maybe_report(self):
        now = time.monotonic()
        if now - self._last_report >= self.report_seconds:
            self._last_report = now
            with self._stats_lock:
                observed, rejected = dict(self.observed), self.rejected
            print("[IQTS] sender={} quotes={} rejected={}".format(self.sender.stats(), observed, rejected))

    @staticmethod
    def _unsubscribe(context, ids):
        for sub_id in ids:
            try:
                context.unsubscribe_quote(sub_id)
            except Exception as exc:
                print("[IQTS] unsubscribe {} failed: {}".format(sub_id, exc))

    def close(self):
        self._closed = True
        self._unsubscribe(self.context, self.ids)
        self.ids.clear()
        self.context = None
        self.sender.close()


def init(ContextInfo):
    global _CAPTURE
    stop(ContextInfo)
    if getattr(ContextInfo, "do_back_test", False):
        print("[IQTS] skipped in backtest mode")
        return
    routes = load_routes(os.environ.get("TS_IQUANT_SUBSCRIPTIONS"))
    sender = Sender(wire.read_token(), host=os.environ.get("TS_IQUANT_HOST", "127.0.0.1"),
                    port=int(os.environ.get("TS_IQUANT_PORT", wire.DEFAULT_PORT)),
                    window=int(os.environ.get("TS_IQUANT_WINDOW", 128)),
                    buffer_batches=int(os.environ.get("TS_IQUANT_BUFFER_BATCHES", 8192)),
                    buffer_bytes=int(os.environ.get("TS_IQUANT_BUFFER_BYTES", 64 * 1024 * 1024)))
    capture = None
    try:
        capture = Capture(sender, batch_bytes=int(os.environ.get("TS_IQUANT_BATCH_BYTES", 256 * 1024)),
                          batch_ticks=int(os.environ.get("TS_IQUANT_BATCH_TICKS", 256)))
        capture.subscribe(ContextInfo, routes, allow_partial=os.environ.get("TS_IQUANT_ALLOW_PARTIAL") == "1")
    except BaseException:
        if capture is None:
            sender.close()
        else:
            capture.close()
        raise
    _CAPTURE = capture
    print("[IQTS] listening at {}:{} stream={}".format(*sender.address, sender.stream_id.hex()))


def handlebar(ContextInfo):
    if _CAPTURE and ContextInfo.is_last_bar():
        _CAPTURE.maybe_report()


def stop(ContextInfo):
    global _CAPTURE
    if _CAPTURE is not None:
        capture, _CAPTURE = _CAPTURE, None
        capture.close()
