"""IQTS v1 wire format, also used for DATA/GAP records in raw files."""

import hmac
import os
import socket
import struct
import time
import zlib
from collections import namedtuple

AUTH, WELCOME, RESUME, READY, DATA, GAP, ACK_DATA, ACK_GAP, SUPERSEDED = range(1, 10)
TAKEOVER, RECONNECT = 0, 1
MAGIC = b"IQTS"
VERSION = 1
HEADER = struct.Struct("!4sBBHQQII")
MAX_DATA = 4 * 1024 * 1024
MAX_TOKEN = 1024
DEFAULT_PORT = 19781
Frame = namedtuple("Frame", "kind first last payload")


class ProtocolError(ValueError):
    pass


def loopback(host):
    if host == "localhost":
        return "127.0.0.1"
    try:
        address = socket.inet_pton(socket.AF_INET, host)
    except OSError:
        address = b""
    if not address.startswith(b"\x7f"):
        raise ValueError("IQTS requires an IPv4 loopback address (127.0.0.0/8)")
    return host


def read_token():
    token = os.environ.get("TS_IQUANT_TOKEN", "").encode("utf-8")
    if not 1 <= len(token) <= MAX_TOKEN:
        raise ValueError("set TS_IQUANT_TOKEN (1 to {} UTF-8 bytes)".format(MAX_TOKEN))
    return token


def token_matches(expected, supplied):
    return hmac.compare_digest(expected, supplied)


def ack_kind(kind):
    return {DATA: ACK_DATA, GAP: ACK_GAP}[kind]


def _validate(kind, first, last, size):
    if kind == AUTH:
        valid = first == last == 0 and 1 <= size <= MAX_TOKEN
    elif kind == WELCOME:
        valid = first == last == 0 and size == 16
    elif kind in (READY, SUPERSEDED):
        valid = first == last == size == 0
    elif kind == RESUME:
        valid = 0 <= first <= last and size == 17
    elif kind == DATA:
        valid = 1 <= first == last and 1 <= size <= MAX_DATA
    elif kind == ACK_DATA:
        valid = 1 <= first == last and size == 0
    elif kind in (GAP, ACK_GAP):
        valid = 1 <= first <= last and size == 0
    else:
        valid = False
    if not valid:
        raise ProtocolError("invalid IQTS frame type, range or payload length")


def pack(kind, first=0, last=0, payload=b""):
    _validate(kind, first, last, len(payload))
    header = HEADER.pack(MAGIC, VERSION, kind, 0, first, last, len(payload), 0)
    crc = zlib.crc32(payload, zlib.crc32(header[:-4]))
    return header[:-4] + struct.pack("!I", crc) + payload


def parse_header(header):
    magic, version, kind, reserved, first, last, size, crc = HEADER.unpack(header)
    if magic != MAGIC or version != VERSION or reserved != 0:
        raise ProtocolError("unsupported IQTS magic, version or flags")
    _validate(kind, first, last, size)
    return kind, first, last, size, crc


def parse_frame(header, parsed, payload):
    kind, first, last, size, crc = parsed
    if len(payload) != size or zlib.crc32(payload, zlib.crc32(header[:-4])) != crc:
        raise ProtocolError("invalid IQTS frame checksum or length")
    return Frame(kind, first, last, payload)


def _read_exact(sock, size, deadline, on_timeout):
    data = bytearray()
    while len(data) < size:
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("IQTS handshake timed out")
        try:
            chunk = sock.recv(size - len(data))
        except socket.timeout:
            if on_timeout is not None:
                on_timeout()
            elif deadline is None:
                raise
            continue
        if not chunk:
            raise EOFError("IQTS peer closed the connection")
        data.extend(chunk)
    return bytes(data)


def read_frame(sock, deadline=None, on_timeout=None, kinds=None):
    header = _read_exact(sock, HEADER.size, deadline, on_timeout)
    parsed = parse_header(header)
    if kinds is not None and parsed[0] not in kinds:
        raise ProtocolError("unexpected IQTS frame type")
    payload = _read_exact(sock, parsed[3], deadline, on_timeout)
    return parse_frame(header, parsed, payload)


class Watermark:
    """Track the durable DATA prefix and the resolved (DATA or GAP) prefix."""

    def __init__(self, data=0, resolved=0):
        if not 0 <= data <= resolved:
            raise ProtocolError("invalid watermarks")
        self.data = data
        self.resolved = resolved
        self.done = {}

    def complete(self, kind, first, last):
        # Callers admit only exact, non-overlapping offered/staged ranges.
        if kind not in (DATA, GAP) or not self.resolved < first <= last:
            raise ProtocolError("invalid completion range")
        if kind == DATA and first != last:
            raise ProtocolError("DATA completion must name one batch")
        self.done[first] = kind, last
        advanced = []
        while self.resolved + 1 in self.done:
            start = self.resolved + 1
            next_kind, end = self.done.pop(start)
            if next_kind == DATA and self.data == self.resolved:
                self.data = end
            self.resolved = end
            advanced.append(start)
        return advanced
