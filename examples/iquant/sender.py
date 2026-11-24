"""Bounded in-memory replay and one authenticated IQTS session."""

import logging
import socket
import threading
import time
import uuid
from collections import OrderedDict

from iquant import protocol as wire

LOG = logging.getLogger(__name__)


def _shutdown(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


class Window:
    def __init__(self, limit, mark):
        self.limit = limit
        self.mark = mark
        self.next_send = mark.resolved + 1
        self.pending = {}

    def offer(self, kind, first, last):
        self.pending[first] = kind, last
        self.next_send = last + 1

    def ack(self, frame):
        if frame.kind not in (wire.ACK_DATA, wire.ACK_GAP):
            raise wire.ProtocolError("expected an IQTS ACK")
        if frame.last <= self.mark.resolved:
            return
        offered = self.pending.get(frame.first)
        if offered is None or (wire.ack_kind(offered[0]), offered[1]) != (frame.kind, frame.last):
            raise wire.ProtocolError("ACK does not match a frame offered on this session")
        if frame.first not in self.mark.done:
            for first in self.mark.complete(offered[0], frame.first, frame.last):
                del self.pending[first]


class _Session:
    def __init__(self, sock, window):
        self.sock = sock
        self.window = window


class Sender:
    def __init__(self, token, host="127.0.0.1", port=wire.DEFAULT_PORT, window=128,
                 buffer_batches=8192, buffer_bytes=64 * 1024 * 1024, handshake_seconds=3.0,
                 on_ack=None):
        if not isinstance(token, bytes) or not 1 <= len(token) <= wire.MAX_TOKEN:
            raise ValueError("invalid IQTS token length")
        if min(window, buffer_batches) < 1 or buffer_bytes <= wire.HEADER.size:
            raise ValueError("window and buffer sizes must be positive")
        if handshake_seconds <= 0:
            raise ValueError("handshake timeout must be positive")
        if on_ack is not None and not callable(on_ack):
            raise ValueError("on_ack must be callable")
        self.on_ack = on_ack
        self.token = token
        self.window = window
        self.buffer_batches = buffer_batches
        self.buffer_bytes = buffer_bytes
        self.handshake_seconds = handshake_seconds
        self.stream_id = uuid.uuid4().bytes
        self._cv = threading.Condition()
        self._buffer = OrderedDict()
        self._bytes = 0
        self._next_seq = 1
        self._highest_offered = 0
        self._durable_data = 0
        self._resolved = 0
        self._evicted = 0
        self._active = None
        self._owner = None
        self._closed = False
        self._clients = {}
        self._handshakes = threading.BoundedSemaphore(8)
        self._listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                self._listen.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            else:
                self._listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._listen.bind((wire.loopback(host), port))
            self._listen.listen(8)
            self._listen.settimeout(0.25)
            self.address = self._listen.getsockname()
            self._acceptor = threading.Thread(target=self._accept, name="iqts-listener", daemon=True)
            self._acceptor.start()
        except BaseException:
            self._listen.close()
            raise

    def publish(self, payload):
        if not isinstance(payload, bytes) or not 1 <= len(payload) <= wire.MAX_DATA:
            raise ValueError("invalid IQTS DATA payload")
        if len(payload) + wire.HEADER.size > self.buffer_bytes:
            raise ValueError("one batch cannot fit in the configured ring")
        with self._cv:
            if self._closed:
                raise RuntimeError("IQTS sender is closed")
            seq = self._next_seq
            frame = wire.pack(wire.DATA, seq, seq, payload)
            self._next_seq += 1
            self._buffer[seq] = frame
            self._bytes += len(frame)
            while len(self._buffer) > self.buffer_batches or self._bytes > self.buffer_bytes:
                self._pop_first()
                self._evicted += 1
            self._cv.notify_all()
            return seq

    def stats(self):
        with self._cv:
            return {
                "last_batch": self._next_seq - 1,
                "first_retained": self._first_retained(),
                "buffered_batches": len(self._buffer),
                "buffered_bytes": self._bytes,
                "evicted_unresolved_batches": self._evicted,
                "data_watermark": self._durable_data,
                "resume_watermark": self._resolved,
                "connected": self._active is not None,
            }

    def _first_retained(self):
        return next(iter(self._buffer), self._next_seq)

    def _pop_first(self):
        self._bytes -= len(self._buffer.popitem(last=False)[1])

    def _adopt(self, mark):
        self._durable_data = max(self._durable_data, mark.data)
        self._resolved = max(self._resolved, mark.resolved)
        while self._buffer and self._first_retained() <= self._resolved:
            self._pop_first()

    def _accept(self):
        while True:
            try:
                sock, _ = self._listen.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self._cv:
                if self._closed or not self._handshakes.acquire(blocking=False):
                    sock.close()
                    continue
                handler = threading.Thread(target=self._serve, args=(sock,),
                                           name="iqts-session", daemon=True)
                self._clients[sock] = handler
                handler.start()

    def _authenticate(self, sock):
        deadline = time.monotonic() + self.handshake_seconds
        auth = wire.read_frame(sock, deadline=deadline, kinds=(wire.AUTH,))
        if not wire.token_matches(self.token, auth.payload):
            raise wire.ProtocolError("IQTS authentication rejected")
        sock.sendall(wire.pack(wire.WELCOME, payload=self.stream_id))
        resume = wire.read_frame(sock, deadline=deadline, kinds=(wire.RESUME,))
        mode, owner = resume.payload[0], resume.payload[1:]
        if mode not in (wire.TAKEOVER, wire.RECONNECT):
            raise wire.ProtocolError("invalid IQTS resume mode")
        with self._cv:
            if self._closed or resume.last > self._highest_offered:
                raise wire.ProtocolError("invalid IQTS resume request")
            if mode == wire.RECONNECT and owner != self._owner:
                session = None
            else:
                mark = wire.Watermark(resume.first, resume.last)
                session = _Session(sock, Window(self.window, mark))
                old, self._active = self._active, session
                self._owner = owner
                self._adopt(mark)
                self._cv.notify_all()
        if session is None:
            sock.sendall(wire.pack(wire.SUPERSEDED))
            raise wire.ProtocolError("IQTS receiver was superseded")
        if old is not None:
            _shutdown(old.sock)
        return session

    def _serve(self, sock):
        session = writer = None
        try:
            try:
                sock.settimeout(min(0.5, self.handshake_seconds))
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                session = self._authenticate(sock)
            finally:
                self._handshakes.release()
            sock.sendall(wire.pack(wire.READY))
            writer = threading.Thread(target=self._send, args=(session,),
                                      name="iqts-writer", daemon=True)
            writer.start()
            while True:
                ack = wire.read_frame(sock, on_timeout=lambda: None, kinds=(wire.ACK_DATA, wire.ACK_GAP))
                with self._cv:
                    if self._closed or self._active is not session:
                        return
                    fresh = ack.last > session.window.mark.resolved and ack.first not in session.window.mark.done
                    session.window.ack(ack)
                    self._adopt(session.window.mark)
                    self._cv.notify_all()
                if fresh and self.on_ack is not None:
                    try:
                        self.on_ack(self.stream_id, ack.kind, ack.first, ack.last)
                    except Exception:
                        LOG.exception("IQTS ACK callback failed")
        except (OSError, EOFError, wire.ProtocolError) as exc:
            LOG.debug("IQTS session ended: %s", exc)
        finally:
            with self._cv:
                if self._active is session:
                    self._active = None
                self._cv.notify_all()
            _shutdown(sock)
            if writer is not None:
                writer.join()
            sock.close()
            with self._cv:
                self._clients.pop(sock, None)

    def _send(self, session):
        window = session.window
        try:
            while True:
                with self._cv:
                    while True:
                        if self._closed or self._active is not session:
                            return
                        first = window.next_send
                        if len(window.pending) < window.limit and first < self._next_seq:
                            break
                        self._cv.wait()
                    floor = self._first_retained()
                    if first < floor:
                        kind, last = wire.GAP, floor - 1
                        data = wire.pack(kind, first, last)
                    else:
                        kind, last = wire.DATA, first
                        data = self._buffer[first]
                    # Reserve before sendall: an ACK or a takeover may arrive before it returns.
                    window.offer(kind, first, last)
                    self._highest_offered = max(self._highest_offered, last)
                session.sock.sendall(data)
                del data
        except OSError:
            # The stream might contain a partial frame. Always replay on a fresh socket.
            _shutdown(session.sock)

    def close(self):
        with self._cv:
            if self._closed:
                return
            self._closed = True
            clients = list(self._clients.items())
            self._cv.notify_all()
        self._listen.close()
        for sock, _ in clients:
            _shutdown(sock)
        self._acceptor.join()
        for _, handler in clients:
            handler.join()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
