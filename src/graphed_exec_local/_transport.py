"""Inter-worker comms backends (M38): concrete :class:`graphed_core.execution.WorkerTransport`
implementations for single-machine executors.

Two interchangeable backends behind one interface — the seam peer reduction and work-stealing ride,
and the seam a future *distributed* executor reuses unchanged:

- :class:`QueueTransport` — **IPC** (the default). Per-endpoint inbox queue + a registry of peers'
  inboxes. ``queue.Queue`` for the in-process / thread-pool case; a :class:`PipeInbox`
  (``multiprocessing.SimpleQueue``, **no feeder thread**) for the process pool (queue-type-agnostic —
  it only uses ``put_nowait`` / ``get_nowait`` / ``get``). Sends are non-blocking off the data path.
- :class:`HttpTransport` — **HTTP** over loopback. Each endpoint runs a tiny stdlib ``http.server`` in
  a daemon thread; ``send`` enqueues to a local outbound buffer that a background sender thread POSTs
  to the destination (so ``send`` stays non-blocking, exactly like the M37 dashboard client). Fully
  exercisable in-process (every endpoint a thread); a real distributed executor swaps the loopback
  registry for remote hosts.

Both honour the contract: ``send`` never blocks the caller, message order/delivery are not
guaranteed, and reduction determinism is the protocol layer's job (it keys by leaf index).
"""

from __future__ import annotations

import contextlib
import pickle
import queue
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

_DEFAULT_MAXSIZE = 10000


class PipeInbox:
    """A cross-process inbox over ``multiprocessing.SimpleQueue``, adapted to the small ``queue.Queue``
    API :class:`QueueTransport` uses (``put_nowait`` / ``get_nowait`` / ``get(timeout)``).

    Why not ``multiprocessing.Queue``: that spawns a background **feeder thread per queue** in every
    process that puts to it, so a peer worker writing to its driver + reduction peers ends up with
    ~``log(N)`` extra threads. With workers ≈ cores (the normal HEP batch slot) those idle-but-scheduled
    threads add context-switch pressure that measurably slows the workers' compute (py-spy: 5 threads/
    worker vs the hub's 1). ``SimpleQueue`` has **no feeder thread** — ``put`` writes the pipe
    synchronously under a lock — so a peer worker holds exactly one thread, like the hub. Our messages
    are tiny and low-volume (O(log N) boundary partials), so the synchronous write never blocks on a
    full pipe; we wait/poll the reader side, which exposes ``poll(timeout)``, for the timed receives."""

    def __init__(self, ctx: Any) -> None:
        self._q = ctx.SimpleQueue()

    def put_nowait(self, item: object) -> None:
        self._q.put(item)  # synchronous pipe write (no feeder); small msgs never fill the pipe buffer

    def get_nowait(self) -> Any:
        if self._q._reader.poll():
            return self._q.get()
        raise queue.Empty

    def get(self, timeout: float | None = None) -> Any:
        if self._q._reader.poll(timeout):
            return self._q.get()
        raise queue.Empty

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._q.close()


class QueueTransport:
    """IPC transport: a per-endpoint inbox queue plus a registry of peers' inboxes. ``queue.Queue``
    (threads / in-process) or :class:`PipeInbox` (processes) — it only calls ``put_nowait`` /
    ``get_nowait`` / ``get``, so the queue type is the caller's choice."""

    def __init__(self, address: str, inbox: Any, outboxes: dict[str, Any]) -> None:
        self.address = address
        self._inbox = inbox
        self._outboxes = outboxes  # peer address -> that peer's inbox queue

    def peers(self) -> tuple[str, ...]:
        return tuple(self._outboxes)

    def send(self, dest: str, message: object) -> bool:
        q = self._outboxes.get(dest)
        if q is None:
            return False
        try:
            q.put_nowait((self.address, message))
            return True
        except queue.Full:
            return False  # best-effort: a full inbox drops, never back-pressures the sender

    def broadcast(self, message: object) -> None:
        for dest in self._outboxes:
            self.send(dest, message)

    def poll(self) -> list[tuple[str, object]]:
        out: list[tuple[str, object]] = []
        while True:
            try:
                out.append(self._inbox.get_nowait())
            except queue.Empty:
                return out

    def recv(self, timeout: float | None = None) -> tuple[str, object] | None:
        try:
            return self._inbox.get(timeout=timeout) if timeout is not None else self._inbox.get_nowait()  # type: ignore[no-any-return]
        except queue.Empty:
            return None

    def close(self) -> None:
        # queue.Queue needs no teardown; a multiprocessing.Queue is closed by its owner (the driver).
        pass


def build_ipc_transports(
    addresses: tuple[str, ...], *, maxsize: int = _DEFAULT_MAXSIZE
) -> dict[str, QueueTransport]:
    """Build a fully-connected set of in-process IPC transports (one ``queue.Queue`` inbox each).

    Used for the thread pool and the conformance tests; the process pool builds the same class over
    ``multiprocessing.Queue`` inboxes handed to workers via the pool initializer (P5)."""
    inboxes: dict[str, queue.Queue[Any]] = {addr: queue.Queue(maxsize=maxsize) for addr in addresses}
    return {
        addr: QueueTransport(addr, inboxes[addr], {p: inboxes[p] for p in addresses if p != addr})
        for addr in addresses
    }


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        with contextlib.suppress(Exception):
            sender, message = pickle.loads(body)
            self.server._deliver((sender, message))  # type: ignore[attr-defined]
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args: Any) -> None:  # silence the default stderr access log
        pass


class _InboxServer(ThreadingHTTPServer):
    # threaded + a deep listen backlog so concurrent worker POSTs are never refused: a refused POST
    # would *drop* a partial, and a dropped partial (unlike dropped telemetry) stalls the reduction.
    daemon_threads = True
    request_queue_size = 256

    def __init__(self, addr: tuple[str, int]) -> None:
        super().__init__(addr, _Handler)
        self._inbox: deque[tuple[str, object]] = deque()
        self._lock = threading.Lock()

    def _deliver(self, item: tuple[str, object]) -> None:
        with self._lock:
            self._inbox.append(item)

    def pop_one(self) -> tuple[str, object] | None:
        with self._lock:
            return self._inbox.popleft() if self._inbox else None

    def drain(self) -> list[tuple[str, object]]:
        with self._lock:
            out = list(self._inbox)
            self._inbox.clear()
        return out


class HttpTransport:
    """HTTP transport over loopback. Each endpoint serves an inbox at ``/msg`` and ships outbound
    messages from a background sender thread (so ``send`` is non-blocking). Build a connected set with
    :func:`build_http_transports`, which assigns ports and wires the registry."""

    def __init__(self, address: str, *, host: str = "127.0.0.1", maxsize: int = _DEFAULT_MAXSIZE) -> None:
        self.address = address
        self._server = _InboxServer((host, 0))
        self.host = str(self._server.server_address[0])
        self.port = int(self._server.server_address[1])
        self._registry: dict[str, tuple[str, int]] = {}
        self._out: queue.Queue[tuple[str, object] | None] = queue.Queue(maxsize=maxsize)
        self.deliveries = 0  # POSTs that got a response (witness/diagnostic)
        self.drops = 0  # messages dropped after exhausting retries
        self._stop = threading.Event()
        self._srv_thread = threading.Thread(
            target=self._server.serve_forever, name=f"gx-http-{address}", daemon=True
        )
        self._send_thread = threading.Thread(target=self._sender, name=f"gx-http-send-{address}", daemon=True)
        self._srv_thread.start()
        self._send_thread.start()

    def set_registry(self, registry: dict[str, tuple[str, int]]) -> None:
        self._registry = dict(registry)

    def peers(self) -> tuple[str, ...]:
        return tuple(a for a in self._registry if a != self.address)

    def send(self, dest: str, message: object) -> bool:
        if dest not in self._registry or dest == self.address:
            return False
        try:
            self._out.put_nowait((dest, message))  # non-blocking; the sender thread does the POST
            return True
        except queue.Full:
            return False

    def broadcast(self, message: object) -> None:
        for dest in self.peers():
            self.send(dest, message)

    def poll(self) -> list[tuple[str, object]]:
        return self._server.drain()

    def recv(self, timeout: float | None = None) -> tuple[str, object] | None:
        # the inbox is server-thread-fed; pop ONE (leaving the rest queued — draining all and
        # returning one would silently drop the rest), with a short sleep up to `timeout`.
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            item = self._server.pop_one()
            if item is not None:
                return item
            if deadline is not None and time.monotonic() >= deadline:
                return None
            self._stop.wait(0.005)
            if self._stop.is_set():
                return None

    def _sender(self) -> None:
        while not self._stop.is_set():
            try:
                item = self._out.get(timeout=0.1)
            except queue.Empty:
                continue
            if item is None:
                break
            dest, message = item
            target = self._registry.get(dest)
            if target is None:
                continue
            body = pickle.dumps((self.address, message))
            url = f"http://{target[0]}:{target[1]}/msg"
            # the sender thread is off the data path, so it RETRIES on a transient failure: peer
            # reduction needs delivery (a lost partial = wrong/missing result), and the receiver
            # dedupes by node identity, so an at-least-once retry is safe.
            delivered = False
            for attempt in range(5):
                try:
                    req = urllib.request.Request(url, data=body, method="POST")
                    urllib.request.urlopen(req, timeout=5).close()
                    delivered = True
                    break
                except (urllib.error.URLError, OSError):
                    if self._stop.is_set():
                        break
                    time.sleep(0.01 * (attempt + 1))
            if delivered:
                self.deliveries += 1
            else:
                self.drops += 1

    def close(self) -> None:
        self._stop.set()
        with contextlib.suppress(Exception):
            self._out.put_nowait(None)
        with contextlib.suppress(Exception):
            self._server.shutdown()
        with contextlib.suppress(Exception):
            self._server.server_close()


def build_http_transports(addresses: tuple[str, ...]) -> dict[str, HttpTransport]:
    """Build a connected set of loopback HTTP transports: bind each endpoint's server (so it has a
    port), then hand every endpoint the full ``{address: (host, port)}`` registry."""
    transports = {addr: HttpTransport(addr) for addr in addresses}
    registry = {addr: (t.host, t.port) for addr, t in transports.items()}
    for t in transports.values():
        t.set_registry(registry)
    return transports


def build_transports(kind: str, addresses: tuple[str, ...]) -> dict[str, Any]:
    """Factory: ``"ipc"`` -> :func:`build_ipc_transports`, ``"http"`` -> :func:`build_http_transports`."""
    if kind == "ipc":
        return build_ipc_transports(addresses)
    if kind == "http":
        return build_http_transports(addresses)
    raise ValueError(f"unknown transport kind {kind!r} (expected 'ipc' or 'http')")
