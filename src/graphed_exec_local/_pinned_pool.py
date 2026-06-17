"""``PinnedProcessPool`` (M38 P7): a process pool of **identity-pinned** workers.

It conforms to the ``concurrent.futures.Executor`` shape — ``submit`` returns a ``Future``,
``shutdown`` + context-manager lifecycle — so it reuses the familiar ergonomics (and gives result /
exception propagation for free). It differs from ``ProcessPoolExecutor`` in exactly the way peer
reduction needs and the stdlib pool cannot provide:

* **Workers are NOT interchangeable.** Worker ``i`` is spawned once, runs ``init_fn(*init_args[i])`` to
  inherit its OWN resources (here: a *bounded* peer-transport subset — its inbox + the outboxes of its
  O(log N) overlay peers), and owns that identity for life. A ``submit`` therefore MUST name its worker
  (``submit(fn, *args, worker=i)``); work is not handed to "any free worker".
* **Per-worker bounded inherited state.** Because each worker is spawned with its own ``init_args``
  (not the pool's shared ``initargs``), the registry a worker inherits is O(log N), so the whole pool
  is O(N log N) — not the O(N²) every-worker-inherits-every-queue of an ``initargs`` registry.

The control channel (per-worker ``submit`` calls) is separate from whatever transport the workers use
among themselves, so peer messages that race ahead of a call simply wait in that transport — no
buffering dance here.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable
from concurrent.futures import Executor, Future
from typing import Any


def _pinned_loop(
    init_fn: Callable[..., None],
    init_args: tuple[Any, ...],
    call_q: Any,
    result_q: Any,
) -> None:
    """The generic pinned-worker body: run the per-worker initializer once (inheriting this worker's
    resources), then serve ``(cid, fn, args)`` calls from this worker's control queue until the
    shutdown sentinel, returning each result (or a picklable error) on the shared result queue."""
    init_fn(*init_args)
    while True:
        item = call_q.get()
        if item is None:  # shutdown sentinel
            return
        cid, fn, args = item
        try:
            result_q.put((cid, True, fn(*args)))
        except BaseException as exc:  # surfaced via the Future in the parent (re-raised on .result())
            try:
                result_q.put((cid, False, exc))
            except Exception:  # an unpicklable cause -> ship a faithful stand-in so the call still fails
                result_q.put((cid, False, RuntimeError(f"{type(exc).__name__}: {exc}")))


class PinnedProcessPool(Executor):
    """An ``Executor`` of identity-pinned worker processes (see module docstring)."""

    def __init__(
        self,
        n_workers: int,
        ctx: Any,
        init_fn: Callable[..., None],
        init_args: list[tuple[Any, ...]],
        *,
        daemon: bool = True,
    ) -> None:
        self._n = n_workers
        self._call = [ctx.SimpleQueue() for _ in range(n_workers)]  # per-worker control (no feeder thread)
        self._result = ctx.SimpleQueue()  # shared worker -> pool
        self._procs = [
            ctx.Process(
                target=_pinned_loop,
                args=(init_fn, init_args[i], self._call[i], self._result),
                daemon=daemon,
            )
            for i in range(n_workers)
        ]
        for p in self._procs:
            p.start()
        self._futures: dict[int, Future[Any]] = {}
        self._cid = 0
        self._lock = threading.Lock()
        self._closed = False
        self._reaper = threading.Thread(target=self._reap, name="graphed-pinned-reaper", daemon=True)
        self._reaper.start()

    def submit(self, fn: Callable[..., Any], /, *args: Any, worker: int, **kwargs: Any) -> Future[Any]:  # type: ignore[override]
        """Run ``fn(*args)`` on the pinned worker ``worker``; return a ``Future``. (No task kwargs —
        ``worker`` is the only keyword, since pinned workers are addressed, not pooled.)"""
        if kwargs:
            raise TypeError("PinnedProcessPool tasks take no keyword arguments")
        if self._closed:
            raise RuntimeError("submit on a shut-down PinnedProcessPool")
        with self._lock:
            cid = self._cid
            self._cid += 1
            fut: Future[Any] = Future()
            self._futures[cid] = fut
        self._call[worker].put((cid, fn, args))
        return fut

    def _reap(self) -> None:
        while True:
            item = self._result.get()
            if item is None:  # shutdown
                return
            cid, ok, value = item
            with self._lock:
                fut = self._futures.pop(cid, None)
            if fut is not None:
                fut.set_result(value) if ok else fut.set_exception(value)

    def workers_alive(self) -> bool:
        """Backstop for a HARD worker crash (no error message shipped): the driver can detect it and
        fail instead of waiting on a Future that will never resolve."""
        return all(p.is_alive() for p in self._procs)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        for q in self._call:
            with contextlib.suppress(Exception):
                q.put(None)  # stop each worker loop
        if wait:
            for p in self._procs:
                p.join(timeout=10)
        for p in self._procs:
            if p.is_alive():
                p.terminate()
        with contextlib.suppress(Exception):
            self._result.put(None)  # stop the reaper thread
        self._reaper.join(timeout=5)  # join it BEFORE closing the result queue it reads (no bad-fd race)
        for q in (*self._call, self._result):  # release the queues' semaphores (no leak at exit)
            with contextlib.suppress(Exception):
                q.close()
