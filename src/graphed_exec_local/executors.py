"""Reference single-machine executors (plan M7): a thread pool AND a process pool, both running the
same `graphed_core.Plan` to one reduced result.

Both share the driver below; they differ only in the worker pool and how per-worker `open_once`
resources are held (thread-local vs a per-process global set by an initializer). Fixed partition sets
use the deterministic straggler-tolerant tree reduction; an adaptive `next_tasks` plan uses a running
fold over a partition set discovered from observed timings. A worker failure (thread or process)
propagates to the driver intact — in particular a picklable `graphed_debug.StageError` is re-raised
as-is, never degraded to an opaque string (plan A.3 #8).
"""

from __future__ import annotations

import contextlib
import hashlib
import multiprocessing
import os
import pickle
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from concurrent.futures import (
    Executor as _PoolExecutor,
)
from typing import TypeVar, cast

from graphed_core import ExecContext, ExecResult, Partition, Plan, StopReason

from ._reduce import plan_tree, running_fold, tree_reduce
from .resources import LocalResources

R = TypeVar("R")

# ---- per-worker resources --------------------------------------------------
_thread_local = threading.local()
_proc_resources: LocalResources | None = None

# M31: a process callable embedding a large compiled IR would otherwise be re-pickled and
# re-shipped on EVERY submit (concurrent.futures does not dedupe callables). Instead it is
# broadcast to each worker ONCE, cached here by content hash, and tasks ship only (token,
# partition). The cache is keyed by hash so re-running the same plan reuses the cached process.
_shared_objects: dict[str, object] = {}


def _thread_resources() -> LocalResources:
    res = getattr(_thread_local, "res", None)
    if res is None:
        res = LocalResources()
        _thread_local.res = res
    return res


def _proc_init() -> None:
    global _proc_resources
    _proc_resources = LocalResources()


def _thread_task(process: Callable[[Partition, LocalResources], object], partition: Partition) -> object:
    return process(partition, _thread_resources())


def _proc_task(process: Callable[[Partition, LocalResources], object], partition: Partition) -> object:
    assert _proc_resources is not None  # set by the pool initializer
    return process(partition, _proc_resources)


def _prime_shared(token: str, payload: bytes) -> int:
    """Cache the broadcast process under ``token`` (idempotent); return this worker's pid so the
    driver can confirm every worker has been primed. The brief hold makes a worker keep this
    task long enough for its siblings to each claim one, so the driver's pid-coverage loop
    completes in a single round rather than depending on scheduling luck."""
    if token not in _shared_objects:
        _shared_objects[token] = pickle.loads(payload)
    time.sleep(0.002)
    return os.getpid()


def _proc_task_shared(token: str, partition: Partition) -> object:
    assert _proc_resources is not None  # set by the pool initializer
    process = cast("Callable[[Partition, LocalResources], object]", _shared_objects[token])
    return process(partition, _proc_resources)


def _combine_task(combine: Callable[[object, object], object], a: object, b: object) -> object:
    """Pool entry for a pooled combine (module-level so a spawned process can import it)."""
    return combine(a, b)


class _BaseExecutor:
    """Shared driver. Subclasses supply the worker pool + the (picklable) worker entry point.

    ``pooled_combines=True`` (M10) schedules the tree-reduce combines onto the SAME worker pool as
    the leaves, instead of running them serially on the driver thread — for heavy partials (large
    histograms, many partitions) the driver otherwise becomes a serial combine bottleneck. The
    combine pairing is the same fixed `plan_tree`, so results stay bit-identical; with a process
    pool, `plan.combine` must be picklable (module-level), exactly like `plan.process`."""

    def __init__(
        self,
        max_workers: int | None = None,
        *,
        on_combine: Callable[[int], None] | None = None,
        pooled_combines: bool = False,
        persistent: bool = False,
    ):
        self.max_workers = max_workers
        self._on_combine = on_combine  # test hook: called per tree-reduce combine with #leaves so far
        self._pooled_combines = pooled_combines
        # persistent=True keeps ONE pool across run() calls (amortizing the import-heavy spawn
        # over many plans — notebooks, sweeps); the default stays a fresh pool per run.
        self._persistent = persistent
        self._kept_pool: _PoolExecutor | None = None
        self._broadcast_tokens: set[str] = set()  # M31: which processes this pool already has

    def _pool(self) -> _PoolExecutor:
        raise NotImplementedError

    def _prepare(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Partition], Future[object]]:
        """Deliver ``process`` to the pool's workers as needed and return ``submit(partition)``.
        Threads share memory (no delivery); processes broadcast the process once (M31)."""
        raise NotImplementedError

    @contextlib.contextmanager
    def _acquired_pool(self) -> Iterator[_PoolExecutor]:
        if not self._persistent:
            self._broadcast_tokens = set()  # a fresh pool: nothing is primed yet
            with self._pool() as pool:
                yield pool
            return
        if self._kept_pool is None:
            self._broadcast_tokens = set()  # newly (re)spawned workers hold no cache
            self._kept_pool = self._pool()
        yield self._kept_pool  # kept alive for the next run()

    def close(self) -> None:
        """Release a persistent pool (idempotent); a later run() lazily respawns."""
        if self._kept_pool is not None:
            self._kept_pool.shutdown(wait=True)
            self._kept_pool = None
            self._broadcast_tokens = set()  # respawned workers will need re-priming

    def __enter__(self) -> _BaseExecutor:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def run(self, plan: Plan[R]) -> ExecResult[R]:
        if plan.next_tasks is not None:
            return self._run_adaptive(plan)
        if self._pooled_combines:
            return self._run_fixed_pooled(plan)
        return self._run_fixed(plan)

    def _run_fixed_pooled(self, plan: Plan[R]) -> ExecResult[R]:
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        combines, root = plan_tree(n)
        assert root is not None  # n >= 1
        waiting: dict[int, list[int]] = {}  # input node -> combine indices needing it
        remaining: dict[int, set[int]] = {}  # combine index -> still-unready inputs
        for ci, (_out, a, b) in enumerate(combines):
            remaining[ci] = {a, b}
            waiting.setdefault(a, []).append(ci)
            waiting.setdefault(b, []).append(ci)

        with self._acquired_pool() as pool:
            submit = self._prepare(pool, plan.process)
            node_of: dict[Future[object], int] = {submit(t.partition): i for i, t in enumerate(tasks)}
            ready: dict[int, R] = {}
            n_combines = 0
            leaves_done = 0
            while node_of:
                done, _pending = wait(list(node_of), return_when=FIRST_COMPLETED)
                for fut in done:
                    node = node_of.pop(fut)
                    ready[node] = cast(R, fut.result())  # re-raises a worker error intact
                    if node < n:
                        leaves_done += 1
                    for ci in waiting.get(node, ()):
                        remaining[ci].discard(node)
                        if not remaining[ci]:
                            out, a, b = combines[ci]
                            # a<b -> deterministic left/right grouping, same tree as the driver path
                            combine = cast("Callable[[object, object], object]", plan.combine)
                            f2 = pool.submit(_combine_task, combine, ready.pop(a), ready.pop(b))
                            node_of[f2] = out
                            n_combines += 1
                            if self._on_combine is not None:
                                self._on_combine(leaves_done)
        return ExecResult(ready[root], n, n_combines, StopReason.EXHAUSTED)

    def _run_fixed(self, plan: Plan[R]) -> ExecResult[R]:
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        with self._acquired_pool() as pool:
            submit = self._prepare(pool, plan.process)
            leaf_of: dict[Future[object], int] = {submit(t.partition): i for i, t in enumerate(tasks)}

            def completed() -> Iterator[tuple[int, R]]:
                for fut in as_completed(leaf_of):
                    yield leaf_of[fut], cast(R, fut.result())  # fut.result() re-raises a worker error

            value, n_combines = tree_reduce(
                n, completed(), plan.combine, plan.empty, on_combine=self._on_combine
            )
        return ExecResult(value, n, n_combines, StopReason.EXHAUSTED)

    def _run_adaptive(self, plan: Plan[R]) -> ExecResult[R]:
        assert plan.next_tasks is not None
        ctx = ExecContext()
        start = time.perf_counter()
        results: list[tuple[int, R]] = []
        submitted: dict[Future[object], tuple[int, int, float]] = {}  # future -> (key, n_entries, t0)
        stopped: StopReason | None = None
        next_tasks = plan.next_tasks

        with self._acquired_pool() as pool:
            submit = self._prepare(pool, plan.process)

            def refill() -> None:
                batch = next_tasks(ctx)  # DONE == None
                if not batch:
                    return
                for task in batch:
                    fut = submit(task.partition)
                    submitted[fut] = (task.key, task.partition.n_entries, time.perf_counter())

            refill()
            while submitted:
                done, _pending = wait(list(submitted), return_when=FIRST_COMPLETED)
                for fut in done:
                    key, n_entries, t0 = submitted.pop(fut)
                    results.append((key, cast(R, fut.result())))  # re-raises a worker error intact
                    ctx.n_done += 1
                    ctx.events_done += n_entries
                    ctx.last_durations[key] = time.perf_counter() - t0
                ctx.elapsed_s = time.perf_counter() - start
                reason = plan.stop.reason(ctx) if plan.stop else None
                if reason is not None:
                    stopped = reason
                    for fut in submitted:
                        fut.cancel()
                    break
                refill()

        value, n_combines = running_fold(iter(results), plan.combine, plan.empty)
        return ExecResult(value, ctx.n_done, n_combines, stopped or StopReason.EXHAUSTED)


class ThreadExecutor(_BaseExecutor):
    """Thread-pool executor: a thread-safe worker pool with thread-local `open_once` resources."""

    def _pool(self) -> _PoolExecutor:
        return ThreadPoolExecutor(max_workers=self.max_workers)

    def _prepare(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Partition], Future[object]]:
        return lambda partition: pool.submit(_thread_task, process, partition)


class ProcessExecutor(_BaseExecutor):
    """Process-pool executor: spawn-based worker processes (cross-platform + free-threaded-safe), each
    with its own ``open_once`` resources. The Plan ``process`` callable and its partials must be
    picklable; a remote ``StageError`` round-trips to the driver intact."""

    def _pool(self) -> _PoolExecutor:
        return ProcessPoolExecutor(
            max_workers=self.max_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_proc_init,
        )

    def _prepare(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Partition], Future[object]]:
        payload = pickle.dumps(process)  # pickled ONCE; the bytes are reused for the broadcast
        token = hashlib.sha256(payload).hexdigest()
        self._broadcast(pool, token, payload)
        return lambda partition: pool.submit(_proc_task_shared, token, partition)

    def _broadcast(self, pool: _PoolExecutor, token: str, payload: bytes) -> None:
        """Prime every worker with ``payload`` exactly once. concurrent.futures exposes no worker
        identity, so we submit priming tasks (each holds briefly, then returns its pid) until the
        set of pids covers the whole pool — the prime is idempotent, so extra hits are harmless."""
        if token in self._broadcast_tokens:
            return
        target = int(getattr(pool, "_max_workers", self.max_workers or 1))
        seen: set[int] = set()
        rounds = 0
        while len(seen) < target and rounds < 1000:
            batch = [pool.submit(_prime_shared, token, payload) for _ in range(target - len(seen))]
            seen.update(f.result() for f in batch)
            rounds += 1
        self._broadcast_tokens.add(token)
