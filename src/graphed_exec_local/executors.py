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

import multiprocessing
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

from ._reduce import running_fold, tree_reduce
from .resources import LocalResources

R = TypeVar("R")

# ---- per-worker resources --------------------------------------------------
_thread_local = threading.local()
_proc_resources: LocalResources | None = None


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


class _BaseExecutor:
    """Shared driver. Subclasses supply the worker pool + the (picklable) worker entry point."""

    def __init__(self, max_workers: int | None = None, *, on_combine: Callable[[int], None] | None = None):
        self.max_workers = max_workers
        self._on_combine = on_combine  # test hook: called per tree-reduce combine with #leaves so far

    def _pool(self) -> _PoolExecutor:
        raise NotImplementedError

    def _entry(self) -> Callable[..., object]:
        raise NotImplementedError

    def run(self, plan: Plan[R]) -> ExecResult[R]:
        if plan.next_tasks is not None:
            return self._run_adaptive(plan)
        return self._run_fixed(plan)

    def _run_fixed(self, plan: Plan[R]) -> ExecResult[R]:
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        with self._pool() as pool:
            leaf_of: dict[Future[object], int] = {
                pool.submit(self._entry(), plan.process, t.partition): i for i, t in enumerate(tasks)
            }

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

        with self._pool() as pool:

            def refill() -> None:
                batch = next_tasks(ctx)  # DONE == None
                if not batch:
                    return
                for task in batch:
                    fut = pool.submit(self._entry(), plan.process, task.partition)
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

    def _entry(self) -> Callable[..., object]:
        return _thread_task


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

    def _entry(self) -> Callable[..., object]:
        return _proc_task
