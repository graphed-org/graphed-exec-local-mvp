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

import atexit
import contextlib
import hashlib
import multiprocessing
import os
import pickle
import queue
import threading
import time
from collections import OrderedDict, deque
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
from multiprocessing.managers import SyncManager
from typing import TypeVar, cast

from graphed_core import ExecContext, ExecResult, Partition, Plan, StopReason, Task
from graphed_core.execution import (
    LocalResources,
    Monitor,
    TaskEvent,
    TaskPhase,
    WorkerProfiler,
    emit_task,
    partition_label,
)
from graphed_debug import StageError

from ._reduce import plan_tree, running_fold, tree_reduce

R = TypeVar("R")

# ---- per-worker resources --------------------------------------------------
_thread_local = threading.local()
_proc_resources: LocalResources | None = None
# M37: a worker->driver side channel for dashboard events (a bounded queue proxy, set by the
# process pool initializer) + a per-worker statistical profiler. Both are None unless a Monitor is
# attached. Emission is best-effort and MUST NOT block a worker (drop-on-full).
_proc_event_q: object | None = None
_proc_profiler: WorkerProfiler | None = None
_MISSING = object()

# M37 (telemetry OFF the data path): a worker emits events into a local in-process buffer (a deque
# append, ~0.1us) instead of doing a Manager().Queue() put per task (~21us IPC round-trip, ~35x
# slower). A per-worker daemon thread drains the buffer on a cadence and ships the whole batch with
# ONE Manager put, so IPC latency + batching never touch the task's critical path. The buffer is
# bounded (drops oldest if the drain can't keep up — best-effort, never blocks the worker).
_proc_buffer: deque[tuple[str, object]] | None = None
_proc_drain_stop: threading.Event | None = None
_proc_drain_thread: threading.Thread | None = None
_proc_last_flush = 0.0
_PROC_DRAIN_INTERVAL = 0.05  # seconds between worker->driver batch flushes
_PROC_BUFFER_CAP = 50000  # bounded local buffer (drop-oldest on overflow)
_PROFILE_FLUSH_INTERVAL = 1.0  # serialize the profiler at most ~1/s, never per task

# M37: every statistical profiler we start (thread-local or per-process) is registered here so a
# single atexit handler can stop it. pyinstrument's sampler timer otherwise outlives the worker and
# raises "'NoneType' object is not callable" as module globals are torn down at interpreter exit.
_live_profilers: list[WorkerProfiler] = []
_profiler_lock = threading.Lock()
_atexit_registered = False


def _register_profiler(profiler: WorkerProfiler) -> None:
    global _atexit_registered
    with _profiler_lock:
        _live_profilers.append(profiler)
        if not _atexit_registered:
            atexit.register(_stop_all_profilers)
            _atexit_registered = True


def _stop_all_profilers() -> None:
    with _profiler_lock:
        for profiler in _live_profilers:
            with contextlib.suppress(Exception):
                profiler.stop()
        _live_profilers.clear()


# M31: a process callable embedding a large compiled IR would otherwise be re-pickled and
# re-shipped on EVERY submit (concurrent.futures does not dedupe callables). Instead it is
# broadcast to each worker ONCE, cached here by content hash, and tasks ship only (token,
# partition). The cache is keyed by hash so re-running the same plan reuses the cached process.
# capacity of the per-worker shared-process cache AND the driver's broadcast-token set --
# the SAME value on both sides, FIFO-evicted in broadcast order (identical across workers
# because every worker receives the same broadcast sequence), so "the driver thinks token T
# is primed" stays equivalent to "every worker has T".
_SHARED_CACHE_CAP = 32
_shared_objects: OrderedDict[str, object] = OrderedDict()


def _thread_resources() -> LocalResources:
    res = getattr(_thread_local, "res", None)
    if res is None:
        res = LocalResources()
        _thread_local.res = res
    return res


def _proc_init(
    profiler_factory: Callable[[], WorkerProfiler] | None = None,
    event_q: object | None = None,
) -> None:
    global _proc_resources, _proc_event_q, _proc_profiler, _proc_buffer
    global _proc_drain_stop, _proc_drain_thread
    _proc_resources = LocalResources()
    _proc_event_q = event_q
    if profiler_factory is not None:
        with contextlib.suppress(Exception):  # a profiler that won't start just disables sampling
            prof = profiler_factory()
            prof.start()
            _proc_profiler = prof
            _register_profiler(prof)  # stopped at worker-process exit (silences sampler teardown)
    if event_q is not None:  # a monitor is attached -> run the off-path drain thread for this worker
        _proc_buffer = deque(maxlen=_PROC_BUFFER_CAP)
        _proc_drain_stop = threading.Event()
        _proc_drain_thread = threading.Thread(
            target=_proc_drain_loop, name="graphed-dash-worker-drain", daemon=True
        )
        _proc_drain_thread.start()
        atexit.register(_proc_drain_final)


def _proc_emit(item: tuple[str, object]) -> None:
    """Append an event to this worker's local buffer (in-process, ~0.1us). The drain thread ships it;
    the data path never blocks on IPC. The bounded deque drops the oldest on overflow (best-effort)."""
    buf = _proc_buffer
    if buf is not None:
        buf.append(item)


def _proc_drain_batch() -> None:
    """Move everything currently buffered to the driver in ONE Manager-queue put (amortizes IPC)."""
    buf, q = _proc_buffer, _proc_event_q
    if buf is None or q is None:
        return
    batch: list[tuple[str, object]] = []
    while True:
        try:
            batch.append(buf.popleft())
        except IndexError:
            break
    if batch:
        with contextlib.suppress(Exception):  # a full driver queue drops the batch; never blocks
            q.put_nowait(("batch", batch))  # type: ignore[attr-defined]


def _proc_drain_loop() -> None:
    stop = _proc_drain_stop
    assert stop is not None
    while not stop.is_set():
        stop.wait(_PROC_DRAIN_INTERVAL)
        _proc_drain_batch()


def _proc_drain_final() -> None:
    """At worker exit: stop sampling (same thread that started it), buffer the final session, and
    flush whatever remains so the last events/profile are not lost."""
    if _proc_drain_stop is not None:
        _proc_drain_stop.set()
    if _proc_profiler is not None:
        with contextlib.suppress(Exception):
            payload = _proc_profiler.stop()
            if payload and _proc_buffer is not None:
                _proc_buffer.append(("profile", (str(os.getpid()), payload)))
    _proc_drain_batch()


def _proc_profile_due() -> bool:
    """True at most once per ``_PROFILE_FLUSH_INTERVAL`` — keeps the (expensive) profiler serialize
    off the per-task path. Called on the worker thread, where pyinstrument is valid."""
    global _proc_last_flush
    now = time.monotonic()
    if now - _proc_last_flush >= _PROFILE_FLUSH_INTERVAL:
        _proc_last_flush = now
        return True
    return False


def _run_with_emit(
    process: Callable[[Partition, LocalResources], object],
    task: Task,
    resources: LocalResources,
    *,
    worker: str,
    emit_event: Callable[[TaskEvent], None],
    emit_profile: Callable[[str, bytes], None],
    profiler: WorkerProfiler | None,
    profile_due: Callable[[], bool],
) -> object:
    """Run one task, emitting STARTED before and FINISHED/ERRORED after (worker-side timing).
    ``emit_event`` is cheap (a local buffer append for processes, an in-process enqueue for threads);
    the (expensive) profiler serialize runs only when ``profile_due()`` says so (time-throttled, off
    the per-task path). SUBMITTED is emitted driver-side. Emission is best-effort."""
    label = partition_label(task.partition)
    n = task.partition.n_entries
    emit_event(TaskEvent(TaskPhase.STARTED, task.key, worker, time.perf_counter(), label, n))
    try:
        result = process(task.partition, resources)
    except BaseException as exc:
        emit_event(
            TaskEvent(
                TaskPhase.ERRORED, task.key, worker, time.perf_counter(), label, n, error=_render_error(exc)
            )
        )
        raise
    emit_event(TaskEvent(TaskPhase.FINISHED, task.key, worker, time.perf_counter(), label, n))
    if profiler is not None and profile_due():
        with contextlib.suppress(Exception):
            payload = profiler.flush()
            if payload:
                emit_profile(worker, payload)
    return result


def _render_error(exc: BaseException) -> str:
    """A concise, picklable error summary for the dashboard. A graphed-debug ``StageError`` already
    str()s to a user-source-mapped message (op + analysis line), so its own text is the best summary;
    anything else gets a plain ``Type: message``."""
    if isinstance(exc, StageError):
        return str(exc)
    return f"{type(exc).__name__}: {exc}"


def _thread_profiler(monitor: Monitor | None) -> WorkerProfiler | None:
    """A per-thread profiler, built once from the monitor's factory and reused for this worker
    thread's later tasks. ``None`` when no monitor or no factory (MVP tier)."""
    if monitor is None:
        return None
    cached = getattr(_thread_local, "prof", _MISSING)
    if cached is not _MISSING:
        return cast("WorkerProfiler | None", cached)
    prof: WorkerProfiler | None = None
    with contextlib.suppress(Exception):
        factory = monitor.worker_profiler_factory()
        if factory is not None:
            prof = factory()
            prof.start()
            _register_profiler(prof)  # stopped at process exit (silences sampler teardown)
    _thread_local.prof = prof
    return prof


def _thread_profile_due() -> bool:
    """Per-thread time throttle for the profiler serialize (see :func:`_proc_profile_due`)."""
    now = time.monotonic()
    last = cast("float", getattr(_thread_local, "last_flush", 0.0))
    if now - last >= _PROFILE_FLUSH_INTERVAL:
        _thread_local.last_flush = now
        return True
    return False


def _thread_task(
    process: Callable[[Partition, LocalResources], object], task: Task, monitor: Monitor | None
) -> object:
    def emit_event(ev: TaskEvent) -> None:
        emit_task(monitor, ev)  # in-process enqueue; the NetworkMonitor's sender thread does the I/O

    def emit_profile(worker: str, payload: bytes) -> None:
        if monitor is not None:
            with contextlib.suppress(Exception):
                monitor.on_profile(worker, payload)

    return _run_with_emit(
        process,
        task,
        _thread_resources(),
        worker=threading.current_thread().name,
        emit_event=emit_event,
        emit_profile=emit_profile,
        profiler=_thread_profiler(monitor),
        profile_due=_thread_profile_due,
    )


def _prime_shared(token: str, payload: bytes) -> int:
    """Cache the broadcast process under ``token`` and return this worker's pid so the driver
    can confirm coverage. Bounded FIFO (cap ``_SHARED_CACHE_CAP``), evicting the oldest
    broadcast -- identical across workers because every worker sees the same broadcast order,
    so it stays in lockstep with the driver's token set. The brief hold makes a worker keep
    this task long enough for its siblings to each claim one (single-round coverage)."""
    if token not in _shared_objects:  # idempotent: re-priming an existing token does not reorder
        _shared_objects[token] = pickle.loads(payload)
        while len(_shared_objects) > _SHARED_CACHE_CAP:
            _shared_objects.popitem(last=False)
    time.sleep(0.002)
    return os.getpid()


def _proc_task_shared(token: str, task: Task) -> object:
    assert _proc_resources is not None  # set by the pool initializer
    process = cast("Callable[[Partition, LocalResources], object]", _shared_objects[token])

    def emit_event(ev: TaskEvent) -> None:
        _proc_emit(("task", ev))  # local buffer append; the drain thread ships it off-path

    def emit_profile(worker: str, payload: bytes) -> None:
        _proc_emit(("profile", (worker, payload)))

    return _run_with_emit(
        process,
        task,
        _proc_resources,
        worker=str(os.getpid()),
        emit_event=emit_event,
        emit_profile=emit_profile,
        profiler=_proc_profiler,
        profile_due=_proc_profile_due,
    )


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
        monitor: Monitor | None = None,
    ):
        self.max_workers = max_workers if max_workers is not None else (os.cpu_count() or 1)
        self._on_combine = on_combine  # test hook: called per tree-reduce combine with #leaves so far
        self._pooled_combines = pooled_combines
        # persistent=True keeps ONE pool across run() calls (amortizing the import-heavy spawn
        # over many plans — notebooks, sweeps); the default stays a fresh pool per run.
        self._persistent = persistent
        self._kept_pool: _PoolExecutor | None = None
        self._broadcast_tokens: OrderedDict[str, None] = (
            OrderedDict()
        )  # M31/M34: primed tokens, FIFO-bounded in lockstep with each worker cache
        self.monitor = monitor  # M37: a passive dashboard observer (None => no instrumentation)

    def _pool(self) -> _PoolExecutor:
        raise NotImplementedError

    def _raw_submit(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        """Deliver ``process`` to the pool's workers as needed and return ``submit(task)``.
        Threads share memory (no delivery); processes broadcast the process once (M31)."""
        raise NotImplementedError

    def _prepare(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        """Wrap the subclass submit with a driver-side SUBMITTED emission (M37). Worker-side STARTED/
        FINISHED/ERRORED come from the worker entry; here we record the moment of submission."""
        raw = self._raw_submit(pool, process)
        monitor = self.monitor
        if monitor is None:
            return raw

        def submit(task: Task) -> Future[object]:
            emit_task(
                monitor,
                TaskEvent(
                    TaskPhase.SUBMITTED,
                    task.key,
                    "driver",
                    time.perf_counter(),
                    partition_label(task.partition),
                    task.partition.n_entries,
                ),
            )
            return raw(task)

        return submit

    def _combine_cb(self, leaves_done: int) -> None:
        """The tree-reduce combine callback: the legacy test hook AND the dashboard monitor (M37)."""
        if self._on_combine is not None:
            self._on_combine(leaves_done)
        if self.monitor is not None:
            with contextlib.suppress(Exception):
                self.monitor.on_combine(leaves_done)

    @contextlib.contextmanager
    def _acquired_pool(self) -> Iterator[_PoolExecutor]:
        if not self._persistent:
            self._broadcast_tokens = OrderedDict()  # a fresh pool: nothing is primed yet
            with self._pool() as pool:
                yield pool
            return
        if self._kept_pool is None:
            self._broadcast_tokens = OrderedDict()  # newly (re)spawned workers hold no cache
            self._kept_pool = self._pool()
        yield self._kept_pool  # kept alive for the next run()

    def close(self) -> None:
        """Release a persistent pool (idempotent); a later run() lazily respawns."""
        if self._kept_pool is not None:
            self._kept_pool.shutdown(wait=True)
            self._kept_pool = None
            self._broadcast_tokens = OrderedDict()  # respawned workers will need re-priming

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
            node_of: dict[Future[object], int] = {submit(t): i for i, t in enumerate(tasks)}
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
                            self._combine_cb(leaves_done)
        return ExecResult(ready[root], n, n_combines, StopReason.EXHAUSTED)

    def _run_fixed(self, plan: Plan[R]) -> ExecResult[R]:
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        with self._acquired_pool() as pool:
            submit = self._prepare(pool, plan.process)
            leaf_of: dict[Future[object], int] = {submit(t): i for i, t in enumerate(tasks)}

            def completed() -> Iterator[tuple[int, R]]:
                for fut in as_completed(leaf_of):
                    yield leaf_of[fut], cast(R, fut.result())  # fut.result() re-raises a worker error

            value, n_combines = tree_reduce(
                n, completed(), plan.combine, plan.empty, on_combine=self._combine_cb
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
                    fut = submit(task)
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

    def _raw_submit(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        return lambda task: pool.submit(_thread_task, process, task, self.monitor)


class ProcessExecutor(_BaseExecutor):
    """Process-pool executor: spawn-based worker processes (cross-platform + free-threaded-safe), each
    with its own ``open_once`` resources. The Plan ``process`` callable and its partials must be
    picklable; a remote ``StageError`` round-trips to the driver intact.

    M37: when a :class:`Monitor` is attached, workers cannot reach the driver's monitor object, so
    they push events onto a bounded ``Manager().Queue()`` that a driver-side **collector daemon
    thread** drains and replays into the monitor. The queue is best-effort (drop-on-full) so
    instrumentation never back-pressures a worker."""

    def __init__(
        self,
        max_workers: int | None = None,
        *,
        on_combine: Callable[[int], None] | None = None,
        pooled_combines: bool = False,
        persistent: bool = False,
        monitor: Monitor | None = None,
    ):
        super().__init__(
            max_workers,
            on_combine=on_combine,
            pooled_combines=pooled_combines,
            persistent=persistent,
            monitor=monitor,
        )
        self._mgr: SyncManager | None = None
        self._event_q: object | None = None
        self._collector: threading.Thread | None = None
        self._collector_stop: threading.Event | None = None

    def _pool(self) -> _PoolExecutor:
        factory = self.monitor.worker_profiler_factory() if self.monitor is not None else None
        return ProcessPoolExecutor(
            max_workers=self.max_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_proc_init,
            initargs=(factory, self._event_q),
        )

    def _raw_submit(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        payload = pickle.dumps(process)  # pickled ONCE; the bytes are reused for the broadcast
        token = hashlib.sha256(payload).hexdigest()
        self._broadcast(pool, token, payload)
        return lambda task: pool.submit(_proc_task_shared, token, task)

    # ---- M37 dashboard collector (worker side-channel -> driver monitor) ----

    @contextlib.contextmanager
    def _acquired_pool(self) -> Iterator[_PoolExecutor]:
        self._ensure_collector()
        try:
            with super()._acquired_pool() as pool:
                yield pool
        finally:
            # The collector's lifecycle follows the POOL, not the run. Non-persistent: the pool is
            # shut down above, so drain + stop now. Persistent: workers stay alive and keep draining
            # their buffers *after* run() returns (emission is off-path/async), so the collector must
            # stay alive across runs or those trailing events are lost — close() stops it.
            if not self._persistent:
                self._stop_collector()

    def _ensure_collector(self) -> None:
        if self.monitor is None:
            return
        if self._mgr is None:  # one manager + queue for this executor's lifetime
            self._mgr = multiprocessing.get_context("spawn").Manager()
            self._event_q = self._mgr.Queue(maxsize=10000)
        if self._collector is None or not self._collector.is_alive():  # idempotent across runs
            self._collector_stop = threading.Event()
            self._collector = threading.Thread(
                target=self._collect_loop, name="graphed-dash-collector", daemon=True
            )
            self._collector.start()

    def _collect_loop(self) -> None:
        q = self._event_q
        stop = self._collector_stop
        assert q is not None and stop is not None
        while not stop.is_set():
            try:
                item = q.get(timeout=0.05)  # type: ignore[attr-defined]
            except queue.Empty:
                continue
            self._dispatch(item)
        while True:  # final drain after the pool's work has settled
            try:
                item = q.get_nowait()  # type: ignore[attr-defined]
            except queue.Empty:
                break
            self._dispatch(item)

    def _dispatch(self, item: tuple[str, object]) -> None:
        monitor = self.monitor
        if monitor is None:
            return
        kind, payload = item
        if kind == "batch":  # a worker drain thread coalesces many events into one Manager put
            for sub in cast("list[tuple[str, object]]", payload):
                self._dispatch(sub)
            return
        with contextlib.suppress(Exception):
            if kind == "task":
                monitor.on_task(cast("TaskEvent", payload))
            elif kind == "profile":
                worker, data = cast("tuple[str, bytes]", payload)
                monitor.on_profile(worker, data)

    def _stop_collector(self) -> None:
        if self._collector is not None and self._collector_stop is not None:
            self._collector_stop.set()
            self._collector.join(timeout=5.0)
            self._collector = None

    def close(self) -> None:
        super().close()
        self._stop_collector()
        if self._mgr is not None:
            self._mgr.shutdown()
            self._mgr = None
            self._event_q = None

    def _broadcast(self, pool: _PoolExecutor, token: str, payload: bytes) -> None:
        """Prime every worker with ``payload`` exactly once. concurrent.futures exposes no worker
        identity, so we submit priming tasks (each holds briefly, then returns its pid) until the
        set of pids covers the whole pool — the prime is idempotent, so extra hits are harmless."""
        if token in self._broadcast_tokens:
            self._broadcast_tokens.move_to_end(token)  # idempotent; keep recency
            return
        target = self.max_workers  # concrete (resolved in __init__) -- no private pool attribute
        seen: set[int] = set()
        rounds = 0
        while len(seen) < target and rounds < 1000:
            batch = [pool.submit(_prime_shared, token, payload) for _ in range(target - len(seen))]
            seen.update(f.result() for f in batch)  # a dead worker surfaces as BrokenProcessPool here
            rounds += 1
        if len(seen) < target:  # never silently mark a token primed without full coverage (P1-3)
            raise RuntimeError(
                f"broadcast reached only {len(seen)}/{target} workers after {rounds} rounds; "
                "refusing to cache an under-primed process (would KeyError on an unprimed worker)"
            )
        self._broadcast_tokens[token] = None
        while len(self._broadcast_tokens) > _SHARED_CACHE_CAP:  # FIFO, lockstep with the workers
            self._broadcast_tokens.popitem(last=False)
