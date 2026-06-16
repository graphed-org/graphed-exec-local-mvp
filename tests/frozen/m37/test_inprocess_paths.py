"""M37 frozen suite (graphed-exec-local slice): exercise the worker telemetry paths IN THE MAIN
PROCESS. In normal use these run inside spawned worker processes (where coverage.py cannot observe
them); here we drive the same module functions directly and via a ThreadExecutor, deterministically.

These also pin the **off-the-data-path** design: a worker emit is a local buffer append (no IPC), a
drain thread batches the buffer to the driver, and the profiler serialize is time-throttled."""

from __future__ import annotations

import pickle
import queue
import threading

from graphed_core import Partition, Plan, Task, TaskPhase
from graphed_debug import StageError
from graphed_debug.errors import SourceFrame
from probe import add, count_entries

import graphed_exec_local.executors as ex
from graphed_exec_local.executors import ThreadExecutor


class FakeProfiler:
    """A deterministic WorkerProfiler stand-in (no pyinstrument): flush/stop always yield bytes."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def flush(self) -> bytes | None:
        return b"prof-bytes"

    def stop(self) -> bytes | None:
        self.stopped = True
        return b"prof-bytes"


def fake_factory() -> FakeProfiler:
    return FakeProfiler()


class ProfMonitor:
    def __init__(self) -> None:
        self.events: list[object] = []
        self.profiles: list[tuple[str, bytes]] = []
        self._lock = threading.Lock()

    def on_task(self, event: object) -> None:
        with self._lock:
            self.events.append(event)

    def on_profile(self, worker: str, payload: bytes) -> None:
        with self._lock:
            self.profiles.append((worker, payload))

    def on_combine(self, leaves_done: int) -> None:
        pass

    def worker_profiler_factory(self):  # type: ignore[no-untyped-def]
        return fake_factory


def _reset_globals() -> None:
    if ex._proc_drain_stop is not None:
        ex._proc_drain_stop.set()
    if ex._proc_drain_thread is not None:
        ex._proc_drain_thread.join(timeout=2)
    ex._proc_resources = None
    ex._proc_event_q = None
    ex._proc_profiler = None
    ex._proc_buffer = None
    ex._proc_drain_stop = None
    ex._proc_drain_thread = None
    ex._proc_last_flush = 0.0
    ex._shared_objects.clear()
    with ex._profiler_lock:
        ex._live_profilers.clear()


def _drain_q(q: queue.Queue) -> list:
    items = []
    while True:
        try:
            items.append(q.get_nowait())
        except queue.Empty:
            return items


def test_thread_profiling_runs_profiler_in_process() -> None:
    mon = ProfMonitor()
    tasks = [Task(k, Partition(f"f{k}.root", "Events", 0, (k + 1) * 3)) for k in range(4)]
    plan = Plan(process=count_entries, combine=add, empty=lambda: 0, tasks=tasks)
    with ThreadExecutor(max_workers=2, monitor=mon, comms=None) as e:  # hub thread-emit path (M37)
        result = e.run(plan)
    assert result.value == sum((k + 1) * 3 for k in range(4))
    assert mon.profiles  # the fake profiler flushed (first task is profile-due) -> on_profile fired
    assert sum(1 for ev in mon.events if ev.phase is TaskPhase.FINISHED) == 4  # type: ignore[attr-defined]


def test_render_error_branches() -> None:
    err = StageError(
        op="divide",
        frames=(SourceFrame(filename="a.py", lineno=7),),
        input_forms=(),
        partition="p",
        cause_type="ZeroDivisionError",
        cause_message="x",
        opt_level=1,
    )
    assert ex._render_error(err) == str(err)  # StageError -> its own source-mapped text
    assert ex._render_error(ValueError("z")) == "ValueError: z"  # generic fallback


def test_emit_appends_to_local_buffer_not_ipc() -> None:
    """The data path append is in-process: it touches the buffer, never the Manager queue."""
    try:
        ex._proc_event_q = "a-sentinel-not-a-queue"  # would explode if _proc_emit did IPC
        ex._proc_buffer = ex.deque(maxlen=3)
        ex._proc_emit(("task", "A"))
        ex._proc_emit(("task", "B"))
        assert list(ex._proc_buffer) == [("task", "A"), ("task", "B")]
        # bounded: a 4th over maxlen=3 drops the oldest, never raises/blocks
        ex._proc_emit(("task", "C"))
        ex._proc_emit(("task", "D"))
        assert list(ex._proc_buffer) == [("task", "B"), ("task", "C"), ("task", "D")]
        ex._proc_buffer = None
        ex._proc_emit(("task", "ignored"))  # no buffer -> no-op
    finally:
        _reset_globals()


def test_drain_batch_ships_one_batched_put() -> None:
    try:
        q: queue.Queue = queue.Queue(maxsize=50)
        ex._proc_event_q = q
        ex._proc_buffer = ex.deque(maxlen=100)
        ex._proc_emit(("task", 1))
        ex._proc_emit(("profile", ("w", b"p")))
        ex._proc_drain_batch()
        items = _drain_q(q)
        assert len(items) == 1 and items[0][0] == "batch"  # the whole buffer in ONE put
        assert items[0][1] == [("task", 1), ("profile", ("w", b"p"))]
        assert len(ex._proc_buffer) == 0  # buffer drained
        ex._proc_drain_batch()  # nothing buffered -> no put
        assert _drain_q(q) == []
    finally:
        _reset_globals()


def test_profile_due_is_time_throttled() -> None:
    try:
        ex._proc_last_flush = 0.0
        assert ex._proc_profile_due() is True  # first call after reset is due
        assert ex._proc_profile_due() is False  # immediate second call is throttled
    finally:
        _reset_globals()


def test_proc_init_worker_entry_and_drain_thread() -> None:
    try:
        q: queue.Queue = queue.Queue(maxsize=100)
        ex._proc_init(fake_factory, q)  # starts the per-worker buffer + drain thread + profiler
        assert ex._proc_resources is not None
        assert ex._proc_profiler is not None
        assert ex._proc_drain_thread is not None and ex._proc_drain_thread.is_alive()
        token = "tok"
        ex._prime_shared(token, pickle.dumps(count_entries))
        out = ex._proc_task_shared(token, Task(0, Partition("f.root", "Events", 0, 9)))  # worker entry
        assert out == 9
        ex._proc_drain_final()  # stop the loop, flush final session + remaining buffer
        kinds = [k for item in _drain_q(q) for (k, _payload) in (item[1] if item[0] == "batch" else [item])]
        assert "task" in kinds and "profile" in kinds  # STARTED/FINISHED + a (throttled) profile flush
    finally:
        _reset_globals()


def test_register_and_stop_all_profilers() -> None:
    try:
        prof = FakeProfiler()
        ex._register_profiler(prof)
        ex._stop_all_profilers()
        assert prof.stopped
    finally:
        _reset_globals()
