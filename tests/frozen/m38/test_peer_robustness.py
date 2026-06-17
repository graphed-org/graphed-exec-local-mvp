"""M38 peer robustness (spike; frozen at P6): peer mode is STRICT (never silently falls back to hub
for a feature it can't do) and propagates a worker failure PROMPTLY + intact (the M7 obligation)."""

from __future__ import annotations

import time

import pytest
from graphed_core import Partition, Plan, Task
from graphed_core.execution import TaskPhase
from graphed_debug._sampler import make_worker_profiler, tree_from_bytes

from graphed_exec_local.executors import ProcessExecutor, ThreadExecutor


def _boom(partition: Partition, resources: object) -> int:
    raise ValueError("kaboom")


def _add(a: int, b: int) -> int:
    return a + b


def _zero() -> int:
    return 0


class _Monitor:  # minimal Monitor-shaped object (only its presence matters for the guard)
    def on_task(self, event: object) -> None: ...
    def on_profile(self, worker: str, payload: bytes) -> None: ...
    def on_combine(self, leaves_done: int) -> None: ...
    def worker_profiler_factory(self) -> None:
        return None


@pytest.mark.parametrize("executor_cls", [ThreadExecutor, ProcessExecutor])
def test_peer_refuses_pooled_combines_loudly(executor_cls) -> None:
    # pooled_combines is a hub-only mechanism (peer's whole model is off-driver combines). Rather than
    # silently running hub, peer mode REFUSES — hub-mode must never sneak into a run asked to be peer.
    with pytest.raises(ValueError, match="pooled_combines"):
        executor_cls(max_workers=2, comms="ipc", pooled_combines=True)


@pytest.mark.parametrize("executor_cls", [ThreadExecutor, ProcessExecutor])
def test_peer_accepts_monitor_and_on_combine(executor_cls) -> None:
    # peer now has emission parity, so these are accepted (no raise) — they are NOT silently dropped.
    executor_cls(max_workers=2, comms="ipc", monitor=_Monitor())
    executor_cls(max_workers=2, comms="ipc", on_combine=lambda n: None)


def _count(partition: Partition, resources: object) -> int:
    return partition.n_entries


class _Recorder:
    def __init__(self) -> None:
        self.events: list = []
        self.combines = 0

    def on_task(self, event) -> None:
        self.events.append(event)

    def on_profile(self, worker: str, payload: bytes) -> None: ...
    def on_combine(self, leaves_done: int) -> None:
        self.combines += 1

    def worker_profiler_factory(self) -> None:
        return None


@pytest.mark.parametrize("executor_cls", [ThreadExecutor, ProcessExecutor])
@pytest.mark.parametrize("kind", ["ipc", "http"])
def test_peer_emits_full_monitor_event_parity(executor_cls, kind) -> None:
    # WITNESS the emission parity that lets peer be the default: under peer mode a monitor sees the
    # same per-task lifecycle + combine count as the hub path (so the dashboard works under peer).
    n = 8
    tasks = tuple(Task(k, Partition(f"f{k}.root", "Events", 0, (k + 1) * 5)) for k in range(n))
    plan = Plan(process=_count, combine=_add, empty=_zero, tasks=tasks)
    rec = _Recorder()
    with executor_cls(max_workers=3, comms=kind, monitor=rec) as ex:
        result = ex.run(plan)
    phases = {p: sum(1 for e in rec.events if e.phase is p) for p in TaskPhase}
    assert phases[TaskPhase.SUBMITTED] == n  # driver-side
    assert phases[TaskPhase.STARTED] == n  # worker-side, one per processed leaf
    assert phases[TaskPhase.FINISHED] == n  # ...no trailing events lost
    assert phases[TaskPhase.ERRORED] == 0
    assert rec.combines == result.n_combines == n - 1  # combine count reported
    assert "driver" in {e.worker for e in rec.events}  # SUBMITTED is tagged driver-side
    assert result.value == sum((k + 1) * 5 for k in range(n))


def _spin(partition: Partition, resources: object) -> int:
    # busy on a WALL-CLOCK budget, RELEASING THE GIL each step (``time.sleep``) so the 10ms off-thread
    # sampler — which needs the GIL for ``sys._current_frames()`` — reliably lands samples. A pure-Python
    # spin holds the GIL for the whole budget and can starve the sampler to ZERO samples on a slow/loaded
    # machine (seen on py3.14 macOS/Windows CI); the real analysis releases the GIL in array kernels, so
    # this mimics that. Per root-prompt R0.10a a witness must be a deterministic invariant, not timing.
    end = time.monotonic() + 0.15
    s, i = 0.0, 0
    while time.monotonic() < end:
        s += (i % 7) ** 0.5
        i += 1
        time.sleep(0.001)  # yield the GIL so the sampler thread can read this thread's stack
    return 1


class _ProfRecorder(_Recorder):
    def __init__(self) -> None:
        super().__init__()
        self.profiles: list[bytes] = []

    def on_profile(self, worker: str, payload: bytes) -> None:
        self.profiles.append(payload)

    def worker_profiler_factory(self):
        return make_worker_profiler


# Realistic profiling combos. (ThreadExecutor, "http") is excluded: under the GIL the HTTP transport's
# per-endpoint server + sender threads plus a per-worker sampler thread all contend in one process, so
# the off-thread sampler starves and may land no sample in a short run — a test-timing artifact, not a
# product gap. HTTP is the process/distributed seam; the in-process thread pool's default is IPC.
# (Follow-up: under FREE-THREADED CPython (3.14t) there is no GIL, so those threads run truly in
# parallel and HTTP+threads becomes reasonable to witness — revisit when 3.14t is the norm.)
@pytest.mark.parametrize(
    ("executor_cls", "kind"),
    [(ThreadExecutor, "ipc"), (ProcessExecutor, "ipc"), (ProcessExecutor, "http")],
)
def test_peer_runs_the_profiler_under_a_monitor(executor_cls, kind) -> None:
    # WITNESS profiling parity (so Dashboard(profile=True) is not silently empty under peer): peer
    # workers run the off-thread sampler and ship a non-empty flamegraph tree to the monitor.
    rec = _ProfRecorder()
    tasks = tuple(Task(k, Partition(f"f{k}.root", "Events", 0, 5)) for k in range(8))
    plan = Plan(process=_spin, combine=_add, empty=_zero, tasks=tasks)
    with executor_cls(max_workers=3, comms=kind, monitor=rec) as ex:
        ex.run(plan)
    assert rec.profiles, "peer workers shipped no profile samples"
    assert any(tree_from_bytes(p).get("count", 0) > 0 for p in rec.profiles)  # real samples landed


@pytest.mark.parametrize("executor_cls", [ThreadExecutor, ProcessExecutor])
@pytest.mark.parametrize("kind", ["ipc", "http"])
def test_peer_propagates_worker_error_promptly(executor_cls, kind) -> None:
    # a process() exception means the root never forms; the driver must detect the failed worker and
    # re-raise its error intact, NOT hang until the safety timeout (pytest-timeout would catch a hang).
    tasks = tuple(Task(k, Partition(f"f{k}.root", "Events", k, k + 1)) for k in range(8))
    plan = Plan(process=_boom, combine=_add, empty=_zero, tasks=tasks)
    with pytest.raises(ValueError, match="kaboom"):
        executor_cls(max_workers=4, comms=kind).run(plan)
