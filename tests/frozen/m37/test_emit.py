"""M37 frozen suite (graphed-exec-local slice): both reference executors EMIT dashboard events, and
do so **passively** — attaching a monitor never changes the reduced result, and a misbehaving
monitor never breaks the run. Worker-side STARTED/FINISHED/ERRORED + driver-side SUBMITTED.

Pinned to ``comms=None`` (the hub monitor seam) since M38: M37 is the hub-path dashboard milestone,
so these assertions exercise the hub collector. Peer-path emission parity is covered by m38
(``test_peer_robustness``). The pin changes only the transport, never an assertion.
"""

from __future__ import annotations

import collections
import threading

import pytest
from graphed_core import Partition, Plan, Task, TaskEvent, TaskPhase
from probe import add, boom, count_entries

from graphed_exec_local.executors import ProcessExecutor, ThreadExecutor

EXECUTORS = [("thread", ThreadExecutor), ("process", ProcessExecutor)]


class Recorder:
    """A thread-safe driver-side monitor (on_task fires from worker threads / the collector thread)."""

    def __init__(self) -> None:
        self.events: list[TaskEvent] = []
        self.combines = 0
        self._lock = threading.Lock()

    def on_task(self, event: TaskEvent) -> None:
        with self._lock:
            self.events.append(event)

    def on_profile(self, worker: str, payload: bytes) -> None:
        pass

    def on_combine(self, leaves_done: int) -> None:
        with self._lock:
            self.combines += 1

    def worker_profiler_factory(self) -> None:
        return None


def _plan(n: int = 8) -> tuple[list[Task], Plan[int]]:
    tasks = [Task(k, Partition(f"f{k}.root", "Events", 0, (k + 1) * 5)) for k in range(n)]
    return tasks, Plan(process=count_entries, combine=add, empty=lambda: 0, tasks=tasks)


@pytest.mark.parametrize("name,Executor", EXECUTORS)
def test_emit_full_phase_sequence(name: str, Executor: type) -> None:
    _, plan = _plan(8)
    rec = Recorder()
    with Executor(max_workers=3, monitor=rec, comms=None) as ex:
        result = ex.run(plan)
    phases = collections.Counter(e.phase for e in rec.events)
    assert phases[TaskPhase.SUBMITTED] == 8
    assert phases[TaskPhase.STARTED] == 8
    assert phases[TaskPhase.FINISHED] == 8
    assert phases[TaskPhase.ERRORED] == 0
    assert sorted(e.key for e in rec.events if e.phase is TaskPhase.FINISHED) == list(range(8))
    assert result.value == sum((k + 1) * 5 for k in range(8))
    # distinct worker labels: a "driver" (SUBMITTED) + >=1 worker
    workers = {e.worker for e in rec.events}
    assert "driver" in workers and len(workers) >= 2


@pytest.mark.parametrize("name,Executor", EXECUTORS)
def test_attaching_a_monitor_is_passive(name: str, Executor: type) -> None:
    _, plan = _plan(8)
    bare = Executor(max_workers=3, comms=None).run(plan)
    rec = Recorder()
    with Executor(max_workers=3, monitor=rec, comms=None) as ex:
        observed = ex.run(plan)
    assert observed.value == bare.value
    assert observed.n_combines == bare.n_combines
    assert observed.n_partitions == bare.n_partitions
    assert rec.combines == bare.n_combines  # on_combine fired once per tree-reduce combine


@pytest.mark.parametrize("name,Executor", EXECUTORS)
def test_errored_task_emits_errored_and_propagates(name: str, Executor: type) -> None:
    tasks = [Task(0, Partition("bad.root", "Events", 0, 10))]
    plan = Plan(process=boom, combine=add, empty=lambda: 0, tasks=tasks)
    rec = Recorder()
    with pytest.raises(ValueError, match="boom"), Executor(max_workers=2, monitor=rec, comms=None) as ex:
        ex.run(plan)
    errored = [e for e in rec.events if e.phase is TaskPhase.ERRORED]
    assert len(errored) >= 1
    assert "boom" in (errored[0].error or "")


@pytest.mark.parametrize("name,Executor", EXECUTORS)
def test_raising_monitor_never_breaks_the_run(name: str, Executor: type) -> None:
    class Bad:
        def on_task(self, event: TaskEvent) -> None:
            raise RuntimeError("monitor exploded")

        def on_profile(self, worker: str, payload: bytes) -> None:
            raise RuntimeError("monitor exploded")

        def on_combine(self, leaves_done: int) -> None:
            raise RuntimeError("monitor exploded")

        def worker_profiler_factory(self) -> None:
            return None

    _, plan = _plan(6)
    with Executor(max_workers=3, monitor=Bad(), comms=None) as ex:
        result = ex.run(plan)
    assert result.value == sum((k + 1) * 5 for k in range(6))
