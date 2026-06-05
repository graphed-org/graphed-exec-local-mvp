"""Thousands of tiny tasks complete with no deadlock, no stall (monotonic progress), no race —
including under the free-threaded interpreter via the experimental CI job (plan M7)."""

from __future__ import annotations

import analyses as A
from graphed_core import Partition, Plan, Task

from graphed_exec_local import ProcessExecutor, ThreadExecutor


def _plan(n: int, process=A.sleep_then_one) -> Plan[int]:
    tasks = [Task(i, Partition("f", "E", i, i + 1)) for i in range(n)]
    return Plan(process=process, combine=A.add_int, empty=A.zero_int, tasks=tasks)


def test_thousands_of_tiny_tasks_complete_without_deadlock_thread() -> None:
    combines: list[int] = []
    ex = ThreadExecutor(max_workers=8, on_combine=combines.append)
    r = ex.run(_plan(2000))
    assert r.value == 2000  # nothing lost or double-counted
    assert r.n_combines == 1999
    # monotonic progress: each on_combine sees a non-decreasing #leaves-delivered (no regression/stall)
    assert combines == sorted(combines)
    assert len(combines) == 1999


def test_repeated_runs_are_race_free_thread() -> None:
    plan = _plan(500)
    ex = ThreadExecutor(max_workers=8)
    assert all(ex.run(plan).value == 500 for _ in range(12))  # identical every time -> no race


def test_many_tasks_process_pool() -> None:
    r = ProcessExecutor(max_workers=4).run(_plan(200, A.one))
    assert r.value == 200 and r.n_combines == 199
