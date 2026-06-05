"""Adaptive reshaping via next_tasks + stopping conditions (plan M7)."""

from __future__ import annotations

import analyses as A
from graphed_core import ExecContext, Partition, Plan, StopCondition, StopReason, Task

from graphed_exec_local import ThreadExecutor


def test_probe_driven_reshaping_processes_all_events_correctly() -> None:
    total = 1200
    state = {"emitted": 0, "batches": 0}

    def next_tasks(ctx: ExecContext):
        if state["emitted"] >= total:
            return None  # DONE
        # a small probe first; once we have observed a timing, size the next chunks larger
        size = 100 if not ctx.last_durations else 400
        tasks = []
        while state["emitted"] < total and len(tasks) < 3:
            start = state["emitted"]
            stop = min(total, start + size)
            tasks.append(Task(start, Partition("f", "E", start, stop)))
            state["emitted"] = stop
        state["batches"] += 1
        return tasks

    plan = Plan(process=A.count_entries, combine=A.add_int, empty=A.zero_int, next_tasks=next_tasks)
    r = ThreadExecutor(max_workers=4).run(plan)
    assert r.value == total  # every event folded in correctly despite the resized partitions
    assert state["batches"] >= 2  # genuinely reshaped across more than one wave
    assert r.stopped is StopReason.EXHAUSTED


def test_stops_at_target_events() -> None:
    # an effectively-infinite supply of 100-event chunks; a target-events stop must end the run early
    def next_tasks(ctx: ExecContext):
        base = ctx.n_done
        return [Task(base + k, Partition("f", "E", 0, 100)) for k in range(4)]

    plan = Plan(
        process=A.count_entries,
        combine=A.add_int,
        empty=A.zero_int,
        next_tasks=next_tasks,
        stop=StopCondition(target_events=500),
    )
    r = ThreadExecutor(max_workers=2).run(plan)
    assert r.stopped is StopReason.TARGET_EVENTS
    assert r.value >= 500  # stopped at or just past the target
    assert r.n_partitions < 50  # did NOT run forever
