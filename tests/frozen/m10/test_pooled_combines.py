"""M10 — pooled combines (finding C.8): tree-reduce combines run on the worker pool, not serially
on the driver thread.

The default driver-side path is pinned by the frozen M7 suite and stays untouched;
``pooled_combines=True`` opts in. Pins: identical results and combine count vs the driver path,
the SAME fixed reduction tree (so results stay bit-identical), combines observed off the driver
(thread idents / worker pids), straggler tolerance preserved, and worker errors still propagate
intact.

M38: these tests pin ``comms=None`` (the hub path). ``pooled_combines`` is a hub-only mechanism — peer
reduction does its combines off-driver by construction and rejects ``pooled_combines`` loudly — so once
peer became the default ``comms``, the pooled-combines feature is exercised explicitly via ``comms=None``.
"""

from __future__ import annotations

import os
import threading

import m10_helpers as H
import pytest
from graphed_core import Partition, Plan, StopReason, Task

from graphed_exec_local import ProcessExecutor, ThreadExecutor


def _tasks(n: int) -> list[Task]:
    return [Task(i, Partition(f"f{i}", "t", i, i + 1)) for i in range(n)]


@pytest.mark.parametrize("n", [1, 2, 5, 16, 33])
def test_pooled_matches_driver_path_results_and_combine_count(n: int) -> None:
    plan = Plan(process=H.leaf_index, combine=H.add_int, empty=H.zero_int, tasks=_tasks(n))
    pooled = ThreadExecutor(comms=None, pooled_combines=True).run(plan)
    driver = ThreadExecutor(comms=None).run(plan)
    assert pooled.value == driver.value == sum(range(n))
    assert pooled.n_combines == driver.n_combines == max(0, n - 1)
    assert pooled.n_partitions == n and pooled.stopped is StopReason.EXHAUSTED


@pytest.mark.parametrize("n", [2, 7, 16, 33])
def test_pooled_uses_the_same_fixed_reduction_tree(n: int) -> None:
    # the combine RESULT is the grouping itself: equal results <=> identical trees, which is what
    # makes pooled execution bit-identical to the driver path for any associative combine
    plan = Plan(process=H.leaf_singleton, combine=H.tree_shape, empty=H.empty_tuple, tasks=_tasks(n))
    pooled = ThreadExecutor(comms=None, pooled_combines=True).run(plan)
    driver = ThreadExecutor(comms=None).run(plan)
    assert pooled.value == driver.value


def test_combines_run_off_the_driver_thread() -> None:
    plan = Plan(process=H.leaf_tid, combine=H.combine_collect_tid, empty=H.empty_pidset, tasks=_tasks(16))
    result = ThreadExecutor(comms=None, max_workers=4, pooled_combines=True).run(plan)
    tids = result.value
    assert isinstance(tids, frozenset) and len(tids) >= 1
    assert threading.get_ident() not in tids, "a pooled combine must not run on the driver thread"


def test_combines_run_in_worker_processes() -> None:
    plan = Plan(process=H.leaf_pid, combine=H.combine_collect_pid, empty=H.empty_pidset, tasks=_tasks(8))
    result = ProcessExecutor(comms=None, max_workers=2, pooled_combines=True).run(plan)
    pids = result.value
    assert isinstance(pids, frozenset) and len(pids) >= 1
    assert os.getpid() not in pids, "a pooled combine must not run in the driver process"


def test_process_pool_pooled_matches_thread_pool_pooled() -> None:
    plan = Plan(process=H.leaf_index, combine=H.add_int, empty=H.zero_int, tasks=_tasks(12))
    a = ProcessExecutor(comms=None, max_workers=2, pooled_combines=True).run(plan)
    b = ThreadExecutor(comms=None, pooled_combines=True).run(plan)
    assert a.value == b.value and a.n_combines == b.n_combines


def test_straggler_does_not_block_other_subtrees() -> None:
    # leaf 0 sleeps; with pooled combines the other subtree's combines still fire early — the
    # on_combine hook sees combines submitted before all leaves are done
    seen: list[int] = []
    plan = Plan(process=H.straggler_leaf, combine=H.add_int, empty=H.zero_int, tasks=_tasks(8))
    result = ThreadExecutor(comms=None, max_workers=4, on_combine=seen.append, pooled_combines=True).run(plan)
    assert result.value == 8
    assert len(seen) == 7
    assert min(seen) < 8, "some combine fired before every leaf was delivered (no barrier)"


def test_worker_error_propagates_intact() -> None:
    plan = Plan(process=H.boom_on_three, combine=H.add_int, empty=H.zero_int, tasks=_tasks(6))
    with pytest.raises(ValueError, match="leaf 3 exploded"):
        ThreadExecutor(comms=None, pooled_combines=True).run(plan)


def test_empty_plan_returns_identity() -> None:
    plan = Plan(process=H.leaf_index, combine=H.add_int, empty=H.zero_int, tasks=[])
    result = ThreadExecutor(comms=None, pooled_combines=True).run(plan)
    assert result.value == 0 and result.n_combines == 0
