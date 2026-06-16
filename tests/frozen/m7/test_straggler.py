"""A single slow straggler must NOT block the reduction of the other partitions (tree reduction),
i.e. combines fire as ready instead of waiting for all leaves (plan M7).

M38: this pins ``comms=None`` (the hub path). The ``on_combine`` hook firing *incrementally* (a
combine as soon as its two inputs are ready) is a property of the hub's driver-side tree reduction;
peer reduction does its combines off-driver and reports the count, so it can't assert incremental
ordering. Peer's straggler tolerance — work-stealing — is covered by the M38 work-stealing suite."""

from __future__ import annotations

import analyses as A
from graphed_core import Partition, Plan, Task

from graphed_exec_local import ThreadExecutor


def test_straggler_does_not_block_reduction_of_others() -> None:
    n = 16
    # leaf at entry_start==0 sleeps 0.4s; the other 15 are instant
    tasks = [Task(i, Partition("f", "E", i, i + 1)) for i in range(n)]
    leaves_at_combine: list[int] = []
    ex = ThreadExecutor(comms=None, max_workers=8, on_combine=leaves_at_combine.append)
    r = ex.run(Plan(process=A.straggler_one, combine=A.add_int, empty=A.zero_int, tasks=tasks))
    assert r.value == n  # all partitions still counted
    assert r.n_combines == n - 1
    # combines fired before the straggler's (last-delivered) leaf arrived — no all-leaves barrier
    assert min(leaves_at_combine) < n
    assert leaves_at_combine == sorted(leaves_at_combine)  # monotonic, never regresses
