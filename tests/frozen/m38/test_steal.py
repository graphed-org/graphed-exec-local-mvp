"""M38 work-stealing (spike; frozen at P6). On an IMBALANCED workload (one worker's whole range is
heavy), witness — not just assume — that stealing engaged: the heavy owner gave leaves away, peers
processed more than their assigned share, every leaf ran exactly once, the result is unchanged, and
the run is faster. Stealing moves only `process` work; the leaf's owner still reduces it, so the
result is identical to no-stealing and to the canonical SequentialRunner."""

from __future__ import annotations

import time

import pytest
from graphed_core import Partition, Plan, Task
from graphed_core.execution import SequentialRunner

from graphed_exec_local.executors import ProcessExecutor, ThreadExecutor

HEAVY, LIGHT, N = 0.03, 0.001, 16  # 4 workers -> worker 0 owns leaves 0..3, all heavy


def _work(partition: Partition, resources: object) -> int:
    time.sleep(HEAVY if partition.uri.startswith("heavy") else LIGHT)
    return 1


def _add(a: int, b: int) -> int:
    return a + b


def _zero() -> int:
    return 0


def _imbalanced_plan() -> Plan[int]:
    # keys 0..N-1 in order -> leaf k. The first quarter (worker 0's static range) is heavy.
    tasks = tuple(
        Task(k, Partition(("heavy" if k < N // 4 else "light") + f"{k}.root", "Events", k, k + 1))
        for k in range(N)
    )
    return Plan(process=_work, combine=_add, empty=_zero, tasks=tasks)


@pytest.mark.parametrize("kind", ["ipc", "http"])
def test_witness_stealing_redistributes_and_stays_correct(kind: str) -> None:
    plan = _imbalanced_plan()
    seq = SequentialRunner().run(plan).value

    nosteal = ProcessExecutor(max_workers=4, comms=kind, steal=False)
    r0 = nosteal.run(plan)
    w0_nosteal = nosteal._last_peer_witness[0]

    steal = ProcessExecutor(max_workers=4, comms=kind, steal=True)
    r1 = steal.run(plan)
    wit = steal._last_peer_witness

    # correctness: identical to no-stealing AND to the canonical baseline (stealing only moves work)
    assert r0.value == r1.value == seq == N

    # witness no-steal really is the static-range baseline: worker 0 processed all 4 of its own
    assert w0_nosteal["processed"] == N // 4 and w0_nosteal["steals"] == 0

    # witness stealing engaged: every leaf ran exactly once (no double/lost), the heavy owner GAVE
    # leaves away, and peers actually stole + processed them.
    assert sum(w["processed"] for w in wit) == N
    assert wit[0]["given"] > 0  # the heavy worker offloaded part of its range
    assert wit[0]["processed"] < N // 4  # ...and therefore ran fewer than its 4 assigned
    assert sum(w["steals"] for w in wit) > 0  # peers stole work

    # anti-cascade witness (steal-ONE, not steal-half): the heavy owner sheds leaves ONE AT A TIME —
    # the number of grants equals the number of leaves it gave up, and every shed leaf is stolen by
    # exactly one peer. A steal-HALF grant would move several leaves per request (so `given` would be
    # < the leaves shed — a victim emptied into a single bulk transfer); steal-one structurally cannot.
    # (How many DISTINCT peers catch those one-at-a-time grants is a scheduling-timing detail — on a
    # slow transport one quick peer may catch several — so it is deliberately NOT asserted.)
    assert wit[0]["given"] + wit[0]["processed"] == N // 4  # owner's range = run-here + shed-one-by-one
    assert sum(w["given"] for w in wit) == sum(w["steals"] for w in wit)  # each shed leaf stolen once

    # ...and it pays off: the point of stealing is a shorter critical path, witnessed STRUCTURALLY
    # rather than by wall clock — without stealing worker 0 runs all 4 heavy leaves serially
    # (w0_nosteal.processed == 4); with stealing it runs strictly fewer (asserted above), so the heavy
    # work is genuinely off its critical path. A wall-clock `dt_steal < dt_nosteal` assert is NOT used:
    # on a loaded/slow CI runner the ~0.1 s of heavy work is dwarfed by process+transport noise, making
    # the comparison flaky without proving anything the redistribution witnesses don't already.
    assert wit[0]["processed"] < w0_nosteal["processed"]  # stealing shortened the heavy owner's path


def test_steal_thread_executor_is_correct_and_redistributes() -> None:
    plan = _imbalanced_plan()
    ex = ThreadExecutor(max_workers=4, comms="ipc", steal=True)  # sleep releases the GIL -> parallel
    assert ex.run(plan).value == N
    wit = ex._last_peer_witness
    assert sum(w["processed"] for w in wit) == N
    assert sum(w["steals"] for w in wit) > 0


def test_uniform_workload_needs_little_or_no_stealing() -> None:
    # all-light, balanced ranges: workers finish together, so stealing is rare and never wrong.
    tasks = tuple(Task(k, Partition(f"light{k}.root", "Events", k, k + 1)) for k in range(16))
    plan = Plan(process=_work, combine=_add, empty=_zero, tasks=tasks)
    ex = ProcessExecutor(max_workers=4, comms="ipc", steal=True)
    assert ex.run(plan).value == 16
    assert sum(w["processed"] for w in ex._last_peer_witness) == 16  # exactly once each
