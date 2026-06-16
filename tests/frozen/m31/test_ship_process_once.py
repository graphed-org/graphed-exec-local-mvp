"""M31 — the process callable is shipped to each worker ONCE, not once per task.

`concurrent.futures` re-pickles and re-ships a submit's callable on every call, so a Plan whose
`process` embeds a large compiled IR pays that wire cost per partition. M31 pickles the process
once in the driver, broadcasts it to every worker (cached by content hash), and submits only
`(token, partition)` per task. These tests measure that property directly — and pin that the
optimization changed nothing about results, determinism, or the thread executor.
"""

from __future__ import annotations

import functions_probe
import shipping_probe
from graphed_core import Partition, Plan, Task

from graphed_exec_local import ProcessExecutor, ThreadExecutor


def _count_plan(n_tasks: int, payload_bytes: int = 0) -> Plan:
    proc = shipping_probe.CountingProcess(payload_bytes)
    tasks = tuple(Task(i, Partition("p", "", i, i + 1)) for i in range(n_tasks))
    return Plan(process=proc, combine=shipping_probe.union, empty=shipping_probe.empty, tasks=tasks)


def test_process_is_unpickled_once_per_worker_not_once_per_task() -> None:
    workers = 4
    plan = _count_plan(n_tasks=40)  # 40 >> 4: per-task shipping would unpickle ~10x per worker
    # comms=None pins the hub broadcast path (M31's subject) since M38 flipped the default to peer.
    with ProcessExecutor(max_workers=workers, persistent=True, comms=None) as ex:
        observed = ex.run(plan).value  # frozenset of (pid, unpickle_count_in_that_worker)
    pids = {pid for pid, _ in observed}
    counts = {cnt for _, cnt in observed}
    assert len(pids) >= 2, "work genuinely spread across multiple workers"
    assert counts == {1}, f"process must be unpickled exactly once per worker, saw counts {counts}"


def test_a_large_process_does_not_scale_per_task() -> None:
    # 2 MB process: if it shipped per task the 40-task run would move ~80 MB; ship-once moves ~8 MB
    plan = _count_plan(n_tasks=40, payload_bytes=2 * 1024 * 1024)
    with ProcessExecutor(max_workers=4, persistent=True, comms=None) as ex:
        observed = ex.run(plan).value
    assert {cnt for _, cnt in observed} == {1}


def test_results_unchanged_and_deterministic() -> None:
    plan = functions_probe.sum_plan(16)
    want = sum(range(0, 160, 10))
    with ProcessExecutor(max_workers=4, persistent=True, comms=None) as ex:
        r1 = int(ex.run(plan).value[0])
        r2 = int(ex.run(plan).value[0])
    assert r1 == want and r2 == want  # correct + byte-identical across runs


def test_thread_executor_unaffected() -> None:
    plan = functions_probe.sum_plan(16)
    assert int(ThreadExecutor(max_workers=4).run(plan).value[0]) == sum(range(0, 160, 10))
