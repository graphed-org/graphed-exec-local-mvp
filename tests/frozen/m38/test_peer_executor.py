"""M38 peer reduction through the real executors (spike; frozen at P6). ThreadExecutor and
ProcessExecutor on BOTH transports (ipc, http) must reduce to the same value as the hub path — and,
for a non-associative float combine, **bit-for-bit** the same (peer uses the identical fixed tree, so
the only difference vs the hub is *where* combines run)."""

from __future__ import annotations

import pytest
from graphed_core import Partition, Plan, Task
from graphed_core.execution import SequentialRunner

from graphed_exec_local.executors import ProcessExecutor, ThreadExecutor

BACKENDS = ["ipc", "http"]


def _nentries(partition: Partition, resources: object) -> int:
    return partition.n_entries


def _add(a: float, b: float) -> float:
    return a + b


def _zero() -> float:
    return 0.0


def _fval(partition: Partition, resources: object) -> float:
    # a distinct, non-round float per partition so the combine is order/grouping sensitive
    return 1.0 / (partition.entry_start + 1)


def _plan(process, n: int = 25, workers: int = 4):
    tasks = tuple(Task(k, Partition(f"f{k}.root", "Events", k, k + 1 + (k % 5))) for k in range(n))
    return Plan(process=process, combine=_add, empty=_zero, tasks=tasks)


@pytest.mark.parametrize("kind", BACKENDS)
@pytest.mark.parametrize("executor_cls", [ThreadExecutor, ProcessExecutor])
def test_peer_matches_hub_through_executor(executor_cls, kind) -> None:
    plan = _plan(_nentries)
    hub = ThreadExecutor(max_workers=4).run(plan)  # comms=None -> the driver-hub path
    seq = SequentialRunner().run(plan)
    peer = executor_cls(max_workers=4, comms=kind).run(plan)
    assert peer.value == hub.value == seq.value  # sum is associative+commutative -> all agree
    assert peer.n_partitions == hub.n_partitions == 25
    assert peer.n_combines == 24  # n-1


@pytest.mark.parametrize("kind", BACKENDS)
@pytest.mark.parametrize("executor_cls", [ThreadExecutor, ProcessExecutor])
def test_peer_is_bit_for_bit_with_hub_on_floats(executor_cls, kind) -> None:
    # WITNESS that peer uses the SAME fixed tree grouping as the hub: with a non-associative float
    # combine, only an identical grouping gives an identical float down to the last ULP.
    plan = _plan(_fval, n=37)
    hub = ThreadExecutor(max_workers=4).run(plan).value
    peer = executor_cls(max_workers=4, comms=kind).run(plan).value
    assert peer == hub  # exact float equality


def test_peer_empty_and_singleton_through_executor() -> None:
    assert ThreadExecutor(comms="ipc").run(_plan(_nentries, n=0)).value == 0.0
    one = ThreadExecutor(comms="ipc").run(_plan(_nentries, n=1))
    assert one.n_partitions == 1 and one.n_combines == 0
