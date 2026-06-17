"""M38 (perf): the IPC peer path resolves each actor's queues from a registry INHERITED via the pool
initializer (``peer_pool_init``) instead of Manager-proxy submit-args — the change that removed the
``multiprocessing.Manager`` server process. The real path runs in worker processes, so the actor
resolution is witnessed in-process (the M37 ``test_inprocess_paths`` pattern); persistent reuse +
straggler drain is witnessed against a real process pool.
"""

from __future__ import annotations

import queue
import threading

import pytest
from graphed_core import Partition, Plan, Task

import graphed_exec_local._peer as _peer
from graphed_exec_local._peer import (
    DRIVER,
    collect_peer_root,
    make_bounds,
    peer_pool_init,
    pooled_peer_actor,
    slice_items,
)
from graphed_exec_local._reduce import tree_reduce
from graphed_exec_local._transport import QueueTransport
from graphed_exec_local.executors import ProcessPoolExecutor


def _cat(a: str, b: str) -> str:
    return f"({a}+{b})"


def _empty() -> str:
    return "e"


def _leaf_value(part: Partition, _resources: object) -> str:
    return str(part.entry_start)


def _flat(n: int) -> str:
    return tree_reduce(n, [(i, str(i)) for i in range(n)], _cat, _empty)[0]


def _partitions(n: int) -> list[Partition]:
    return [Partition(f"f{i}", "Events", i, i + 1) for i in range(n)]


@pytest.mark.parametrize(("n", "w"), [(8, 4), (15, 4), (100, 8)])
def test_pooled_actor_resolves_inherited_registry_and_matches_flat_tree(n: int, w: int) -> None:
    """Run ``pooled_peer_actor`` in threads after installing the registry via ``peer_pool_init`` —
    each actor must look up its OWN inbox + the peers' outboxes by address and reduce bit-for-bit
    like the flat tree (no Manager proxies, no per-submit queue args)."""
    worker_addrs = tuple(f"w{i}" for i in range(w))
    addrs = (DRIVER, *worker_addrs)
    registry: dict[str, queue.Queue[object]] = {a: queue.Queue() for a in addrs}
    bounds = make_bounds(n, w)
    items = slice_items(_partitions(n), bounds, worker_addrs)
    driver_t = QueueTransport(DRIVER, registry[DRIVER], {a: registry[a] for a in worker_addrs})
    witness: dict[str, dict[str, int]] = {}

    def actor_main(addr: str) -> None:
        witness[addr] = pooled_peer_actor(addr, n, bounds, worker_addrs, _leaf_value, _cat, items[addr])

    try:
        peer_pool_init(registry)  # the pool initializer; in-process all threads share the global
        assert _peer._peer_registry is registry  # witness: the registry was installed for resolution
        threads = [threading.Thread(target=actor_main, args=(a,)) for a in worker_addrs]
        for t in threads:
            t.start()
        root = collect_peer_root(driver_t, _empty, n, timeout_s=30)
        for t in threads:
            t.join(timeout=10)
    finally:
        _peer._peer_registry = None  # reset the process global for test isolation

    assert root == _flat(n)  # the registry-resolved actors produced the identical grouping
    assert sum(w_["processed"] for w_ in witness.values()) == n
    assert sum(w_["n_combines"] for w_ in witness.values()) == n - 1


def test_pooled_actor_without_initializer_is_a_loud_error() -> None:
    """If the pool initializer never ran (misconfiguration), the actor refuses loudly rather than
    resolving against a stale/absent registry."""
    _peer._peer_registry = None
    with pytest.raises(AssertionError, match="peer_pool_init"):
        pooled_peer_actor("w0", 1, [0, 1], ("w0",), _leaf_value, _cat, [(0, Partition("f", "E", 0, 1))])


def _count(_part: Partition, _resources: object) -> int:
    return 1


def _add(a: int, b: int) -> int:
    return a + b


def _zero() -> int:
    return 0


def test_persistent_ipc_reuses_pool_and_registry_across_runs() -> None:
    """A persistent full-registry IPC executor reuses its worker pool + raw-queue registry on the second
    run (spawn paid once) and drains any straggler first — same result both runs, no Manager involved."""
    tasks = tuple(Task(i, Partition(f"p{i}", "Events", i, i + 1)) for i in range(8))
    plan = Plan(process=_count, combine=_add, empty=_zero, tasks=tasks)
    with ProcessPoolExecutor(max_workers=4, persistent=True, comms="ipc", steal=True) as ex:
        r1 = ex.run(plan).value
        pool1 = ex._peer_pool
        registry1 = ex._peer_registry
        r2 = ex.run(plan).value  # reuse branch: same pool + registry, straggler-drained
        assert ex._peer_pool is pool1  # witness: the pool was REUSED, not respawned
        assert ex._peer_registry is registry1  # ... and so was the inherited queue registry
    assert r1 == r2 == 8  # every leaf counted once, both runs


def test_full_registry_warns_when_fd_budget_exceeded(monkeypatch: pytest.MonkeyPatch) -> None:
    """ProcessPoolExecutor WARNS (recommending PinnedPoolExecutor) rather than silently switching pools
    when the full-registry fd footprint would strain the per-process limit. We force the predicate
    ``_exceeds_fd_budget`` True (it is exercised for real on every normal small-w run, returning False);
    this isolates the warning *wiring* without needing to exhaust the OS fd limit on the test host."""
    monkeypatch.setattr("graphed_exec_local.executors._exceeds_fd_budget", lambda _w: True)
    tasks = tuple(Task(i, Partition(f"p{i}", "Events", i, i + 1)) for i in range(4))
    plan = Plan(process=_count, combine=_add, empty=_zero, tasks=tasks)
    # no silent switch — it still runs on the full-registry pool, just warns
    with (
        pytest.warns(UserWarning, match="PinnedPoolExecutor"),
        ProcessPoolExecutor(max_workers=2, comms="ipc", steal=False) as ex,
    ):
        assert ex.run(plan).value == 4
