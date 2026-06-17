"""M38 P7: identity-pinned IPC workers with a BOUNDED (O(log N)) communication overlay, so the peer
registry a worker inherits is O(log N) — not the O(N²) "every worker inherits every queue" that bites
large single machines (>128 cores). Covers the shared overlay machinery (lifelines + reduction edges),
the per-worker actor that resolves its inherited transport, the PinnedProcessPool executor, and that a
persistent IPC executor reuses its pinned pool. (The dynamic-cluster runtime that recomputes this
overlay on membership change is Phase 2; it reuses worker_outbox_addresses.)
"""

from __future__ import annotations

import math
import multiprocessing
import queue
import threading

import pytest
from graphed_core import Partition, Plan, Task

import graphed_exec_local._peer as _peer
from graphed_exec_local._peer import (
    DRIVER,
    collect_peer_root,
    lifeline_neighbors,
    make_bounds,
    pinned_peer_actor,
    pinned_peer_init,
    worker_outbox_addresses,
)
from graphed_exec_local._pinned_pool import PinnedProcessPool, _pinned_loop
from graphed_exec_local._reduce import tree_reduce
from graphed_exec_local._transport import QueueTransport
from graphed_exec_local.executors import ProcessExecutor


def _cat(a: str, b: str) -> str:
    return f"({a}+{b})"


def _empty() -> str:
    return "e"


def _leaf_value(part: Partition, _resources: object) -> str:
    return str(part.entry_start)


def _flat(n: int) -> str:
    return tree_reduce(n, [(i, str(i)) for i in range(n)], _cat, _empty)[0]


# ---- the bounded overlay: O(log N) degree (the sub-quadratic-inheritance witness) -----------------


@pytest.mark.parametrize(("n", "w"), [(8, 4), (32, 8), (64, 16), (256, 32), (1024, 64), (4096, 128)])
def test_overlay_degree_is_logarithmic_not_linear(n: int, w: int) -> None:
    """Each worker's outbox set (what it inherits beyond its inbox) is O(log w), NOT O(w) — so a
    worker inherits O(log N) queue handles and the whole registry is O(N log N)."""
    addrs = tuple(f"w{i}" for i in range(w))
    out = worker_outbox_addresses(n, make_bounds(n, w), addrs)
    degree = max(len(s) for s in out.values())
    budget = 3 * max(1, math.ceil(math.log2(w))) + 2  # generous O(log w)
    assert degree <= budget, f"overlay degree {degree} not O(log {w}) (budget {budget})"
    assert degree < w  # the whole point: strictly sub-linear in the worker count
    # every outbox target is a real address or the driver (no dangling edges)
    assert all(t in set(addrs) | {DRIVER} for s in out.values() for t in s)


def test_lifelines_are_a_symmetric_hypercube() -> None:
    # symmetric so a steal request and its response ride the same edge; degree O(log w)
    w = 16
    nb = {i: lifeline_neighbors(i, w) for i in range(w)}
    assert all(i in nb[j] for i in range(w) for j in nb[i])  # symmetric
    assert all(len(nb[i]) <= math.ceil(math.log2(w)) for i in range(w))  # O(log w) degree


# ---- the per-worker actor resolving its inherited transport (in-process, single worker) ----------


def test_pinned_actor_resolves_inherited_transport_and_reduces() -> None:
    """``pinned_peer_init`` installs THIS worker's transport (its bounded subset) as the process state
    ``pinned_peer_actor`` reads. Single worker (w=1) so one process-global suffices in-process; the
    multi-worker reduction is covered by test_peer_reduce + the real ProcessExecutor tests."""
    n = 4
    registry: dict[str, queue.Queue[object]] = {DRIVER: queue.Queue(), "w0": queue.Queue()}
    pinned_peer_init("w0", registry["w0"], {DRIVER: registry[DRIVER]}, ())
    assert _peer._pinned is not None and _peer._pinned[0] == "w0"  # witness: identity installed
    driver_t = QueueTransport(DRIVER, registry[DRIVER], {"w0": registry["w0"]})
    items = [(i, Partition(f"f{i}", "Events", i, i + 1)) for i in range(n)]
    wit: dict[str, dict[str, int]] = {}

    def run() -> None:
        wit["w0"] = pinned_peer_actor(n, make_bounds(n, 1), ("w0",), _leaf_value, _cat, items, steal=False)

    t = threading.Thread(target=run)
    t.start()
    root = collect_peer_root(driver_t, _empty, n, timeout_s=30)
    t.join(timeout=10)
    try:
        assert root == _flat(n)
        assert wit["w0"]["processed"] == n
    finally:
        _peer._pinned = None  # reset the process global for test isolation


def test_pinned_actor_without_init_is_a_loud_error() -> None:
    _peer._pinned = None
    with pytest.raises(AssertionError, match="pinned_peer_init"):
        pinned_peer_actor(1, [0, 1], ("w0",), _leaf_value, _cat, [(0, Partition("f", "E", 0, 1))])


# ---- the PinnedProcessPool executor -------------------------------------------------------------


def test_pinned_loop_serves_calls_then_shuts_down() -> None:
    """The generic pinned-worker body (in-process): run the initializer once, serve ok + erroring
    calls, stop on the shutdown sentinel."""
    call_q: queue.Queue = queue.Queue()
    result_q: queue.Queue = queue.Queue()
    inited: list = []
    t = threading.Thread(target=_pinned_loop, args=(lambda *a: inited.append(a), ("tag",), call_q, result_q))
    t.start()
    call_q.put((0, lambda a, b: a + b, (2, 3)))
    assert result_q.get(timeout=5) == (0, True, 5)
    call_q.put((1, lambda: 1 / 0, ()))
    cid, ok, val = result_q.get(timeout=5)
    assert cid == 1 and ok is False and isinstance(val, ZeroDivisionError)
    call_q.put(None)  # shutdown sentinel
    t.join(timeout=5)
    assert not t.is_alive() and inited == [("tag",)]


_PP = None


def _pp_init(tag: str) -> None:
    global _PP
    _PP = tag


def _pp_echo(x: int) -> tuple:
    return (_PP, x)


def _pp_boom() -> None:
    raise ValueError("boom")


def test_pinned_process_pool_pins_identity_and_propagates_errors() -> None:
    """Each worker runs its OWN init (so worker 0 != worker 1's inherited state); submit targets a
    specific worker; errors surface through the Future (M6/M7)."""
    ctx = multiprocessing.get_context("spawn")
    pool = PinnedProcessPool(2, ctx, _pp_init, [("a",), ("b",)])
    try:
        assert pool.submit(_pp_echo, 5, worker=0).result(timeout=15) == ("a", 5)  # worker 0's identity
        assert pool.submit(_pp_echo, 7, worker=1).result(timeout=15) == ("b", 7)  # worker 1's identity
        assert pool.workers_alive()
        with pytest.raises(ValueError, match="boom"):
            pool.submit(_pp_boom, worker=0).result(timeout=15)
    finally:
        pool.shutdown()


# ---- the executor reuses its pinned pool across runs (spawn paid once) ---------------------------


def _count(_part: Partition, _resources: object) -> int:
    return 1


def _add(a: int, b: int) -> int:
    return a + b


def _zero() -> int:
    return 0


def test_persistent_ipc_reuses_pinned_pool_across_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    # force the identity-pinned path (small w defaults to the full-registry pool below the fd threshold)
    monkeypatch.setenv("GRAPHED_PEER_PINNED", "1")
    tasks = tuple(Task(i, Partition(f"p{i}", "Events", i, i + 1)) for i in range(8))
    plan = Plan(process=_count, combine=_add, empty=_zero, tasks=tasks)
    with ProcessExecutor(max_workers=4, persistent=True, comms="ipc", steal=True) as ex:
        r1 = ex.run(plan).value
        pool1 = ex._peer_pool
        assert isinstance(pool1, PinnedProcessPool)  # the identity-pinned pool, not the full-registry one
        r2 = ex.run(plan).value  # same (n, w) -> reuse the pinned pool, no respawn
        assert ex._peer_pool is pool1  # witness: identity-pinned workers were REUSED
    assert r1 == r2 == 8
