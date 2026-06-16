"""M38 peer reduction (spike; frozen at P6). The distributed (off-driver) reduction must equal the
flat ``tree_reduce`` **bit-for-bit** — same grouping — regardless of transport backend, the order a
worker processes its leaves, or message timing. A non-commutative concat combine makes equality prove
the left/right *grouping*, not just the leaf multiset."""

from __future__ import annotations

import random
import threading
import time

import pytest

from graphed_exec_local._peer import (
    PeerReducer,
    collect_peer_root,
    make_bounds,
    run_peer_worker,
    worker_of,
)
from graphed_exec_local._reduce import tree_reduce
from graphed_exec_local._transport import build_transports


def _cat(a: str, b: str) -> str:
    return f"({a}+{b})"


def _empty() -> str:
    return "e"


def _flat(n: int) -> str:
    return tree_reduce(n, [(i, str(i)) for i in range(n)], _cat, _empty)[0]


def _run(
    n: int, w: int, kind: str, *, shuffle_seed: int | None = None, jitter: bool = False
) -> tuple[str, dict[str, PeerReducer[str]]]:
    """Run the peer reduction and return (root, {worker -> its reducer}) so a test can both check the
    result AND witness, from the reducers' counters, that the work was genuinely distributed."""
    worker_addrs = tuple(f"w{i}" for i in range(w))
    transports = build_transports(kind, ("driver", *worker_addrs))
    reducers: dict[str, PeerReducer[str]] = {}
    try:
        bounds = make_bounds(n, w)
        items: dict[str, list[tuple[int, str]]] = {a: [] for a in worker_addrs}
        for leaf in range(n):
            items[worker_addrs[worker_of(leaf, bounds)]].append((leaf, str(leaf)))
        if shuffle_seed is not None:
            for a in worker_addrs:
                random.Random((shuffle_seed, a).__hash__() & 0xFFFFFFFF).shuffle(items[a])

        def worker_main(addr: str) -> None:
            reducer: PeerReducer[str] = PeerReducer(addr, transports[addr], n, bounds, worker_addrs, _cat)
            reducers[addr] = reducer
            src = items[addr]
            if jitter:

                def gen():
                    for it in src:
                        time.sleep(random.random() * 0.001)
                        yield it

                run_peer_worker(reducer, transports[addr], gen())
            else:
                run_peer_worker(reducer, transports[addr], src)

        threads = [threading.Thread(target=worker_main, args=(a,)) for a in worker_addrs]
        for t in threads:
            t.start()
        root = collect_peer_root(transports["driver"], _empty, n, timeout_s=30)
        for t in threads:
            t.join(timeout=10)
        return root, reducers
    finally:
        for t in transports.values():
            t.close()


def _peer_reduce(n: int, w: int, kind: str, *, shuffle_seed: int | None = None, jitter: bool = False) -> str:
    return _run(n, w, kind, shuffle_seed=shuffle_seed, jitter=jitter)[0]


SHAPES = [
    (1, 1),
    (2, 2),
    (3, 2),
    (4, 2),
    (5, 3),
    (7, 3),
    (8, 4),
    (15, 4),
    (16, 4),
    (17, 5),
    (31, 8),
    (100, 8),
]


@pytest.mark.parametrize("kind", ["ipc", "http"])
@pytest.mark.parametrize(("n", "w"), SHAPES)
def test_peer_matches_flat_tree(kind: str, n: int, w: int) -> None:
    assert _peer_reduce(n, w, kind) == _flat(n)  # bit-for-bit identical grouping, off the driver


@pytest.mark.parametrize("kind", ["ipc", "http"])
def test_peer_is_order_and_timing_invariant(kind: str) -> None:
    n, w = 31, 4
    ref = _flat(n)
    for seed in range(8):
        assert _peer_reduce(n, w, kind, shuffle_seed=seed) == ref  # worker-local order shuffled
    assert _peer_reduce(n, w, kind, jitter=True) == ref  # random per-leaf delays (message races)


def test_peer_empty_plan_returns_identity() -> None:
    assert _peer_reduce(0, 1, "ipc") == "e"


@pytest.mark.parametrize("kind", ["ipc", "http"])
def test_witness_reduction_is_genuinely_off_driver_and_distributed(kind: str) -> None:
    # WITNESS (not just the result): prove the mechanism engaged — combines were spread across workers
    # and partials crossed worker->worker, so a degenerate "everything funnels to w0 / via the driver"
    # path could not have produced this.
    n, w = 100, 8
    root, reducers = _run(n, w, kind)
    assert root == _flat(n)

    total_combines = sum(r.n_combines for r in reducers.values())
    workers_that_combined = sum(1 for r in reducers.values() if r.n_combines > 0)
    total_peer_sends = sum(r.peer_sends for r in reducers.values())
    total_peer_recvs = sum(r.peer_recvs for r in reducers.values())
    holders_of_root = [a for a, r in reducers.items() if r.have_root]

    assert total_combines == n - 1  # every combine accounted for, nothing done on the driver
    assert workers_that_combined >= 2  # genuinely distributed, not all on one worker
    assert total_peer_sends > 0  # partials actually crossed the transport worker->worker
    assert total_peer_sends == total_peer_recvs  # every hand-off was sent AND received
    assert holders_of_root == ["w0"]  # the root forms on the leftmost-leaf owner, once
