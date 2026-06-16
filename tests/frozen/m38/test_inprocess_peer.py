"""M38 in-process coverage of the per-worker process-actors and driver-side edge paths.

``ipc_peer_actor`` and ``http_peer_actor`` are the picklable entry points ``ProcessExecutor`` submits
to its worker pool, so in a real run they execute in *worker processes* where coverage instrumentation
in the driver process cannot see them (the same gap M37 closed for the hub worker entry via
``test_inprocess_paths``). Here we drive the **exact** discovery + reduction protocol the executor uses
(``_peer_ipc`` / ``_peer_http``) but with the actors running in threads, so the actor bodies — and the
worker-process resource cache they reuse — are exercised under instrumentation. We also pin the
driver-side timeout and the worker ``done`` paths. These are WITNESSED end-to-end (root bit-for-bit ==
the flat tree), not smoke calls.
"""

from __future__ import annotations

import queue
import threading

import pytest
from graphed_core import Partition

from graphed_exec_local._peer import (
    DRIVER,
    PeerReducer,
    collect_peer_root,
    http_driver_handshake,
    http_peer_actor,
    ipc_peer_actor,
    make_bounds,
    run_peer_worker,
    slice_items,
)
from graphed_exec_local._reduce import tree_reduce
from graphed_exec_local._transport import HttpTransport, QueueTransport


def _cat(a: str, b: str) -> str:
    return f"({a}+{b})"


def _empty() -> str:
    return "e"


def _leaf_value(part: Partition, _resources: object) -> str:
    """``process``: the leaf index is carried in ``entry_start`` so the result mirrors ``str(leaf)``."""
    return str(part.entry_start)


def _flat(n: int) -> str:
    return tree_reduce(n, [(i, str(i)) for i in range(n)], _cat, _empty)[0]


def _partitions(n: int) -> list[Partition]:
    return [Partition(f"f{i}", "Events", i, i + 1) for i in range(n)]


@pytest.mark.parametrize(("n", "w"), [(8, 4), (15, 4), (100, 8)])
def test_ipc_peer_actor_in_process_matches_flat_tree(n: int, w: int) -> None:
    """Run ``ipc_peer_actor`` (the ProcessExecutor IPC entry) in threads over ``queue.Queue`` inboxes,
    exactly as ``_peer_ipc`` wires it, and witness the off-driver root is bit-for-bit the flat tree."""
    worker_addrs = tuple(f"w{i}" for i in range(w))
    addrs = (DRIVER, *worker_addrs)
    inboxes: dict[str, queue.Queue[object]] = {a: queue.Queue() for a in addrs}
    bounds = make_bounds(n, w)
    items = slice_items(_partitions(n), bounds, worker_addrs)
    driver_t = QueueTransport(DRIVER, inboxes[DRIVER], {a: inboxes[a] for a in worker_addrs})

    witness: dict[str, dict[str, int]] = {}

    def actor_main(addr: str) -> None:
        witness[addr] = ipc_peer_actor(
            addr,
            inboxes[addr],
            {p: inboxes[p] for p in addrs if p != addr},
            n,
            bounds,
            worker_addrs,
            _leaf_value,
            _cat,
            items[addr],
        )

    threads = [threading.Thread(target=actor_main, args=(a,)) for a in worker_addrs]
    for t in threads:
        t.start()
    root = collect_peer_root(driver_t, _empty, n, timeout_s=30)
    for t in threads:
        t.join(timeout=10)

    assert root == _flat(n)  # the actor body produced the identical grouping, off the driver
    total_combines = sum(wt["n_combines"] for wt in witness.values())
    total_processed = sum(wt["processed"] for wt in witness.values())
    assert total_combines == n - 1  # every combine ran inside an actor, none on the driver
    assert total_processed == n  # every leaf processed exactly once (no double, no drop)
    assert sum(wt["peer_sends"] for wt in witness.values()) > 0  # partials crossed worker->worker


@pytest.mark.parametrize(("n", "w"), [(8, 4), (31, 8)])
def test_http_peer_actor_in_process_matches_flat_tree(n: int, w: int) -> None:
    """Run ``http_peer_actor`` (the ProcessExecutor HTTP entry) in threads, driving the real loopback
    discovery handshake the executor uses (``http_driver_handshake`` + ``collect_peer_root``)."""
    worker_addrs = tuple(f"w{i}" for i in range(w))
    bounds = make_bounds(n, w)
    items = slice_items(_partitions(n), bounds, worker_addrs)
    driver_t = HttpTransport(DRIVER)

    witness: dict[str, dict[str, int]] = {}

    def actor_main(addr: str) -> None:
        witness[addr] = http_peer_actor(
            addr,
            driver_t.host,
            driver_t.port,
            n,
            bounds,
            worker_addrs,
            _leaf_value,
            _cat,
            items[addr],
        )

    threads = [threading.Thread(target=actor_main, args=(a,)) for a in worker_addrs]
    try:
        for t in threads:
            t.start()
        http_driver_handshake(driver_t, worker_addrs, timeout_s=30.0)
        root = collect_peer_root(driver_t, _empty, n, timeout_s=30)
        for t in threads:
            t.join(timeout=10)
    finally:
        driver_t.close()

    assert root == _flat(n)
    assert sum(wt["processed"] for wt in witness.values()) == n
    assert sum(wt["n_combines"] for wt in witness.values()) == n - 1


def test_collect_peer_root_times_out_and_releases_workers() -> None:
    """No worker ever ships a root -> the bounded driver wait surfaces a ``TimeoutError`` (and still
    broadcasts ``done`` so any worker would be released) rather than hanging."""
    inboxes: dict[str, queue.Queue[object]] = {DRIVER: queue.Queue(), "w0": queue.Queue()}
    driver_t = QueueTransport(DRIVER, inboxes[DRIVER], {"w0": inboxes["w0"]})
    with pytest.raises(TimeoutError, match="did not produce a root"):
        collect_peer_root(driver_t, _empty, n=4, timeout_s=0.1)
    # the worker was released even though the root never came (send wraps as (sender, message))
    assert inboxes["w0"].get_nowait() == (DRIVER, ("done",))


def test_run_peer_worker_returns_on_prebuffered_done() -> None:
    """A ``done`` already buffered (e.g. raced ahead of the HTTP handshake) is drained from ``pending``
    first and ends the worker without touching the live transport."""
    inboxes: dict[str, queue.Queue[object]] = {DRIVER: queue.Queue(), "w0": queue.Queue()}
    transport = QueueTransport("w0", inboxes["w0"], {DRIVER: inboxes[DRIVER]})
    reducer: PeerReducer[str] = PeerReducer("w0", transport, 1, [0, 1], ("w0",), _cat)
    # no local items, the driver's done is already in the prebuffer -> return immediately
    run_peer_worker(reducer, transport, [], prebuffered=[(DRIVER, ("done",))])
    assert reducer.n_combines == 0  # nothing settled; the worker just observed done and stopped
