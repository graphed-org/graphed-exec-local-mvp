"""Peer reduction (M38): the tree reduction runs **across the workers, off the driver**.

The driver is no longer the combine hub. Each worker owns a contiguous **leaf range** and reduces it
with the lazy index tree (:mod:`_reduce`); the partials that straddle a range boundary are handed
**worker→worker** over a :class:`graphed_core.execution.WorkerTransport`. The driver only collects the
final root.

Why it stays **bit-for-bit identical** to the existing (hub) path — even for non-associative float
combines like histogram addition: every node keeps its **global** ``(level, pos)`` identity in the one
fixed ``plan_tree`` (even position = left operand, odd = right, unpaired carries up). Distributing the
*combines* across workers never changes the *grouping*, so the result is the same down to the last
ULP. Determinism is independent of message timing — a parked node waits for its fixed sibling, whoever
sends it whenever.

Ownership routing (the segment-tree merge): node ``(level, pos)`` is owned by the worker holding its
leftmost leaf ``pos << level``. The parent of a pair is owned by the **even** child's owner, so a
worker that forms an **odd** node it doesn't own ships it to that owner; an **even** node parks until
its odd sibling arrives (locally or from a peer). Only the O(log N) boundary nodes ever cross the
wire. Termination is a driver ``done`` broadcast once the root (owned by worker 0) arrives — no
fragile message counting, no barrier.
"""

from __future__ import annotations

import bisect
import contextlib
import time
from collections import deque
from collections.abc import Callable, Iterable, Sequence
from typing import Any, Generic, TypeVar

from graphed_core.execution import LocalResources, Partition, TaskEvent, TaskPhase, partition_label
from graphed_debug import StageError

from ._transport import HttpTransport, QueueTransport

R = TypeVar("R")
_MISSING: Any = object()

DRIVER = "driver"

# Only steal after a worker has been idle this long — long enough that a BALANCED run (all workers
# finish within ~ms of each other) completes before any steal fires, so stealing is near-free on the
# common case; short enough that a genuinely imbalanced load (a straggler idle for 100s of ms) still
# rebalances promptly. Without this, idle workers fire spurious steals near the end of balanced runs
# and add coordination tail latency for no benefit.
STEAL_DELAY = 0.01
# After the first request, back off exponentially up to this cap while requests keep being DENIED, so
# an idle worker during a balanced run's reduction tail doesn't storm peers with steal-requests (the
# denial traffic alone — even with zero successful steals — measurably slowed uniform runs). A
# successful steal resets the backoff, so a genuine straggler is still drained promptly.
MAX_STEAL_BACKOFF = 0.1
# Serialize the off-thread profiler at most ~1/s (its sampler keeps running continuously; only the
# flush + ship is throttled), never per leaf — the M37 R20.7 discipline.
PROFILE_FLUSH_INTERVAL = 1.0


def make_bounds(n: int, n_workers: int) -> list[int]:
    """Contiguous leaf-range boundaries: worker ``w`` owns leaves ``[bounds[w], bounds[w+1])``. Use
    ``n_workers <= n`` so every range is non-empty (the executor clamps)."""
    return [(i * n) // n_workers for i in range(n_workers + 1)]


def worker_of(leaf: int, bounds: list[int]) -> int:
    """The worker index owning ``leaf`` (the range containing it)."""
    return max(0, min(len(bounds) - 2, bisect.bisect_right(bounds, leaf) - 1))


class PeerReducer(Generic[R]):
    """Per-worker reduction state: feed local partials + peer nodes, keep/route/park by ownership,
    bubbling up the one global fixed tree. Worker 0 forms and ships the root to the driver."""

    def __init__(
        self,
        address: str,
        transport: Any,
        n: int,
        bounds: list[int],
        worker_addresses: tuple[str, ...],
        combine: Callable[[R, R], R],
    ) -> None:
        self.address = address
        self._t = transport
        self.n = n
        self._bounds = bounds
        self._workers = worker_addresses
        self._combine = combine
        self._present: dict[tuple[int, int], R] = {}  # parked nodes awaiting a sibling (the frontier)
        self.n_combines = 0  # combines this worker performed (witness: combines are distributed)
        self.peer_sends = 0  # nodes shipped worker->worker (witness: the reduction is off-driver)
        self.peer_recvs = 0  # peer nodes consumed (witness: hand-offs were actually received)
        self.root: R | None = None
        self.have_root = False

    def _level_size(self, level: int) -> int:
        return (self.n + (1 << level) - 1) >> level

    def _owner(self, level: int, pos: int) -> str:
        return self._workers[worker_of(pos << level, self._bounds)]

    def settle(self, level: int, pos: int, value: R) -> None:
        """Place a node and bubble it up: combine with present siblings, route odd nodes owned by a
        peer to that peer, park nodes still missing a sibling. Same (level,pos)/left-right rule as the
        flat tree, so the grouping is identical regardless of who runs the combine or when."""
        present = self._present
        while True:
            if self._level_size(level) == 1:  # the global root (worker 0 only ever reaches it)
                self.root = value
                self.have_root = True
                self._t.send(DRIVER, ("root", value))
                return
            if pos % 2 == 0:
                if pos + 1 >= self._level_size(level):  # unpaired (last of an odd level) -> carry up
                    level, pos = level + 1, pos >> 1
                    continue
                other = present.pop((level, pos + 1), _MISSING)
                if other is _MISSING:
                    present[(level, pos)] = value  # park: wait for the odd sibling (local or peer)
                    return
                left, right = value, other
            else:
                parent_owner = self._owner(level, pos - 1)  # parent is owned by the even child's owner
                if parent_owner != self.address:
                    self._t.send(parent_owner, ("node", level, pos, value))  # off-driver hand-off
                    self.peer_sends += 1
                    return
                other = present.pop((level, pos - 1), _MISSING)
                if other is _MISSING:
                    present[(level, pos)] = value  # park: wait for my own even sibling to form
                    return
                left, right = other, value
            value = self._combine(left, right)
            self.n_combines += 1
            level, pos = level + 1, pos >> 1


def run_peer_worker(
    reducer: PeerReducer[R],
    transport: Any,
    local_items: Iterable[tuple[int, R]],
    *,
    poll_timeout: float = 0.05,
    prebuffered: Sequence[tuple[str, Any]] = (),
) -> None:
    """Run one worker: settle every local ``(leaf, partial)``, then drain peer hand-offs until the
    driver broadcasts ``done`` (sent once the root has arrived — all combines are then complete).

    ``prebuffered`` are messages already received off the transport (e.g. ``node`` hand-offs that
    raced ahead of the HTTP discovery handshake); they are processed before resuming live ``recv``."""
    for leaf, partial in local_items:
        reducer.settle(0, leaf, partial)
    seen: set[tuple[int, int]] = set()  # dedup by node identity (a reliable transport may retry-send)
    pending = list(prebuffered)
    while True:
        if pending:
            item: tuple[str, Any] | None = pending.pop(0)
        else:
            item = transport.recv(timeout=poll_timeout)
        if item is None:
            continue
        payload: Any = item[1]
        tag = payload[0]
        if tag == "node":
            _, level, pos, value = payload
            if (level, pos) in seen:  # a duplicate hand-off must not combine twice
                continue
            seen.add((level, pos))
            reducer.peer_recvs += 1
            reducer.settle(level, pos, value)
        elif tag == "done":
            return


def collect_peer_root(
    driver_transport: Any,
    empty: Callable[[], R],
    n: int,
    *,
    poll_timeout: float = 0.05,
    timeout_s: float | None = None,
) -> R:
    """Driver side: wait for worker 0's root, then broadcast ``done`` to release the workers. For an
    empty plan there is no root — return the identity. ``timeout_s`` (if set) bounds the wait so a lost
    root surfaces as a ``TimeoutError`` instead of hanging."""
    if n == 0:
        driver_transport.broadcast(("done",))
        return empty()

    deadline = None if timeout_s is None else time.monotonic() + timeout_s
    root: R = _MISSING
    while root is _MISSING:
        got = driver_transport.recv(timeout=poll_timeout)
        if got is not None:
            payload: Any = got[1]
            if payload[0] == "root":
                root = payload[1]
                break
        if deadline is not None and time.monotonic() >= deadline:
            driver_transport.broadcast(("done",))  # release workers before surfacing the failure
            raise TimeoutError(f"peer reduction did not produce a root within {timeout_s}s")
    driver_transport.broadcast(("done",))
    return root


# ---- executor-facing helpers: per-worker actor + cross-process transport setup --------------


def slice_items(
    partitions: Sequence[Partition], bounds: list[int], worker_addresses: tuple[str, ...]
) -> dict[str, list[tuple[int, Partition]]]:
    """Split the key-ordered partitions into each worker's ``[(leaf, partition), ...]`` by leaf range."""
    out: dict[str, list[tuple[int, Partition]]] = {a: [] for a in worker_addresses}
    for leaf, part in enumerate(partitions):
        out[worker_addresses[worker_of(leaf, bounds)]].append((leaf, part))
    return out


def process_and_reduce(
    address: str,
    transport: Any,
    n: int,
    bounds: list[int],
    worker_addresses: tuple[str, ...],
    process: Callable[[Partition, Any], R],
    combine: Callable[[R, R], R],
    items: Sequence[tuple[int, Partition]],
    resources: LocalResources,
    *,
    steal: bool = True,
    emit: bool = False,
    profiler_factory: Callable[[], Any] | None = None,
    prebuffered: Sequence[tuple[str, Any]] = (),
    close_resources: bool = True,
) -> dict[str, int]:
    """The per-worker actor: run ``process`` on its leaves, peer-reduce over the transport, and return
    witness counters. Runs in a thread (ThreadExecutor) or a worker process (ProcessExecutor).

    **Work-stealing** (``steal=True``, the default): a worker that drains its own leaves steals from a
    busy peer. Stealing redistributes only the ``process`` work — the leaf's **owner still settles it**
    into the reduction (a thief ships the computed partial back as a ``leaf`` message), so the fixed
    tree and the result are **unchanged**: stealing moves *where* a leaf is computed, never *where* it
    reduces. The loop interleaves a non-blocking inbox drain between leaves, so a busy worker answers
    steal requests and the hot path (``process``) is never blocked on transport I/O (the R20.7 rule).

    ``resources`` (its ``open_once`` cache) is supplied by the caller; ``close_resources=False`` keeps
    it open across runs (a persistent pool reusing file handles, like the hub path)."""
    reducer: PeerReducer[R] = PeerReducer(address, transport, n, bounds, worker_addresses, combine)
    mine: deque[tuple[int, Partition]] = deque(items)  # leaves to PROCESS (own + stolen)
    seen: set[tuple[int, int]] = set()  # dedup settled nodes/leaves (a reliable transport may retry)
    peers = tuple(a for a in worker_addresses if a != address)
    stats = {"steals": 0, "given": 0, "processed": 0}
    victim = 0
    idle_since: float | None = None  # when this worker ran out of local work (gates the steal delay)
    backoff = STEAL_DELAY  # current wait between steal-requests; grows on denial, resets on a grant
    next_steal_at = 0.0  # monotonic time of the next allowed steal-request
    pending = list(prebuffered)
    done = False
    events: list[TaskEvent] = []  # M37 monitor events, batched to the driver off the hot path

    def emit_event(phase: TaskPhase, leaf: int, part: Partition, error: str | None = None) -> None:
        events.append(
            TaskEvent(
                phase, leaf, address, time.perf_counter(), partition_label(part), part.n_entries, error=error
            )
        )

    def ship_events() -> None:
        if events:
            transport.send(DRIVER, ("events", events.copy()))  # the driver forwards them to the monitor
            events.clear()

    # off-thread profiler parity with the hub: a worker samples its task thread and ships the flamegraph
    # tree to the driver (forwarded to monitor.on_profile), throttled so the serialize is never per-leaf.
    profiler = None
    last_flush = time.monotonic()
    if profiler_factory is not None:
        with contextlib.suppress(Exception):  # a profiler that won't start just disables sampling
            profiler = profiler_factory()
            profiler.start()

    def ship_profile(payload: bytes | None) -> None:
        if payload:
            transport.send(DRIVER, ("profile", address, payload))

    def owner(leaf: int) -> str:
        return worker_addresses[worker_of(leaf, bounds)]

    def settle_leaf(leaf: int, value: R) -> None:
        if (0, leaf) not in seen:  # idempotent: own-processing and a thief's `leaf` can't double-count
            seen.add((0, leaf))
            reducer.settle(0, leaf, value)

    def handle(payload: Any) -> None:
        nonlocal done
        tag = payload[0]
        if tag == "node":
            _, lvl, pos, val = payload
            if (lvl, pos) not in seen:
                seen.add((lvl, pos))
                reducer.peer_recvs += 1
                reducer.settle(lvl, pos, val)
        elif tag == "leaf":  # a partial (computed by a thief) for a leaf I OWN -> I settle it
            settle_leaf(payload[1], payload[2])
        elif tag == "steal_req":
            # Hand ONE leaf from the FAR end (steal-one, Blumofe-Leiserson / Cilk). NOT steal-half:
            # under many idle thieves hitting one victim, serialized "half each" drains the victim
            # geometrically (keeps W/2^k) and over-concentrates work on the first thief -> worse
            # makespan for our coarse, independent partitions. Steal-one lets k thieves each take one
            # leaf fairly with no cascade; steals are cheap relative to a partition, so the extra
            # steal attempts cost ~nothing. (`len > 1`: never give away the leaf I'm about to run.)
            thief = payload[1]
            if steal and len(mine) > 1:
                stats["given"] += 1
                transport.send(thief, ("steal_resp", [mine.pop()]))
            else:
                transport.send(thief, ("steal_resp", []))
        elif tag == "steal_resp":
            granted = payload[1]
            mine.extend(granted)
            stats["steals"] += len(granted)
        elif tag == "done":
            done = True

    try:
        while not done:
            while pending and not done:
                handle(pending.pop(0))
            for _sender, payload in transport.poll():  # cheap non-blocking drain between leaves
                handle(payload)
                if done:
                    break
            if done:
                break
            if emit:
                ship_events()  # batch off-path: one send per leaf-ish, negligible for coarse partitions
            if profiler is not None and time.monotonic() - last_flush >= PROFILE_FLUSH_INTERVAL:
                with contextlib.suppress(Exception):
                    ship_profile(profiler.flush())
                last_flush = time.monotonic()
            if mine:
                idle_since = None  # have work again (incl. a successful steal) -> reset steal backoff
                backoff = STEAL_DELAY
                leaf, part = mine.popleft()
                if emit:
                    emit_event(TaskPhase.STARTED, leaf, part)
                try:
                    partial = process(part, resources)  # HOT PATH (read + compute)
                except BaseException as exc:
                    if emit:
                        msg = str(exc) if isinstance(exc, StageError) else f"{type(exc).__name__}: {exc}"
                        emit_event(TaskPhase.ERRORED, leaf, part, error=msg)
                        ship_events()  # surface the error to the dashboard before the run tears down
                    raise
                stats["processed"] += 1
                if emit:
                    emit_event(TaskPhase.FINISHED, leaf, part)
                if owner(leaf) == address:
                    settle_leaf(leaf, partial)  # my leaf -> reduce here
                else:
                    transport.send(owner(leaf), ("leaf", leaf, partial))  # stolen -> back to its owner
            elif steal and peers:
                # no local work: request a steal once idle past STEAL_DELAY, then back off on each
                # denial so a balanced run's reduction tail isn't flooded with steal-requests.
                now = time.monotonic()
                if idle_since is None:
                    idle_since, next_steal_at, backoff = now, now + STEAL_DELAY, STEAL_DELAY
                if now >= next_steal_at:
                    transport.send(peers[victim % len(peers)], ("steal_req", address))
                    victim += 1
                    next_steal_at = now + backoff
                    backoff = min(backoff * 2, MAX_STEAL_BACKOFF)
                got = transport.recv(timeout=0.01)  # stay responsive to node/leaf/done while idle
                if got is not None:
                    handle(got[1])
            else:
                got = transport.recv(timeout=0.05)
                if got is not None:
                    handle(got[1])
        if emit:
            ship_events()  # final flush after `done` — the driver drains until workers finish
        if profiler is not None:
            with contextlib.suppress(Exception):
                ship_profile(profiler.stop())  # final sample tree + join the sampler thread
    finally:
        if close_resources:
            resources.close()
    return {
        "n_combines": reducer.n_combines,
        "peer_sends": reducer.peer_sends,
        "peer_recvs": reducer.peer_recvs,
        **stats,
    }


# A per-WORKER-PROCESS resource cache reused across run()s of a persistent pool, so a worker reopens a
# file at most once over its whole life (file locality across runs — the hub path's _proc_resources
# does the same). One peer actor runs at a time per process, so no intra-process concurrency.
_peer_proc_resources: LocalResources | None = None


def _worker_proc_resources() -> LocalResources:
    global _peer_proc_resources
    if _peer_proc_resources is None:
        _peer_proc_resources = LocalResources()
    return _peer_proc_resources


def ipc_peer_actor(
    address: str,
    inbox: Any,
    outboxes: dict[str, Any],
    n: int,
    bounds: list[int],
    worker_addresses: tuple[str, ...],
    process: Callable[[Partition, Any], Any],
    combine: Callable[[Any, Any], Any],
    items: Sequence[tuple[int, Partition]],
    steal: bool = True,
    emit: bool = False,
    profiler_factory: Callable[[], Any] | None = None,
) -> dict[str, int]:
    """Module-level (picklable) IPC actor for ``ProcessExecutor``: build the queue transport from the
    Manager-queue inbox/outboxes handed in, then process + peer-reduce. Reuses the process resource
    cache across runs (file locality for a persistent pool)."""
    transport = QueueTransport(address, inbox, outboxes)
    return process_and_reduce(
        address,
        transport,
        n,
        bounds,
        worker_addresses,
        process,
        combine,
        items,
        _worker_proc_resources(),
        steal=steal,
        emit=emit,
        profiler_factory=profiler_factory,
        close_resources=False,
    )


def http_peer_actor(
    address: str,
    driver_host: str,
    driver_port: int,
    n: int,
    bounds: list[int],
    worker_addresses: tuple[str, ...],
    process: Callable[[Partition, Any], Any],
    combine: Callable[[Any, Any], Any],
    items: Sequence[tuple[int, Partition]],
    steal: bool = True,
    emit: bool = False,
    profiler_factory: Callable[[], Any] | None = None,
) -> dict[str, int]:
    """Module-level (picklable) HTTP actor for ``ProcessExecutor``: bind a loopback server, announce
    ``(host, port)`` to the driver, wait for the assembled registry (buffering any node hand-offs that
    race ahead), then process + peer-reduce. Proves the transport works across real processes."""
    transport = HttpTransport(address)
    transport.set_registry({DRIVER: (driver_host, driver_port)})
    transport.send(DRIVER, ("hello", address, transport.host, transport.port))
    prebuffered: list[tuple[str, Any]] = []
    registry: dict[str, tuple[str, int]] | None = None
    while registry is None:
        got = transport.recv(timeout=0.2)
        if got is None:
            continue
        payload: Any = got[1]
        if payload[0] == "registry":
            registry = payload[1]
        else:
            prebuffered.append((got[0], payload))  # a node/done that raced ahead — keep it
    transport.set_registry(registry)
    try:
        return process_and_reduce(
            address,
            transport,
            n,
            bounds,
            worker_addresses,
            process,
            combine,
            items,
            _worker_proc_resources(),
            steal=steal,
            emit=emit,
            profiler_factory=profiler_factory,
            prebuffered=prebuffered,
            close_resources=False,
        )
    finally:
        transport.close()


def http_driver_handshake(
    driver_transport: HttpTransport, worker_addresses: tuple[str, ...], *, timeout_s: float = 30.0
) -> None:
    """Driver side of the HTTP discovery: gather each worker's announced ``(host, port)``, assemble the
    full registry, and send it to every worker so they can address each other."""

    registry: dict[str, tuple[str, int]] = {DRIVER: (driver_transport.host, driver_transport.port)}
    deadline = time.monotonic() + timeout_s
    while len(registry) <= len(worker_addresses):
        got = driver_transport.recv(timeout=0.1)
        if got is not None:
            payload: Any = got[1]
            if payload[0] == "hello":
                _, addr, host, port = payload
                registry[addr] = (host, port)
                continue
        if time.monotonic() >= deadline:
            raise TimeoutError("HTTP peer workers did not all announce in time")
    driver_transport.set_registry(registry)
    for addr in worker_addresses:
        driver_transport.send(addr, ("registry", registry))
