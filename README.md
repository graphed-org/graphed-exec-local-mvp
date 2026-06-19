# graphed-exec-local

The **reference single-machine executors** for [`graphed`](https://github.com/graphed-org/graphed-mvp)
(milestone M7). The execution *contract* lives in `graphed-core` (`graphed_core.execution`: `Plan`,
`Task`, `Partition`, `Executor`, `Monitor`, `WorkerTransport`); this repo implements it for one
machine and is the executor the integration suites run real analyses through — thousands of tiny
tasks, deliberate stragglers, worker crashes. Part of the
[graphed-org](https://github.com/graphed-org) project.

A `Plan` is four pieces the executor consumes but never interprets: `process(partition, resources)`
does one partition's work, `combine(R, R)` merges two partials associatively, `empty()` is the
identity, and `tasks` is the fixed partition set. The executor runs them to **one reduced result**.

## The executors

All executors share one driver and differ only in the worker pool and the resource plumbing:

- **`ThreadExecutor`** — a thread pool; workers share the driver's address space, `open_once`
  resources are thread-local.
- **`ProcessPoolExecutor`** — a **spawn**-based process pool (spawn deliberately: forked CPython
  inherits lock/allocator state that bites at scale, and spawn is the semantics every platform and
  free-threaded build shares). `open_once` resources are a per-process global installed by the pool
  initializer; `process`/`combine`/`empty` must be picklable.
- **`PinnedPoolExecutor`** — the same process semantics with a bounded peer-communication footprint
  for large many-core machines (see below).
- **`ProcessExecutor`** — a **deprecated alias** for `ProcessPoolExecutor`.

```python
import numpy as np
from graphed_core import Partition, Plan, Task
from graphed_exec_local import ProcessPoolExecutor

def count(partition, resources):          # module-level => picklable
    return np.asarray([partition.entry_stop - partition.entry_start])

def add(a, b): return a + b
def zero():    return np.zeros(1, dtype=int)

parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
plan  = Plan(process=count, combine=add, empty=zero,
             tasks=tuple(Task(i, p) for i, p in enumerate(parts)))

ProcessPoolExecutor(max_workers=4).run(plan).value     # -> array([700])
```

## Deterministic *and* straggler-tolerant tree reduction

The heart of the package (`_reduce.py`): `plan_tree(n)` builds a fixed binary combine-tree **over
leaf indices** before anything runs; `tree_reduce` then consumes leaf results in **whatever order
they complete** and fires each combine the moment both inputs exist. This sidesteps the usual
either/or — combine-in-completion-order (fast but float results depend on who finished first) versus
wait-for-all-then-reduce (deterministic but a straggler stalls everything):

- **Determinism** — the grouping is a pure function of the leaf count, so the result is bit-for-bit
  identical regardless of completion order, worker count, or executor class.
- **Straggler tolerance** — a slow partition blocks only the `log n` combines on its own root-path;
  every other subtree reduces meanwhile. No barrier.

By default combines run on the driver thread as results arrive. `pooled_combines=True` schedules them
onto the same worker pool (same fixed pairing, identical result) for workloads whose partials are
heavy enough that a serial driver-side merge becomes the bottleneck.

## Inter-worker comms: peer reduction + work-stealing (M38)

By **default** (`comms="ipc"`) the reduction runs **across the workers, off the driver**, over the
`graphed_core.execution.WorkerTransport` seam (`_transport.py`) — an addressable, non-blocking,
best-effort channel. Two backends: **IPC** (`QueueTransport` over `multiprocessing.SimpleQueue`
inboxes, no `Manager` server in the data path) for one machine, and **HTTP** (loopback HTTP server +
a discovery handshake) as the path a real distributed scheduler reuses.

- **Peer reduction** (`_peer.py`) — each worker owns a contiguous leaf range and reduces it with the
  lazy index tree (`LazyReducer`, the same fixed `plan_tree` computed by index arithmetic so N can be
  huge with no O(N) pre-pass). Partials straddling a range boundary are handed worker→worker by
  ownership. Every node keeps its **global** `(level, pos)` identity, so distributing the combines
  never changes the grouping — **bit-for-bit identical to the driver-hub path**, even for
  non-associative float histograms. The driver only collects the root.
- **Work-stealing** (`steal=True`, peer only) — an idle worker steals **one** leaf from a busy peer's
  far end (Blumofe–Leiserson, not steal-half). Only the `process` work moves; the leaf's owner still
  reduces it, so the tree and the result are unchanged. Idle delay + exponential backoff make it free
  on balanced loads.

The two process executors differ **only** in the peer pool, chosen explicitly by class (no silent
switch): `ProcessPoolExecutor` inherits the full registry into every worker (O(N²) fds — the right,
fastest default up to roughly the fd limit), while `PinnedPoolExecutor` pins each worker to a bounded
O(log N) overlay (reduction targets + a hypercube lifeline graph), giving an O(N log N) registry for
large many-core machines or low `RLIMIT_NOFILE`. `ProcessPoolExecutor` *warns* and points you at
`PinnedPoolExecutor` when its worker count would strain the fd budget — it warns rather than
switching, so the pool in use is always the one named at the call site.

`comms=None` selects the legacy driver-hub reduction (still the path for `pooled_combines`); peer
**refuses** `pooled_combines` loudly rather than silently degrading to the hub.

## Errors cross the boundary intact

A worker exception propagates to the driver as the exception it was. A `graphed_debug.StageError` —
picklable by design — re-raises in the driver carrying the failing op, the user's source frames, and
the failing partition. "Remote errors are opaque strings" (plan §A.3 #8) is the legacy failure this
stack was built against; the integration suite pins the round trip under both pools.

## Adaptive plans

A `Plan` with a `next_tasks` hook runs as a **running fold** instead of a fixed tree: the driver
folds results as they complete and periodically consults `next_tasks(ExecContext)` — which sees
elapsed time, completed counts, and errors — to obtain more partitions or a stop reason. This is the
seam for timing-aware partitioning; the fixed tree remains the path for known partition sets, where
determinism matters most.

## Persistent pools and broadcast cache

`persistent=True` keeps one process pool across `run()` calls (amortizing the import-heavy spawn over
many plans — notebooks, sweeps), released by `close()` or context-manager exit with lazy respawn:

```python
with ProcessPoolExecutor(max_workers=4, persistent=True) as ex:
    for plan in plans:           # one spawn, amortized over every plan
        results.append(ex.run(plan).value)
```

The process executors **ship the `process` callable to each worker once** (M31) and keep a
**FIFO-bounded per-worker broadcast cache** (M31/M34) kept in lockstep with the driver's token set,
so large shared payloads are not re-pickled per task.

## Live observability: the monitor seam (M37)

Every executor accepts an optional `monitor=` — a passive `graphed_core.execution.Monitor` that
*watches* a run without changing it. Each task emits one `SUBMITTED` (driver-side), then `STARTED`,
then exactly one of `FINISHED` / `ERRORED` (worker-side). The thread pool calls the monitor in
process; the process/peer paths forward worker events over a bounded queue drained by a driver-side
collector thread. Emission is **best-effort, drop-on-full** — a full queue or a slow monitor drops
events and never back-pressures a worker — so the reduced result and the combine count are
byte-identical whether or not a monitor (even a profiling one) is attached.

The monitor is whatever you pass: a tiny recorder in a test, or the live
[`graphed_debug.Dashboard`](https://github.com/graphed-org/graphed-debug-mvp) for a web view of a
running analysis (flamegraph included; it works under the hub and the peer paths alike):

```python
from graphed_debug import Dashboard
from graphed_exec_local import ProcessPoolExecutor

with Dashboard(profile=True) as dash:
    ProcessPoolExecutor(monitor=dash.monitor).run(plan)
```

## Install

```bash
pip install "graphed-core @ git+https://github.com/graphed-org/graphed-core-mvp@main"  # needs Rust
pip install "graphed @ git+https://github.com/graphed-org/graphed-mvp@main"
pip install "graphed-debug @ git+https://github.com/graphed-org/graphed-debug-mvp@main"
pip install -e ".[dev,docs]"
```

## Develop

```bash
uvx prek run --all-files        # ruff (lint + format) + mypy, via .pre-commit-config.yaml
pytest tests/frozen             # the frozen acceptance suite
sphinx-build -W -b html docs docs/_build/html
```

`docs/design.rst` is the engineering walkthrough; `docs/api.rst` is the API reference, generated
automatically from the package by `sphinx.ext.autosummary` so it never drifts from the code.

Status: see `.graphed/state.json` and `CLAUDE.md`. Defers to the root `graphed-project/CLAUDE.md`;
the project plan always wins.
