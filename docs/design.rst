How graphed-exec-local works
============================

``graphed-exec-local`` is the reference executor: it takes a ``graphed_core.Plan`` — process
each partition, combine the partials, start from empty — and runs it on one machine with a
thread pool or a process pool, producing one reduced result. "Reference" does not mean toy:
this is the executor the integration suites run real analyses through (thousands of tiny
tasks, deliberate stragglers, worker crashes), and its semantics — determinism under any
completion order, straggler tolerance, errors that survive the process boundary — are the
contract any future distributed executor must match.

.. contents::
   :local:
   :depth: 2


The Plan contract
-----------------

An executor consumes, and never interprets, four things::

    Plan(process = f(partition, resources) -> R,    # one partition's work
         combine = f(R, R) -> R,                    # associative merge
         empty   = f() -> R,                        # the identity
         tasks   = (Task(key, partition), ...))     # the fixed partition set

``process``/``combine``/``empty`` must be picklable for the process pool (module-level
functions, ``functools.partial`` of them, or frozen dataclasses — the conventions every
graphed writer/aggregator follows). ``resources.open_once(uri, opener)`` gives workers
file-handle reuse: thread-local sets for the thread pool, a per-process global installed by the
pool initializer for the process pool. An optional ``next_tasks`` hook switches the driver into
adaptive mode (below).

A minimal, runnable plan::

    import numpy as np
    from graphed_core import Partition, Plan, Task
    from graphed_exec_local import ProcessExecutor

    def count(partition, resources):          # module-level: picklable
        return np.asarray([partition.entry_stop - partition.entry_start])

    def add(a, b):  return a + b
    def zero():     return np.zeros(1, dtype=int)

    parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
    plan  = Plan(process=count, combine=add, empty=zero,
                 tasks=tuple(Task(i, p) for i, p in enumerate(parts)))

    ProcessExecutor(max_workers=4).run(plan).value     # -> array([700])


The fixed combine tree: deterministic *and* straggler-tolerant
--------------------------------------------------------------

The heart of the package is ``plan_tree`` + ``tree_reduce``, and the design resolves a tension
worth spelling out.

*Naively*, you either combine results in completion order (fast, but floating-point results
then depend on which worker finished first — non-deterministic), or you wait for all leaves
and reduce in index order (deterministic, but one straggler stalls everything).

The fixed tree does neither. ``plan_tree(n)`` builds a binary combine-tree **over leaf
indices** — pairing (0,1), (2,3), … level by level — *before* anything runs. ``tree_reduce``
then consumes leaf results in **whatever order they complete** and fires each combine the
moment both of its inputs exist. Consequences:

* **Determinism**: the grouping is a pure function of the leaf count, so the result is
  bit-for-bit identical regardless of completion order, worker count, or executor class. (For
  float-summing combines this is what makes "deterministic per configuration" a theorem rather
  than a hope; integer-counting combines are exact under any tree at all.)
* **Straggler tolerance**: a slow partition blocks only the ``log n`` combines on its own
  root-path; every other subtree reduces to completion meanwhile. There is no barrier. The
  frozen suite pins this with a deliberately slow leaf and a probe asserting that combines
  keep firing while it sleeps.

By default combines run on the driver thread as results arrive — fine when partials are small.
``pooled_combines=True`` schedules the combines onto the *same worker pool* as the leaves
(same fixed pairing, so results are unchanged), for workloads whose partials are heavy enough
that a serial driver-side merge becomes the bottleneck — large histograms over many
partitions, concatenated path lists, and the like.

The two pools
-------------

``ThreadExecutor`` and ``ProcessExecutor`` share one driver; they differ only in the
``concurrent.futures`` pool and the resource plumbing. The process pool uses the **spawn**
context deliberately: forked CPython processes inherit lock and allocator state that bites
exactly when you scale, and spawn is the semantics every platform shares. The cost is an
import-heavy worker startup, which leads to:

**Persistent pools.** By default each ``run()`` spawns a fresh pool — the right default for
isolation, and the pinned historical behavior. But a notebook running eight small plans, or a
benchmark sweep running hundreds, pays that import-heavy spawn per plan and can end up *slower
parallel than sequential*. ``ProcessExecutor(max_workers=4, persistent=True)`` keeps one pool
across ``run()`` calls (worker state demonstrably survives between runs — that is the test),
released by ``close()`` or context-manager exit, with lazy respawn afterwards::

    with ProcessExecutor(max_workers=4, persistent=True) as ex:
        for plan in plans:           # one spawn, amortized over every plan
            results.append(ex.run(plan).value)

Errors cross the boundary intact
--------------------------------

A worker exception propagates to the driver as the exception it was. In particular a
``graphed_debug.StageError`` — which is picklable by design — re-raises in the driver carrying
the failing op, the user's source frames, and the failing partition. The executor adds nothing
and strips nothing; "remote errors are opaque strings" is the legacy failure this stack was
built against, and the integration suite pins the round trip.

Adaptive plans
--------------

A plan with ``next_tasks`` runs as a **running fold** instead of a fixed tree: the driver
folds results as they complete and periodically consults ``next_tasks(ExecContext)`` — which
sees elapsed time, completed counts, and errors — to obtain more partitions or a
``StopReason``. This is the seam for timing-aware partitioning (grow chunk sizes as observed
throughput stabilizes) without changing the executor; the fixed tree remains the path for
known partition sets, where determinism matters most.


Live observability: the monitor seam (M37)
------------------------------------------

Every executor accepts an optional ``monitor=`` — a passive ``graphed_core.execution.Monitor`` that
*watches* a run. It is the seam a live dashboard plugs into (see ``graphed-debug``'s ``Dashboard``),
but the executor knows nothing about rendering or transport: it only emits a small, picklable
``TaskEvent`` vocabulary.

The lifecycle of one task is three events: the driver emits ``SUBMITTED`` when it hands the task to
the pool; the worker emits ``STARTED`` before running it and exactly one of ``FINISHED`` / ``ERRORED``
after. Where those worker events go differs by pool, and this is the interesting part:

* **Thread pool** — workers share the driver's address space, so they call the monitor directly.
* **Process pool** — workers cannot reach the driver's monitor object, so they push events onto a
  bounded ``multiprocessing.Manager().Queue()``; a **driver-side collector daemon thread** drains it
  and replays them into the monitor. (The driver still emits ``SUBMITTED`` locally.) A per-worker
  statistical profiler, if one is supplied via the monitor's ``worker_profiler_factory``, rides the
  same queue.

The non-negotiable property is **passivity**: emission is best-effort and *drop-on-full*. If the
monitor is slow or its queue is full, events are dropped — never buffered into back-pressure that
would change task timing (and thus the adaptive ``next_tasks`` path) or stall a worker. A monitor that
raises is swallowed. The upshot, pinned by the suite: a run's ``ExecResult.value`` and combine count
are byte-identical whether or not a monitor (even a profiling one) is attached. Observability here is
strictly a side channel, never part of the computation.


Inter-worker comms: peer reduction + work-stealing (M38)
--------------------------------------------------------

By default (``comms="ipc"``) the reduction runs **across the workers, off the driver**. The seam is
:class:`graphed_core.execution.WorkerTransport` — an addressable, non-blocking, best-effort message
channel — with two backends: **IPC** (``QueueTransport`` over ``multiprocessing.SimpleQueue`` inboxes,
one per address, no ``Manager`` server in the data path) for a single machine, and **HTTP** (loopback
``http.server`` + a discovery handshake; ``HttpTransport``) as the path a real distributed scheduler
reuses. Determinism is *not* the transport's job; it is the reduction protocol's.

The IPC path has **two worker pools, and you pick which by choosing the executor class** — there is no
silent runtime switch. :class:`~graphed_exec_local.ProcessPoolExecutor` (the default; original M7
behaviour) uses a full-registry pool: every worker *inherits the whole registry* (O(N²) fds — fine
while N is well under the per-process fd limit, and the fast common path). :class:`~graphed_exec_local.PinnedPoolExecutor`
uses a ``PinnedProcessPool`` of **identity-pinned** workers that each inherit ONLY their inbox + the
O(log N) outboxes of their *overlay* peers (reduction targets + a symmetric **hypercube lifeline**
graph + driver, ``worker_outbox_addresses``), so the registry is O(N log N), not O(N²). Both bound
work-stealing to the lifelines, and both produce **bit-for-bit identical** results — only the
communication footprint differs.

**Which to use.** Default to ``ProcessPoolExecutor``: it is the simplest and is fastest up to roughly
the fd limit. Reach for ``PinnedPoolExecutor`` on large many-core machines (>~128 cores, or any low
``RLIMIT_NOFILE``), where the full registry's O(N²) descriptors would exhaust the limit. So you are not
surprised, ``ProcessPoolExecutor`` *warns* (via the advisory predicate ``_exceeds_fd_budget``) and
points you at ``PinnedPoolExecutor`` when its worker count would strain the budget — it warns rather
than switching, so the pool in use is always the one named at the call site. ``ProcessExecutor`` remains
as a **deprecated alias** for ``ProcessPoolExecutor``. (A *dynamic* cluster — workers joining/dying —
needs a lazy-connect transport + multi-hop routing over this same overlay: the Phase-2 distributed
runtime, which reuses ``worker_outbox_addresses``.)

* **Peer reduction** (``_peer.py``). Each worker owns a contiguous **leaf range** and reduces it with
  the lazy index tree (``_reduce.LazyReducer`` — the same fixed ``plan_tree``, computed by index
  arithmetic, frontier-bounded so N can be huge with no O(N) pre-pass). Partials that straddle a range
  boundary are handed **worker→worker** by ownership (a segment-tree merge: node ``(level,pos)`` is
  owned by the worker holding its leftmost leaf; an odd node is shipped to its parent's owner). Every
  node keeps its **global** ``(level,pos)`` identity, so distributing the *combines* never changes the
  *grouping* — the result is **bit-for-bit identical to the old driver-hub path even for
  non-associative float histograms**. The driver only collects the root (a ``done`` broadcast
  terminates); a worker failure is detected promptly and re-raised intact (the M7 obligation). On the
  real ADL benchmark this is within noise of the hub — the driver is no longer the combine bottleneck.
* **Work-stealing**. An idle worker steals **one** leaf from a busy peer's far end
  (Blumofe–Leiserson/Cilk — *not* steal-half, which under many idle thieves drains a victim
  geometrically and over-concentrates work). Stealing redistributes only the ``process`` work — the
  leaf's **owner still reduces it** (the thief ships the partial back), so the tree and the result are
  unchanged. An idle delay + exponential backoff make it free on balanced loads (no spurious steals)
  while rebalancing a genuine straggler.
* **Parity with the hub.** Peer emits the full monitor lifecycle (SUBMITTED/STARTED/FINISHED/ERRORED +
  the combine count) and runs the off-thread profiler, so the live dashboard — flamegraph included —
  works under peer. ``comms=None`` selects the legacy driver-hub path (still used for
  ``pooled_combines`` and the broadcast-cache tests); peer **refuses** ``pooled_combines`` loudly
  rather than silently degrading to hub.


Phase 2 (deliberately not built)
--------------------------------

* **Distributed executors** (TaskVine / HTCondor / Slurm / Dask) — the ``Plan`` contract *and* the
  ``WorkerTransport`` seam are built so they can be written against later; the MVP is single-machine.
* NUMA-aware placement.
* **Adaptive chunk-size policies** shipped as library code (the ``next_tasks`` hook exists;
  policies beyond tests are user-land for now).
* **Per-query resource hints** (memory-bound combinatoric stages want fewer concurrent
  workers — observed empirically on trijet workloads; the executor currently treats all plans
  alike).

See :doc:`improvements` for the live tracked list.
