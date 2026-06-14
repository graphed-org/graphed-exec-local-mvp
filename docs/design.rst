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


Phase 2 (deliberately not built)
--------------------------------

* **Distributed executors** (TaskVine / HTCondor / Slurm / Dask) — the entire point of the
  ``Plan`` contract is that they can be written against it later; the MVP is single-machine
  only.
* **Work stealing between pools** and NUMA-aware placement.
* **Adaptive chunk-size policies** shipped as library code (the ``next_tasks`` hook exists;
  policies beyond tests are user-land for now).
* **Per-query resource hints** (memory-bound combinatoric stages want fewer concurrent
  workers — observed empirically on trijet workloads; the executor currently treats all plans
  alike).

See :doc:`improvements` for the live tracked list.
