"""M34 — bounded shared-process cache, broadcast coverage, and the LocalResources dedup.

The M31 ship-once broadcast cached the process per worker but never evicted: a persistent pool
over many distinct plans accumulated every compiled-IR-bearing process for the workers' whole
lifetime. M34 bounds that cache (FIFO, cap _SHARED_CACHE_CAP) in lockstep with the driver's
broadcast-token set, asserts full worker coverage before caching a token (never a silent
under-prime), and reuses the one canonical bounded LocalResources from graphed_core (P3-6/P0-1).

M38: these broadcast-cache tests pin ``comms=None`` (the hub path). The ship-once broadcast is a
hub mechanism — peer reduction ships the process to each worker directly (no per-worker token cache) —
so after peer became the default ``comms``, the broadcast cache is exercised explicitly via ``comms=None``.
"""

from __future__ import annotations

import cache_probe
import graphed_core
from graphed_core.execution import Partition, Plan, Task

import graphed_exec_local
from graphed_exec_local import ProcessExecutor
from graphed_exec_local.executors import _SHARED_CACHE_CAP


def _plan(process, n_tasks=2):  # type: ignore[no-untyped-def]
    tasks = tuple(Task(i, Partition("p", "", i, i + 1)) for i in range(n_tasks))
    return Plan(process=process, combine=cache_probe.add, empty=cache_probe.zero, tasks=tasks)


def test_local_resources_is_the_canonical_core_one() -> None:
    assert graphed_exec_local.LocalResources is graphed_core.LocalResources  # no duplicate class


def test_shared_cache_stays_bounded_across_many_distinct_plans() -> None:
    with ProcessExecutor(comms=None, max_workers=2, persistent=True) as ex:
        for i in range(_SHARED_CACHE_CAP + 6):  # more distinct processes than the cap
            assert ex.run(_plan(cache_probe.Tagged(i))).value == cache_probe.Tagged(i).i * 2
        size = ex.run(_plan(cache_probe.cache_size)).value // 2  # both tasks return the same size
        assert size <= _SHARED_CACHE_CAP  # the cache never grew without bound
        assert size >= _SHARED_CACHE_CAP - 2  # ... and it DID fill+evict (not merely never-filled)


def test_an_evicted_plan_still_runs_correctly_after_rebroadcast() -> None:
    with ProcessExecutor(comms=None, max_workers=2, persistent=True) as ex:
        first = cache_probe.Tagged(0)
        ex.run(_plan(first))
        for i in range(1, _SHARED_CACHE_CAP + 4):  # evict `first` from the cache
            ex.run(_plan(cache_probe.Tagged(i)))
        # re-running the evicted plan re-broadcasts transparently and still computes correctly
        assert ex.run(_plan(first)).value == 0


def test_broadcast_covers_every_worker() -> None:
    # each worker that runs a task must have been primed (else _proc_task_shared KeyErrors);
    # a clean run over more tasks than workers exercises full coverage
    with ProcessExecutor(comms=None, max_workers=4, persistent=True) as ex:
        assert ex.run(_plan(cache_probe.Tagged(7), n_tasks=40)).value == 7 * 40
