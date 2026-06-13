"""M37 capstone (graphed-exec-local slice): a real ``ProcessExecutor`` run with a live ``Dashboard``
attached — the cross-process plumbing end-to-end. Worker events forward over the side-queue to the
driver-side collector; the statistical sampler's per-worker sessions merge into one flamegraph; and
the reduced result is **unchanged** by the dashboard's presence.

graphed-debug is a runtime dependency of this package, so importing ``Dashboard`` here is in-deps
(R13.8). pyinstrument is in the dev extra, so ``profile=True`` is exercised.
"""

from __future__ import annotations

from graphed_core import Partition, Plan, Task
from graphed_debug import Dashboard
from probe import addf, cpu_work

from graphed_exec_local.executors import ProcessExecutor


def _plan(n: int = 6) -> Plan[float]:
    tasks = [Task(k, Partition(f"f{k}.root", "Events", 0, 8 + k)) for k in range(n)]
    return Plan(process=cpu_work, combine=addf, empty=lambda: 0.0, tasks=tasks)


def test_process_executor_with_dashboard_is_passive_and_profiles() -> None:
    plan = _plan(6)
    bare = ProcessExecutor(max_workers=2).run(plan).value

    dash = Dashboard(profile=True)  # not start()ed: this exercises the Monitor + sampler plumbing
    with ProcessExecutor(max_workers=2, monitor=dash, persistent=True) as ex:
        observed = ex.run(plan).value
    snap = dash.snapshot()

    # passivity: the dashboard changed nothing
    assert abs(observed - bare) < 1e-9
    # the full event stream reached the driver across the process boundary
    assert snap["counters"]["submitted"] == 6
    assert snap["counters"]["finished"] == 6
    assert snap["counters"]["errored"] == 0
    assert snap["inflight"] == 0
    worker_pids = [w for w in snap["workers"] if w != "driver"]
    assert len(worker_pids) >= 1  # >=1 worker process observed

    # the flamegraph is a well-formed {name,value,children} tree
    flame = snap["flame"]
    assert set(flame) == {"name", "value", "children"}
    if snap["profile"]:  # pyinstrument installed -> a non-empty merged profile arrived
        assert flame["children"], "profiling produced no frames despite heavy worker work"
        assert flame["value"] > 0
