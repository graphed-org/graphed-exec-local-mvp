"""M37 capstone (graphed-exec-local slice): a real ``ProcessExecutor`` run with a live ``Dashboard``
attached — the cross-process + network plumbing end-to-end. Worker events forward over the in-process
side-queue to the driver collector, then over a **websocket** to the Perspective ``DashboardServer``;
per-worker off-thread sampler stack-trees ride the same path and the server merges them into one
flamegraph. The reduced result is **unchanged** by the dashboard's presence.

graphed-debug is a runtime dependency of this package, so importing ``Dashboard`` is in-deps (R13.8);
the dashboard *extra* (perspective/tornado/websocket) is gated with ``importorskip`` so a leg without
those wheels (e.g. free-threaded 3.14t) skips this cleanly.
"""

from __future__ import annotations

import time

import pytest

pytest.importorskip("perspective")
pytest.importorskip("websocket")

from graphed_core import Partition, Plan, Task
from graphed_debug import Dashboard
from probe import addf, cpu_work

from graphed_exec_local.executors import ProcessExecutor


def _plan(n: int = 6) -> Plan[float]:
    tasks = [Task(k, Partition(f"f{k}.root", "Events", 0, 8 + k)) for k in range(n)]
    return Plan(process=cpu_work, combine=addf, empty=lambda: 0.0, tasks=tasks)


def test_process_executor_with_dashboard_over_the_network() -> None:
    plan = _plan(6)
    bare = ProcessExecutor(max_workers=2).run(plan).value

    with Dashboard(profile=True) as dash:
        with ProcessExecutor(max_workers=2, monitor=dash.monitor, persistent=True) as ex:
            observed = ex.run(plan).value
        snap = dash.wait_for(finished=6, timeout=30)

        # passivity: streaming over the websocket changed nothing
        assert abs(observed - bare) < 1e-9
        # the full event stream crossed process boundary + network to the Perspective server
        assert snap["stats"]["submitted"] == 6
        assert snap["stats"]["finished"] == 6
        assert snap["stats"]["errored"] == 0
        assert snap["stats"]["inflight"] == 0

        # the off-thread sampler's stack-trees traversed the same transport and merged into the
        # server flamegraph (statistical, so poll briefly; the work is heavy enough to reliably sample)
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and dash.snapshot()["profile_samples"] == 0:
            time.sleep(0.05)
        assert dash.snapshot()["profile_samples"] > 0
        assert dash.server.flamegraph()["value"] > 0
