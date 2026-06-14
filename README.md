# graphed-exec-local

Reference single-machine executors for **graphed** (milestone M7). Two interchangeable executors run a
`graphed_core.Plan` to one reduced result:

- **`ThreadExecutor`** — a thread-safe worker pool (thread-local `open_once` resources).
- **`ProcessExecutor`** — a spawn-based process pool (per-process resources; picklable tasks).

Both use a **deterministic, straggler-tolerant tree reduction** (a fixed combine-tree by partition
key → bit-for-bit results; a slow leaf blocks only its own path, never the whole reduction), honor
`open_once` file-locality and stopping conditions, support adaptive reshaping via `next_tasks`, and
surface a remote `graphed_debug.StageError` to the driver intact — never an opaque worker traceback
(plan A.3 #8). Single machine only; the published execution contract is provisional.

## Live observability (M37)

Every executor takes an optional **`monitor=`** — a passive `graphed_core.execution.Monitor` that
*observes* a run without changing it. Each task emits one `SUBMITTED` (driver-side), then `STARTED`,
then exactly one of `FINISHED` / `ERRORED` (worker-side); the thread pool calls the monitor in
process, while the process pool forwards worker events over a bounded `Manager().Queue()` drained by
a driver-side collector thread. Emission is **best-effort** — a full queue or a slow monitor drops
events and never back-pressures a worker — so attaching a monitor leaves the reduced result and the
combine count byte-identical (the determinism gate is green attached-or-not).

The monitor is whatever you pass: a tiny recorder in a test, or the live
[`graphed_debug.Dashboard`](https://github.com/graphed-org/graphed-debug-mvp) for a web view of a
running analysis (it works on `ThreadExecutor`, `ProcessExecutor`, or any future executor):

```python
from graphed_debug import Dashboard
from graphed_exec_local import ProcessExecutor

with Dashboard(profile=True) as dash:
    ProcessExecutor(monitor=dash.monitor).run(plan)
```

Defers to the root `graphed-project/CLAUDE.md`; the project plan always wins.
