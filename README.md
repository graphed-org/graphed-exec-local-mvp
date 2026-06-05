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

Defers to the root `graphed-project/CLAUDE.md`; the project plan always wins.
