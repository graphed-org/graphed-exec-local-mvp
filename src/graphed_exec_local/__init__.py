"""graphed-exec-local (plan M7): reference single-machine executors for a `graphed_core.Plan`.

Interchangeable executors — `ThreadExecutor` (thread pool) and two spawn-based process pools — all run
a Plan to one reduced result via a deterministic, straggler-tolerant tree reduction, honor `open_once`
file-locality and stopping conditions, support adaptive reshaping via `next_tasks`, and surface a
remote `StageError` to the driver intact (plan A.3 #8). Single machine only; the published contract
(`graphed_core.execution`) is provisional until exercised by a real distributed adapter.

The two process executors differ ONLY in the peer-reduction IPC pool, chosen explicitly (no silent
switch): `ProcessPoolExecutor` inherits the full queue registry into every worker (simple, the right
default up to ~the fd limit), while `PinnedPoolExecutor` pins each worker to a bounded O(log N) overlay
(O(N log N) registry — for large many-core machines). `ProcessExecutor` is a deprecated alias for
`ProcessPoolExecutor`. See `docs/design.rst` for which to use when.
"""

from __future__ import annotations

from graphed_core.execution import LocalResources

from ._reduce import plan_tree, running_fold, tree_reduce
from .executors import (
    PinnedPoolExecutor,
    ProcessExecutor,
    ProcessPoolExecutor,
    ThreadExecutor,
)

__all__ = [
    "LocalResources",
    "PinnedPoolExecutor",
    "ProcessExecutor",
    "ProcessPoolExecutor",
    "ThreadExecutor",
    "plan_tree",
    "running_fold",
    "tree_reduce",
]
__version__ = "0.0.1"
