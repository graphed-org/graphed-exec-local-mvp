"""graphed-exec-local (plan M7): reference single-machine executors for a `graphed_core.Plan`.

Two interchangeable executors — `ThreadExecutor` (thread pool) and `ProcessExecutor` (spawn-based
process pool) — both run a Plan to one reduced result via a deterministic, straggler-tolerant tree
reduction, honor `open_once` file-locality and stopping conditions, support adaptive reshaping via
`next_tasks`, and surface a remote `StageError` to the driver intact (plan A.3 #8). Single machine
only; the published contract (`graphed_core.execution`) is provisional until exercised by a real
distributed adapter.
"""

from __future__ import annotations

from ._reduce import plan_tree, running_fold, tree_reduce
from .executors import ProcessExecutor, ThreadExecutor
from .resources import LocalResources

__all__ = [
    "LocalResources",
    "ProcessExecutor",
    "ThreadExecutor",
    "plan_tree",
    "running_fold",
    "tree_reduce",
]
__version__ = "0.0.1"
