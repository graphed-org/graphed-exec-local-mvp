"""Per-worker resources (plan M7): the ``open_once`` file-locality directive.

Each worker (thread or process) gets its own `WorkerResources`; ``open_once(uri, opener)`` opens a
uri at most once per worker and reuses the handle for that worker's later chunks. A counting opener
in the suite asserts a multi-chunk single-file task opens the file exactly once per worker.
"""

from __future__ import annotations

from collections.abc import Callable


class LocalResources:
    """A `graphed_core.WorkerResources` implementation backed by a per-worker handle cache."""

    def __init__(self) -> None:
        self._handles: dict[str, object] = {}
        self.open_count = 0  # how many real opens this worker performed (test introspection)

    def open_once(self, uri: str, opener: Callable[[str], object]) -> object:
        if uri not in self._handles:
            self._handles[uri] = opener(uri)
            self.open_count += 1
        return self._handles[uri]
