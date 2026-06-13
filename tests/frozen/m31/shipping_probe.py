"""Probe processes for the M31 ship-once tests (importable by spawned workers, like m7's helpers).

``CountingProcess`` records, in a WORKER-GLOBAL list, every time it is unpickled in that worker
(``__setstate__``). Each call returns ``(worker_pid, times_unpickled_in_this_worker)``. With the
ship-once design the process is delivered to each worker exactly once (via the broadcast), so
every call reports a count of 1 no matter how many tasks ran; per-task shipping would make the
count climb. ``payload_bytes`` lets a test inflate the process so the wire cost of re-shipping
would be obvious if it happened.
"""

from __future__ import annotations

import os
import time

_UNPICKLES: list[int] = []  # per worker process: one append per unpickle of a CountingProcess


class CountingProcess:
    def __init__(self, payload_bytes: int = 0) -> None:
        self._payload = b"x" * payload_bytes  # ride-along bulk, to make re-shipping visible

    def __getstate__(self) -> dict[str, object]:
        return {"_payload": self._payload}

    def __setstate__(self, state: dict[str, object]) -> None:
        self._payload = state["_payload"]
        _UNPICKLES.append(os.getpid())  # one entry per unpickle IN THIS WORKER

    def __call__(self, partition: object, resources: object) -> frozenset:
        time.sleep(0.01)  # hold briefly so trivial tasks genuinely spread across workers
        return frozenset({(os.getpid(), len(_UNPICKLES))})


def union(a: frozenset, b: frozenset) -> frozenset:
    return a | b


def empty() -> frozenset:
    return frozenset()
