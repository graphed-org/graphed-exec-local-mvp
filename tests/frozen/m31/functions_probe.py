"""Module-level numeric process for the M31 equivalence checks (picklable by spawned workers)."""

from __future__ import annotations

import numpy as np
from graphed_core import Partition, Plan, Task


def _sum_proc(partition: Partition, resources: object) -> np.ndarray:
    return np.asarray([partition.entry_start])


def _add(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return a + b


def _zero() -> np.ndarray:
    return np.zeros(1, dtype=int)


def sum_plan(n_tasks: int) -> Plan:
    tasks = tuple(Task(i, Partition("p", "", i * 10, i * 10 + 10)) for i in range(n_tasks))
    return Plan(process=_sum_proc, combine=_add, empty=_zero, tasks=tasks)
