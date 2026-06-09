"""Shared, PICKLABLE process/combine functions for the M10 pooled-combines suite (module-level so
a spawned process pool can import them)."""

from __future__ import annotations

import os
import threading
import time

from graphed_core import Partition


def leaf_index(part: Partition, res: object) -> int:
    return part.entry_start


def add_int(a: int, b: int) -> int:
    return a + b


def zero_int() -> int:
    return 0


def leaf_singleton(part: Partition, res: object) -> tuple[object, ...]:
    return (part.entry_start,)


def tree_shape(a: object, b: object) -> tuple[object, ...]:
    """A combine that RECORDS the grouping: the result is the combine tree itself, so two runs (or
    two execution modes) produced the same reduction tree iff the results are equal."""
    return (a, b)


def empty_tuple() -> tuple[object, ...]:
    return ()


def leaf_pid(part: Partition, res: object) -> frozenset[int]:
    return frozenset()


def combine_collect_pid(a: frozenset[int], b: frozenset[int]) -> frozenset[int]:
    """Records WHERE each combine ran: the union of input pids plus this process's pid."""
    return a | b | {os.getpid()}


def empty_pidset() -> frozenset[int]:
    return frozenset()


def leaf_tid(part: Partition, res: object) -> frozenset[int]:
    return frozenset()


def combine_collect_tid(a: frozenset[int], b: frozenset[int]) -> frozenset[int]:
    return a | b | {threading.get_ident()}


def straggler_leaf(part: Partition, res: object) -> int:
    if part.entry_start == 0:
        time.sleep(0.4)
    return 1


def boom_on_three(part: Partition, res: object) -> int:
    if part.entry_start == 3:
        raise ValueError("leaf 3 exploded")
    return 1
