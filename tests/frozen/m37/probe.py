"""Module-level (picklable) plan callables for the M37 emit suite — importable by spawned workers."""

from __future__ import annotations

from graphed_core import Partition


def count_entries(partition: Partition, resources: object) -> int:
    return partition.n_entries


def add(a: int, b: int) -> int:
    return a + b


def addf(a: float, b: float) -> float:
    return a + b


def boom(partition: Partition, resources: object) -> int:
    raise ValueError(f"boom on {partition.uri}")


def cpu_work(partition: Partition, resources: object) -> float:
    """Deliberately heavy (tens of ms) so the statistical sampler reliably captures frames."""
    s = 0.0
    for i in range(partition.n_entries * 30000):
        s += (i % 7) ** 0.5
    return s
