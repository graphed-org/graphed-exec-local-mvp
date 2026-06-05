"""Associative tree reduction (plan M7).

`plan_tree` builds a fixed binary combine-tree over n leaves; `tree_reduce` consumes leaf results in
*whatever order they complete* and fires each combine as soon as BOTH its inputs are ready. Two
consequences the milestone needs:

- **deterministic** result — the combine grouping is fixed by leaf index, so a fixed partition set
  reduces bit-for-bit regardless of completion order;
- **straggler-tolerant** — a slow leaf only blocks the combines on its own path to the root; every
  other subtree reduces independently (no barrier that waits for all leaves first).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import TypeVar

R = TypeVar("R")

# one combine: result node `out` = combine(node `a`, node `b`), with a < b (left-right, deterministic)
Combine = tuple[int, int, int]


def plan_tree(n: int) -> tuple[list[Combine], int | None]:
    """Build the fixed combine-tree over leaves 0..n-1. Returns (combines, root-node-id)."""
    if n <= 0:
        return [], None
    combines: list[Combine] = []
    level = list(range(n))
    next_id = n
    while len(level) > 1:
        nxt: list[int] = []
        i = 0
        while i < len(level):
            if i + 1 < len(level):
                combines.append((next_id, level[i], level[i + 1]))
                nxt.append(next_id)
                next_id += 1
                i += 2
            else:
                nxt.append(level[i])  # an unpaired node carries up unchanged
                i += 1
        level = nxt
    return combines, level[0]


def tree_reduce(
    n: int,
    completed: Iterable[tuple[int, R]],
    combine: Callable[[R, R], R],
    empty: Callable[[], R],
    *,
    on_combine: Callable[[int], None] | None = None,
) -> tuple[R, int]:
    """Reduce n leaves to one value. ``completed`` yields (leaf_index, partial) as leaves finish.
    ``on_combine(leaves_delivered_so_far)`` is called per combine (lets tests prove the reduction is
    incremental, not a barrier). Returns (value, n_combines)."""
    combines, root = plan_tree(n)
    if root is None:
        return empty(), 0

    waiting: dict[int, list[int]] = {}  # input node -> combine indices needing it
    remaining: dict[int, set[int]] = {}  # combine index -> still-unready inputs
    for ci, (_out, a, b) in enumerate(combines):
        remaining[ci] = {a, b}
        waiting.setdefault(a, []).append(ci)
        waiting.setdefault(b, []).append(ci)

    ready: dict[int, R] = {}
    n_combines = 0

    def fire_ready(node: int, work: list[int]) -> None:
        for ci in waiting.get(node, ()):
            remaining[ci].discard(node)
            if not remaining[ci]:
                work.append(ci)

    for leaves_delivered, (leaf, value) in enumerate(completed, start=1):
        ready[leaf] = value
        work: list[int] = []
        fire_ready(leaf, work)
        while work:
            ci = work.pop()
            out, a, b = combines[ci]
            ready[out] = combine(ready[a], ready[b])  # a<b -> deterministic left/right grouping
            n_combines += 1
            if on_combine is not None:
                on_combine(leaves_delivered)
            fire_ready(out, work)

    return ready[root], n_combines


def running_fold(
    completed: Iterator[tuple[int, R]],
    combine: Callable[[R, R], R],
    empty: Callable[[], R],
) -> tuple[R, int]:
    """A degenerate (chain) reduction for the adaptive path, where the partition set is not known up
    front: fold partials in completion order. Requires a commutative+associative ``combine``."""
    acc: R | None = None
    n_combines = 0
    for _key, value in completed:
        if acc is None:
            acc = value
        else:
            acc = combine(acc, value)
            n_combines += 1
    return (empty() if acc is None else acc), n_combines
