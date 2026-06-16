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
from typing import Generic, TypeVar

R = TypeVar("R")
_MISSING = object()

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


class LazyReducer(Generic[R]):
    """A **lazy** deterministic tree reducer (M38): the SAME fixed binary tree as :func:`plan_tree`,
    but its combine partners are computed by index arithmetic on the fly — it never materialises the
    combine list / waiting-sets, so N can be huge without an O(N) pre-pass.

    A partial enters at leaf position ``leaf`` (level 0). A node at ``(level, pos)`` combines with its
    sibling ``pos ^ 1`` to form ``(level+1, pos >> 1)``, with the **even** position the left operand
    and the **odd** the right — the exact left/right grouping of :func:`plan_tree`, so the result is
    bit-for-bit identical regardless of the order leaves are fed. A node whose sibling position lies
    past the (odd-sized) level's end is *unpaired* and carries up unchanged. Only nodes still waiting
    for a sibling are held (``present``) — the live **frontier**, O(log N) for roughly in-order
    completion, never the whole tree. Peer reduction (P3) reuses this per worker over a leaf range."""

    def __init__(
        self,
        n: int,
        combine: Callable[[R, R], R],
        empty: Callable[[], R],
        *,
        on_combine: Callable[[int], None] | None = None,
    ) -> None:
        self.n = n
        self._combine = combine
        self._empty = empty
        self._on_combine = on_combine
        self._present: dict[tuple[int, int], R] = {}
        self.n_combines = 0
        self.delivered = 0
        self.max_frontier = 0  # largest live frontier seen (test/diagnostic: proves no O(N) blowup)

    def _level_size(self, level: int) -> int:
        return (self.n + (1 << level) - 1) >> level

    def feed(self, leaf: int, value: R) -> None:
        """Deliver one partial (leaf index + value), bubbling it up as far as siblings allow."""
        self.delivered += 1
        level, pos = 0, leaf
        present = self._present
        while True:
            if self._level_size(level) == 1:  # reached the root
                present[(level, pos)] = value
                break
            if pos % 2 == 0:
                if pos + 1 >= self._level_size(level):  # unpaired (last node of an odd level) -> carry
                    level, pos = level + 1, pos >> 1
                    continue
                sib = pos + 1
            else:
                sib = pos - 1
            other = present.pop((level, sib), _MISSING)
            if other is _MISSING:  # sibling not here yet -> park on the frontier and wait
                present[(level, pos)] = value
                break
            left, right = (value, other) if pos % 2 == 0 else (other, value)
            value = self._combine(left, right)  # type: ignore[arg-type]  # _MISSING handled above
            self.n_combines += 1
            if self._on_combine is not None:
                self._on_combine(self.delivered)
            level, pos = level + 1, pos >> 1
        if len(present) > self.max_frontier:
            self.max_frontier = len(present)

    def result(self) -> R:
        """The reduced value. Valid once all ``n`` leaves have been fed (the root has formed)."""
        if self.n == 0:
            return self._empty()
        level = 0
        while self._level_size(level) > 1:
            level += 1
        return self._present[(level, 0)]


def lazy_tree_reduce(
    n: int,
    completed: Iterable[tuple[int, R]],
    combine: Callable[[R, R], R],
    empty: Callable[[], R],
    *,
    on_combine: Callable[[int], None] | None = None,
) -> tuple[R, int]:
    """Lazy equivalent of :func:`tree_reduce` (same bit-for-bit result, no pre-built combine graph)."""
    reducer: LazyReducer[R] = LazyReducer(n, combine, empty, on_combine=on_combine)
    for leaf, value in completed:
        reducer.feed(leaf, value)
    return reducer.result() if n else empty(), reducer.n_combines


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
