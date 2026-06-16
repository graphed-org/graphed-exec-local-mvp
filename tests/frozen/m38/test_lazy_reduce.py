"""M38 lazy reduction (spike; frozen at P6). The lazy index-arithmetic reducer must produce the
EXACT same tree grouping as the existing ``tree_reduce`` (bit-for-bit, any completion order) while
holding only an O(log N) frontier — never materialising the whole combine graph."""

from __future__ import annotations

import math
import random

import pytest

from graphed_exec_local._reduce import LazyReducer, lazy_tree_reduce, tree_reduce


# a NON-commutative associative combine: parenthesised concatenation. Equality then proves the
# left/right *grouping* matches, not just the multiset of leaves.
def _cat(a: str, b: str) -> str:
    return f"({a}+{b})"


def _empty() -> str:
    return "e"


def _ref(n: int) -> str:
    """Ground truth: the current tree_reduce, fed in leaf order."""
    return tree_reduce(n, [(i, str(i)) for i in range(n)], _cat, _empty)[0]


@pytest.mark.parametrize("n", [0, 1, 2, 3, 4, 5, 7, 8, 15, 16, 17, 31, 64, 100])
def test_lazy_matches_tree_reduce_in_any_order(n: int) -> None:
    ref = _ref(n)
    for seed in range(25):
        order = list(range(n))
        random.Random(seed).shuffle(order)
        val, nc = lazy_tree_reduce(n, [(i, str(i)) for i in order], _cat, _empty)
        assert val == ref, f"n={n} seed={seed}: {val} != {ref}"  # identical grouping, any order
        assert nc == max(0, n - 1)  # a full reduction is always n-1 combines


def test_lazy_frontier_is_logarithmic_not_linear() -> None:
    # 100k leaves delivered in order (as tasks finish ~chronologically): the live frontier must stay
    # O(log N), proving we never pre-build / hold the whole combine graph.
    n = 100_000
    r: LazyReducer[int] = LazyReducer(n, lambda a, b: a + b, lambda: 0)
    for i in range(n):
        r.feed(i, 1)
    assert r.result() == n  # every leaf counted exactly once
    assert r.n_combines == n - 1
    assert r.max_frontier <= 2 * math.ceil(math.log2(n)) + 4  # ~O(log N), nowhere near N


def test_lazy_empty_and_singleton() -> None:
    assert lazy_tree_reduce(0, [], _cat, _empty) == ("e", 0)
    assert lazy_tree_reduce(1, [(0, "x")], _cat, _empty) == ("x", 0)


def test_partial_then_resume_is_still_deterministic() -> None:
    # feed half out of order, then the rest: the bubbling frontier carries across and the result still
    # matches the canonical grouping (the property peer reduction relies on).
    n = 50
    r: LazyReducer[str] = LazyReducer(n, _cat, _empty)
    order = list(range(n))
    random.Random(7).shuffle(order)
    for leaf in order:
        r.feed(leaf, str(leaf))
    assert r.result() == _ref(n)
    assert r.n_combines == n - 1
