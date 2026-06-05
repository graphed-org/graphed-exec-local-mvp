"""The associative tree reduction (plan M7): deterministic + straggler-tolerant, tested directly."""

from __future__ import annotations

import random

import pytest

from graphed_exec_local import plan_tree, running_fold, tree_reduce


@pytest.mark.parametrize("n", [0, 1, 2, 3, 4, 5, 7, 8, 16, 17])
def test_plan_tree_has_n_minus_1_combines(n: int) -> None:
    combines, root = plan_tree(n)
    assert len(combines) == max(0, n - 1)  # a binary tree over n leaves has n-1 internal combines
    assert (root is None) == (n == 0)
    # the tree shape is fixed (deterministic grouping); each combine's inputs precede its output
    for out, a, b in combines:
        assert a < out and b < out
    if combines:
        assert combines[-1][0] == root  # the last combine produces the root


def _reduce(order: list[int], n: int) -> int:
    completed = ((leaf, leaf) for leaf in order)  # partial value == leaf index
    value, _ = tree_reduce(n, completed, lambda a, b: a + b, lambda: 0)
    return value


def test_tree_reduce_is_order_independent() -> None:
    n = 32
    expected = sum(range(n))
    base = list(range(n))
    for _ in range(20):
        shuffled = base[:]
        random.shuffle(shuffled)
        assert _reduce(shuffled, n) == expected  # any completion order -> same reduced value


def test_tree_reduce_empty_and_single() -> None:
    assert tree_reduce(0, iter([]), lambda a, b: a + b, lambda: -1) == (-1, 0)  # empty -> identity
    assert tree_reduce(1, iter([(0, 99)]), lambda a, b: a + b, lambda: 0) == (99, 0)  # single -> no combine


def test_tree_reduce_is_incremental_not_a_barrier() -> None:
    # the straggler (leaf 0) is delivered LAST; combines for the other subtrees must fire before it
    n = 16
    order = [*range(1, n), 0]
    leaves_at_each_combine: list[int] = []
    completed = ((leaf, leaf) for leaf in order)
    _value, n_combines = tree_reduce(
        n, completed, lambda a, b: a + b, lambda: 0, on_combine=leaves_at_each_combine.append
    )
    assert n_combines == n - 1
    # at least one combine fired before all leaves arrived (no all-leaves barrier)...
    assert min(leaves_at_each_combine) < n
    # ...but the root combine necessarily waits for the straggler (the last-delivered leaf)
    assert max(leaves_at_each_combine) == n


def test_running_fold() -> None:
    assert running_fold(iter([]), lambda a, b: a + b, lambda: 0) == (0, 0)
    assert running_fold(iter([(0, 5)]), lambda a, b: a + b, lambda: 0) == (5, 0)
    assert running_fold(iter([(0, 1), (1, 2), (2, 3)]), lambda a, b: a + b, lambda: 0) == (6, 2)
