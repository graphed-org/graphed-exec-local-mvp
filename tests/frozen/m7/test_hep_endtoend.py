"""HEP end-to-end via the executor (plan M7): a real graphed analysis runs over partitions through
BOTH executors and reproduces the single-pass histogram bit-for-bit, invariant to partition count."""

from __future__ import annotations

import analyses as A
import numpy as np
import pytest
from graphed_core import Plan

from graphed_exec_local import ProcessExecutor, ThreadExecutor


def _met_plan(n_chunks: int) -> Plan:
    return Plan(
        process=A.met_partial, combine=A.hist_add, empty=A.hist_zero, tasks=A.met_partitions(n_chunks)
    )


@pytest.mark.parametrize("Ex", [ThreadExecutor, ProcessExecutor])
def test_met_histogram_reproduces_single_pass_bit_for_bit(Ex: type) -> None:
    full = A.met_full_counts()
    assert int(full.sum()) == A.N_EVENTS  # sanity: the analysis histograms every event
    r = Ex(max_workers=4).run(_met_plan(8))
    assert np.array_equal(r.value, full)  # partitioned + tree-reduced == single pass, exactly
    assert r.n_combines == 7


def test_result_is_invariant_to_partition_count() -> None:
    # chunking the same data any number of ways must give the identical reduced histogram
    full = A.met_full_counts()
    for n_chunks in (1, 3, 8, 25):
        r = ThreadExecutor(max_workers=4).run(_met_plan(n_chunks))
        assert np.array_equal(r.value, full), f"{n_chunks} chunks changed the histogram"
