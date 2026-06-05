"""The open_once file-locality directive: a multi-chunk single-file task opens the file exactly once
per worker (counting fake reader) (plan M7)."""

from __future__ import annotations

import analyses as A
from graphed_core import Partition, Plan, Task

from graphed_exec_local import ThreadExecutor


def _multi_chunk_plan(n_chunks: int) -> Plan[int]:
    tasks = [Task(i, Partition("one.root", "Events", i * 10, (i + 1) * 10)) for i in range(n_chunks)]
    return Plan(process=A.read_chunk_open_once, combine=A.add_int, empty=A.zero_int, tasks=tasks)


def test_single_worker_opens_the_file_exactly_once() -> None:
    A.reset_opens()
    ThreadExecutor(max_workers=1).run(_multi_chunk_plan(20))
    assert A.open_count() == 1  # 20 chunks of the same file -> one open on the single worker


def test_multiple_workers_open_at_most_once_each() -> None:
    A.reset_opens()
    ThreadExecutor(max_workers=4).run(_multi_chunk_plan(40))
    assert 1 <= A.open_count() <= 4  # one open per worker that touched the file, never per chunk
