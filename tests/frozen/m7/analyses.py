"""Shared, PICKLABLE process functions for the M7 executor suite (module-level so a spawned process
pool can import them). Non-HEP fns are light; HEP fns import awkward/corpus lazily."""

from __future__ import annotations

import time

import numpy as np
from graphed import Session
from graphed_core import Partition


# ---- simple / concurrency ---------------------------------------------------
def count_entries(part: Partition, res: object) -> int:
    return part.n_entries


def add_int(a: int, b: int) -> int:
    return a + b


def zero_int() -> int:
    return 0


def one(part: Partition, res: object) -> int:
    return 1


def sleep_then_one(part: Partition, res: object) -> int:
    # a small deterministic delay derived from the partition (no RNG): 0..3 ms
    time.sleep((part.entry_start % 4) * 0.001)
    return 1


def straggler_one(part: Partition, res: object) -> int:
    # the leaf at entry_start==0 is an artificial straggler; the rest are instant
    if part.entry_start == 0:
        time.sleep(0.4)
    return 1


# ---- open_once (thread-mode counting reader) --------------------------------
_OPENS: list[str] = []  # threads share module globals; each real open appends


def reset_opens() -> None:
    _OPENS.clear()


def open_count() -> int:
    return len(_OPENS)


def _counting_opener(uri: str) -> object:
    _OPENS.append(uri)
    return f"handle::{uri}"


def read_chunk_open_once(part: Partition, res: object) -> int:
    res.open_once(part.uri, _counting_opener)  # type: ignore[attr-defined]
    return part.n_entries


# ---- HEP: a real analysis over partitions, reduced to a histogram -----------
N_EVENTS = 4000
HIST = {"bins": 40, "start": 0.0, "stop": 200.0, "name": "met"}


def _load_dataset(uri: str) -> object:
    from graphed_corpus import make_events  # noqa: PLC0415

    return make_events(n_events=N_EVENTS, seed=1234)


def _met_counts(events: object) -> np.ndarray:
    from graphed_awkward import AwkwardBackend, from_awkward  # noqa: PLC0415
    from graphed_corpus.histograms import hist1d  # noqa: PLC0415

    s = Session(AwkwardBackend())
    ev = from_awkward(s, "events", events)
    values = s.materialize(ev.MET.pt)  # ADL1-style: the MET pT spectrum
    h = hist1d(values, **HIST)  # integer counts -> exact under summation
    return np.asarray(h.values(), dtype=np.int64)


def met_partial(part: Partition, res: object) -> np.ndarray:
    # open the dataset once per worker (file-locality), then take this chunk's events
    events = res.open_once(part.uri, _load_dataset)[part.entry_start : part.entry_stop]  # type: ignore[attr-defined]
    return _met_counts(events)


def met_full_counts() -> np.ndarray:
    from graphed_corpus import make_events  # noqa: PLC0415

    return _met_counts(make_events(n_events=N_EVENTS, seed=1234))


def hist_add(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return a + b


def hist_zero() -> np.ndarray:
    return np.zeros(HIST["bins"], dtype=np.int64)


def met_partitions(n_chunks: int) -> list:
    from graphed_core import Task  # noqa: PLC0415

    edges = np.linspace(0, N_EVENTS, n_chunks + 1, dtype=int)
    return [
        Task(i, Partition("events://corpus", "Events", int(edges[i]), int(edges[i + 1])))
        for i in range(n_chunks)
    ]


# ---- remote StageError (must round-trip from a worker PROCESS intact, plan A.3 #8) -------------
def raise_stage_error(part: Partition, res: object) -> int:
    """Build + run a deliberately-failing analysis IN THE WORKER; the StageError must reach the
    driver picklable (never an opaque string)."""
    import graphed_debug as gd  # noqa: PLC0415
    import graphed_numpy as gn  # noqa: PLC0415

    s = Session(gn.NumpyBackend())
    events = gn.from_record(s, "events", pt=np.arange(1.0, 5.0))
    bad = (events["pt"] * 2.0).map(lambda a: a[100], name="worker_oob")  # out-of-range in the worker
    return gd.run(s, bad, opt_level=1)  # raises graphed_debug.StageError
