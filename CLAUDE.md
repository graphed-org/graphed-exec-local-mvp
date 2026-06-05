# CLAUDE.md — graphed-exec-local

Defers to the root **`graphed-project/CLAUDE.md`**; the **project plan
(`graphed-project-plan-gated.md`) always wins.** This file distills **milestone M7**.

## What this repo is

`graphed-exec-local`: the **reference executors**. The execution *contract* lives in `graphed-core`
(`graphed_core.execution`: `Plan`, `Task`, `Partition`, `StopCondition`, `Executor`); this repo
implements it for a single machine, **two ways**: a thread pool and a process pool.

> Guardrails (M7): single machine only (no cluster — Phase 2); the published contract is **provisional
> until exercised by a real adapter**; executors MUST return remote `StageError`s intact (picklable,
> per M6), never an opaque string.

## Implemented (M7)

- `ThreadExecutor` + `ProcessExecutor` (`executors.py`) — same driver, different pool. Per-worker
  `open_once` resources are thread-local (threads) / a per-process global via an initializer
  (processes, spawn context: cross-platform + free-threaded-safe).
- **Deterministic, straggler-tolerant tree reduction** (`_reduce.py`): `plan_tree` builds a fixed
  binary combine-tree by leaf index; `tree_reduce` fires each combine as soon as both inputs are
  ready (a slow leaf blocks only its path to the root). Fixed grouping ⇒ bit-for-bit results.
- Adaptive reshaping: an `next_tasks(context)` plan pulls partitions sized from observed timings; a
  `running_fold` reduces the discovered set. Stopping conditions (target events / wall-clock / error
  budget) end submission early.
- `open_once` file-locality (`resources.py`): a uri is opened at most once per worker.
- Error propagation: a worker failure re-raises in the driver intact; a `StageError` round-trips and
  renders via M6's `format_traceback`.

## Validated against the corpus

The AGC ttbar slice + dimuon + ADL analyses run end-to-end over partitions via BOTH executors and
reproduce the corpus reference histograms **bit-for-bit**, invariant to `opt_level` and projection.

## Dependencies / gates

Runtime: `graphed-core` (contract) + `graphed-debug` (StageError) + `graphed` (Session). Tests use
`graphed-numpy` / `graphed-awkward` / `graphed-corpus`. Gates: `ruff` + `ruff format` · `mypy
--strict` · `pytest tests/frozen --cov=graphed_exec_local` (≥90%) · `sphinx -W`.

Status: see `.graphed/state.json`.
