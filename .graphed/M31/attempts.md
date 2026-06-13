# M31 attempts — graphed-exec-local (ship the process to workers once, not per task)

## Iteration 0 — 2026-06-13 (freeze-M31-0)

- MEASURED finding (notebook speedup audit): the persistent-pool per-task framework round-trip
  is ~0.1ms (no-op task), NOT the 40-80ms I had wrongly claimed; pickle of the 13.8KB process
  is ~0.01ms. So at the current scale this is negligible — but concurrent.futures re-pickles
  and re-ships the `process` callable on EVERY submit (it does not dedupe callables), so a Plan
  whose process embeds a large compiled IR (or an inlined model) pays that wire cost per
  partition. The user asked for the architecturally-correct ship-once design regardless.
- DESIGN: pickle the process ONCE in the driver; broadcast those bytes to every worker; cache
  worker-side in a module global keyed by sha256(content); submit only (token, partition) per
  task. Broadcast = a pid-coverage loop (concurrent.futures exposes no worker identity): submit
  priming tasks that each hold 2ms (so siblings each claim one) and return os.getpid(), until
  the pid set covers pool._max_workers; idempotent, so extra hits are harmless. Cached per
  (pool, token): re-running the same plan, or a persistent pool across plans, never re-broadcasts
  the same process; close()/respawn clears the token set. Threads share memory -> no delivery
  (direct submit). _entry() replaced by _prepare(pool, process)->submit(partition) on both
  executors; all three submit paths (fixed, pooled-combine, adaptive) route through it.
- frozen m31 (4) + probes: CountingProcess records per-WORKER unpickle count via __setstate__;
  with 40 tasks/4 workers the process is unpickled exactly once per worker (counts == {1}),
  whereas per-task shipping gave {1..10} (non-vacuity: pinned that exact pre-impl spread); a
  2MB process does not scale per-task; numeric results unchanged + byte-identical across runs;
  ThreadExecutor unaffected.
- Gates (via python -m graphed_orchestrator.precommit): 75 passed · coverage >=90 · ruff/mypy/
  sphinx clean · toml/yaml/integrity ok.
