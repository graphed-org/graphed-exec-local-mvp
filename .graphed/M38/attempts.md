# M38 — inter-worker comms, peer reduction, work-stealing (attempts log)

Scope deviation (recorded): the plan §F lists work-stealing + distributed executors as Phase 2 and
§A.4 scopes this repo single-machine. The project owner pulled **inter-worker communication + peer
reduction + work-stealing** into MVP, keeping the executor single-machine but building the transport
seam so a future distributed executor reuses it unchanged. Root prompt **R21** binds this.

## What landed

- **Transport seam** in `graphed_core.execution.WorkerTransport` (the exec-protocol home): an
  addressable, non-blocking, best-effort message channel (send/broadcast/poll/recv/peers/close). Two
  backends in `graphed_exec_local._transport`: **`QueueTransport`** (IPC — `queue.Queue` for threads,
  `multiprocessing.Queue` for processes; the default) and **`HttpTransport`** (loopback `http.server`
  + a background sender, the path to true distributed schedulers). One conformance suite runs against
  both. The recv-drains-all-but-returns-one bug + the single-thread-backlog drop were found via the
  witnesses and fixed (ThreadingHTTPServer + retry + dedup; pop-one).
- **Lazy reduction** (`_reduce.LazyReducer`): the same fixed `plan_tree` computed by index arithmetic,
  frontier-bounded (O(log N)) — proven bit-for-bit == `tree_reduce` over fuzzed orders, no pre-built
  graph (huge N without an O(N) pre-pass).
- **Peer reduction** (`_peer.py`): each worker owns a contiguous leaf range, reduces it locally, and
  hands the O(log N) boundary partials worker→worker by ownership (segment-tree merge); the leaf's
  OWNER still settles it, so the grouping — and the result — is **identical to the hub even for
  non-associative float histograms**. Driver `done`-broadcast termination; prompt worker-error
  re-raise (M7 obligation), bit-for-bit on real ADL data, **no data-path regression** (−15% vs hub).
- **Work-stealing**: steal-ONE (Blumofe–Leiserson/Cilk, not steal-half — avoids the multi-thief
  over-drain cascade; literature review in the session), gated by an idle delay + exponential backoff
  so balanced loads pay nothing. Stealing moves only `process` work; the owner still reduces, so the
  result is unchanged. Witnessed: imbalanced → redistributed + faster + spread across ≥2 thieves;
  uniform → ~0 steals, no regression.
- **Monitor + profiling parity** so peer can be the default: workers emit SUBMITTED(driver)/STARTED/
  FINISHED/ERRORED (batched over the transport, drained until workers finish) + the driver fires the
  n-1 `on_combine`; workers run the off-thread `WorkerProfiler` and ship flamegraph trees
  (`Dashboard(profile=True)` is not silently empty under peer). Strict: peer refuses
  `pooled_combines` (a hub-only mechanism) loudly — hub never silently sneaks into a peer run.
- **Default flipped** to `comms="ipc"` (peer + work-stealing). `comms=None` selects the hub path.

## Sanctioned refreezes (the default flip)

Three frozen suites test **hub-only mechanisms** and were pinned to `comms=None` (a sanctioned redo;
the precommit integrity scan's `REFREEZE` advisory under `--allow-refreeze tests/frozen` is the
sanction):

- `tests/frozen/m10/test_pooled_combines.py` — `pooled_combines` is hub-only (peer does off-driver
  combines + refuses it).
- `tests/frozen/m34/test_bounded_cache_and_dedup.py` — the ship-once **broadcast cache** is a hub
  optimization (peer ships per worker).
- `tests/frozen/m7/test_straggler.py` — incremental `on_combine` ordering is a hub tree-reduce
  property (peer's straggler tolerance is work-stealing, covered by the M38 steal suite).

Three further hub-mechanism tests were pinned to `comms=None` in the post-freeze coverage fix below
(`m37/test_emit.py`, `m37/test_inprocess_paths.py`, `m31/test_ship_process_once.py`) to RESTORE
hub-path coverage the flip had moved onto the peer path — same rationale, no assertion weakened. The
m37 dashboard capstone + profiling pass UNCHANGED on the peer default. Cross-repo smoke
(`scripts/test_all_repos.py`): all 11 repos green under the flip.

## Post-freeze CI fix: the coverage gate (freeze-M38-0 → freeze-M38-1)

`freeze-M38-0` was pushed with all local precommit checks green, but CI went **red on every matrix
leg**: `Coverage failure: total of 86 is less than fail-under=90`. The local precommit ran `pytest -q`
(no coverage); CI runs `pytest tests/frozen --cov=graphed_exec_local --cov-branch` — so an
under-covered diff passed locally and only failed in CI. Two causes, both consequences of the
**default flip to peer**:

1. **Subprocess-only actors.** `ipc_peer_actor` / `http_peer_actor` are the picklable entry points
   `ProcessExecutor` submits to its worker pool, so in a real run they execute in *worker processes*
   where the driver's coverage instrumentation can't see them (the same gap M37 closed for the hub
   worker entry via `test_inprocess_paths`). Closed with a **new frozen file**
   `tests/frozen/m38/test_inprocess_peer.py` (7 tests): it drives the EXACT `_peer_ipc` / `_peer_http`
   discovery+reduction protocol the executor uses, but with the actors running in threads, so the
   actor bodies + the worker-process resource cache are exercised under instrumentation — WITNESSED
   end-to-end (root bit-for-bit == the flat tree), plus the `collect_peer_root` timeout and the
   `run_peer_worker` done-via-prebuffer paths.
2. **Hub-path coverage lost to the flip.** The hub monitor collector (`_ensure_collector` /
   `_collect_loop` / `_dispatch`) and the ship-once `_broadcast` were covered by m37/m31 tests that —
   under the new peer default — now run the PEER path, leaving the hub code uncovered. Restored by
   pinning the hub-mechanism tests to `comms=None` (their original M37/M31 intent; peer-path parity is
   covered by m38 `test_peer_robustness`): `m37/test_emit.py`, `m37/test_inprocess_paths.py`,
   `m31/test_ship_process_once.py`. (`m37/test_capstone_dashboard.py` stays on the peer default — its
   `inflight` drain assertion is peer-shaped, and `test_emit`'s process+monitor variant already covers
   the hub collector.) No assertion was weakened; only the transport was pinned to the path each test
   was written to exercise.

Result: frozen-suite coverage **94%** (`_peer` 92, `_reduce` 97, `_transport` 96, `executors` 93).
The precommit gate itself was upgraded to run each repo's own CI `--cov` command (graphed-orchestrator
`precommit.check_coverage`), so this class of "green locally, red in CI" can't recur.

## Gates

`tests/frozen/m38` (94 tests) green on both backends; frozen coverage 94% (≥90 line+branch);
ruff + ruff format + mypy --strict clean; sphinx -W. Freeze tag `freeze-M38-1`.

## Deferred (Phase-2 within M38)

- HTTP + ThreadExecutor profiling under free-threaded CPython 3.14t (no GIL → the transport + sampler
  threads run in parallel, so the sampler no longer starves; excluded from the witness under the GIL).
- Per-combine `on_combine` emission for peer (driver reports the count today); a steal-half/bulk knob
  for fine-grained workloads.
