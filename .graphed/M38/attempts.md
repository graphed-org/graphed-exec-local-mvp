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

Every other frozen suite (m7/m31/m37 incl. the dashboard capstone + profiling) passes UNCHANGED under
the peer default. Cross-repo smoke (`scripts/test_all_repos.py`): all 11 repos green under the flip.

## Gates

`tests/frozen/m38` (87 tests) green on both backends; ruff + ruff format + mypy --strict clean;
sphinx -W. Freeze tag `freeze-M38-0`.

## Deferred (Phase-2 within M38)

- HTTP + ThreadExecutor profiling under free-threaded CPython 3.14t (no GIL → the transport + sampler
  threads run in parallel, so the sampler no longer starves; excluded from the witness under the GIL).
- Per-combine `on_combine` emission for peer (driver reports the count today); a steal-half/bulk knob
  for fine-grained workloads.
