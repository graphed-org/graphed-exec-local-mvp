# M38 — inter-worker comms, peer reduction, work-stealing (attempts log)

> **freeze-M38-5 (2026-06-16):** sanctioned refreeze of `test_peer_robustness.py` only — the profiling
> witness `_spin` was a pure-Python busy loop holding the GIL, which intermittently starved the
> GIL-needing off-thread sampler to ZERO samples on slow py3.14 macOS/Windows CI (a timing-flaky
> witness, R0.10a). Fixed by releasing the GIL each step (`time.sleep`) + a longer budget, so the
> sampler reliably lands samples on any machine — mimicking how the real analysis releases the GIL in
> array kernels. No assertion weakened. (Surfaced by the Manager-removal commit's CI; unrelated to that
> change — the HTTP path it failed on was untouched.)

Scope deviation (recorded): the plan §F lists work-stealing + distributed executors as Phase 2 and
§A.4 scopes this repo single-machine. The project owner pulled **inter-worker communication + peer
reduction + work-stealing** into MVP, keeping the executor single-machine but building the transport
seam so a future distributed executor reuses it unchanged. Root prompt **R21** binds this.

## Post-M38 perf fix: the peer collection tail-join (2026-06-16)

Owner reported a perceived "work-stealing regression in IPC mode". Investigated by replicating the
ADL notebook's cell-32 benchmark (8 queries, one combined plan, persistent 4-worker pool, warm + 50
samples) and sweeping configs (`coffea-benchmarks-graphed-mvp/bench_*.py`). **Measured, not assumed
(R0.11):**

- **Stealing is NOT the cause.** `steal=True` ≈ `steal=False` within noise (±2%) in every mode
  (no-monitor / dashboard / dashboard+profile); `steals=0` on balanced loads; the steal-loop's
  coordination cost is ~3 ms. Lengthening the steal poll would not help (and slightly hurts the tail).
- **The regression is peer-vs-hub** (the default flip): +48 % at 8 files, +7.6 % at 32 files — a
  roughly FIXED per-run overhead that amortizes at scale.
- **Decomposed** (1 leaf/worker, shared-wall-clock instrumentation, since reverted): dispatch+setup
  4 ms, cross-worker reduce 2 ms, compute 111 ms — i.e. coordination is negligible. The consistent,
  removable cost was the **driver tail-join**: after the root was already in hand, `_collect_peer`
  polled ``f.done()`` on a 20 ms cadence waiting for workers to notice the ``done`` broadcast (~25-30 ms
  every run). **Fix:** on the no-monitor fast path, block on the futures' completion (woken instantly)
  instead of polling. A/B (same session, stash): peer 249.7 → 224.1 ms (−25.6 ms); gap +48 % → +23 %.
  `open_once` is warm (open-count stable across runs — not a re-open issue).
- **Residual ~40 ms** (peer compute makespan > hub) is **contention-sensitive** — near-zero on a quiet
  machine (a low-load timeline showed peer 179 ≈ hub 170 ms), inflating under load.

### Residual root cause + fix: remove the Manager server (py-spy)

py-spy (``--subprocesses``, same benchmark) showed the peer path ran **9 processes vs the hub's 6**,
including a ``multiprocessing.Manager`` **server process consuming 31 % of sampled thread-time** doing
pure socket-IPC (``_recv`` 76 % + ``accept`` 21 %, zero compute). The IPC ``QueueTransport`` used
``Manager().Queue()`` proxies *because they are picklable* (passable as per-submit args); every queue
op was a socket-RPC to that server, and the extra process + threads inflated worker compute under load.
Workers ran the identical compute in both paths (awkward kernels / ``decompress`` / ``_carry``) — no
peer-specific algorithm.

**Fix:** create the inbox queues as **raw ``mp.Queue``** in the driver and **inherit** them in every
worker via the pool ``initializer`` (``peer_pool_init``); the actor (``pooled_peer_actor``) resolves
its inbox/outboxes from that process-global registry by address (a cheap string submit-arg). No Manager
server, native pipes. Notebook ratification (8 files, 50 samples): peer-vs-hub gap **+23 % → +12.8 %**
(and **p25 ≈ hub**, 159 vs 158 ms — at low contention peer now matches the hub). Cumulative with the
tail-join fix: original +48 % → +12.8 %. New frozen ``test_pooled_transport.py`` covers the
registry-resolved actor + persistent reuse/drain in-process.

### Residual saturation tail root cause + fix: SimpleQueue (no feeder threads)

The remaining tail was investigated since **workers ≈ cores is the real HEP batch slot** (not workers
< cores). A startup **stagger A/B first refuted a "lockstep" hypothesis** (staggering only *added*
latency: +12.5 % → +23.5 % at 8 ms). py-spy then localised it: per-worker self-time is ~identical to
the hub (no CPU sink), but the **peer worker carried 5 live threads vs the hub's 1** — raw
``multiprocessing.Queue`` spawns a **feeder thread per queue** a process puts to, so a peer worker
(driver + reduction peers) ran ~4 feeders. With workers ≈ cores those idle-but-scheduled threads add
context-switch pressure that slows the workers' compute (invisible in self-time; shows only at
saturation — explains why p25 == hub but the median lifts).

**Fix:** ``PipeInbox`` — a ``multiprocessing.SimpleQueue`` (no feeder thread; ``put`` writes the pipe
synchronously; the reader's ``poll(timeout)`` gives the timed receive) wrapped to the queue API.
Peer worker threads **5 → 1**, like the hub. Result (per-worker work fixed, this 10-core box):

| W | pre (mp.Queue) | post (SimpleQueue) |
|---|---|---|
| 2 | −5.5 % | +1.1 % |
| 4 | **+23.7 %** | **−1.5 %** |
| 8 | +12–17 % | +10.5 % |
| 10 (workers == cores) | — | **+7.6 %** |

The gap **closes with headroom (W=2,4)** and, crucially, **shrinks toward true saturation** (W=8→10:
+10.5 → +7.6 %) — the opposite of "scales poorly with more processes." The residual ~7.6 % at
workers == cores is the inherent cost of *distributed* reduction: the hub offloads its N−1 combines
onto the otherwise-idle driver core, the peer does them on the busy workers — the very property that
lets peer scale past a single-driver bottleneck. Left as-is (chasing it would defeat the off-driver
design); small and shrinking. (Still 10 cores here — true large-machine scaling untested; the O(N²)
registry inheritance remains the item to watch at very large N.)

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

## Post-freeze CI fix #2: a flaky steal witness (freeze-M38-1 → freeze-M38-2)

`freeze-M38-1` passed local checks + CI on Linux but went red on the **slower macOS/Windows legs**:
`test_steal.py::test_witness_stealing_redistributes_and_stays_correct[http]` —
`assert len(thieves) >= 2` saw only 1. This was an over-specified, timing-dependent assertion: with
**steal-ONE** + a slow (http) transport, one quick idle peer can catch several of the heavy owner's
one-at-a-time grants before others' requests arrive, so the number of DISTINCT thieves is a scheduling
detail, not an invariant — the run is still correct (every other witness passed). Replaced with the
**deterministic** steal-one invariant that the original line was a flaky proxy for:
`wit[0]["given"] + wit[0]["processed"] == N//4` (the owner's range = leaves it ran + leaves it shed
one-at-a-time) and `sum(given) == sum(steals)` (each shed leaf stolen exactly once). A steal-HALF grant
would move several leaves per request, so `given` < leaves shed — these asserts would catch it; they
test the anti-cascade *mechanism* directly and are transport/timing independent. Sanctioned refreeze
(`--allow-refreeze tests/frozen/m38`); validated stable across repeated ipc+http runs.

## Post-freeze CI fix #3: a flaky wall-clock speedup assert (freeze-M38-2 → freeze-M38-3)

Same test, the NEXT assertion flaked on the slow legs: `assert dt_steal < dt_nosteal` saw
`1.86 < 1.78` (steal run marginally SLOWER). On a loaded/slow CI runner the ~0.1 s of heavy work is
dwarfed by process-startup + http-transport noise (both runs measured ~1.8 s, ~15× the ~0.12 s ideal),
so a wall-clock speedup comparison is inherently flaky and proves nothing the witnesses don't. Removed
the `time.perf_counter()` measurements + the assert; the speedup is witnessed **structurally** instead:
`wit[0]["processed"] < w0_nosteal["processed"]` — with stealing the heavy owner runs strictly fewer of
its own leaves, so the heavy work is genuinely off its critical path (the whole point of stealing),
deterministically and with no wall-clock dependence. This was the last timing-dependent assertion in
the m38 suite (grep-verified). Sanctioned refreeze (`--allow-refreeze tests/frozen/m38`).

**Lesson (recorded):** a frozen test must assert deterministic INVARIANTS, never wall-clock timing or
emergent scheduling distributions — both flake on slow/contended CI even when the mechanism is correct.

## Post-freeze CI fix #4: steal-engagement window too tight for slow CI (freeze-M38-3 → freeze-M38-4)

With the timing-comparison asserts gone, the ENGAGEMENT witnesses themselves (`given > 0`, `steals >
0`) flaked on the two slowest legs (macOS/Windows py3.13): they saw 0. Work-stealing engagement is
intrinsically timing-gated — an idle peer's steal request must reach the busy owner *before* it
finishes its range — and the 4×0.03=0.12 s owner window was too tight: on a heavily contended runner
the steal handshake (idle-gate + transport + scheduling) didn't always land before the owner was done,
so no steal occurred even though the mechanism is correct. Fix is **scenario sizing**, not the
assertion: `HEAVY` 0.03 → 0.2 (a ~0.8 s owner window vs a few-ms handshake — ~16× margin). The asserts
stay on the structural counters (`given`/`steals`), per R0.10a; only the window the scenario leaves for
engagement grew. No `--allow-refreeze` shape change beyond `tests/frozen/m38`.

## Gates

`tests/frozen/m38` (94 tests) green on both backends; frozen coverage 94% (≥90 line+branch);
ruff + ruff format + mypy --strict clean; sphinx -W. Freeze tag `freeze-M38-4`.

## Deferred (Phase-2 within M38)

- HTTP + ThreadExecutor profiling under free-threaded CPython 3.14t (no GIL → the transport + sampler
  threads run in parallel, so the sampler no longer starves; excluded from the witness under the GIL).
- Per-combine `on_combine` emission for peer (driver reports the count today); a steal-half/bulk knob
  for fine-grained workloads.
