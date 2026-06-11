## Iteration 0 — phase REVIEW — 2026-06-05T19:08:22Z

- summary: local gates green; ci_confirmed=False
- gates: {'frozen_tests': True, 'coverage': True, 'lint': True, 'types': True, 'determinism': True, 'benchmark': None, 'integrity_scan': True}
- l0_count=0 escalated=False reject_count=0

## Iteration 0 — phase DONE — 2026-06-05T19:28:21Z

- summary: local gates green; ci_confirmed=True
- gates: {'frozen_tests': True, 'coverage': True, 'lint': True, 'types': True, 'determinism': True, 'benchmark': None, 'integrity_scan': True}
- l0_count=0 escalated=False reject_count=0


## 2026-06-11 — persistent worker pools (additive; the ADL-notebook/sweep finding)

- Spawning a fresh import-heavy pool per run() dwarfs small-plan work (the ADL notebook's eight
  queries ran 3x SLOWER parallel than sequential on a 50k skim). persistent=True (opt-in) keeps
  ONE pool across run() calls — witnessed by worker-global state surviving between runs — with
  close()/context-manager release and lazy respawn; the DEFAULT (fresh pool per run) is pinned
  unchanged. New frozen file tests/frozen/m7/test_persistent_pool.py (5 tests; 4/5 failed
  pre-impl, the default pin passed by design). Measured: the eight ADL queries at 2.0x (50k
  skim) and 2.8x (400k, 8 files) with a persistent x4 pool vs sequential.
- Implementation note: a blanket text replace recursed _acquired_pool into itself (caught by
  the suite, fixed pre-commit).
