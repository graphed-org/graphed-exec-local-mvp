# Contributing to graphed-exec-local

Part of the `graphed` project, governed by the gated three-role pipeline. The root
[`graphed-project/CLAUDE.md`](https://github.com/graphed-org/graphed-project) and the project plan
are authoritative; the plan always wins.

## Guardrails (M2)

- A **trivial** backend proving the seam — 1-D numpy bags only, no HEP (that is `graphed-awkward`,
  M3). `external_payload` flags opaque callables as a preservation risk (real hashing is M9).

## Integrity rules — NON-NEGOTIABLE (plan A.7 / B.6)

Never edit/skip/weaken `tests/frozen/**`; never lower a threshold or relax CI; never stub the thing
under test. Dispute a frozen test via `.graphed/<Mx>/disputes/<test_id>.md`.

## Local gates

```bash
pip install "graphed-core @ git+https://github.com/graphed-org/graphed-core@main"   # needs Rust
pip install "graphed @ git+https://github.com/graphed-org/graphed@main"
pip install -e ".[dev,docs]"
ruff check . && ruff format --check . && mypy
pytest tests/frozen/m2 --cov=graphed_exec_local --cov-branch   # 100%
sphinx-build -W -b html docs docs/_build/html
```
