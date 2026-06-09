# Frozen acceptance suite — M10 (graphed-exec-local): pooled combines

Remediation milestone for MVP-shortcoming finding C.8 (see `mvp-shortcomings.md` in the
superproject): the M7 driver ran every tree-reduce combine serially on the driver thread, making
the driver a combine bottleneck for heavy partials.

| Test file | Verifies |
|---|---|
| `test_pooled_combines.py` | `pooled_combines=True`: results + combine count identical to the driver path; the SAME fixed reduction tree (bit-identical results); combines observed off-driver (thread idents / worker pids); straggler tolerance preserved; worker errors propagate intact; empty plan |

The default (driver-side combines) is pinned by the frozen M7 suite and is untouched.
