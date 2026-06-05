Future improvements
===================

Catalogued, not silently dropped (plan A.7 / Part F).

- **Distributed executors** (TaskVine / HTCondor / Slurm) are Phase 2; this repo is single-machine
  only. The execution contract is deliberately minimal and **provisional** until a real distributed
  adapter exercises it.
- **True work-stealing / morsel-driven scheduling** beyond the pool's shared-queue behavior.
- **In-worker tree combines** (combine partials in workers, not the driver) for very large fan-in;
  the current driver-side combine is straggler-tolerant but serial in the driver.
- **Precision-based stopping** (statistical convergence) is contracted but not yet implemented.
