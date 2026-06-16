Future improvements
===================

Catalogued, not silently dropped (plan A.7 / Part F).

- **Distributed executors** (TaskVine / HTCondor / Slurm) are Phase 2; this repo is single-machine
  only. The execution contract and the **WorkerTransport seam (M38)** are built so a real distributed
  adapter — reusing the HTTP backend's socket transport — can be written against them.
- **In-worker tree combines** and **work-stealing** — **done (M38):** peer reduction runs the combines
  across the workers off the driver (the default ``comms="ipc"``); an idle worker steals one leaf from
  a busy peer (steal-one). Remaining follow-ups: (a) **HTTP + ThreadExecutor profiling under
  free-threaded CPython 3.14t** — excluded from the witness under the GIL (the transport + sampler
  threads contend and the off-thread sampler can starve; with no GIL they run in parallel, so revisit
  when 3.14t is the norm); (b) **per-combine ``on_combine`` emission** for peer (the driver reports the
  count today); (c) a **steal-half / bulk-transfer knob** for fine-grained workloads (steal-one is the
  coarse-partition default).
- **Precision-based stopping** (statistical convergence) is contracted but not yet implemented.
