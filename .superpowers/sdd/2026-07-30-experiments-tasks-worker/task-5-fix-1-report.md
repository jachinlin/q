# Task 5 Fix Round 1 Report

Date: 2026-08-02

## Outcome

All six Important findings and both Minor findings from `task-5-review.md`
are resolved. The worker now serializes one instance from claim through finish,
linearizes shutdown against claim, prevents stale heartbeat progress writes,
normalizes failures under global traversal/UTF-8/output budgets, redacts all
retained free-form strings and unsafe key names, reads cancellation state in one
joined SQLite snapshot, and converges a cancellation that wins the final
success race to `CANCELLED`.

No Task 6 runner, client, adapter, provider, or experiment execution behavior
was added.

## Changes by finding

### I1: worker concurrency and shutdown/claim linearization

- Added an instance execution lock held across claim, handler execution,
  heartbeat shutdown, and finish.
- Added a short shared claim gate around the shutdown check plus claim and
  around `request_shutdown()`.
- If claim obtains the gate first, the task is claimed before shutdown
  linearizes and the active handler sees shutdown at its next boundary. If
  shutdown obtains it first, later `run_once()` calls do not claim.
- Added deterministic tests for concurrent `run_once()` serialization and the
  claim-started-before-shutdown ordering.

### I2: stale periodic heartbeat overwrite

- The latest-progress lock now covers both selecting/updating the in-memory
  progress and the corresponding durable heartbeat write.
- A barrier-controlled test forces a periodic heartbeat to capture old progress
  while an immediate update begins, proving the old write completes before the
  newer value and cannot overwrite it afterward.

### I3: globally bounded deterministic normalization

- Added one per-error budget: 128 visited nodes, 16,384 raw UTF-8 input bytes,
  16,384 normalized JSON bytes, 50 mapping/list entries per container, and
  depth 4.
- Mapping input is iterated directly and stopped at 50 keys; an arbitrary
  overridden/eager `items()` method is never invoked.
- Normalization failures converge to the fixed valid payload
  `WORKER_ERROR_NORMALIZATION_FAILED` instead of escaping after claim.
- Added below/above-budget durable task, attempt, and audit assertions plus an
  adversarial Mapping traversal test.

### I4: retained context disclosure

- Free-form strings under allowlisted scalar, list, and nested mapping shapes
  are conservatively replaced with fixed markers.
- Context output keys are canonicalized; secret-bearing arbitrary key names are
  collapsed to the fixed `redacted` key so keys cannot become a disclosure
  channel. The marker remains stable under the Worker's second normalization
  pass.
- Numeric and boolean machine values remain explicit safe scalar types; large
  integers and non-finite floats become fixed markers.
- Tests inject secrets into message, remediation, scalar/list/nested values,
  sensitive values, an ignored value, and a sensitive key name, then inspect
  task, attempt, audit, and log surfaces.

### I5: cancellation read snapshot

- `TaskQueue.is_cancel_requested()` now fetches the attempt and task identity,
  owner, and status columns with one joined SELECT before applying the existing
  owner and active-pair fences.
- A SQLAlchemy cursor barrier proves a concurrent committed cancellation cannot
  synthesize a mixed RUNNING/CANCEL_REQUESTED observation.

### I6: final-boundary cancellation convergence

- A `SUCCEEDED` finish conflict is reconciled only after the owner-fenced joined
  cancellation read confirms `CANCEL_REQUESTED`; the Worker then finishes the
  attempt and task as `CANCELLED`.
- Other conflicts and coordination/ownership failures continue to propagate.
- A deterministic SQLite race test places cancellation after the handler's last
  token check and before success finish, and asserts both durable rows end in
  `CANCELLED`.

### M1 and M2

- Poll interval, heartbeat interval, and heartbeat join timeout now reject bool,
  zero, negative, NaN, and infinity at construction as non-positive/non-finite
  values, before any claim.
- Test helper threads are non-daemon and all cancellation-test releases and
  bounded joins execute in `finally` blocks.

## TDD evidence

1. I1 red: two deterministic concurrency tests failed (`max_active == 2` and
   shutdown completed inside the pre-claim/claim gap).
   Green: `C:/q5-fix1-i1-green-0802` -> 2 passed.
2. I2 red: `C:/q5-fix1-i2-red-0802` -> 1 failed because the immediate update
   persisted while the older periodic heartbeat was blocked.
   Green: `C:/q5-fix1-i2-green-0802` -> 3 passed.
3. I3/I4 red: `C:/q5-fix1-i34-red-0802` -> 5 failed, covering retained secret
   shapes, oversized normalization stranding RUNNING work, and traversal past
   item 50. Green: `C:/q5-fix1-i34-green-0802` -> 5 passed.
4. I5 red: `C:/q5-fix1-i5-red-0802` -> 1 failed with an artificial state
   mismatch across two SELECTs. Green: `C:/q5-fix1-i5-green-0802` -> 2 passed.
5. I6 red: `C:/q5-fix1-i6-red-0802` -> 1 failed with a propagated
   `TaskQueueConflict` and both rows left `CANCEL_REQUESTED`. Green:
   `C:/q5-fix1-i6-green-0802` -> 2 passed in 2.11s.
6. M1 red: `C:/q5-fix1-m1-red-0802` -> 15 failed. Green:
   `C:/q5-fix1-m1-green-0802` -> 17 passed in 6.59s, including default timing
   behavior.
7. Self-review red: `C:/q5-fix1-self-review-red-0802` -> 2 failed for an
   overridden eager Mapping `items()` and a secret embedded in a context key.
   After diagnosing and fixing second-pass redaction idempotence,
   `C:/q5-fix1-self-review-green2-0802` -> 5 passed.

## Fresh verification

- Worker unit and real spawn recovery:
  `uv run pytest tests/unit/tasks/test_worker.py tests/integration/test_worker_recovery.py -v --basetemp=C:/q5-fix1-worker-green2-0802`
  -> 37 passed in 13.53s.
- Queue/concurrency/backtest regressions:
  `uv run pytest tests/integration/test_task_queue.py tests/integration/test_task_queue_concurrency.py tests/integration/test_backtest_cancellation.py tests/integration/test_backtest_timeline.py -q --basetemp=C:/q5-fix1-regression-0802`
  -> 65 passed in 18.22s.
- Tracked lint: `uv run ruff check src tests` -> all checks passed.
- Static typing: `uv run mypy src` -> success in 88 source files.
- Full suite: `uv run pytest -q --basetemp=C:/q5-fix1-full-0802`
  -> 1308 passed, 6 skipped in 261.51s.
- Exact root lint: `uv run ruff check .` -> 16 pre-existing errors exclusively
  in untracked `.r2i3g/` and `.r2target/` test artifacts. Those directories
  were preserved and were not edited, removed, ignored, or staged.

## Files in this fix

- `src/quant_core/tasks/worker.py`
- `src/quant_core/tasks/queue.py`
- `tests/unit/tasks/test_worker.py`
- `.superpowers/sdd/2026-07-30-experiments-tasks-worker/task-5-fix-1-report.md`

The requested commit message is
`fix: harden worker concurrency and failure boundaries`.
