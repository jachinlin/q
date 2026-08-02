# Task 5 Fix Round 2 Report

Date: 2026-08-02

## Outcome

The scoped N1 and N2 findings from `task-5-rereview-1.md` are resolved.
The claim/shutdown linearization gate is now reentrant for a same-thread
shutdown callback, and every active completion path uses one owner-fenced
cancellation-convergence policy.

No Task 6 runner, client, adapter, provider, or experiment execution behavior
was added. Existing untracked temporary directories were not edited, cleaned,
ignored, or staged.

## N1: reentrant shutdown during claim

### Root cause

`run_once()` held a non-reentrant `threading.Lock` around the shutdown check and
`queue.claim()`. If `claim()` synchronously called `request_shutdown()` on the
same thread, that call attempted to acquire the already-held lock and could
never return.

### Fix and ordering

- Replaced only the claim gate with `threading.RLock`.
- The existing execution lock still serializes each Worker from claim through
  finish.
- The claim gate still makes the pre-claim shutdown check and claim one ordered
  region. An external shutdown that arrives after claim has entered linearizes
  after that claim; a shutdown that obtains the gate first prevents later
  claims.
- A shutdown invoked reentrantly on the claim thread can set the event and
  return without waiting on itself. This directly covers the lock shape a
  future main-thread Python signal callback could encounter, but no SIGINT or
  Windows console adapter was added or claimed as verified.

### Bounded test cleanup

The reproduction runs in a Windows `spawn` child process. It observes entry
into claim, gives the synchronous shutdown call a 0.5-second deadline, and
always terminates/joins, then kills/joins if necessary, before closing the
multiprocessing queue. Therefore the deliberate red deadlock cannot leave a
test thread, child process, or database resource behind.

## N2: cancellation convergence for all outcomes

### Root cause

The shared `_finish()` reconciliation branch applied only to `SUCCEEDED`, and
unknown/unregistered task types called `queue.finish(FAILED)` directly. When a
real SQLite cancellation committed before either `FAILED` finish, the queue
correctly rejected failure and left task and attempt `CANCEL_REQUESTED`, but the
Worker propagated the conflict instead of completing cancellation.

### Fix and conflict safety

- Unknown/unregistered task failure now calls the same `_finish()` method as
  handler outcomes.
- Any non-`CANCELLED` outcome that receives `TaskQueueConflict` performs the
  existing single joined, owner-fenced cancellation read.
- The Worker retries exactly once with `CANCELLED` only when that read returns
  true for a consistent owned `CANCEL_REQUESTED` pair.
- A false cancellation read re-raises the original finish conflict. Ownership,
  terminal-state, queue-busy, and cancellation-read conflicts still propagate;
  they cannot be converted into cancellation. A `CANCELLED` finish conflict is
  never reconciled recursively.

Both new N2 tests use the real SQLite `TaskQueue`. Their wrapper supplies only a
barrier immediately before the first `FAILED` finish so cancellation can commit
at the intended boundary; assertions inspect actual task/attempt rows and the
terminal finish sequence.

## TDD evidence

### N1 red and green

- Red:
  `uv run pytest tests/unit/tasks/test_worker.py::test_claim_can_request_shutdown_reentrantly_without_deadlock -v --basetemp=C:/q5-fix2-n1-red-0802`
  -> 1 failed in 3.58s. Claim entered, but the same-thread shutdown call did not
  return within 0.5 seconds.
- Green, including the preserved I1 contracts:
  `uv run pytest tests/unit/tasks/test_worker.py::test_claim_can_request_shutdown_reentrantly_without_deadlock tests/unit/tasks/test_worker.py::test_same_worker_serializes_concurrent_run_once_claim_through_finish tests/unit/tasks/test_worker.py::test_claim_started_before_shutdown_holds_the_shared_linearization_gate -v --basetemp=C:/q5-fix2-n1-green-0802`
  -> 3 passed in 4.54s.

### N2 red and green

- Red:
  `uv run pytest tests/unit/tasks/test_worker.py::test_cancel_winning_after_final_boundary_converges_handler_failed_outcome tests/unit/tasks/test_worker.py::test_cancel_winning_unknown_task_finish_converges_to_cancelled -v --basetemp=C:/q5-fix2-n2-red-0802`
  -> 2 failed in 2.19s. Both returned `TaskQueueConflict`; the real durable rows
  remained `CANCEL_REQUESTED`.
- Green, including original I6, normal unknown dispatch, and lost-heartbeat
  ownership safety:
  `uv run pytest tests/unit/tasks/test_worker.py::test_cancel_winning_after_last_boundary_converges_success_to_cancelled tests/unit/tasks/test_worker.py::test_cancel_winning_after_final_boundary_converges_handler_failed_outcome tests/unit/tasks/test_worker.py::test_cancel_winning_unknown_task_finish_converges_to_cancelled tests/unit/tasks/test_worker.py::test_unregistered_or_unknown_task_type_finishes_nonretryable_failure tests/unit/tasks/test_worker.py::test_background_heartbeat_failure_propagates_without_success_finish -v --basetemp=C:/q5-fix2-n2-green-0802`
  -> 6 passed in 3.07s.

## Fresh verification

- Task 5 Worker and real spawn recovery:
  `uv run pytest tests/unit/tasks/test_worker.py tests/integration/test_worker_recovery.py -v --basetemp=C:/q5-fix2-worker-0802`
  -> 40 passed in 15.88s.
- Queue, queue concurrency, backtest cancellation, and timeline contracts:
  `uv run pytest tests/integration/test_task_queue.py tests/integration/test_task_queue_concurrency.py tests/integration/test_backtest_cancellation.py tests/integration/test_backtest_timeline.py -q --basetemp=C:/q5-fix2-contracts-0802`
  -> 65 passed in 18.18s.
- Tracked lint: `uv run ruff check src tests` -> all checks passed.
- Static typing: `uv run mypy src` -> success in 88 source files.
- Full suite: `uv run pytest -q --basetemp=C:/q5-fix2-full-0802`
  -> 1311 passed, 6 skipped in 257.18s.
- Exact root lint: `uv run ruff check .` -> the same 16 pre-existing errors,
  exclusively in untracked `.r2i3g/` and `.r2target/` artifacts.

## Files in this fix

- `src/quant_core/tasks/worker.py`
- `tests/unit/tasks/test_worker.py`
- `.superpowers/sdd/2026-07-30-experiments-tasks-worker/task-5-fix-2-report.md`

The requested commit message is
`fix: close worker shutdown and cancellation races`.
