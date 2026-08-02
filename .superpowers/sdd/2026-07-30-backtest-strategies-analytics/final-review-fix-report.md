# Final review consolidated fix report

## Scope and base

- Fix base: `0edb213483d1aeec2cc12b7f533afd9a6d122985`.
- Implementation commit: `daec103fccec78f424b9339c0b63e9f969fceabc` (`fix: close backtest analytics review gaps`).
- Scope was limited to the four findings in `final-review-findings.md`. No strategy-laboratory runner/adapters, Dashboard work, deferred minor cleanup, temporary-directory cleanup, or progress-ledger edits were made.
- The systematic-debugging, test-driven-development, writing-good-tests, and verification-before-completion instructions were read before source changes.

## Finding 1: end-date BUY cannot register T+1

### Root cause

`BacktestEngine.run()` requested an inclusive calendar ending exactly at `request.end_date`, while `PortfolioAccount.apply()` correctly resolves every BUY lot's `sellable_date` with `TradingCalendar.next_session(fill.trade_date)`. Therefore an adapter honoring the exact requested range could never resolve a BUY on the final simulated session. `_validate_calendar()` checked only inclusive natural-date coverage and did not prove that a real trading session existed after the requested end.

### RED

Command:

```powershell
uv run pytest tests/integration/test_backtest_timeline.py::test_end_date_buy_uses_requested_next_session_coverage_and_publishes tests/integration/test_backtest_timeline.py::test_engine_rejects_missing_post_end_session_before_market_loop -vv --basetemp=.pytest-f1-red
```

Exact result:

```text
collected 2 items
tests/integration/test_backtest_timeline.py::test_end_date_buy_uses_requested_next_session_coverage_and_publishes FAILED [ 50%]
tests/integration/test_backtest_timeline.py::test_engine_rejects_missing_post_end_session_before_market_loop FAILED [100%]

E ValueError: no later trading session in loaded coverage
E Failed: DID NOT RAISE ValueError
============================== 2 failed in 2.14s ==============================
```

The first failure reproduced the production abort at `accounting.py:347`; the second proved the engine entered/published from the loop instead of rejecting insufficient coverage before reading market slices.

### Minimal implementation

- Extended `BacktestMarketData.calendar()` with the explicit keyword contract `include_next_session: bool`.
- `BacktestEngine.run()` now requests `include_next_session=True` without guessing with natural-date `+1`.
- `_validate_calendar()` calls `calendar.next_session(request.end_date)` before constructing the account or entering the market loop and raises a stable coverage error if the adapter fails to supply the first actual later session.
- The positive regression uses a multi-day holiday gap (`2024-01-09` to `2024-01-12`), publishes successfully, and observes the final-session lot as not sellable on its buy date. The existing weekend T+1 regression continues to prove unlocking occurs on the next loaded actual session rather than the next natural date.

### GREEN

Command:

```powershell
uv run pytest tests/integration/test_backtest_timeline.py::test_end_date_buy_uses_requested_next_session_coverage_and_publishes tests/integration/test_backtest_timeline.py::test_engine_rejects_missing_post_end_session_before_market_loop tests/unit/backtest/test_accounting.py::test_t_plus_one_unlocks_after_weekend_on_next_loaded_session -vv --basetemp=.pytest-f1-green
```

Exact result:

```text
collected 3 items
tests/integration/test_backtest_timeline.py::test_end_date_buy_uses_requested_next_session_coverage_and_publishes PASSED [ 33%]
tests/integration/test_backtest_timeline.py::test_engine_rejects_missing_post_end_session_before_market_loop PASSED [ 66%]
tests/unit/backtest/test_accounting.py::test_t_plus_one_unlocks_after_weekend_on_next_loaded_session PASSED [100%]
============================== 3 passed in 2.04s ==============================
```

## Finding 2: period returns omit the cross-period first-session return

### Root cause

`_period_returns()` grouped observations by month/year and divided each group's last NAV by that same group's first NAV. For every non-first group this discarded the daily return from the previous period's ending NAV to the new period's first session. The same defect affected portfolio and benchmark month/year returns and prevented period returns from compounding to the full cumulative return.

### RED

Command:

```powershell
uv run pytest tests/unit/analytics/test_performance.py::test_calculate_performance_matches_hand_checked_metrics_and_periods tests/unit/analytics/test_performance.py::test_annual_returns_include_the_first_session_move_across_years tests/unit/analytics/test_performance.py::test_period_returns_compound_to_full_cumulative_return -vv --basetemp=.pytest-f2-red
```

Exact result:

```text
collected 3 items
tests/unit/analytics/test_performance.py::test_calculate_performance_matches_hand_checked_metrics_and_periods FAILED [ 33%]
tests/unit/analytics/test_performance.py::test_annual_returns_include_the_first_session_move_across_years FAILED [ 66%]
tests/unit/analytics/test_performance.py::test_period_returns_compound_to_full_cumulative_return FAILED [100%]

February obtained portfolio_return: 0.11111111111111116; expected: 0.0
2024 obtained portfolio_return: 0.22222222222222232; expected: 0.1
compounded obtained: 0.34444444444444455; expected: 0.21
============================== 3 failed in 1.04s ==============================
```

### Minimal implementation

For each non-first period, `_period_returns()` now uses the immediately preceding observation as the portfolio and benchmark baseline. The first period retains its original first-observation baseline. Period keys, `period_start`, `period_end`, schemas, ordering, and relative-return definition remain unchanged.

### GREEN

Command:

```powershell
uv run pytest tests/unit/analytics/test_performance.py::test_calculate_performance_matches_hand_checked_metrics_and_periods tests/unit/analytics/test_performance.py::test_annual_returns_include_the_first_session_move_across_years tests/unit/analytics/test_performance.py::test_period_returns_compound_to_full_cumulative_return -vv --basetemp=.pytest-f2-green
```

Exact result:

```text
collected 3 items
tests/unit/analytics/test_performance.py::test_calculate_performance_matches_hand_checked_metrics_and_periods PASSED [ 33%]
tests/unit/analytics/test_performance.py::test_annual_returns_include_the_first_session_move_across_years PASSED [ 66%]
tests/unit/analytics/test_performance.py::test_period_returns_compound_to_full_cumulative_return PASSED [100%]
============================== 3 passed in 0.80s ==============================
```

## Finding 3: zero initial cash conflicts with positive-NAV contracts

### Root cause

`BacktestRequest.__post_init__()` admitted `initial_cash_fen == 0` with a nonnegative check. Downstream strategy state creation and analytics validation both require strictly positive NAV, so the public request boundary could construct a run that later either failed during target generation or published a zero-NAV artifact that analytics rejected.

### RED

Command:

```powershell
uv run pytest tests/regression/test_backtest_golden.py::test_backtest_request_rejects_nonpositive_initial_cash tests/regression/test_backtest_golden.py::test_backtest_request_accepts_one_fen_initial_cash -vv --basetemp=.pytest-f3-red
```

Exact result:

```text
collected 3 items
tests/regression/test_backtest_golden.py::test_backtest_request_rejects_nonpositive_initial_cash[0] FAILED [ 33%]
tests/regression/test_backtest_golden.py::test_backtest_request_rejects_nonpositive_initial_cash[-1] FAILED [ 66%]
tests/regression/test_backtest_golden.py::test_backtest_request_accepts_one_fen_initial_cash PASSED [100%]

E Failed: DID NOT RAISE ValueError
E Expected regex: 'positive integer'
E Actual message: 'initial_cash_fen must be a nonnegative integer'
========================= 2 failed, 1 passed in 1.53s =========================
```

### Minimal implementation

Changed only the public `BacktestRequest` construction guard to require an actual `int` greater than zero and emit `initial_cash_fen must be a positive integer`. Existing positive-NAV strategy and analytics invariants remain intact; artifact compatibility validators were not broadened or repurposed.

### GREEN

Command:

```powershell
uv run pytest tests/regression/test_backtest_golden.py::test_backtest_request_rejects_nonpositive_initial_cash tests/regression/test_backtest_golden.py::test_backtest_request_accepts_one_fen_initial_cash -vv --basetemp=.pytest-f3-green
```

Exact result:

```text
collected 3 items
tests/regression/test_backtest_golden.py::test_backtest_request_rejects_nonpositive_initial_cash[0] PASSED [ 33%]
tests/regression/test_backtest_golden.py::test_backtest_request_rejects_nonpositive_initial_cash[-1] PASSED [ 66%]
tests/regression/test_backtest_golden.py::test_backtest_request_accepts_one_fen_initial_cash PASSED [100%]
============================== 3 passed in 1.35s ==============================
```

## Finding 4: pre-account-start actions incorrectly require snapshots

### Root cause

`PortfolioAccount.begin_session()` treated every missing `record_date` entry in `_record_quantities` identically. It did not track when the cash-only account actually entered its first session, so a record date before that lifecycle was incorrectly treated like a skipped/missing record date within an active lifecycle. A cash-only account with no initial-holdings port can determine the former entitlement is zero, but must continue to fail closed for the latter.

### RED

Command:

```powershell
uv run pytest tests/unit/backtest/test_accounting.py::test_pre_lifecycle_record_date_has_zero_entitlement tests/unit/backtest/test_accounting.py::test_in_lifecycle_missing_record_date_snapshot_fails_closed tests/unit/backtest/test_accounting.py::test_record_date_entitlements_are_idempotent -vv --basetemp=.pytest-f4-red
```

Exact result:

```text
collected 3 items
tests/unit/backtest/test_accounting.py::test_pre_lifecycle_record_date_has_zero_entitlement FAILED [ 33%]
tests/unit/backtest/test_accounting.py::test_in_lifecycle_missing_record_date_snapshot_fails_closed PASSED [ 66%]
tests/unit/backtest/test_accounting.py::test_record_date_entitlements_are_idempotent PASSED [100%]

E ValueError: corporate action requires record-date evidence
========================= 1 failed, 2 passed in 1.56s =========================
```

The two passing safeguards in the RED run established that the new behavior must not weaken in-lifecycle evidence checks or alter entitled/idempotent action accounting.

### Minimal implementation

- Added `_lifecycle_start`, assigned atomically on the first successful `begin_session()`.
- When record-date evidence is absent and `record_date < lifecycle_start`, entitlement is explicitly zero.
- When evidence is absent on or after lifecycle start, the existing fail-closed error remains.
- Existing evidence continues to determine entitlement exactly as before.

### GREEN

Command:

```powershell
uv run pytest tests/unit/backtest/test_accounting.py::test_pre_lifecycle_record_date_has_zero_entitlement tests/unit/backtest/test_accounting.py::test_in_lifecycle_missing_record_date_snapshot_fails_closed tests/unit/backtest/test_accounting.py::test_record_date_entitlements_are_idempotent -vv --basetemp=.pytest-f4-green
```

Exact result:

```text
collected 3 items
tests/unit/backtest/test_accounting.py::test_pre_lifecycle_record_date_has_zero_entitlement PASSED [ 33%]
tests/unit/backtest/test_accounting.py::test_in_lifecycle_missing_record_date_snapshot_fails_closed PASSED [ 66%]
tests/unit/backtest/test_accounting.py::test_record_date_entitlements_are_idempotent PASSED [100%]
============================== 3 passed in 1.44s ==============================
```

## Final verification

### Focused backtest/accounting/analytics suites

```powershell
uv run pytest tests/unit/backtest tests/integration/test_backtest_timeline.py tests/integration/test_backtest_cancellation.py tests/regression/test_backtest_golden.py tests/regression/test_accounting_ledger_golden.py tests/unit/analytics tests/integration/test_analysis_materialization.py -q --basetemp=C:\qff260802
```

```text
169 passed in 5.25s
```

### Complete repository suite

```powershell
uv run pytest -q --basetemp=C:\qfb260802
```

```text
1060 passed, 3 skipped in 187.36s (0:03:07)
```

The short absolute basetemp is intentionally outside the source tree. A discarded earlier relative-basetemp run triggered the repository's expected `data_root must be outside source_root` safety guard and Windows long-path failures; a focused probe with the corrected absolute short basetemp passed `24 passed`, confirming this was runner placement rather than a product regression.

### Static checks

```powershell
uv run ruff format --check src tests
# 130 files already formatted (exit 0)

uv run ruff check .
# All checks passed! (exit 0)

uv run mypy
# Success: no issues found in 77 source files (exit 0)

git diff --check
# exit 0; only Git's existing LF-to-CRLF working-copy warnings
```

The additionally executed whole-repository `uv run ruff format --check .` exits 1 solely for Python code fences in these two unchanged planning documents:

- `docs/superpowers/plans/2026-08-01-baostock-forward-adjustment.md`
- `docs/superpowers/plans/2026-08-02-strategy-verification-remediation.md`

`git diff --exit-code 0edb213483d1aeec2cc12b7f533afd9a6d122985 daec103 -- <both files>` exits 0, proving the implementation commit did not modify them. They were left untouched because the findings explicitly exclude deferred minors.

## Outcome and concern

All four requested findings are fixed and protected by independent behavioral regressions. Focused and complete pytest suites, Python-source/test formatting, Ruff lint, mypy, and diff whitespace checks pass. The only concern is the pre-existing whole-repository Ruff formatting debt in the two out-of-scope Markdown plan files described above.
