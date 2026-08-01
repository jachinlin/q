# Task 2 — BaoStock forward adjustment core

## Scope delivered

- Added `AdjustmentMode.FORWARD` and the `PriceAdjustmentService.bars()` branch.
- FORWARD reads raw bars through `as_of`, computes factors on the complete context,
  and returns only rows through `end`.
- It never reads `corporate_actions_as_of()`.  Its event metadata remains the
  existing typed empty lineage schema: unit event factors, null availability, and
  empty components.
- Only `open`, `high`, `low`, `close`, and `preclose` are scaled.  Volume,
  amount, and all other non-price fields remain raw.
- Added `_required_positive`, `_validate_unique_bar_keys`, and log-domain
  `_forward_adjust` with finite/positive checks and a stable cumulative-overflow
  error.

## TDD evidence

### RED

`uv run pytest tests/point_in_time/test_adjustments.py -k "forward" -v --basetemp=C:\t\fwd2-red`

Result: `1 failed, 20 deselected`; expected failure was
`AttributeError: type object 'AdjustmentMode' has no attribute 'FORWARD'`.

After adding the additional helper tests, the forward test collection also failed
as expected on missing `_forward_adjust`.

### GREEN

The final target run was:

`uv run pytest tests/point_in_time/test_adjustments.py -v --basetemp=C:\t\fwd2-final`

Result: `34 passed`.

The tests cover the fixed no-corporate-action snapshot, `end < as_of`, stable
empty metadata, independent two-instrument factors with shuffled input, empty
bars, IPO `preclose=None`, missing sessions, duplicate keys, null/negative/NaN/
infinite non-initial preclose and prior close values, cumulative overflow, and
invalid time bounds.

### Mutation evidence

Temporarily reversed the formula from
`log(preclose) - log(previous_close)` to its reciprocal and ran:

`uv run pytest tests/point_in_time/test_adjustments.py -k "uses_baostock_preclose" -v --basetemp=C:\t\fwd2-mutant`

Result: expected failure. The fixed sample produced factors `1.5` where it
asserts `2/3`. The source was immediately restored before final verification.

## Final verification

- `uv run pytest tests/point_in_time -v --basetemp=C:\t\fwd2-pit-final` — 80 passed.
- `uv run ruff format --check src/quant_core/data/adjustments.py tests/point_in_time/test_adjustments.py` — 2 files already formatted.
- `uv run ruff check src/quant_core/data/adjustments.py tests/point_in_time/test_adjustments.py` — all checks passed.
- `uv run mypy src` — success, 53 source files.
- `git diff --check` — clean.
- `uv run pytest -q --basetemp=C:\t\fwd2-full` — 485 passed, 2 failed (see concerns).

## Commit

`feat: add BaoStock forward price adjustment` (the final revision is reported by
the task handoff to avoid a self-referential commit hash in this committed file).

## Concerns

1. The brief's fixed numerical sample is internally inconsistent. Its prescribed
   formula with `close=[10, 12, 8.4, 9]` and `preclose=[0, 10, 8, 8.4]` yields
   `[2/3, 2/3, 1, 1]`, rather than `[0.8, 0.8, 1, 1]`; additionally its specified
   `close[1]=9.6` cannot equal its specified `preclose[2]=8.0`. The implementation
   and tests follow the supplied pseudocode formula and its adjusted-price
   continuity invariant.
2. The full suite's two failures are pre-existing Task 1 availability baseline
   mismatches, not Task 2 paths: `test_fake_tushare_runs_through_same_pipeline_and_canonical_contract` and
   `test_offline_snapshot_matches_reviewed_semantic_golden` disagree on
   BaoStock daily-bar availability/content hashes.
