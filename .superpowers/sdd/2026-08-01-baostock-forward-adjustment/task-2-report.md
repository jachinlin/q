# Task 2 - BaoStock forward adjustment core

## Scope delivered

- Added `AdjustmentMode.FORWARD` and the `PriceAdjustmentService.bars()` branch.
- FORWARD reads raw bars through `as_of`, computes factors on the complete context,
  and returns only rows through `end`.
- It never reads `corporate_actions_as_of()`. Its event metadata retains typed
  empty lineage: unit event factors, null availability, and empty components.
- Only `open`, `high`, `low`, `close`, and `preclose` are scaled. Volume, amount,
  and all other non-price fields remain raw.
- Added `_required_positive`, `_validate_unique_bar_keys`, and log-domain
  `_forward_adjust` with finite/positive checks and a stable cumulative-overflow
  error.

## Initial TDD evidence

### RED

`uv run pytest tests/point_in_time/test_adjustments.py -k "forward" -v --basetemp=C:\t\fwd2-red`

Result: `1 failed, 20 deselected`; expected failure was
`AttributeError: type object 'AdjustmentMode' has no attribute 'FORWARD'`.
After adding the helper tests, collection also failed as expected on missing
`_forward_adjust`.

### GREEN

The original target run passed 34 tests. After Fix Round 1, the final target run
passes 38 tests. Coverage includes the fixed no-corporate-action snapshot,
`end < as_of`, stable empty metadata, independent two-instrument factors with
shuffled input, empty bars, IPO `preclose=None`, missing sessions, duplicate
keys, null/zero/signed-zero/negative/NaN/infinite non-initial preclose and prior
close values, cumulative overflow, and invalid time bounds.

### Reciprocal mutation evidence

Temporarily reversing the formula from
`log(preclose) - log(previous_close)` to its reciprocal made the fixed sample
fail with factors `1.5` where it asserts `2/3`. The source was restored before
final verification.

## Fix Round 1

The reviewer showed that changing `_required_positive` from `value <= 0` to
`value < 0` survived the original test suite. Four parameterized cases now
directly cover non-initial `preclose=0.0/-0.0` and previous-session
`close=0.0/-0.0`.

### RED

With the temporary `< 0` mutation:

`uv run pytest tests/point_in_time/test_adjustments.py -k "invalid_factor_inputs" -v --basetemp=C:\t\fwd2-fix1-red`

Result: `4 failed, 10 passed, 24 deselected`. All four zero cases reached
`math domain error` instead of the required stable field-specific `ValueError`.

### GREEN

After restoring `value <= 0`, the same selected suite with
`--basetemp=C:\t\fwd2-fix1-green` passed all 14 cases.

The report's former numerical-conflict concern was also removed. The confirmed
brief now consistently specifies factors `[2/3, 2/3, 1, 1]` and adjusted second
close `8.0`.

## Final verification

- `uv run pytest tests/point_in_time/test_adjustments.py -v --basetemp=C:\t\fwd2-fix1` - 38 passed.
- `uv run pytest tests/point_in_time -v --basetemp=C:\t\fwd2-fix1-pit` - 84 passed.
- `uv run ruff format --check src/quant_core/data/adjustments.py tests/point_in_time/test_adjustments.py` - clean.
- `uv run ruff check src/quant_core/data/adjustments.py tests/point_in_time/test_adjustments.py` - clean.
- `uv run mypy src` - clean for 53 source files.
- `git diff --check` - clean.
- `uv run pytest -q --basetemp=C:\t\fwd2-fix1-full` - 491 passed.

## Commits

- Initial Task 2: `bdbdd40` (`feat: add BaoStock forward price adjustment`).
- Fix Round 1: recorded in the task handoff after commit.

## Concerns

None in Task 2.
