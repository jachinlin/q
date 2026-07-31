# Task 1 Report: snapshot-bound point-in-time research repository

## Changes

- Added `SnapshotResearchRepository` and the `ResearchDataRepository` protocol.
- Every query accepts an explicit `SnapshotId`, resolves only that snapshot's
  catalog-bound dataset version and Parquet partitions, and accepts no caller
  physical paths.
- Added parameterized DuckDB reads for bars, financial observations, and
  security status. Canonical schema column names are the only dynamic SQL
  identifiers; all values, including partition paths, are positional
  parameters.
- Financial reads treat `as_of` as Asia/Shanghai end-of-day, convert it to UTC,
  exclude unknown/unusable availability, and choose the newest available
  revision per instrument/period/metric.
- Added `SnapshotDatasetMissing`, a `QuantError` with the stable code
  `SNAPSHOT_DATASET_MISSING` and deterministic context.
- Added canonical Parquet fixture data and five behavioral repository tests.

## RED evidence

Command:

```text
uv run pytest tests/point_in_time/test_research_repository.py -v --basetemp=C:\t\pit1
```

Output before implementation:

```text
collected 0 items / 1 error
ModuleNotFoundError: No module named 'quant_core.data.repository'
```

The failure was the expected missing production module.

## GREEN evidence

Command:

```text
uv run pytest tests/point_in_time/test_research_repository.py -v --basetemp=C:\t\pit1
```

Output after implementation:

```text
collected 5 items
5 passed
```

## Complete verification

- `uv run pytest tests/point_in_time/test_research_repository.py -v --basetemp=C:\t\pit1` — 5 passed.
- `uv run pytest -q --basetemp=C:\t\pit1` — 210 passed in 40.45s.
- `uv run ruff format --check` — 66 files already formatted.
- `uv run ruff check` — passed.
- `uv run mypy` — `Success: no issues found in 36 source files`.
- `git diff --check` — passed.

## Commit

`git commit -m "feat: add point-in-time research repository"`

## Concerns

None. The task deliberately returns a `pl.LazyFrame` backed by the completed,
parameterized DuckDB result so the caller retains the standard lazy dataframe
interface while the repository never exposes unbound filesystem paths.
