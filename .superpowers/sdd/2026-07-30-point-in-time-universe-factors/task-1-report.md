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

## Fix round 1: catalog publication, partition identity, and PIT boundary hardening

### Changes

- Research reads now reject snapshots unless their catalog status is `PUBLISHED`
  and `published_at` is present (`SNAP_NOT_PUBLISHED`). Dataset versions must
  likewise be published.
- Dataset version construction and registration reject duplicate resolved
  partition paths and duplicate `content_hash` values. The research reader
  independently detects either duplicate identity in a malformed catalog and
  raises `SNAPSHOT_CATALOG_INVALID` before issuing `UNION ALL`.
- Financial fixture data now locks the Shanghai end-of-day cutoff: the row at
  `2024-04-29 15:59:59.999999 UTC` is visible, while the row at
  `2024-04-29 16:00:00 UTC`, an unknown availability timestamp, and
  `pit_usable=False` are not. Equal availability timestamps select the highest
  revision.

### RED evidence

```text
uv run pytest tests/point_in_time/test_research_repository.py tests/integration/test_snapshot_publication.py -v --basetemp=C:\t\pit1f

collected 44 items
4 failed, 40 passed
```

The failures showed that the reader attempted to read a real DRAFT metadata
snapshot, accepted duplicate catalog partitions and a DRAFT dataset version,
and that `DatasetVersionSpec` accepted duplicate path/content identities.

### GREEN and complete verification

```text
uv run pytest tests/point_in_time/test_research_repository.py tests/integration/test_snapshot_publication.py -v --basetemp=C:\t\pit1f
44 passed

uv run pytest -q --basetemp=C:\t\pit1f
215 passed in 38.90s

uv run ruff format --check
66 files already formatted

uv run ruff check
All checks passed!

uv run mypy
Success: no issues found in 36 source files

git diff --check
passed
```

### Fix round 1 concerns

None.

### Fix round 1 commit

`git commit -m "fix: harden snapshot research catalog reads"`
