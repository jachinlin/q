# Task 6 Fix Round 3 Report

## Outcome

- Removed the experiment-wide instrument-by-session completeness requirement.
  VALIDATE now retains dataset/calendar coverage and exact benchmark bars for every
  requested execution session plus the first later session.
- Sparse lifecycle evidence is delegated to the existing business boundaries:
  newly listed and delisted instruments may lack out-of-lifecycle observations,
  suspended instruments may lack a bar, and a bar without same-day status still
  fails closed in `SnapshotBacktestMarketData.market_slice()`.
- Market-slice status assembly now ignores status-only instruments that have no
  session bar, while still requiring status for every returned bar.
- Snapshot manifests are verified from one no-follow descriptor with regular-file,
  size, component identity, post-read descriptor identity, and post-read path
  identity checks. Reads are capped at 4 MiB and issued in chunks no larger than
  64 KiB. Publication readback uses the same path.
- Published Parquet partitions are opened once and passed to `ParquetFile` through
  that same verified file object. File size, row count, schema, and row-group size
  are checked before content reads. Verification is capped at 8 GiB per partition,
  64 GiB per dataset, and 512 MiB uncompressed per row group.
- The canonical content hash remains the existing Arrow IPC stream hash. Row groups
  are written sequentially into one IPC writer backed by a SHA-256-only sink, so no
  whole-table `pq.read_table(path)` or `BufferOutputStream` materialization remains.
  Verified scan copies are also copied from the already-open descriptor.

## TDD evidence

- Sparse-universe RED: 4 failed independently. Newly listed, suspended/no-bar, and
  post-delist cases failed the daily Cartesian check; missing status failed the
  status Cartesian check.
- Manifest RED: 2 failed independently because verification used `Path.read_bytes`
  and did not reject a configured oversize before reading.
- Partition RED: 4 failed independently because verification used
  `pq.read_table(path)`, `BufferOutputStream`, no descriptor-bound `ParquetFile`, and
  no fstat size precheck.
- Multi-row-group legacy-hash characterization was green before implementation and
  remains green. Its oracle is exactly `pq.read_table(path)` followed by one
  `writer.write_table(table)` call.
- After implementation all 10 intended red tests and the characterization test pass.

## Fresh verification

- Task 6 focused gate: `421 passed, 2 skipped in 56.69s`.
- Runtime/snapshot/repository/adapter affected gate: `122 passed in 25.03s`.
- Universe business-contract gate: `32 passed in 4.61s`.
- Artifact/analysis/registry/migration: `88 passed, 3 skipped in 45.67s`.
- Factor/materialization: `308 passed in 112.65s`.
- PIT/universe/repository/adjustments: `158 passed in 8.59s`.
- Concurrent snapshot publication reliability regression: three consecutive runs
  each passed (`1 passed, 37 deselected`).
- Unfiltered repository suite: `1423 passed, 8 skipped in 286.28s`.
- `uv run ruff check src tests`: all checks passed.
- Scoped `uv run ruff format --check` for all changed source/test files: clean.
  Repository-wide format check continues to identify 23 historical untouched files;
  they were not rewritten.
- `uv run mypy src`: success, no issues in 96 source files.
- `uv run python -m compileall -q src tests`: exit 0.
- `git diff --check`: clean, with only Git CRLF conversion notices.

## Scope notes

- The content-hash algorithm and catalog schema were not migrated.
- No replacement/attack probes were added; the new coverage is ordinary contract,
  reliability, descriptor-source, size-precheck, and memory-instrumentation testing.
- Historical untracked test directories and the earlier ignored round-2 report were
  not staged or modified.
