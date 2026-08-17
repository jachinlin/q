# Task 6 Fix Round 4 Report

## Outcome

- Closed the remaining partition trust-boundary finding. Physical dataset
  verification and scan leases no longer derive a supposed root from a catalog
  partition path.
- `verify_published_dataset()` now requires the keyword-only
  `trusted_curated_root: Path`; `SnapshotResearchRepository` requires the same
  keyword-only constructor input. There is no optional value, default, fallback,
  or catalog-path derivation.
- `ExperimentRuntimeFactory` also requires the curated root and passes it to every
  eager required-dataset verification. The default production composition passes
  the same `Settings.curated_root` to both the repository and runtime factory.
- The scan lease opens the catalog partition below the independently supplied root
  before creating its owned temporary copy. The copy is placed inside the trusted
  curated root and is verified from its own internally controlled temporary root.
- `open_verified_file()` now checks every lexical component from the filesystem
  root through the trusted root and target. All trusted-root/ancestor components
  must already exist, be directories, and be non-link/non-reparse entries. The
  helper never creates the trusted root.
- Round-3 descriptor binding, fstat/file/dataset/row-group budgets, streaming legacy
  hash, sparse lifecycle behavior, Windows link handling, and concurrent publication
  behavior are unchanged.

## Root cause and TDD evidence

- Root cause: both production partition call sites supplied
  `partition.path.absolute().parent`, making containment tautological. The
  independently configured `Settings.curated_root` stopped at the composition root.
- Four repository boundary tests were added first:
  - eager verification rejects an ordinary catalog partition outside curated root;
  - eager verification accepts a partition in a nested curated-root directory;
  - scan/read lease rejects an outside-root catalog partition;
  - scan/read lease accepts and reads a nested in-root partition.
- Those four tests produced a stable `4 failed, 35 deselected`: the required APIs
  did not yet accept `trusted_curated_root`.
- A separate default-worker wiring test produced a stable
  `1 failed, 60 deselected`: production called the repository without the required
  configured root.
- After implementation the five intended red tests pass (`4 passed` plus
  `1 passed`). No replacement, race, or attack-style probe was used.

## Explicit-call migration

- Migrated 61 test repository construction sites, seven direct verifier call sites,
  and four runtime-factory construction sites to explicit trusted roots.
- The real acceptance workload resolves its root from `QUANT_DATA_ROOT/data/curated`;
  integration fixtures pass their explicit curated/store fixture paths.
- A final source search found no self-derived partition root and no optional/default
  `trusted_curated_root` production signature.

## Fresh verification

- Affected runtime/snapshot/repository/adapter/universe gate:
  `159 passed in 29.59s`.
- Task 6 focused gate: `422 passed, 2 skipped in 57.22s`.
- PIT/repository/universe/adjustments: `162 passed in 9.30s`.
- Runtime/client/forward-pipeline/strategy-runner/acceptance neighbors:
  `46 passed, 2 skipped in 40.00s`.
- Artifact/analysis/registry/migration: `88 passed, 3 skipped in 45.43s`.
- Factor/materialization: `308 passed in 119.27s`.
- Unfiltered repository suite: `1428 passed, 8 skipped in 286.47s`.
- `uv run ruff check src tests`: all checks passed.
- Scoped `uv run ruff format --check` for the eleven changed source/test files:
  all files formatted.
- `uv run mypy src`: success, no issues in 96 source files.
- `uv run python -m compileall -q src tests`: exit 0.
- `git diff --check`: clean, with only Git CRLF conversion notices.

## Scope notes

- No hash algorithm, storage layout, catalog schema, or public snapshot semantics
  changed.
- Historical and round-3 temporary directories remain untracked and unstaged.
