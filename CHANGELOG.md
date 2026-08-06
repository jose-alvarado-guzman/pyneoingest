# Changelog

All notable changes to this project will be documented in this file.

## [4.0.1] - 2026-08-06

### Fixed
- `get_query_visualization`: removed `node_caption` and `relationship_caption`
  parameters, which were dropped in `neo4j-viz` 1.0.0. Captions are now always
  derived automatically from node labels and relationship types.
- CI: replaced `IS NODE KEY` constraints (Enterprise Edition only) with
  `IS UNIQUE` in test fixtures so the test suite runs on Neo4j Community Edition.
- CI: replaced missing `age_data.csv` reference in `test_get_batches` with the
  committed `people.csv`.

### Changed
- `neo4j-viz` dependency tightened from `>=0.5.0` to `>=1.0.0` to match the
  current stable API.

## [4.0.0] - 2026-08-06

### Added
- `get_query_visualization` — execute a Cypher query and render the result as an
  interactive graph using [neo4j-viz](https://pypi.org/project/neo4j-viz/).
  Supports customizable node and relationship captions.
- `neo4j-rust-ext` is now a core dependency, providing automatic Rust-accelerated
  data serialization for all users (Python 3.10+ only).
- `neo4j-viz>=0.5.0` added as a core dependency for query graph visualization.
- `py.typed` marker — the package is now recognized as typed by mypy and pyright.
- GitHub Actions CI pipeline running tests against Neo4j 5 + APOC on Python 3.10/3.11/3.12.
- MkDocs documentation site deployed to GitHub Pages.

### Changed
- Minimum Python version raised to **3.10** (required by `neo4j-rust-ext` and `neo4j-viz`).
- Minimum Neo4j driver version raised to **5.0.0**.
- `execute_write_queries_with_data` and `execute_write_query_with_data`: parallel
  loading now uses `ThreadPoolExecutor` instead of `multiprocessing.Pool`, sharing
  a single persistent driver across all workers.
- `get_batches` rewrote using pure Python range slices — removes `numpy` dependency,
  fixes a correctness bug where single-batch cases with a remainder returned all rows
  as one chunk instead of two.
- `get_schema_visualization` now uses `network.write_html()` (replaces deprecated
  `notebook=True` / `show()`).
- Test suite split from one monolithic test method into isolated test classes
  (`TestWriteMethods`, `TestReadAndEdaMethods`) — 22 tests total.
- `run_test.sh` no longer contains hardcoded credentials; reads from `.env` file.

### Fixed
- Logger handler accumulation: repeated `Neo4jInstance` instantiation no longer
  duplicates log lines.
- `iloc` → `loc` in batch loading to avoid `IndexError` on non-default DataFrame indexes.
- `ServiceUnavailable` re-raise in `_get_driver` now chains the original exception.
- `_execute_write` and `_execute_read` preserve exception messages and chains.
- `np.NaN` → `np.nan` (removed in NumPy 2.0).
- `Dict['str', Any]` string literal type annotation corrected to `Dict[str, Any]`.
- `if rows:` → `if rows is not None:` to avoid silently skipping empty batch lists.
- `result.graph()` now correctly populated by consuming records before returning.
- `MANIFEST.in` recursive-exclude fixed with missing wildcard pattern.

### Removed
- `numpy` removed as a direct dependency.
- `requirements.txt` removed (superseded by `pyproject.toml`).
- Hardcoded credentials removed from `run_test.sh`.
- Stale `pyneoinstance/fileload/context.py` removed from the distributed package.

## [3.0.1] - 2024-03-22

### Changed
- Performance optimizations for `execute_write_query_with_data`:
  persistent driver, lazy batch conversion, early column validation.
- Additional code quality and correctness fixes.

## [3.0.0] - 2024-03-01

### Added
- Initial public release with read/write query methods, DataFrame ingestion,
  and graph EDA methods.
