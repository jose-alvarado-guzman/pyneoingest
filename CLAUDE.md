# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

`pyneoinstance` is a Python package that wraps the official Neo4j driver to provide a higher-level API for submitting Cypher queries and performing exploratory data analysis (EDA) on graphs. It is published to PyPI and supports Python 3.10+ (CI tests 3.10–3.15).

## Commands

**Install for development:**
```bash
pip install -e .
```

**Run all tests** (requires a live Neo4j instance with APOC plugin):
```bash
cp .env.example .env   # first time only; fill in NEO4J_USER / NEO4J_PASSWORD / NEO4J_URI
./run_test.sh          # sources .env, checks the variables are set, runs python -m unittest
```
Or manually: `set -a; source .env; set +a; python -m unittest`. `.env` is gitignored — never commit it or print the password.

**WARNING: the test suite wipes the target database.** `_clean_db` in `test/test_neo4jdb.py` runs `MATCH (n) DETACH DELETE n` and `CALL apoc.schema.assert({},{})` (drops all constraints and indexes). Only point `NEO4J_URI` at a disposable instance.

**Run a single test file:**
```bash
python -m unittest test.test_neo4jdb
```

**Build distribution:**
```bash
python -m build
twine check --strict dist/*
```

## Architecture

The public API is two symbols re-exported from `pyneoinstance/__init__.py`:
- `Neo4jInstance` — the main class for all Neo4j interactions
- `load_yaml_file` — helper to load YAML config files with optional key validation

### Package layout

```
pyneoinstance/
  database/
    neo4jdbms.py   # Neo4jInstance class — all query and EDA methods live here
    context.py     # thin re-export of util/functions.py symbols
  fileload/
    loadyaml.py    # load_yaml_file: parses YAML, validates required keys
    helpers.py     # lower_dict_keys, check_required_keys
  util/
    functions.py   # get_logger, get_batches, get_columns_diff, get_file_name,
                   # is_reachable_url, timing
```

### Key design decisions

**One driver per instance.** `Neo4jInstance.__init__` creates a single driver (`self._driver`) that all methods share; each method opens its own short-lived session from it. Callers release it with `close()` or by using the instance as a context manager (`with Neo4jInstance(...) as graph:`).

**Parallel loading via threads.** `execute_write_queries_with_data` splits a DataFrame into row-index batches (`get_batches`, default `batchSize=100_000`). With `parallel=True` it uses a `ThreadPoolExecutor` where each worker opens its own session on the shared driver (sessions are not thread-safe; the driver is). Sequential loading reuses one session for all batches. Each batch is passed to the query as the `$rows` parameter, so data-loading queries follow the `WITH $rows AS rows UNWIND rows AS row ...` pattern. `get_columns_diff` warns when a query references columns missing from the DataFrame.

**`IN TRANSACTIONS` queries use auto-commit + manual retry.** Queries containing `CALL { ... } IN TRANSACTIONS` or `IN [n] CONCURRENT TRANSACTIONS` (detected by `_IMPLICIT_TX_RE`) are only valid in an implicit transaction, so they can't go through `session.execute_read`/`execute_write`. The read path, `get_query_visualization`, and `_execute_write` route them through `session.run` via `_run_autocommit_with_retry`, which retries `ServiceUnavailable`/`SessionExpired`/`TransientError` with exponential backoff (the driver only retries managed transactions). Any new query path must handle both branches the same way.

**EDA methods require APOC.** `get_node_label_freq`, `get_rela_type_freq`, `get_properties`, `get_rela_source_target_freq`, and `get_schema_visualization` call APOC procedures (`apoc.cypher.run`, `apoc.meta.data`, `apoc.meta.stats`, `apoc.meta.schema`) and raise `ClientError` if APOC is not installed. `get_constraints`/`get_indexes` use plain `SHOW` commands.

**Two visualization libraries.** `get_query_visualization` returns a neo4j-viz `VisualizationGraph` (render with `vg.render()` in a notebook). `get_schema_visualization` returns a `pyvis.network.Network` (render with `network.show('file.html')`).

**Cypher queries stored in YAML.** The recommended pattern (shown in tests and README) is to keep Cypher strings in a YAML config file and load them with `load_yaml_file`. Test fixtures follow this pattern in `test/cypher_queries.yaml`.

### Dependencies

`comm` and `traitlets>=5.10.0` are declared directly even though the code doesn't import them: `neo4j-viz`/`anywidget` import `comm` without declaring it, and `neo4j-viz` subclasses `traitlets.Instance[...]` (generic only since traitlets 5.10) while declaring just `traitlets>=5`. Without these, import fails on environments with ipywidgets 7 or older traitlets, notably Google Colab. Don't remove them unless neo4j-viz fixes its metadata. Check new dependency changes against a Colab-like environment, not just a fresh venv (a fresh venv resolves to ipywidgets 8, which masks the problem).

### Tests

Tests are integration tests against a real Neo4j instance — there are no mocks (a few tests inject a transient `ServiceUnavailable` to exercise the retry path). The test suite requires:
- A running Neo4j instance with APOC installed
- `NEO4J_USER`, `NEO4J_PASSWORD`, `NEO4J_URI` environment variables set (via `.env`)
- Test data CSVs in `test/` (people.csv, movies.csv, roles.csv)
- A Cypher query YAML in `test/cypher_queries.yaml`

`test/context.py` manipulates `sys.path` so tests import from the local source tree rather than any installed copy.

CI (`.github/workflows/tests.yml`) runs the suite on every push/PR to `main` against a `neo4j:5` service container with APOC, across Python 3.10–3.15. `docs.yml` deploys the mkdocs site (`docs/`, numpy-style docstrings via mkdocstrings) to GitHub Pages on push to `main`.

## Release process

1. Bump the version in **both** `pyproject.toml` (`version`) and `pyneoinstance/__init__.py` (`__version__`).
2. Add a `## [X.Y.Z] - YYYY-MM-DD` entry at the top of `CHANGELOG.md` (`### Fixed` / `### Added` sections).
3. Commit as `Release X.Y.Z: <summary>`, push to `main`, and wait for the Tests workflow to pass.
4. `python -m build && twine check --strict dist/*`, then upload with `twine upload dist/pyneoinstance-X.Y.Z*`. This is done manually by the maintainer — there is no publish workflow and no stored PyPI credentials.
5. Tag the commit the dist was built from: `git tag -a vX.Y.Z -m "Release X.Y.Z: <summary>"` and push the tag.

PyPI versions can't be overwritten, so don't upload before CI passes. The README is the PyPI long description — image links must be absolute `raw.githubusercontent.com/.../main/images/...` URLs, not relative paths.

Machine-specific setup (local virtualenv paths, etc.) belongs in `CLAUDE.local.md`, which is gitignored.
