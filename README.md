# deltasort

Compaction and global lexicographic ordering for Delta Lake tables, built on delta-rs.

## Goals
- Ensure true global ordering by user-provided columns (e.g., `objectId,dateTime`) so downstream clients can read without sorting.
- Compact small files while enforcing ordering.
- Minimize rewrites by default via partition-aware planning; optionally do a full-table sorted rewrite for strict global order.
- Provide a validator to check ordering, with a validate-only mode for safe audits.

## Components
- `crates/sorter-core`: core library for planning, rewriting, committing, and validating.
- `crates/sorter-cli`: CLI `deltasort` wrapping the core library.
- `crates/sorter-py`: native Python module (PyO3) exposing sorter-core to Python as `deltasort_rs`.
- `python/deltasort`: thin Python wrapper that calls the native module.

## Install / Build
Use one of the following workflows (equivalent):
- Makefile targets (recommended for convenience) — see “Using Makefile” below.
- Manual commands (cargo/pip/maturin) — documented in the sections that follow.

Avoid mixing steps between the two unless you know what you’re doing.
Prerequisites
- Rust toolchain (stable)
- Python 3.8+ with pip
- Linux: `patchelf` (install via `pip install maturin[patchelf]` or your package manager)

## Using Makefile
Quick commands (run from repo root):

- `make` or `make help`: list available targets
- `make build-cli`: build CLI (`./target/debug/deltasort`)
- `make fmt` / `make lint` / `make test`: format, lint, and run Rust tests
- `make test-all`: run Rust tests then Python tests
- `make setup-maturin`: install `maturin` (and `patchelf` on Linux)
- `make py-dev`: develop-install the native Python module via maturin
- `make setup-py`: install Python runtime/test deps
- `make py-test`: run Python tests (`python/tests`)
- `make py-build`: build a Python wheel via maturin
- `make clean`: clean Cargo artifacts

Examples

Rust workflow
```
make fmt
make lint
make test
make build-cli
```

Python workflow
```
make setup-maturin
make py-dev
make setup-py
make py-test
```

Tips
- Override the Python interpreter: `make PY=python3.11 py-dev`
- `make` targets are shorthands for the manual commands documented below; both approaches are equivalent.

Build CLI (optional)
- `cargo build -p sorter-cli` → `./target/debug/deltasort`

Python native bindings (PyO3)
- Install maturin: `pip install maturin` (Linux users may prefer `pip install "maturin[patchelf]"`)
- Build wheel: `python -m maturin build -m crates/sorter-py/Cargo.toml`
- Dev install into current env: `python -m maturin develop -m crates/sorter-py/Cargo.toml`
- Verify: `python -c "import deltasort_rs; print('ok')"`

## CLI Usage
- Sort columns are required and define lexicographic order.
- Core flags:
  - `--table <uri>`: Delta table path or URI.
  - `--sort-columns col1,col2,...`: leading-to-trailing lexicographic keys.
  - `--target-file-size-bytes <n>`: target parquet file size (advisory; future versions enforce).
  - `--predicate <expr>`: optional filter to limit scope (reserved, future use).
  - `--concurrency <n>`: parallelism for IO/compute (default 8).
  - `--log-level <level>`: override `RUST_LOG` (off,error,warn,info,debug,trace).
  - `--nulls <first|last>`: choose NULL sort behavior (default first).
  - `--dry-run`: plan only; do not modify the table.
  - `--validate-only`: run ordering validator; exit non-zero on violations.
  - `--repartition-by-sort-key`: perform a full-table overwrite sorted by the key to guarantee strict global ordering (rewrites the entire table; opt-in only).

Examples
```
# Plan only (no changes)
./target/debug/deltasort \
  --table /path/to/table \
  --sort-columns objectId,dateTime \
  --dry-run

# Partition-aware rewrite (default): rewrite only partitions that fail validation
./target/debug/deltasort \
  --table /path/to/table \
  --sort-columns objectId,dateTime

# Strict global ordering (full-table sorted overwrite)
./target/debug/deltasort \
  --table /path/to/table \
  --sort-columns objectId,dateTime \
  --repartition-by-sort-key

# Validate-only (no rewrite); exits non-zero if violations are found
./target/debug/deltasort \
  --table /path/to/table \
  --sort-columns objectId,dateTime \
  --validate-only

# With logging enabled via CLI
./target/debug/deltasort \
  --table /path/to/table \
  --sort-columns objectId,dateTime \
  --log-level info
```

Notes
- Default behavior is partition-aware; it skips partitions already in order and only rewrites failing partitions.
- `--repartition-by-sort-key` is optional and may be expensive on large tables; it guarantees strict global ordering across all files. This mode uses DataFusion’s execution plan and Delta’s writer to atomically replace table contents.
- Metrics are logged at `info` level per partition and as a run summary; set `--log-level info` (or `RUST_LOG=info`) to view.
- Per-partition overwrite uses a schema-aware `replaceWhere`: numbers/booleans are emitted unquoted, strings are single-quoted (with escaping), and NULLs use `IS NULL`. This avoids accidental over/under-matching when committing.

Behavior summary
- Always sorts what it rewrites: In both modes, any data we rewrite is sorted lexicographically by the provided columns before writing Parquet files.
- Default mode: Partition-aware rewrite only — avoids a full-table rewrite. It validates partitions and rewrites only those that fail ordering checks, sorting their rows by the sort key.
- Strict mode (`--repartition-by-sort-key`): Full-table sorted overwrite — reads all rows, repartitions by the sort key, writes new files in order, and replaces the entire table atomically. Use when you require strict global ordering across all partitions/files.

Clarification: “read without sorting”
- The phrase “downstream clients can read without sorting” means consumers can scan the table in key order without issuing an extra sort at query time. It does not mean deltasort itself “mostly reads without sorting.” The tool sorts all rewritten data; only already-valid partitions are skipped to minimize work.

## Python Usage
Install runtime deps in your environment: `pip install deltalake pandas pyarrow` (for the examples). Install the native bindings as above (maturin build/develop) so `deltasort_rs` is available.

Basic usage
```python
from deltasort import SortOptimizer

opt = SortOptimizer("/path/to/table")

# Validate-only: raises RuntimeError on ordering violations
opt.validate(["objectId", "dateTime"])  # may raise

# Partition-aware compaction + sort (default)
opt.compact(["objectId", "dateTime"], concurrency=8)

# Full global ordering (optional)
opt.compact(["objectId", "dateTime"], repartition_by_sort_key=True)
```

## Examples
- Quickstart: `python examples/python/quickstart.py /tmp/delta_table`
- Validate only: `python examples/python/validate_only.py /tmp/delta_table objectId,dateTime`
- Partitioned table: `python examples/python/partitioned_quickstart.py /tmp/delta_part_table`
- Predicate typing (int/bool partitions): `python examples/python/predicate_typing.py /tmp/delta_predicate_typing`

## Validation
The validator performs two checks:
- Cross-file boundary: samples min/max tuples per file for the sort columns and checks boundary monotonicity across files.
- Intra-file monotonicity: scans per-file rows for lexicographic monotonicity.
Use `--validate-only` (or `SortOptimizer.validate(...)`) to audit ordering without modifying the table. It reports the number of files checked and total violations; the Python wrapper raises on violations.

## Testing
- Makefile: `make test-all` runs Rust tests then Python tests. Requires `make py-dev` and `make setup-py` first.
- Rust: `cargo test -p sorter-core --lib`
- Python:
  - `pip install pytest deltalake pandas pyarrow`
  - Install native bindings via maturin develop (see Install / Build)
  - Run: `pytest -q python/tests`

## Roadmap
MVP completed
- Partition-aware compaction + global sort with user-defined columns
- Strict global option via full-table sorted overwrite (opt-in)
- Validator (cross-file + intra-file) and `--validate-only`
- NULLs control (`--nulls first|last`)
- Schema-aware `replaceWhere` for numbers/booleans/strings/NULLs
- Python bindings (PyO3) and tests/examples

Next (nice-to-haves)
- Structured metrics output (e.g., JSON) and richer observability
- Tighter target file-size adherence
- DATE/TIMESTAMP literal forms in typed predicates; per-column sort direction
- Load/performance tests and CI (build wheels, run cargo + pytest)

Python examples
- Quickstart (create small table, compact, validate): `python examples/python/quickstart.py /tmp/delta_table`
- Validate only: `python examples/python/validate_only.py /tmp/delta_table objectId,dateTime`
- Partitioned table quickstart (partitioned by objectId): `python examples/python/partitioned_quickstart.py /tmp/delta_part_table`
