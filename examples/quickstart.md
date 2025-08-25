# Quickstart: deltasort

- Build CLI:
  - `cargo build -p sorter-cli`
- Dry run:
  - `./target/debug/deltasort --table s3://bucket/table --sort-columns objectId,dateTime --dry-run`
- Python wrapper:
  - `from deltasort import SortOptimizer`
  - `SortOptimizer("s3://bucket/table").compact(["objectId","dateTime"], dry_run=True)`

Notes
- This scaffold stubs planning/execute/commit; the next iteration wires Delta transactions and Arrow sorting.
- Use `RUST_LOG=info` for logs.
