"""
Quickstart: compact with global sort from Python

Requirements:
- pip install deltalake pandas pyarrow
- build CLI first: cargo build -p sorter-cli

Run:
  python examples/python/quickstart.py /tmp/delta_table
"""
import sys
from pathlib import Path
import pandas as pd
from deltalake import write_deltalake, DeltaTable

from deltasort import SortOptimizer


def main(table_uri: str) -> None:
    p = Path(table_uri)
    if p.exists():
        # Overwrite for a clean run
        for child in p.rglob("*"):
            try:
                child.unlink()
            except IsADirectoryError:
                pass
        for child in sorted(p.rglob("*"), reverse=True):
            try:
                child.rmdir()
            except Exception:
                pass

    # Create an unsorted table
    df = pd.DataFrame({
        "objectId": ["B", "A", "B", "A"],
        "dateTime": ["2021-02-02", "2021-02-01", "2021-01-01", "2021-03-01"],
        "value": [4, 1, 2, 3],
    })
    write_deltalake(table_uri, df, mode="overwrite")

    # Validate before compaction (should fail with non-zero if violated when run through CLI)
    SortOptimizer(table_uri).validate(["objectId", "dateTime"])  # may raise if violations are found

    # Compact + global sort
    SortOptimizer(table_uri).compact(sort_columns=["objectId", "dateTime"], concurrency=4)

    # Validate after compaction
    SortOptimizer(table_uri).validate(["objectId", "dateTime"])  # should pass

    # Show rows
    dt = DeltaTable(table_uri)
    print(dt.to_pandas().sort_values(["objectId", "dateTime"]).reset_index(drop=True))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python examples/python/quickstart.py /path/to/table")
        sys.exit(2)
    main(sys.argv[1])
