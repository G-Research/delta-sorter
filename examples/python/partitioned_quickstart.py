"""
Partitioned table quickstart: partition by leading sort key

This example creates a Delta table partitioned by `objectId`, then enforces
global lexicographic ordering by (objectId, dateTime). Because the leading
sort key equals the partition key, per-partition rewrites are sufficient to
achieve global ordering.

Run:
  python examples/python/partitioned_quickstart.py /tmp/delta_part_table
"""
import sys
from pathlib import Path
import pandas as pd
from deltalake import write_deltalake, DeltaTable

from deltasort import SortOptimizer


def reset_dir(path: Path) -> None:
    if not path.exists():
        return
    # best-effort cleanup for example purposes
    for child in path.rglob("*"):
        try:
            child.unlink()
        except IsADirectoryError:
            pass
        except Exception:
            pass
    for child in sorted(path.rglob("*"), reverse=True):
        try:
            child.rmdir()
        except Exception:
            pass


def main(table_uri: str) -> None:
    p = Path(table_uri)
    reset_dir(p)

    # Create an unsorted, partitioned table (partitioned by objectId)
    df = pd.DataFrame(
        {
            "objectId": ["B", "A", "B", "A", "B", "A"],
            "dateTime": [
                "2021-02-02",
                "2021-02-01",
                "2021-01-01",
                "2021-03-01",
                "2021-02-03",
                "2021-01-15",
            ],
            "value": [4, 1, 2, 3, 5, 0],
        }
    )
    write_deltalake(table_uri, df, mode="overwrite", partition_by=["objectId"]) 

    # Validate before compaction (may fail if boundaries are out of order)
    try:
        SortOptimizer(table_uri).validate(["objectId", "dateTime"])  # may raise
        print("Initial ordering already valid (partitioned).")
    except Exception as e:
        print(f"Initial validation failed (expected for demo): {e}")

    # Compact + globally sort by (objectId, dateTime)
    SortOptimizer(table_uri).compact(sort_columns=["objectId", "dateTime"], concurrency=4)

    # Validate after compaction
    SortOptimizer(table_uri).validate(["objectId", "dateTime"])  # should pass
    print("Ordering validated successfully after compaction.")

    # Display rows (sorted for display)
    dt = DeltaTable(table_uri)
    print(
        dt.to_pandas()
        .sort_values(["objectId", "dateTime"])  # for display
        .reset_index(drop=True)
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python examples/python/partitioned_quickstart.py /path/to/partitioned_table")
        sys.exit(2)
    main(sys.argv[1])
