import sys
from pathlib import Path

import pandas as pd
from deltasort import SortOptimizer
from deltalake import write_deltalake, DeltaTable


def main(table_uri: str):
    # Create a small dataset partitioned by integer and boolean columns
    df = pd.DataFrame(
        {
            "id": [2, 1, 2, 1],
            "flag": [True, False, False, True],
            "ts": ["2021-01-02", "2021-01-01", "2021-01-03", "2021-01-02"],
            "val": [20, 10, 30, 15],
        }
    )
    write_deltalake(table_uri, df, mode="overwrite", partition_by=["id", "flag"])

    # Run compaction + sort; the tool will build a schema-aware replaceWhere for each partition
    # (integers/booleans unquoted, strings quoted, NULL -> IS NULL)
    opt = SortOptimizer(table_uri)
    opt.compact(["id", "ts"], concurrency=2)

    # Validate ordering (should pass)
    opt.validate(["id", "ts"])

    # Show resulting data sorted by id,ts for clarity
    dt = DeltaTable(table_uri)
    pdf = dt.to_pandas().sort_values(["id", "ts"]).reset_index(drop=True)
    print(pdf)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python examples/python/predicate_typing.py /path/to/table")
        sys.exit(1)
    main(sys.argv[1])
