from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
deltalake = pytest.importorskip("deltalake")
from deltasort import SortOptimizer


def _write_partitioned_numeric_bool(table_uri: str):
    df = pd.DataFrame(
        {
            "id": [1, 1, 2, 2],
            "flag": [True, False, True, False],
            "ts": ["2021-01-01", "2021-01-02", "2021-01-01", "2021-01-02"],
            "val": [10, 20, 30, 40],
        }
    )
    # Partition by both numeric and boolean columns
    deltalake.write_deltalake(table_uri, df, mode="overwrite", partition_by=["id", "flag"])


def test_compact_partitioned_numeric_bool(tmp_table: str):
    _write_partitioned_numeric_bool(tmp_table)
    # Run compaction; success indicates typed replaceWhere worked for numeric/bool partitions
    opt = SortOptimizer(tmp_table)
    opt.compact(["id", "ts"], concurrency=2)
    # Validate ordering by a non-partition column (partition columns may not be materialized in data files)
    opt.validate(["ts"])
