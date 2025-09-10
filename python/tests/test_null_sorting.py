import pytest
from deltasort import SortOptimizer

pd = pytest.importorskip("pandas")
deltalake = pytest.importorskip("deltalake")


def _write_with_nulls(table_uri: str):
    df = pd.DataFrame(
        {
            "objectId": ["A", None, "B", None, "A"],
            "dateTime": ["2021-02-01", "2021-01-01", None, "2021-03-01", None],
            "value": [1, 2, 3, 4, 5],
        }
    )
    deltalake.write_deltalake(table_uri, df, mode="overwrite")


def test_null_sorting_first_vs_last(tmp_table: str):
    _write_with_nulls(tmp_table)

    opt = SortOptimizer(tmp_table)
    # Compact with NULLS FIRST
    opt.compact(["objectId", "dateTime"], nulls="first")
    # Validate with NULLS FIRST should pass
    opt.validate(["objectId", "dateTime"], nulls="first")

    # Now compact with NULLS LAST and validate
    opt.compact(["objectId", "dateTime"], nulls="last")
    opt.validate(["objectId", "dateTime"], nulls="last")

    # Validate with NULLS FIRST is not guaranteed to fail for all datasets.
