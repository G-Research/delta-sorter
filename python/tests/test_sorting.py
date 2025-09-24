# Single file:
# Single column, for multiple types
# Multiple columns, with multiple types
#
# Multiple files
# ...

import pytest
from hypothesis import given, strategies as st
from deltasort import SortOptimizer

pd = pytest.importorskip("pandas")
deltalake = pytest.importorskip("deltalake")

INTS = st.integers(min_value=-10, max_value=10)
BOOLS = st.booleans()
# Omit nans and infs; separate tests will check those edge cases.
SIMPLE_FLOATS = st.floats(allow_nan=False, allow_infinity=False)
STRINGS = st.text()
# TODO TIMESTAMPS = st.times()


@pytest.mark.parametrize("values_strategy", [INTS, BOOLS, SIMPLE_FLOATS, STRINGS])
@given(data=st.data())
def test_single_column_single_file(
    tmp_path_factory: pytest.TempPathFactory,
    values_strategy: st.SearchStrategy,
    data: st.SearchStrategy,
) -> None:
    """
    For a single file with a single column and no nulls, requesting a sort
    actually sorts the data, and validation correctly identifies sorted vs
    not-sorted.
    """
    tmp_table = str(tmp_path_factory.mktemp("table"))

    # Use trick from https://stackoverflow.com/a/57447263 to get
    # parameterization of the values type:
    values = data.draw(st.lists(values_strategy, min_size=1, max_size=5))
    sorted_values = sorted(values)
    orig_is_sorted = values == sorted_values

    df = pd.DataFrame({"values": values})
    deltalake.write_deltalake(tmp_table, df, mode="overwrite")

    # Validation should only succeed if original values are sorted:
    opt = SortOptimizer(tmp_table)
    if orig_is_sorted:
        opt.validate(["values"])
    else:
        with pytest.raises(RuntimeError):
            opt.validate(["values"])

    opt.compact(["values"])

    # The data should be in the correct order:
    sorted_df = deltalake.DeltaTable(tmp_table).to_pandas()

    expected_df = pd.DataFrame({"values": sorted_values})
    assert sorted_df.equals(expected_df), sorted_df.compare(expected_df)

    # And validation should now always succeed:
    opt.validate(["values"])
