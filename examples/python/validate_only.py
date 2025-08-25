"""
Validate-only example

Run:
  python examples/python/validate_only.py /tmp/delta_table objectId,dateTime
"""
import sys
from deltasort import SortOptimizer


def main(table_uri: str, cols: str) -> None:
    cols_list = [c.strip() for c in cols.split(",") if c.strip()]
    SortOptimizer(table_uri).validate(cols_list)
    print("Ordering validated successfully.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python examples/python/validate_only.py /path/to/table col1,col2")
        sys.exit(2)
    main(sys.argv[1], sys.argv[2])
