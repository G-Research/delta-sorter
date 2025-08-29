from __future__ import annotations
from typing import Optional
from deltasort_rs import compact as rs_compact, validate as rs_validate


class SortOptimizer:
    def __init__(self, table_uri: str):
        self.table_uri = table_uri

    def compact(
        self,
        sort_columns: list[str],
        target_file_size_bytes: Optional[int] = None,
        predicate: Optional[str] = None,
        concurrency: int = 8,
        dry_run: bool = False,
        repartition_by_sort_key: bool = False,
        nulls: str = "first",
    ) -> None:
        rs_compact(
            table_uri=self.table_uri,
            sort_columns=list(sort_columns),
            target_file_size_bytes=target_file_size_bytes,
            predicate=predicate,
            concurrency=concurrency,
            dry_run=dry_run,
            repartition_by_sort_key=repartition_by_sort_key,
            nulls=nulls,
        )

    def validate(self, sort_columns: list[str], nulls: str = "first") -> None:
        """Run ordering validation and raise if violations are found."""
        # rs_validate returns a dict with validator report; raise if violations
        rep = rs_validate(self.table_uri, list(sort_columns), nulls)
        if rep.get("boundary_violations", 0) > 0:
            raise RuntimeError(
                f"Ordering violations: {rep['boundary_violations']} (sample: {rep.get('details_sample')})"
            )
