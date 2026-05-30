from typing import List, Optional, TypedDict


class ValidationReport(TypedDict):
    checked_files: int
    boundary_violations: int
    details_sample: List[str]


def compact(
    table_uri: str,
    sort_columns: List[str],
    target_file_size_bytes: Optional[int] = ...,
    predicate: Optional[str] = ...,
    concurrency: Optional[int] = ...,
    dry_run: Optional[bool] = ...,
    repartition_by_sort_key: Optional[bool] = ...,
    nulls: str = ...,
) -> None: ...


def validate(
    table_uri: str, sort_columns: List[str], nulls: str = ...
) -> ValidationReport: ...
