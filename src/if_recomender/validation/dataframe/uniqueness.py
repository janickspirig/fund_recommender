import polars as pl
from typing import List

from if_recomender.validation.models import OutputValidationResult


def validate_uniqueness(
    df: pl.DataFrame,
    dataset_name: str,
    group_columns: List[str],
) -> OutputValidationResult:
    """Check that column combination is unique across rows."""
    if any(col not in df.columns for col in group_columns):
        return OutputValidationResult(
            dataset_name=dataset_name,
            validation_name="validate_uniqueness",
            passed=False,
            error_count=1,
            details=f"Not all columns in {group_columns} found in DataFrame",
        )

    grouped = df.group_by(group_columns).agg(pl.len().alias("num_occurrence"))

    groups_with_gaps = grouped.filter(pl.col("num_occurrence") > 1)

    if groups_with_gaps.height == 0:
        return OutputValidationResult(
            dataset_name=dataset_name,
            validation_name="validate_uniqueness",
            passed=True,
            details="All values in group are unique",
        )

    affected_groups = groups_with_gaps.select(group_columns).to_dicts()

    return OutputValidationResult(
        dataset_name=dataset_name,
        validation_name="validate_uniqueness",
        passed=False,
        error_count=len(affected_groups),
        details=f"{len(affected_groups)} groups are not unique",
        affected_groups=affected_groups,
    )
