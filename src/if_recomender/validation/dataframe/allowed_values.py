"""Validate that values in a column are among allowed values."""

from typing import Any
import polars as pl

from if_recomender.validation.models import OutputValidationResult
import inspect


def validate_allowed_values(
    df: pl.DataFrame,
    dataset_name: str,
    value_column: str,
    allowed_values: list[Any],
) -> OutputValidationResult:
    """Check that non-null values in a column are among allowed values.

    Null values are excluded from validation.

    Args:
        df: DataFrame to validate.
        dataset_name: Name for reporting.
        value_column: Column to check.
        allowed_values: List of valid values.

    Returns:
        OutputValidationResult with pass/fail status.
    """
    if value_column not in df.columns:
        return OutputValidationResult(
            dataset_name=dataset_name,
            validation_name=inspect.currentframe().f_code.co_name,
            passed=False,
            error_count=1,
            details=f"Column '{value_column}' not found in DataFrame",
        )

    not_allowed_values_df = df.filter(
        pl.col(value_column).is_not_null() & ~pl.col(value_column).is_in(allowed_values)
    )
    error_count = not_allowed_values_df.height

    if error_count == 0:
        return OutputValidationResult(
            dataset_name=dataset_name,
            validation_name=inspect.currentframe().f_code.co_name,
            passed=True,
            details=f"All values in '{value_column}' are among allowed values",
        )

    value_counts = (
        not_allowed_values_df.group_by(value_column)
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )

    details = f"{error_count} rows with disallowed values."

    affected_groups = value_counts.to_dicts()

    return OutputValidationResult(
        dataset_name=dataset_name,
        validation_name=inspect.currentframe().f_code.co_name,
        passed=False,
        error_count=error_count,
        details=details,
        affected_groups=affected_groups,
    )
