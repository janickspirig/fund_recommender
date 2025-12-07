"""Validate numeric column values are within bounds."""

import polars as pl

from if_recomender.validation.models import OutputValidationResult
import inspect


def validate_bounds(
    df: pl.DataFrame,
    dataset_name: str,
    value_column: str,
    identifier_column: str = None,
    lower_bound: float = None,
    upper_bound: float = None,
) -> OutputValidationResult:
    """Check that values in column are within lower/upper bounds (inclusive)."""
    if value_column not in df.columns:
        return OutputValidationResult(
            dataset_name=dataset_name,
            validation_name=inspect.currentframe().f_code.co_name,
            passed=False,
            error_count=1,
            details=f"Column '{value_column}' not found in DataFrame",
        )

    mask = None
    if lower_bound is not None and upper_bound is not None:
        mask = (pl.col(value_column) < lower_bound) | (
            pl.col(value_column) > upper_bound
        )
    elif lower_bound is not None:
        mask = pl.col(value_column) < lower_bound
    elif upper_bound is not None:
        mask = pl.col(value_column) > upper_bound

    if mask is None:
        return OutputValidationResult(
            dataset_name=dataset_name,
            validation_name=inspect.currentframe().f_code.co_name,
            passed=True,
            details="No bounds specified for validation.",
        )

    out_of_bounds_df = df.filter(mask)
    error_count = out_of_bounds_df.height

    if error_count == 0:
        return OutputValidationResult(
            dataset_name=dataset_name,
            validation_name=inspect.currentframe().f_code.co_name,
            passed=True,
            details=f"All values in '{value_column}' are within specified bounds",
        )

    columns = [identifier_column, value_column] if identifier_column else [value_column]
    affected_rows = out_of_bounds_df.select(columns).to_dicts()

    bound_details = []
    if lower_bound is not None:
        bound_details.append(f"lower_bound={lower_bound}")
    if upper_bound is not None:
        bound_details.append(f"upper_bound={upper_bound}")

    bound_str = ", ".join(bound_details)

    return OutputValidationResult(
        dataset_name=dataset_name,
        validation_name=inspect.currentframe().f_code.co_name,
        passed=False,
        error_count=error_count,
        details=f"{error_count} rows in '{value_column}' are out of bounds ({bound_str})",
        affected_groups=affected_rows,
    )
