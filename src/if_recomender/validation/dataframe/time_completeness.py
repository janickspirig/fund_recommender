"""Check for gaps in time periods within groups."""

import polars as pl

from if_recomender.validation.models import OutputValidationResult


def _add_year_month_columns(df: pl.DataFrame, time_column: str) -> pl.DataFrame:
    """Add _year and _month from time column, handling int/date/string formats."""
    col = pl.col(time_column)
    dtype = df[time_column].dtype

    if dtype == pl.Date:
        return df.with_columns(
            [
                col.dt.year().alias("_year"),
                col.dt.month().alias("_month"),
            ]
        )
    elif dtype == pl.Datetime:
        return df.with_columns(
            [
                col.dt.year().alias("_year"),
                col.dt.month().alias("_month"),
            ]
        )
    elif dtype in (
        pl.Int64,
        pl.Int32,
        pl.Int16,
        pl.Int8,
        pl.UInt64,
        pl.UInt32,
        pl.UInt16,
        pl.UInt8,
    ):
        return df.with_columns(
            [
                (col // 100).alias("_year"),
                (col % 100).alias("_month"),
            ]
        )
    elif dtype == pl.Utf8:
        return df.with_columns(
            [
                (col + "01").str.to_date("%Y%m%d").dt.year().alias("_year"),
                (col + "01").str.to_date("%Y%m%d").dt.month().alias("_month"),
            ]
        )
    else:
        return df.with_columns(
            [
                (col.cast(pl.Int64) // 100).alias("_year"),
                (col.cast(pl.Int64) % 100).alias("_month"),
            ]
        )


def validate_time_completeness(
    df: pl.DataFrame,
    dataset_name: str,
    time_column: str,
    group_column: str,
) -> OutputValidationResult:
    """Check for gaps between min and max period per group."""
    if time_column not in df.columns:
        return OutputValidationResult(
            dataset_name=dataset_name,
            validation_name="validate_time_completeness",
            passed=False,
            error_count=1,
            details=f"Column '{time_column}' not found in DataFrame",
        )

    if group_column not in df.columns:
        return OutputValidationResult(
            dataset_name=dataset_name,
            validation_name="validate_time_completeness",
            passed=False,
            error_count=1,
            details=f"Column '{group_column}' not found in DataFrame",
        )

    df_with_ym = _add_year_month_columns(df, time_column)
    df_with_ym = df_with_ym.with_columns(
        [(pl.col("_year") * 12 + pl.col("_month")).alias("_year_month")]
    )

    grouped = df_with_ym.group_by(group_column).agg(
        [
            pl.col("_year_month").min().alias("min_year_month"),
            pl.col("_year_month").max().alias("max_year_month"),
            pl.col(time_column).n_unique().alias("actual_count"),
        ]
    )

    grouped = grouped.with_columns(
        [
            (pl.col("max_year_month") - pl.col("min_year_month") + 1).alias(
                "expected_count"
            )
        ]
    ).with_columns(
        [(pl.col("expected_count") - pl.col("actual_count")).alias("missing_count")]
    )

    groups_with_gaps = grouped.filter(pl.col("missing_count") > 0)

    if groups_with_gaps.height == 0:
        return OutputValidationResult(
            dataset_name=dataset_name,
            validation_name="validate_time_completeness",
            passed=True,
            details="All groups have complete time series",
        )

    total_gaps = groups_with_gaps.select(pl.col("missing_count").sum()).item()
    affected_groups = groups_with_gaps.select(pl.col(group_column)).to_dicts()

    return OutputValidationResult(
        dataset_name=dataset_name,
        validation_name="validate_time_completeness",
        passed=False,
        error_count=total_gaps,
        details=f"{len(affected_groups)} groups have gaps in time periods ({total_gaps} total missing periods)",
        affected_groups=affected_groups,
    )
