"""Check for gaps in time periods within groups."""

from typing import Optional

import polars as pl

from if_recomender.validation.models import OutputValidationResult


def _result(
    dataset_name: str, passed: bool, error_count: int = 0, details: str = ""
) -> OutputValidationResult:
    """Build OutputValidationResult with fixed validation_name."""
    return OutputValidationResult(
        dataset_name=dataset_name,
        validation_name="validate_time_completeness",
        passed=passed,
        error_count=error_count,
        details=details,
    )


def _to_date(
    df: pl.DataFrame, time_column: str, date_format: Optional[str]
) -> pl.DataFrame:
    """Convert time column to Date type, adding '_date' column."""
    col = df[time_column]

    # Already a date type
    if col.dtype in (pl.Date, pl.Datetime):
        return df.with_columns(pl.col(time_column).cast(pl.Date).alias("_date"))

    # Integer or string: convert to string, append "01" if monthly, parse
    str_col = pl.col(time_column).cast(pl.Utf8)
    if date_format:
        date_expr = str_col.str.to_date(date_format)
    else:
        date_expr = (str_col + "01").str.to_date("%Y%m%d")

    return df.with_columns(date_expr.alias("_date"))


def _calculate_expected_monthly(group_stats: pl.DataFrame) -> pl.DataFrame:
    """Calculate expected months: max_ym - min_ym + 1."""
    return (
        group_stats.with_columns(
            (pl.col("_date").dt.year() * 12 + pl.col("_date").dt.month()).alias("_ym")
        )
        .group_by("_group")
        .agg(
            pl.col("_ym").min().alias("min_ym"),
            pl.col("_ym").max().alias("max_ym"),
            pl.col("_ym").n_unique().alias("actual"),
        )
        .with_columns((pl.col("max_ym") - pl.col("min_ym") + 1).alias("expected"))
    )


def _calculate_expected_daily(
    df_parsed: pl.DataFrame, group_column: str
) -> pl.DataFrame:
    """Calculate expected trading days between first and last date per group."""
    all_days = df_parsed.select("_date").unique().sort("_date")

    group_stats = df_parsed.group_by(group_column).agg(
        pl.col("_date").min().alias("first"),
        pl.col("_date").max().alias("last"),
        pl.col("_date").n_unique().alias("actual"),
    )

    # Cross join and filter to days within each group's range
    cross = group_stats.join(all_days, how="cross")
    expected_per_group = (
        cross.filter(
            (pl.col("_date") >= pl.col("first")) & (pl.col("_date") <= pl.col("last"))
        )
        .group_by(group_column)
        .agg(pl.len().alias("expected"))
    )

    return group_stats.join(expected_per_group, on=group_column)


def validate_time_completeness(
    df: pl.DataFrame,
    dataset_name: str,
    time_column: str,
    group_column: str,
    date_format: Optional[str] = None,
) -> OutputValidationResult:
    """Check for gaps between min and max time per group.

    Granularity is auto-detected from date_format:
    - If date_format contains '%d' -> daily completeness check
    - Otherwise -> monthly completeness check
    """
    # Validate columns exist
    if time_column not in df.columns:
        return _result(dataset_name, False, 1, f"Column '{time_column}' not found")
    if group_column not in df.columns:
        return _result(dataset_name, False, 1, f"Column '{group_column}' not found")

    # Parse dates
    df_parsed = _to_date(df, time_column, date_format)

    # Detect granularity and calculate expected counts
    is_daily = date_format and "%d" in date_format

    if is_daily:
        result = _calculate_expected_daily(df_parsed, group_column)
        unit = "trading days"
    else:
        # For monthly, rename group column temporarily for shared logic
        df_with_group = df_parsed.with_columns(pl.col(group_column).alias("_group"))
        result = _calculate_expected_monthly(df_with_group).rename(
            {"_group": group_column}
        )
        unit = "months"

    # Calculate gaps
    result = result.with_columns(
        (pl.col("expected") - pl.col("actual")).alias("missing")
    )
    groups_with_gaps = result.filter(pl.col("missing") > 0)

    if groups_with_gaps.height == 0:
        return _result(
            dataset_name, True, 0, f"All groups have complete {unit[:-1]}ly time series"
        )

    total_gaps = groups_with_gaps["missing"].sum()
    return _result(
        dataset_name,
        False,
        total_gaps,
        f"{groups_with_gaps.height} groups have gaps ({total_gaps} missing {unit})",
    )
