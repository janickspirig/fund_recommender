import polars as pl


def pri_create_fund_managers(characteristics: pl.DataFrame) -> pl.DataFrame:
    """
    Create fund managers aggregation table with fund counts and inception date ranges.

    Aggregates fund managers from characteristics table:
    - Counts active funds per manager
    - Calculates earliest and latest inception dates per manager

    Args:
        characteristics: Primary characteristics table with fund_manager, is_active, inception_date

    Returns:
        DataFrame with columns:
        - fund_manager: Manager name
        - num_funds: Count of active funds (is_active == 1) per manager
        - earliest_inception: Minimum inception_date per manager
        - latest_inception: Maximum inception_date per manager
    """
    # Filter to active funds only and exclude null fund_manager
    active_funds = characteristics.filter(
        (pl.col("is_active") == 1) & (pl.col("fund_manager").is_not_null())
    )

    # Parse inception_date if it's a string (convert to date for min/max operations)
    # Check if inception_date is string type
    if active_funds["inception_date"].dtype == pl.Utf8:
        # Parse as date (format: YYYY-MM-DD)
        active_funds = active_funds.with_columns(
            [
                pl.col("inception_date")
                .str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
                .alias("inception_date_parsed")
            ]
        )
        inception_col = "inception_date_parsed"
    else:
        inception_col = "inception_date"

    # Group by fund_manager and aggregate
    fund_managers = (
        active_funds.group_by("fund_manager")
        .agg(
            [
                pl.col("cnpj").n_unique().alias("num_funds"),  # Count distinct CNPJs
                pl.col(inception_col).min().alias("earliest_inception"),
                pl.col(inception_col).max().alias("latest_inception"),
            ]
        )
        .sort("num_funds", descending=True)
    )

    # Convert dates back to string if needed (for CSV compatibility)
    if inception_col == "inception_date_parsed":
        fund_managers = fund_managers.with_columns(
            [
                pl.col("earliest_inception").cast(pl.Utf8).alias("earliest_inception"),
                pl.col("latest_inception").cast(pl.Utf8).alias("latest_inception"),
            ]
        )

    return fund_managers
