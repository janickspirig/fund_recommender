import polars as pl


def pri_create_fund_managers(characteristics: pl.DataFrame) -> pl.DataFrame:
    """Aggregate fund counts and inception date ranges per manager.

    Only counts active funds (is_active=1) with non-null fund_manager.

    Args:
        characteristics: Fund characteristics with fund_manager, is_active, inception_date.

    Returns:
        DataFrame with fund_manager, num_funds, earliest_inception, latest_inception.
    """
    active_funds = characteristics.filter(
        (pl.col("is_active") == 1) & (pl.col("fund_manager").is_not_null())
    )

    if active_funds["inception_date"].dtype == pl.Utf8:
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

    fund_managers = (
        active_funds.group_by("fund_manager")
        .agg(
            [
                pl.col("cnpj").n_unique().alias("num_funds"),
                pl.col(inception_col).min().alias("earliest_inception"),
                pl.col(inception_col).max().alias("latest_inception"),
            ]
        )
        .sort("num_funds", descending=True)
    )

    # Convert dates back to string for CSV compatibility
    if inception_col == "inception_date_parsed":
        fund_managers = fund_managers.with_columns(
            [
                pl.col("earliest_inception").cast(pl.Utf8).alias("earliest_inception"),
                pl.col("latest_inception").cast(pl.Utf8).alias("latest_inception"),
            ]
        )

    return fund_managers
