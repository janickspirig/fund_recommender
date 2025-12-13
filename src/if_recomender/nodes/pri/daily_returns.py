import polars as pl


def pri_create_daily_returns(daily_nav: pl.DataFrame) -> pl.DataFrame:
    """Calculate daily returns as (quota_t - quota_{t-1}) / quota_{t-1}.

    Only consecutive trading days produce valid returns. Weekends and holidays
    are naturally excluded since no data exists for those days.

    Args:
        daily_nav: Daily NAV data with cnpj, date, quota_value.

    Returns:
        DataFrame with cnpj, date, quota_value, quota_value_previous,
        daily_return, date_rank.
    """
    sorted_data = daily_nav.sort(["cnpj", "date"])

    with_prev = sorted_data.with_columns(
        [
            pl.col("quota_value").shift(1).over("cnpj").alias("quota_value_previous"),
            pl.col("date").shift(1).over("cnpj").alias("date_previous"),
        ]
    )

    returns_df = with_prev.with_columns(
        (
            (pl.col("quota_value") - pl.col("quota_value_previous"))
            / pl.col("quota_value_previous")
        ).alias("daily_return")
    )

    returns_df = returns_df.with_columns(
        pl.col("date").rank("ordinal", descending=True).over("cnpj").alias("date_rank")
    )

    returns_df = returns_df.filter(
        pl.col("daily_return").is_not_null() & pl.col("daily_return").is_finite()
    ).select(
        "cnpj",
        "date",
        "period",
        "quota_value",
        "quota_value_previous",
        "daily_return",
        "date_rank",
    )

    return returns_df
