import polars as pl


def pri_create_returns_per_fund(
    period_nav: pl.DataFrame, num_period_months: int
) -> pl.DataFrame:
    """
    Calculate monthly returns for each fund, filtered to last N months.

    Returns = (P_t - P_{t-1}) / P_{t-1}

    Args:
        period_nav: DataFrame with cnpj, period, price (NAV)
        num_period_months: Number of recent months to include (typically 12)

    Returns:
        DataFrame with columns:
        - cnpj: Fund identifier
        - period: YYYYMM
        - price: Current month NAV
        - price_previous: Previous month NAV
        - monthly_return: (price - price_previous) / price_previous
        - period_rank: Rank within fund (1=most recent)
    """
    sorted_data = period_nav.sort(["cnpj", "period"])

    sorted_data = sorted_data.with_columns(
        [
            pl.col("period")
            .rank("ordinal", descending=True)
            .over("cnpj")
            .alias("period_rank")
        ]
    )

    filtered_data = sorted_data.filter(pl.col("period_rank") <= num_period_months)

    returns_df = filtered_data.with_columns(
        pl.col("price").shift(1).over("cnpj").alias("price_previous"),
        (
            (pl.col("price") - pl.col("price").shift(1).over("cnpj"))
            / pl.col("price").shift(1).over("cnpj")
        ).alias("monthly_return"),
    )

    returns_df = returns_df.filter(
        pl.col("monthly_return").is_not_null() & pl.col("monthly_return").is_finite()
    ).select(
        "cnpj", "period", "price", "price_previous", "monthly_return", "period_rank"
    )

    return returns_df
