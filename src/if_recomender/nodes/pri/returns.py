import polars as pl


def _subtract_one_month(period: int) -> int:
    """Get previous calendar month from YYYYMM format."""
    year = period // 100
    month = period % 100
    if month == 1:
        return (year - 1) * 100 + 12
    else:
        return year * 100 + (month - 1)


def pri_create_returns_per_fund(period_nav: pl.DataFrame) -> pl.DataFrame:
    """Calculate monthly returns as (P_t - P_{t-1}) / P_{t-1}.

    Only consecutive calendar months produce valid returns. Skipped months
    yield null to avoid incorrect multi-month return calculations.

    Args:
        period_nav: NAV data with cnpj, period (YYYYMM), price.

    Returns:
        DataFrame with cnpj, period, price, price_previous, monthly_return, period_rank.
    """
    sorted_data = period_nav.sort(["cnpj", "period"])

    sorted_data = sorted_data.with_columns(
        pl.col("period")
        .map_elements(_subtract_one_month, return_dtype=pl.Int32)
        .alias("prev_period")
    )

    with_prev = sorted_data.join(
        sorted_data.select(["cnpj", "period", "price"]).rename(
            {"price": "price_previous"}
        ),
        left_on=["cnpj", "prev_period"],
        right_on=["cnpj", "period"],
        how="left",
    )

    returns_df = with_prev.with_columns(
        ((pl.col("price") - pl.col("price_previous")) / pl.col("price_previous")).alias(
            "monthly_return"
        )
    )

    returns_df = returns_df.with_columns(
        pl.col("period")
        .rank("ordinal", descending=True)
        .over("cnpj")
        .alias("period_rank")
    )

    returns_df = returns_df.filter(
        pl.col("monthly_return").is_not_null() & pl.col("monthly_return").is_finite()
    ).select(
        "cnpj", "period", "price", "price_previous", "monthly_return", "period_rank"
    )

    return returns_df
