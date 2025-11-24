import polars as pl


def _subtract_one_month(period: int) -> int:
    """Calculate previous calendar month from YYYYMM format.

    Args:
        period: Period in YYYYMM format (e.g., 202401)

    Returns:
        Previous calendar month in YYYYMM format (e.g., 202312)

    Examples:
        202401 -> 202312
        202403 -> 202402
    """
    year = period // 100
    month = period % 100
    if month == 1:
        return (year - 1) * 100 + 12
    else:
        return year * 100 + (month - 1)


def pri_create_returns_per_fund(
    period_nav: pl.DataFrame, num_period_months: int
) -> pl.DataFrame:
    """
    Calculate monthly returns for each fund, filtered to last N months.

    Uses true calendar month comparison to ensure returns
    are only calculated between consecutive months. If a fund skips a month, the return for the next month will be null rather than incorrectly computed as a multi-month return.

    Returns = (P_t - P_{t-1}) / P_{t-1}

    Args:
        period_nav: DataFrame with cnpj, period, price (NAV)
        num_period_months: Number of recent months to include (typically 12)

    Returns:
        DataFrame with columns:
        - cnpj: Fund identifier
        - period: YYYYMM
        - price: Current month NAV
        - price_previous: Previous calendar month NAV
        - monthly_return: (price - price_previous) / price_previous
        - period_rank: Rank within fund (1=most recent)
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

    returns_df = returns_df.filter(pl.col("period_rank") <= num_period_months)

    returns_df = returns_df.filter(
        pl.col("monthly_return").is_not_null() & pl.col("monthly_return").is_finite()
    ).select(
        "cnpj", "period", "price", "price_previous", "monthly_return", "period_rank"
    )

    return returns_df
