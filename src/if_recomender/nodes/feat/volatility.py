"""Volatility feature calculation from daily returns.

Uses fixed trading-day windows (not calendar months) for stable,
reproducible volatility estimates.
"""

import math
from datetime import date, timedelta

import polars as pl


def _period_to_last_date(period: int) -> date:
    """Convert YYYYMM period to last day of that month."""
    year = period // 100
    month = period % 100

    if month == 12:
        next_year = year + 1
        next_month = 1
    else:
        next_year = year
        next_month = month + 1

    first_of_next = date(next_year, next_month, 1)
    return first_of_next - timedelta(days=1)


def _get_last_n_trading_days(
    sorted_returns: pl.DataFrame,
    n_days: int,
) -> pl.DataFrame:
    """Select the last N trading days for each fund.

    For each fund, sorts by date ascending and takes the last N rows.
    If fewer than N rows are available, uses all available rows.

    Args:
        sorted_returns: Returns data sorted by (cnpj, date).
        n_days: Number of trading days to select.

    Returns:
        DataFrame with last N trading days per fund.
    """
    return (
        sorted_returns.group_by("cnpj")
        .agg(pl.all().tail(n_days))
        .explode(pl.all().exclude("cnpj"))
    )


def _calculate_annualized_volatility(
    returns: pl.DataFrame,
    trading_days_12m: int,
    output_col: str,
) -> pl.DataFrame:
    """Calculate annualized volatility for each fund."""
    annualization_factor = math.sqrt(trading_days_12m)
    return returns.group_by("cnpj").agg(
        (pl.col("daily_return").std() * annualization_factor).alias(output_col)
    )


def feat_calculate_volatility(
    daily_returns: pl.DataFrame,
    max_period: int,
    trading_days_12m: int,
    trading_days_3m: int,
) -> pl.DataFrame:
    """Calculate annualized volatility metrics from daily returns.

    Uses fixed trading-day windows for reproducible results:
    - volatility: All available data up to ref_date
    - volatility_12m: Last trading_days_12m trading days
    - volatility_3m: Last trading_days_3m trading days

    All volatilities are annualized using: daily_std * sqrt(trading_days_12m)

    Args:
        daily_returns: Daily returns with cnpj, date, daily_return.
        max_period: Reference period in YYYYMM format (used to derive ref_date).
        trading_days_12m: Number of trading days for 12-month window (also used for annualization).
        trading_days_3m: Number of trading days for 3-month window.

    Returns:
        DataFrame with cnpj, ref_date, volatility, volatility_12m, volatility_3m.
    """
    ref_date = _period_to_last_date(max_period)

    daily_returns = daily_returns.with_columns(
        pl.col("date").str.to_date().alias("date")
    )

    filtered_returns = daily_returns.filter(pl.col("date") <= ref_date)
    sorted_returns = filtered_returns.sort(["cnpj", "date"])

    long_term_vol = _calculate_annualized_volatility(
        sorted_returns, trading_days_12m, "volatility"
    )

    returns_12m = _get_last_n_trading_days(sorted_returns, trading_days_12m)
    vol_12m = _calculate_annualized_volatility(
        returns_12m, trading_days_12m, "volatility_12m"
    )

    returns_3m = _get_last_n_trading_days(sorted_returns, trading_days_3m)
    vol_3m = _calculate_annualized_volatility(
        returns_3m, trading_days_12m, "volatility_3m"
    )

    result = long_term_vol.join(vol_12m, on="cnpj", how="left")
    result = result.join(vol_3m, on="cnpj", how="left")
    result = result.with_columns(pl.lit(ref_date).alias("ref_date"))

    return result.select(
        ["cnpj", "ref_date", "volatility", "volatility_12m", "volatility_3m"]
    )
