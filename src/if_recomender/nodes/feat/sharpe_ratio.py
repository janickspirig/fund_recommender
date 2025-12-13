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

    For each fund, takes the last N rows after sorting by date ascending.
    If fewer than N rows are available, uses all available rows.
    """
    return (
        sorted_returns.group_by("cnpj")
        .agg(pl.all().tail(n_days))
        .explode(pl.all().exclude("cnpj"))
    )


def _calculate_annualized_excess_return_with_count(
    returns: pl.DataFrame,
    rf_daily: float,
    trading_days_12m: int,
    output_col: str,
    count_col: str,
) -> pl.DataFrame:
    """Calculate annualized mean excess return and observation count for each fund.

    Formula: μ_annual = mean(daily_return - rf_daily) * trading_days_12m

    Returns DataFrame with cnpj, output_col (excess return), and count_col (n observations).
    """
    return returns.group_by("cnpj").agg(
        [
            ((pl.col("daily_return") - rf_daily).mean() * trading_days_12m).alias(
                output_col
            ),
            pl.len().alias(count_col),
        ]
    )


def feat_calculate_sharpe_ratio(
    daily_returns: pl.DataFrame,
    volatility_per_fund: pl.DataFrame,
    risk_free_rate_annual: float,
    max_period: int,
    trading_days_12m: int,
    trading_days_3m: int,
    epsilon_volatility: float,
    sharpe_cap: float,
) -> pl.DataFrame:
    """Calculate annualized Sharpe ratios using daily returns and pre-calculated volatility.

    Formula:
        rf_daily = (1 + rf_annual)^(1/trading_days_12m) - 1
        μ_annual = mean(daily_return - rf_daily) * trading_days_12m
        sharpe = μ_annual / volatility_Nm

    Args:
        daily_returns: Daily returns with cnpj, date, daily_return.
        volatility_per_fund: Pre-calculated volatilities with cnpj, volatility_12m, volatility_3m.
        risk_free_rate_annual: Annual risk-free rate (e.g., 0.1371 for CDI).
        max_period: Reference period in YYYYMM format.
        trading_days_12m: Number of trading days for 12-month window (also used for annualization).
        trading_days_3m: Number of trading days for 3-month window.
        epsilon_volatility: Minimum volatility threshold; funds below this get null Sharpe.
        sharpe_cap: Maximum absolute Sharpe value; values are clipped to [-cap, +cap].

    Returns:
        DataFrame with cnpj, sharpe_12m, sharpe_3m, pct_cov_12m, pct_cov_3m.
    """
    ref_date = _period_to_last_date(max_period)
    rf_daily = (1 + risk_free_rate_annual) ** (1 / trading_days_12m) - 1

    daily_returns = daily_returns.with_columns(
        pl.col("date").str.to_date().alias("date")
    )

    filtered_returns = daily_returns.filter(pl.col("date") <= ref_date)
    sorted_returns = filtered_returns.sort(["cnpj", "date"])

    returns_12m = _get_last_n_trading_days(sorted_returns, trading_days_12m)
    excess_12m = _calculate_annualized_excess_return_with_count(
        returns_12m, rf_daily, trading_days_12m, "excess_return_12m", "n_obs_12m"
    )

    returns_3m = _get_last_n_trading_days(sorted_returns, trading_days_3m)
    excess_3m = _calculate_annualized_excess_return_with_count(
        returns_3m, rf_daily, trading_days_12m, "excess_return_3m", "n_obs_3m"
    )

    result = excess_12m.join(excess_3m, on="cnpj", how="outer")

    result = result.join(
        volatility_per_fund.select(["cnpj", "volatility_12m", "volatility_3m"]),
        on="cnpj",
        how="left",
    )

    result = result.with_columns(
        [
            (pl.col("n_obs_12m") / trading_days_12m).alias("pct_cov_12m"),
            (pl.col("n_obs_3m") / trading_days_3m).alias("pct_cov_3m"),
        ]
    )

    result = result.with_columns(
        [
            pl.when(
                pl.col("volatility_12m").is_not_null()
                & pl.col("volatility_12m").is_finite()
                & (pl.col("volatility_12m") > epsilon_volatility)
                & pl.col("excess_return_12m").is_not_null()
                & pl.col("excess_return_12m").is_finite()
            )
            .then(pl.col("excess_return_12m") / pl.col("volatility_12m"))
            .otherwise(None)
            .alias("sharpe_12m_raw"),
            pl.when(
                pl.col("volatility_3m").is_not_null()
                & pl.col("volatility_3m").is_finite()
                & (pl.col("volatility_3m") > epsilon_volatility)
                & pl.col("excess_return_3m").is_not_null()
                & pl.col("excess_return_3m").is_finite()
            )
            .then(pl.col("excess_return_3m") / pl.col("volatility_3m"))
            .otherwise(None)
            .alias("sharpe_3m_raw"),
        ]
    )

    result = result.with_columns(
        [
            pl.col("sharpe_12m_raw").clip(-sharpe_cap, sharpe_cap).alias("sharpe_12m"),
            pl.col("sharpe_3m_raw").clip(-sharpe_cap, sharpe_cap).alias("sharpe_3m"),
        ]
    )

    return result.select(
        ["cnpj", "sharpe_12m", "sharpe_3m", "pct_cov_12m", "pct_cov_3m"]
    )
