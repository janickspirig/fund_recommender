import polars as pl
import math


def _subtract_months(period: int, months: int) -> int:
    """
    Subtract N months from a YYYYMM period.

    Args:
        period: Period in YYYYMM format (e.g., 202509)
        months: Number of months to subtract

    Returns:
        Period in YYYYMM format after subtracting months
    """
    year = period // 100
    month = period % 100

    total_months = year * 12 + month - 1

    total_months -= months

    new_year = total_months // 12
    new_month = (total_months % 12) + 1

    return new_year * 100 + new_month


def _get_n_month_window_periods(max_period: int, n_months: int) -> list[int]:
    """
    Get the list of N periods for the N-month window.

    Window bounds: [max_period - (n-1), ..., max_period] (last N complete months in chronological order)

    Args:
        max_period: Maximum period in YYYYMM format
        n_months: Number of months in the window (e.g., 12, 3, 1)

    Returns:
        List of N periods in chronological order [max_period - (n-1), ..., max_period]
    """
    periods = []
    for i in range(n_months - 1, -1, -1):  # From (n-1) down to 0
        periods.append(_subtract_months(max_period, i))
    return periods


def _calculate_sharpe_for_window(
    returns_sorted: pl.DataFrame,
    max_period: int,
    n_months: int,
    rf_monthly: float,
    epsilon_volatility: float = 1e-8,
) -> pl.DataFrame:
    """
    Calculate annualized Sharpe ratio for N-month window.

    Args:
        returns_sorted: DataFrame with cnpj, period, monthly_return
        max_period: Latest period in YYYYMM format
        n_months: Window size (12, 3, or 1)
        rf_monthly: Monthly risk-free rate
        epsilon_volatility: Minimum volatility threshold

    Returns:
        DataFrame with cnpj and sharpe_Nm column
    """
    window_periods = _get_n_month_window_periods(max_period, n_months)

    window_returns = returns_sorted.filter(pl.col("period").is_in(window_periods))

    sharpe_col = f"sharpe_{n_months}m"

    sharpe_per_fund = window_returns.group_by("cnpj").agg(
        [
            pl.col("monthly_return")
            .filter(
                pl.col("monthly_return").is_not_null()
                & pl.col("monthly_return").is_finite()
            )
            .count()
            .alias("n_valid_returns"),
            pl.col("monthly_return").mean().alias("mean_return"),
            pl.col("monthly_return").std().alias("std_return"),
            pl.col("period").n_unique().alias("n_periods"),
        ]
    )

    # excess return
    sharpe_per_fund = sharpe_per_fund.with_columns(
        [(pl.col("mean_return") - rf_monthly).alias("average_monthly_excess_return")]
    )

    # monthly sharpe ratio
    sharpe_per_fund = sharpe_per_fund.with_columns(
        (pl.col("average_monthly_excess_return") / pl.col("std_return")).alias(
            "monthly_sharpe"
        )
    )

    # annualize: sharpe = monthly_sharpe × sqrt(12)
    sharpe_per_fund = sharpe_per_fund.with_columns(
        [(pl.col("monthly_sharpe") * math.sqrt(12)).alias(sharpe_col)]
    )

    sharpe_per_fund = sharpe_per_fund.with_columns(
        [
            pl.when(
                (pl.col("n_valid_returns") >= n_months)
                & (pl.col("n_periods") == n_months)
                & (pl.col("std_return") > epsilon_volatility)
                & (pl.col("mean_return").is_not_null())
                & (pl.col("std_return").is_not_null())
                & (pl.col("mean_return").is_finite())
                & (pl.col("std_return").is_finite())
            )
            .then(pl.col(sharpe_col))
            .otherwise(None)
            .alias(sharpe_col)
        ]
    )

    return sharpe_per_fund.select(["cnpj", sharpe_col])


def feat_calculate_sharpe_ratio(
    returns_per_fund: pl.DataFrame, risk_free_rate_annual: float
) -> pl.DataFrame:
    """
    Calculate annualized Sharpe ratios for 12-month and 3-month windows.

    Args:
        returns_per_fund: DataFrame with cnpj, period, monthly_return
        risk_free_rate_annual: Annual risk-free rate (e.g., 0.114 for 11.4%)

    Returns:
        DataFrame with cnpj, sharpe_12m, sharpe_3m (annualized)
    """
    returns_sorted = returns_per_fund.sort(["cnpj", "period"])
    max_period = returns_sorted["period"].max()

    rf_monthly = (1 + risk_free_rate_annual) ** (1 / 12) - 1

    epsilon_volatility = 1e-8

    sharpe_12m = _calculate_sharpe_for_window(
        returns_sorted, max_period, 12, rf_monthly, epsilon_volatility
    )

    sharpe_3m = _calculate_sharpe_for_window(
        returns_sorted, max_period, 3, rf_monthly, epsilon_volatility
    )

    all_cnpjs = pl.concat(
        [sharpe_12m.select("cnpj"), sharpe_3m.select("cnpj")]
    ).unique()

    sharpe_all = all_cnpjs.join(sharpe_12m, on="cnpj", how="left")
    sharpe_all = sharpe_all.join(sharpe_3m, on="cnpj", how="left")

    return sharpe_all
