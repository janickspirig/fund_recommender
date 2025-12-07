import polars as pl
import math


def _subtract_months(period: int, months: int) -> int:
    """Subtract N months from a YYYYMM period."""
    year = period // 100
    month = period % 100

    total_months = year * 12 + month - 1

    total_months -= months

    new_year = total_months // 12
    new_month = (total_months % 12) + 1

    return new_year * 100 + new_month


def _get_n_month_window_periods(max_period: int, n_months: int) -> list[int]:
    """Get the last N periods ending at max_period in chronological order."""
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
    """Calculate annualized Sharpe ratio for N-month window."""
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

    sharpe_per_fund = sharpe_per_fund.with_columns(
        [(pl.col("mean_return") - rf_monthly).alias("average_monthly_excess_return")]
    )

    sharpe_per_fund = sharpe_per_fund.with_columns(
        (pl.col("average_monthly_excess_return") / pl.col("std_return")).alias(
            "monthly_sharpe"
        )
    )

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
    returns_per_fund: pl.DataFrame, risk_free_rate_annual: float, max_period: int
) -> pl.DataFrame:
    """Calculate annualized Sharpe ratios for 12-month and 3-month windows.

    Funds with incomplete data or near-zero volatility get null Sharpe values.

    Args:
        returns_per_fund: Monthly returns with cnpj, period, monthly_return.
        risk_free_rate_annual: Annual risk-free rate (e.g., 0.1371 for CDI).
        max_period: Most recent period in YYYYMM format.

    Returns:
        DataFrame with cnpj, sharpe_12m, sharpe_3m (annualized).
    """
    returns_sorted = returns_per_fund.sort(["cnpj", "period"])

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
