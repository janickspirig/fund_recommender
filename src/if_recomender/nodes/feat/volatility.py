import polars as pl


def feat_calculate_volatility(returns_per_fund: pl.DataFrame) -> pl.DataFrame:
    """Calculate volatility as standard deviation of monthly returns.

    Args:
        returns_per_fund: Monthly returns with cnpj, period, monthly_return.

    Returns:
        DataFrame with cnpj, mean_return, volatility (std), n_periods.
    """

    volatility_features = returns_per_fund.group_by("cnpj").agg(
        [
            pl.col("monthly_return").mean().alias("mean_return"),
            pl.col("monthly_return").std().alias("volatility"),
            pl.col("monthly_return").count().alias("n_periods"),
        ]
    )

    return volatility_features
