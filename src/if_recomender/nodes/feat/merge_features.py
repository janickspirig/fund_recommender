import polars as pl


def feat_merge_all_features(
    volatility_per_fund: pl.DataFrame,
    sharpe_ratio_per_fund: pl.DataFrame,
    liquidity_per_fund: pl.DataFrame,
    concentration_per_fund: pl.DataFrame,
    asset_diversification_per_fund: pl.DataFrame,
    credit_quality_per_fund: pl.DataFrame,
    fund_age_per_fund: pl.DataFrame,
    characteristics: pl.DataFrame,
) -> pl.DataFrame:
    """Merge all feature tables into one consolidated DataFrame.

    Only includes active funds. Missing features are filled with null.

    Args:
        volatility_per_fund: Volatility metrics per fund.
        sharpe_ratio_per_fund: Sharpe ratio metrics (12m, 3m).
        liquidity_per_fund: Liquidity score based on redemption days.
        concentration_per_fund: Portfolio concentration HHI.
        asset_diversification_per_fund: Asset-class diversification HHI.
        credit_quality_per_fund: Credit quality score.
        fund_age_per_fund: Fund age and score.
        characteristics: Fund characteristics with is_active flag.

    Returns:
        DataFrame with all features joined on cnpj.
    """

    all_features = characteristics.select("cnpj", "is_active").filter(
        pl.col("is_active") == 1
    )

    all_features = all_features.join(
        volatility_per_fund.select(["cnpj", "volatility"]),
        on="cnpj",
        how="left",
    )

    all_features = all_features.join(
        sharpe_ratio_per_fund.select(
            [
                "cnpj",
                "sharpe_12m",
                "sharpe_3m",
            ]
        ),
        on="cnpj",
        how="left",
    )

    all_features = all_features.join(
        liquidity_per_fund.select(
            ["cnpj", "redemption_days", "is_active", "liquidity_score"]
        ),
        on="cnpj",
        how="left",
    )

    all_features = all_features.join(concentration_per_fund, on="cnpj", how="left")

    all_features = all_features.join(
        asset_diversification_per_fund, on="cnpj", how="left"
    )

    all_features = all_features.join(credit_quality_per_fund, on="cnpj", how="left")

    all_features = all_features.join(
        fund_age_per_fund.select(
            [
                "cnpj",
                "inception_date",
                "fund_age_days",
                "fund_age_years",
                "fund_age_score",
            ]
        ),
        on="cnpj",
        how="left",
    ).fill_nan(None)
    return all_features
