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
    """
    Merge all feature tables into a single consolidated table.

    All inputs are in-memory DataFrames (MemoryDatasets).
    Output is persisted as the single source of truth for features.

    Args:
        volatility_per_fund: Volatility metrics
        sharpe_ratio_per_fund: Sharpe ratio metrics (sharpe_12m, sharpe_3m)
        liquidity_per_fund: Liquidity metrics
        concentration_per_fund: Concentration metrics
        asset_diversification_per_fund: Asset diversification metrics
        credit_quality_per_fund: Credit quality metrics (optional)
        fund_age_per_fund: Fund age metrics
        characteristics: Base DataFrame with all funds in scope

    Returns:
        DataFrame with ~40 columns containing all features per fund
    """

    all_features = characteristics.select("cnpj", "is_active").filter(
        pl.col("is_active") == 1
    )

    all_features = all_features.join(
        volatility_per_fund.select(["cnpj", "volatility"]),
        on="cnpj",
        how="left",
    )

    # Join sharpe (includes sharpe_12m, sharpe_3m)
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

    # Join liquidity
    all_features = all_features.join(
        liquidity_per_fund.select(
            ["cnpj", "redemption_days", "is_active", "liquidity_score"]
        ),
        on="cnpj",
        how="left",
    )

    # Join concentration (all columns)
    all_features = all_features.join(concentration_per_fund, on="cnpj", how="left")

    # Join asset diversification (all columns)
    all_features = all_features.join(
        asset_diversification_per_fund, on="cnpj", how="left"
    )

    # Join credit quality (left join - not all funds have private credit)
    all_features = all_features.join(credit_quality_per_fund, on="cnpj", how="left")

    # Join fund age (select key columns)
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
