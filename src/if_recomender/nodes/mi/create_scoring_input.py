import polars as pl


def mi_create_scoring_input(
    all_features_per_fund: pl.DataFrame,
    normalization_lower_percentile: float,
    normalization_upper_percentile: float,
    use_log_volatility: bool,
) -> pl.DataFrame:
    """Normalize features to 0-1 scale using percentile-based scaling.

    Inverts volatility and HHI metrics so higher scores = better for all.
    Uses Sharpe 12m with 3m fallback.

    Args:
        all_features_per_fund: Merged features with raw metric values.
        normalization_lower_percentile: Lower percentile for clipping (e.g., 0.05).
        normalization_upper_percentile: Upper percentile for clipping (e.g., 0.95).
        use_log_volatility: Whether to apply log transform to volatility.

    Returns:
        DataFrame with cnpj and normalized score columns (0-1 scale).
    """
    combined = all_features_per_fund

    combined = combined.with_columns(pl.col("credit_quality_score").cast(pl.Float64))

    combined = combined.with_columns(
        [(pl.col("volatility") + 1).log().alias("volatility_transformed")]
    )

    vol_p_lower = combined["volatility_transformed"].quantile(
        normalization_lower_percentile
    )
    vol_p_upper = combined["volatility_transformed"].quantile(
        normalization_upper_percentile
    )

    # Volatility: Invert (lower volatility = more stable = better)
    combined = combined.with_columns(
        (
            1.0
            - (
                (
                    pl.col("volatility_transformed").clip(
                        lower_bound=vol_p_lower, upper_bound=vol_p_upper
                    )
                    - vol_p_lower
                )
                / (vol_p_upper - vol_p_lower)
            )
        ).alias("volatility_score")
    )

    combined = combined.with_columns(
        pl.when(pl.col("sharpe_12m").is_not_null())
        .then(pl.col("sharpe_12m"))
        .when(pl.col("sharpe_3m").is_not_null())
        .then(pl.col("sharpe_3m"))
        .otherwise(None)
        .alias("sharpe_selected")
    )

    sharpe_p_lower = combined["sharpe_selected"].quantile(
        normalization_lower_percentile
    )
    sharpe_p_upper = combined["sharpe_selected"].quantile(
        normalization_upper_percentile
    )

    combined = combined.with_columns(
        (
            (
                pl.col("sharpe_selected").clip(
                    lower_bound=sharpe_p_lower, upper_bound=sharpe_p_upper
                )
                - sharpe_p_lower
            )
            / (sharpe_p_upper - sharpe_p_lower)
            if sharpe_p_upper > sharpe_p_lower
            else 1.0
        ).alias("sharpe_score")
    )

    liq_p_lower = combined["liquidity_score"].quantile(normalization_lower_percentile)
    liq_p_upper = combined["liquidity_score"].quantile(normalization_upper_percentile)
    combined = combined.with_columns(
        (
            (
                pl.col("liquidity_score").clip(
                    lower_bound=liq_p_lower, upper_bound=liq_p_upper
                )
                - liq_p_lower
            )
            / (liq_p_upper - liq_p_lower)
        ).alias("liquidity_score_final")
    )

    # Concentration: Invert HHI (lower HHI = more diversified = better)
    conc_hhi_p_lower = combined["concentration_hhi"].quantile(
        normalization_lower_percentile
    )
    conc_hhi_p_upper = combined["concentration_hhi"].quantile(
        normalization_upper_percentile
    )

    combined = combined.with_columns(
        (
            1.0
            - (
                (
                    pl.col("concentration_hhi").clip(
                        lower_bound=conc_hhi_p_lower, upper_bound=conc_hhi_p_upper
                    )
                    - conc_hhi_p_lower
                )
                / (conc_hhi_p_upper - conc_hhi_p_lower)
            )
        ).alias("concentration_score_final")
    )

    # Asset Diversification: Invert HHI (lower HHI = more diversified = better)
    asset_hhi_p_lower = combined["asset_diversification_hhi"].quantile(
        normalization_lower_percentile
    )
    asset_hhi_p_upper = combined["asset_diversification_hhi"].quantile(
        normalization_upper_percentile
    )
    combined = combined.with_columns(
        (
            1.0
            - (
                (
                    pl.col("asset_diversification_hhi").clip(
                        lower_bound=asset_hhi_p_lower, upper_bound=asset_hhi_p_upper
                    )
                    - asset_hhi_p_lower
                )
                / (asset_hhi_p_upper - asset_hhi_p_lower)
            )
        ).alias("asset_diversification_score_final")
    )

    age_p_lower = combined["fund_age_score"].quantile(normalization_lower_percentile)
    age_p_upper = combined["fund_age_score"].quantile(normalization_upper_percentile)
    combined = combined.with_columns(
        (
            (
                pl.col("fund_age_score").clip(
                    lower_bound=age_p_lower, upper_bound=age_p_upper
                )
                - age_p_lower
            )
            / (age_p_upper - age_p_lower)
        ).alias("fund_age_score_final")
    )

    credit_p_lower = combined["credit_quality_score"].quantile(
        normalization_lower_percentile
    )
    credit_p_upper = combined["credit_quality_score"].quantile(
        normalization_upper_percentile
    )
    combined = combined.with_columns(
        (
            (
                pl.col("credit_quality_score").clip(credit_p_lower, credit_p_upper)
                - credit_p_lower
            )
            / (credit_p_upper - credit_p_lower)
        ).alias("credit_quality_score_final")
    )

    scoring_input = combined.select(
        [
            "cnpj",
            "volatility_score",
            "sharpe_score",
            "liquidity_score_final",
            "concentration_score_final",
            "asset_diversification_score_final",
            "fund_age_score_final",
            "credit_quality_score_final",
        ]
    ).rename(
        {
            "liquidity_score_final": "liquidity_score",
            "concentration_score_final": "concentration_score",
            "asset_diversification_score_final": "asset_diversification_score",
            "fund_age_score_final": "fund_age_score",
            "credit_quality_score_final": "credit_quality_score",
        }
    )

    return scoring_input
