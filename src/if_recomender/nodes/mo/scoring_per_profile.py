import polars as pl
from typing import Dict


def mo_scoring_per_profile(
    scoring_input: pl.DataFrame,
    investor_profiles: Dict[str, Dict[str, float]],
    gamma: float,
) -> pl.DataFrame:
    """Calculate weighted scores and rankings per investor profile.

    Weights are re-normalized based on available features per fund.
    A soft coverage penalty is applied so funds with more features rank higher.

    Args:
        scoring_input: Normalized feature scores (0-1) with cnpj.
        investor_profiles: Profile configs with feature weights.
        gamma: Coverage penalty factor (0 < gamma <= 1). At coverage=0, score
            is multiplied by gamma; at coverage=1, no penalty is applied.

    Returns:
        DataFrame with cnpj, investor_profile, score, rank, pct_features_considered.
    """

    scoring_input = scoring_input.with_columns(
        pl.col("credit_quality_score").cast(pl.Float64)
    )

    feature_columns = {
        "volatility_score": "w_volatility",
        "sharpe_score": "w_risk_reward",
        "liquidity_score": "w_liquidity",
        "concentration_score": "w_concentration",
        "asset_diversification_score": "w_asset_diversification",
        "fund_age_score": "w_fund_age",
        "credit_quality_score": "w_credit_quality",
    }

    profile_data = []
    for profile_name, weights in investor_profiles.items():
        profile_data.append(
            {
                "investor_profile": profile_name,
                "w_liquidity": weights["liquidity"],
                "w_risk_reward": weights["risk_reward"],
                "w_volatility": weights["volatility"],
                "w_concentration": weights["concentration"],
                "w_asset_diversification": weights["asset_diversification"],
                "w_fund_age": weights["fund_age"],
                "w_credit_quality": weights.get("credit_quality", 0.0),
            }
        )

    profiles_df = pl.DataFrame(profile_data)

    cross_joined = scoring_input.join(profiles_df, how="cross")

    availability_exprs = [
        pl.col(feat_col).is_finite().fill_null(False).cast(pl.Int8).alias(f"{feat_col}_available")
        for feat_col in feature_columns.keys()
    ]
    cross_joined = cross_joined.with_columns(availability_exprs)

    weight_sum_expr = sum(
        pl.col(weight_col) * pl.col(f"{feat_col}_available")
        for feat_col, weight_col in feature_columns.items()
    )

    cross_joined = cross_joined.with_columns(
        [weight_sum_expr.alias("available_weight_sum")]
    )

    pct_features_expr = sum(
        pl.col(f"{feat_col}_available") for feat_col in feature_columns.keys()
    )

    cross_joined = cross_joined.with_columns(
        [
            (pct_features_expr / len(feature_columns) * 100).alias(
                "pct_features_considered"
            )
        ]
    )

    cross_joined = cross_joined.with_columns(
        [
            pl.when(pl.col("available_weight_sum") == 0)
            .then(None)
            .otherwise(pl.col("available_weight_sum"))
            .alias("available_weight_sum")
        ]
    )

    score_expr = sum(
        pl.when(pl.col(f"{feat_col}_available") == 1)
        .then((pl.col(weight_col) / pl.col("available_weight_sum")) * pl.col(feat_col))
        .otherwise(0.0)
        for feat_col, weight_col in feature_columns.items()
    )

    scored = cross_joined.with_columns([
        score_expr.alias("score_raw"),
        (score_expr * (gamma + (1 - gamma) * (pl.col("pct_features_considered") / 100))).alias("score")
    ])

    scored_w_rank = (
        scored.with_columns(
            pl.col("score")
            .rank(method="ordinal", descending=True)
            .over("investor_profile")
            .alias("rank")
        )
        .select(
            ["cnpj", "investor_profile", "score", "rank", "pct_features_considered"]
        )
        .sort(["investor_profile", "rank"])
    )

    return scored_w_rank
