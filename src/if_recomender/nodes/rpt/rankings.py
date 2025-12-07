import polars as pl
from typing import Dict, Tuple

from if_recomender.utils import pl_cnpj_to_formatted


def rpt_create_rankings(
    fund_scores_per_profile: pl.DataFrame,
    guardrail_mark: pl.DataFrame,
    characteristics: pl.DataFrame,
    n_top_funds_output: int,
    investor_profiles: Dict[str, Dict],
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Create shortlist and full ranking per profile after guardrail filtering.

    Applies profile-specific fund subtype and investor type filters.

    Args:
        fund_scores_per_profile: Scored funds with cnpj, investor_profile, score.
        guardrail_mark: Guardrail results with cnpj, pass_guardrail.
        characteristics: Fund metadata with cnpj, commercial_name, fund_subtype.
        n_top_funds_output: Number of top funds per profile for shortlist.
        investor_profiles: Profile configs with optional filters.

    Returns:
        Tuple of (shortlist, complete_ranking) DataFrames.
    """

    fund_scores_per_profile = fund_scores_per_profile.join(
        guardrail_mark.filter(pl.col("pass_guardrail")).select("cnpj"),
        on="cnpj",
        how="inner",
    )

    enriched_scores = fund_scores_per_profile.join(
        characteristics.select(
            [
                "cnpj",
                "target_investor_type",
                "fund_subtype",
                "fund_manager",
                "commercial_name",
            ]
        ),
        on="cnpj",
        how="left",
    )

    filtered_scores = []

    # filtering by profile-specific config
    unique_profiles = enriched_scores["investor_profile"].unique()
    for profile_name in unique_profiles:
        profile_data = enriched_scores.filter(
            pl.col("investor_profile") == profile_name
        )

        profile_config = investor_profiles[profile_name]

        if "target_investor_profile" in profile_config:
            allowed_types = profile_config["target_investor_profile"]

            profile_data = profile_data.filter(
                pl.col("target_investor_type").is_in(allowed_types)
            )

        if "allowed_fund_subtypes" in profile_config:
            allowed_subtypes = profile_config["allowed_fund_subtypes"]

            profile_data = profile_data.filter(
                pl.col("fund_subtype").is_in(allowed_subtypes)
            )

        filtered_scores.append(profile_data)

    enriched_scores = pl.concat(filtered_scores)

    top_funds = []
    complete_rankings = []

    for profile_name in enriched_scores["investor_profile"].unique():
        profile_ranking = enriched_scores.filter(
            (pl.col("investor_profile") == profile_name)
            & (pl.col("score").is_not_null())
        )

        full_profile_ranking = (
            profile_ranking.with_columns(
                pl.col("score").rank(method="ordinal", descending=True).alias("Rank")
            )
            .sort("score", descending=True)
            .select(
                pl_cnpj_to_formatted("cnpj").alias("CNPJ"),
                pl.col("commercial_name").alias("Fund Name"),
                pl.col("investor_profile").alias("Investor Profile"),
                pl.col("score").round(3).alias("Score"),
                pl.col("Rank"),
            )
        )

        profile_top_funds = full_profile_ranking.head(n_top_funds_output)

        complete_rankings.append(full_profile_ranking)
        top_funds.append(profile_top_funds)

    top_funds = pl.concat(top_funds)
    complete_rankings = pl.concat(complete_rankings)

    return top_funds, complete_rankings
