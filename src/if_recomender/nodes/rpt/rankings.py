import polars as pl
from typing import Dict, Any, Tuple


def rpt_create_rankings(
    fund_scores_per_profile: pl.DataFrame,
    characteristics: pl.DataFrame,
    fund_managers: pl.DataFrame,
    n_top_funds_output: int,
    investor_profiles: Dict[str, Dict],
    guardrails: Dict[str, Any],
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Create shortlist and complete ranking for each investor profile.

    Applies profile-specific filters and guardrails as configured in parameters.yml.

    Args:
        fund_scores_per_profile: Scored funds with ranks
        characteristics: Fund metadata
        fund_managers: Manager statistics
        n_top_funds_output: Number of top funds per profile
        investor_profiles: Profile configs with optional filters
        guardrails: Min offer count and manager exclusion rules

    Returns:
        Tuple of (shortlist, complete_ranking) DataFrames
    """

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

    # filtering
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

    # guardrails
    min_offer_count = guardrails.get("min_offer_count_provider", 0)
    exclude_funds_without_manager = guardrails.get(
        "exclude_funds_without_manager", False
    )

    enriched_scores = enriched_scores.join(
        fund_managers.select(["fund_manager", "num_funds"]),
        on="fund_manager",
        how="left",
    )

    if exclude_funds_without_manager:
        enriched_scores = enriched_scores.filter(
            (pl.col("fund_manager").is_not_null())
            & (pl.col("num_funds") >= min_offer_count)
        )

    enriched_scores = enriched_scores.filter(
        (pl.col("fund_manager").is_null()) | (pl.col("num_funds") >= min_offer_count)
    )

    # create shortlist and complete ranking after filtering
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
                pl.col("cnpj").alias("CNPJ"),
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
