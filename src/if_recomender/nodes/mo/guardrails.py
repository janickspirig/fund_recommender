"""Guardrail nodes for filtering funds before final recommendations.

Each guardrail runs in parallel and returns cnpj + failed flag.
"""

import polars as pl
from typing import Any, Dict


def mo_guardrail_min_offer_per_issuer(
    scores_per_profile: pl.DataFrame,
    fund_managers: pl.DataFrame,
    characteristics: pl.DataFrame,
    config: Dict[str, Any],
) -> pl.DataFrame:
    """Filter funds whose manager has fewer than min_offer_count funds.

    Args:
        scores_per_profile: Scored funds with cnpj.
        fund_managers: Manager aggregation with fund_manager, num_funds.
        characteristics: Fund metadata with cnpj, fund_manager.
        config: Config with active (bool) and params.min_offer_count (int).

    Returns:
        DataFrame with cnpj, failed (bool).
    """
    cnpjs = scores_per_profile.select("cnpj").unique()

    if not config.get("active", False):
        return cnpjs.with_columns(pl.lit(False).alias("failed"))

    min_count = config.get("params", {}).get("min_offer_count", 0)

    cnpj_manager = characteristics.select(["cnpj", "fund_manager"])
    manager_counts = fund_managers.select(["fund_manager", "num_funds"])

    enriched = cnpjs.join(cnpj_manager, on="cnpj", how="left").join(
        manager_counts, on="fund_manager", how="left"
    )

    return enriched.select(
        [
            "cnpj",
            (
                pl.col("fund_manager").is_null()
                | pl.col("num_funds").is_null()
                | (pl.col("num_funds") < min_count)
            ).alias("failed"),
        ]
    )


def mo_guardrail_min_sharpe_12m(
    scores_per_profile: pl.DataFrame,
    sharpe_ratio: pl.DataFrame,
    config: Dict[str, Any],
) -> pl.DataFrame:
    """Filter funds with missing or below-threshold 12-month Sharpe ratio.

    Args:
        scores_per_profile: Scored funds with cnpj.
        sharpe_ratio: Sharpe data with cnpj, sharpe_12m.
        config: Config with active (bool) and params.min_sharpe_12m (float).

    Returns:
        DataFrame with cnpj, failed (bool).
    """
    cnpjs = scores_per_profile.select("cnpj").unique()

    if not config.get("active", False):
        return cnpjs.with_columns(pl.lit(False).alias("failed"))

    min_sharpe = config.get("params", {}).get("min_sharpe_12m", 0.0)

    enriched = cnpjs.join(
        sharpe_ratio.select(["cnpj", "sharpe_12m"]), on="cnpj", how="left"
    )

    return enriched.select(
        [
            "cnpj",
            (
                pl.col("sharpe_12m").is_null() | (pl.col("sharpe_12m") < min_sharpe)
            ).alias("failed"),
        ]
    )


def mo_guardrail_min_sharpe_3m(
    scores_per_profile: pl.DataFrame,
    sharpe_ratio: pl.DataFrame,
    config: Dict[str, Any],
) -> pl.DataFrame:
    """Filter funds with missing or below-threshold 3-month Sharpe ratio.

    Args:
        scores_per_profile: Scored funds with cnpj.
        sharpe_ratio: Sharpe data with cnpj, sharpe_3m.
        config: Config with active (bool) and params.min_sharpe_3m (float).

    Returns:
        DataFrame with cnpj, failed (bool).
    """
    cnpjs = scores_per_profile.select("cnpj").unique()

    if not config.get("active", False):
        return cnpjs.with_columns(pl.lit(False).alias("failed"))

    min_sharpe = config.get("params", {}).get("min_sharpe_3m", 0.0)

    enriched = cnpjs.join(
        sharpe_ratio.select(["cnpj", "sharpe_3m"]), on="cnpj", how="left"
    )

    return enriched.select(
        [
            "cnpj",
            (pl.col("sharpe_3m").is_null() | (pl.col("sharpe_3m") < min_sharpe)).alias(
                "failed"
            ),
        ]
    )


def mo_guardrail_no_funds_wo_manager(
    scores_per_profile: pl.DataFrame,
    characteristics: pl.DataFrame,
    config: Dict[str, Any],
) -> pl.DataFrame:
    """Filter funds without an assigned manager.

    Args:
        scores_per_profile: Scored funds with cnpj.
        characteristics: Fund metadata with cnpj, fund_manager.
        config: Config with active (bool).

    Returns:
        DataFrame with cnpj, failed (bool).
    """
    cnpjs = scores_per_profile.select("cnpj").unique()

    if not config.get("active", False):
        return cnpjs.with_columns(pl.lit(False).alias("failed"))

    cnpj_manager = characteristics.select(["cnpj", "fund_manager"])
    enriched = cnpjs.join(cnpj_manager, on="cnpj", how="left")

    return enriched.select(
        [
            "cnpj",
            pl.col("fund_manager").is_null().alias("failed"),
        ]
    )


def mo_guardrail_include_only_active_funds(
    scores_per_profile: pl.DataFrame,
    returns_per_fund: pl.DataFrame,
    max_period: int,
    config: Dict[str, Any],
) -> pl.DataFrame:
    """Filter funds without data in the most recent period.

    Args:
        scores_per_profile: Scored funds with cnpj.
        returns_per_fund: Returns data with cnpj, period.
        max_period: Most recent period in YYYYMM format.
        config: Config with active (bool).

    Returns:
        DataFrame with cnpj, failed (bool).
    """
    cnpjs = scores_per_profile.select("cnpj").unique()

    if not config.get("active", False):
        return cnpjs.with_columns(pl.lit(False).alias("failed"))

    active_funds = (
        returns_per_fund.filter(pl.col("period") == max_period)
        .select("cnpj")
        .unique()
        .with_columns(pl.lit(True).alias("has_latest_data"))
    )

    enriched = cnpjs.join(active_funds, on="cnpj", how="left")

    return enriched.select(
        [
            "cnpj",
            (pl.col("has_latest_data").is_null() | ~pl.col("has_latest_data")).alias(
                "failed"
            ),
        ]
    )


def mo_guardrail_no_extreme_returns(
    scores_per_profile: pl.DataFrame,
    returns_per_fund: pl.DataFrame,
    config: Dict[str, Any],
) -> pl.DataFrame:
    """Filter funds with extreme monthly returns (likely capital flow issues).

    Funds with any monthly return exceeding the threshold are flagged.
    This catches cases where NAV changes due to subscriptions/redemptions
    are incorrectly interpreted as investment returns.

    Args:
        scores_per_profile: Scored funds with cnpj.
        returns_per_fund: Returns data with cnpj, period, monthly_return.
        config: Config with active (bool) and params.max_abs_monthly_return (float).

    Returns:
        DataFrame with cnpj, failed (bool).
    """
    cnpjs = scores_per_profile.select("cnpj").unique()

    if not config.get("active", False):
        return cnpjs.with_columns(pl.lit(False).alias("failed"))

    max_return = config.get("params", {}).get("max_abs_monthly_return", 1.0)

    extreme_funds = (
        returns_per_fund.filter(
            (pl.col("monthly_return") > max_return)
            | (pl.col("monthly_return") < -max_return)
        )
        .select("cnpj")
        .unique()
        .with_columns(pl.lit(True).alias("has_extreme_return"))
    )

    enriched = cnpjs.join(extreme_funds, on="cnpj", how="left")

    return enriched.select(
        [
            "cnpj",
            pl.col("has_extreme_return").fill_null(False).alias("failed"),
        ]
    )


def mo_guardrail_merge(
    gr_min_offer: pl.DataFrame,
    gr_sharpe_12m: pl.DataFrame,
    gr_sharpe_3m: pl.DataFrame,
    gr_no_manager: pl.DataFrame,
    gr_active_funds: pl.DataFrame,
    gr_extreme_returns: pl.DataFrame,
) -> pl.DataFrame:
    """Merge all guardrail results into pass/fail with failed_guardrails list.

    Args:
        gr_min_offer: Result from min_offer_per_issuer guardrail.
        gr_sharpe_12m: Result from min_sharpe_12m guardrail.
        gr_sharpe_3m: Result from min_sharpe_3m guardrail.
        gr_no_manager: Result from no_funds_wo_manager guardrail.
        gr_active_funds: Result from include_only_active_funds guardrail.
        gr_extreme_returns: Result from no_extreme_returns guardrail.

    Returns:
        DataFrame with cnpj, pass_guardrail (bool), failed_guardrails (comma-separated).
    """
    guardrails = [
        ("min_offer_per_issuer", gr_min_offer),
        ("min_sharpe_12m", gr_sharpe_12m),
        ("min_sharpe_3m", gr_sharpe_3m),
        ("no_funds_wo_manager", gr_no_manager),
        ("include_only_active_funds", gr_active_funds),
        ("no_extreme_returns", gr_extreme_returns),
    ]

    result = gr_min_offer.select("cnpj")

    for name, df in guardrails:
        result = result.join(
            df.rename({"failed": f"failed_{name}"}),
            on="cnpj",
            how="left",
        )

    failed_cols = [f"failed_{name}" for name, _ in guardrails]

    result = result.with_columns(
        pl.concat_str(
            [
                pl.when(pl.col(f"failed_{name}"))
                .then(pl.lit(name))
                .otherwise(pl.lit(""))
                for name, _ in guardrails
            ],
            separator=",",
        )
        .str.replace_all(r"^,+|,+$", "")
        .str.replace_all(r",+", ",")
        .alias("failed_guardrails_raw")
    )

    result = result.with_columns(
        [
            ~pl.any_horizontal(*[pl.col(c) for c in failed_cols]).alias(
                "pass_guardrail"
            ),
            pl.when(pl.col("failed_guardrails_raw") == "")
            .then(pl.lit(None))
            .otherwise(pl.col("failed_guardrails_raw"))
            .alias("failed_guardrails"),
        ]
    )

    return result.select(["cnpj", "pass_guardrail", "failed_guardrails"])
