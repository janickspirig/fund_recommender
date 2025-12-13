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
    daily_returns: pl.DataFrame,
    max_ref_date: str,
    config: Dict[str, Any],
) -> pl.DataFrame:
    """Filter funds without data on the most recent reference date.
    Args:
        scores_per_profile: Scored funds with cnpj.
        daily_returns: Daily returns data with cnpj, date.
        max_ref_date: Most recent date in YYYY-MM-DD format.
        config: Config with active (bool).

    Returns:
        DataFrame with cnpj, failed (bool).
    """
    cnpjs = scores_per_profile.select("cnpj").unique()

    if not config.get("active", False):
        return cnpjs.with_columns(pl.lit(False).alias("failed"))

    active_funds = (
        daily_returns.filter(pl.col("date") == max_ref_date)
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
    daily_returns: pl.DataFrame,
    config: Dict[str, Any],
) -> pl.DataFrame:
    """Filter funds with extreme daily returns.

    Funds with any daily return exceeding the threshold are flagged.
    Since we now use quota-based returns, extreme values indicate
    data quality issues rather than capital flow distortions.

    Args:
        scores_per_profile: Scored funds with cnpj.
        daily_returns: Daily returns data with cnpj, daily_return.
        config: Config with active (bool) and params.max_abs_daily_return (float).

    Returns:
        DataFrame with cnpj, failed (bool).
    """
    cnpjs = scores_per_profile.select("cnpj").unique()

    if not config.get("active", False):
        return cnpjs.with_columns(pl.lit(False).alias("failed"))

    # Default to 0.10 (10% daily return as extreme)
    max_return = config.get("params", {}).get("max_abs_daily_return", 0.10)

    extreme_funds = (
        daily_returns.filter(
            (pl.col("daily_return") > max_return)
            | (pl.col("daily_return") < -max_return)
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


def mo_guardrail_min_cov_sharpe_12m(
    scores_per_profile: pl.DataFrame,
    sharpe_ratio: pl.DataFrame,
    config: Dict[str, Any],
) -> pl.DataFrame:
    """Filter funds with insufficient 12-month data coverage.

    Args:
        scores_per_profile: Scored funds with cnpj.
        sharpe_ratio: Sharpe data with cnpj, pct_cov_12m.
        config: Config with active (bool) and params.min_coverage_12m (float).

    Returns:
        DataFrame with cnpj, failed (bool).
    """
    cnpjs = scores_per_profile.select("cnpj").unique()

    if not config.get("active", False):
        return cnpjs.with_columns(pl.lit(False).alias("failed"))

    min_coverage = config.get("params", {}).get("min_coverage_12m", 0.80)

    enriched = cnpjs.join(
        sharpe_ratio.select(["cnpj", "pct_cov_12m"]), on="cnpj", how="left"
    )

    return enriched.select(
        [
            "cnpj",
            (
                pl.col("pct_cov_12m").is_null() | (pl.col("pct_cov_12m") < min_coverage)
            ).alias("failed"),
        ]
    )


def mo_guardrail_min_cov_sharpe_3m(
    scores_per_profile: pl.DataFrame,
    sharpe_ratio: pl.DataFrame,
    config: Dict[str, Any],
) -> pl.DataFrame:
    """Filter funds with insufficient 3-month data coverage.

    Args:
        scores_per_profile: Scored funds with cnpj.
        sharpe_ratio: Sharpe data with cnpj, pct_cov_3m.
        config: Config with active (bool) and params.min_coverage_3m (float).

    Returns:
        DataFrame with cnpj, failed (bool).
    """
    cnpjs = scores_per_profile.select("cnpj").unique()

    if not config.get("active", False):
        return cnpjs.with_columns(pl.lit(False).alias("failed"))

    min_coverage = config.get("params", {}).get("min_coverage_3m", 0.80)

    enriched = cnpjs.join(
        sharpe_ratio.select(["cnpj", "pct_cov_3m"]), on="cnpj", how="left"
    )

    return enriched.select(
        [
            "cnpj",
            (
                pl.col("pct_cov_3m").is_null() | (pl.col("pct_cov_3m") < min_coverage)
            ).alias("failed"),
        ]
    )


def mo_guardrail_merge(
    gr_min_offer: pl.DataFrame,
    gr_sharpe_12m: pl.DataFrame,
    gr_sharpe_3m: pl.DataFrame,
    gr_no_manager: pl.DataFrame,
    gr_active_funds: pl.DataFrame,
    gr_extreme_returns: pl.DataFrame,
    gr_cov_sharpe_12m: pl.DataFrame,
    gr_cov_sharpe_3m: pl.DataFrame,
) -> pl.DataFrame:
    """Merge all guardrail results into pass/fail with failed_guardrails list.

    Args:
        gr_min_offer: Result from min_offer_per_issuer guardrail.
        gr_sharpe_12m: Result from min_sharpe_12m guardrail.
        gr_sharpe_3m: Result from min_sharpe_3m guardrail.
        gr_no_manager: Result from no_funds_wo_manager guardrail.
        gr_active_funds: Result from include_only_active_funds guardrail.
        gr_extreme_returns: Result from no_extreme_returns guardrail.
        gr_cov_sharpe_12m: Result from min_cov_sharpe_12m guardrail.
        gr_cov_sharpe_3m: Result from min_cov_sharpe_3m guardrail.

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
        ("min_cov_sharpe_12m", gr_cov_sharpe_12m),
        ("min_cov_sharpe_3m", gr_cov_sharpe_3m),
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
