import polars as pl


def feat_calculate_asset_diversification(
    composition: pl.DataFrame,
    instrument_prices: pl.DataFrame,
    period_nav: pl.DataFrame,
) -> pl.DataFrame:
    """
    Calculate asset-class diversification metrics from current holdings.

    Measures how a fund's portfolio is distributed across different asset categories
    (Government, FundQuotas, Derivatives, FixedIncome, PrivateCredit, BankDeposits,
    ForeignAssets, OtherAssets).

    Filters instrument_prices to latest period per fund BEFORE joining to prevent
    memory overflow from massive intermediate tables.

    Args:
        composition: DataFrame with cnpj, instrument_id, asset_category, is_active
        instrument_prices: DataFrame with cnpj, instrument_id, period, position_value
        period_nav: DataFrame with cnpj, period, price (NAV)

    Returns:
        DataFrame with columns:
        - cnpj: Fund identifier
        - asset_diversification_hhi: Category-level HHI (0-1, higher = more concentrated)
        - top_category_pct: % of portfolio in largest asset category
    """

    active_composition = composition.filter(pl.col("is_active") == 1)

    latest_period_per_fund = period_nav.group_by("cnpj").agg(
        pl.col("period").max().alias("latest_period")
    )

    latest_prices = (
        instrument_prices.join(latest_period_per_fund, on="cnpj", how="inner")
        .filter(pl.col("period") == pl.col("latest_period"))
        .select(["cnpj", "instrument_id", "position_value"])
    )

    prices_with_cat = latest_prices.join(
        active_composition.select(["cnpj", "instrument_id", "asset_category"]),
        on=["cnpj", "instrument_id"],
        how="inner",
    ).filter(pl.col("asset_category").is_not_null())

    category_values = prices_with_cat.group_by(["cnpj", "asset_category"]).agg(
        pl.col("position_value").sum().alias("category_value")
    )

    category_values = category_values.with_columns(
        [pl.col("category_value").sum().over("cnpj").alias("total_value")]
    )

    category_values = category_values.filter(pl.col("total_value") > 0)

    category_values = category_values.with_columns(
        [(pl.col("category_value") / pl.col("total_value")).alias("w_cat")]
    )

    diversification = category_values.group_by("cnpj").agg(
        (pl.col("w_cat") ** 2).sum().alias("category_hhi"),
        pl.col("w_cat").max().alias("top_category_pct"),
    )

    diversification = diversification.select(
        "cnpj",
        pl.col("category_hhi").alias("asset_diversification_hhi"),
        pl.col("top_category_pct"),
    )

    # set HHI and related metrics to null when HHI > 1.0 (data quality issue)
    diversification = diversification.with_columns(
        [
            pl.when(pl.col("asset_diversification_hhi") > 1.0)
            .then(None)
            .otherwise(pl.col("asset_diversification_hhi"))
            .alias("asset_diversification_hhi"),
            pl.when(pl.col("asset_diversification_hhi") > 1.0)
            .then(None)
            .otherwise(pl.col("top_category_pct"))
            .alias("top_category_pct"),
        ]
    )

    return diversification
