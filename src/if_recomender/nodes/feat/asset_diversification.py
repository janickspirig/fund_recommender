import polars as pl


def feat_calculate_asset_diversification(
    composition: pl.DataFrame,
    instrument_prices: pl.DataFrame,
) -> pl.DataFrame:
    """
    Calculate asset-class diversification metrics from current holdings.

    Measures how a fund's portfolio is distributed across different asset categories
    (Government, FundQuotas, Derivatives, FixedIncome, PrivateCredit, BankDeposits,
    ForeignAssets, OtherAssets).

    Args:
        composition: DataFrame with cnpj, instrument_id, asset_category, is_active
        instrument_prices: DataFrame with cnpj, instrument_id, period, position_value

    Returns:
        DataFrame with columns:
        - cnpj: Fund identifier
        - asset_diversification_hhi: Category-level HHI (0-1, higher = more concentrated)
        - top_category_pct: % of portfolio in largest asset category
    """

    latest_period = instrument_prices["period"].max()

    active_composition = composition.filter(pl.col("is_active") == 1)

    current_prices = instrument_prices.filter(pl.col("period") == latest_period)

    prices_with_cat = current_prices.join(
        active_composition.select(["cnpj", "instrument_id", "asset_category"]),
        on=["cnpj", "instrument_id"],
        how="inner",
    ).filter(pl.col("asset_category").is_not_null())

    category_values = prices_with_cat.group_by(["cnpj", "asset_category"]).agg(
        pl.col("position_value").sum().alias("category_value")
    )

    category_values = category_values.with_columns(
        [pl.col("category_value").sum().over("cnpj").alias("total_value")]
    ).with_columns([(pl.col("category_value") / pl.col("total_value")).alias("w_cat")])

    diversification = category_values.group_by("cnpj").agg(
        (pl.col("w_cat") ** 2).sum().alias("category_hhi"),
        pl.col("w_cat").max().alias("top_category_pct"),
    )

    diversification = diversification.select(
        "cnpj",
        pl.col("category_hhi").alias("asset_diversification_hhi"),
        pl.col("top_category_pct"),
    )

    return diversification
