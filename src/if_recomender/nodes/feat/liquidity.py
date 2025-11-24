import polars as pl


def feat_calculate_liquidity(characteristics: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate liquidity score based on redemption days.

    Args:
        characteristics: DataFrame with cnpj, redemption_days, is_active

    Returns:
        DataFrame with cnpj, redemption_days, is_active, liquidity_score
    """
    active_funds = characteristics.filter(pl.col("is_active") == 1)
    min_days = active_funds["redemption_days"].min()
    max_days = active_funds["redemption_days"].max()

    liquidity_features = characteristics.select(
        ["cnpj", "redemption_days", "is_active"]
    ).with_columns(
        pl.when(pl.col("is_active") == 1)
        .then(
            pl.when(max_days > min_days)
            .then(
                1.0 - ((pl.col("redemption_days") - min_days) / (max_days - min_days))
            )
            .otherwise(1.0)
        )
        .otherwise(0.0)
        .alias("liquidity_score")
    )

    return liquidity_features
