import polars as pl


def feat_calculate_liquidity(characteristics: pl.DataFrame) -> pl.DataFrame:
    """Calculate liquidity score normalized from redemption days.

    Higher score = faster redemption. Inactive funds get score of 0.

    Args:
        characteristics: Fund characteristics with cnpj, redemption_days, is_active.

    Returns:
        DataFrame with cnpj, redemption_days, is_active, liquidity_score (0-1).
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
