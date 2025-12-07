import polars as pl


def feat_calculate_asset_diversification(
    instrument_prices: pl.DataFrame,
) -> pl.DataFrame:
    """Calculate asset-class diversification from current holdings.

    Uses gross exposure (absolute values) so shorts do not cancel longs.

    Args:
        instrument_prices: Position data with cnpj, asset_type, position_value, is_active.

    Returns:
        DataFrame with cnpj, asset_diversification_hhi (0-1), top_category_pct.
    """
    latest_prices_w_type = (
        instrument_prices.filter(pl.col("is_active") == 1)
        .filter(pl.col("asset_type").is_not_null())
        .select(["cnpj", "asset_type", "instrument_id", "position_value"])
    )

    category_values = latest_prices_w_type.group_by(["cnpj", "asset_type"]).agg(
        pl.col("position_value").abs().sum().alias("category_value")
    )

    category_values = (
        category_values.with_columns(
            pl.col("category_value").sum().over("cnpj").alias("total_value")
        )
        .filter(pl.col("total_value") > 0)
        .with_columns((pl.col("category_value") / pl.col("total_value")).alias("w_cat"))
    )

    diversification = category_values.group_by("cnpj").agg(
        (pl.col("w_cat") ** 2).sum().alias("asset_diversification_hhi"),
        pl.col("w_cat").max().alias("top_category_pct"),
    )

    return diversification
