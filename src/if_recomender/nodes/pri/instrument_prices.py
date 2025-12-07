"""Position values time series from instrument registry."""

import polars as pl


def pri_create_instrument_prices(
    instrument_registry: pl.DataFrame,
    max_period: int,
) -> pl.DataFrame:
    """Extract position values time series from instrument registry.

    Marks positions as active (is_active=1) if they appear in the latest period.

    Args:
        instrument_registry: All BLC positions with instrument_id and period.
        max_period: Most recent period in YYYYMM format.

    Returns:
        DataFrame with cnpj, instrument_id, period, position_value, asset_type, is_active.
    """
    latest_period = str(max_period)

    return instrument_registry.select(
        [
            "cnpj",
            "instrument_id",
            "period",
            pl.col("VL_MERC_POS_FINAL").cast(pl.Float64).alias("position_value"),
            pl.col("TP_ATIVO").alias("asset_type"),
            pl.when(pl.col("period").cast(pl.Utf8) == latest_period)
            .then(1)
            .otherwise(0)
            .alias("is_active"),
        ]
    )
