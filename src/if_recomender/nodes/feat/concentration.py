import polars as pl


def feat_calculate_concentration(
    instrument_prices: pl.DataFrame,
) -> pl.DataFrame:
    """Calculate portfolio concentration metrics from current holdings.

    Uses position weights to compute HHI. Higher HHI = more concentrated.

    Args:
        instrument_prices: Position data with cnpj, instrument_id, position_value, is_active.

    Returns:
        DataFrame with cnpj, concentration_hhi (0-1), concentration_num_holdings.
    """

    latest_prices = instrument_prices.filter(pl.col("is_active") == 1).select(
        ["cnpj", "instrument_id", "position_value"]
    )

    latest_prices = latest_prices.with_columns(
        pl.col("position_value").abs().sum().over("cnpj").alias("fund_gross")
    ).with_columns(
        (pl.col("position_value").abs() / pl.col("fund_gross")).alias("position_pct")
    )

    hhi_df = latest_prices.group_by("cnpj").agg(
        [
            (pl.col("position_pct") ** 2).sum().alias("hhi"),
            pl.col("instrument_id").n_unique().alias("n_holdings"),
        ]
    )

    concentration = hhi_df.select(
        "cnpj",
        pl.col("hhi").alias("concentration_hhi"),
        pl.col("n_holdings").alias("concentration_num_holdings"),
    )
    return concentration
