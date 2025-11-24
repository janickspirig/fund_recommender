import polars as pl


def pri_create_monthly_composition(
    spine_funds: pl.DataFrame,
    blc_1: Dict[str, pl.DataFrame],
    blc_2: Dict[str, pl.DataFrame],
    blc_3: Dict[str, pl.DataFrame],
    blc_4: Dict[str, pl.DataFrame],
    blc_5: Dict[str, pl.DataFrame],
    blc_6: Dict[str, pl.DataFrame],
    blc_7: Dict[str, pl.DataFrame],
    blc_8: Dict[str, pl.DataFrame],
) -> pl.DataFrame:
    """
    Creates monthly level of composition per fund.

    Args:
        period_fi_fund_data: Polars DataFrame with filtered period fixed income data.
    Returns:
        Polars DataFrame with columns: cnpj (fund identifier), period (YYYYMM), price (float value).
    """
    feature_primary_table = period_fi_fund_data.select(
        [
            pl.col("cnpj"),  # Already normalized
            pl.col("DT_COMPTC")
            .str.to_datetime(strict=False)
            .dt.strftime("%Y%m")
            .alias("period"),
            pl.col("VL_PATRIM_LIQ").cast(pl.Float64).alias("price"),
        ]
    )

    return feature_primary_table
