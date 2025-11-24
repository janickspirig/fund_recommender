import polars as pl


def pri_create_monthly_nav_data(period_fi_fund_data: pl.DataFrame) -> pl.DataFrame:
    """
    Create feature primary table from filtered data containing monthly net asset values.
    Args:
        period_fi_fund_data: Polars DataFrame with filtered period fixed income data.
    Returns:
        Polars DataFrame with columns: cnpj (fund identifier), period (YYYYMM), price (float value).
    """

    feature_primary_table = period_fi_fund_data.select(
        [
            pl.col("cnpj"),
            pl.col("DT_COMPTC")
            .str.to_datetime(strict=False)
            .dt.strftime("%Y%m")
            .alias("period"),
            pl.col("VL_PATRIM_LIQ").cast(pl.Float64).alias("price"),
        ]
    )

    return feature_primary_table
