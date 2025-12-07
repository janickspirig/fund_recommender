import polars as pl


def pri_create_monthly_nav_data(period_fi_fund_data: pl.DataFrame) -> pl.DataFrame:
    """Extract monthly NAV values from CVM period data.

    Converts date to YYYYMM format and deduplicates records.

    Args:
        period_fi_fund_data: CVM data with cnpj, DT_COMPTC, VL_PATRIM_LIQ.

    Returns:
        DataFrame with cnpj, period (YYYYMM string), price (NAV as float).
    """

    period_fi_fund_data = period_fi_fund_data.select(
        [
            pl.col("cnpj"),
            pl.col("DT_COMPTC")
            .str.to_datetime(strict=False)
            .dt.strftime("%Y%m")
            .alias("period"),
            pl.col("VL_PATRIM_LIQ").cast(pl.Float64).alias("price"),
        ]
    )

    # addressing issue in cvm data of duplicated funds
    period_fi_fund_data = period_fi_fund_data.unique()

    return period_fi_fund_data
