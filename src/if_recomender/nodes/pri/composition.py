import polars as pl


def pri_create_composition(
    instrument_prices: pl.DataFrame,
    nav_per_period: pl.DataFrame,
    blc_8_accounting_entry_patterns: list,
) -> pl.DataFrame:
    """
    Create static instrument registry from price history.
    Determines is_active based on presence in latest period per fund.
    Excludes accounting entries (payables, receivables) from blc_8.

    Args:
        instrument_prices: DataFrame with cnpj, instrument_id, period, position_value
        nav_per_period: DataFrame with cnpj, period, price (NAV data)
        blc_8_accounting_entry_patterns: List of patterns to exclude from blc_8

    Returns:
        DataFrame with columns:
        - cnpj: Fund identifier
        - instrument_id: Unique instrument identifier
        - blc_type: Extracted from instrument_id (blc_1 to blc_8)
        - asset_category: Standardized category
        - is_active: 1 if held in latest period per fund, 0 otherwise

    """

    if blc_8_accounting_entry_patterns:
        pattern = "(?i)" + "|".join(blc_8_accounting_entry_patterns)
        instrument_prices = instrument_prices.filter(
            ~pl.col("instrument_id").str.contains(pattern)
        )

    latest_period_per_fund = nav_per_period.group_by("cnpj").agg(
        pl.col("period").max().alias("latest_period")
    )

    unique_instruments = instrument_prices.select(["cnpj", "instrument_id"]).unique()

    unique_instruments = unique_instruments.with_columns(
        [
            pl.concat_str(
                [
                    pl.col("instrument_id").str.split("_").list.get(0),
                    pl.lit("_"),
                    pl.col("instrument_id").str.split("_").list.get(1),
                ]
            ).alias("blc_type")
        ]
    )

    category_map = {
        "blc_1": "Government",
        "blc_2": "FundQuotas",
        "blc_3": "Derivatives",
        "blc_4": "Equities",
        "blc_5": "PrivateCredit",
        "blc_6": "BankDeposits",
        "blc_7": "ForeignAssets",
        "blc_8": "OtherAssets",
    }

    unique_instruments = unique_instruments.with_columns(
        [pl.col("blc_type").replace(category_map, default=None).alias("asset_category")]
    )

    active_instruments = (
        instrument_prices.join(latest_period_per_fund, on="cnpj", how="inner")
        .filter(pl.col("period") == pl.col("latest_period"))
        .select(["cnpj", "instrument_id"])
        .with_columns([pl.lit(1).alias("is_active")])
    )

    composition = unique_instruments.join(
        active_instruments, on=["cnpj", "instrument_id"], how="left"
    )

    composition = composition.with_columns([pl.col("is_active").fill_null(0)]).select(
        ["cnpj", "instrument_id", "blc_type", "asset_category", "is_active"]
    )

    return composition
