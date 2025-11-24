import polars as pl


def feat_calculate_concentration(
    composition: pl.DataFrame,
    instrument_prices: pl.DataFrame,
    period_nav: pl.DataFrame,
) -> pl.DataFrame:
    """
    Calculate portfolio concentration metrics from instrument prices.

    Uses composition.is_active to identify current holdings and aggregates by max period
    to get the latest position value for each instrument. This ensures each instrument
    is counted exactly once, preventing HHI > 1 issues.

    Args:
        composition: DataFrame with cnpj, instrument_id, is_active
        instrument_prices: DataFrame with cnpj, instrument_id, period, position_value
        period_nav: DataFrame with cnpj, period, price (NAV)

    Returns:
        DataFrame with columns:
        - cnpj: Fund identifier
        - concentration_hhi: Herfindahl-Hirschman Index (0-1, higher = more concentrated)
        - concentration_top10_pct: % of NAV in top 10 holdings
        - concentration_num_holdings: Number of positions
    """

    active_composition = composition.filter(pl.col("is_active") == 1)

    latest_period_per_fund = period_nav.group_by("cnpj").agg(
        pl.col("period").max().alias("latest_period")
    )

    latest_prices = (
        instrument_prices.join(latest_period_per_fund, on="cnpj", how="inner")
        .filter(pl.col("period") == pl.col("latest_period"))
        .select(["cnpj", "instrument_id", "position_value"])
    )

    latest_prices = latest_prices.join(
        active_composition.select(["cnpj", "instrument_id"]),
        on=["cnpj", "instrument_id"],
        how="inner",
    )

    latest_nav = (
        period_nav.join(latest_period_per_fund, on="cnpj", how="inner")
        .filter(pl.col("period") == pl.col("latest_period"))
        .select(["cnpj", "price"])
        .filter((pl.col("price") > 0) & pl.col("price").is_not_null())
    )

    prices_with_nav = latest_prices.join(
        latest_nav.select(["cnpj", "price"]), on="cnpj", how="inner"
    )

    prices_with_nav = prices_with_nav.with_columns(
        (pl.col("position_value") / pl.col("price")).alias("position_pct")
    )

    hhi_df = prices_with_nav.group_by("cnpj").agg(
        [
            (pl.col("position_pct") ** 2).sum().alias("hhi"),
            pl.col("instrument_id").count().alias("n_holdings"),
        ]
    )

    top10_df = prices_with_nav.group_by("cnpj").agg(
        pl.col("position_pct").top_k(10).sum().alias("top_10_pct")
    )

    concentration = hhi_df.join(top10_df, on="cnpj", how="left")

    concentration = concentration.select(
        "cnpj",
        pl.col("hhi").alias("concentration_hhi"),
        pl.col("top_10_pct").alias("concentration_top10_pct"),
        pl.col("n_holdings").alias("concentration_num_holdings"),
    )

    # Set HHI and related metrics to null when HHI > 1.0 (data quality issue)
    concentration = concentration.with_columns(
        [
            pl.when(pl.col("concentration_hhi") > 1.0)
            .then(None)
            .otherwise(pl.col("concentration_hhi"))
            .alias("concentration_hhi"),
            pl.when(pl.col("concentration_hhi") > 1.0)
            .then(None)
            .otherwise(pl.col("concentration_top10_pct"))
            .alias("concentration_top10_pct"),
        ]
    )

    return concentration
