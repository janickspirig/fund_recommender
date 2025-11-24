import polars as pl


def feat_calculate_concentration(
    instrument_prices: pl.DataFrame,
    period_nav: pl.DataFrame,
) -> pl.DataFrame:
    """
    Calculate portfolio concentration metrics from instrument prices.

    Args:
        instrument_prices: DataFrame with cnpj, instrument_id, period, position_value
        period_nav: DataFrame with cnpj, period, price (NAV)

    Returns:
        DataFrame with columns:
        - cnpj: Fund identifier
        - concentration_hhi: Herfindahl-Hirschman Index (0-1, higher = more concentrated)
        - concentration_top10_pct: % of NAV in top 10 holdings
        - concentration_num_holdings: Number of positions
    """
    latest_period = period_nav["period"].max()

    latest_nav = period_nav.filter(pl.col("period") == latest_period)
    latest_prices = instrument_prices.filter(pl.col("period") == latest_period)

    prices_with_nav = latest_prices.join(
        latest_nav.select(["cnpj", "price"]),
        on="cnpj",
        how="left",
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

    return concentration
