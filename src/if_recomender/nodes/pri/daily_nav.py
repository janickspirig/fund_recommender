import polars as pl


def pri_create_daily_nav_data(
    daily_quotas: pl.DataFrame,
    funds_in_scope: pl.DataFrame,
) -> pl.DataFrame:
    """Extract daily NAV (quota) values from normalized CVM daily data.

    Handles two scenarios:
    1. Data WITH ID_SUBCLASSE (Oct 2024+): Selects primary share class by
       highest NAV on latest date, uses consistently across all dates.
    2. Data WITHOUT ID_SUBCLASSE (older): Deduplicates by highest NAV per day
       (funds may have changed TP_FUNDO_CLASSE over time).

    Args:
        daily_quotas: Normalized daily quota data (from int_daily_quotas).
        funds_in_scope: DataFrame with 'cnpj' column of funds to include.

    Returns:
        DataFrame with cnpj, date, period, quota_value for each trading day.
    """
    cnpjs_in_scope = funds_in_scope["cnpj"].to_list()
    df = daily_quotas.filter(pl.col("cnpj").is_in(cnpjs_in_scope))

    df = df.filter(
        pl.col("VL_QUOTA").is_not_null()
        & pl.col("VL_QUOTA").is_finite()
        & (pl.col("VL_QUOTA") > 0)
    )

    df_with_subclass = df.filter(pl.col("ID_SUBCLASSE").is_not_null())
    df_without_subclass = df.filter(pl.col("ID_SUBCLASSE").is_null())

    results = []

    if df_with_subclass.height > 0:
        latest_dates = df_with_subclass.group_by("cnpj").agg(
            pl.col("DT_COMPTC").max().alias("latest_date")
        )
        df_latest = df_with_subclass.join(latest_dates, on="cnpj")
        latest_records = df_latest.filter(pl.col("DT_COMPTC") == pl.col("latest_date"))

        primary_classes = (
            latest_records.sort(["cnpj", "VL_PATRIM_LIQ"], descending=[False, True])
            .group_by("cnpj")
            .first()
            .select(["cnpj", "ID_SUBCLASSE"])
            .rename({"ID_SUBCLASSE": "primary_subclass"})
        )

        df_primary = df_with_subclass.join(primary_classes, on="cnpj").filter(
            pl.col("ID_SUBCLASSE") == pl.col("primary_subclass")
        )
        results.append(df_primary)

    if df_without_subclass.height > 0:
        # For each (cnpj, date), keep the record with highest NAV
        df_deduped = df_without_subclass.sort(
            ["cnpj", "DT_COMPTC", "VL_PATRIM_LIQ"], descending=[False, False, True]
        ).unique(subset=["cnpj", "DT_COMPTC"], keep="first")
        results.append(df_deduped)

    combined = pl.concat(results, how="diagonal_relaxed") if results else df.head(0)

    result = combined.select(
        [
            pl.col("cnpj"),
            pl.col("DT_COMPTC").str.to_datetime(strict=False).dt.date().alias("date"),
            pl.col("DT_COMPTC")
            .str.to_datetime(strict=False)
            .dt.strftime("%Y%m")
            .cast(pl.Int32)
            .alias("period"),
            pl.col("VL_QUOTA").cast(pl.Float64).alias("quota_value"),
        ]
    )

    result = result.unique(subset=["cnpj", "date"], keep="first")
    return result.sort(["cnpj", "date"])
