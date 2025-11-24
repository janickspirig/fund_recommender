import polars as pl
import datetime


def feat_calculate_fund_age(
    characteristics: pl.DataFrame,
    fund_age_cap_years: float,
) -> pl.DataFrame:
    """
    Calculate fund age and normalize to 0-1 score.

    Args:
        characteristics: DataFrame with cnpj, inception_date
        fund_age_cap_years: Maximum years for score cap

    Returns:
        DataFrame with cnpj, inception_date, fund_age_days, fund_age_years, fund_age_score
    """

    age_data = characteristics.select("cnpj", "inception_date")

    age_data = age_data.with_columns(
        pl.col("inception_date").str.to_date().alias("inception_date_parsed")
    )

    today = datetime.date.today()
    age_data = age_data.with_columns(
        (pl.lit(today) - pl.col("inception_date_parsed"))
        .dt.total_days()
        .alias("fund_age_days")
    )

    age_data = age_data.with_columns(
        pl.when(pl.col("fund_age_days").is_null())
        .then(None)
        .otherwise(pl.col("fund_age_days"))
        .alias("fund_age_days")
    )

    age_data = age_data.with_columns(
        (pl.col("fund_age_days") / 365.0).alias("fund_age_years")
    )

    # convert to to 0-1 score (capped at fund_age_cap_years)
    age_data = age_data.with_columns(
        [
            pl.when(pl.col("fund_age_years").is_null())
            .then(None)
            .otherwise((pl.col("fund_age_years") / fund_age_cap_years).clip(0.0, 1.0))
            .alias("fund_age_score")
        ]
    ).select(
        "cnpj",
        "inception_date",
        "fund_age_days",
        "fund_age_years",
        "fund_age_score",
    )

    return age_data
