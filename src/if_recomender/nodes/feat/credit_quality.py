import polars as pl


def feat_calculate_credit_quality(
    composition: pl.DataFrame,
    instrument_rating: pl.DataFrame,
    instrument_prices: pl.DataFrame,
    credit_rating_order: list,
    credit_rating_investment_grade_threshold: str,
) -> pl.DataFrame:
    """
    Calculate credit quality metrics from ratings.
    Only considers PrivateCredit instruments that are active (is_active = 1).
    Only the latest period/rating per instrument is considered.

    Args:
        composition: DataFrame with cnpj, instrument_id, asset_category, is_active
        instrument_rating: DataFrame with cnpj, instrument_id, period, credit_rating
        instrument_prices: DataFrame with cnpj, instrument_id, period, position_value
        credit_rating_order: List of credit ratings in descending order (best to worst)

    Returns:
        DataFrame with columns:
        - cnpj: Fund identifier
        - pct_rated: % of private credit positions with ratings (latest period only)
        - avg_rating_score: Numeric score (best=1.0, worst=0.1)
        - pct_investment_grade: % rated as investment grade (top 60% of rating order)
        - credit_quality_score: Combined score (0-1, higher = better quality)
    """

    private_credit_instruments = composition.filter(
        (pl.col("asset_category") == "PrivateCredit") & (pl.col("is_active") == 1)
    ).select(["cnpj", "instrument_id"])

    private_credit_prices = instrument_prices.join(
        private_credit_instruments, on=["cnpj", "instrument_id"], how="inner"
    )

    latest_periods = private_credit_prices.group_by(["cnpj", "instrument_id"]).agg(
        pl.col("period").max().alias("latest_period")
    )

    private_credit_prices_latest = (
        private_credit_prices.join(
            latest_periods, on=["cnpj", "instrument_id"], how="inner"
        )
        .filter(pl.col("period") == pl.col("latest_period"))
        .drop("latest_period")
    )

    with_ratings = private_credit_prices_latest.join(
        instrument_rating.select(["cnpj", "instrument_id", "period", "credit_rating"]),
        on=["cnpj", "instrument_id", "period"],
        how="left",
    )

    n = len(credit_rating_order)
    rating_map = {rating: (n - i) / n for i, rating in enumerate(credit_rating_order)}

    investment_grade_threshold = credit_rating_order.index(
        credit_rating_investment_grade_threshold
    )
    investment_grade_ratings = credit_rating_order[: investment_grade_threshold + 1]

    with_ratings = with_ratings.with_columns(
        pl.col("credit_rating").replace(rating_map, default=None).alias("rating_score"),
        pl.when(pl.col("credit_rating").is_in(investment_grade_ratings))
        .then(1)
        .otherwise(
            pl.when(pl.col("credit_rating").is_not_null()).then(0).otherwise(None)
        )
        .alias("is_investment_grade"),
    )

    credit_quality = (
        with_ratings.filter(
            pl.col("credit_rating").is_not_null()
            & pl.col("rating_score").is_not_null()
            & pl.col("is_investment_grade").is_not_null()
        )
        .group_by("cnpj")
        .agg(
            (pl.col("credit_rating").is_not_null().sum() / pl.count() * 100).alias(
                "pct_rated"
            ),
            pl.col("rating_score").mean().alias("avg_rating_score"),
            (pl.col("is_investment_grade").sum() / pl.count() * 100).alias(
                "pct_investment_grade"
            ),
        )
    )

    # weighting: 40% pct_rated, 40% avg_rating_score, 20% pct_investment_grade
    credit_quality = credit_quality.with_columns(
        (
            pl.col("pct_rated") / 100 * 0.4
            + pl.col("avg_rating_score").fill_null(0) * 0.4
            + pl.col("pct_investment_grade") / 100 * 0.2
        ).alias("credit_quality_score")
    ).select(
        "cnpj",
        "pct_rated",
        "avg_rating_score",
        "pct_investment_grade",
        "credit_quality_score",
    )

    return credit_quality
