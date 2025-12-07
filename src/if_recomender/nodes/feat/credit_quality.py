import polars as pl


def feat_calculate_credit_quality(
    instrument_rating: pl.DataFrame,
    instrument_prices: pl.DataFrame,
    credit_rating: dict,
) -> pl.DataFrame:
    """Calculate credit quality score from ratings on active positions.

    Combines avg_rating_score and pct_investment_grade using configured weights.
    Only considers positions that are active and have ratings in the order list.

    Args:
        instrument_rating: Ratings with cnpj, instrument_id, period, credit_rating.
        instrument_prices: Position data with cnpj, instrument_id, period, is_active.
        credit_rating: Config with order, threshold, and weights.

    Returns:
        DataFrame with cnpj, pct_rated, avg_rating_score, pct_investment_grade,
        credit_quality_score.
    """
    credit_rating_order = credit_rating["order"]
    investment_grade_threshold_str = credit_rating[
        "credit_rating_investment_grade_threshold"
    ]
    weight_avg = credit_rating["weight_avg_rating_score"]
    weight_ig = credit_rating["weight_investment_grade"]

    latest_prices = instrument_prices.filter(pl.col("is_active") == 1).select(
        ["cnpj", "instrument_id", "period", "position_value"]
    )

    latest_prices_w_ratings = latest_prices.join(
        instrument_rating, on=["cnpj", "instrument_id", "period"], how="inner"
    )

    n = len(credit_rating_order)
    rating_map = {rating: (n - i) / n for i, rating in enumerate(credit_rating_order)}

    investment_grade_threshold = credit_rating_order.index(
        investment_grade_threshold_str
    )
    investment_grade_ratings = credit_rating_order[: investment_grade_threshold + 1]

    latest_prices_w_ratings = latest_prices_w_ratings.with_columns(
        pl.col("credit_rating").replace(rating_map, default=None).alias("rating_score"),
        pl.when(pl.col("credit_rating").is_in(investment_grade_ratings))
        .then(1)
        .otherwise(
            pl.when(pl.col("credit_rating").is_not_null()).then(0).otherwise(None)
        )
        .alias("is_investment_grade"),
    )

    credit_quality = (
        latest_prices_w_ratings.filter(
            pl.col("rating_score").is_not_null()
            & pl.col("is_investment_grade").is_not_null()
        )
        .group_by("cnpj")
        .agg(
            (pl.col("credit_rating").is_not_null().sum() / pl.count()).alias(
                "pct_rated"
            ),
            pl.col("rating_score").mean().alias("avg_rating_score"),
            (pl.col("is_investment_grade").sum() / pl.count()).alias(
                "pct_investment_grade"
            ),
        )
    )

    credit_quality = credit_quality.with_columns(
        (
            pl.col("avg_rating_score") * weight_avg
            + pl.col("pct_investment_grade") * weight_ig
        ).alias("credit_quality_score")
    ).select(
        "cnpj",
        "pct_rated",
        "avg_rating_score",
        "pct_investment_grade",
        "credit_quality_score",
    )

    return credit_quality
