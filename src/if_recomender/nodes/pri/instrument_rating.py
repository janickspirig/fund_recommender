"""Credit ratings extraction from BLC5 positions."""

import polars as pl


def pri_create_instrument_rating(instrument_registry: pl.DataFrame) -> pl.DataFrame:
    """Extract credit ratings from BLC_5 positions in the instrument registry.

    Only includes positions with non-null ratings (GRAU_RISCO).

    Args:
        instrument_registry: All BLC positions with instrument_id and period.

    Returns:
        DataFrame with cnpj, instrument_id, period, issuer_cnpj, credit_rating, rating_agency.
    """
    return (
        instrument_registry.filter(pl.col("blc_type") == "blc_5")
        .filter(pl.col("GRAU_RISCO").is_not_null())
        .select(
            [
                "cnpj",
                "instrument_id",
                "period",
                pl.col("CNPJ_EMISSOR").str.replace_all(r"\D", "").alias("issuer_cnpj"),
                pl.col("GRAU_RISCO").alias("credit_rating"),
                pl.col("AG_RISCO").alias("rating_agency"),
            ]
        )
    )
